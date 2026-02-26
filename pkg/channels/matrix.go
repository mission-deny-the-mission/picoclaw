package channels

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/utils"
)

// MatrixChannel implements the Channel interface for Matrix
// using the Matrix Client-Server API with sync for receiving messages
// and the mautrix SDK for sending messages.
type MatrixChannel struct {
	*BaseChannel
	config    config.MatrixConfig
	client    *mautrix.Client
	roomCache sync.Map // roomID -> *mautrix.Room
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewMatrixChannel creates a new Matrix channel instance.
func NewMatrixChannel(cfg config.MatrixConfig, messageBus *bus.MessageBus) (*MatrixChannel, error) {
	if cfg.HomeserverURL == "" || cfg.AccessToken == "" || cfg.UserID == "" {
		return nil, fmt.Errorf("matrix homeserver_url, access_token, and user_id are required")
	}

	base := NewBaseChannel("matrix", cfg, messageBus, cfg.AllowFrom)

	// Create Matrix client
	client, err := mautrix.NewClient(cfg.HomeserverURL, id.UserID(cfg.UserID), cfg.AccessToken)
	if err != nil {
		return nil, fmt.Errorf("failed to create matrix client: %w", err)
	}

	return &MatrixChannel{
		BaseChannel: base,
		config:      cfg,
		client:      client,
	}, nil
}

// Start launches the Matrix sync loop.
func (c *MatrixChannel) Start(ctx context.Context) error {
	logger.InfoC("matrix", "Starting Matrix channel")

	c.ctx, c.cancel = context.WithCancel(ctx)

	// Verify credentials
	whoami, err := c.client.Whoami(c.ctx)
	if err != nil {
		return fmt.Errorf("matrix whoami failed: %w", err)
	}

	logger.InfoCF("matrix", "Matrix client connected", map[string]any{
		"user_id":     whoami.UserID,
		"device_id":   whoami.DeviceID,
		"homeserver":  c.config.HomeserverURL,
	})

	// Start sync loop in background
	go c.syncLoop()

	c.setRunning(true)
	logger.InfoC("matrix", "Matrix channel started")
	return nil
}

// syncLoop continuously syncs with the Matrix server to receive messages.
func (c *MatrixChannel) syncLoop() {
	syncParams := &mautrix.ReqSync{}
	nextBatch := ""

	for {
		select {
		case <-c.ctx.Done():
			logger.InfoC("matrix", "Matrix sync loop stopped")
			return
		default:
		}

		if nextBatch != "" {
			syncParams.Since = nextBatch
		}

		resp, err := c.client.Sync(c.ctx, syncParams)
		if err != nil {
			logger.ErrorCF("matrix", "Sync error", map[string]any{
				"error": err.Error(),
			})
			// Wait before retrying
			select {
			case <-time.After(5 * time.Second):
			case <-c.ctx.Done():
				return
			}
			continue
		}

		nextBatch = resp.NextBatch

		// Process rooms
		for roomID, room := range resp.Rooms.Join {
			c.processRoomEvents(roomID, room)
		}

		// Process invited rooms (auto-join)
		for roomID, invite := range resp.Rooms.Invite {
			c.processInvite(roomID, invite)
		}
	}
}

// processInvite handles room invitations by auto-joining.
func (c *MatrixChannel) processInvite(roomID id.RoomID, invite mautrix.InvitedRoom) {
	// Check if inviter is allowed
	inviterID := id.UserID("")
	for _, event := range invite.State.StateEvents {
		if event.Type == event.StateMember && event.StateKey != nil {
			// Extract sender (inviter)
			inviterID = event.Sender
			break
		}
	}

	// Check allowlist
	if !c.IsAllowed(string(inviterID)) {
		logger.DebugCF("matrix", "Rejecting room invite from non-allowed user", map[string]any{
			"room_id":   roomID,
			"inviter":   inviterID,
		})
		return
	}

	// Auto-join the room
	_, err := c.client.JoinRoom(c.ctx, roomID.String(), nil)
	if err != nil {
		logger.ErrorCF("matrix", "Failed to join room", map[string]any{
			"room_id": roomID,
			"error":   err.Error(),
		})
		return
	}

	logger.InfoCF("matrix", "Auto-joined room", map[string]any{
		"room_id": roomID,
		"inviter": inviterID,
	})
}

// processRoomEvents processes events from a room.
func (c *MatrixChannel) processRoomEvents(roomID id.RoomID, room mautrix.JoinedRoom) {
	for _, evt := range room.Timeline.Events {
		if evt.Type != event.EventMessage {
			continue
		}

		// Skip own messages
		if evt.Sender.String() == c.config.UserID {
			continue
		}

		// Check allowlist
		if !c.IsAllowed(evt.Sender.String()) {
			logger.DebugCF("matrix", "Message rejected by allowlist", map[string]any{
				"sender_id": evt.Sender,
				"room_id":   roomID,
			})
			continue
		}

		// Skip edits
		if evt.Content.RelatesTo != nil && evt.Content.RelatesTo.RelType == "m.replace" {
			continue
		}

		senderID := evt.Sender.String()
		content := ""
		var mediaPaths []string
		localFiles := []string{}

		defer func() {
			for _, file := range localFiles {
				if err := os.Remove(file); err != nil {
					logger.DebugCF("matrix", "Failed to cleanup temp file", map[string]any{
						"file":  file,
						"error": err.Error(),
					})
				}
			}
		}()

		// Extract message content
		msgContent := evt.Content
		msgType := msgContent.MsgType

		if msgType == event.MsgText {
			content = msgContent.Body
			// Strip bot mention if present
			content = c.stripBotMention(content, roomID.String())
		} else if msgType == event.MsgImage {
			localPath := c.downloadMedia(roomID, evt.ID, msgContent.Body, "image")
			if localPath != "" {
				localFiles = append(localFiles, localPath)
				mediaPaths = append(mediaPaths, localPath)
				content = "[image]"
			}
		} else if msgType == event.MsgFile {
			localPath := c.downloadMedia(roomID, evt.ID, msgContent.Body, "file")
			if localPath != "" {
				localFiles = append(localFiles, localPath)
				mediaPaths = append(mediaPaths, localPath)
				content = "[file]"
			}
		} else if msgType == event.MsgAudio {
			localPath := c.downloadMedia(roomID, evt.ID, msgContent.Body, "audio")
			if localPath != "" {
				localFiles = append(localFiles, localPath)
				mediaPaths = append(mediaPaths, localPath)
				content = "[audio]"
			}
		} else if msgType == event.MsgVideo {
			localPath := c.downloadMedia(roomID, evt.ID, msgContent.Body, "video")
			if localPath != "" {
				localFiles = append(localFiles, localPath)
				mediaPaths = append(mediaPaths, localPath)
				content = "[video]"
			}
		} else if msgType == event.MsgLocation {
			content = fmt.Sprintf("[location: %s]", msgContent.Body)
		} else {
			content = fmt.Sprintf("[%s]", msgType)
		}

		if strings.TrimSpace(content) == "" {
			continue
		}

		// Determine peer kind and ID
		peerKind := "room"
		peerID := roomID.String()

		// Check if it's a direct message
		if c.isDirectMessage(roomID) {
			peerKind = "direct"
			peerID = senderID
		}

		metadata := map[string]string{
			"platform":   "matrix",
			"room_id":    roomID.String(),
			"event_id":   evt.ID.String(),
			"peer_kind":  peerKind,
			"peer_id":    peerID,
			"sender_id":  senderID,
		}

		logger.DebugCF("matrix", "Received message", map[string]any{
			"sender_id": senderID,
			"room_id":   roomID,
			"preview":   utils.Truncate(content, 50),
		})

		// Send typing notification
		c.sendTyping(roomID)

		c.HandleMessage(senderID, roomID.String(), content, mediaPaths, metadata)
	}
}

// isDirectMessage checks if a room is a direct message.
func (c *MatrixChannel) isDirectMessage(roomID id.RoomID) bool {
	// Try to get DM status
	dmEvent, err := c.client.GetAccountData(c.ctx, id.AccountDataDirectChats)
	if err != nil {
		return false
	}

	if dmEvent.Content.Raw == nil {
		return false
	}

	// Check if room is in DM list
	if dmList, ok := dmEvent.Content.Raw["m.direct"].(map[string]interface{}); ok {
		for _, rooms := range dmList {
			if roomList, ok := rooms.([]interface{}); ok {
				for _, room := range roomList {
					if room == roomID.String() {
						return true
					}
				}
			}
		}
	}

	return false
}

// stripBotMention removes @bot mentions from message content.
func (c *MatrixChannel) stripBotMention(content, roomID string) string {
	// Try to get bot's display name in this room
	displayName := c.config.UserID

	// Remove @displayName mentions
	content = strings.ReplaceAll(content, "@"+displayName, "")

	// Remove @localpart mentions
	if idx := strings.Index(c.config.UserID, ":"); idx > 0 {
		localpart := c.config.UserID[1:idx] // Skip @
		content = strings.ReplaceAll(content, "@"+localpart, "")
	}

	return strings.TrimSpace(content)
}

// downloadMedia downloads media from Matrix.
func (c *MatrixChannel) downloadMedia(roomID id.RoomID, eventID id.EventID, body, mediaType string) string {
	// Build media URL
	mediaURL := c.client.BuildMediaURL(eventID.String())

	ext := "." + mediaType
	if mediaType == "image" {
		ext = ".jpg"
	} else if mediaType == "file" {
		ext = ""
	}

	filename := fmt.Sprintf("matrix_%s%s", eventID, ext)
	return utils.DownloadFile(mediaURL, filename, utils.DownloadOptions{
		LoggerPrefix: "matrix",
		ExtraHeaders: map[string]string{
			"Authorization": "Bearer " + c.config.AccessToken,
		},
	})
}

// sendTyping sends a typing notification.
func (c *MatrixChannel) sendTyping(roomID id.RoomID) {
	_, err := c.client.SendTyping(c.ctx, roomID, true, 30000)
	if err != nil {
		logger.DebugCF("matrix", "Failed to send typing notification", map[string]any{
			"error": err.Error(),
		})
	}
}

// Stop gracefully stops the Matrix client.
func (c *MatrixChannel) Stop(ctx context.Context) error {
	logger.InfoC("matrix", "Stopping Matrix channel")

	if c.cancel != nil {
		c.cancel()
	}

	c.setRunning(false)
	logger.InfoC("matrix", "Matrix channel stopped")
	return nil
}

// Send sends a message to Matrix.
func (c *MatrixChannel) Send(ctx context.Context, msg bus.OutboundMessage) error {
	if !c.IsRunning() {
		return fmt.Errorf("matrix channel not running")
	}

	roomID := id.RoomID(msg.ChatID)

	// Send as text message
	_, err := c.client.SendMessageEvent(ctx, roomID, event.EventMessage, &event.MessageEventContent{
		MsgType: event.MsgText,
		Body:    msg.Content,
	})

	if err != nil {
		return fmt.Errorf("failed to send matrix message: %w", err)
	}

	logger.DebugCF("matrix", "Message sent", map[string]any{
		"room_id": roomID,
	})

	return nil
}
