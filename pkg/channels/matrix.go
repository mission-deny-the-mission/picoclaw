package channels

import (
	"context"
	"fmt"
	"os"
	"strings"

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
	config config.MatrixConfig
	client *mautrix.Client
	syncer *mautrix.DefaultSyncer
	ctx    context.Context
	cancel context.CancelFunc
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

	// Set up syncer
	syncer := mautrix.NewDefaultSyncer()
	client.Syncer = syncer

	return &MatrixChannel{
		BaseChannel: base,
		config:      cfg,
		client:      client,
		syncer:      syncer,
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

	// Register event handlers
	c.syncer.OnEventType(event.EventMessage, c.onMessage)
	c.syncer.OnEventType(event.StateMember, c.onMemberEvent)

	// Start sync loop in background
	go func() {
		err := c.client.Sync()
		if err != nil && c.ctx.Err() == nil {
			logger.ErrorCF("matrix", "Sync failed", map[string]any{
				"error": err.Error(),
			})
		}
	}()

	c.setRunning(true)
	logger.InfoC("matrix", "Matrix channel started")
	return nil
}

// onMemberEvent handles room membership events (for auto-joining invites).
func (c *MatrixChannel) onMemberEvent(ctx context.Context, evt *event.Event) {
	if evt.Type != event.StateMember {
		return
	}

	// Check if this is an invite to the bot
	membership, ok := evt.Content.Raw["membership"].(string)
	if !ok || membership != "invite" {
		return
	}

	// Check if the state key is the bot's user ID
	if evt.StateKey == nil || *evt.StateKey != c.config.UserID {
		return
	}

	// Check allowlist
	if !c.IsAllowed(evt.Sender.String()) {
		logger.DebugCF("matrix", "Rejecting room invite from non-allowed user", map[string]any{
			"room_id": evt.RoomID,
			"inviter": evt.Sender,
		})
		return
	}

	// Auto-join the room
	_, err := c.client.JoinRoomByID(ctx, evt.RoomID)
	if err != nil {
		logger.ErrorCF("matrix", "Failed to join room", map[string]any{
			"room_id": evt.RoomID,
			"error":   err.Error(),
		})
		return
	}

	logger.InfoCF("matrix", "Auto-joined room", map[string]any{
		"room_id": evt.RoomID,
		"inviter": evt.Sender,
	})
}

// onMessage handles incoming messages.
func (c *MatrixChannel) onMessage(ctx context.Context, evt *event.Event) {
	// Skip own messages
	if evt.Sender.String() == c.config.UserID {
		return
	}

	// Check allowlist
	if !c.IsAllowed(evt.Sender.String()) {
		logger.DebugCF("matrix", "Message rejected by allowlist", map[string]any{
			"sender_id": evt.Sender,
			"room_id":   evt.RoomID,
		})
		return
	}

	// Parse message content
	msgContent := evt.Content.AsMessage()
	if msgContent == nil {
		logger.DebugC("matrix", "Message content is nil")
		return
	}

	// Skip edits
	if msgContent.RelatesTo != nil && msgContent.RelatesTo.Type == "m.replace" {
		return
	}

	senderID := evt.Sender.String()
	roomID := evt.RoomID.String()
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
	msgType := msgContent.MsgType

	if msgType == event.MsgText {
		content = msgContent.Body
		// Strip bot mention if present
		content = c.stripBotMention(content, roomID)
	} else if msgType == event.MsgImage {
		localPath := c.downloadMedia(evt.ID, msgContent.Body, "image")
		if localPath != "" {
			localFiles = append(localFiles, localPath)
			mediaPaths = append(mediaPaths, localPath)
			content = "[image]"
		}
	} else if msgType == event.MsgFile {
		localPath := c.downloadMedia(evt.ID, msgContent.Body, "file")
		if localPath != "" {
			localFiles = append(localFiles, localPath)
			mediaPaths = append(mediaPaths, localPath)
			content = "[file]"
		}
	} else if msgType == event.MsgAudio {
		localPath := c.downloadMedia(evt.ID, msgContent.Body, "audio")
		if localPath != "" {
			localFiles = append(localFiles, localPath)
			mediaPaths = append(mediaPaths, localPath)
			content = "[audio]"
		}
	} else if msgType == event.MsgVideo {
		localPath := c.downloadMedia(evt.ID, msgContent.Body, "video")
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
		return
	}

	// Determine peer kind and ID
	peerKind := "room"
	peerID := roomID

	// Check if it's a direct message (simplified check)
	if strings.HasPrefix(roomID, "!") && len(strings.Split(roomID, ":")) == 2 {
		// Could be DM, check room members
		if c.isDirectMessage(roomID) {
			peerKind = "direct"
			peerID = senderID
		}
	}

	metadata := map[string]string{
		"platform":   "matrix",
		"room_id":    roomID,
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

	c.HandleMessage(senderID, roomID, content, mediaPaths, metadata)
}

// isDirectMessage checks if a room is a direct message.
func (c *MatrixChannel) isDirectMessage(roomID string) bool {
	// Try to get DM status
	dmEvent := make(map[string]interface{})
	err := c.client.GetAccountData(c.ctx, "m.direct", &dmEvent)
	if err != nil {
		return false
	}

	// Check if room is in DM list
	if dmList, ok := dmEvent["m.direct"].(map[string]interface{}); ok {
		for _, rooms := range dmList {
			if roomList, ok := rooms.([]interface{}); ok {
				for _, room := range roomList {
					if room == roomID {
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
func (c *MatrixChannel) downloadMedia(eventID id.EventID, body, mediaType string) string {
	// Build media URL using the event ID string directly
	// Event ID format: $<hash>:<homeserver>
	eventIDStr := eventID.String()
	var homeServer string
	if idx := strings.Index(eventIDStr, ":"); idx > 0 {
		homeServer = eventIDStr[idx+1:]
	} else {
		// Fallback to config homeserver
		homeServer = c.config.UserID[strings.Index(c.config.UserID, ":")+1:]
	}

	mediaURL := fmt.Sprintf("%s/_matrix/media/v3/download/%s/%s",
		c.client.HomeserverURL.String(),
		homeServer,
		eventIDStr)

	ext := "." + mediaType
	if mediaType == "image" {
		ext = ".jpg"
	} else if mediaType == "file" {
		ext = ""
	}

	filename := fmt.Sprintf("matrix_%s%s", eventIDStr, ext)
	return utils.DownloadFile(mediaURL, filename, utils.DownloadOptions{
		LoggerPrefix: "matrix",
		ExtraHeaders: map[string]string{
			"Authorization": "Bearer " + c.config.AccessToken,
		},
	})
}

// sendTyping sends a typing notification.
func (c *MatrixChannel) sendTyping(roomID string) {
	roomIDObj := id.RoomID(roomID)
	userIDObj := id.UserID(c.config.UserID)
	// Send typing event directly
	_, err := c.client.SendStateEvent(c.ctx, roomIDObj, event.Type{"m.typing", event.StateEventType}, userIDObj.String(), map[string]bool{
		"typing": true,
	})
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
