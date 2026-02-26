package channels

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"maunium.net/go/mautrix"
	mautrixcrypto "maunium.net/go/mautrix/crypto"
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
	config     config.MatrixConfig
	client     *mautrix.Client
	syncer     *mautrix.DefaultSyncer
	crypto     *mautrixcrypto.OlmMachine
	stateStore *MemoryStateStore
	ctx        context.Context
	cancel     context.CancelFunc
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
	syncer.ParseEventContent = true
	// Set up filter to receive room messages and toDevice events (needed for E2EE keys)
	syncer.FilterJSON = &mautrix.Filter{
		Room: &mautrix.RoomFilter{
			Timeline: &mautrix.FilterPart{
				Limit: 20,
			},
		},
		// Include to_device events for E2EE key sharing
		AccountData: &mautrix.FilterPart{
			Limit: 10,
		},
	}
	client.Syncer = syncer

	// Set up crypto for E2EE with in-memory stores
	cryptoStore := mautrixcrypto.NewMemoryStore(nil)
	stateStore := NewMemoryStateStore()
	crypto := mautrixcrypto.NewOlmMachine(client, nil, cryptoStore, stateStore)
	// Note: Don't assign to client.Crypto as OlmMachine doesn't implement CryptoHelper
	// We use the OlmMachine directly for encryption/decryption
	
	// Load crypto store
	loadErr := crypto.Load(context.Background())
	if loadErr != nil {
		logger.WarnCF("matrix", "Failed to load crypto store (continuing without persistence)", map[string]any{
			"error": loadErr.Error(),
		})
	} else {
		logger.InfoC("matrix", "Matrix crypto store loaded")
	}

	return &MatrixChannel{
		BaseChannel: base,
		config:      cfg,
		client:      client,
		syncer:      syncer,
		crypto:      crypto,
		stateStore:  stateStore,
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

	// Register sync handler to process room events
	c.syncer.OnSync(c.processSync)

	logger.InfoC("matrix", "Matrix event handlers registered")
	logger.InfoCF("matrix", "Syncer ParseEventContent enabled", map[string]any{
		"parse_content": c.syncer.ParseEventContent,
	})

	// Start sync loop in background
	go func() {
		logger.InfoC("matrix", "Starting Matrix sync loop")
		for {
			select {
			case <-c.ctx.Done():
				logger.InfoC("matrix", "Matrix sync loop stopped")
				return
			default:
			}

			err := c.client.Sync()
			if err != nil {
				if c.ctx.Err() == nil {
					logger.ErrorCF("matrix", "Sync error, retrying in 5s", map[string]any{
						"error": err.Error(),
					})
					time.Sleep(5 * time.Second)
				} else {
					return
				}
			}
		}
	}()

	c.setRunning(true)
	logger.InfoC("matrix", "Matrix channel started")
	return nil
}

// processSync handles sync responses and processes room events.
func (c *MatrixChannel) processSync(ctx context.Context, resp *mautrix.RespSync, since string) bool {
	logger.InfoCF("matrix", "Sync response received", map[string]any{
		"rooms_joined":  len(resp.Rooms.Join),
		"rooms_invited": len(resp.Rooms.Invite),
		"next_batch":    resp.NextBatch,
		"since":         since,
	})

	// Let crypto process the sync response first (handles key sharing, etc.)
	if c.crypto != nil {
		c.crypto.ProcessSyncResponse(ctx, resp, since)
	}
	
	// Process toDevice events for E2EE key sharing
	if c.crypto != nil && len(resp.ToDevice.Events) > 0 {
		logger.DebugCF("matrix", "Processing toDevice events", map[string]any{
			"event_count": len(resp.ToDevice.Events),
		})
		for _, evt := range resp.ToDevice.Events {
			c.crypto.HandleToDeviceEvent(ctx, evt)
		}
	}
	
	// Handle device list changes for E2EE
	if c.crypto != nil {
		deviceLists := resp.DeviceLists
		if len(deviceLists.Changed) > 0 || len(deviceLists.Left) > 0 {
			logger.DebugCF("matrix", "Processing device lists", map[string]any{
				"changed": len(deviceLists.Changed),
				"left":    len(deviceLists.Left),
			})
			c.crypto.HandleDeviceLists(ctx, &deviceLists, since)
		}
	}

	// Process joined rooms
	for roomID, room := range resp.Rooms.Join {
		timelineCount := len(room.Timeline.Events)
		stateCount := len(room.State.Events)
		logger.InfoCF("matrix", "Processing joined room", map[string]any{
			"room_id":        roomID,
			"timeline_count": timelineCount,
			"state_count":    stateCount,
			"limited":        room.Timeline.Limited,
		})

		// Check for encryption state events to enable outbound encryption
		if c.crypto != nil {
			for _, evt := range room.State.Events {
				if evt.Type == event.StateEncryption {
					logger.InfoCF("matrix", "Processing encryption state event", map[string]any{
						"room_id":   roomID,
						"algorithm": evt.Content.Raw["algorithm"],
					})
				}
				// Handle member events for key sharing
				if evt.Type == event.StateMember {
					c.crypto.HandleMemberEvent(ctx, evt)
				}
			}
		}

		if timelineCount == 0 {
			continue
		}
		for _, evt := range room.Timeline.Events {
			// Set the room ID on the event since mautrix doesn't populate it from sync
			evt.RoomID = roomID
			
			logger.InfoCF("matrix", "Processing event", map[string]any{
				"type":     evt.Type,
				"sender":   evt.Sender.String(),
				"event_id": evt.ID.String(),
				"room_id":  evt.RoomID,
			})

			// Handle encryption state events to enable outbound encryption
			if evt.Type == event.StateEncryption && c.stateStore != nil {
				algorithm, _ := evt.Content.Raw["algorithm"].(string)
				logger.InfoCF("matrix", "Processing encryption state event", map[string]any{
					"room_id":   roomID,
					"algorithm": algorithm,
				})
				// Mark room as encrypted in state store
				encryptionContent := &event.EncryptionEventContent{
					Algorithm: id.Algorithm(algorithm),
				}
				c.stateStore.SetEncryptionEvent(ctx, roomID, encryptionContent)
			}

			// Handle encrypted events
			if evt.Type == event.EventEncrypted && c.crypto != nil {
				// Manually parse encrypted content if not already parsed
				if evt.Content.Parsed == nil {
					var encryptedContent event.EncryptedEventContent
					err := json.Unmarshal(evt.Content.VeryRaw, &encryptedContent)
					if err != nil {
						logger.DebugCF("matrix", "Failed to parse encrypted content", map[string]any{
							"event_id": evt.ID.String(),
							"error":    err.Error(),
						})
						continue
					}
					evt.Content.Parsed = &encryptedContent
				}
				// Decrypt the event
				decrypted, err := c.crypto.DecryptMegolmEvent(ctx, evt)
				if err != nil {
					logger.WarnCF("matrix", "Failed to decrypt event (may need key share)", map[string]any{
						"error":    err.Error(),
						"event_id": evt.ID.String(),
					})
					// Request room key if decryption fails
					c.crypto.HandleEncryptedEvent(ctx, evt)
					continue
				}
				logger.InfoCF("matrix", "Decrypted event", map[string]any{
					"type":     decrypted.Type,
					"event_id": evt.ID.String(),
				})
				if decrypted.Type == event.EventMessage {
					c.onMessage(ctx, decrypted)
				}
				continue
			}
			
			if evt.Type == event.EventMessage {
				c.onMessage(ctx, evt)
			} else if evt.Type == event.StateMember {
				c.onMemberEvent(ctx, evt)
			}
		}
	}

	// Process invited rooms (auto-join)
	for roomID, invite := range resp.Rooms.Invite {
		logger.InfoCF("matrix", "Processing room invite", map[string]any{
			"room_id": roomID,
			"event_count": len(invite.State.Events),
		})
		
		// Check if any invite event is for the bot
		for _, evt := range invite.State.Events {
			if evt.Type != event.StateMember {
				continue
			}
			
			// Check if this is an invite to the bot
			membership, ok := evt.Content.Raw["membership"].(string)
			if !ok || membership != "invite" {
				continue
			}
			
			// Check if the state key is the bot's user ID
			if evt.StateKey == nil || *evt.StateKey != c.config.UserID {
				continue
			}
			
			// Check allowlist
			if !c.IsAllowed(evt.Sender.String()) {
				logger.DebugCF("matrix", "Rejecting room invite from non-allowed user", map[string]any{
					"room_id": roomID,
					"inviter": evt.Sender,
				})
				continue
			}
			
			// Auto-join the room
			_, err := c.client.JoinRoomByID(ctx, roomID)
			if err != nil {
				logger.ErrorCF("matrix", "Failed to join room", map[string]any{
					"room_id": roomID,
					"error":   err.Error(),
				})
			} else {
				logger.InfoCF("matrix", "Auto-joined room", map[string]any{
					"room_id": roomID,
					"inviter": evt.Sender,
				})
				// Wait a moment for the join to propagate
				time.Sleep(2 * time.Second)
			}
		}
	}

	return true
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
	logger.InfoCF("matrix", "Received message event", map[string]any{
		"sender":   evt.Sender,
		"room_id":  evt.RoomID,
		"room_id_str": evt.RoomID.String(),
		"event_id": evt.ID,
	})

	// Skip own messages
	if evt.Sender.String() == c.config.UserID {
		logger.DebugC("matrix", "Skipping own message")
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
	
	logger.InfoCF("matrix", "Processing message before HandleMessage", map[string]any{
		"sender_id": senderID,
		"room_id":   roomID,
		"room_id_empty": roomID == "",
	})

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

	// Create message content
	content := &event.MessageEventContent{
		MsgType: event.MsgText,
		Body:    msg.Content,
	}

	// Try to encrypt if crypto is available
	if c.crypto != nil {
		// Check if room is encrypted using state store
		if c.stateStore != nil {
			isEncrypted, err := c.stateStore.IsEncrypted(ctx, roomID)
			if err != nil {
				logger.DebugCF("matrix", "Could not check encryption status", map[string]any{
					"error": err.Error(),
				})
			} else {
				logger.DebugCF("matrix", "Room encryption status", map[string]any{
					"room_id":      roomID,
					"is_encrypted": isEncrypted,
				})
			}
		}
		
		encrypted, encryptErr := c.crypto.EncryptMegolmEvent(ctx, roomID, event.EventMessage, content)
		if encryptErr != nil {
			logger.WarnCF("matrix", "Sending unencrypted (encryption setup required)", map[string]any{
				"error":   encryptErr.Error(),
				"room_id": roomID,
				"hint":    "Bot needs to create outbound session first",
			})
			// Fall through to send unencrypted
		} else {
			_, sendErr := c.client.SendMessageEvent(ctx, roomID, event.EventEncrypted, encrypted)
			if sendErr != nil {
				return fmt.Errorf("failed to send encrypted matrix message: %w", sendErr)
			}
			logger.InfoCF("matrix", "Encrypted message sent", map[string]any{
				"room_id": roomID,
			})
			return nil
		}
	}

	// Send unencrypted
	_, err := c.client.SendMessageEvent(ctx, roomID, event.EventMessage, content)
	if err != nil {
		return fmt.Errorf("failed to send matrix message: %w", err)
	}

	logger.DebugCF("matrix", "Unencrypted message sent", map[string]any{
		"room_id": roomID,
	})

	return nil
}
