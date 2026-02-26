package channels

import (
	"context"
	"sync"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// MemoryStateStore implements the StateStore interface for E2EE
// It tracks which rooms are encrypted and their encryption settings
type MemoryStateStore struct {
	encryptedRooms map[id.RoomID]*event.EncryptionEventContent
	mu             sync.RWMutex
}

// NewMemoryStateStore creates a new in-memory state store
func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{
		encryptedRooms: make(map[id.RoomID]*event.EncryptionEventContent),
	}
}

// IsEncrypted returns whether a room is encrypted
func (s *MemoryStateStore) IsEncrypted(ctx context.Context, roomID id.RoomID) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, encrypted := s.encryptedRooms[roomID]
	return encrypted, nil
}

// GetEncryptionEvent returns the encryption event's content for an encrypted room
func (s *MemoryStateStore) GetEncryptionEvent(ctx context.Context, roomID id.RoomID) (*event.EncryptionEventContent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.encryptedRooms[roomID], nil
}

// FindSharedRooms returns the encrypted rooms that another user is also in
func (s *MemoryStateStore) FindSharedRooms(ctx context.Context, userID id.UserID) ([]id.RoomID, error) {
	// For now, return all encrypted rooms
	// A full implementation would track room memberships
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	rooms := make([]id.RoomID, 0, len(s.encryptedRooms))
	for roomID := range s.encryptedRooms {
		rooms = append(rooms, roomID)
	}
	return rooms, nil
}

// SetEncryptionEvent marks a room as encrypted with the given encryption event
func (s *MemoryStateStore) SetEncryptionEvent(ctx context.Context, roomID id.RoomID, encryptionEvent *event.EncryptionEventContent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.encryptedRooms[roomID] = encryptionEvent
	return nil
}

// ClearEncryptionEvent removes encryption tracking for a room
func (s *MemoryStateStore) ClearEncryptionEvent(ctx context.Context, roomID id.RoomID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.encryptedRooms, roomID)
	return nil
}
