package collab

import (
	"context"
	"sync"
)

// A Store keeps documents between sessions. It holds snapshots, which are
// self-contained: a document restored from one can still serve a participant
// that has been away, because the snapshot carries the whole history.
//
// Implementations must be safe for concurrent use.
type Store interface {
	// Load returns the snapshot for a document, or nil if there is none yet.
	// Returning nil is how a store says "new document", and is not an error.
	Load(ctx context.Context, document string) ([]byte, error)

	// Save records the current snapshot, replacing any previous one.
	Save(ctx context.Context, document string, snapshot []byte) error
}

// MemoryStore keeps documents in memory. It is the default, it is what the tests
// use, and it is enough for a single process that does not need to survive a
// restart. Anything else — Postgres, object storage — implements [Store].
type MemoryStore struct {
	mu   sync.Mutex
	docs map[string][]byte
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{docs: map[string][]byte{}}
}

// Load returns a copy of the stored snapshot, or nil if the document is new.
func (s *MemoryStore) Load(_ context.Context, document string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.docs[document]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), stored...), nil
}

// Save records a copy of the snapshot.
func (s *MemoryStore) Save(_ context.Context, document string, snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.docs[document] = append([]byte(nil), snapshot...)
	return nil
}

// Documents returns the names of the documents held, which is what a caller
// needs to inspect or migrate a store.
func (s *MemoryStore) Documents() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.docs))
	for name := range s.docs {
		out = append(out, name)
	}
	return out
}
