//go:build (js && wasm) || !js

package collab

import (
	"context"
	"sort"
	"sync"
	"time"
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
	// written is when each document was last saved, which is what [Idle]
	// answers from. now is time.Now, replaced in tests so that a document can
	// be made old without waiting for it to become old.
	written map[string]time.Time
	now     func() time.Time
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		docs:    map[string][]byte{},
		written: map[string]time.Time{},
		now:     time.Now,
	}
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
	s.written[document] = s.now()
	return nil
}

// Idle returns the documents this store has not been asked to write for longer
// than d, which is a [MemoryStore]'s half of [Archivable].
func (s *MemoryStore) Idle(_ context.Context, d time.Duration) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := s.now().Add(-d)
	var out []string
	for name, at := range s.written {
		if at.Before(cutoff) {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out, nil
}

// Release forgets a document if this store still holds exactly want.
//
// The comparison and the removal are one step under the same lock, so a save
// that lands while a document is being archived cannot be deleted by the
// release that follows it.
func (s *MemoryStore) Release(_ context.Context, document string, want []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.docs[document]
	if !ok {
		return nil
	}
	if err := stillHolds(stored, want); err != nil {
		return err
	}
	delete(s.docs, document)
	delete(s.written, document)
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
