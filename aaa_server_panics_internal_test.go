package collab

import (
	"context"
	"errors"
	"testing"
)

// bareStore is a Store and nothing more: no SiteStore, no ConditionalStore.
// load and err are what Load answers.
type bareStore struct {
	load []byte
	err  error
}

func (p bareStore) Load(context.Context, string) ([]byte, error) { return p.load, p.err }
func (p bareStore) Save(context.Context, string, []byte) error   { return nil }

// server.go:316 and :322: a server given no store and no clock makes its own.
func TestAServerWithoutAStoreOrAClockMakesItsOwn(t *testing.T) {
	s := NewServer(Config{})
	defer func() { _ = s.Close(context.Background()) }()
	if s.store == nil {
		t.Error("NewServer left the store nil")
	}
	if s.now == nil {
		t.Error("NewServer left the clock nil")
	}
}

// server.go:458: a stored snapshot that does not decode is an internal failure
// of opening, and no document is handed back.
func TestOpeningAnUnreadableSnapshotFailsWithoutPanicking(t *testing.T) {
	s := NewServer(Config{Store: bareStore{load: []byte("not a snapshot")}})
	defer func() { _ = s.Close(context.Background()) }()
	noPanic(t, "open", func() {
		d, err := s.open(context.Background(), "d")
		var se *sessionError
		if !errors.As(err, &se) || se.kind != errInternal || d != nil {
			t.Errorf("open = %v, %v; want no document and an internal failure", d, err)
		}
	})
}

// server.go:1206: a store that keeps no participants is saved without asking it
// to.
func TestPersistingToAStoreThatKeepsNoParticipantsDoesNotAskIt(t *testing.T) {
	s := NewServer(Config{Store: bareStore{}})
	defer func() { _ = s.Close(context.Background()) }()
	d, err := s.open(context.Background(), "d")
	if err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	d.dirty = true
	d.mu.Unlock()
	noPanic(t, "persist", func() {
		if err := d.persist(context.Background()); err != nil {
			t.Errorf("persist = %v", err)
		}
	})
}
