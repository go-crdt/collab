package collab_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// watchedStore records every document name the server asks it about, so a test
// can say what a refused session caused the server to do rather than only what
// the session was told.
type watchedStore struct {
	inner collab.Store
	mu    sync.Mutex
	asked []string
}

func (w *watchedStore) Load(ctx context.Context, document string) ([]byte, error) {
	w.mu.Lock()
	w.asked = append(w.asked, document)
	w.mu.Unlock()
	return w.inner.Load(ctx, document)
}

func (w *watchedStore) Save(ctx context.Context, document string, snapshot []byte) error {
	w.mu.Lock()
	w.asked = append(w.asked, document)
	w.mu.Unlock()
	return w.inner.Save(ctx, document, snapshot)
}

func (w *watchedStore) names() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.asked...)
}

// [collab.Config.Authorize] documents a property that is not about the answer it
// gives: a refused session "neither reads the store nor reveals whether the
// document exists". That is what makes a refusal not an existence oracle — ask
// for a name and a server that opened it first would answer differently for one
// that exists and one that does not, whatever it then says.
//
// The property is an ORDER, and an order is invisible to a test that only checks
// the answer. The whole suite stayed green with the authorisation moved to after
// openAndJoin, which is how this test came to be written.
func TestARefusedJoinDoesNotTouchTheStore(t *testing.T) {
	store := &watchedStore{inner: collab.NewMemoryStore()}
	refused := errors.New("not your document")

	_, conn := serve(t, collab.Config{
		Store: store,
		Authorize: func(_ context.Context, document string, _ crdt.SiteID) error {
			if document == "theirs" {
				return refused
			}
			return nil
		},
	})

	// An allowed join first, as the positive control: it proves the server does
	// ask this store when it opens a document, so "theirs was never asked for"
	// below means the refusal stopped it rather than the store being unused.
	mine, err := collab.Join(t.Context(), collab.GRPC(conn),
		collab.ClientConfig{Document: "mine", Site: 1})
	if err != nil {
		t.Fatalf("joining a document this session may open: %v", err)
	}
	mine.Close()

	var sawMine bool
	for _, name := range store.names() {
		if name == "mine" {
			sawMine = true
		}
	}
	if !sawMine {
		t.Fatal("the allowed join never reached the store, so this test cannot tell " +
			"a refusal from a store nobody uses")
	}
	before := len(store.names())

	if _, err := collab.Join(t.Context(), collab.GRPC(conn),
		collab.ClientConfig{Document: "theirs", Site: 2}); err == nil {
		t.Fatal("a session the policy refuses was allowed to join")
	}

	for _, name := range store.names()[before:] {
		if name == "theirs" {
			t.Errorf("a refused join reached the store for %q.\n"+
				"Config.Authorize says a refused session neither reads the store nor "+
				"reveals whether the document exists; it is asked BEFORE the document "+
				"is opened, and this says it was not.", name)
		}
	}
}
