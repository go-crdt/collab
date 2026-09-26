//go:build !js

package collab

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/go-crdt/crdt"
)

// condStore is a [ConditionalStore] that records what it was asked and can be told
// to answer [ErrChanged], which is what a store does when somebody else has written.
type condStore struct {
	mu sync.Mutex
	*MemoryStore
	loadToken Token   // handed back by LoadToken
	expects   []Token // every expect SaveIf was given, in order
	next      Token   // what SaveIf returns on success
	refuse    bool
}

func (c *condStore) LoadToken(ctx context.Context, document string) ([]byte, Token, error) {
	snapshot, err := c.MemoryStore.Load(ctx, document)
	c.mu.Lock()
	defer c.mu.Unlock()
	return snapshot, c.loadToken, err
}

func (c *condStore) SaveIf(ctx context.Context, document string, snapshot []byte, expect Token) (Token, error) {
	c.mu.Lock()
	c.expects = append(c.expects, expect)
	refuse, next := c.refuse, c.next
	c.mu.Unlock()
	if refuse {
		return nil, ErrChanged
	}
	if err := c.MemoryStore.Save(ctx, document, snapshot); err != nil {
		return nil, err
	}
	return next, nil
}

func (c *condStore) asked() []Token {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Token(nil), c.expects...)
}

// A server saves conditionally where its store can, and the condition is what the
// server last read or wrote.
//
// This is what turns TestTwoServersOverOneStoreLoseTheEarlierSave from a silent loss
// into something an operator is told about: the store says ErrChanged, the document
// goes back to dirty so the work is kept, and the error travels to whoever asked for
// the save. See [ConditionalStore].
func TestAServerSavesAgainstWhatItLastRead(t *testing.T) {
	ctx := context.Background()
	store := &condStore{MemoryStore: NewMemoryStore(), loadToken: Token("v1"), next: Token("v2")}
	srv := NewServer(Config{Store: store})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	// ONE writer across the three writes, sending only what it has added each
	// time. A fresh composite per write would restart the site's sequence numbers
	// and its second write would collide with its first -- which crdt.ErrCollidingID
	// refuses, and did when this test was first written.
	mine := crdt.NewComposite(7)
	text, err := mine.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	var sent crdt.CompositeVersion
	write := func(what string) {
		t.Helper()
		doc, err := srv.open(ctx, "paper")
		if err != nil {
			t.Fatal(err)
		}
		sub, err := doc.enrol(joinMsg{Document: "paper", Site: 7}, false)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { doc.leave(context.WithoutCancel(ctx), sub) })
		if _, err := text.Insert(text.Len(), what); err != nil {
			t.Fatal(err)
		}
		raw, err := crdt.AppendPartOps(nil, mine.OpsSince(sent))
		if err != nil {
			t.Fatal(err)
		}
		sent = mine.Version()
		if err := doc.applyOperations(ctx, sub, raw); err != nil {
			t.Fatal(err)
		}
	}

	write("first")
	if err := srv.Flush(ctx); err != nil {
		t.Fatalf("the first conditional save failed: %v", err)
	}
	// The token the LOAD produced is what the first save was made against. Without
	// that the condition would be meaningless: it would say "whatever is there now".
	if asked := store.asked(); len(asked) != 1 || string(asked[0]) != "v1" {
		t.Fatalf("the first save expected %q, want the token LoadToken gave (v1)", asked)
	}

	// And what the save returned is what the next one is made against, or a server
	// could only ever save once.
	write("second")
	if err := srv.Flush(ctx); err != nil {
		t.Fatalf("the second conditional save failed: %v", err)
	}
	if asked := store.asked(); len(asked) != 2 || string(asked[1]) != "v2" {
		t.Fatalf("the second save expected %q, want the token the first save returned (v2)", asked)
	}

	// Now somebody else writes: the store refuses, the error reaches the caller, and
	// the work is still here to be saved again.
	store.mu.Lock()
	store.refuse = true
	store.mu.Unlock()
	write("third")
	err = srv.Flush(ctx)
	if !errors.Is(err, ErrChanged) {
		t.Fatalf("a refused save gave %v, want ErrChanged", err)
	}
	store.mu.Lock()
	store.refuse = false
	store.mu.Unlock()
	if err := srv.Flush(ctx); err != nil {
		t.Fatalf("the retry after a refusal failed: %v", err)
	}
	// The retry saved something, which is how we know the refusal kept the work
	// rather than dropping it: a document left clean would have had nothing to write.
	if asked := store.asked(); len(asked) != 4 {
		t.Errorf("the store was asked %d times, want 4: load, two saves, a refusal and a retry", len(asked))
	}
}

// A store that cannot refuse is used exactly as before.
func TestAPlainStoreIsStillSavedBlindly(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	doc, err := srv.open(ctx, "paper")
	if err != nil {
		t.Fatal(err)
	}
	if doc.cond != nil {
		t.Fatal("a MemoryStore was taken for a ConditionalStore")
	}
	if doc.token != nil {
		t.Fatalf("a plain store produced a token %q", doc.token)
	}
	sub, err := doc.enrol(joinMsg{Document: "paper", Site: 7}, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { doc.leave(context.WithoutCancel(ctx), sub) })
	mine := crdt.NewComposite(7)
	text, err := mine.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := text.Insert(0, "plain"); err != nil {
		t.Fatal(err)
	}
	raw, err := crdt.AppendPartOps(nil, mine.OpsSince(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.applyOperations(ctx, sub, raw); err != nil {
		t.Fatal(err)
	}
	if err := srv.Flush(ctx); err != nil {
		t.Fatalf("a blind save failed: %v", err)
	}
}
