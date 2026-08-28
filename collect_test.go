//go:build !js

package collab_test

import (
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// A document being edited in a room gives back what everybody in it has
// certainly seen, while they are still in it.
//
// The map parts, which are what is left to give back: a text and a list had a
// collection too and it was withdrawn, because it left two replicas holding
// different documents. See the note in the crdt package.
//
// This is what the acknowledgements are for. A server can know a version every
// replica has delivered, and that is the one thing collecting asks for and a
// replica on its own cannot supply.
func TestADocumentInUseGivesBackWhatEverybodyHasSeen(t *testing.T) {
	store := collab.NewMemoryStore()
	srv, conn := serve(t, collab.Config{
		Store:        store,
		PersistEvery: 2 * time.Millisecond,
		CollectEvery: 2 * time.Millisecond,
	})
	ada := join(t, conn, collab.ClientConfig{Document: "paper", Site: 1})
	grace := join(t, conn, collab.ClientConfig{Document: "paper", Site: 2})

	cells := func(t *testing.T, c *collab.Client) *collab.Map {
		t.Helper()
		m, err := c.Map("cells")
		if err != nil {
			t.Fatal(err)
		}
		return m
	}

	// Ada writes a pool of keys and Grace takes half of them away, so there are
	// tombstones for the server to give back.
	const keys = 40
	for i := 0; i < keys; i++ {
		if err := cells(t, ada).Set(key(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	await(t, grace, "all the keys", func() bool { return len(cells(t, grace).Keys()) == keys })
	for i := 0; i < keys; i += 2 {
		if err := cells(t, grace).Delete(key(i)); err != nil {
			t.Fatal(err)
		}
	}
	want := keys / 2

	// What was deleted, and so what the stored document would still be carrying
	// if nothing were collected.
	tombstones := func() int {
		t.Helper()
		if err := srv.Flush(t.Context()); err != nil {
			t.Fatal(err)
		}
		raw, err := store.Load(t.Context(), "paper")
		if err != nil {
			t.Fatal(err)
		}
		doc, err := crdt.LoadComposite(99, raw)
		if err != nil {
			t.Fatalf("what was stored is unreadable: %v", err)
		}
		m, err := doc.Map("cells")
		if err != nil {
			t.Fatal(err)
		}
		return m.Tombstones()
	}

	deadline := time.Now().Add(60 * time.Second)
	var left int
	for {
		left = tombstones()
		if left < want && len(cells(t, ada).Keys()) == want {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the document never gave anything back: %d tombstones for %d keys deleted",
				left, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Logf("%d keys written, %d deleted, %d tombstones left", keys, want, left)

	// And it still says what it said, to both of them.
	await(t, grace, "what is left", func() bool { return len(cells(t, grace).Keys()) == want })
	if got := len(cells(t, ada).Keys()); got != want {
		t.Fatalf("ada holds %d keys, want %d", got, want)
	}

	// Both carry on afterwards, which is the difference between collecting and
	// rewriting.
	if err := cells(t, ada).Set("after", []byte("x")); err != nil {
		t.Fatalf("editing after a collection: %v", err)
	}
	await(t, grace, "the new key", func() bool {
		_, ok := cells(t, grace).Get("after")
		return ok
	})
}

func key(i int) string { return "cell-" + string(rune('a'+i%26)) + string(rune('a'+i/26)) }
