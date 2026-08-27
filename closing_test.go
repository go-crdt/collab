//go:build !js

package collab_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// An edit made and then closed on must survive, even while the same client is
// sending something else.
//
// A participant acknowledges what arrives, from the goroutine that receives it,
// so a client that writes once and closes has two goroutines wanting the stream
// at the moment it is wound down. Writing to a carrier whose sending side has
// been closed does not fail that one message: it takes the stream down, and
// with it whatever the server had not read — which is the edit. Forty sessions
// doing the most ordinary thing there is, writing a character and closing the
// tab, lost work every time before this.
func TestAnEditSurvivesClosingWhileSomethingElseIsSending(t *testing.T) {
	store := collab.NewMemoryStore()
	srv, conn := serve(t, collab.Config{Store: store})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	var wg sync.WaitGroup
	var mu sync.Mutex
	wrote := 0
	const sessions = 40
	for i := range sessions {
		wg.Add(1)
		go func(site int) {
			defer wg.Done()
			c, err := collab.Join(t.Context(), collab.GRPC(conn),
				collab.ClientConfig{Document: "raced", Site: crdt.SiteID(site + 1)})
			if err != nil {
				return // a refused join is not the failure under test
			}
			defer c.Close()
			text, err := c.Text("body")
			if err != nil {
				return
			}
			if err := text.Insert(0, "x"); err != nil {
				return
			}
			mu.Lock()
			wrote++
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	// The sessions are closing as this runs; give the server the moment it needs
	// to read what they sent before it is asked what it has.
	time.Sleep(200 * time.Millisecond)

	if err := srv.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	kept, err := store.Load(t.Context(), "raced")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := crdt.LoadComposite(99, kept)
	if err != nil {
		t.Fatalf("what was stored is unreadable: %v", err)
	}
	body, err := doc.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if got := body.Len(); got != wrote {
		t.Fatalf("%d characters were written and %d survived", wrote, got)
	}
	if wrote == 0 {
		t.Fatal("no session got far enough to write, so nothing was tested")
	}
	t.Logf("all %d characters survived %d sessions closing on them", wrote, sessions)
}

// And an edit that meets the close is refused rather than silently dropped: the
// caller is told, instead of being left believing work went out that did not.
func TestAnEditAfterTheStreamIsClosedIsRefused(t *testing.T) {
	srv, conn := serve(t, collab.Config{})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	c := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	text, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if err := text.Insert(0, "before"); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := text.Insert(0, "after"); err == nil {
		t.Fatal("an edit made after closing reported success")
	}
}
