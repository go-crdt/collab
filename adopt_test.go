//go:build !js

package collab_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// A superseded host keeps its work by bringing it, and needs no protocol to.
//
// Two tabs that both elected host hold two documents; the one that steps down
// with [ErrHostSuperseded] re-joins the survivor. Arriving empty-handed, it
// adopts the survivor's document and whatever it held is gone -- which is fine
// for an ordinary join, where somebody chose to enter a room that exists, and
// is not fine here, because this tab WAS the room and its buffer was the seed.
//
// [ClientConfig.Resume] already is the answer: it is documented as keeping the
// work done while disconnected, and a tab that has just been superseded is a
// tab that was disconnected. Nothing new travels; the union does the rest.
//
// This is the evidence for go-crdt/collab#152, where it was written up as the
// option that would cost the most. It costs one argument.
func TestASupersededHostKeepsItsWorkByBringingIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Two independent servers, as two tabs that both elected host have.
	survivor := collab.NewServer(collab.Config{Store: collab.NewMemoryStore()})
	t.Cleanup(func() { _ = survivor.Close(context.Background()) })
	loser := collab.NewServer(collab.Config{Store: collab.NewMemoryStore()})
	t.Cleanup(func() { _ = loser.Close(context.Background()) })

	join := func(srv *collab.Server, site crdt.SiteID, resume []byte) *collab.Client {
		t.Helper()
		tr, sc := collab.Pipe()
		go func() { _ = srv.ServePipe(ctx, sc) }()
		c, err := collab.Join(ctx, tr, collab.ClientConfig{Document: "paper", Site: site, Resume: resume})
		if err != nil {
			t.Fatalf("join: %v", err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	text := func(c *collab.Client) *collab.Text {
		t.Helper()
		d, err := c.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		return d
	}

	// Each tab seeded its own room and typed into it.
	sc := join(survivor, 1, nil)
	if err := text(sc).Insert(0, "SURVIVOR seeded this"); err != nil {
		t.Fatal(err)
	}
	lc := join(loser, 2, nil)
	if err := text(lc).Insert(0, "LOSER typed this"); err != nil {
		t.Fatal(err)
	}

	// The loser is superseded. What it holds today is exactly this.
	held := lc.Snapshot()
	t.Logf("the losing tab holds %q in %d bytes", text(lc).String(), len(held))
	_ = lc.Close()

	// It re-joins the survivor carrying that snapshot.
	rejoined := join(survivor, 2, held)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(text(sc).String(), "LOSER") && strings.Contains(text(rejoined).String(), "SURVIVOR") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("the survivor now reads %q", text(sc).String())
	t.Logf("the re-joined tab reads %q", text(rejoined).String())

	if !strings.Contains(text(sc).String(), "LOSER") {
		t.Error("the survivor never learned what the superseded tab held")
	}
	if !strings.Contains(text(rejoined).String(), "SURVIVOR") {
		t.Error("the re-joined tab never learned what the survivor held")
	}
	if text(sc).String() != text(rejoined).String() {
		t.Errorf("they disagree: %q vs %q", text(sc).String(), text(rejoined).String())
	}
}
