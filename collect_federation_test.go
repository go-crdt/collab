//go:build !js

package collab

import (
	"context"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// A server in a federation collects, because a link says what it holds.
//
// A link is a participant of the document it follows: it joins that server, and
// [Server.Follow] joins its own as well. A participant that never says anything
// holds a document's floor at nothing for ever, so before the link acknowledged,
// Config.CollectEvery did nothing at all in a federation — silently, which is
// the worst way for a setting to do nothing.
//
// What a link promises is what its own server has applied, not what everybody
// behind it has. That is enough, and the next test is why.
func TestAFederatedServerCollectsOnceTheLinkSaysWhatItHolds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lyon := NewServer(Config{Store: NewMemoryStore(), CollectEvery: time.Millisecond})
	defer func() { _ = lyon.Close(context.Background()) }()
	paris := NewServer(Config{Store: NewMemoryStore(), CollectEvery: time.Millisecond})
	defer func() { _ = paris.Close(context.Background()) }()

	tr, sc := Pipe()
	go func() { _ = paris.ServePipe(ctx, sc) }()
	author, err := Join(ctx, tr, ClientConfig{Document: "paper", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = author.Close() }()
	cells, err := author.Map("cells")
	if err != nil {
		t.Fatal(err)
	}

	document := func(s *Server) *document {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.docs["paper"]
	}
	tombstones := func(s *Server) int {
		d := document(s)
		if d == nil {
			return -1
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		m, err := d.doc.Map("cells")
		if err != nil {
			return -1
		}
		return m.Tombstones()
	}

	link := &directDial{srv: paris, ctx: ctx}
	go func() { _ = lyon.Follow(ctx, link, "paper", crdt.SiteID(9001)) }()
	until(t, "the link to reach Paris", func() bool {
		d := document(paris)
		if d == nil {
			return false
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		return len(d.subs) == 2
	})

	if err := cells.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	// The tombstone has to be there before its going away means anything: an
	// empty document has none either, and a test that cannot tell those apart
	// passes whatever the code does. This one did, until it was checked against
	// the change reverted.
	until(t, "Paris to hold the write", func() bool { return tombstones(paris) == 0 && len(cells.Keys()) == 1 })
	if err := cells.Delete("k"); err != nil {
		t.Fatal(err)
	}
	until(t, "Paris to hold the deletion", func() bool { return tombstones(paris) == 1 })
	// And then to give it back, with a link in the room, on its own timer.
	until(t, "Paris to give the tombstone back", func() bool { return tombstones(paris) == 0 })
	// Giving it back is the proof that the floor moved: nothing is collected
	// without one, and before the link acknowledged, nothing here ever was.
}

// And a participant behind the link is not collected past, which is what makes
// the promise above enough.
//
// A link says what its own server has applied. The people behind it are
// participants of that server's document, and that document's own floor is what
// protects them: the server they ask still holds what it told the peer it had.
func TestAParticipantBehindALinkIsNotCollectedPast(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lyon := NewServer(Config{Store: NewMemoryStore(), CollectEvery: time.Millisecond})
	defer func() { _ = lyon.Close(context.Background()) }()
	paris := NewServer(Config{Store: NewMemoryStore(), CollectEvery: time.Millisecond})
	defer func() { _ = paris.Close(context.Background()) }()

	tr, sc := Pipe()
	go func() { _ = paris.ServePipe(ctx, sc) }()
	author, err := Join(ctx, tr, ClientConfig{Document: "paper", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = author.Close() }()
	cells, err := author.Map("cells")
	if err != nil {
		t.Fatal(err)
	}

	link := &directDial{srv: paris, ctx: ctx}
	go func() { _ = lyon.Follow(ctx, link, "paper", crdt.SiteID(9001)) }()

	// A reader on Lyon, which takes the value and then goes away holding it.
	away := &gatedLink{srv: lyon, ctx: ctx}
	reader, err := JoinWithRetry(ctx, away.dial, ClientConfig{Document: "paper", Site: 2},
		RetryPolicy{Wait: time.Millisecond, Ceiling: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	theirs, err := reader.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	if err := cells.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	until(t, "the reader on Lyon to see it", func() bool {
		v, held := theirs.Get("k")
		return held && string(v) == "v"
	})
	away.away()

	// Paris deletes it and gives the tombstone back, which it may: the link has
	// told it that Lyon holds the deletion.
	if err := cells.Delete("k"); err != nil {
		t.Fatal(err)
	}
	document := func(s *Server) *document {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.docs["paper"]
	}
	tombstones := func(s *Server) int {
		d := document(s)
		if d == nil {
			return -1
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		m, err := d.doc.Map("cells")
		if err != nil {
			return -1
		}
		return m.Tombstones()
	}
	until(t, "Paris to give the tombstone back", func() bool { return tombstones(paris) == 0 })

	// Lyon must not: the reader is one of its participants and has said nothing
	// since it went away. Ten passes of its collector to be sure.
	until(t, "Lyon to hold the deletion", func() bool { return tombstones(lyon) == 1 })
	time.Sleep(10 * time.Millisecond)
	if got := tombstones(lyon); got != 1 {
		t.Fatalf("Lyon gave back the tombstone its own reader has not delivered: %d left", got)
	}

	// And the reader comes back to a key that is gone, which is the whole point.
	away.back()
	until(t, "the reader to agree that the key is gone", func() bool {
		_, held := theirs.Get("k")
		return !held
	})
}

// directDial hands out sessions on a server in this process.
type directDial struct {
	srv *Server
	ctx context.Context
}

func (d *directDial) open(ctx context.Context) (carrierConn, error) {
	transport, server := Pipe()
	go func() { _ = d.srv.ServePipe(d.ctx, server) }()
	return transport.open(ctx)
}
