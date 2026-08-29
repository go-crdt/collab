//go:build !js

package collab

import (
	"context"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// A server that is in a federation does not collect, and this is that pinned
// down rather than left to be discovered.
//
// [document.collectable] is the meet over every site that has been in the
// document, and a site that has acknowledged nothing takes it to nothing. A
// follow link is a site: it joins the server it follows, and it joins its own
// server too — see [Server.Follow]. Neither of those sessions is a [Client],
// and only a Client sends kindAcknowledge. So both servers have a participant
// that will never say what it holds, and neither of them can ever collect.
//
// That is the safe direction and not a regression: the meet has always refused
// an answer while any participant was silent. It is worth a test because
// Config.CollectEvery otherwise does nothing at all in a federated deployment
// and says nothing about it.
func TestAFederatedServerDoesNotCollect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lyon := NewServer(Config{Store: NewMemoryStore(), CollectEvery: time.Millisecond})
	defer func() { _ = lyon.Close(context.Background()) }()
	paris := NewServer(Config{Store: NewMemoryStore(), CollectEvery: time.Millisecond})
	defer func() { _ = paris.Close(context.Background()) }()

	// Somebody in Paris, writing and deleting, and acknowledging every word of
	// it: on its own that is a room whose meet advances.
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
	if err := cells.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
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
	// Paris on its own collects, and the timer does it without being asked.
	// This is the control: without it, everything below would pass on a server
	// that was not collecting for some other reason.
	until(t, "Paris to give the tombstone back", func() bool { return tombstones(paris) == 0 })

	// Now Lyon follows Paris. Neither of them can collect any more.
	link := &directDial{srv: paris, ctx: ctx}
	// Follow runs the link and does not return while it is up, so it goes in a
	// goroutine the way a deployment would run it.
	go func() { _ = lyon.Follow(ctx, link, "paper", crdt.SiteID(9001)) }()
	until(t, "the link to reach Paris", func() bool {
		d := document(paris)
		d.mu.Lock()
		defer d.mu.Unlock()
		return len(d.subs) == 2
	})

	document(paris).mu.Lock()
	_, parisCan := document(paris).collectable()
	document(paris).mu.Unlock()
	if parisCan {
		t.Fatal("Paris reported a version to collect against while a link that never acknowledges is in the room")
	}
	document(lyon).mu.Lock()
	_, lyonCan := document(lyon).collectable()
	document(lyon).mu.Unlock()
	if lyonCan {
		t.Fatal("Lyon reported one, and its own end of the link has acknowledged nothing either")
	}

	// And in practice: another deletion, and the tombstone stays.
	if err := cells.Set("m", []byte("w")); err != nil {
		t.Fatal(err)
	}
	if err := cells.Delete("m"); err != nil {
		t.Fatal(err)
	}
	until(t, "Paris to hold the second tombstone", func() bool { return tombstones(paris) > 0 })
	held := tombstones(paris)
	time.Sleep(50 * time.Millisecond) // fifty passes of the collector
	if got := tombstones(paris); got < held {
		t.Fatalf("Paris went from %d tombstones to %d with a silent link in the room", held, got)
	}

	// And the deletion still reaches Lyon, which is the part that matters: not
	// collecting costs space and never correctness.
	tr2, sc2 := Pipe()
	go func() { _ = lyon.ServePipe(ctx, sc2) }()
	reader, err := Join(ctx, tr2, ClientConfig{Document: "paper", Site: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	theirs, err := reader.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	until(t, "Lyon's reader to agree that the key is gone", func() bool {
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
