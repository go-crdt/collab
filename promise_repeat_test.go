//go:build !js

package collab

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// A promise that says what the last one said is not put on the wire.
//
// This is what closes the cycle go-crdt/collab#174 describes, and it is the
// second place it was tried.
//
// wakeLinks fires on every acknowledgement and a woken link sends its promise;
// that promise arrives at the peer as an acknowledgement, so acknowledge runs
// THERE and wakes that server's other links. wakeLinks skips the session that
// spoke, which breaks a two-cycle -- in a two-server mesh the only link IS the
// speaker -- and does not break a three-cycle, which is what the test that caught
// it once is.
//
// Waking only on news was built first and rejected on measurement: see the
// comment in document.acknowledge. Comparing versions there costs a map walk per
// participant per operation; comparing them here costs one bytes.Equal per link
// per promise, and the encodings are canonical so byte equality is exact.
//
// Driven by waking the link directly, with the document untouched between wakes,
// because that is the state the cycle actually presents: something woke us and
// nothing we promise has moved. Counting messages in a mesh cannot show this --
// the symptom the issue recorded was two extra messages out of 2212, and no
// aggregate over a dozen runs resolves 0.09%.
func TestALinkDoesNotRepeatAPromiseItAlreadySent(t *testing.T) {
	var mu sync.Mutex
	var acks []ackMsg
	peer := &brokenPeer{
		firstMsg: welcomeWith(welcomeMsg{}),
		sends: func(kind byte, msg any) error {
			if kind == kindAcknowledge {
				mu.Lock()
				acks = append(acks, msg.(ackMsg))
				mu.Unlock()
			}
			return nil
		},
	}
	s := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	go func() { _ = s.Follow(ctx, peer, "doc", 42) }()

	// The link, once it has registered itself: waiting for the document alone
	// lands inside the window between opening one and enrolling in it.
	var doc *document
	var link *subscriber
	until(t, "the link to register itself", func() bool {
		s.mu.Lock()
		doc = s.docs["doc"]
		s.mu.Unlock()
		if doc == nil {
			return false
		}
		doc.mu.Lock()
		defer doc.mu.Unlock()
		for sub := range doc.links {
			link = sub
			return true
		}
		return false
	})

	// Somebody else writes and acknowledges, so the link has something to
	// promise and sends it once.
	other, err := doc.enrol(joinMsg{Document: "doc", Site: 7}, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { doc.leave(context.WithoutCancel(ctx), other) })
	mine := crdt.NewComposite(7)
	cells, err := mine.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cells.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	raw, err := crdt.AppendPartOps(nil, mine.OpsSince(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.applyOperations(ctx, other, raw); err != nil {
		t.Fatal(err)
	}
	version, err := mine.Version().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	clocks, err := mine.Clocks().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.acknowledge(other, version, clocks); err != nil {
		t.Fatal(err)
	}
	until(t, "the link to promise once", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(acks) >= 1
	})

	// Now wake it repeatedly with nothing changed at all. Each wake recomputes
	// the same promise, and none of them is worth a message.
	for range 5 {
		doc.mu.Lock()
		select {
		case link.promiseMoved <- struct{}{}:
		default:
		}
		doc.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(acks) != 1 {
		t.Errorf("the link put %d acknowledgements on the wire, want 1: %d wakes with nothing moved", len(acks), 5)
	}
}
