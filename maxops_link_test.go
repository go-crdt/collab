//go:build !js

package collab

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// A link is bounded like anybody else, and this is the path where that matters.
//
// [Config.MaxOperations] was written for a participant sending one enormous
// message. The reason to prove it on a LINK is that a link is the one session
// whose other end may be run by somebody else: the doctrine in this package's
// documentation says a federating server bounds what it is sent, and until this
// test that was read off the code -- follow hands what it receives to the same
// applyOperations a participant's batch goes through -- rather than witnessed.
// Reading a call chain is how a capability comes to exist and be unreachable,
// which happened here once already: crdt.ErrCollidingID shipped in v0.48.0
// unreachable from this package because ApplyAbsorbed dropped it.
//
// Both cases run the same two servers over the same carrier and differ only in
// the bound. The second is not decoration: a test that only asserted the
// follower holds nothing would pass just as well if the link had never worked at
// all, which is the way three earlier federation witnesses passed for the wrong
// reason.
func TestALinkIsBoundedLikeAnybodyElse(t *testing.T) {
	// enough operations that a bound can sit either side of them, and few enough
	// that the peer builds the document quickly.
	const ops = 40
	body := strings.Repeat("z", ops)

	run := func(t *testing.T, max int) (followErr error, held string, heard []error) {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var mu sync.Mutex
		mine := NewServer(Config{
			Store:         NewMemoryStore(),
			MaxOperations: max,
			OnOperationsRefused: func(_ string, _ crdt.SiteID, err error) {
				mu.Lock()
				heard = append(heard, err)
				mu.Unlock()
			},
		})
		defer func() { _ = mine.Close(context.Background()) }()
		theirs := NewServer(Config{Store: NewMemoryStore()})
		defer func() { _ = theirs.Close(context.Background()) }()

		// The peer's document, written by a participant of THEIRS so that what
		// crosses the link is a welcome carrying the whole thing in one message
		// -- which is the shape the bound is about.
		tr, sc := Pipe()
		go func() { _ = theirs.ServePipe(ctx, sc) }()
		c, err := Join(ctx, tr, ClientConfig{Document: "paper", Site: 7})
		if err != nil {
			t.Fatalf("join their server: %v", err)
		}
		defer func() { _ = c.Close() }()
		txt, err := c.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		// One insert per character, so the message carries `ops` operations
		// rather than one: the bound counts operations, not calls.
		for i, r := range body {
			if err := txt.Insert(i, string(r)); err != nil {
				t.Fatalf("insert %d: %v", i, err)
			}
		}
		awaitOn(t, theirs, body)

		// Follow returns the session's error, so the refusal needs no polling.
		done := make(chan error, 1)
		go func() {
			done <- mine.Follow(ctx, &directDial{srv: theirs, ctx: ctx}, "paper", crdt.SiteID(9001))
		}()

		// Whichever comes first, and the harness is not told which to expect: a
		// refused link RETURNS, an accepted one never does and instead leaves the
		// document holding what it was sent. Waiting for only one of the two is
		// how a fixture comes to agree with the answer it is checking.
		reads := func() string {
			mine.mu.Lock()
			d := mine.docs["paper"]
			mine.mu.Unlock()
			if d == nil {
				return ""
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			text, err := d.doc.Text("body")
			if err != nil {
				return ""
			}
			return text.String()
		}
		deadline := time.Now().Add(10 * time.Second)
	settled:
		for time.Now().Before(deadline) {
			select {
			case followErr = <-done:
				break settled
			default:
			}
			if held = reads(); held == body {
				break settled
			}
			time.Sleep(10 * time.Millisecond)
		}
		// Read once more after either outcome: a link that returned may have
		// applied part of what it carried, and that is exactly what the refused
		// case has to be able to see.
		held = reads()
		mu.Lock()
		defer mu.Unlock()
		return followErr, held, append([]error(nil), heard...)
	}

	t.Run("more than the bound ends the link and lands nothing", func(t *testing.T) {
		err, held, heard := run(t, ops-1)
		if !errors.Is(err, crdt.ErrTooManyOps) {
			t.Fatalf("Follow gave %v; want crdt.ErrTooManyOps underneath", err)
		}
		if held != "" {
			t.Errorf("the follower holds %q; a refused batch must land nothing", held)
		}
		// The operator is the only one who can act on this: the other end of a
		// link is somebody else's server, and it is not reading our logs.
		if len(heard) == 0 {
			t.Error("nothing reached OnOperationsRefused; a link's refusal would be silent")
		}
	})

	t.Run("the same link under a bound that admits it lands", func(t *testing.T) {
		err, held, heard := run(t, ops)
		if err != nil {
			t.Fatalf("Follow returned %v under a bound that admits the message", err)
		}
		if held != body {
			t.Errorf("the follower holds %q, want %q -- so the case above proves a refusal and not a broken link", held, body)
		}
		if len(heard) != 0 {
			t.Errorf("the operator was told about an accepted message: %v", heard)
		}
	})
}
