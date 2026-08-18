//go:build !js

package collab_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// Two servers, each with its own participants and its own store, converging
// because one follows the other.
//
// This is the shape a second datacentre takes: a replica near each participant,
// so that a keystroke is echoed locally instead of across an ocean, and a site
// that goes down takes nothing with it that the other does not also hold.
func TestAServerFollowsAnother(t *testing.T) {
	_, connA := serve(t, collab.Config{Store: collab.NewMemoryStore()})
	srvB, connB := serve(t, collab.Config{Store: collab.NewMemoryStore()})

	// B follows A. The link runs until the test ends.
	linked := make(chan error, 1)
	go func() { linked <- srvB.Follow(t.Context(), collab.GRPC(connA), "doc", 999) }()

	ada := join(t, connA, collab.ClientConfig{Document: "doc", Site: 1})
	grace := join(t, connB, collab.ClientConfig{Document: "doc", Site: 2})

	// Ada is on A, Grace is on B, and neither server has met the other's
	// participant.
	if err := body(t, ada).Insert(0, "depuis A"); err != nil {
		t.Fatal(err)
	}
	awaitFor(t, grace, "Ada's text to cross the link", settleJS, func() bool {
		return text(t, grace) == "depuis A"
	})

	// And back the other way, which is the half a one-directional relay would
	// not give: B's link is a participant of A, so what B learns it tells A.
	if err := body(t, grace).Insert(grace0(t, grace), " et depuis B"); err != nil {
		t.Fatal(err)
	}
	awaitFor(t, ada, "Grace's text to cross back", settleJS, func() bool {
		return text(t, ada) == "depuis A et depuis B"
	})

	// Both replicas hold the same document, not merely the same text.
	awaitBoth(t, ada, grace, "the two servers to agree", func() bool {
		return text(t, ada) == text(t, grace)
	})

	select {
	case err := <-linked:
		t.Fatalf("the link ended: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
}

// grace0 is where to append, in the units the text handle counts in.
func grace0(t *testing.T, c *collab.Client) int {
	t.Helper()
	return body(t, c).Len()
}

// Two servers that follow each other. This is the arrangement a pair of
// datacentres actually takes — neither is the origin, both accept writes — and
// it is the one that loops if operations that changed nothing are passed on.
//
// The loop would not be visible as a wrong document: every replica converges on
// the right text either way, because applying an operation twice is what a CRDT
// makes harmless. It would be visible as two servers talking to each other
// forever about one keystroke. So this watches the traffic settle rather than
// the text arrive: after everyone agrees, nothing more is said.
func TestTwoServersFollowingEachOther(t *testing.T) {
	srvA, connA := serve(t, collab.Config{Store: collab.NewMemoryStore()})
	srvB, connB := serve(t, collab.Config{Store: collab.NewMemoryStore()})

	go func() { _ = srvB.Follow(t.Context(), collab.GRPC(connA), "doc", 998) }()
	go func() { _ = srvA.Follow(t.Context(), collab.GRPC(connB), "doc", 999) }()

	ada := join(t, connA, collab.ClientConfig{Document: "doc", Site: 1})
	grace := join(t, connB, collab.ClientConfig{Document: "doc", Site: 2})

	if err := body(t, ada).Insert(0, "aller"); err != nil {
		t.Fatal(err)
	}
	awaitFor(t, grace, "the text to cross", settleJS, func() bool {
		return text(t, grace) == "aller"
	})
	if err := body(t, grace).Insert(body(t, grace).Len(), " retour"); err != nil {
		t.Fatal(err)
	}
	awaitFor(t, ada, "the text to cross back", settleJS, func() bool {
		return text(t, ada) == "aller retour"
	})

	// Now the part that would fail if an operation nobody learned from were
	// passed on: with the two links echoing each other, a participant would
	// keep being told about a document that has stopped changing. Nothing is
	// edited from here on, so nothing should arrive.
	drained := func(c *collab.Client) int {
		n := 0
		for {
			select {
			case <-c.Changes():
				n++
			case <-time.After(time.Second):
				return n
			}
		}
	}
	if n := drained(ada); n > 0 {
		t.Fatalf("Ada was told about %d changes after everything had settled", n)
	}
	if n := drained(grace); n > 0 {
		t.Fatalf("Grace was told about %d changes after everything had settled", n)
	}
	if text(t, ada) != "aller retour" || text(t, grace) != "aller retour" {
		t.Fatalf("the two disagree: %q and %q", text(t, ada), text(t, grace))
	}
}

// What Follow refuses before it opens anything.
func TestFollowRefusesWhatItCannotDo(t *testing.T) {
	srv, conn := serve(t, collab.Config{Store: collab.NewMemoryStore()})

	if err := srv.Follow(t.Context(), collab.GRPC(conn), "", 5); err == nil {
		t.Fatal("Follow with no document name succeeded")
	}
	// Site 0 is the server's own replica: a link taking it would be minting
	// operations as the server, which is the one identity nothing may claim.
	if err := srv.Follow(t.Context(), collab.GRPC(conn), "doc", 0); err == nil {
		t.Fatal("Follow as the server's own site succeeded")
	}
}

// A link whose peer refuses it ends, rather than retrying forever inside the
// call: coming back is the operator's policy and not this library's.
func TestFollowEndsWhenThePeerRefuses(t *testing.T) {
	_, refusing := serve(t, collab.Config{
		Store: collab.NewMemoryStore(),
		Authorize: func(context.Context, string, crdt.SiteID) error {
			return errors.New("not for you")
		},
	})
	srv, _ := serve(t, collab.Config{Store: collab.NewMemoryStore()})

	done := make(chan error, 1)
	go func() { done <- srv.Follow(t.Context(), collab.GRPC(refusing), "doc", 7) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Follow returned no error against a server that refused it")
		}
	case <-time.After(settle):
		t.Fatal("Follow did not end when the peer refused it")
	}
}

// A link that is cancelled ends, and lets go of the document it was holding
// open — which is what stops a followed document being pinned in memory
// forever.
func TestFollowEndsWhenCancelled(t *testing.T) {
	_, connA := serve(t, collab.Config{Store: collab.NewMemoryStore()})
	srvB, _ := serve(t, collab.Config{Store: collab.NewMemoryStore()})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- srvB.Follow(ctx, collab.GRPC(connA), "doc", 6) }()

	// Let it establish before pulling it out.
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(settle):
		t.Fatal("a cancelled link did not end")
	}
}
