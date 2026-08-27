//go:build !js

package collab_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// totalSeen adds up a version, so two of them can be compared as "how much of
// the history is in here".
func totalSeen(v crdt.CompositeVersion) uint64 {
	var n uint64
	for _, vv := range v {
		for _, seq := range vv {
			n += seq
		}
	}
	return n
}

// The measurement the collection work needs before anything is collected: in a
// room that is actually being used, does what everybody has certainly seen
// advance, or does it sit at nothing?
//
// It is asked here rather than assumed because the answer decides whether a
// policy for collecting is worth writing. A meet that never moves is a feature
// nobody can use.
func TestWhatEverybodyHasSeenAdvances(t *testing.T) {
	const editors = 8
	const rounds = 20

	srv, conn := serve(t, collab.Config{})
	clients := make([]*collab.Client, 0, editors)
	for i := 0; i < editors; i++ {
		clients = append(clients, join(t, conn, collab.ClientConfig{
			Document: "paper", Site: crdt.SiteID(i + 1),
		}))
	}

	// Nobody has acknowledged anything they have not been sent, so at the very
	// start the meet is either nothing or the empty version.
	if v, ok := srv.Stable("paper"); ok && totalSeen(v) != 0 {
		t.Fatalf("before anybody typed, %d operations were already called stable", totalSeen(v))
	}

	for round := 0; round < rounds; round++ {
		for i, c := range clients {
			if err := body(t, c).Insert(0, fmt.Sprintf("%d", i)); err != nil {
				t.Fatalf("participant %d: %v", i, err)
			}
		}
	}
	want := editors * rounds

	// Everyone converges first: the meet cannot pass what the slowest has.
	for _, c := range clients {
		await(t, c, "the whole text", func() bool {
			return len(body(t, c).String()) == want
		})
	}

	// And then what everybody has certainly seen has to reach the same place. It
	// arrives after convergence rather than with it: an acknowledgement
	// describes what its sender had applied when it was sent, so it is always a
	// round-trip behind the state it describes.
	deadline := time.Now().Add(20 * time.Second)
	var seen uint64
	for {
		v, ok := srv.Stable("paper")
		if ok {
			seen = totalSeen(v)
			if seen >= uint64(want) {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("what everybody has certainly seen stalled at %d of %d operations", seen, want)
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Logf("%d participants, %d operations: everybody has certainly seen all of them", editors, want)
}

// A participant that says nothing holds the answer back, rather than being
// counted as having everything. That is the safe direction: a replica nobody has
// heard from is exactly the replica that might not have seen the operation.
func TestOneSilentParticipantHoldsTheAnswerBack(t *testing.T) {
	srv, conn := serve(t, collab.Config{})
	ada := join(t, conn, collab.ClientConfig{Document: "paper", Site: 1})
	if err := body(t, ada).Insert(0, "hello"); err != nil {
		t.Fatal(err)
	}
	await(t, ada, "her own text", func() bool {
		return body(t, ada).String() == "hello"
	})
	// Wait for her acknowledgement to land, so the meet is hers alone.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if v, ok := srv.Stable("paper"); ok && totalSeen(v) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("one participant alone never acknowledged anything")
		}
		time.Sleep(2 * time.Millisecond)
	}

	// A second participant arrives and is sent a snapshot. Until it says so,
	// the meet must not claim it has anything.
	grace := join(t, conn, collab.ClientConfig{Document: "paper", Site: 2})
	await(t, grace, "the text", func() bool {
		return body(t, grace).String() == "hello"
	})
	// It will acknowledge, and the meet comes back — the point being that it
	// went through zero rather than staying at Ada's version while a
	// participant that had said nothing was in the room.
	deadline = time.Now().Add(10 * time.Second)
	for {
		v, ok := srv.Stable("paper")
		if ok && totalSeen(v) >= 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the meet never recovered once the second participant acknowledged")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// A participant that only reads is heard from too, which is the whole point of
// acknowledging from the goroutine that receives.
//
// It is also what a room mostly is. One person writing and thirty watching is
// the ordinary shape of a document being reviewed, and if the thirty said
// nothing the answer would sit where the writer last was — which is to say, one
// message behind, for ever.
func TestAParticipantThatOnlyReadsIsHeardFrom(t *testing.T) {
	srv, conn := serve(t, collab.Config{})
	writer := join(t, conn, collab.ClientConfig{Document: "paper", Site: 1})
	readers := make([]*collab.Client, 0, 4)
	for i := 0; i < 4; i++ {
		readers = append(readers, join(t, conn, collab.ClientConfig{
			Document: "paper", Site: crdt.SiteID(i + 2),
		}))
	}

	const want = 12
	for i := 0; i < want; i++ {
		if err := body(t, writer).Insert(0, "x"); err != nil {
			t.Fatal(err)
		}
	}
	for i, r := range readers {
		await(t, r, "the text", func() bool { return len(body(t, r).String()) == want })
		_ = i
	}

	// Nobody but the writer has written anything, so if a reader could not
	// acknowledge, the meet would stop at whatever the writer last reported —
	// which is before it received its own last broadcast.
	deadline := time.Now().Add(20 * time.Second)
	var seen uint64
	for {
		if v, ok := srv.Stable("paper"); ok {
			seen = totalSeen(v)
			if seen >= want {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("with four readers and one writer, the answer stalled at %d of %d", seen, want)
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Logf("one writer and %d readers: all %d operations are certainly seen", len(readers), want)
}
