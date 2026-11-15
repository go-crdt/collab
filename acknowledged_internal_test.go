//go:build !js

package collab

import (
	"testing"

	"github.com/go-crdt/crdt"
)

// ⛔⛔ THE COVERAGE GATE CAUGHT THAT THE NEW ACCESSOR HAD NO TEST OF ITS OWN:
// 18 statements uncovered, all of [Server.Acknowledged], while a unit test of
// the FORMATTER that prints its output passed. A test of what a function's
// result is rendered into is not a test of the function.
//
// The document is built by hand rather than through a server and real clients,
// for the two things a running room cannot stage on demand: a participant that
// has acknowledged NOTHING, and subscribers inserted in an order the map will
// not preserve.
func TestAcknowledgedIsTheAxisTheMeetIsAMinimumOver(t *testing.T) {
	part := crdt.Part{Kind: crdt.PartText, Name: "body"}
	ver := func(n uint64) crdt.CompositeVersion {
		return crdt.CompositeVersion{part: crdt.VersionVector{1: n}}
	}
	s := &Server{docs: map[string]*document{}}

	// ⛔ A document nobody opened is false, exactly as Stable is: the question
	// has no subject, which is not the same as an empty answer.
	if acks, ok := s.Acknowledged("nosuch"); ok || acks != nil {
		t.Errorf("an unopened document must answer (nil, false), got (%v, %v)", acks, ok)
	}

	d := &document{}
	s.docs["paper"] = d
	// ⛔ But OPEN AND EMPTY IS TRUE, with an empty table, and the distinction is
	// the point: "the document is gone" and "the room emptied" are different
	// findings, and a caller diagnosing a stall acts differently on each. The
	// first version of this method answered false for both, and a review named
	// the conflation — the test's message would have reported a misnamed
	// document as a room that emptied.
	if acks, ok := s.Acknowledged("paper"); !ok || len(acks) != 0 {
		t.Errorf("an empty room must answer (empty, true), got (%v, %v)", acks, ok)
	}

	high := &subscriber{site: 7, have: ver(9)}
	low := &subscriber{site: 2, have: ver(4)}
	silent := &subscriber{site: 5} // joined, said nothing
	d.subs = map[*subscriber]struct{}{high: {}, low: {}, silent: {}}

	acks, ok := s.Acknowledged("paper")
	if !ok || len(acks) != 3 {
		t.Fatalf("three participants, got (%v, %v)", acks, ok)
	}

	// ⛔ SORTED, and asserted over repeated calls because d.subs is a map: one
	// call has a one-in-six chance of coming out ordered by accident, which is
	// exactly how a missing sort ships. The same reasoning as the file-order
	// defect in go-fleettools/fleettools#250 — a verdict that moves on the same
	// bytes cannot be compared with the previous one.
	for i := 0; i < 24; i++ {
		got, _ := s.Acknowledged("paper")
		if got[0].Site != 2 || got[1].Site != 5 || got[2].Site != 7 {
			t.Fatalf("call %d came back unsorted: %d, %d, %d", i, got[0].Site, got[1].Site, got[2].Site)
		}
	}

	// ⛔ Silence is nil, NOT an empty version. A participant the server has
	// heard nothing from is the case that takes the meet to nothing, and an
	// empty CompositeVersion reads as "has acknowledged a history of length 0",
	// which is a different statement a caller would act on differently.
	if acks[1].Have != nil {
		t.Errorf("a participant that said nothing must have a nil version, got %v", acks[1].Have)
	}
	if acks[0].Have == nil || acks[2].Have == nil {
		t.Fatal("the two that did acknowledge must carry their versions")
	}

	// ⛔⛔ CLONED, NOT ALIASED, and this is the assertion that matters most: the
	// server's `have` is guarded by document.mu, so handing a caller the same
	// map would be a data race the moment the next acknowledgement arrives —
	// and a silent corruption if the caller writes to it, as this does.
	acks[0].Have[part][1] = 999
	if low.have[part][1] != 4 {
		t.Errorf("the version was aliased rather than cloned: the server now reads %d", low.have[part][1])
	}
}
