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
	// ⛔⛔ TWO PARTS AND TWO SITES, NOT ONE OF EACH, and the reason is the only
	// thing in this file that could hide a real defect. A Clone that is deep
	// for the FIRST entry it iterates and shallow for the rest — of the parts,
	// or of the sites within a vector — is invisible to a fixture with one
	// part and one site: the write below would land on the one entry that was
	// copied. crdt@v0.57.0 loops over every part and every site
	// unconditionally, so this is a gap in the WITNESS rather than in the code,
	// which is exactly the kind that survives a refactor nobody connects to it.
	part := crdt.Part{Kind: crdt.PartText, Name: "body"}
	other := crdt.Part{Kind: crdt.PartMap, Name: "cells"}
	ver := func(n uint64) crdt.CompositeVersion {
		return crdt.CompositeVersion{
			part:  crdt.VersionVector{1: n, 2: n + 1},
			other: crdt.VersionVector{1: n + 2, 2: n + 3},
		}
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
	// ⛔ NON-NIL, not merely empty, because the doc promises an empty SLICE: a
	// caller that branches on `acks == nil` instead of the bool would be back
	// in the conflation this contract exists to remove. The review that asked
	// for the contract also caught the assertion being weaker than the doc.
	if acks, ok := s.Acknowledged("paper"); !ok || acks == nil || len(acks) != 0 {
		t.Errorf("an empty room must answer (empty non-nil, true), got (%#v, %v)", acks, ok)
	}

	high := &subscriber{site: 7, have: ver(9)}
	low := &subscriber{site: 2, have: ver(4)}
	silent := &subscriber{site: 5} // joined, said nothing
	d.subs = map[*subscriber]struct{}{high: {}, low: {}, silent: {}}

	acks, ok := s.Acknowledged("paper")
	if !ok || len(acks) != 3 {
		t.Fatalf("three participants, got (%v, %v)", acks, ok)
	}

	// ⛔ SORTED, and asserted over repeated calls because d.subs is a map, which
	// is exactly how a missing sort ships. The same reasoning as the file-order
	// defect in go-fleettools/fleettools#250 — a verdict that moves on the same
	// bytes cannot be compared with the previous one.
	//
	// ⚠ The odds per call are about one in THREE, not one in six: with three
	// keys in one bucket Go randomises the start offset, so the reachable
	// orders are rotations rather than permutations. A review corrected the
	// arithmetic. 24 calls is (1/3)^24, around 1e-12, so the count stands —
	// and asserting the exact order rather than "is sorted" also catches a
	// comparator the wrong way round.
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
	// Every part and every site, for the reason given where `other` is declared:
	// a clone that copied only the first of either would pass a single write.
	for _, p := range []crdt.Part{part, other} {
		for s := crdt.SiteID(1); s <= 2; s++ {
			acks[0].Have[p][s] = 999
		}
	}
	// And a part inserted into what the caller was handed must not appear in
	// the server's own map — the other route a shared OUTER map takes.
	acks[0].Have[crdt.Part{Kind: crdt.PartText, Name: "forged"}] = crdt.VersionVector{1: 7}
	want := ver(4)
	for p, vv := range want {
		for s, n := range vv {
			if low.have[p][s] != n {
				t.Errorf("part %v site %d was aliased rather than cloned: the server now reads %d, not %d",
					p, s, low.have[p][s], n)
			}
		}
	}
	if len(low.have) != len(want) {
		t.Errorf("the outer map was shared: the server now holds %d part(s), not %d", len(low.have), len(want))
	}
}
