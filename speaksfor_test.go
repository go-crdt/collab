//go:build !js

package collab

import (
	"context"
	"slices"
	"testing"

	"github.com/go-crdt/crdt"
)

// Sites names every site in a batch, over all three kinds.
//
// The kinds are the point. This walk had three copies in this repository and the
// one an operator would copy read a batch's text and nothing else, so a site the
// server did not federate with was refused for a character and allowed for a map
// entry (go-crdt/collab#178). Exported once, there is nothing left to get wrong.
func TestSitesNamesEveryKind(t *testing.T) {
	id := func(site crdt.SiteID, seq uint64) crdt.ID { return crdt.ID{Site: site, Seq: seq} }
	batches := []crdt.PartOps{{
		Part: crdt.Part{Kind: crdt.PartText, Name: "body"},
		Text: []crdt.Op{
			{Kind: crdt.OpInsert, ID: id(7, 1), Clock: 1, Char: 'a'},
			{Kind: crdt.OpInsert, ID: id(7, 2), Clock: 2, Char: 'b'},
		},
	}, {
		Part: crdt.Part{Kind: crdt.PartList, Name: "items"},
		List: []crdt.ListOp{{Kind: crdt.OpInsert, ID: id(8, 1), Clock: 1, Value: []byte("v")}},
	}, {
		Part: crdt.Part{Kind: crdt.PartMap, Name: "cells"},
		Map:  []crdt.MapOp{{Kind: crdt.MapSet, ID: id(9, 1), Clock: 1, Key: "k", Value: []byte("v")}},
	}}

	got := slices.Collect(Sites(batches...))
	// Repeats included and in order: a hundred characters from one site name it a
	// hundred times, and a policy that wants a set can make one.
	if want := []crdt.SiteID{7, 7, 8, 9}; !slices.Equal(got, want) {
		t.Errorf("Sites gave %v, want %v", got, want)
	}

	// Stopping early stops the walk, which is what lets a policy refuse on the
	// first site it does not like rather than reading the whole batch -- and it is
	// asserted in each KIND separately, because each loop has its own way out and
	// a test that only breaks during the text leaves two of them unreached. The
	// coverage gate said so, which is what it is for.
	for _, only := range []struct {
		kind  string
		batch crdt.PartOps
	}{
		{"text", batches[0]},
		{"list", batches[1]},
		{"map", batches[2]},
	} {
		t.Run("breaking during the "+only.kind, func(t *testing.T) {
			seen := 0
			for range Sites(only.batch) {
				seen++
				break
			}
			if seen != 1 {
				t.Errorf("breaking after one site walked %d of them", seen)
			}
		})
	}
}

// SpeaksFor asks about the sites a session carries, and never about its own.
//
// The four rows are the corners of the rule, and the third is the attack in
// go-crdt/collab#175: a link may not write as one of this server's own users. A
// policy phrased as "the scopes I federate with" invites listing your own among
// them, and gitstore's example did exactly that, which is how a link from Lyon came
// to be allowed to write as ada@paris. Here that temptation is gone, because a
// session's own site is allowed without the predicate being consulted at all.
func TestSpeaksForAsksOnlyAboutOtherSites(t *testing.T) {
	site := func(eppn string) crdt.SiteID { return crdt.DeriveSiteID([]byte(eppn)) }
	adaParis, graceParis := site("ada@paris.example.ac"), site("grace@paris.example.ac")
	graceLyon, linkLyon := site("grace@lyon.example.ac"), site("link@lyon.example.ac")

	var asked []crdt.SiteID
	policy := SpeaksFor(func(_ context.Context, document string, session, carried crdt.SiteID) bool {
		if document != "paper" {
			t.Errorf("the predicate was told document %q", document)
		}
		asked = append(asked, carried)
		// Lyon and nobody else. Paris is NOT listed: our own users do not need to
		// be, which is the whole point.
		return carried == graceLyon || carried == linkLyon
	})
	batchBy := func(s crdt.SiteID) []crdt.PartOps {
		return []crdt.PartOps{{
			Part: crdt.Part{Kind: crdt.PartText, Name: "body"},
			Text: []crdt.Op{{Kind: crdt.OpInsert, ID: crdt.ID{Site: s, Seq: 1}, Clock: 1, Char: 'x'}},
		}}
	}

	for _, c := range []struct {
		name             string
		session, carries crdt.SiteID
		refused          bool
		askedAbout       bool
	}{
		{"our own user, writing for herself", adaParis, adaParis, false, false},
		{"the Lyon link, carrying Lyon", linkLyon, graceLyon, false, true},
		{"the Lyon link, carrying ONE OF OURS", linkLyon, adaParis, true, true},
		{"our own user, handing over another of ours", adaParis, graceParis, true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			asked = nil
			err := policy(context.Background(), "paper", c.session, batchBy(c.carries))
			if c.refused && err == nil {
				t.Fatalf("session %d was allowed to carry site %d", c.session, c.carries)
			}
			if !c.refused && err != nil {
				t.Fatalf("session %d was refused carrying site %d: %v", c.session, c.carries, err)
			}
			if asked := len(asked) > 0; asked != c.askedAbout {
				t.Errorf("the predicate was consulted = %v, want %v", asked, c.askedAbout)
			}
		})
	}
}

// What SpeaksFor costs, so a deployment choosing it knows, and so the reason
// OwnSiteOnly is written out rather than built from it stays checkable.
//
// Measured on this machine at 53.1 ns for a one-operation batch against 3.50 ns
// for the inlined walk in OwnSiteOnly — fifteen times, for two indirect calls an
// operation. Fifty nanoseconds is nothing beside the 2.5 µs an edit costs a
// participant in fan-out; it is only worth avoiding on the path every server
// takes.
func BenchmarkSpeaksFor(b *testing.B) {
	policy := SpeaksFor(func(context.Context, string, crdt.SiteID, crdt.SiteID) bool { return true })
	batches := benchBatches(b, 7, 1)
	other := []crdt.PartOps{{
		Part: batches[0].Part,
		Text: append([]crdt.Op(nil), batches[0].Text...),
	}}
	// A site that is not the session's, so the predicate is actually reached.
	for i := range other[0].Text {
		other[0].Text[i].ID.Site = 8
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := policy(ctx, "d", 7, other); err != nil {
			b.Fatal(err)
		}
	}
}
