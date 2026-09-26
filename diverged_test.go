//go:build !js

package collab_test

import (
	"errors"
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// compose builds a one-part document written by site, so that two of them can
// be given the same site name and different words.
func compose(t *testing.T, site crdt.SiteID, text string) []byte {
	t.Helper()
	c := crdt.NewComposite(site)
	doc, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := doc.Insert(0, text); err != nil {
		t.Fatal(err)
	}
	return c.Snapshot()
}

// TestAMergeRefusesTwoSnapshotsThatClaimOneHistory is the failure this whole
// arc is about, arriving through a shared repository instead of a link.
//
// Two servers, each with its own gitstore, pulling each other. One of them has
// a participant claiming a site belonging to the other's user. Both snapshots
// then report the same version vector and hold different text — and merging
// them would graft one history onto the other and attribute every character to
// whoever the names say, with no error anywhere.
//
// The controls are the other two cases, and without them this proves nothing: a
// merge that refused everything would pass the first case too.
func TestAMergeRefusesTwoSnapshotsThatClaimOneHistory(t *testing.T) {
	t.Run("same history, different documents, refused", func(t *testing.T) {
		mine := compose(t, 7, "GENUINE")
		theirs := compose(t, 7, "FORGERY")

		if _, err := collab.MergeSnapshots(mine, theirs); !errors.Is(err, collab.ErrDiverged) {
			t.Fatalf("MergeSnapshots gave %v, want collab.ErrDiverged", err)
		}
		// And the other way round, since neither side is privileged.
		if _, err := collab.MergeSnapshots(theirs, mine); !errors.Is(err, collab.ErrDiverged) {
			t.Errorf("reversed, MergeSnapshots gave %v, want collab.ErrDiverged", err)
		}
	})

	t.Run("control: two sides that really agree merge", func(t *testing.T) {
		same := compose(t, 7, "GENUINE")
		merged, err := collab.MergeSnapshots(same, same)
		if err != nil {
			t.Fatalf("merging a document with itself gave %v", err)
		}
		if len(merged) == 0 {
			t.Error("merging a document with itself produced nothing")
		}
	})

	t.Run("control: different sites and different histories merge", func(t *testing.T) {
		mine := compose(t, 7, "hers")
		theirs := compose(t, 9, "his")
		merged, err := collab.MergeSnapshots(mine, theirs)
		if err != nil {
			t.Fatalf("two honest replicas were refused: %v", err)
		}
		c, err := crdt.LoadComposite(1, merged)
		if err != nil {
			t.Fatal(err)
		}
		body, err := c.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		// Both writers' text is in the result, which is what says the refusal
		// above is about divergence and not about refusing to work.
		if got := body.String(); len(got) != len("hers")+len("his") {
			t.Errorf("merged text is %q; both sides should be in it", got)
		}
	})

	t.Run("control: one side ahead is not a divergence", func(t *testing.T) {
		// The same site, one document a prefix of the other's history. Their
		// versions differ, so this is an honest replica that is behind — and it
		// must merge, not be refused. It is also the case ErrDiverged's own
		// documentation says it cannot catch.
		c := crdt.NewComposite(7)
		doc, err := c.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := doc.Insert(0, "abc"); err != nil {
			t.Fatal(err)
		}
		behind := c.Snapshot()
		if _, err := doc.Insert(3, "def"); err != nil {
			t.Fatal(err)
		}
		ahead := c.Snapshot()

		if _, err := collab.MergeSnapshots(ahead, behind); err != nil {
			t.Errorf("a replica that is merely behind was refused: %v", err)
		}
	})
}
