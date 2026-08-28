//go:build (js && wasm) || !js

package collab_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// writing returns a snapshot of a document whose body is text, written by a
// replica of its own so that two of them are genuinely concurrent.
func writing(t *testing.T, site crdt.SiteID, text string) []byte {
	t.Helper()
	made := crdt.NewComposite(site)
	body, err := made.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, text); err != nil {
		t.Fatal(err)
	}
	return made.Snapshot()
}

// saying reads the body back out of a snapshot.
func saying(t *testing.T, snapshot []byte) string {
	t.Helper()
	doc, err := crdt.LoadComposite(1, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	body, err := doc.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	return body.String()
}

// answering is a store that holds what it is given and refuses when it was told
// to. What a MultiStore does with a store that is down is the whole question
// here, so the store that is down has to be a real one.
type answering struct {
	held    map[string][]byte
	loadErr error
	saveErr error
	saves   int
}

func holding(t *testing.T, site crdt.SiteID, text string) *answering {
	t.Helper()
	return &answering{held: map[string][]byte{"doc": writing(t, site, text)}}
}

func (s *answering) Load(_ context.Context, document string) ([]byte, error) {
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	return s.held[document], nil
}

func (s *answering) Save(_ context.Context, document string, snapshot []byte) error {
	s.saves++
	if s.saveErr != nil {
		return s.saveErr
	}
	if s.held == nil {
		s.held = map[string][]byte{}
	}
	s.held[document] = append([]byte(nil), snapshot...)
	return nil
}

func TestMergingSnapshotsLosesNeitherSide(t *testing.T) {
	ada, grace := writing(t, 3, "ada"), writing(t, 7, "grace")

	merged, err := collab.MergeSnapshots(ada, grace)
	if err != nil {
		t.Fatal(err)
	}
	got := saying(t, merged)
	for _, want := range []string{"ada", "grace"} {
		if !strings.Contains(got, want) {
			t.Fatalf("the merge dropped a side: %q does not contain %q", got, want)
		}
	}

	// Symmetric to the byte, which is what lets two instances resolve the same
	// divergence independently and still agree.
	other, err := collab.MergeSnapshots(grace, ada)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(merged, other) {
		t.Fatal("merging the same two snapshots in the other order gave different bytes")
	}

	// Idempotent, so a merge that runs twice is not a second document.
	again, err := collab.MergeSnapshots(merged, ada)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(merged, again) {
		t.Fatal("merging in a snapshot that was already there changed the document")
	}
}

func TestMergingWithNothingIsTheOtherSide(t *testing.T) {
	ada := writing(t, 3, "ada")

	for _, c := range []struct {
		name         string
		ours, theirs []byte
		want         []byte
	}{
		{"they have never held it", ada, nil, ada},
		{"we have never held it", nil, ada, ada},
		{"neither has", nil, nil, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := collab.MergeSnapshots(c.ours, c.theirs)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, c.want) {
				t.Fatalf("got %d bytes, want %d", len(got), len(c.want))
			}
		})
	}
}

func TestMergingSaysWhichSideItCouldNotRead(t *testing.T) {
	ada, junk := writing(t, 3, "ada"), []byte("this is not a snapshot")

	for _, c := range []struct {
		name         string
		ours, theirs []byte
		want         string
	}{
		{"ours is unreadable", junk, ada, "our side"},
		{"theirs is unreadable", ada, junk, "their side"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if _, err := collab.MergeSnapshots(c.ours, c.theirs); err == nil {
				t.Fatal("merging unreadable bytes succeeded")
			} else if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("the error does not say which side: %v", err)
			}
		})
	}
}

func TestAMultiStoreReadsEveryStoreAndMergesThem(t *testing.T) {
	// The two stores hold different documents, which is what a save that failed
	// halfway leaves behind. Reading the first and stopping would drop a name.
	both := collab.NewMultiStore(holding(t, 3, "ada"), holding(t, 7, "grace"))

	got, err := both.Load(context.Background(), "doc")
	if err != nil {
		t.Fatal(err)
	}
	text := saying(t, got)
	for _, want := range []string{"ada", "grace"} {
		if !strings.Contains(text, want) {
			t.Fatalf("reading the stores dropped a side: %q does not contain %q", text, want)
		}
	}
}

func TestAStoreAddedToARunningServerIsBackfilled(t *testing.T) {
	// The property that makes this usable on a deployment that already exists:
	// the new store answers with nothing, the merge is the old store's document
	// unchanged, and the next save writes it across.
	existing, fresh := holding(t, 3, "ada"), &answering{}
	both := collab.NewMultiStore(existing, fresh)

	snapshot, err := both.Load(context.Background(), "doc")
	if err != nil {
		t.Fatal(err)
	}
	if got := saying(t, snapshot); got != "ada" {
		t.Fatalf("the empty store changed the document: %q", got)
	}
	if err := both.Save(context.Background(), "doc", snapshot); err != nil {
		t.Fatal(err)
	}
	if _, ok := fresh.held["doc"]; !ok {
		t.Fatal("the store that was added holds nothing after a save")
	}
}

func TestADocumentNoStoreHoldsIsNilAndNotAnError(t *testing.T) {
	// Nil is how a store says "new document", and a MultiStore of stores that
	// all say it has to say it too, or every new document would look unreadable.
	got, err := collab.NewMultiStore(&answering{}, &answering{}).Load(context.Background(), "doc")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("a document no store holds came back as %d bytes", len(got))
	}
}

func TestAStoreThatCannotBeReadMakesTheDocumentUnavailable(t *testing.T) {
	// Whichever store is down, and whether or not another one answered: falling
	// back to what did answer would serve a document missing whatever only the
	// unreadable store held, and the next save would write that shortened
	// document over the store that was merely unreachable.
	gone := errors.New("the disk is gone")
	for _, stores := range [][]collab.Store{
		{&answering{loadErr: gone}, holding(t, 3, "ada")},
		{holding(t, 3, "ada"), &answering{loadErr: gone}},
	} {
		_, err := collab.NewMultiStore(stores...).Load(context.Background(), "doc")
		if !errors.Is(err, gone) {
			t.Fatalf("a store that could not be read did not fail the load: %v", err)
		}
	}
}

func TestAnUnreadableSnapshotSaysWhichStoreItCameFrom(t *testing.T) {
	// The first store's bytes are handed back unparsed, so it takes a second
	// store holding something unreadable to reach the merge's own failure.
	unreadable := &answering{held: map[string][]byte{"doc": []byte("not a snapshot")}}
	_, err := collab.NewMultiStore(holding(t, 3, "ada"), unreadable).Load(context.Background(), "doc")
	if err == nil {
		t.Fatal("an unreadable snapshot in the second store was merged anyway")
	}
	if !strings.Contains(err.Error(), "store 1") {
		t.Fatalf("the error does not name the store: %v", err)
	}
}

func TestSavingTriesEveryStoreAndReportsEveryRefusal(t *testing.T) {
	full, readOnly := errors.New("no space left on device"), errors.New("read-only")
	first := &answering{saveErr: full}
	middle := &answering{}
	last := &answering{saveErr: readOnly}

	err := collab.NewMultiStore(first, middle, last).Save(context.Background(), "doc", writing(t, 3, "ada"))
	if err == nil {
		t.Fatal("a save that two stores refused was reported as a success")
	}
	for _, want := range []error{full, readOnly} {
		if !errors.Is(err, want) {
			t.Fatalf("the error does not carry %v: %v", want, err)
		}
	}
	// The store between them was written even though the one before it failed:
	// a store being down must not stop the others from being saved.
	if _, ok := middle.held["doc"]; !ok {
		t.Fatal("a store after a failing one was never written")
	}
	if last.saves != 1 {
		t.Fatalf("the last store was attempted %d times, want 1", last.saves)
	}
}

func TestSavingToEveryStoreSucceedsQuietly(t *testing.T) {
	first, second := &answering{}, &answering{}
	snapshot := writing(t, 3, "ada")
	if err := collab.NewMultiStore(first, second).Save(context.Background(), "doc", snapshot); err != nil {
		t.Fatal(err)
	}
	for i, store := range []*answering{first, second} {
		if !bytes.Equal(store.held["doc"], snapshot) {
			t.Fatalf("store %d holds something other than what was saved", i)
		}
	}
}

func TestAMultiStoreOfOneIsThatStore(t *testing.T) {
	only := holding(t, 3, "ada")
	got, err := collab.NewMultiStore(only).Load(context.Background(), "doc")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, only.held["doc"]) {
		t.Fatal("a single store's bytes did not come back unchanged")
	}
}

func TestAMultiStoreOfNothingIsAProgrammingError(t *testing.T) {
	defer func() {
		got, ok := recover().(string)
		if !ok || !strings.Contains(got, "at least one store") {
			t.Fatalf("a store list of nothing did not say why it was refused: %v", got)
		}
	}()
	collab.NewMultiStore()
	t.Fatal("a store that would silently have kept nothing was accepted")
}

// A server given a MultiStore keeps its documents in every store, and finds
// them again in each one afterwards.
func TestAServerKeepsDocumentsInEveryStore(t *testing.T) {
	first, second := &answering{}, &answering{}
	var store collab.Store = collab.NewMultiStore(first, second)

	if err := store.Save(context.Background(), "doc", writing(t, 3, "ada")); err != nil {
		t.Fatal(err)
	}
	for i, kept := range []*answering{first, second} {
		if got := saying(t, kept.held["doc"]); got != "ada" {
			t.Fatalf("store %d holds %q", i, got)
		}
	}
}

// Two stores holding the same document, one of which has collected, still merge
// into one that holds everything either held.
//
// The difference has to be taken from the collected side: it is the only one
// that can still say what the other is missing, because what it gave back is
// not in its differences any more. Which way round it goes is not something a
// caller should have to know.
func TestMergingWhenOneSideHasCollected(t *testing.T) {
	a := crdt.NewComposite(1)
	body, err := a.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, "AAA"); err != nil {
		t.Fatal(err)
	}
	// The other store's copy stops here, before anything else happens.
	behind := a.Snapshot()

	// A second site writes, so the first run is one of its own and can die
	// whole; then it does, and what is left is collected.
	peer := crdt.NewComposite(2)
	history, err := a.OpsSince(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.Apply(history...); err != nil {
		t.Fatal(err)
	}
	peerBody, err := peer.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	theirs, err := peerBody.Insert(peerBody.Len(), "BBB")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Apply(crdt.PartOps{
		Part: crdt.Part{Kind: crdt.PartText, Name: "body"}, Text: theirs,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := body.Delete(0, 3); err != nil {
		t.Fatal(err)
	}
	if n := a.Collect(a.Version()); n == 0 {
		t.Fatal("nothing was collected, so nothing here is being tested")
	}
	ahead := a.Snapshot()
	want := body.String()

	// Both ways round, because a caller does not choose which store it reads
	// first.
	for _, order := range []struct {
		name         string
		ours, theirs []byte
	}{
		{"the collected side second", behind, ahead},
		{"the collected side first", ahead, behind},
	} {
		t.Run(order.name, func(t *testing.T) {
			merged, err := collab.MergeSnapshots(order.ours, order.theirs)
			if err != nil {
				t.Fatalf("MergeSnapshots: %v", err)
			}
			doc, err := crdt.LoadComposite(9, merged)
			if err != nil {
				t.Fatalf("the merge is unreadable: %v", err)
			}
			text, err := doc.Text("body")
			if err != nil {
				t.Fatal(err)
			}
			if text.String() != want {
				t.Fatalf("the merge reads %q, want %q", text.String(), want)
			}
		})
	}
}

// Two sides that have each collected past the other cannot be merged, and
// saying so is the only honest answer.
//
// Neither can produce a difference from where the other stands, and there is no
// third replica to ask. Whoever holds these two has to pick one, which is a
// choice this package will not make on their behalf.
func TestTwoSidesThatCollectedPastEachOther(t *testing.T) {
	// A shared base written by two sites, so each has a run of its own that the
	// other can take away whole.
	one := crdt.NewComposite(1)
	oneBody, err := one.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oneBody.Insert(0, "AAA"); err != nil {
		t.Fatal(err)
	}
	two := crdt.NewComposite(2)
	history, err := one.OpsSince(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := two.Apply(history...); err != nil {
		t.Fatal(err)
	}
	twoBody, err := two.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	theirs, err := twoBody.Insert(twoBody.Len(), "BBB")
	if err != nil {
		t.Fatal(err)
	}
	part := crdt.Part{Kind: crdt.PartText, Name: "body"}
	if err := one.Apply(crdt.PartOps{Part: part, Text: theirs}); err != nil {
		t.Fatal(err)
	}

	// Now they part company: each takes the other's line away and collects,
	// without hearing that the other did the same.
	if _, err := oneBody.Delete(3, 3); err != nil {
		t.Fatal(err)
	}
	if n := one.Collect(one.Version()); n == 0 {
		t.Fatal("the first side collected nothing")
	}
	if _, err := twoBody.Delete(0, 3); err != nil {
		t.Fatal(err)
	}
	if n := two.Collect(two.Version()); n == 0 {
		t.Fatal("the second side collected nothing")
	}

	if _, err := collab.MergeSnapshots(one.Snapshot(), two.Snapshot()); !errors.Is(err, crdt.ErrCollected) {
		t.Fatalf("merging two sides that collected past each other = %v, want ErrCollected", err)
	}
}
