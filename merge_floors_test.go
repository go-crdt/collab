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

// reading opens a merged snapshot so a test can ask what survived.
func reading(t *testing.T, snapshot []byte) *crdt.Composite {
	t.Helper()
	held, err := crdt.LoadComposite(7, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return held
}

// keysOf and textOf reach for one part of a merged document.
func keysOf(t *testing.T, snapshot []byte, name string) *crdt.Map {
	t.Helper()
	keys, err := reading(t, snapshot).Map(name)
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func textOf(t *testing.T, snapshot []byte, name string) *crdt.Doc {
	t.Helper()
	text, err := reading(t, snapshot).Text(name)
	if err != nil {
		t.Fatal(err)
	}
	return text
}

// collecting returns a document holding one deleted key, and the same document
// again with that key's tombstone collected away. It is what a server with
// Config.CollectEvery writes, and it needs no purge and no operator: the second
// snapshot is what is persisted, and any store stale across the collect holds
// something like the first.
//
// It carries a text part as well as the map, so that a store stale enough to
// predate the text part encodes shorter than this does. That is not decoration:
// the tie-break of last resort compares the bytes, and a fixture where the
// collected side happens to sort first is one where the base would have been
// the right one whether or not the floors were consulted.
func collecting(t *testing.T) (withTombstone, collected []byte) {
	t.Helper()
	held := crdt.NewComposite(1)
	cells, err := held.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	cells.Set("k", []byte("v"))
	cells.Delete("k")
	notes, err := held.Text("notes")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := notes.Insert(0, "added since"); err != nil {
		t.Fatal(err)
	}
	withTombstone = held.Snapshot()
	if n := held.Collect(held.Version(), held.Clocks()); n != 1 {
		t.Fatalf("collected %d tombstones, want 1", n)
	}
	return withTombstone, held.Snapshot()
}

// stale returns what a store that predates all of that still holds: the map
// alone, and a write to it that never met the deletion.
func stale(t *testing.T, key, value string) []byte {
	t.Helper()
	held := crdt.NewComposite(2)
	cells, err := held.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	cells.Set(key, []byte(value))
	return held.Snapshot()
}

// A key a collect dropped does not come back, whichever way round the merge is
// given its two sides.
//
// This is reachable with nothing purging anything: a server with
// Config.CollectEvery collects and persists, a second store is stale across
// that collect, and MultiStore.Load meets the two. One order used to hand back
// the deleted key alive; the other used to drop the stale side's write and
// return nil.
func TestMergingDoesNotResurrectAKeyACollectDropped(t *testing.T) {
	_, collected := collecting(t)
	lagging := stale(t, "k", "other") // never saw the deletion, and writes the key again

	// The stale side sorts first, so the base is the collected one only because
	// its floor is higher and not because the tie-break of last resort would
	// have reached for it anyway.
	if bytes.Compare(lagging, collected) >= 0 {
		t.Fatal("the fixture no longer straddles the byte comparison, so it no longer tests the floors")
	}

	for _, c := range []struct {
		name         string
		ours, theirs []byte
	}{
		{"the collected side first", collected, lagging},
		{"the stale side first", lagging, collected},
	} {
		t.Run(c.name, func(t *testing.T) {
			merged, err := collab.MergeSnapshots(c.ours, c.theirs)
			if err == nil {
				if value, held := keysOf(t, merged, "cells").Get("k"); held {
					t.Fatalf("the merge brought back a key the other side deleted: k=%q", value)
				}
				t.Fatal("the merge dropped a write it could not carry and said nothing")
			}
			if !errors.Is(err, crdt.ErrStranded) {
				t.Fatalf("want ErrStranded, got %v", err)
			}
		})
	}
}

// A write the merge cannot carry is named rather than dropped, even when it
// names a key nobody deleted.
//
// The key is a different one, so nothing here could come back alive; what is at
// stake is only whether a write survives, and it used to survive one order and
// vanish in the other.
func TestMergingSaysSoRatherThanDroppingAWriteItCannotCarry(t *testing.T) {
	_, collected := collecting(t)
	lagging := stale(t, "j", "w")

	for _, c := range []struct {
		name         string
		ours, theirs []byte
	}{
		{"the collected side first", collected, lagging},
		{"the stale side first", lagging, collected},
	} {
		t.Run(c.name, func(t *testing.T) {
			merged, err := collab.MergeSnapshots(c.ours, c.theirs)
			if err == nil {
				t.Fatalf("the merge returned %d bytes rather than saying it could not carry the write", len(merged))
			}
			if !errors.Is(err, crdt.ErrStranded) {
				t.Fatalf("want ErrStranded, got %v", err)
			}
		})
	}
}

// Two orders, one collected side, the same bytes — and the collect is not
// undone by having been merged with a copy that predates it.
func TestMergingIsSymmetricWhenOneSideCollected(t *testing.T) {
	withTombstone, collected := collecting(t)

	ab, err := collab.MergeSnapshots(withTombstone, collected)
	if err != nil {
		t.Fatal(err)
	}
	ba, err := collab.MergeSnapshots(collected, withTombstone)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ab, ba) {
		t.Fatalf("the two orders gave different bytes:\n  first  %d bytes %x\n  second %d bytes %x", len(ab), ab, len(ba), ba)
	}
	cells := keysOf(t, ab, "cells")
	if cells.CollectedBelow() == 0 {
		t.Fatal("the merge undid the collect")
	}
	if _, held := cells.Get("k"); held {
		t.Fatal("the deleted key is back")
	}
}

// A purged side is the base whichever way round it is given, so the text it
// discarded does not come back.
//
// The stale side cannot be told about the purge: a purged run is in no
// operation, insertions and deletions alike, which is what makes the side that
// purged the only one of the two that can be the base.
func TestMergingWithAPurgedSideKeepsThePurge(t *testing.T) {
	held := crdt.NewComposite(1)
	body, err := held.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, "world"); err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, "hello "); err != nil {
		t.Fatal(err)
	}
	before := held.Snapshot() // a store stale across the purge
	if _, err := body.Delete(0, 6); err != nil {
		t.Fatal(err)
	}
	if n := body.Purge(); n != 6 {
		t.Fatalf("purged %d characters, want 6", n)
	}
	after := held.Snapshot()

	ab, err := collab.MergeSnapshots(before, after)
	if err != nil {
		t.Fatal(err)
	}
	ba, err := collab.MergeSnapshots(after, before)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ab, ba) {
		t.Fatalf("the two orders gave different bytes:\n  first  %d bytes %x\n  second %d bytes %x", len(ab), ab, len(ba), ba)
	}
	merged := textOf(t, ab, "body")
	if got := merged.String(); got != "world" {
		t.Fatalf("the merge resurrected purged text: %q", got)
	}
	if merged.PurgedBelow() == 0 {
		t.Fatal("the merge undid the purge")
	}
}

// Either side could be the base — each can still serve the other — and the
// floor still does not go backwards.
//
// Without this the merge would be free to take the un-purged side, and a
// MultiStore would then undo an operator's purge on every read and write the
// un-purged document back on the next save.
func TestMergingKeepsTheHigherFloorWhenEitherSideCouldBeTheBase(t *testing.T) {
	held := crdt.NewComposite(1)
	body, err := held.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, "world"); err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, "hello "); err != nil {
		t.Fatal(err)
	}
	if _, err := body.Delete(0, 6); err != nil {
		t.Fatal(err)
	}
	full := held.Snapshot()

	// The same operations exactly, so each side can serve the other; only the
	// purge tells them apart.
	other, err := crdt.LoadComposite(2, full)
	if err != nil {
		t.Fatal(err)
	}
	theirBody, err := other.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	theirBody.Purge()
	purged := other.Snapshot()

	if err := other.CanServe(reading(t, full).Version()); err != nil {
		t.Fatalf("the fixture is not the one being tested: the purged side cannot serve the other (%v)", err)
	}

	ab, err := collab.MergeSnapshots(full, purged)
	if err != nil {
		t.Fatal(err)
	}
	ba, err := collab.MergeSnapshots(purged, full)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ab, ba) {
		t.Fatalf("the two orders gave different bytes:\n  first  %d bytes %x\n  second %d bytes %x", len(ab), ab, len(ba), ba)
	}
	if textOf(t, ab, "body").PurgedBelow() == 0 {
		t.Fatal("the merge undid the purge")
	}
}

// discardingEachOthersPast returns two replicas of one document, each having
// purged a run the other still needs. Neither can be brought up to the other:
// what a purge took is in no operation, so there is nothing either could send.
func discardingEachOthersPast(t *testing.T) (left, right []byte) {
	t.Helper()
	opening := crdt.NewComposite(1)
	body, err := opening.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	// Two runs rather than one: a purge discards a run only when every
	// character of it has been deleted, so each side has to be able to delete a
	// whole one.
	if _, err := body.Insert(0, "world"); err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, "hello "); err != nil {
		t.Fatal(err)
	}
	shared := opening.Snapshot()

	discarding := func(site crdt.SiteID, at, count int) []byte {
		t.Helper()
		held, err := crdt.LoadComposite(site, shared)
		if err != nil {
			t.Fatal(err)
		}
		text, err := held.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := text.Delete(at, count); err != nil {
			t.Fatal(err)
		}
		if n := text.Purge(); n != count {
			t.Fatalf("purged %d characters, want %d", n, count)
		}
		return held.Snapshot()
	}
	return discarding(2, 0, 6), discarding(3, 6, 5)
}

// Neither side can serve the other, each having purged a run the other still
// needs: a named refusal, the same either way round.
func TestMergingRefusesTwoSidesThatEachDiscardedTheOthersPast(t *testing.T) {
	left, right := discardingEachOthersPast(t)

	for _, c := range []struct {
		name         string
		ours, theirs []byte
	}{
		{"left first", left, right},
		{"right first", right, left},
	} {
		t.Run(c.name, func(t *testing.T) {
			merged, err := collab.MergeSnapshots(c.ours, c.theirs)
			if !errors.Is(err, collab.ErrUnmergeable) {
				t.Fatalf("want ErrUnmergeable, got %v and %d bytes", err, len(merged))
			}
		})
	}
}

// The side that cannot serve is the base even where the floors cross and say
// nothing useful.
//
// One side purged a text, the other collected a map, so neither has given up
// more than the other and the floors have no opinion. Only [crdt.Doc.CanServe]
// does: the side that purged emits neither the insertions nor the deletions of
// what it discarded, so it has to be underneath. The fixture is deliberately
// one where the byte comparison that settles crossed floors points the other
// way, which is what makes the CanServe rule the thing under test rather than a
// rule that happens to agree with it.
func TestMergingPutsThePurgedSideUnderneathWhenTheFloorsCross(t *testing.T) {
	opening := crdt.NewComposite(1)
	body, err := opening.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, "world"); err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, "hello "); err != nil {
		t.Fatal(err)
	}
	cells, err := opening.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	cells.Set("k", []byte("v"))
	cells.Delete("k")
	shared := opening.Snapshot()

	purging, err := crdt.LoadComposite(2, shared)
	if err != nil {
		t.Fatal(err)
	}
	purgingBody, err := purging.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := purgingBody.Delete(0, 6); err != nil {
		t.Fatal(err)
	}
	if n := purgingBody.Purge(); n != 6 {
		t.Fatalf("purged %d characters, want 6", n)
	}
	purged := purging.Snapshot()

	collecting, err := crdt.LoadComposite(3, shared)
	if err != nil {
		t.Fatal(err)
	}
	collectingCells, err := collecting.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	if n := collectingCells.Collect(collectingCells.Version(), 99); n != 1 {
		t.Fatalf("collected %d tombstones, want 1", n)
	}
	collected := collecting.Snapshot()

	// The fixture is only the one described above while these hold.
	if err := collecting.CanServe(purging.Version()); err != nil {
		t.Fatalf("the collected side should be able to serve the purged one: %v", err)
	}
	if err := purging.CanServe(collecting.Version()); !errors.Is(err, crdt.ErrPurged) {
		t.Fatalf("the purged side should not be able to serve the collected one: %v", err)
	}
	if bytes.Compare(purged, collected) <= 0 {
		t.Fatal("the fixture no longer straddles the byte comparison, so it no longer tests CanServe")
	}

	for _, c := range []struct {
		name         string
		ours, theirs []byte
	}{
		{"the purged side first", purged, collected},
		{"the collected side first", collected, purged},
	} {
		t.Run(c.name, func(t *testing.T) {
			merged, err := collab.MergeSnapshots(c.ours, c.theirs)
			if err != nil {
				t.Fatal(err)
			}
			body := textOf(t, merged, "body")
			if got := body.String(); got != "world" {
				t.Fatalf("the merge resurrected purged text: %q", got)
			}
			if body.PurgedBelow() == 0 {
				t.Fatal("the merge undid the purge")
			}
			if _, held := keysOf(t, merged, "cells").Get("k"); held {
				t.Fatal("the deleted key is back")
			}
		})
	}
}

// Equal floors on both sides: either could be the base, both orders agree, and
// neither side's own writes are lost to the choice.
func TestMergingKeepsBothSidesWhenTheFloorsAreEqual(t *testing.T) {
	_, collected := collecting(t)

	writing := func(site crdt.SiteID, key string) []byte {
		t.Helper()
		held, err := crdt.LoadComposite(site, collected)
		if err != nil {
			t.Fatal(err)
		}
		cells, err := held.Map("cells")
		if err != nil {
			t.Fatal(err)
		}
		cells.Set(key, []byte(key))
		return held.Snapshot()
	}
	left, right := writing(2, "left"), writing(3, "right")

	ab, err := collab.MergeSnapshots(left, right)
	if err != nil {
		t.Fatal(err)
	}
	ba, err := collab.MergeSnapshots(right, left)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ab, ba) {
		t.Fatalf("equal floors merged asymmetrically:\n  first  %d bytes %x\n  second %d bytes %x", len(ab), ab, len(ba), ba)
	}
	cells := keysOf(t, ab, "cells")
	for _, key := range []string{"left", "right"} {
		if _, held := cells.Get(key); !held {
			t.Fatalf("the merge lost %q, keeping %v", key, cells.Keys())
		}
	}
	if cells.CollectedBelow() == 0 {
		t.Fatal("the merge undid the collect")
	}
}

// Floors that genuinely cross: each side collected further than the other, on a
// different map. No one snapshot can hold both economies, so one is declined —
// the same one in both orders, which is the property that matters, because the
// tombstone kept is only a tombstone and neither key comes back.
func TestMergingCrossedFloorsDeclinesOneEconomyAndNotTheOther(t *testing.T) {
	opening := crdt.NewComposite(1)
	for _, name := range []string{"one", "two"} {
		cells, err := opening.Map(name)
		if err != nil {
			t.Fatal(err)
		}
		cells.Set("k", []byte("v"))
		cells.Delete("k")
	}
	shared := opening.Snapshot()

	collectingOne := func(site crdt.SiteID, name string) []byte {
		t.Helper()
		held, err := crdt.LoadComposite(site, shared)
		if err != nil {
			t.Fatal(err)
		}
		cells, err := held.Map(name)
		if err != nil {
			t.Fatal(err)
		}
		if n := cells.Collect(cells.Version(), 99); n != 1 {
			t.Fatalf("collected %d tombstones of %q, want 1", n, name)
		}
		return held.Snapshot()
	}
	left, right := collectingOne(2, "one"), collectingOne(3, "two")

	ab, err := collab.MergeSnapshots(left, right)
	if err != nil {
		t.Fatal(err)
	}
	ba, err := collab.MergeSnapshots(right, left)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ab, ba) {
		t.Fatalf("crossed floors merged asymmetrically:\n  first  %d bytes %x\n  second %d bytes %x", len(ab), ab, len(ba), ba)
	}
	for _, name := range []string{"one", "two"} {
		if _, held := keysOf(t, ab, name).Get("k"); held {
			t.Fatalf("the deleted key came back in %q", name)
		}
	}
}

// Merging a snapshot with itself gives back exactly those bytes, over a
// document that has purged, collected and holds a list as well.
//
// A pull leans on this: adopt puts every file the other instance had and this
// one lacked into the worktree, so a document only they held is merged with
// itself.
func TestMergingASnapshotWithItselfIsThatSnapshot(t *testing.T) {
	held := crdt.NewComposite(1)
	body, err := held.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, "world"); err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, "hello "); err != nil {
		t.Fatal(err)
	}
	if _, err := body.Delete(0, 6); err != nil {
		t.Fatal(err)
	}
	if n := body.Purge(); n != 6 {
		t.Fatalf("purged %d characters, want 6", n)
	}
	cells, err := held.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	cells.Set("k", []byte("v"))
	cells.Delete("k")
	items, err := held.List("items")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := items.Insert(0, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if n := held.Collect(held.Version(), held.Clocks()); n != 1 {
		t.Fatalf("collected %d tombstones, want 1", n)
	}
	snapshot := held.Snapshot()

	merged, err := collab.MergeSnapshots(snapshot, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(merged, snapshot) {
		t.Fatalf("merging a snapshot with itself changed it: %d bytes became %d", len(snapshot), len(merged))
	}
}

// The control: a document nobody purged or collected merges as it always did,
// both orders alike.
func TestMergingPlainConcurrentEditsIsUnchanged(t *testing.T) {
	opening := crdt.NewComposite(1)
	body, err := opening.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, "hello world"); err != nil {
		t.Fatal(err)
	}
	other, err := crdt.LoadComposite(2, opening.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	theirBody, err := other.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(11, "!"); err != nil {
		t.Fatal(err)
	}
	if _, err := theirBody.Delete(0, 6); err != nil {
		t.Fatal(err)
	}

	ab, err := collab.MergeSnapshots(opening.Snapshot(), other.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	ba, err := collab.MergeSnapshots(other.Snapshot(), opening.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ab, ba) {
		t.Fatal("plain concurrent edits merged asymmetrically")
	}
	if got := textOf(t, ab, "body").String(); got != "world!" {
		t.Fatalf("the merged text is %q, want %q", got, "world!")
	}
}

// A MultiStore whose stores cannot be merged makes the document unavailable
// rather than serving one of them.
//
// It is the operational face of the refusal and the thing an operator will
// notice: a document that used to open, wrongly, now stops opening. That is the
// doctrine this store already follows for a store it cannot read — an error
// stops at one document and somebody can fix it, while silent loss is
// discovered by the person who wrote the paragraph.
func TestAMultiStoreRefusesRatherThanServingOneUnmergeableSide(t *testing.T) {
	left, right := discardingEachOthersPast(t)
	stores := collab.NewMultiStore(
		&answering{held: map[string][]byte{"doc": left}},
		&answering{held: map[string][]byte{"doc": right}},
	)

	snapshot, err := stores.Load(context.Background(), "doc")
	if !errors.Is(err, collab.ErrUnmergeable) {
		t.Fatalf("want ErrUnmergeable, got %v and %d bytes", err, len(snapshot))
	}
	if !strings.Contains(err.Error(), "store 1") {
		t.Fatalf("the error does not name the store: %v", err)
	}
}

// Two sides that collected to the SAME floor but gave up DIFFERENT tombstones.
//
// The floors cannot see which tombstones went, only how far each side reached,
// so nothing separates the two — and a base chosen by a non-strict test would
// be chosen by the argument order instead. Measured before the arms were made
// strict: 34 bytes each way, and not the same 34 bytes. The visible document is
// the same either way, which is what makes it a byte-level defect and not a
// data-loss one: it is gitstore's merge driver, where two instances resolving
// one conflict must reach the same commit, that pays for it.
//
// Two deleting sites are what make the fixture possible: a stable version can
// cover one site's deletion without covering the other's, and a single site's
// cannot.
func TestMergingIsSymmetricWhenTheFloorsAreEqualAndTheTombstonesAreNot(t *testing.T) {
	cells := crdt.Part{Kind: crdt.PartMap, Name: "cells"}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	one, two := crdt.NewComposite(1), crdt.NewComposite(2)
	m1, err := one.Map("cells")
	must(err)
	var ops []crdt.PartOps
	for _, k := range []string{"a", "b"} {
		op, err := m1.Set(k, []byte(k))
		must(err)
		ops = append(ops, crdt.PartOps{Part: cells, Map: []crdt.MapOp{op}})
	}
	must(two.Apply(ops...))
	m2, err := two.Map("cells")
	must(err)
	da, err := m1.Delete("a")
	must(err)
	db, err := m2.Delete("b")
	must(err)
	must(two.Apply(crdt.PartOps{Part: cells, Map: []crdt.MapOp{da}}))
	must(one.Apply(crdt.PartOps{Part: cells, Map: []crdt.MapOp{db}}))
	shared := one.Snapshot()

	side := func(site crdt.SiteID, stable crdt.VersionVector) []byte {
		t.Helper()
		c, err := crdt.LoadComposite(site, shared)
		must(err)
		mm, err := c.Map("cells")
		must(err)
		if dropped := mm.Collect(stable, 99); dropped != 1 {
			t.Fatalf("site %d dropped %d tombstones, want exactly one", site, dropped)
		}
		return c.Snapshot()
	}
	left := side(3, crdt.VersionVector{1: 3})  // site 1's deletion of "a"
	right := side(4, crdt.VersionVector{2: 1}) // site 2's deletion of "b"

	ab, err := collab.MergeSnapshots(left, right)
	must(err)
	ba, err := collab.MergeSnapshots(right, left)
	must(err)
	if !bytes.Equal(ab, ba) {
		t.Fatalf("equal floors and unequal tombstones merged asymmetrically:\n  left-first  %d bytes %x\n  right-first %d bytes %x",
			len(ab), ab, len(ba), ba)
	}
	m := keysOf(t, ab, "cells")
	if got := m.CollectedBelow(); got != 99 {
		t.Fatalf("the merge came back with floor %d, want the 99 both sides reached", got)
	}
	if keys := m.Keys(); len(keys) != 0 {
		t.Fatalf("the merge brought back %v; both keys were deleted", keys)
	}
}

// A text and a map may share a name — crdt keeps the kinds in separate
// name-spaces — so the floors are keyed by the whole [crdt.Part] and not by its
// name.
//
// ⚠ This test does NOT pin that choice, and no test can: keying by name alone
// was tried, here and on the sharper fixture of a purged text beside a
// collected map of the same name, and both keyings give the same bytes. The
// base choice is only observable when one side's floor would discard what the
// other holds, and a text's purge and a map's collect never do that to each
// other. It is recorded as an equivalent mutant rather than left as a gap
// somebody spends a day trying to close. What the test does establish is worth
// having on its own: a shared name merges symmetrically and neither part loses
// its floor.
func TestFloorsOfATextAndAMapSharingANameDoNotCollide(t *testing.T) {
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	build := func(site crdt.SiteID, purgeText bool) []byte {
		t.Helper()
		c := crdt.NewComposite(site)
		body, err := c.Text("shared")
		must(err)
		_, err = body.Insert(0, "gone")
		must(err)
		_, err = body.Delete(0, 4)
		must(err)
		m, err := c.Map("shared")
		must(err)
		_, err = m.Set("k", []byte("v"))
		must(err)
		if purgeText {
			if body.Purge() == 0 {
				t.Fatal("nothing was purged")
			}
		}
		return c.Snapshot()
	}
	purged, plain := build(1, true), build(2, false)

	// The purged side dominates on the text and neither has a map floor, so it
	// is the base whichever way round the pair arrives.
	ab, err := collab.MergeSnapshots(purged, plain)
	must(err)
	ba, err := collab.MergeSnapshots(plain, purged)
	must(err)
	if !bytes.Equal(ab, ba) {
		t.Fatalf("a shared name merged asymmetrically:\n  first  %d bytes %x\n  second %d bytes %x", len(ab), ab, len(ba), ba)
	}
	if got := textOf(t, ab, "shared").PurgedBelow(); got == 0 {
		t.Fatal("the merge undid the text's purge")
	}
	if _, held := keysOf(t, ab, "shared").Get("k"); !held {
		t.Fatal("the map that shares the text's name lost its key")
	}
}
