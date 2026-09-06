//go:build !js

package collab

import (
	"bytes"
	"testing"

	"github.com/go-crdt/crdt"
)

// A checked snapshot is the snapshot, with nine bytes in front of it.
//
// That is the whole point of it existing beside [PackSnapshot]: a medium that
// stores the difference between one version and the next can still see the
// difference. The numbers are on checkedRawMagic.
func TestACheckedSnapshotStillHoldsTheSnapshot(t *testing.T) {
	snapshot := written(t, "a git repository deltas one version against the one before it")
	checked := CheckSnapshot(snapshot)

	if len(checked) != len(snapshot)+9 {
		t.Errorf("checked is %d bytes for a %d-byte snapshot; expected nine more", len(checked), len(snapshot))
	}
	if !bytes.Equal(checked[9:], snapshot) {
		t.Error("the snapshot is not in there as it was given")
	}
	got, err := UnpackSnapshot(checked)
	if err != nil {
		t.Fatalf("a checked snapshot does not unpack: %v", err)
	}
	if !bytes.Equal(got, snapshot) {
		t.Error("a checked snapshot unpacks to something else")
	}

	// Packed and checked are told apart by what they write at the front, so a
	// store may change from one to the other and still read what it holds.
	if packed, err := UnpackSnapshot(PackSnapshot(snapshot)); err != nil || !bytes.Equal(packed, snapshot) {
		t.Errorf("one reader no longer reads both: %v", err)
	}
}

// Every single-bit flip of a checked snapshot is refused.
//
// Counted the same way as the packed half, because the claim is the same claim:
// bare, roughly half of them load as a different document.
func TestEverySingleBitFlipOfACheckedSnapshotIsRefused(t *testing.T) {
	snapshot := written(t, "the medium is allowed to change a byte nobody asked it to change")
	base, ok := reloaded(snapshot)
	if !ok {
		t.Fatal("the snapshot does not load")
	}

	checked := CheckSnapshot(snapshot)
	var silent int
	for i := range checked {
		for bit := 0; bit < 8; bit++ {
			flipped := append([]byte(nil), checked...)
			flipped[i] ^= 1 << bit
			out, err := UnpackSnapshot(flipped)
			if err != nil {
				continue
			}
			if got, ok := reloaded(out); ok && got != base {
				silent++
			}
		}
	}
	if silent != 0 {
		t.Errorf("%d flips of a checked snapshot loaded as a different document", silent)
	}
	t.Logf("%d bit positions, %d served as a different document", len(checked)*8, silent)
}

// reloaded is the identity of a document: the bytes a reload re-emits.
func reloaded(b []byte) (string, bool) {
	c, err := crdt.LoadComposite(2, b)
	if err != nil {
		return "", false
	}
	return string(c.Snapshot()), true
}

// Bytes that say they are checked and are too short to be are refused rather
// than indexed into.
func TestACheckedSnapshotTooShortToHoldAChecksumIsRefused(t *testing.T) {
	for _, n := range []int{0, 1, 3} {
		stored := append(checkedRawMagic[:], make([]byte, n)...)
		if _, err := UnpackSnapshot(stored); err == nil {
			t.Errorf("%d bytes after the magic was accepted", n)
		}
	}
}
