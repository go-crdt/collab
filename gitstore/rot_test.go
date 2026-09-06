package gitstore

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-crdt/crdt"
)

// made returns a store, a composite with something in it, and where the state
// file for "doc" landed once it was saved.
func made(t *testing.T) (*Store, []byte, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := New(dir, WithAuthor("loom", "loom@example"))
	if err != nil {
		t.Fatal(err)
	}
	c := crdt.NewComposite(1)
	body, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, strings.Repeat("a file in a work tree is a plain file. ", 6)); err != nil {
		t.Fatal(err)
	}
	snapshot := c.Snapshot()
	if err := s.Save(context.Background(), "doc", snapshot); err != nil {
		t.Fatal(err)
	}
	var at string
	if err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && filepath.Base(p) == stateFile {
			at = p
		}
		return nil
	}); err != nil || at == "" {
		t.Fatalf("no %s under %s (%v)", stateFile, dir, err)
	}
	return s, snapshot, at
}

// A state file a byte of which changed is refused, not served.
//
// This package said "git is content-addressed, so gitstore cannot serve bytes
// that are not the bytes it stored". Git is, and the objects are safe; but
// [Store.Load] reads tree.Filesystem.Open, which is the checked-out file and
// not the object. Measured before the frame existed: of 304 one-bit changes to
// that file, 304 were handed back to the caller and none refused.
func TestAStateFileChangedOnDiskIsRefused(t *testing.T) {
	s, base, at := made(t)
	onDisk, err := os.ReadFile(at)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// What the store hands back, and what that turns into one layer up. The
	// second is the guarantee that matters: a store handing back bytes that no
	// longer open is a refusal one step later, while a store handing back a
	// DIFFERENT document is the loss this frame exists to prevent.
	var handedBack, different int
	for i := range onDisk {
		flipped := append([]byte(nil), onDisk...)
		flipped[i] ^= 1
		if err := os.WriteFile(at, flipped, 0o600); err != nil {
			t.Fatal(err)
		}
		got, err := s.Load(ctx, "doc")
		if err != nil {
			continue
		}
		handedBack++
		if c, err := crdt.LoadComposite(2, got); err == nil && string(c.Snapshot()) != string(base) {
			different++
		}
	}
	if err := os.WriteFile(at, onDisk, 0o600); err != nil {
		t.Fatal(err)
	}
	if different != 0 {
		t.Errorf("%d of %d one-bit changes on disk became a different document", different, len(onDisk))
	}
	// The residue is the frame's own magic: five bytes, and a change to one of
	// them stops the bytes being recognised as framed, so they are read as a
	// snapshot written before the frame existed. Those bytes then start with a
	// checksum where a version belongs, so they do not open -- which is what
	// the count above says, and why it is the count this test asserts on.
	t.Logf("%d bytes on disk: %d handed back, %d opened as a different document",
		len(onDisk), handedBack, different)
}

// A state file written before the frame existed still reads.
//
// A repository that has been running holds bare snapshots, so refusing them
// would lose every document in it at once. They gain the frame the next time
// the document is saved, which is what makes this need no migration.
func TestAStateFileWrittenBeforeTheFrameStillReads(t *testing.T) {
	s, snapshot, at := made(t)
	ctx := context.Background()

	// Written the way the previous version of this package wrote it.
	if err := os.WriteFile(at, snapshot, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load(ctx, "doc")
	if err != nil {
		t.Fatalf("a state file written before the frame is refused: %v", err)
	}
	if string(got) != string(snapshot) {
		t.Fatal("a state file written before the frame reads back as something else")
	}

	if err := s.Save(ctx, "doc", got); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(at)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) == string(snapshot) {
		t.Fatal("saving a bare state file left it bare")
	}
}
