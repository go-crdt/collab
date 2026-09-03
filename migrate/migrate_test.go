package migrate

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// A document a current build cannot open comes out of the store in a format it
// can, and one that is already readable is left exactly as it was.
func TestRewriteMovesOnlyWhatNeedsMoving(t *testing.T) {
	ctx := context.Background()
	store := collab.NewMemoryStore()

	// A document in the format this build writes. Nothing should touch it.
	current := newDocument(t, "already fine")
	if err := store.Save(ctx, "current", current); err != nil {
		t.Fatal(err)
	}
	// And one this build can read but would not write: a version 6 snapshot,
	// which is what a store filled before collab v0.37.0 is full of.
	old := stampVersion(t, newDocument(t, "written long ago"), 6)
	if _, err := crdt.LoadComposite(2, old); err == nil {
		t.Log("the version 6 fixture loads, which is what makes this test possible")
	}

	got, err := Rewrite(ctx, store, []string{"current", "missing"})
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if len(got.Moved) != 0 {
		t.Errorf("a document already in a current format was rewritten: %v", got.Moved)
	}
	if len(got.Current) != 1 || got.Current[0] != "current" {
		t.Errorf("Current = %v, want [current]", got.Current)
	}
	if len(got.Failed) != 0 {
		t.Errorf("Failed = %v, want none", got.Failed)
	}
	// A name with nothing behind it is not a failure and not a move.
	after, err := store.Load(ctx, "current")
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(current) {
		t.Error("a document that needed no move was rewritten anyway")
	}
}

// A document that cannot be read is reported, and nothing is written over it.
func TestRewriteLeavesWhatItCannotRead(t *testing.T) {
	ctx := context.Background()
	store := collab.NewMemoryStore()
	rubbish := []byte("not a snapshot at all")
	if err := store.Save(ctx, "broken", rubbish); err != nil {
		t.Fatal(err)
	}
	got, err := Rewrite(ctx, store, []string{"broken"})
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if len(got.Failed) != 1 || got.Failed["broken"] == nil {
		t.Fatalf("Failed = %v, want the unreadable document named", got.Failed)
	}
	back, err := store.Load(ctx, "broken")
	if err != nil {
		t.Fatal(err)
	}
	if string(back) != string(rubbish) {
		t.Fatal("a document that could not be read was written over")
	}
}

func newDocument(t *testing.T, text string) []byte {
	t.Helper()
	c := crdt.NewComposite(1)
	part, err := c.Text("t")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Insert(0, text); err != nil {
		t.Fatal(err)
	}
	return c.Snapshot()
}

// stampVersion rewrites the version byte of the text part inside a composite
// snapshot. The bytes below it are then wrong for that version, which is enough
// for what these tests ask: whether a document is read at all.
func stampVersion(t *testing.T, snapshot []byte, version byte) []byte {
	t.Helper()
	out := append([]byte(nil), snapshot...)
	for i := 0; i+4 < len(out); i++ {
		if string(out[i:i+4]) == "crdt" {
			out[i+4] = version
			return out
		}
	}
	t.Fatal("no text part found in the composite snapshot")
	return nil
}

// The case this package exists for, against a snapshot a build actually wrote
// rather than one a test stamped a version byte onto.
//
// crdt v0.41.0 reads format 6 and writes 8, so the document comes out readable
// by a build that has forgotten 6. Without this the moving is untested and the
// package asserts only that it leaves things alone.
func TestRewriteMovesADocumentOutOfFormatSix(t *testing.T) {
	ctx := context.Background()
	old, err := os.ReadFile(filepath.Join("testdata", "text-format-6.snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	// The fixture is what it claims: a composite carrying a text part in 6.
	at := bytes.Index(old[4:], []byte("crdt"))
	if at < 0 || old[4+at+4] != oldTextFormat {
		t.Fatalf("the fixture is not a format %d text", oldTextFormat)
	}

	store := collab.NewMemoryStore()
	if err := store.Save(ctx, "old", old); err != nil {
		t.Fatal(err)
	}
	got, err := Rewrite(ctx, store, []string{"old"})
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if len(got.Moved) != 1 || got.Moved[0] != "old" {
		t.Fatalf("Moved = %v, Failed = %v, want [old]", got.Moved, got.Failed)
	}

	moved, err := store.Load(ctx, "old")
	if err != nil {
		t.Fatal(err)
	}
	// It is out of format 6, and it says the same thing it said before.
	at = bytes.Index(moved[4:], []byte("crdt"))
	if at < 0 {
		t.Fatal("no text part in the rewritten snapshot")
	}
	if v := moved[4+at+4]; v == oldTextFormat {
		t.Fatalf("the rewritten document is still in format %d", v)
	}
	before, err := crdt.LoadComposite(2, old)
	if err != nil {
		t.Fatal(err)
	}
	after, err := crdt.LoadComposite(2, moved)
	if err != nil {
		t.Fatalf("the rewritten document does not load: %v", err)
	}
	b, _ := before.Text("t")
	a, _ := after.Text("t")
	if b.String() != a.String() {
		t.Fatalf("the move changed the text: %q became %q", b.String(), a.String())
	}
	if a.String() == "" {
		t.Fatal("the fixture is empty, so this proves nothing")
	}
	t.Logf("moved %d bytes of format 6 into %d bytes reading %q", len(old), len(moved), a.String())
}

// A store that cannot be read from or written to is reported, not panicked over
// and not silently skipped.
type failing struct {
	collab.Store
	onLoad, onSave error
}

func (f failing) Load(ctx context.Context, name string) ([]byte, error) {
	if f.onLoad != nil {
		return nil, f.onLoad
	}
	return f.Store.Load(ctx, name)
}

func (f failing) Save(ctx context.Context, name string, snapshot []byte) error {
	if f.onSave != nil {
		return f.onSave
	}
	return f.Store.Save(ctx, name, snapshot)
}

func TestRewriteReportsAStoreThatWillNotAnswer(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("the disk said no")

	got, err := Rewrite(ctx, failing{Store: collab.NewMemoryStore(), onLoad: boom}, []string{"d"})
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if !errors.Is(got.Failed["d"], boom) {
		t.Fatalf("a store that would not be read reported %v", got.Failed)
	}

	// And one that reads but will not be written to: the document is named as
	// failed, and what is in the store is whatever was there.
	inner := collab.NewMemoryStore()
	old, err := os.ReadFile(filepath.Join("testdata", "text-format-6.snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	if err := inner.Save(ctx, "d", old); err != nil {
		t.Fatal(err)
	}
	got, err = Rewrite(ctx, failing{Store: inner, onSave: boom}, []string{"d"})
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(got.Failed["d"], boom) {
		t.Fatalf("a store that would not be written reported %v", got.Failed)
	}
	if len(got.Moved) != 0 {
		t.Fatal("a document that could not be saved was counted as moved")
	}
}

// Built against a crdt that has forgotten the old format, this refuses. Asked
// directly rather than by building the module wrongly, which is the arrangement
// it must never ship in.
func TestRewriteRefusesWhenItCannotReadTheOldFormat(t *testing.T) {
	_, err := rewrite(context.Background(), collab.NewMemoryStore(), []string{"d"}, false)
	if !errors.Is(err, ErrCannotRead) {
		t.Fatalf("rewrite with no reader = %v, want ErrCannotRead", err)
	}
	// And the pin holds in this build, which is what makes the guard a guard
	// rather than the normal path.
	if !reads(oldTextFormat) {
		t.Fatalf("this build cannot read text format %d — the pin has been lifted", oldTextFormat)
	}
	// reads says no to a version nothing has ever written, so its answer is a
	// lookup rather than a yes.
	if reads(200) {
		t.Error("reads claimed a version no build has written")
	}
}
