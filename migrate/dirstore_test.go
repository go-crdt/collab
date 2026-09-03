package migrate

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// The whole path a person actually walks: a real store on disk, enumerated the
// way that store enumerates, then rewritten.
//
// Everything else here uses a MemoryStore, which would go on passing if the
// documented sequence -- Documents, then Rewrite -- did not work against the
// store people keep documents in. That is the same gap that let a protobuf
// conversion go untested while every unit test passed.
func TestRewriteAgainstAStoreOnDisk(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := collab.NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	old, err := os.ReadFile(filepath.Join("testdata", "text-format-6.snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, "written-long-ago", old); err != nil {
		t.Fatal(err)
	}
	// And one this build already writes, to be sure the walk does not rewrite
	// what it need not.
	c := crdt.NewComposite(1)
	part, err := c.Text("t")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Insert(0, "written today"); err != nil {
		t.Fatal(err)
	}
	current := c.Snapshot()
	if err := store.Save(ctx, "written-today", current); err != nil {
		t.Fatal(err)
	}

	// Enumerated the way a DirStore enumerates: no context, an error.
	names, err := store.Documents()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 {
		t.Fatalf("the store lists %v, want two documents", names)
	}

	got, err := Rewrite(ctx, store, names)
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if len(got.Moved) != 1 || got.Moved[0] != "written-long-ago" {
		t.Fatalf("Moved = %v, Failed = %v", got.Moved, got.Failed)
	}
	if len(got.Current) != 1 || got.Current[0] != "written-today" {
		t.Fatalf("Current = %v", got.Current)
	}

	// The old one is out of format 6 ON DISK, and says what it always said.
	//
	// The version byte is what is asserted, not that it loads. This module is
	// pinned to a crdt that reads format 6, so loading proves nothing here: it
	// would succeed just as well if Rewrite had reported a move and written
	// nothing. Breaking the save to check found exactly that.
	moved, err := store.Load(ctx, "written-long-ago")
	if err != nil {
		t.Fatal(err)
	}
	at := bytes.Index(moved[4:], []byte("crdt"))
	if at < 0 {
		t.Fatal("no text part in the stored document")
	}
	if v := moved[4+at+4]; v == oldTextFormat {
		t.Fatalf("the document on disk is still in format %d — nothing was written", v)
	}
	doc, err := crdt.LoadComposite(2, moved)
	if err != nil {
		t.Fatalf("the rewritten document does not load: %v", err)
	}
	text, err := doc.Text("t")
	if err != nil {
		t.Fatal(err)
	}
	if text.String() == "" {
		t.Fatal("the rewritten document is empty")
	}
	// The untouched one is untouched, on disk and not merely in the result.
	after, err := store.Load(ctx, "written-today")
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(current) {
		t.Fatal("a document that needed no move was rewritten on disk")
	}
	t.Logf("moved %q out of format 6 in a store at %s", text.String(), dir)
}
