//go:build !js

package collab_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-crdt/collab"
)

// A document name is arbitrary UTF-8 and is expected to carry structure, so the
// names a real consumer uses are not file names. These are the ones that would
// have collided, escaped into each other, or escaped the directory.
func TestADocumentNameIsNotAFileName(t *testing.T) {
	dir := t.TempDir()
	store, err := collab.NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	names := []string{
		"project:default",
		"project:ods:chapitre un.ods",
		"file:src/main.tex",
		"../escape",
		"..",
		".",
		"a\\b",
		"CON",          // reserved on one of the systems this is built for
		"un thé glacé", // and a name that is only awkward to look at
		"emoji:😀",
		strings.Repeat("deep/", 20) + "leaf",
	}
	for _, name := range names {
		if err := store.Save(t.Context(), name, []byte(name+"!")); err != nil {
			t.Fatalf("Save(%q): %v", name, err)
		}
	}

	// A document with no name is refused rather than allowed to name the
	// directory itself, which is what the empty name encodes to. The server
	// refuses one at the door for the same reason.
	if err := store.Save(t.Context(), "", []byte("x")); !errors.Is(err, collab.ErrNoDocument) {
		t.Fatalf("Save with no name = %v, want ErrNoDocument", err)
	}
	if _, err := store.Load(t.Context(), ""); !errors.Is(err, collab.ErrNoDocument) {
		t.Fatalf("Load with no name = %v, want ErrNoDocument", err)
	}

	// Every one of them reads back as itself: no two of them share a file.
	for _, name := range names {
		got, err := store.Load(t.Context(), name)
		if err != nil {
			t.Fatalf("Load(%q): %v", name, err)
		}
		if want := name + "!"; string(got) != want {
			t.Fatalf("Load(%q) = %q, want %q", name, got, want)
		}
	}

	// And nothing was written outside the directory, which is what "../escape"
	// was there to check.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(names) {
		t.Fatalf("%d documents left %d files", len(names), len(entries))
	}
	for _, e := range entries {
		if e.IsDir() || strings.ContainsAny(e.Name(), `/\:.`) {
			t.Fatalf("a document was stored as %q", e.Name())
		}
	}

	// Documents says what the file names cannot.
	held, err := store.Documents()
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(held)
	want := slices.Clone(names)
	slices.Sort(want)
	if !slices.Equal(held, want) {
		t.Fatalf("Documents() = %q, want %q", held, want)
	}
}

func TestADocumentNobodyHasSavedIsNotAnError(t *testing.T) {
	store, err := collab.NewDirStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Load(t.Context(), "never written")
	if err != nil {
		t.Fatalf("Load of a new document = %v, want nil", err)
	}
	if got != nil {
		t.Fatalf("Load of a new document returned %d bytes", len(got))
	}
	held, err := store.Documents()
	if err != nil {
		t.Fatal(err)
	}
	if len(held) != 0 {
		t.Fatalf("an empty store holds %q", held)
	}
}

// A reader sees one whole version or the one before it, never half of either.
func TestASaveReplacesRatherThanRewrites(t *testing.T) {
	dir := t.TempDir()
	store, err := collab.NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	first := bytes.Repeat([]byte("a"), 4096)
	if err := store.Save(t.Context(), "doc", first); err != nil {
		t.Fatal(err)
	}
	// A save that is interrupted cannot be staged here, so what is checked is
	// the property that makes it safe: the file the reader opens is never the
	// file being written.
	before, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	second := bytes.Repeat([]byte("b"), 8192)
	if err := store.Save(t.Context(), "doc", second); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 1 || len(after) != 1 {
		t.Fatalf("saving twice left %d then %d files", len(before), len(after))
	}
	if before[0].Name() != after[0].Name() {
		t.Fatalf("the document moved from %q to %q", before[0].Name(), after[0].Name())
	}
	got, err := store.Load(t.Context(), "doc")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, second) {
		t.Fatalf("Load returned %d bytes, want the %d of the second save", len(got), len(second))
	}
}

// A crash during a save leaves a temporary file behind and the previous
// snapshot intact. Opening the directory again clears the one and keeps the
// other.
func TestOpeningADirectoryClearsUpAfterACrash(t *testing.T) {
	dir := t.TempDir()
	store, err := collab.NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(t.Context(), "doc", []byte("what was there")); err != nil {
		t.Fatal(err)
	}
	// What a save interrupted between writing and renaming leaves.
	leftover := filepath.Join(dir, ".collab-half-written")
	if err := os.WriteFile(leftover, []byte("half"), 0o600); err != nil {
		t.Fatal(err)
	}

	again, err := collab.NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(leftover); !os.IsNotExist(err) {
		t.Fatalf("the half-written file is still there: %v", err)
	}
	got, err := again.Load(t.Context(), "doc")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "what was there" {
		t.Fatalf("the previous snapshot is %q", got)
	}
	// And a half-written file is never mistaken for a document.
	held, err := again.Documents()
	if err != nil {
		t.Fatal(err)
	}
	if len(held) != 1 || held[0] != "doc" {
		t.Fatalf("Documents() = %q", held)
	}
}

// A directory shared with anything else is not a reason to fail: a file this
// store did not write is skipped rather than reported.
func TestSomethingElseInTheDirectoryIsIgnored(t *testing.T) {
	dir := t.TempDir()
	store, err := collab.NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(t.Context(), "doc", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README"), []byte("not ours"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "subdir"), 0o700); err != nil {
		t.Fatal(err)
	}
	held, err := store.Documents()
	if err != nil {
		t.Fatal(err)
	}
	if len(held) != 1 || held[0] != "doc" {
		t.Fatalf("Documents() = %q, want just the one document", held)
	}
}

func TestADirStoreSaysWhatItCannotDo(t *testing.T) {
	if _, err := collab.NewDirStore(""); err == nil {
		t.Error("a store with no directory was accepted")
	}
	// A directory that cannot be made, because a file is in its way.
	dir := t.TempDir()
	inTheWay := filepath.Join(dir, "file")
	if err := os.WriteFile(inTheWay, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := collab.NewDirStore(filepath.Join(inTheWay, "under")); err == nil {
		t.Error("a directory that cannot exist was accepted")
	}

	store, err := collab.NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	// A document whose file cannot be read is reported rather than treated as
	// new, because treating it as new would quietly start it again from empty.
	unreadable := t.TempDir()
	blocked, err := collab.NewDirStore(unreadable)
	if err != nil {
		t.Fatal(err)
	}
	if err := blocked.Save(t.Context(), "doc", []byte("x")); err != nil {
		t.Fatal(err)
	}
	held, err := blocked.Documents()
	if err != nil || len(held) != 1 {
		t.Fatalf("Documents() = %q, %v", held, err)
	}
	name := filepath.Join(unreadable, "ZG9j") // base64 of "doc"
	if err := os.Chmod(name, 0o000); err != nil {
		t.Skip("this filesystem does not enforce permissions")
	}
	t.Cleanup(func() { _ = os.Chmod(name, 0o600) })
	if _, err := blocked.Load(t.Context(), "doc"); err == nil {
		t.Error("a document that cannot be read was reported as new")
	}

	// And a store whose directory has gone reports it rather than an empty list.
	gone := filepath.Join(dir, "gone")
	away, err := collab.NewDirStore(gone)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(gone); err != nil {
		t.Fatal(err)
	}
	if _, err := away.Documents(); err == nil {
		t.Error("a directory that is not there reported no documents")
	}
	if err := away.Save(t.Context(), "doc", []byte("x")); err == nil {
		t.Error("saving into a directory that is not there succeeded")
	}
	_ = store
}
