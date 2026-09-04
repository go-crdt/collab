//go:build !js

package collab

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// A zero-length snapshot file is what a crash between creating and writing
// leaves, and what a full disk leaves. It is not a new document, and it used
// to be read as one: the server opened an empty replica and the next save made
// the loss permanent. Now the store refuses it, the server refuses to open it,
// and the bytes on disk are left exactly as they were found.
func TestAZeroLengthSnapshotIsATornWriteNotANewDocument(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, "d", written(t, "the whole document somebody wrote")); err != nil {
		t.Fatal(err)
	}
	p, _ := store.path("d")
	if err := os.Truncate(p, 0); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Load(ctx, "d"); err == nil || !strings.Contains(err.Error(), "empty on disk") {
		t.Fatalf("Load of a zero-length file: %v, want a refusal naming the torn write", err)
	}

	// And through the server: the document cannot be opened, so nothing can
	// be saved over it.
	srv := NewServer(Config{Store: store})
	defer func() { _ = srv.Close(ctx) }()
	transport, conn := Pipe()
	go func() { _ = srv.ServePipe(ctx, conn) }()
	if _, err := Join(ctx, transport, ClientConfig{Document: "d", Site: 1}); err == nil {
		t.Fatal("the server opened a torn document as a new one")
	}
	if err := srv.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != 0 {
		t.Fatalf("the torn file was written over (%d bytes now); it must be left for a person to look at", st.Size())
	}
}

// emptyAnswering is a store that answers a present, zero-length snapshot —
// what a store that does not know the contract does for a torn file — and
// remembers whether anything was saved over it.
type emptyAnswering struct{ saved bool }

func (*emptyAnswering) Load(context.Context, string) ([]byte, error) { return []byte{}, nil }
func (s *emptyAnswering) Save(context.Context, string, []byte) error {
	s.saved = true
	return nil
}

// A zero-length hot file used to read as "not in the hot store", and the
// archive answered instead: a STALE document served as the current one, and
// written back over the hot copy at the next save.
func TestTieredDoesNotServeTheArchiveForATornHotFile(t *testing.T) {
	ctx := context.Background()
	hot, err := NewDirStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cold := NewMemoryStore()
	store := NewTiered(hot, cold)
	if err := store.Save(ctx, "d", written(t, "first")); err != nil {
		t.Fatal(err)
	}
	p, _ := hot.path("d")
	old := time.Now().Add(-48 * time.Hour)
	_ = os.Chtimes(p, old, old)
	if moved, err := store.Archive(ctx, time.Hour); err != nil || moved != 1 {
		t.Fatalf("archive: moved=%d err=%v", moved, err)
	}
	// Brought back and edited: the hot copy is now newer than the archive.
	if _, err := store.Load(ctx, "d"); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, "d", written(t, "first, then more")); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(p, 0); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load(ctx, "d")
	if err == nil {
		text, _ := bodyOf(t, got)
		t.Fatalf("a torn hot file was answered with %q — the archive's stale copy", text)
	}
	// Control: an intact hot file still wins over the archive.
	if err := hot.Save(ctx, "d", written(t, "first, then more")); err != nil {
		t.Fatal(err)
	}
	got, err = store.Load(ctx, "d")
	if err != nil {
		t.Fatal(err)
	}
	if text, _ := bodyOf(t, got); text != "first, then more" {
		t.Fatalf("control: read %q", text)
	}
}

// A hot store that does not know the contract and answers a present, empty
// snapshot for a torn file: Tiered must not read that as "not in the hot
// store" and answer with the archive's stale copy.
func TestTieredDoesNotFallThroughToTheArchiveOnAnEmptyHotAnswer(t *testing.T) {
	ctx := context.Background()
	cold := NewMemoryStore()
	if err := cold.Save(ctx, "d", written(t, "stale, from the archive")); err != nil {
		t.Fatal(err)
	}
	store := NewTiered(emptyHot{}, cold)
	got, err := store.Load(ctx, "d")
	if err == nil && len(got) > 0 {
		text, _ := bodyOf(t, got)
		t.Fatalf("the archive's stale copy %q was served for a torn hot file", text)
	}
}

// emptyHot is a hot store that answers a present, zero-length snapshot for
// everything, and holds nothing worth archiving.
type emptyHot struct{}

func (emptyHot) Load(context.Context, string) ([]byte, error)          { return []byte{}, nil }
func (emptyHot) Save(context.Context, string, []byte) error            { return nil }
func (emptyHot) Idle(context.Context, time.Duration) ([]string, error) { return nil, nil }
func (emptyHot) Release(context.Context, string, []byte) error         { return nil }

// bodyOf reads the "body" text of a snapshot [written] made.
func bodyOf(t *testing.T, snapshot []byte) (string, error) {
	t.Helper()
	doc, err := crdt.LoadComposite(1, snapshot)
	if err != nil {
		return "", err
	}
	text, err := doc.Text("body")
	if err != nil {
		return "", err
	}
	return text.String(), nil
}

// caseFolding reports whether the filesystem under dir folds case, which is
// the macOS default and NTFS. The aliasing below only exists there; elsewhere
// the test has nothing to show and says so.
func caseFolding(t *testing.T, dir string) bool {
	t.Helper()
	probe := filepath.Join(dir, "CaSe-probe")
	if err := os.WriteFile(probe, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(probe) }()
	_, err := os.Stat(filepath.Join(dir, "case-probe"))
	return err == nil
}

// Two document names can encode to file names that differ only in case —
// "aaa" and "aaG" become "YWFh" and "YWFH" — and a filesystem that folds case
// answers for one with the other. Reading would serve the wrong document;
// writing would overwrite it. Both are refused, and the first document is left
// exactly as it was.
func TestADirStoreRefusesANameTheFilesystemFoldsOntoAnother(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	if !caseFolding(t, dir) {
		t.Skip("this filesystem tells the two names apart; nothing to refuse")
	}
	store, err := NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	a, b := "aaa", "aaG"
	if ea, eb := base64.URLEncoding.EncodeToString([]byte(a)), base64.URLEncoding.EncodeToString([]byte(b)); !strings.EqualFold(ea, eb) || ea == eb {
		t.Fatalf("the two names no longer alias (%s, %s); pick another pair", ea, eb)
	}
	if err := store.Save(ctx, a, written(t, "document A")); err != nil {
		t.Fatal(err)
	}
	if got, err := store.Load(ctx, b); err == nil {
		text, _ := bodyOf(t, got)
		t.Fatalf("Load(%q) answered %q — document A served under B's name", b, text)
	} else if !strings.Contains(err.Error(), "another name") {
		t.Fatalf("Load(%q): %v, want the aliasing named", b, err)
	}
	if err := store.Save(ctx, b, written(t, "document B")); err == nil || !strings.Contains(err.Error(), "another name") {
		t.Fatalf("Save(%q): %v, want a refusal naming the aliasing", b, err)
	}
	got, err := store.Load(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	if text, _ := bodyOf(t, got); text != "document A" {
		t.Fatalf("document A now reads %q", text)
	}
	// The refused save left nothing behind.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), tempPrefix) {
			t.Fatalf("a temp file was left: %s", e.Name())
		}
	}
	// Through the server: B cannot be opened, A still can and reads its own.
	srv := NewServer(Config{Store: store})
	defer func() { _ = srv.Close(ctx) }()
	tb, cb := Pipe()
	go func() { _ = srv.ServePipe(ctx, cb) }()
	if _, err := Join(ctx, tb, ClientConfig{Document: b, Site: 7}); err == nil {
		t.Fatalf("the server opened %q on top of %q", b, a)
	}
	ta, ca := Pipe()
	go func() { _ = srv.ServePipe(ctx, ca) }()
	c, err := Join(ctx, ta, ClientConfig{Document: a, Site: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	body, _ := c.Text("body")
	if body.String() != "document A" {
		t.Fatalf("document A reads %q through the server", body.String())
	}
}

// folded is a directory entry as a case-folding filesystem would list it:
// the name on disk, which is not the name that was asked for.
type folded struct{ name string }

func (f folded) Name() string             { return f.name }
func (folded) IsDir() bool                { return false }
func (folded) Type() os.FileMode          { return 0 }
func (folded) Info() (os.FileInfo, error) { return nil, errors.New("not needed") }

// The same refusal on a filesystem that tells names apart, through the seam:
// the file exists (statFile answers) and the listing shows it under another
// name, which is exactly what a folding filesystem shows. This is what keeps
// the refusal covered on Linux, where the real-filesystem test above skips.
func TestADirStoreRefusesWhatTheListingNamesDifferently(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, "aaa", written(t, "document A")); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSites(ctx, "aaa", []byte("sites")); err != nil {
		t.Fatal(err)
	}
	real := base64.URLEncoding.EncodeToString([]byte("aaa"))
	// The same spelling with the case of the last letter swapped: what a
	// folding filesystem would show for a file written under the other.
	last := real[len(real)-1:]
	if strings.ToUpper(last) == last {
		last = strings.ToLower(last)
	} else {
		last = strings.ToUpper(last)
	}
	other := real[:len(real)-1] + last
	if other == real {
		t.Fatalf("pick a name whose encoding ends in a letter: %s", real)
	}
	was, wasStat := readDir, statFile
	defer func() { readDir, statFile = was, wasStat }()
	readDir = func(string) ([]os.DirEntry, error) { return []os.DirEntry{folded{other}}, nil }
	statFile = func(string) (os.FileInfo, error) { return nil, nil }

	if _, err := store.Load(ctx, "aaa"); err == nil || !strings.Contains(err.Error(), "another name") {
		t.Fatalf("Load: %v, want the aliasing named", err)
	}
	if err := store.Save(ctx, "aaa", written(t, "document B")); err == nil || !strings.Contains(err.Error(), "another name") {
		t.Fatalf("Save: %v, want a refusal", err)
	}
	if _, err := store.LoadSites(ctx, "aaa"); err == nil || !strings.Contains(err.Error(), "another name") {
		t.Fatalf("LoadSites: %v, want the aliasing named", err)
	}
	if err := store.Release(ctx, "aaa", written(t, "document A")); err == nil || !strings.Contains(err.Error(), "another name") {
		t.Fatalf("Release: %v, want the aliasing named", err)
	}
	// The refused save left no temp file and the document is untouched.
	readDir, statFile = was, wasStat
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), tempPrefix) {
			t.Fatalf("a temp file was left: %s", e.Name())
		}
	}
	got, err := store.Load(ctx, "aaa")
	if err != nil {
		t.Fatal(err)
	}
	if text, _ := bodyOf(t, got); text != "document A" {
		t.Fatalf("the document now reads %q", text)
	}
}
