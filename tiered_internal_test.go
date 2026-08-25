//go:build !js

package collab

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A DirStore archives and comes back the same as one in memory does, which is
// the half of [Archivable] that has a filesystem underneath it.
func TestADirStoreArchivesByHowLongAgoAFileWasWritten(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	hot, err := NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	cold := NewMemoryStore()
	store := NewTiered(hot, cold)

	kept := []byte("what somebody wrote")
	if err := store.Save(ctx, "d", kept); err != nil {
		t.Fatal(err)
	}
	// Written a moment ago, so nothing is idle.
	switch moved, err := store.Archive(ctx, time.Hour); {
	case err != nil:
		t.Fatal(err)
	case moved != 0:
		t.Fatalf("%d fresh documents were archived", moved)
	}

	// Age the file rather than the clock: a DirStore reads the filesystem's
	// idea of when a document was last written, so that is what has to move.
	path, err := hot.path("d")
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	switch moved, err := store.Archive(ctx, 24*time.Hour); {
	case err != nil:
		t.Fatal(err)
	case moved != 1:
		t.Fatalf("archived %d documents, want 1", moved)
	}
	if got, _ := hot.Load(ctx, "d"); got != nil {
		t.Fatal("the file is still there")
	}
	got, err := store.Load(ctx, "d")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, kept) {
		t.Fatalf("an archived document read back as %q", got)
	}
}

// Every way a DirStore can fail to release a document, and none of them is a
// removal.
func TestEveryWayADirStoreRefusesToRelease(t *testing.T) {
	ctx := context.Background()
	store, err := NewDirStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, "d", []byte("what is there")); err != nil {
		t.Fatal(err)
	}

	t.Run("a document with no name", func(t *testing.T) {
		if err := store.Release(ctx, "", []byte("anything")); !errors.Is(err, ErrNoDocument) {
			t.Fatalf("releasing a nameless document gave %v", err)
		}
	})

	t.Run("a file that will not open", func(t *testing.T) {
		restore := readFile
		readFile = func(string) ([]byte, error) { return nil, errors.New("it will not open") }
		defer func() { readFile = restore }()
		if err := store.Release(ctx, "d", []byte("what is there")); err == nil {
			t.Fatal("a file that could not be read was released anyway")
		} else if !strings.Contains(err.Error(), "reading document") {
			t.Fatalf("the error does not say where it happened: %v", err)
		}
	})

	t.Run("a file that will not decompress", func(t *testing.T) {
		restore := readFile
		readFile = func(string) ([]byte, error) {
			return append(append([]byte(nil), checkedMagic[:]...), 1, 2, 3, 4, 5, 6, 7, 8), nil
		}
		defer func() { readFile = restore }()
		// Unreadable is not "holds what you expected", and it is certainly not
		// a reason to remove it.
		if err := store.Release(ctx, "d", []byte("what is there")); err == nil {
			t.Fatal("an unreadable document was released")
		}
	})

	t.Run("a file that will not go", func(t *testing.T) {
		restore := removeFile
		removeFile = func(string) error { return errors.New("it will not go") }
		defer func() { removeFile = restore }()
		if err := store.Release(ctx, "d", []byte("what is there")); err == nil {
			t.Fatal("a file that could not be removed reported success")
		} else if !strings.Contains(err.Error(), "releasing document") {
			t.Fatalf("the error does not say where it happened: %v", err)
		}
	})

	t.Run("and after all that it is still there", func(t *testing.T) {
		if got, _ := store.Load(ctx, "d"); !bytes.Equal(got, []byte("what is there")) {
			t.Fatalf("the document reads %q", got)
		}
	})
}

func TestADirStoreCannotListWhatItCannotRead(t *testing.T) {
	store, err := NewDirStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	restore := readDir
	readDir = func(string) ([]os.DirEntry, error) { return nil, errors.New("it will not open") }
	defer func() { readDir = restore }()
	if _, err := store.Idle(context.Background(), time.Hour); err == nil {
		t.Fatal("a directory that could not be read listed documents anyway")
	}
}

// Bringing an archived document back to the hot store is a shortcut for next
// time, not the answer to this read: a hot store that will not take it still
// gives the reader their document.
func TestAnArchivedDocumentIsReturnedEvenIfItCannotBeBroughtBack(t *testing.T) {
	ctx := context.Background()
	cold := NewMemoryStore()
	kept := []byte("what somebody wrote")
	if err := cold.Save(ctx, "d", kept); err != nil {
		t.Fatal(err)
	}
	got, err := NewTiered(&unwritableHot{MemoryStore: NewMemoryStore()}, cold).Load(ctx, "d")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, kept) {
		t.Fatalf("the reader got %q", got)
	}
}

// unwritableHot loads but will not take a document back.
type unwritableHot struct{ *MemoryStore }

func (u *unwritableHot) Save(context.Context, string, []byte) error {
	return errors.New("no space left on device")
}

// Every way archiving can fail, and what it does with each.
func TestEveryWayArchivingCanFail(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")

	t.Run("the hot store will not say what is idle", func(t *testing.T) {
		_, err := NewTiered(hotRefusing{boom}, NewMemoryStore()).Archive(ctx, time.Hour)
		if !errors.Is(err, boom) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("a document that will not be read is skipped, not released", func(t *testing.T) {
		inner, wind := aged(t)
		hot := &unreadableOnce{MemoryStore: inner, name: "d", err: boom}
		if err := hot.MemoryStore.Save(ctx, "d", []byte("x")); err != nil {
			t.Fatal(err)
		}
		wind(2 * time.Hour)
		moved, err := NewTiered(hot, NewMemoryStore()).Archive(ctx, time.Hour)
		if moved != 0 || !errors.Is(err, boom) {
			t.Fatalf("moved %d, err %v", moved, err)
		}
	})

	t.Run("a document that is already gone is not counted", func(t *testing.T) {
		hot, wind := aged(t)
		if err := hot.Save(ctx, "d", []byte("x")); err != nil {
			t.Fatal(err)
		}
		wind(2 * time.Hour)
		// Somebody else archived it between the listing and the read.
		if err := hot.Release(ctx, "d", []byte("x")); err != nil {
			t.Fatal(err)
		}
		moved, err := NewTiered(hot, NewMemoryStore()).Archive(ctx, time.Hour)
		if moved != 0 || err != nil {
			t.Fatalf("moved %d, err %v", moved, err)
		}
	})

	t.Run("a release that fails for its own reasons is reported", func(t *testing.T) {
		inner, wind := aged(t)
		hot := &unreleasable{MemoryStore: inner, err: boom}
		if err := hot.MemoryStore.Save(ctx, "d", []byte("x")); err != nil {
			t.Fatal(err)
		}
		wind(2 * time.Hour)
		moved, err := NewTiered(hot, NewMemoryStore()).Archive(ctx, time.Hour)
		if moved != 0 || !errors.Is(err, boom) {
			t.Fatalf("moved %d, err %v", moved, err)
		}
	})
}

type unreadableOnce struct {
	*MemoryStore
	name string
	err  error
}

func (u *unreadableOnce) Load(ctx context.Context, document string) ([]byte, error) {
	if document == u.name {
		return nil, u.err
	}
	return u.MemoryStore.Load(ctx, document)
}

type unreleasable struct {
	*MemoryStore
	err error
}

func (u *unreleasable) Release(context.Context, string, []byte) error { return u.err }

// The ordinary read: the hot store has it, and the archive is not asked.
func TestAHotDocumentIsReadWithoutAskingTheArchive(t *testing.T) {
	ctx := context.Background()
	hot := NewMemoryStore()
	kept := []byte("what somebody is writing right now")
	if err := hot.Save(ctx, "d", kept); err != nil {
		t.Fatal(err)
	}
	// An archive that would fail if it were asked, so that "not asked" is
	// asserted rather than assumed.
	got, err := NewTiered(hot, refusingStore{err: errors.New("the archive was asked")}).Load(ctx, "d")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, kept) {
		t.Fatalf("read back %q", got)
	}
}

// A document listed as idle that is gone by the time it is read is skipped
// rather than counted, which is what two archivers passing each other looks
// like.
func TestADocumentThatVanishesBetweenTheListingAndTheReadIsSkipped(t *testing.T) {
	ctx := context.Background()
	hot := &vanishing{MemoryStore: NewMemoryStore(), name: "d"}
	moved, err := NewTiered(hot, NewMemoryStore()).Archive(ctx, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 0 {
		t.Fatalf("%d documents that were not there were archived", moved)
	}
}

// vanishing lists a document it does not have.
type vanishing struct {
	*MemoryStore
	name string
}

func (v *vanishing) Idle(context.Context, time.Duration) ([]string, error) {
	return []string{v.name}, nil
}

// A directory holds things that are not documents, and Idle steps over them.
func TestIdleStepsOverWhatIsNotADocument(t *testing.T) {
	dir := t.TempDir()
	store, err := NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "a-directory"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, tempPrefix+"half-written"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "not base64 at all!"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	// All of them old enough to be picked up if anything were listening.
	old := time.Now().Add(-48 * time.Hour)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := os.Chtimes(filepath.Join(dir, e.Name()), old, old); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.Idle(context.Background(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("Idle returned %q, and none of those is a document", got)
	}
}

// A file that goes away while the directory is being read is not a document
// this pass can say anything about.
func TestIdleStepsOverAFileThatWentAwayWhileItWasBeingListed(t *testing.T) {
	store, err := NewDirStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	restore := readDir
	readDir = func(string) ([]os.DirEntry, error) { return []os.DirEntry{gone{}}, nil }
	defer func() { readDir = restore }()

	got, err := store.Idle(context.Background(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("Idle returned %q for a file it could not stat", got)
	}
}

// gone is a directory entry whose file is no longer there.
type gone struct{}

func (gone) Name() string      { return base64.URLEncoding.EncodeToString([]byte("d")) }
func (gone) IsDir() bool       { return false }
func (gone) Type() os.FileMode { return 0 }
func (gone) Info() (os.FileInfo, error) {
	return nil, errors.New("it is not there any more")
}
