//go:build !js

package collab

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io/fs"
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

// emptyCold is an archive whose answer for a document it does not have is a
// zero-length slice rather than nil -- a store's own refusal, a torn file, or
// simply a different idea of "missing". It records every write it is given.
type emptyCold struct {
	loads  int
	stored map[string][]byte
}

func (c *emptyCold) Load(context.Context, string) ([]byte, error) {
	c.loads++
	return []byte{}, nil
}

func (c *emptyCold) Save(_ context.Context, document string, snapshot []byte) error {
	if c.stored == nil {
		c.stored = map[string][]byte{}
	}
	c.stored[document] = snapshot
	return nil
}

// countingHot is a hot store that says what it was asked to write.
type countingHot struct {
	*MemoryStore
	writes []string
}

func (h *countingHot) Save(ctx context.Context, document string, snapshot []byte) error {
	h.writes = append(h.writes, document)
	return h.MemoryStore.Save(ctx, document, snapshot)
}

// ⛔ A zero-length answer from the ARCHIVE must not be brought back.
//
// tiered.go:126, `if len(cold) == 0 { return nil, nil }`, survived a mutation
// sweep of the root package's remaining 32 refusals on 2026-10-17. Deleting it
// falls through to the line that brings a document back -- so an archive which
// answers "I do not have it" with an empty slice makes Tiered write an EMPTY
// snapshot into the hot store, for a document nobody ever had. From then on
// the hot store answers non-nil for it, the first branch of Load returns that,
// and the document can never be opened: the archive is no longer consulted.
//
// The nil-versus-empty reasoning is spelled out two branches above, for the
// hot store. The same distinction on the cold side was held by nothing.
func TestAnEmptyAnswerFromTheArchiveIsNotBroughtBack(t *testing.T) {
	ctx := context.Background()
	hot := &countingHot{MemoryStore: NewMemoryStore()}
	cold := &emptyCold{}
	store := NewTiered(hot, cold)

	got, err := store.Load(ctx, "never-existed")
	if err != nil {
		t.Fatalf("an archive that does not have the document returned an error: %v", err)
	}
	// The control: the archive WAS asked, so the nil below is about what it
	// said and not about a hot store that answered first.
	if cold.loads != 1 {
		t.Fatalf("the archive was consulted %d times, want 1", cold.loads)
	}
	if got != nil {
		t.Errorf("Load returned a %d-byte snapshot for a document nothing holds; nil is how a store says "+
			"\"new document\", and anything else makes the server open and save an empty one", len(got))
	}
	if len(hot.writes) != 0 {
		t.Errorf("an empty snapshot was written to the hot store for %v: every later Load would then "+
			"answer non-nil for a document that does not exist, and the archive would never be asked again",
			hot.writes)
	}

	// And the whole way round, as the control for the control: a real archived
	// document IS brought back, so the test above is about emptiness rather
	// than about Tiered never promoting anything.
	full := NewTiered(&countingHot{MemoryStore: NewMemoryStore()}, NewMemoryStore())
	if err := full.cold.Save(ctx, "archived", []byte("what somebody wrote")); err != nil {
		t.Fatal(err)
	}
	switch back, err := full.Load(ctx, "archived"); {
	case err != nil:
		t.Fatal(err)
	case !bytes.Equal(back, []byte("what somebody wrote")):
		t.Fatalf("an archived document came back as %q", back)
	}
	if hot := full.hot.(*countingHot); len(hot.writes) != 1 {
		t.Errorf("a real document was promoted %d times, want 1", len(hot.writes))
	}
}

// ⛔ A CORRUPT HOT COPY MUST BE REPORTED, NOT READ AS "SOMEBODY ELSE WROTE IT".
//
// dirstore.go:322, the UnpackSnapshot error check in DirStore.Release,
// survived a mutation sweep of dirstore.go, dircompress.go and durability.go
// on 2026-10-17 -- 39 refusals, three survivors. Deleting it is not a quiet
// loss of a message: `unpacked` is then nil, stillHolds compares nil against
// what the archiver asked to release, and the answer is ErrChanged.
//
// ErrChanged is the one error Tiered.Archive treats as BENIGN. Its own comment
// says why -- "somebody wrote it while it was being copied… releasing now
// would delete the newer one" -- so it `continue`s, silently. A file whose
// checksum does not match would therefore be skipped with no failure
// reported, for as long as it stays quiet, and the only thing that knew it was
// corrupt would have thrown the knowledge away.
//
// The guard's own comment states the rule it keeps: "Unreadable is not 'holds
// what you expected', and it is certainly not a reason to remove it."
//
// Reached through Archive rather than through Release, because Archive is what
// decides whether anybody hears about it: the second read is the one that is
// corrupted, which is also the real shape of the hazard -- a file that was
// fine when it was copied and is not when it is released.
func TestACorruptHotCopyIsReportedRatherThanTakenForAChange(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	hot, err := NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	cold := NewMemoryStore()
	store := NewTiered(hot, cold)

	if err := store.Save(ctx, "d", []byte("what somebody wrote")); err != nil {
		t.Fatal(err)
	}

	// The control FIRST: with nothing corrupted, this document archives and is
	// counted. Without it, "one failure reported" below could be a Tiered that
	// never archives anything.
	switch moved, err := store.Archive(ctx, 0); {
	case err != nil:
		t.Fatalf("archiving an intact document: %v", err)
	case moved != 1:
		t.Fatalf("%d documents archived, want 1", moved)
	}
	if back, err := cold.Load(ctx, "d"); err != nil || string(back) != "what somebody wrote" {
		t.Fatalf("the archive holds %q, %v", back, err)
	}

	// Now the same again, with the file corrupting itself between the read that
	// copies it and the read that releases it.
	if err := store.Save(ctx, "d2", []byte("the second one")); err != nil {
		t.Fatal(err)
	}
	var reads int
	realRead := readFile
	readFile = func(path string) ([]byte, error) {
		reads++
		raw, err := realRead(path)
		if err != nil || reads < 2 {
			return raw, err
		}
		// Flip the last byte, which the checksum covers.
		bad := append([]byte(nil), raw...)
		bad[len(bad)-1] ^= 0xff
		return bad, nil
	}
	t.Cleanup(func() { readFile = realRead })

	removed := 0
	realRemove := removeFile
	removeFile = func(path string) error {
		removed++
		return realRemove(path)
	}
	t.Cleanup(func() { removeFile = realRemove })

	moved, err := store.Archive(ctx, 0)
	if err == nil {
		t.Fatalf("a corrupt hot copy was archived silently: moved=%d", moved)
	}
	// ⛔ What is asserted is that the error is NOT the benign one and that it
	// names the document — not its wording. The first version of this test
	// demanded the words "corrupted" or "checksum", and failed on
	//
	//	collab: releasing "d2" after archiving it: collab: reading document
	//	"d2": collab: reading a compressed document: unexpected EOF
	//
	// which is the truth: a flipped byte breaks the DECOMPRESSION before the
	// checksum is ever compared, so insisting on the checksum's words would
	// assert which layer happens to notice first.
	if errors.Is(err, ErrChanged) {
		t.Errorf("a corrupt document was reported as one that CHANGED, which Archive treats as benign: %v", err)
	}
	if !strings.Contains(err.Error(), "d2") {
		t.Errorf("the failure does not say which document: %v", err)
	}
	if removed != 0 {
		t.Errorf("the corrupt file was removed %d time(s): unreadable is not a reason to delete it", removed)
	}
	// And it is still there to be looked at.
	if _, err := readDir(dir); err != nil {
		t.Fatal(err)
	}
}

// ⛔ A FILE THAT CANNOT BE READ IS NOT A TORN WRITE.
//
// dirstore.go:144, the read error check in DirStore.Load, also survived that
// sweep, and the reason is that the two guards below it catch every case:
// zero bytes read reaches "empty on disk, which is a torn write", and a
// partial read reaches UnpackSnapshot and fails its checksum. The error is
// reported either way.
//
// What is lost is which error. A permission problem, a failing disk or a file
// held by something else would be reported as a TORN WRITE -- sending whoever
// reads it to look at the last crash instead of at the mode bits, and the
// message is the whole of what they have. So this holds the diagnosis rather
// than the outcome, and says so.
func TestAFileThatCannotBeReadIsNotReportedAsATornWrite(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, "d", []byte("what somebody wrote")); err != nil {
		t.Fatal(err)
	}
	// The control: it loads before anything is broken.
	if got, err := store.Load(ctx, "d"); err != nil || string(got) != "what somebody wrote" {
		t.Fatalf("Load = %q, %v", got, err)
	}

	realRead := readFile
	readFile = func(string) ([]byte, error) { return nil, fs.ErrPermission }
	t.Cleanup(func() { readFile = realRead })

	_, err = store.Load(ctx, "d")
	if err == nil {
		t.Fatal("a file that could not be read loaded anyway")
	}
	if !errors.Is(err, fs.ErrPermission) {
		t.Errorf("the read failure was not carried through: %v", err)
	}
	if strings.Contains(err.Error(), "torn write") {
		t.Errorf("a permission problem was reported as a torn write, which sends the reader to the "+
			"wrong place entirely: %v", err)
	}
}
