package gitstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport"
)

// Every call this store makes to git can fail — a disk fills, a lock is held,
// an index is corrupt — and those are real errors, so this package carries a
// branch for each of them. A branch nothing reaches is a branch nobody checks.
//
// Reaching them through go-git means corrupting a repository in whatever way
// that version of go-git happens to mind, which pins a test to a dependency
// rather than to a property. Through the seam, a test says "staging fails" and
// means it.

// breaks names one thing a repository can be made to fail at.
type breaks string

const (
	atCreate breaks = "create"
	atOpen   breaks = "open"
	atAdd    breaks = "add"
	atClean  breaks = "clean"
	atCommit breaks = "commit"
	atHead   breaks = "head"
	atTag    breaks = "tag"
	atLog    breaks = "log"
	atFileAt breaks = "fileAt"
	atResolv breaks = "resolve"
	atDirs   breaks = "dirs"
	atWrite  breaks = "write" // the file opens and refuses the bytes

	// And the ones that need somewhere else to be reachable, which is the
	// whole reason they are here: a network is not something a test can be
	// asked to unplug.
	atPush     breaks = "push"
	atFetch    breaks = "fetch"
	atContains breaks = "contains"
	atDocsAt   breaks = "documentsAt"
	atAdopt    breaks = "adopt"
	atMerge    breaks = "mergeCommit"
)

var errBroken = errors.New("the repository said no")

// broken is a repository that works until it is asked to do the one thing it
// has been told to fail at.
type broken struct {
	repository
	at breaks
}

func (b *broken) fails(at breaks) error {
	if b.at == at {
		return fmt.Errorf("%s: %w", at, errBroken)
	}
	return nil
}

func (b *broken) create(name string) (io.WriteCloser, error) {
	if err := b.fails(atCreate); err != nil {
		return nil, err
	}
	if b.at == atWrite {
		return refusingWriter{}, nil
	}
	return b.repository.create(name)
}

func (b *broken) open(name string) (io.ReadCloser, error) {
	if err := b.fails(atOpen); err != nil {
		return nil, err
	}
	return b.repository.open(name)
}

func (b *broken) add(name string) error {
	if err := b.fails(atAdd); err != nil {
		return err
	}
	return b.repository.add(name)
}

func (b *broken) clean() (bool, error) {
	if err := b.fails(atClean); err != nil {
		return false, err
	}
	return b.repository.clean()
}

func (b *broken) commit(message string, who object.Signature) error {
	if err := b.fails(atCommit); err != nil {
		return err
	}
	return b.repository.commit(message, who)
}

func (b *broken) head() (plumbing.Hash, error) {
	if err := b.fails(atHead); err != nil {
		return plumbing.ZeroHash, err
	}
	return b.repository.head()
}

func (b *broken) tag(name string, at plumbing.Hash, tagger object.Signature, message string) error {
	if err := b.fails(atTag); err != nil {
		return err
	}
	return b.repository.tag(name, at, tagger, message)
}

func (b *broken) log(under string) ([]Revision, error) {
	if err := b.fails(atLog); err != nil {
		return nil, err
	}
	return b.repository.log(under)
}

func (b *broken) fileAt(hash plumbing.Hash, name string) ([]byte, error) {
	if err := b.fails(atFileAt); err != nil {
		return nil, err
	}
	return b.repository.fileAt(hash, name)
}

func (b *broken) resolve(revision string) (plumbing.Hash, error) {
	if err := b.fails(atResolv); err != nil {
		return plumbing.ZeroHash, err
	}
	return b.repository.resolve(revision)
}

func (b *broken) dirs() ([]string, error) {
	if err := b.fails(atDirs); err != nil {
		return nil, err
	}
	return b.repository.dirs()
}

// refusingWriter is a file that opens and will not take the bytes, which is
// what a disk filling up between the two looks like.
type refusingWriter struct{}

func (refusingWriter) Write([]byte) (int, error) { return 0, errBroken }
func (refusingWriter) Close() error              { return nil }

// storeThatBreaks returns a store whose repository fails at one thing, with a
// document already saved so that the failure is the only thing new.
func storeThatBreaks(t *testing.T, at breaks) (*Store, []byte) {
	t.Helper()
	real, err := openRepo(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := newStore(real, WithClock(stamps()))
	snapshot := paper(t, 1, "already here").Snapshot()
	if err := s.Save(t.Context(), "d", snapshot); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(t.Context(), "v1", "the first"); err != nil {
		t.Fatal(err)
	}
	s.repo = &broken{repository: real, at: at}
	return s, snapshot
}

func TestEveryWayTheRepositoryCanRefuse(t *testing.T) {
	ctx := t.Context()
	changed := paper(t, 1, "something else").Snapshot()

	cases := []struct {
		at   breaks
		what string
		do   func(s *Store) error
	}{
		{atOpen, "reading a document", func(s *Store) error { _, err := s.Load(ctx, "d"); return err }},
		{atCreate, "writing the state", func(s *Store) error { return s.Save(ctx, "d", changed) }},
		{atWrite, "filling up mid-write", func(s *Store) error { return s.Save(ctx, "d", changed) }},
		{atAdd, "staging", func(s *Store) error { return s.Save(ctx, "d", changed) }},
		{atClean, "reading the worktree", func(s *Store) error { return s.Save(ctx, "d", changed) }},
		{atCommit, "committing", func(s *Store) error { return s.Save(ctx, "d", changed) }},
		{atHead, "releasing with no head", func(s *Store) error { return s.Release(ctx, "v2", "") }},
		{atTag, "tagging", func(s *Store) error { return s.Release(ctx, "v2", "") }},
		{atLog, "reading the history", func(s *Store) error { _, err := s.History("d"); return err }},
		{atResolv, "resolving a revision", func(s *Store) error { _, err := s.At("d", "v1"); return err }},
		{atFileAt, "reading a file at a revision", func(s *Store) error { _, err := s.At("d", "v1"); return err }},
		{atDirs, "listing the repository", func(s *Store) error { _, err := s.Documents(); return err }},
		// A document that exists is checked against the listing, so that a
		// filesystem answering for another name is caught; a listing that
		// fails is reported rather than guessed around.
		{atDirs, "checking which name the filesystem answered for", func(s *Store) error { _, err := s.Load(ctx, "d"); return err }},
	}
	for _, c := range cases {
		t.Run(string(c.at), func(t *testing.T) {
			s, before := storeThatBreaks(t, c.at)
			err := c.do(s)
			if err == nil {
				t.Fatalf("%s did not fail", c.what)
			}
			if !errors.Is(err, errBroken) {
				t.Fatalf("%s failed as %v, which does not carry what went wrong", c.what, err)
			}
			if !strings.Contains(err.Error(), "gitstore:") {
				t.Errorf("the error does not say where it came from: %v", err)
			}
			// Whatever failed, the document is still a document: it holds
			// either what it held or what the save was trying to write, and
			// both are readable. A save that fails after the files are written
			// loses the commit and not the work — see Save.
			s.repo = s.repo.(*broken).repository
			raw, err := s.Load(ctx, "d")
			if err != nil {
				t.Fatalf("the document became unreadable: %v", err)
			}
			back, err := crdt.LoadComposite(2, raw)
			if err != nil {
				t.Fatalf("what is left is not a snapshot: %v", err)
			}
			text, _ := back.Text("file:paper.tex")
			if got := text.String(); got != "already here" && got != "something else" {
				t.Fatalf("the document reads %q after %s failed, which is neither "+
					"what it held nor what was being written", got, c.what)
			}
			_ = before
		})
	}
}

// Reconcile fails the same way, and leaves what was there.
func TestReconcileWhenTheRepositoryRefuses(t *testing.T) {
	s, _ := storeThatBreaks(t, atCommit)
	theirs := paper(t, 2, "from elsewhere").Snapshot()
	if err := s.Reconcile(t.Context(), "d", theirs); err == nil {
		t.Fatal("a reconcile committed against a repository that refuses to commit")
	}
}

// A reader that hands over some bytes and then breaks, which is what a file
// going away underneath a read looks like.
type halfRead struct{ read bool }

func (h *halfRead) Read(p []byte) (int, error) {
	if h.read {
		return 0, errBroken
	}
	h.read = true
	copy(p, "half")
	return 4, nil
}
func (h *halfRead) Close() error { return nil }

type breaksMidRead struct{ repository }

func (b breaksMidRead) open(string) (io.ReadCloser, error) { return &halfRead{}, nil }

// A read that fails part way through is a failure, not a short document.
func TestAReadThatBreaksPartWayThrough(t *testing.T) {
	real, err := openRepo(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := newStore(real, WithClock(stamps()))
	if err := s.Save(t.Context(), "d", paper(t, 1, "one").Snapshot()); err != nil {
		t.Fatal(err)
	}
	s.repo = breaksMidRead{repository: real}
	raw, err := s.Load(t.Context(), "d")
	if err == nil {
		t.Fatalf("a broken read handed back %q as the document", raw)
	}
	if raw != nil {
		t.Fatalf("it also handed back %d bytes", len(raw))
	}
}

// The state is written before the text, so a repository that refuses the second
// file fails on one of the rendered ones.
type breaksOnTheSecondFile struct {
	repository
	seen int
}

func (b *breaksOnTheSecondFile) create(name string) (io.WriteCloser, error) {
	b.seen++
	if b.seen > 1 {
		return nil, errBroken
	}
	return b.repository.create(name)
}

func TestAFileThatCannotBeWritten(t *testing.T) {
	real, err := openRepo(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := newStore(real, WithClock(stamps()))
	s.repo = &breaksOnTheSecondFile{repository: real}
	if err := s.Save(t.Context(), "d", paper(t, 1, "one").Snapshot()); err == nil {
		t.Fatal("a save whose text could not be written succeeded")
	}
}

// nameOf reads back only what dirFor wrote. A directory that is merely
// percent-shaped is somebody else's.
func TestWhatIsNotADocumentDirectory(t *testing.T) {
	for _, dir := range []string{
		"%",         // an escape with nothing after it
		"%A",        // an escape with one digit
		"%ZZname",   // not hexadecimal
		"%2e",       // lower case: dirFor writes %2E, so this is not its work
		"%41",       // "A", which dirFor would have written as A
		"plain/sub", // a separator, which dirFor encodes
	} {
		if name, ok := nameOf(dir); ok {
			t.Errorf("%q was read as the document %q", dir, name)
		}
	}
	// And what it does write round-trips, legibly.
	for _, name := range []string{"project:default", "a b", "é", ".hidden", "plain"} {
		dir, err := dirFor(name)
		if err != nil {
			t.Fatal(err)
		}
		back, ok := nameOf(dir)
		if !ok || back != name {
			t.Errorf("%q -> %q -> %q, %v", name, dir, back, ok)
		}
		t.Logf("%-18q -> %s", name, dir)
	}
}

// A reader that returns nothing and no error. io.Reader allows it and says it
// means nothing happened; this must not spin on it.
type saysNothing struct{ repository }

type nothingReader struct{}

func (nothingReader) Read([]byte) (int, error) { return 0, nil }
func (nothingReader) Close() error             { return nil }

func (s saysNothing) open(string) (io.ReadCloser, error) { return nothingReader{}, nil }

func TestAReaderThatSaysNothing(t *testing.T) {
	real, err := openRepo(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := newStore(real, WithClock(stamps()))
	if err := s.Save(t.Context(), "d", paper(t, 1, "one").Snapshot()); err != nil {
		t.Fatal(err)
	}
	s.repo = saysNothing{repository: real}

	done := make(chan []byte, 1)
	go func() { raw, _ := s.Load(t.Context(), "d"); done <- raw }()
	select {
	case raw := <-done:
		if len(raw) != 0 {
			t.Fatalf("it invented %d bytes", len(raw))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Load spun on a reader that says nothing")
	}
}

func (b *broken) push(ctx context.Context, url string, auth transport.AuthMethod) error {
	if err := b.fails(atPush); err != nil {
		return err
	}
	return b.repository.push(ctx, url, auth)
}

func (b *broken) fetch(ctx context.Context, url string, auth transport.AuthMethod) (plumbing.Hash, error) {
	if err := b.fails(atFetch); err != nil {
		return plumbing.ZeroHash, err
	}
	return b.repository.fetch(ctx, url, auth)
}

func (b *broken) contains(hash plumbing.Hash) (bool, error) {
	if err := b.fails(atContains); err != nil {
		return false, err
	}
	return b.repository.contains(hash)
}

func (b *broken) documentsAt(hash plumbing.Hash) ([]string, error) {
	if err := b.fails(atDocsAt); err != nil {
		return nil, err
	}
	return b.repository.documentsAt(hash)
}

func (b *broken) adopt(from plumbing.Hash) error {
	if err := b.fails(atAdopt); err != nil {
		return err
	}
	return b.repository.adopt(from)
}

func (b *broken) mergeCommit(message string, who object.Signature, other plumbing.Hash) error {
	if err := b.fails(atMerge); err != nil {
		return err
	}
	return b.repository.mergeCommit(message, who, other)
}
