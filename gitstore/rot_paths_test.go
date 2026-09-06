package gitstore

import (
	"io"
	"strings"
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
	"github.com/go-git/go-git/v5/plumbing"
)

// corrupted is a framed state file with one byte of its body changed, which is
// what a medium that rots leaves behind.
func corrupted(t *testing.T) []byte {
	t.Helper()
	c := crdt.NewComposite(1)
	body, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, "a commit holds what a work tree holds"); err != nil {
		t.Fatal(err)
	}
	framed := collab.CheckSnapshot(c.Snapshot())
	framed[len(framed)-3] ^= 1
	return framed
}

// rotsInCommits hands back a corrupted state file from any commit.
type rotsInCommits struct {
	repository
	bad []byte
}

func (r rotsInCommits) fileAt(plumbing.Hash, string) ([]byte, error) { return r.bad, nil }

// rotsInTheWorktree hands back a corrupted state file from the worktree.
type rotsInTheWorktree struct {
	repository
	bad []byte
}

func (r rotsInTheWorktree) open(string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(string(r.bad))), nil
}

// A revision whose state does not match its checksum is refused.
//
// [Store.At] reads the state file out of a commit, and a commit is a git object
// — so this is the path where content addressing does hold. It is checked
// anyway: the frame is the store's own answer, and a store that checks one of
// its readers and not the others has a reader that is quietly different.
func TestARevisionWhoseStateDoesNotCheckIsRefused(t *testing.T) {
	real, err := openRepo(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := newStore(real, WithClock(stamps()))
	if err := s.Save(t.Context(), "d", paper(t, 1, "one").Snapshot()); err != nil {
		t.Fatal(err)
	}
	s.repo = rotsInCommits{repository: real, bad: corrupted(t)}
	if got, err := s.At("d", "HEAD"); err == nil {
		t.Fatalf("a corrupted revision was handed back as %d bytes", len(got))
	}
}

// Neither side of a pull is merged without checking it first.
//
// A merge takes two states and writes the result over both. A side that has
// rotted would therefore not be served once and forgotten; it would be
// committed, pushed, and become the document everybody has.
func TestAPullDoesNotMergeASideThatDoesNotCheck(t *testing.T) {
	for _, c := range []struct {
		name string
		fake func(repository, []byte) repository
	}{
		{"theirs", func(r repository, bad []byte) repository { return rotsInCommits{repository: r, bad: bad} }},
		{"ours", func(r repository, bad []byte) repository { return rotsInTheWorktree{repository: r, bad: bad} }},
	} {
		t.Run(c.name, func(t *testing.T) {
			real, err := openRepo(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			s := newStore(real, WithClock(stamps()))
			if err := s.Save(t.Context(), "d", paper(t, 1, "one").Snapshot()); err != nil {
				t.Fatal(err)
			}
			head, err := real.resolve("HEAD")
			if err != nil {
				t.Fatal(err)
			}
			dir, err := dirFor("d")
			if err != nil {
				t.Fatal(err)
			}
			s.repo = c.fake(real, corrupted(t))
			if err := s.pull("d", dir, head); err == nil {
				t.Fatal("a side that does not match its checksum was merged")
			}
		})
	}
}
