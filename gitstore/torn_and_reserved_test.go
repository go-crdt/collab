package gitstore

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

func statePathOf(s *Store, document string) string {
	dir, _ := dirFor(document)
	return filepath.Join(s.repo.root(), dir, stateFile)
}

func paperText(t *testing.T, raw []byte) string {
	t.Helper()
	doc, err := crdt.LoadComposite(99, raw)
	if err != nil {
		t.Fatal(err)
	}
	text, err := doc.Text("file:paper.tex")
	if err != nil {
		t.Fatal(err)
	}
	return text.String()
}

// The state file is created with O_TRUNC and written in place, so a crash
// between the two leaves a zero-length file. That is a torn write, not a new
// document, and it used to be read as one: the next save COMMITTED an empty
// document over the history.
func TestAZeroLengthStateIsATornWriteNotANewDocument(t *testing.T) {
	ctx := context.Background()
	s := store(t)
	if err := s.Save(ctx, "d", paper(t, 1, "the whole paper").Snapshot()); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(statePathOf(s, "d"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(ctx, "d"); err == nil || !strings.Contains(err.Error(), "torn write") {
		t.Fatalf("Load: %v, want the torn write named", err)
	}
	srv := collab.NewServer(collab.Config{Store: s})
	transport, conn := collab.Pipe()
	go func() { _ = srv.ServePipe(ctx, conn) }()
	if _, err := collab.Join(ctx, transport, collab.ClientConfig{Document: "d", Site: 5}); err == nil {
		t.Fatal("the server opened a torn document as a new one")
	}
	if err := srv.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// History is untouched: HEAD still reads the whole paper.
	head, err := s.At("d", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if got := paperText(t, head); got != "the whole paper" {
		t.Fatalf("HEAD reads %q", got)
	}
}

// The rendered files share the document's directory with the snapshot and
// with git's own metadata, and a part name reaches the store from ANY
// participant. Names that would land on either are not rendered at all.
func TestAPartCannotBeRenderedOverTheSnapshotOrGit(t *testing.T) {
	for _, name := range []string{
		"file:state.crdt", "file:../state.crdt", "file:x/../state.crdt", "file:STATE.CRDT",
		"file:state.crdt/inside", "file:.git", "file:.git/config", "file:.GIT/HEAD",
	} {
		if at, ok := defaultFileFor(crdt.Part{Kind: crdt.PartText, Name: name}); ok {
			t.Errorf("%s would be rendered at %q", name, at)
		}
	}
	// And the control: an ordinary name still renders.
	if at, ok := defaultFileFor(crdt.Part{Kind: crdt.PartText, Name: "file:notes/state.crdt.bak"}); !ok || at != "notes/state.crdt.bak" {
		t.Fatalf("an ordinary name was refused: %q %v", at, ok)
	}

	// Through a server with no authorisation at all, which is the default.
	ctx := context.Background()
	s := store(t)
	if err := s.Save(ctx, "d", paper(t, 1, "the whole paper").Snapshot()); err != nil {
		t.Fatal(err)
	}
	srv := collab.NewServer(collab.Config{Store: s})
	transport, conn := collab.Pipe()
	go func() { _ = srv.ServePipe(ctx, conn) }()
	c, err := collab.Join(ctx, transport, collab.ClientConfig{Document: "d", Site: 5})
	if err != nil {
		t.Fatal(err)
	}
	evil, err := c.Text("file:../state.crdt")
	if err != nil {
		t.Fatal(err)
	}
	if err := evil.Insert(0, "this is not a snapshot"); err != nil {
		t.Fatal(err)
	}
	// A second participant is the barrier that the server holds the write.
	t2, c2 := collab.Pipe()
	go func() { _ = srv.ServePipe(ctx, c2) }()
	w, err := collab.Join(ctx, t2, collab.ClientConfig{Document: "d", Site: 6})
	if err != nil {
		t.Fatal(err)
	}
	wb, _ := w.Text("file:../state.crdt")
	for range 5000 {
		if wb.String() == "this is not a snapshot" {
			break
		}
		<-w.Changes()
	}
	_ = w.Close()
	_ = c.Close()
	if err := srv.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// The snapshot is still a snapshot, and the next server opens it.
	raw, err := s.Load(ctx, "d")
	if err != nil {
		t.Fatal(err)
	}
	if got := paperText(t, raw); got != "the whole paper" {
		t.Fatalf("the paper now reads %q", got)
	}
	srv2 := collab.NewServer(collab.Config{Store: s})
	t3, c3 := collab.Pipe()
	go func() { _ = srv2.ServePipe(ctx, c3) }()
	if _, err := collab.Join(ctx, t3, collab.ClientConfig{Document: "d", Site: 7}); err != nil {
		t.Fatalf("the document cannot be opened any more: %v", err)
	}
	_ = srv2.Close(ctx)
}

// Names are percent-encoded so that a person can read them, which keeps
// case, and a filesystem that folds case — the macOS default and NTFS —
// answers for "doc" with "Doc". Serving that would hand one document's history
// to another; writing it would overwrite it. Both are refused.
func TestANameTheFilesystemFoldsOntoAnotherDocumentIsRefused(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	probe := filepath.Join(dir, "CaSe")
	if err := os.WriteFile(probe, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "case")); err != nil {
		t.Skip("this filesystem tells the two names apart; nothing to refuse")
	}
	_ = os.Remove(probe)
	s, err := New(dir, WithClock(stamps()))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, "Doc", paper(t, 1, "upper").Snapshot()); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Load(ctx, "doc"); err == nil {
		t.Fatalf("Load(\"doc\") answered %q — \"Doc\" served under another name", paperText(t, got))
	} else if !strings.Contains(err.Error(), "another name") {
		t.Fatalf("Load(\"doc\"): %v, want the aliasing named", err)
	}
	if err := s.Save(ctx, "doc", paper(t, 2, "lower").Snapshot()); err == nil || !strings.Contains(err.Error(), "another name") {
		t.Fatalf("Save(\"doc\"): %v, want a refusal naming the aliasing", err)
	}
	got, err := s.Load(ctx, "Doc")
	if err != nil {
		t.Fatal(err)
	}
	if text := paperText(t, got); text != "upper" {
		t.Fatalf("\"Doc\" now reads %q", text)
	}
	docs, err := s.Documents()
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 || docs[0] != "Doc" {
		t.Fatalf("Documents() = %v", docs)
	}
}

// dirsFolded lists the documents as a case-folding filesystem would: the
// directory that exists is shown under another spelling than the one asked
// for. This keeps the refusal covered on a filesystem that tells names apart.
type dirsFolded struct{ repository }

func (d dirsFolded) dirs() ([]string, error) {
	names, err := d.repository.dirs()
	for i := range names {
		names[i] = strings.ToUpper(names[i])
	}
	return names, err
}

func TestANameTheListingSpellsDifferentlyIsRefusedThroughTheSeam(t *testing.T) {
	ctx := context.Background()
	s := store(t)
	if err := s.Save(ctx, "doc", paper(t, 1, "mine").Snapshot()); err != nil {
		t.Fatal(err)
	}
	s.repo = dirsFolded{s.repo}
	if _, err := s.Load(ctx, "doc"); err == nil || !strings.Contains(err.Error(), "another name") {
		t.Fatalf("Load: %v, want the aliasing named", err)
	}
	if err := s.Save(ctx, "doc", paper(t, 2, "theirs").Snapshot()); err == nil || !strings.Contains(err.Error(), "another name") {
		t.Fatalf("Save: %v, want a refusal", err)
	}
	s.repo = s.repo.(dirsFolded).repository
	got, err := s.Load(ctx, "doc")
	if err != nil {
		t.Fatal(err)
	}
	if text := paperText(t, got); text != "mine" {
		t.Fatalf("the document now reads %q", text)
	}
}
