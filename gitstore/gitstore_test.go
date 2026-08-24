package gitstore

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
	"github.com/go-crdt/crdt/structured"
	git "github.com/go-git/go-git/v5"
)

// a document to save: one file of text, one comment anchored into it, and a
// chat beside it — the three things a collaborative document holds that a
// rendered file cannot.
func paper(t *testing.T, site crdt.SiteID, text string) *crdt.Composite {
	t.Helper()
	doc := crdt.NewComposite(site)
	body, err := doc.Text("file:paper.tex")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, text); err != nil {
		t.Fatal(err)
	}
	return doc
}

func stamps() func() time.Time {
	at := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	return func() time.Time {
		at = at.Add(time.Minute)
		return at
	}
}

func store(t *testing.T) *Store {
	t.Helper()
	s, err := New(t.TempDir(), WithAuthor("loom", "loom@example"), WithClock(stamps()))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// The text is what the history is about, and the state is what a restore uses.
// Both are written from one snapshot in one commit, so they cannot drift.
func TestADocumentIsBothReadableAndRestorable(t *testing.T) {
	ctx := t.Context()
	s := store(t)

	doc := paper(t, 1, "On rivers.")
	if err := s.Save(ctx, "project:default", doc.Snapshot()); err != nil {
		t.Fatal(err)
	}

	// Readable: the text is a file in the repository.
	files := gitShow(t, s, "HEAD")
	var found string
	for name, content := range files {
		if strings.HasSuffix(name, "paper.tex") {
			found = content
		}
	}
	if found != "On rivers." {
		t.Fatalf("the repository holds %q for paper.tex; files are %v", found, keys(files))
	}

	// Restorable: the snapshot is beside it, and it comes back whole.
	raw, err := s.Load(ctx, "project:default")
	if err != nil {
		t.Fatal(err)
	}
	back, err := crdt.LoadComposite(2, raw)
	if err != nil {
		t.Fatal(err)
	}
	body, err := back.Text("file:paper.tex")
	if err != nil {
		t.Fatal(err)
	}
	if body.String() != "On rivers." {
		t.Fatalf("the restored document reads %q", body.String())
	}
}

// What the text cannot carry, and the state does. A document restored from a
// release is the document that was released, not one that says the same words.
func TestWhatOnlyTheStateCarries(t *testing.T) {
	ctx := t.Context()
	s := store(t)

	// A comment anchored to a stretch of the text, which is what a rendered
	// file has nowhere to put.
	doc := crdt.NewComposite(1)
	rich := structured.RichTextOf(doc)
	if _, err := rich.Insert(0, "the quick brown fox"); err != nil {
		t.Fatal(err)
	}
	if _, err := rich.Mark(4, 9, "comment", []byte("which quick?"), structured.ExpandNone); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, "notes", doc.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, "v1", "the first draft"); err != nil {
		t.Fatal(err)
	}

	// The text moves on.
	if _, err := rich.Insert(0, "PS. "); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, "notes", doc.Snapshot()); err != nil {
		t.Fatal(err)
	}

	// The release comes back with the comment on the same words.
	raw, err := s.At("notes", "v1")
	if err != nil {
		t.Fatal(err)
	}
	back, err := crdt.LoadComposite(2, raw)
	if err != nil {
		t.Fatal(err)
	}
	released := structured.RichTextOf(back)
	if released.Text() != "the quick brown fox" {
		t.Fatalf("the release reads %q", released.Text())
	}
	marks := released.MarksAt(5)
	if string(marks["comment"]) != "which quick?" {
		t.Fatalf("the comment did not come back: %v", marks)
	}
	if got := released.MarksAt(0); got != nil {
		t.Fatalf("the comment moved: %v", got)
	}
}

// A release names a commit that already exists rather than making one, so the
// history holds only states somebody was editing.
func TestAReleaseNamesAStateNobodyInvented(t *testing.T) {
	ctx := t.Context()
	s := store(t)
	doc := paper(t, 1, "one")
	if err := s.Save(ctx, "d", doc.Snapshot()); err != nil {
		t.Fatal(err)
	}
	before := gitLog(t, s)
	if err := s.Release(ctx, "v1", "first"); err != nil {
		t.Fatal(err)
	}
	if after := gitLog(t, s); len(after) != len(before) {
		t.Fatalf("releasing made a commit: %d before, %d after", len(before), len(after))
	}
	history, err := s.History("d")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].Release != "v1" {
		t.Fatalf("history = %+v", history)
	}
	// Releasing before anything is saved says so rather than tagging nothing.
	empty := store(t)
	if err := empty.Release(ctx, "v1", ""); err == nil {
		t.Fatal("a release was made against a repository with no commits")
	}
	if err := s.Release(ctx, "", "no name"); err == nil {
		t.Fatal("a release with no name was made")
	}
}

// A save that changes nothing makes no commit. A server persists on a timer, so
// most saves of a document nobody is touching are the last one again.
func TestSavingTheSameThingTwiceIsOneCommit(t *testing.T) {
	ctx := t.Context()
	s := store(t)
	doc := paper(t, 1, "steady")
	snapshot := doc.Snapshot()
	for range 5 {
		if err := s.Save(ctx, "d", snapshot); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(gitLog(t, s)); got != 1 {
		t.Fatalf("five identical saves made %d commits", got)
	}
	if _, err := doc.Text("file:paper.tex"); err != nil {
		t.Fatal(err)
	}
	body, _ := doc.Text("file:paper.tex")
	if _, err := body.Insert(6, " on"); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, "d", doc.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if got := len(gitLog(t, s)); got != 2 {
		t.Fatalf("a real change made %d commits in total", got)
	}
}

// Several documents share one repository and do not appear in each other's
// history.
func TestDocumentsDoNotShareAHistory(t *testing.T) {
	ctx := t.Context()
	s := store(t)
	for _, name := range []string{"alpha", "beta"} {
		doc := paper(t, 1, name)
		if err := s.Save(ctx, name, doc.Snapshot()); err != nil {
			t.Fatal(err)
		}
	}
	alpha, err := s.History("alpha")
	if err != nil {
		t.Fatal(err)
	}
	if len(alpha) != 1 {
		t.Fatalf("alpha has %d revisions, want its own one", len(alpha))
	}
	if !strings.HasPrefix(alpha[0].Message, "alpha:") {
		t.Fatalf("alpha's history holds %q", alpha[0].Message)
	}
	docs, err := s.Documents()
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 2 || docs[0] != "alpha" || docs[1] != "beta" {
		t.Fatalf("Documents = %v", docs)
	}
}

// A document nobody has saved is a new one, and a name that cannot be a
// directory is refused.
func TestWhatTheStoreRefuses(t *testing.T) {
	ctx := t.Context()
	s := store(t)
	raw, err := s.Load(ctx, "never-saved")
	if err != nil || raw != nil {
		t.Fatalf("Load of an unsaved document = %v, %v", raw, err)
	}
	if _, err := s.Load(ctx, ""); err != ErrNoDocument {
		t.Fatalf("Load(\"\") = %v", err)
	}
	if err := s.Save(ctx, "", nil); err != ErrNoDocument {
		t.Fatalf("Save(\"\") = %v", err)
	}
	if err := s.Save(ctx, "d", []byte("not a snapshot")); err == nil {
		t.Fatal("bytes that are not a snapshot were committed")
	}
	if _, err := s.At("d", "nothing-like-this"); err == nil {
		t.Fatal("a revision that does not exist resolved")
	}
	if _, err := s.At("d", ""); err == nil {
		t.Fatal("an unnamed revision resolved")
	}
	if got, err := s.History("never-saved"); err != nil || got != nil {
		t.Fatalf("History of an unsaved document = %v, %v", got, err)
	}
}

// A part name comes from a peer and is arbitrary UTF-8, so a name that would
// climb out of the document's directory is not a path here.
func TestAPartNameCannotEscapeItsDocument(t *testing.T) {
	for _, name := range []string{
		"file:../../etc/passwd",
		"file:/etc/passwd",
		"file:..",
		"file:",
		"chat",
	} {
		part := crdt.Part{Kind: crdt.PartText, Name: name}
		at, ok := defaultFileFor(part)
		if ok && (strings.HasPrefix(at, "/") || strings.Contains(at, "..")) {
			t.Errorf("%q became the path %q", name, at)
		}
		if ok {
			t.Logf("%-24q -> %q", name, at)
		} else {
			t.Logf("%-24q -> not a file", name)
		}
	}
	// A list is not a file whatever it is called.
	if _, ok := defaultFileFor(crdt.Part{Kind: crdt.PartList, Name: "file:chat"}); ok {
		t.Error("a list part was rendered as a file")
	}
}

// The store is what collab asks a store to be, driven through a real server.
func TestAServerKeepsItsDocumentsHere(t *testing.T) {
	ctx := t.Context()
	s := store(t)
	srv := collab.NewServer(collab.Config{Store: s})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	serveCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	local, server := collab.Pipe()
	go func() { _ = srv.ServePipe(serveCtx, server) }()

	c, err := collab.Join(ctx, local, collab.ClientConfig{Document: "project:paper", Site: 7})
	if err != nil {
		t.Fatalf("joining over the pipe: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	body, err := c.Text("file:paper.tex")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, "written through a server"); err != nil {
		t.Fatal(err)
	}

	// The edit travels to the server before the server has anything to write,
	// so this waits on a deadline rather than assuming one flush is enough.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := srv.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		raw, err := s.Load(ctx, "project:paper")
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the server never wrote the document down")
		}
		time.Sleep(20 * time.Millisecond)
	}

	files := gitShow(t, s, "HEAD")
	for name, content := range files {
		if strings.HasSuffix(name, "paper.tex") && content == "written through a server" {
			return
		}
	}
	t.Fatalf("the server's document did not reach the repository: %v", keys(files))
}

// --- reading the repository with git itself, not with the library under test

func gitLog(t *testing.T, s *Store) []string {
	t.Helper()
	out := run(t, s, "log", "--format=%H")
	if strings.TrimSpace(out) == "" {
		return nil
	}
	return strings.Split(strings.TrimSpace(out), "\n")
}

func gitShow(t *testing.T, s *Store, rev string) map[string]string {
	t.Helper()
	names := strings.Fields(run(t, s, "ls-tree", "-r", "--name-only", rev))
	out := map[string]string{}
	for _, name := range names {
		out[name] = run(t, s, "show", rev+":"+name)
	}
	return out
}

// run drives the real git binary against the store's repository, so the tests
// assert on what git says the repository is rather than on what the library
// that wrote it believes.
func run(t *testing.T, s *Store, args ...string) string {
	t.Helper()
	return runIn(t, s.repo.root(), args...)
}

// runIn is run against any repository, which the remote tests need because the
// repository two instances share is not either of their stores.
func runIn(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// A caller decides what a file is. The default is the convention a document of
// files already uses; anything else is the caller's to say.
func TestTheCallerDecidesWhatAFileIs(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	s, err := New(dir, WithClock(stamps()), WithFiles(func(p crdt.Part) (string, bool) {
		if p.Kind != crdt.PartText {
			return "", false
		}
		return "text/" + p.Name + ".txt", true
	}))
	if err != nil {
		t.Fatal(err)
	}
	doc := crdt.NewComposite(1)
	for _, name := range []string{"one", "two", "three", "four"} {
		text, err := doc.Text(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := text.Insert(0, "of "+name); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Save(ctx, "d", doc.Snapshot()); err != nil {
		t.Fatal(err)
	}
	files := gitShow(t, s, "HEAD")
	for _, want := range []string{"text/one.txt", "text/four.txt"} {
		found := false
		for name := range files {
			if strings.HasSuffix(name, want) {
				found = true
			}
		}
		if !found {
			t.Errorf("no %s in %v", want, keys(files))
		}
	}
	// Four files, so the subject says the first three and how many more.
	subject := strings.SplitN(run(t, s, "log", "-1", "--format=%s"), "\n", 2)[0]
	if !strings.Contains(subject, "and 1 more") {
		t.Errorf("the subject is %q", subject)
	}
}

// What a commit is about, in the subject line, for each number of files.
func TestWhatACommitSaysItIsAbout(t *testing.T) {
	cases := []struct {
		names []string
		want  string
	}{
		{nil, "state"},
		{[]string{"a"}, "a"},
		{[]string{"a", "b"}, "a, b"},
		{[]string{"a", "b", "c"}, "a, b, c"},
		{[]string{"a", "b", "c", "d"}, "a, b, c and 1 more"},
	}
	for _, c := range cases {
		if got := describe(c.names); got != c.want {
			t.Errorf("describe(%d names) = %q, want %q", len(c.names), got, c.want)
		}
	}
}

// A revision is a release name or a commit, and anything else says so.
func TestARevisionIsANameOrACommit(t *testing.T) {
	ctx := t.Context()
	s := store(t)
	doc := paper(t, 1, "one")
	if err := s.Save(ctx, "d", doc.Snapshot()); err != nil {
		t.Fatal(err)
	}
	hash := strings.TrimSpace(run(t, s, "rev-parse", "HEAD"))

	// By commit hash.
	byHash, err := s.At("d", hash)
	if err != nil {
		t.Fatal(err)
	}
	// By an annotated tag.
	if err := s.Release(ctx, "annotated", "with a message"); err != nil {
		t.Fatal(err)
	}
	byTag, err := s.At("d", "annotated")
	if err != nil {
		t.Fatal(err)
	}
	if string(byHash) != string(byTag) {
		t.Fatal("the same commit read two ways gave two snapshots")
	}
	// And by a lightweight tag, which git also makes and which carries no
	// object of its own.
	run(t, s, "tag", "lightweight", hash)
	byLight, err := s.At("d", "lightweight")
	if err != nil {
		t.Fatalf("a lightweight tag did not resolve: %v", err)
	}
	if string(byLight) != string(byHash) {
		t.Fatal("a lightweight tag resolved to something else")
	}
	history, err := s.History("d")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].Release == "" {
		t.Fatalf("history = %+v", history)
	}

	// A document that is not in that revision.
	if _, err := s.At("absent", hash); err == nil {
		t.Fatal("a document that was never saved read at a revision")
	}
	if _, err := s.At("", hash); err != ErrNoDocument {
		t.Fatalf("At with no document = %v", err)
	}
	if _, err := s.History(""); err != ErrNoDocument {
		t.Fatalf("History with no document = %v", err)
	}
}

// A directory nothing here wrote is not a document.
func TestADirectoryThisStoreDidNotMakeIsNotADocument(t *testing.T) {
	ctx := t.Context()
	s := store(t)
	doc := paper(t, 1, "real")
	if err := s.Save(ctx, "real", doc.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if err := mkdirIn(s, "not-base64-at-all!", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := s.write("loose-file", []byte("x")); err != nil {
		t.Fatal(err)
	}
	docs, err := s.Documents()
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 || docs[0] != "real" {
		t.Fatalf("Documents = %v, want just the one this store wrote", docs)
	}
}

// A path that is a file is not a repository.
func TestWhatCannotBeOpened(t *testing.T) {
	dir := t.TempDir()
	file := dir + "/a-file"
	if err := os.WriteFile(file, []byte("not a repository"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(file); err == nil {
		t.Fatal("a file was opened as a repository")
	}
}

// A document that cannot be read is not a new document.
//
// Saying it were is the worst thing this store could do: a server told "there
// is nothing here" starts an empty one and saves it over what it could not read.
func TestAnUnreadableDocumentIsNotANewOne(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads anything, so there is no unreadable file to make")
	}
	ctx := t.Context()
	s := store(t)
	doc := paper(t, 1, "please do not lose me")
	if err := s.Save(ctx, "d", doc.Snapshot()); err != nil {
		t.Fatal(err)
	}

	dir, err := dirFor("d")
	if err != nil {
		t.Fatal(err)
	}
	at := filepath.Join(s.repo.root(), dir, stateFile)
	if err := os.Chmod(at, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(at, 0o644) })

	raw, err := s.Load(ctx, "d")
	if err == nil {
		t.Fatalf("a document that will not open read as a new one: %d bytes", len(raw))
	}
	if raw != nil {
		t.Fatalf("it also handed back %d bytes", len(raw))
	}
	if errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("an unreadable document was reported as absent: %v", err)
	}
}

// A bare repository has nowhere to write.
func TestABareRepositoryIsRefused(t *testing.T) {
	dir := t.TempDir()
	if _, err := git.PlainInit(dir, true); err != nil {
		t.Fatal(err)
	}
	if _, err := New(dir); err == nil {
		t.Fatal("a bare repository was opened as a store")
	}
}

// A release name that is already taken says so rather than moving the tag.
func TestAReleaseNameIsNotReused(t *testing.T) {
	ctx := t.Context()
	s := store(t)
	doc := paper(t, 1, "one")
	if err := s.Save(ctx, "d", doc.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, "v1", "first"); err != nil {
		t.Fatal(err)
	}
	body, _ := doc.Text("file:paper.tex")
	if _, err := body.Insert(3, " more"); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, "d", doc.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, "v1", "again"); err == nil {
		t.Fatal("v1 was released twice; the first one would have moved")
	}
	// And v1 still names what it named.
	raw, err := s.At("d", "v1")
	if err != nil {
		t.Fatal(err)
	}
	back, err := crdt.LoadComposite(2, raw)
	if err != nil {
		t.Fatal(err)
	}
	text, _ := back.Text("file:paper.tex")
	if text.String() != "one" {
		t.Fatalf("v1 now names %q", text.String())
	}
}

// A hash that is a real object and not a commit.
func TestAHashThatIsNotACommit(t *testing.T) {
	ctx := t.Context()
	s := store(t)
	doc := paper(t, 1, "one")
	if err := s.Save(ctx, "d", doc.Snapshot()); err != nil {
		t.Fatal(err)
	}
	// The tree of HEAD is an object, and it is not a commit.
	tree := strings.TrimSpace(run(t, s, "rev-parse", "HEAD^{tree}"))
	if _, err := s.At("d", tree); err == nil {
		t.Fatalf("a tree object %s resolved as a revision", tree)
	}
}

// A file that is written and cannot then be read is not committed, and does not
// half-commit either: staging it fails and the save says so.
func TestAFileThatCannotBeStaged(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads anything")
	}
	ctx := t.Context()
	s := store(t)
	doc := paper(t, 1, "one")
	if err := s.Save(ctx, "d", doc.Snapshot()); err != nil {
		t.Fatal(err)
	}
	dir, err := dirFor("d")
	if err != nil {
		t.Fatal(err)
	}
	// The directory itself refuses to be written into or read from.
	at := filepath.Join(s.repo.root(), dir)
	if err := os.Chmod(at, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(at, 0o755) })

	body, _ := doc.Text("file:paper.tex")
	if _, err := body.Insert(3, " more"); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, "d", doc.Snapshot()); err == nil {
		t.Fatal("a save into a directory nothing can write succeeded")
	}
}

// The repository can go away underneath a store, and reading it says so rather
// than pretending there are no documents.
func TestTheRepositoryGoingAway(t *testing.T) {
	ctx := t.Context()
	s := store(t)
	doc := paper(t, 1, "one")
	if err := s.Save(ctx, "d", doc.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(s.repo.root()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Documents(); err == nil {
		t.Fatal("a repository that is gone listed its documents")
	}
}

// Two instances sharing a repository, which is what federation over a git
// remote would be.
//
// They diverge, because working separately is what that means. Git then reports
// a conflict on the state file, and that conflict is the one in this design
// that never needs a person.
func TestTwoInstancesSharingARepository(t *testing.T) {
	// Paris and Lyon each hold the document and each edit it, having agreed on
	// an opening line and then heard nothing from each other.
	opening := crdt.NewComposite(crdt.DeriveSiteID([]byte("ada@paris.ac.example")))
	body, err := opening.Text("file:paper.tex")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, "On rivers."); err != nil {
		t.Fatal(err)
	}
	shared := opening.Snapshot()

	paris, err := crdt.LoadComposite(crdt.DeriveSiteID([]byte("ada@paris.ac.example")), shared)
	if err != nil {
		t.Fatal(err)
	}
	lyon, err := crdt.LoadComposite(crdt.DeriveSiteID([]byte("grace@lyon.ac.example")), shared)
	if err != nil {
		t.Fatal(err)
	}
	parisBody, _ := paris.Text("file:paper.tex")
	lyonBody, _ := lyon.Text("file:paper.tex")
	if _, err := parisBody.Insert(parisBody.Len(), " They run downhill."); err != nil {
		t.Fatal(err)
	}
	if _, err := lyonBody.Insert(0, "PS. "); err != nil {
		t.Fatal(err)
	}

	// Whichever side resolves the conflict, both reach the same commit — which
	// is what a merge driver has to be true of, or the two instances have
	// merely disagreed somewhere new.
	oneWay, err := Merge(paris.Snapshot(), lyon.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	otherWay, err := Merge(lyon.Snapshot(), paris.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if string(oneWay) != string(otherWay) {
		t.Fatalf("the two sides resolved the same conflict differently: %d bytes and %d",
			len(oneWay), len(otherWay))
	}

	// And it holds what both wrote.
	merged, err := crdt.LoadComposite(1, oneWay)
	if err != nil {
		t.Fatal(err)
	}
	text, _ := merged.Text("file:paper.tex")
	if got := text.String(); got != "PS. On rivers. They run downhill." {
		t.Fatalf("the merge reads %q", got)
	}

	// Merging again changes nothing, so a pull that finds nothing new commits
	// nothing.
	again, err := Merge(oneWay, otherWay)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(oneWay) {
		t.Fatal("merging the same two snapshots twice gave two answers")
	}
}

// Reconcile is what a pull does with the other instance's state, and the text
// is written again rather than merged — so a conflict marker never reaches a
// document.
func TestReconcileRegeneratesTheTextRatherThanMergingIt(t *testing.T) {
	ctx := t.Context()
	s := store(t)

	shared := paper(t, 1, "On rivers.")
	if err := s.Save(ctx, "d", shared.Snapshot()); err != nil {
		t.Fatal(err)
	}

	// The other instance did something else to the same sentence.
	theirs, err := crdt.LoadComposite(2, shared.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	theirBody, _ := theirs.Text("file:paper.tex")
	if _, err := theirBody.Insert(0, "PS. "); err != nil {
		t.Fatal(err)
	}

	// And this one carried on too, so both sides moved.
	ourBody, _ := shared.Text("file:paper.tex")
	if _, err := ourBody.Insert(ourBody.Len(), " They run downhill."); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, "d", shared.Snapshot()); err != nil {
		t.Fatal(err)
	}

	if err := s.Reconcile(ctx, "d", theirs.Snapshot()); err != nil {
		t.Fatal(err)
	}

	files := gitShow(t, s, "HEAD")
	for name, content := range files {
		if !strings.HasSuffix(name, "paper.tex") {
			continue
		}
		if strings.Contains(content, "<<<<<<<") || strings.Contains(content, ">>>>>>>") {
			t.Fatalf("a conflict marker reached the document: %q", content)
		}
		if content != "PS. On rivers. They run downhill." {
			t.Fatalf("the reconciled text is %q", content)
		}
		return
	}
	t.Fatalf("no paper.tex in %v", keys(files))
}

// A document this instance has never seen is simply taken.
func TestReconcilingSomethingThisInstanceDoesNotHave(t *testing.T) {
	ctx := t.Context()
	s := store(t)
	theirs := paper(t, 2, "written elsewhere")
	if err := s.Reconcile(ctx, "new-here", theirs.Snapshot()); err != nil {
		t.Fatal(err)
	}
	raw, err := s.Load(ctx, "new-here")
	if err != nil || raw == nil {
		t.Fatalf("Load = %d bytes, %v", len(raw), err)
	}
	back, err := crdt.LoadComposite(1, raw)
	if err != nil {
		t.Fatal(err)
	}
	text, _ := back.Text("file:paper.tex")
	if text.String() != "written elsewhere" {
		t.Fatalf("it reads %q", text.String())
	}
}

// What a merge refuses: bytes that are not a snapshot, on either side, and a
// snapshot promising operations it does not carry.
func TestWhatAMergeRefuses(t *testing.T) {
	ctx := t.Context()
	good := paper(t, 1, "one").Snapshot()
	if _, err := Merge([]byte("not a snapshot"), good); err == nil {
		t.Fatal("our side was not a snapshot and merged anyway")
	}
	if _, err := Merge(good, []byte("not a snapshot")); err == nil {
		t.Fatal("their side was not a snapshot and merged anyway")
	}
	s := store(t)
	if err := s.Save(ctx, "d", good); err != nil {
		t.Fatal(err)
	}
	if err := s.Reconcile(ctx, "d", []byte("not a snapshot")); err == nil {
		t.Fatal("a pull of nonsense was committed")
	}
	if err := s.Reconcile(ctx, "", good); err != ErrNoDocument {
		t.Fatalf("Reconcile with no document = %v", err)
	}
	// What is stored is untouched by any of that.
	raw, err := s.Load(ctx, "d")
	if err != nil || string(raw) != string(good) {
		t.Fatalf("the stored document changed: %d bytes, %v", len(raw), err)
	}
}

// mkdirIn makes a directory in the store's repository, for the tests that need
// something in the way.
func mkdirIn(s *Store, name string, perm os.FileMode) error {
	return os.MkdirAll(filepath.Join(s.repo.root(), name), perm)
}

// The move to collab.MergeSnapshots changed one thing about Merge, and a
// behaviour that changed is a behaviour worth pinning: an empty side used to be
// an error and is now how a store says it has never held the document.
func TestMergingWithASideThatWasNeverHeld(t *testing.T) {
	held := paper(t, 1, "one").Snapshot()

	for _, c := range []struct {
		name         string
		ours, theirs []byte
		want         []byte
	}{
		{"they have never held it", held, nil, held},
		{"we have never held it", nil, held, held},
		{"neither has", nil, nil, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := Merge(c.ours, c.theirs)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, c.want) {
				t.Fatalf("got %d bytes, want %d", len(got), len(c.want))
			}
		})
	}
}
