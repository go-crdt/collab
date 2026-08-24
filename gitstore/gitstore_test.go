package gitstore

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
	"github.com/go-crdt/crdt/structured"
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
	dir := s.tree.Filesystem.Root()
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
		files []rendered
		want  string
	}{
		{nil, "state"},
		{[]rendered{{path: "a"}}, "a"},
		{[]rendered{{path: "a"}, {path: "b"}}, "a, b"},
		{[]rendered{{path: "a"}, {path: "b"}, {path: "c"}}, "a, b, c"},
		{[]rendered{{path: "a"}, {path: "b"}, {path: "c"}, {path: "d"}}, "a, b, c and 1 more"},
	}
	for _, c := range cases {
		if got := describe(c.files); got != c.want {
			t.Errorf("describe(%d files) = %q, want %q", len(c.files), got, c.want)
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
	if err := s.tree.Filesystem.MkdirAll("not-base64-at-all!", 0o755); err != nil {
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
