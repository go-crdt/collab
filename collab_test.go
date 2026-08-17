package collab_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/collab/collabpb"
	"github.com/go-crdt/crdt"
	"github.com/go-crdt/crdt/awareness"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// settle bounds how long a test waits for a change to reach a participant.
// Everything here runs in one process over an in-memory pipe, so anything near
// this bound is a failure, not a slow machine.
const settle = 10 * time.Second

// serve starts a Collab server on an in-memory connection and returns both,
// cleaned up when the test ends.
func serve(t *testing.T, cfg collab.Config) (*collab.Server, *grpc.ClientConn) {
	t.Helper()
	srv := collab.NewServer(cfg)
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	collabpb.RegisterCollabServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///collab",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		gs.Stop()
		_ = lis.Close()
	})
	return srv, conn
}

// join adds a participant, closed when the test ends.
func join(t *testing.T, conn *grpc.ClientConn, cfg collab.ClientConfig) *collab.Client {
	t.Helper()
	c, err := collab.Join(t.Context(), conn, cfg)
	if err != nil {
		t.Fatalf("Join(%q, site %d): %v", cfg.Document, cfg.Site, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// body returns the text part these tests edit. A document holds named parts now,
// so a test that means "the text" has to say which text — and every one of them
// means this one.
func body(t *testing.T, c *collab.Client) *collab.Text {
	t.Helper()
	h, err := c.Text("body")
	if err != nil {
		t.Fatalf("Text(\"body\"): %v", err)
	}
	return h
}

// text is body's string, which is what most assertions here are about.
func text(t *testing.T, c *collab.Client) string {
	t.Helper()
	return body(t, c).String()
}

// await blocks until want holds, or fails the test with what it saw instead.
func await(t *testing.T, c *collab.Client, what string, want func() bool) {
	t.Helper()
	deadline := time.After(settle)
	for !want() {
		select {
		case <-c.Changes():
		case <-c.Done():
			if !want() {
				t.Fatalf("session ended before %s: %v (text %q)", what, c.Err(), text(t, c))
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s; text is %q", what, text(t, c))
		}
	}
}

// awaitText is the common case: wait for a participant to hold exactly this text.
func awaitText(t *testing.T, c *collab.Client, want string) {
	t.Helper()
	await(t, c, "the text to be "+want, func() bool { return text(t, c) == want })
}

func TestOneEditReachesTheOthers(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	ada := join(t, conn, collab.ClientConfig{Document: "notes", Site: 1})
	grace := join(t, conn, collab.ClientConfig{Document: "notes", Site: 2})

	if err := body(t, ada).Insert(0, "hello"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	awaitText(t, grace, "hello")

	if err := body(t, grace).Insert(5, " world"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	awaitText(t, ada, "hello world")

	if err := body(t, ada).Delete(0, 6); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	awaitText(t, grace, "world")

	if got, want := ada.Document(), "notes"; got != want {
		t.Errorf("Document() = %q, want %q", got, want)
	}
	if got, want := ada.Site(), crdt.SiteID(1); got != want {
		t.Errorf("Site() = %d, want %d", got, want)
	}
	if got, want := body(t, ada).Len(), len("world"); got != want {
		t.Errorf("Len() = %d, want %d", got, want)
	}
}

// Everyone typing at once is the case the whole design exists for.
func TestConcurrentParticipantsConverge(t *testing.T) {
	const participants = 5
	_, conn := serve(t, collab.Config{})

	clients := make([]*collab.Client, participants)
	for i := range clients {
		clients[i] = join(t, conn, collab.ClientConfig{Document: "draft", Site: crdt.SiteID(i + 1)})
	}
	// Give everyone the same starting point so the concurrent edits collide.
	if err := body(t, clients[0]).Insert(0, "[]"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	for _, c := range clients {
		awaitText(t, c, "[]")
	}

	runs := []string{"aaa", "BBB", "ccc", "DDD", "eee"}
	done := make(chan error, participants)
	for i, c := range clients {
		go func() {
			// One call per participant, not one per character. Typing character
			// by character means asking for a *position* each time, and a peer's
			// insertion arriving between two keystrokes moves what that position
			// refers to — so the second character would hang off someone else's,
			// and the run would split for a perfectly good reason. A single
			// insertion is uninterrupted by construction, which is what makes the
			// contiguity assertion below meaningful.
			done <- body(t, c).Insert(1, runs[i])
		}()
	}
	for range clients {
		if err := <-done; err != nil {
			t.Fatalf("concurrent insert: %v", err)
		}
	}

	want := len("[]") + participants*3
	for i, c := range clients {
		await(t, c, "every character to arrive", func() bool { return body(t, c).Len() == want })
		if got := body(t, c).Len(); got != want {
			t.Fatalf("participant %d holds %d characters, want %d", i, got, want)
		}
	}
	// Converged means identical, not merely complete.
	settled := text(t, clients[0])
	for i, c := range clients {
		await(t, c, "convergence", func() bool { return text(t, c) == settled })
		if got := text(t, c); got != settled {
			t.Fatalf("participant %d holds %q, participant 0 holds %q", i, got, settled)
		}
	}
	// Nobody's run was chopped up by anybody else's.
	for _, run := range runs {
		if !strings.Contains(settled, run) {
			t.Fatalf("%q was split apart in %q", run, settled)
		}
	}
}

// A participant arriving after the work has been done is sent the document.
func TestLateJoinerIsSentTheDocument(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	ada := join(t, conn, collab.ClientConfig{Document: "minutes", Site: 1})
	// A witness already in the document tells us when the server has the edit.
	// Without it the late joiner could arrive first and be sent an empty
	// document quite correctly, and the test would prove nothing.
	witness := join(t, conn, collab.ClientConfig{Document: "minutes", Site: 9})
	if err := body(t, ada).Insert(0, "already written"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	awaitText(t, witness, "already written")

	grace := join(t, conn, collab.ClientConfig{Document: "minutes", Site: 2})
	if got, want := text(t, grace), "already written"; got != want {
		t.Fatalf("a late joiner sees %q, want %q — the document was not sent on join", got, want)
	}
	if err := body(t, grace).Insert(body(t, grace).Len(), ", and more"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	awaitText(t, ada, "already written, and more")
}

// Work done while disconnected has to reach everyone else when the participant
// comes back, and what happened meanwhile has to reach the participant. Neither
// direction may be dropped.
func TestResumeCarriesWorkBothWays(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	ada := join(t, conn, collab.ClientConfig{Document: "novel", Site: 1})
	grace := join(t, conn, collab.ClientConfig{Document: "novel", Site: 2})
	if err := body(t, ada).Insert(0, "chapter"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	awaitText(t, grace, "chapter")

	// Grace goes offline and keeps working on her own replica.
	kept := grace.Snapshot()
	if err := grace.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	offline, err := crdt.LoadComposite(2, kept)
	if err != nil {
		t.Fatalf("LoadComposite: %v", err)
	}
	offlineBody, err := offline.Text("body")
	if err != nil {
		t.Fatalf("Text: %v", err)
	}
	if _, err := offlineBody.Insert(offlineBody.Len(), " two"); err != nil {
		t.Fatalf("offline Insert: %v", err)
	}

	// Ada carries on without her.
	if err := body(t, ada).Insert(0, "the "); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	awaitText(t, ada, "the chapter")

	back := join(t, conn, collab.ClientConfig{
		Document: "novel",
		Site:     2,
		Resume:   offline.Snapshot(),
	})
	awaitText(t, back, "the chapter two")
	awaitText(t, ada, "the chapter two")
}

// Presence travels, and a participant who leaves stops being shown.
func TestPresence(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	ada := join(t, conn, collab.ClientConfig{Document: "shared", Site: 1})
	grace := join(t, conn, collab.ClientConfig{Document: "shared", Site: 2})
	if err := body(t, ada).Insert(0, "0123456789"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	awaitText(t, grace, "0123456789")

	if err := ada.SetCursor(awareness.Cursor{Anchor: 2, Head: 5}, map[string]string{"name": "ada"}); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}
	await(t, grace, "ada's cursor", func() bool {
		for _, p := range grace.Peers() {
			if p.Site == 1 && p.Cursor == (awareness.Cursor{Anchor: 2, Head: 5}) && p.Meta["name"] == "ada" {
				return true
			}
		}
		return false
	})

	// A late joiner is told about everyone already present.
	third := join(t, conn, collab.ClientConfig{Document: "shared", Site: 3})
	found := false
	for _, p := range third.Peers() {
		if p.Site == 1 && p.Cursor.Head == 5 {
			found = true
		}
	}
	if !found {
		t.Fatalf("a joiner was not told who is present: %+v", third.Peers())
	}

	if err := ada.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	await(t, grace, "ada's departure", func() bool {
		for _, p := range grace.Peers() {
			if p.Site == 1 {
				return false
			}
		}
		return true
	})
}

// A document outlives the participants: the last one out writes it, and a new
// server reads it back.
func TestDocumentSurvivesEveryoneLeaving(t *testing.T) {
	store := collab.NewMemoryStore()
	_, conn := serve(t, collab.Config{Store: store})

	ada, err := collab.Join(t.Context(), conn, collab.ClientConfig{Document: "archive", Site: 1})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if err := body(t, ada).Insert(0, "kept"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := ada.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	waitForStore(t, store, "archive")
	if got, want := store.Documents(), []string{"archive"}; len(got) != 1 || got[0] != want[0] {
		t.Fatalf("Documents() = %v, want %v", got, want)
	}

	// A brand new server, the same store.
	_, second := serve(t, collab.Config{Store: store})
	grace := join(t, second, collab.ClientConfig{Document: "archive", Site: 2})
	if got, want := text(t, grace), "kept"; got != want {
		t.Fatalf("a restarted server serves %q, want %q", got, want)
	}
}

// Flush persists without waiting for anyone to leave.
func TestFlushPersistsWhileInUse(t *testing.T) {
	store := collab.NewMemoryStore()
	srv, conn := serve(t, collab.Config{Store: store})
	ada := join(t, conn, collab.ClientConfig{Document: "live", Site: 1})
	if err := body(t, ada).Insert(0, "in progress"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	await(t, ada, "the server to have the edit", func() bool { return true })

	// Flush is a no-op until the server has actually seen the edit, so retry
	// until the store holds it rather than racing the round trip.
	deadline := time.After(settle)
	for {
		if err := srv.Flush(t.Context()); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		saved, err := store.Load(t.Context(), "live")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if len(saved) > 0 {
			doc, err := crdt.LoadComposite(9, saved)
			if err != nil {
				t.Fatalf("the flushed snapshot is unreadable: %v", err)
			}
			saved, err := doc.Text("body")
			if err != nil {
				t.Fatalf("Text: %v", err)
			}
			if got, want := saved.String(), "in progress"; got != want {
				t.Fatalf("the flushed document is %q, want %q", got, want)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("Flush never wrote the document")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Flushing again writes nothing, because nothing changed.
	if err := srv.Flush(t.Context()); err != nil {
		t.Fatalf("second Flush: %v", err)
	}
}

// waitForStore blocks until a document has been written.
func waitForStore(t *testing.T, store *collab.MemoryStore, name string) {
	t.Helper()
	deadline := time.After(settle)
	for {
		saved, err := store.Load(t.Context(), name)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if len(saved) > 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("%q was never written", name)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestMemoryStore(t *testing.T) {
	store := collab.NewMemoryStore()
	if got, err := store.Load(t.Context(), "absent"); err != nil || got != nil {
		t.Fatalf("Load of an unknown document = %v, %v; want nil, nil", got, err)
	}
	if got := store.Documents(); len(got) != 0 {
		t.Fatalf("Documents() = %v, want none", got)
	}

	original := []byte("snapshot")
	if err := store.Save(t.Context(), "doc", original); err != nil {
		t.Fatalf("Save: %v", err)
	}
	original[0] = 'X'
	got, err := store.Load(t.Context(), "doc")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(got) != "snapshot" {
		t.Fatalf("Load returned %q: the store kept a reference to the caller's bytes", got)
	}
	got[0] = 'Y'
	if again, _ := store.Load(t.Context(), "doc"); string(again) != "snapshot" {
		t.Fatalf("Load returned %q: the store handed out its own bytes", again)
	}
}

// A binding keeps its own copy of the text and is told the edits, because being
// handed the whole text would throw away the selection and the scroll position
// on every keystroke anybody else makes. This is that loop, over a session.
func TestAViewIsKeptInStepByTheChanges(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	writer := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	watcher := join(t, conn, collab.ClientConfig{Document: "doc", Site: 2})

	var text []rune
	catchUp := func(want string) {
		t.Helper()
		awaitText(t, watcher, want)
		for _, part := range watcher.TakeChanges() {
			// A view binds to one part, so it takes the changes for that part and
			// leaves the rest to whoever is watching them.
			if want := (crdt.Part{Kind: crdt.PartText, Name: "body"}); part.Part != want {
				t.Fatalf("change is for %v, want %v", part.Part, want)
			}
			for _, c := range part.Text {
				if c.Pos < 0 || c.Pos+c.Removed > len(text) {
					t.Fatalf("change %+v does not fit a view of %d characters", c, len(text))
				}
				tail := append([]rune(nil), text[c.Pos+c.Removed:]...)
				text = append(append(text[:c.Pos], []rune(c.Text)...), tail...)
			}
		}
		if got := string(text); got != want {
			t.Fatalf("the view holds %q, the document %q", got, want)
		}
	}

	if err := body(t, writer).Insert(0, "the quick fox"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	catchUp("the quick fox")

	if err := body(t, writer).Insert(10, "brown "); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	catchUp("the quick brown fox")

	if err := body(t, writer).Delete(3, 6); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	catchUp("the brown fox")

	// Taking them again yields nothing: they were taken.
	if got := watcher.TakeChanges(); len(got) != 0 {
		t.Fatalf("TakeChanges again returned %+v, want nothing", got)
	}
	// And a participant's own edits are not reported back to it.
	if err := body(t, watcher).Insert(0, ">> "); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if got := watcher.TakeChanges(); len(got) != 0 {
		t.Fatalf("a local edit was reported as a change: %+v", got)
	}
}

// An anchor taken through a client keeps naming its character while everyone
// else edits, which is what a comment pinned to a line needs.
func TestAnchorsThroughASession(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	ada := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	grace := join(t, conn, collab.ClientConfig{Document: "doc", Site: 2})

	if err := body(t, ada).Insert(0, "chapter one"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	awaitText(t, grace, "chapter one")

	anchor, err := body(t, grace).Anchor(8) // the "o" of one
	if err != nil {
		t.Fatalf("Anchor: %v", err)
	}
	if !body(t, grace).Visible(anchor) {
		t.Fatal("the anchored character is reported as gone")
	}

	if err := body(t, ada).Insert(0, "part two, "); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	awaitText(t, grace, "part two, chapter one")
	pos, ok := body(t, grace).Position(anchor)
	if !ok || pos != 18 {
		t.Fatalf("Position after an insertion above = %d, %v; want 18, true", pos, ok)
	}

	if err := body(t, ada).Delete(18, 3); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	awaitText(t, grace, "part two, chapter ")
	if body(t, grace).Visible(anchor) {
		t.Fatal("the anchored character is reported as present after being deleted")
	}
	if pos, _ := body(t, grace).Position(anchor); pos != 18 {
		t.Fatalf("Position of the deleted character = %d, want 18", pos)
	}

	// And who wrote what, which is the other thing a margin shows.
	runs := body(t, grace).AuthorRuns()
	if len(runs) != 1 || runs[0].Site != 1 || runs[0].Len != body(t, grace).Len() {
		t.Fatalf("AuthorRuns() = %+v, want all %d characters by site 1", runs, body(t, grace).Len())
	}
}

// A browser counts UTF-16 code units, and hands those offsets straight to the
// session. An emoji is one character and two units, so a session that took them
// for runes would edit in the wrong place — silently, once, and then for ever.
func TestEditingInTheUnitsABrowserCounts(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	browser := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	watcher := join(t, conn, collab.ClientConfig{Document: "doc", Site: 2})

	if err := body(t, browser).Insert(0, "a😀b"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if got, want := body(t, browser).Len(), 3; got != want {
		t.Fatalf("Len() = %d, want %d runes", got, want)
	}
	if got, want := body(t, browser).LenUTF16(), 4; got != want {
		t.Fatalf("LenUTF16() = %d, want %d — what the browser would report", got, want)
	}

	// The browser's caret after the emoji is at unit 3, not rune 3.
	if err := body(t, browser).InsertUTF16(3, "X"); err != nil {
		t.Fatalf("InsertUTF16: %v", err)
	}
	awaitText(t, watcher, "a😀Xb")

	// An offset inside the emoji is refused rather than moved.
	if err := body(t, browser).InsertUTF16(2, "!"); !errors.Is(err, crdt.ErrSurrogateBoundary) {
		t.Fatalf("InsertUTF16 inside a character = %v, want ErrSurrogateBoundary", err)
	}
	// So is a deletion that would cut one in half.
	if err := body(t, browser).DeleteUTF16(2, 1); !errors.Is(err, crdt.ErrSurrogateBoundary) {
		t.Fatalf("DeleteUTF16 inside a character = %v, want ErrSurrogateBoundary", err)
	}
	if err := body(t, browser).DeleteUTF16(1, 2); err != nil {
		t.Fatalf("DeleteUTF16: %v", err)
	}
	awaitText(t, watcher, "aXb")
	if got, want := body(t, watcher).LenUTF16(), 3; got != want {
		t.Fatalf("LenUTF16() = %d, want %d", got, want)
	}
}
