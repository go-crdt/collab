package collab_test

import (
	"context"
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

// await blocks until want holds, or fails the test with what it saw instead.
func await(t *testing.T, c *collab.Client, what string, want func() bool) {
	t.Helper()
	deadline := time.After(settle)
	for !want() {
		select {
		case <-c.Changes():
		case <-c.Done():
			if !want() {
				t.Fatalf("session ended before %s: %v (text %q)", what, c.Err(), c.Text())
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s; text is %q", what, c.Text())
		}
	}
}

// awaitText is the common case: wait for a participant to hold exactly this text.
func awaitText(t *testing.T, c *collab.Client, want string) {
	t.Helper()
	await(t, c, "the text to be "+want, func() bool { return c.Text() == want })
}

func TestOneEditReachesTheOthers(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	ada := join(t, conn, collab.ClientConfig{Document: "notes", Site: 1})
	grace := join(t, conn, collab.ClientConfig{Document: "notes", Site: 2})

	if err := ada.Insert(0, "hello"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	awaitText(t, grace, "hello")

	if err := grace.Insert(5, " world"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	awaitText(t, ada, "hello world")

	if err := ada.Delete(0, 6); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	awaitText(t, grace, "world")

	if got, want := ada.Document(), "notes"; got != want {
		t.Errorf("Document() = %q, want %q", got, want)
	}
	if got, want := ada.Site(), crdt.SiteID(1); got != want {
		t.Errorf("Site() = %d, want %d", got, want)
	}
	if got, want := ada.Len(), len("world"); got != want {
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
	if err := clients[0].Insert(0, "[]"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	for _, c := range clients {
		awaitText(t, c, "[]")
	}

	runs := []string{"aaa", "BBB", "ccc", "DDD", "eee"}
	done := make(chan error, participants)
	for i, c := range clients {
		go func() {
			for j, r := range runs[i] {
				if err := c.Insert(1+j, string(r)); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
	}
	for range clients {
		if err := <-done; err != nil {
			t.Fatalf("concurrent insert: %v", err)
		}
	}

	want := len("[]") + participants*3
	for i, c := range clients {
		await(t, c, "every character to arrive", func() bool { return c.Len() == want })
		if got := c.Len(); got != want {
			t.Fatalf("participant %d holds %d characters, want %d", i, got, want)
		}
	}
	// Converged means identical, not merely complete.
	text := clients[0].Text()
	for i, c := range clients {
		await(t, c, "convergence", func() bool { return c.Text() == text })
		if got := c.Text(); got != text {
			t.Fatalf("participant %d holds %q, participant 0 holds %q", i, got, text)
		}
	}
	// Nobody's run was chopped up by anybody else's.
	for _, run := range runs {
		if !strings.Contains(text, run) {
			t.Fatalf("%q was split apart in %q", run, text)
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
	if err := ada.Insert(0, "already written"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	awaitText(t, witness, "already written")

	grace := join(t, conn, collab.ClientConfig{Document: "minutes", Site: 2})
	if got, want := grace.Text(), "already written"; got != want {
		t.Fatalf("a late joiner sees %q, want %q — the document was not sent on join", got, want)
	}
	if err := grace.Insert(grace.Len(), ", and more"); err != nil {
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
	if err := ada.Insert(0, "chapter"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	awaitText(t, grace, "chapter")

	// Grace goes offline and keeps working on her own replica.
	kept := grace.Snapshot()
	if err := grace.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	offline, err := crdt.Load(2, kept)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, err := offline.Insert(offline.Len(), " two"); err != nil {
		t.Fatalf("offline Insert: %v", err)
	}

	// Ada carries on without her.
	if err := ada.Insert(0, "the "); err != nil {
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
	if err := ada.Insert(0, "0123456789"); err != nil {
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
	if err := ada.Insert(0, "kept"); err != nil {
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
	if got, want := grace.Text(), "kept"; got != want {
		t.Fatalf("a restarted server serves %q, want %q", got, want)
	}
}

// Flush persists without waiting for anyone to leave.
func TestFlushPersistsWhileInUse(t *testing.T) {
	store := collab.NewMemoryStore()
	srv, conn := serve(t, collab.Config{Store: store})
	ada := join(t, conn, collab.ClientConfig{Document: "live", Site: 1})
	if err := ada.Insert(0, "in progress"); err != nil {
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
			doc, err := crdt.Load(9, saved)
			if err != nil {
				t.Fatalf("the flushed snapshot is unreadable: %v", err)
			}
			if got, want := doc.String(), "in progress"; got != want {
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
