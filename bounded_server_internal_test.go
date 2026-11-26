//go:build !js

package collab

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// Every wait in this file has a deadline of a few seconds and fails with
// t.Errorf when it is missed. The guards held here sit in front of a wait: with
// one deleted, the suite does not fail, it hangs until go test's own ten-minute
// timeout kills the binary, which mutsweep reads as HUNG and which proves
// nothing was asserted. A deadline turns the hang into a failure in seconds.

const boundedWait = 4 * time.Second

// rawSession is a session driven by hand over a pipe, so that a test sees
// exactly what the server sends and when it ends it.
type rawSession struct {
	t      *testing.T
	conn   carrierConn
	served chan error
	cancel context.CancelFunc
}

func openRaw(t *testing.T, s *Server, j joinMsg) *rawSession {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), boundedWait)
	transport, serverEnd := Pipe()
	served := make(chan error, 1)
	go func() { served <- s.ServePipe(ctx, serverEnd) }()
	conn, err := transport.open(ctx)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	r := &rawSession{t: t, conn: conn, served: served, cancel: cancel}
	t.Cleanup(func() { _ = conn.Close(); cancel() })
	if err := conn.Send(kindJoin, j); err != nil {
		t.Fatal(err)
	}
	return r
}

// ended waits for the server side of the session to finish and returns why.
// ok is false when it did not end before the deadline, which is reported.
func (r *rawSession) ended(what string) (error, bool) {
	r.t.Helper()
	select {
	case err := <-r.served:
		return err, true
	case <-time.After(boundedWait):
		r.t.Errorf("%s: the session did not end within %v", what, boundedWait)
		return nil, false
	}
}

// refusedAs checks the session ended with a sessionError of the given kind.
func (r *rawSession) refusedAs(what string, kind errKind) {
	r.t.Helper()
	err, ok := r.ended(what)
	if !ok {
		return
	}
	var se *sessionError
	if !errors.As(err, &se) || se.kind != kind {
		r.t.Errorf("%s: the session ended with %v, want a sessionError of kind %v", what, err, kind)
	}
}

// welcome reads the answer to a join and reports whether it was a welcome.
func (r *rawSession) welcome(what string) (welcomeMsg, bool) {
	r.t.Helper()
	kind, msg, err := r.conn.Recv()
	if err != nil {
		r.t.Errorf("%s: no answer to the join: %v", what, err)
		return welcomeMsg{}, false
	}
	w, ok := msg.(welcomeMsg)
	if kind != kindWelcome || !ok {
		r.t.Errorf("%s: the join was answered with kind=%d (%T)", what, kind, msg)
		return welcomeMsg{}, false
	}
	return w, true
}

// server.go:415: a document nobody is evicting is opened at once, not waited
// for.
func TestOpeningAFreshDocumentDoesNotWait(t *testing.T) {
	s := NewServer(Config{})
	defer func() { _ = s.Close(context.Background()) }()
	ctx, cancel := context.WithTimeout(context.Background(), boundedWait)
	defer cancel()
	start := time.Now()
	d, err := s.open(ctx, "fresh")
	if err != nil || d == nil {
		t.Fatalf("open = %v, %v", d, err)
	}
	if took := time.Since(start); took > boundedWait/2 {
		t.Errorf("open took %v", took)
	}
}

// server.go:446: a store that cannot be read refuses the join.
func TestAJoinOnAnUnreadableStoreIsRefusedInternally(t *testing.T) {
	s := NewServer(Config{Store: bareStore{err: errors.New("unreadable")}})
	defer func() { _ = s.Close(context.Background()) }()
	r := openRaw(t, s, joinMsg{Document: "d", Site: 1})
	r.refusedAs("unreadable store", errInternal)
}

// server.go:457: what the store holds is what a joiner is given.
func TestAJoinIsGivenWhatTheStoreHolds(t *testing.T) {
	store := NewMemoryStore()
	first := NewServer(Config{Store: store})
	ctx, cancel := context.WithTimeout(context.Background(), boundedWait)
	defer cancel()
	a, aconn := Pipe()
	go func() { _ = first.ServePipe(ctx, aconn) }()
	c, err := Join(ctx, a, ClientConfig{Document: "d", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	body, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, "kept"); err != nil {
		t.Fatal(err)
	}
	// The edit travels asynchronously: wait until the server holds it, seen by
	// a second participant of the same server.
	p, pconn := Pipe()
	go func() { _ = first.ServePipe(ctx, pconn) }()
	probe, err := Join(ctx, p, ClientConfig{Document: "d", Site: 3})
	if err != nil {
		t.Fatal(err)
	}
	pt, err := probe.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(boundedWait); pt.String() != "kept"; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the first server never received the edit")
		}
	}
	_ = probe.Close()
	_ = c.Close()
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	second := NewServer(Config{Store: store})
	defer func() { _ = second.Close(context.Background()) }()
	b, bconn := Pipe()
	go func() { _ = second.ServePipe(ctx, bconn) }()
	d, err := Join(ctx, b, ClientConfig{Document: "d", Site: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	got, err := d.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != "kept" {
		t.Errorf("a second server gave the joiner %q, want %q", got.String(), "kept")
	}
}

// server.go:731 and :732: Authorize is asked, and a refusal is a refusal.
func TestAuthorizeIsAskedAndItsRefusalEndsTheJoin(t *testing.T) {
	var mu sync.Mutex
	asked := 0
	s := NewServer(Config{Authorize: func(context.Context, string, crdt.SiteID) error {
		mu.Lock()
		asked++
		mu.Unlock()
		return errors.New("not you")
	}})
	defer func() { _ = s.Close(context.Background()) }()
	r := openRaw(t, s, joinMsg{Document: "d", Site: 1})
	r.refusedAs("refused by Authorize", errRefused)
	mu.Lock()
	defer mu.Unlock()
	if asked != 1 {
		t.Errorf("Authorize was asked %d times, want 1", asked)
	}
}

// server.go:779: a message the session cannot use ends it.
func TestAMessageTheSessionCannotUseEndsIt(t *testing.T) {
	s := NewServer(Config{})
	defer func() { _ = s.Close(context.Background()) }()
	r := openRaw(t, s, joinMsg{Document: "d", Site: 1})
	if _, ok := r.welcome("join"); !ok {
		return
	}
	if err := r.conn.Send(0x7f, nil); err != nil {
		t.Fatal(err)
	}
	r.refusedAs("an unknown message kind", errInvalid)
}

// server.go:877 and :910: a join is answered with a welcome that carries the
// document.
func TestAJoinIsAnsweredWithTheDocument(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	if err := store.Save(ctx, "paper", purgedSnapshot(t)); err != nil {
		t.Fatal(err)
	}
	s := NewServer(Config{Store: store})
	defer func() { _ = s.Close(context.Background()) }()
	// A joiner that says what it reads is sent the snapshot.
	speaks, _ := Mine().MarshalBinary()
	r := openRaw(t, s, joinMsg{Document: "paper", Site: 7, Speaks: speaks})
	w, ok := r.welcome("join with a purged document")
	if ok && len(w.Snapshot) == 0 {
		t.Error("the welcome carried no snapshot")
	}
}

// server.go:878: a document that cannot serve this joiner refuses it.
func TestAJoinThatCannotBeComposedIsRefused(t *testing.T) {
	store := NewMemoryStore()
	if err := store.Save(context.Background(), "paper", purgedSnapshot(t)); err != nil {
		t.Fatal(err)
	}
	s := NewServer(Config{Store: store})
	defer func() { _ = s.Close(context.Background()) }()
	r := openRaw(t, s, joinMsg{Document: "paper", Site: 7})
	err, ok := r.ended("a purged document and a joiner that says nothing")
	if ok && !errors.Is(err, crdt.ErrPurged) {
		t.Errorf("the session ended with %v, want crdt.ErrPurged", err)
	}
}

// server.go:1240 and :1241: what a joiner says it holds must decode.
func TestAJoinWithAMalformedVersionIsRefused(t *testing.T) {
	s := NewServer(Config{})
	defer func() { _ = s.Close(context.Background()) }()
	r := openRaw(t, s, joinMsg{Document: "d", Site: 1, Have: []byte{0xff, 0xff, 0xff}})
	r.refusedAs("a malformed version", errInvalid)
}

// server.go:1007: operations that do not decode end the session.
func TestOperationsThatDoNotDecodeEndTheSession(t *testing.T) {
	s := NewServer(Config{})
	defer func() { _ = s.Close(context.Background()) }()
	r := openRaw(t, s, joinMsg{Document: "d", Site: 1})
	if _, ok := r.welcome("join"); !ok {
		return
	}
	if err := r.conn.Send(kindOperation, opsMsg{Operations: []byte("garbage")}); err != nil {
		t.Fatal(err)
	}
	r.refusedAs("garbage operations", errInvalid)
}

// server.go:1025 and :1026: AuthorizeOperations is asked about what a session
// sends, and its refusal ends that session.
func TestARefusedBatchOfOperationsEndsTheSession(t *testing.T) {
	var mu sync.Mutex
	asked := 0
	s := NewServer(Config{AuthorizeOperations: func(context.Context, string, crdt.SiteID, []crdt.PartOps) error {
		mu.Lock()
		asked++
		mu.Unlock()
		return errors.New("not these")
	}})
	defer func() { _ = s.Close(context.Background()) }()
	ctx, cancel := context.WithTimeout(context.Background(), boundedWait)
	defer cancel()
	tr, sc := Pipe()
	go func() { _ = s.ServePipe(ctx, sc) }()
	c, err := Join(ctx, tr, ClientConfig{Document: "d", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	body, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, "refused"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.Done():
	case <-time.After(boundedWait):
		t.Errorf("the session was still open %v after a refused batch", boundedWait)
	}
	mu.Lock()
	defer mu.Unlock()
	if asked == 0 {
		t.Error("AuthorizeOperations was never asked")
	}
}

// server.go:1134: closing a participant's queue twice is harmless.
func TestClosingAQueueTwiceIsHarmless(t *testing.T) {
	d := &document{subs: map[*subscriber]struct{}{}}
	sub := &subscriber{out: make(chan wireMsg, 1)}
	noPanic(t, "close twice", func() {
		d.close(sub)
		d.close(sub)
	})
}

// countingStore counts saves; it keeps participants too, so persist asks about
// them.
type countingStore struct {
	*MemoryStore
	mu    sync.Mutex
	saves int
}

func (c *countingStore) Save(ctx context.Context, name string, b []byte) error {
	c.mu.Lock()
	c.saves++
	c.mu.Unlock()
	return c.MemoryStore.Save(ctx, name, b)
}

// server.go:1161 and :1180: a document is saved when it changed and only then;
// a change to the participants alone does not rewrite the document.
func TestPersistSavesTheDocumentOnlyWhenItChanged(t *testing.T) {
	store := &countingStore{MemoryStore: NewMemoryStore()}
	s := NewServer(Config{Store: store})
	defer func() { _ = s.Close(context.Background()) }()
	ctx, cancel := context.WithTimeout(context.Background(), boundedWait)
	defer cancel()
	d, err := s.open(ctx, "d")
	if err != nil {
		t.Fatal(err)
	}
	saves := func() int {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.saves
	}

	d.mu.Lock()
	d.dirty, d.sitesDirty = false, true
	d.mu.Unlock()
	if err := d.persist(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := saves(); n != 0 {
		t.Errorf("a participants-only change saved the document %d times", n)
	}

	d.mu.Lock()
	d.dirty = true
	d.mu.Unlock()
	if err := d.persist(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := saves(); n != 1 {
		t.Errorf("a changed document was saved %d times, want 1", n)
	}
}
