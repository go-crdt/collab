package collab_test

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/collab/collabpb"
	"github.com/go-crdt/crdt"
	"github.com/go-crdt/crdt/awareness"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// Everything a server is sent comes from a participant, and everything a client
// is sent comes from a server. This file drives both sides with a peer that does
// not play along.

// rawSession opens a session with no client-side help, so a test can send
// whatever it likes.
func rawSession(t *testing.T, conn *grpc.ClientConn) collabpb.Collab_SessionClient {
	t.Helper()
	stream, err := collabpb.NewCollabClient(conn).Session(t.Context())
	if err != nil {
		t.Fatalf("Session: %v", err)
	}
	return stream
}

// expectCode drives a session to its end and asserts why it ended.
func expectCode(t *testing.T, stream collabpb.Collab_SessionClient, want codes.Code) {
	t.Helper()
	for {
		_, err := stream.Recv()
		if err == nil {
			continue
		}
		if got := status.Code(err); got != want {
			t.Fatalf("session ended with %v (%v), want %v", got, err, want)
		}
		return
	}
}

func joinMessage(document string, site uint64, have []byte) *collabpb.ClientMessage {
	return &collabpb.ClientMessage{Body: &collabpb.ClientMessage_Join{
		Join: &collabpb.Join{Document: document, Site: site, Have: have},
	}}
}

func TestServerRejectsBadSessions(t *testing.T) {
	_, conn := serve(t, collab.Config{})

	tests := []struct {
		name string
		send []*collabpb.ClientMessage
	}{
		{
			"opening with something other than a join",
			[]*collabpb.ClientMessage{{Body: &collabpb.ClientMessage_Presence{
				Presence: &collabpb.Presence{},
			}}},
		},
		{"opening with an empty message", []*collabpb.ClientMessage{{}}},
		{"joining no document", []*collabpb.ClientMessage{joinMessage("", 1, nil)}},
		{
			"joining with a malformed version vector",
			[]*collabpb.ClientMessage{joinMessage("doc", 1, []byte{0xff, 0xff})},
		},
		{
			"joining twice",
			[]*collabpb.ClientMessage{joinMessage("doc", 1, nil), joinMessage("doc", 1, nil)},
		},
		{
			"sending nothing after joining",
			[]*collabpb.ClientMessage{joinMessage("doc", 1, nil), {}},
		},
		{
			"sending malformed operations",
			[]*collabpb.ClientMessage{joinMessage("doc", 1, nil), {
				Body: &collabpb.ClientMessage_Operations{
					Operations: &collabpb.Operations{Operations: []byte{0x09, 0x09}},
				},
			}},
		},
		{
			"sending malformed presence",
			[]*collabpb.ClientMessage{joinMessage("doc", 1, nil), {
				Body: &collabpb.ClientMessage_Presence{
					Presence: &collabpb.Presence{Update: []byte{0xff}},
				},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := rawSession(t, conn)
			for _, msg := range tt.send {
				if err := stream.Send(msg); err != nil {
					break // the server may have hung up already
				}
			}
			expectCode(t, stream, codes.InvalidArgument)
		})
	}
}

// A session that hangs up before saying anything ends without ceremony.
func TestServerHandlesAnImmediateHangUp(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	stream := rawSession(t, conn)
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("the session did not end")
	}
}

// A presence update older than one already applied changes nothing and is not
// passed on.
func TestStalePresenceIsIgnored(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	watcher := join(t, conn, collab.ClientConfig{Document: "doc", Site: 9})

	stream := rawSession(t, conn)
	if err := stream.Send(joinMessage("doc", 1, nil)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	for _, clock := range []uint64{2, 1} { // the second is stale
		update := awareness.Update{Site: 1, Clock: clock, Cursor: awareness.Cursor{Head: int(clock)}}
		raw, err := update.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&collabpb.ClientMessage{Body: &collabpb.ClientMessage_Presence{
			Presence: &collabpb.Presence{Update: raw},
		}}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	await(t, watcher, "the newer cursor", func() bool {
		for _, p := range watcher.Peers() {
			if p.Site == 1 && p.Cursor.Head == 2 {
				return true
			}
		}
		return false
	})
	// The stale update must not have overwritten the newer one.
	for _, p := range watcher.Peers() {
		if p.Site == 1 && p.Cursor.Head != 2 {
			t.Fatalf("a stale update won: cursor is at %d, want 2", p.Cursor.Head)
		}
	}
}

// failingStore fails whichever half the test asks it to.
type failingStore struct {
	inner    *collab.MemoryStore
	mu       sync.Mutex
	loadErr  error
	saveErr  error
	badBytes []byte
}

func (s *failingStore) Load(ctx context.Context, document string) ([]byte, error) {
	s.mu.Lock()
	loadErr, bad := s.loadErr, s.badBytes
	s.mu.Unlock()
	if loadErr != nil {
		return nil, loadErr
	}
	if bad != nil {
		return bad, nil
	}
	return s.inner.Load(ctx, document)
}

func (s *failingStore) Save(ctx context.Context, document string, snapshot []byte) error {
	s.mu.Lock()
	saveErr := s.saveErr
	s.mu.Unlock()
	if saveErr != nil {
		return saveErr
	}
	return s.inner.Save(ctx, document, snapshot)
}

func (s *failingStore) stopFailing() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveErr = nil
}

func TestServerReportsAStoreThatCannotBeRead(t *testing.T) {
	for _, tt := range []struct {
		name  string
		store *failingStore
	}{
		{"unreadable store", &failingStore{inner: collab.NewMemoryStore(), loadErr: errors.New("disk on fire")}},
		{"corrupt document", &failingStore{inner: collab.NewMemoryStore(), badBytes: []byte("not a snapshot")}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, conn := serve(t, collab.Config{Store: tt.store})
			stream := rawSession(t, conn)
			if err := stream.Send(joinMessage("doc", 1, nil)); err != nil {
				t.Fatalf("Send: %v", err)
			}
			expectCode(t, stream, codes.Internal)
		})
	}
}

// A document that could not be written stays queued, so the next Flush tries
// again rather than losing it.
func TestAFailedSaveIsRetried(t *testing.T) {
	store := &failingStore{inner: collab.NewMemoryStore(), saveErr: errors.New("disk full")}
	srv, conn := serve(t, collab.Config{Store: store})
	ada := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	if err := ada.Insert(0, "work"); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	deadline := time.After(settle)
	for {
		err := srv.Flush(t.Context())
		if err != nil && strings.Contains(err.Error(), "disk full") {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("Flush never reported the failing store (last error %v)", err)
		case <-time.After(10 * time.Millisecond):
		}
	}

	store.stopFailing()
	if err := srv.Flush(t.Context()); err != nil {
		t.Fatalf("Flush after the store recovered: %v", err)
	}
	saved, err := store.Load(t.Context(), "doc")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	doc, err := crdt.Load(9, saved)
	if err != nil {
		t.Fatalf("the retried snapshot is unreadable: %v", err)
	}
	if got, want := doc.String(), "work"; got != want {
		t.Fatalf("the retried snapshot holds %q, want %q", got, want)
	}
}

// hookStore runs a function the first time a document is read, so a test can
// make two sessions open the same document at once on purpose.
type hookStore struct {
	inner *collab.MemoryStore
	mu    sync.Mutex
	fired bool
	hook  func()
}

// Load fires the hook once. The flag is set before the call, not after, because
// the hook opens the same document again and would otherwise re-enter this.
func (s *hookStore) Load(ctx context.Context, document string) ([]byte, error) {
	s.mu.Lock()
	fire := !s.fired && s.hook != nil
	s.fired = true
	s.mu.Unlock()
	if fire {
		s.hook()
	}
	return s.inner.Load(ctx, document)
}

func (s *hookStore) Save(ctx context.Context, document string, snapshot []byte) error {
	return s.inner.Save(ctx, document, snapshot)
}

// Two sessions opening one document at the same moment must end up on the same
// hub, or they would each edit a replica the other never hears about.
func TestSimultaneousOpensShareOneDocument(t *testing.T) {
	store := &hookStore{inner: collab.NewMemoryStore()}
	_, conn := serve(t, collab.Config{Store: store})

	var ready sync.WaitGroup
	ready.Add(1)
	store.hook = func() {
		// Runs inside the first open, before it registers the document: this
		// second participant gets there first.
		defer ready.Done()
		second, err := collab.Join(t.Context(), conn, collab.ClientConfig{Document: "race", Site: 2})
		if err != nil {
			t.Errorf("second Join: %v", err)
			return
		}
		if err := second.Insert(0, "second"); err != nil {
			t.Errorf("second Insert: %v", err)
		}
		t.Cleanup(func() { _ = second.Close() })
	}

	first := join(t, conn, collab.ClientConfig{Document: "race", Site: 1})
	ready.Wait()
	awaitText(t, first, "second")
}

// A participant that stops reading is disconnected rather than allowed to hold
// up the document or be served state that is quietly out of date.
func TestAParticipantThatCannotKeepUpIsDropped(t *testing.T) {
	_, conn := serve(t, collab.Config{Backlog: 1})

	// A raw session that joins and then never reads again.
	idle := rawSession(t, conn)
	if err := idle.Send(joinMessage("doc", 1, nil)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := idle.Recv(); err != nil {
		t.Fatalf("Recv: %v", err)
	}

	busy := join(t, conn, collab.ClientConfig{Document: "doc", Site: 2})
	dropped := make(chan error, 1)
	go func() {
		for {
			if _, err := idle.Recv(); err != nil {
				dropped <- err
				return
			}
		}
	}()

	// Keep writing until the idle participant's queue overflows.
	deadline := time.After(settle)
	for {
		select {
		case err := <-dropped:
			if got := status.Code(err); got != codes.ResourceExhausted {
				t.Fatalf("the idle participant was dropped with %v (%v), want ResourceExhausted", got, err)
			}
			return
		case <-deadline:
			t.Fatal("an idle participant was never dropped")
		default:
		}
		if err := busy.Insert(0, strings.Repeat("x", 64)); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}
}

// stubServer answers a session however the test says, so the client can be shown
// things a real server would never send.
type stubServer struct {
	collabpb.UnimplementedCollabServer
	reply func(stream collabpb.Collab_SessionServer) error
}

func (s *stubServer) Session(stream collabpb.Collab_SessionServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return s.reply(stream)
}

// serveStub starts a server that replies with whatever reply sends.
func serveStub(t *testing.T, reply func(stream collabpb.Collab_SessionServer) error) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	collabpb.RegisterCollabServer(gs, &stubServer{reply: reply})
	go func() { _ = gs.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///stub",
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
	return conn
}

// welcomeOf builds a server's opening message.
func welcomeOf(w *collabpb.Welcome) *collabpb.ServerMessage {
	return &collabpb.ServerMessage{Body: &collabpb.ServerMessage_Welcome{Welcome: w}}
}

func TestJoinRejectsABadWelcome(t *testing.T) {
	tests := []struct {
		name  string
		reply func(stream collabpb.Collab_SessionServer) error
	}{
		{
			"no welcome at all",
			func(stream collabpb.Collab_SessionServer) error {
				return stream.Send(operationsOf(nil))
			},
		},
		{
			"an unreadable document",
			func(stream collabpb.Collab_SessionServer) error {
				return stream.Send(welcomeOf(&collabpb.Welcome{Snapshot: []byte("rubbish")}))
			},
		},
		{
			"unusable operations",
			func(stream collabpb.Collab_SessionServer) error {
				return stream.Send(welcomeOf(&collabpb.Welcome{Operations: []byte{0x09, 0x09}}))
			},
		},
		{
			"malformed presence",
			func(stream collabpb.Collab_SessionServer) error {
				return stream.Send(welcomeOf(&collabpb.Welcome{Presence: [][]byte{{0xff}}}))
			},
		},
		{
			"a malformed version vector",
			func(stream collabpb.Collab_SessionServer) error {
				return stream.Send(welcomeOf(&collabpb.Welcome{Version: []byte{0xff, 0xff}}))
			},
		},
		{
			"nothing, then a hang-up",
			func(stream collabpb.Collab_SessionServer) error { return nil },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := serveStub(t, tt.reply)
			if c, err := collab.Join(t.Context(), conn, collab.ClientConfig{Document: "doc", Site: 1}); err == nil {
				_ = c.Close()
				t.Fatal("Join accepted a welcome it should have refused")
			}
		})
	}
}

func operationsOf(raw []byte) *collabpb.ServerMessage {
	return &collabpb.ServerMessage{Body: &collabpb.ServerMessage_Operations{
		Operations: &collabpb.Operations{Operations: raw},
	}}
}

func presenceOf(raw []byte) *collabpb.ServerMessage {
	return &collabpb.ServerMessage{Body: &collabpb.ServerMessage_Presence{
		Presence: &collabpb.Presence{Update: raw},
	}}
}

// A session that goes wrong mid-flight ends, and says why.
func TestSessionEndsOnBadServerMessages(t *testing.T) {
	tests := []struct {
		name string
		then *collabpb.ServerMessage
	}{
		{"unusable operations", operationsOf([]byte{0x09, 0x09})},
		{"malformed presence", presenceOf([]byte{0xff})},
		{"a second welcome", welcomeOf(&collabpb.Welcome{})},
		{"an empty message", &collabpb.ServerMessage{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := serveStub(t, func(stream collabpb.Collab_SessionServer) error {
				if err := stream.Send(welcomeOf(&collabpb.Welcome{})); err != nil {
					return err
				}
				if err := stream.Send(tt.then); err != nil {
					return err
				}
				<-stream.Context().Done()
				return nil
			})
			c, err := collab.Join(t.Context(), conn, collab.ClientConfig{Document: "doc", Site: 1})
			if err != nil {
				t.Fatalf("Join: %v", err)
			}
			defer c.Close()

			select {
			case <-c.Done():
			case <-time.After(settle):
				t.Fatal("the session did not end")
			}
			if c.Err() == nil {
				t.Fatal("the session ended without saying why")
			}
			// An edit after the session ended is refused, with the reason.
			if err := c.Insert(0, "x"); err == nil {
				t.Fatal("an edit was accepted after the session ended")
			}
		})
	}
}

// A session that ends cleanly still refuses later edits.
func TestEditingAfterClose(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	c := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	if err := c.Insert(0, "before"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Closing twice is harmless.
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	for _, tt := range []struct {
		name string
		run  func() error
	}{
		{"insert", func() error { return c.Insert(0, "after") }},
		{"delete", func() error { return c.Delete(0, 1) }},
		{"cursor", func() error { return c.SetCursor(awareness.Cursor{}, nil) }},
	} {
		if err := tt.run(); err == nil {
			t.Errorf("%s was accepted after Close", tt.name)
		}
	}
}

func TestClientRejectsBadConfiguration(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	if _, err := collab.Join(t.Context(), conn, collab.ClientConfig{Site: 1}); err == nil {
		t.Fatal("Join accepted a session with no document")
	}
	if _, err := collab.Join(t.Context(), conn, collab.ClientConfig{
		Document: "doc", Site: 1, Resume: []byte("not a snapshot"),
	}); err == nil {
		t.Fatal("Join accepted an unreadable snapshot to resume from")
	}
}

// An edit outside the document is refused locally and never reaches the wire.
func TestOutOfRangeEditsAreLocal(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	ada := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	witness := join(t, conn, collab.ClientConfig{Document: "doc", Site: 2})

	if err := ada.Insert(5, "x"); !errors.Is(err, crdt.ErrOutOfRange) {
		t.Fatalf("Insert past the end = %v, want ErrOutOfRange", err)
	}
	if err := ada.Delete(0, 1); !errors.Is(err, crdt.ErrOutOfRange) {
		t.Fatalf("Delete past the end = %v, want ErrOutOfRange", err)
	}
	// An edit that changes nothing sends nothing, and the session is unharmed.
	if err := ada.Insert(0, ""); err != nil {
		t.Fatalf("empty Insert: %v", err)
	}
	if err := ada.Insert(0, "fine"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	awaitText(t, witness, "fine")
	if err := ada.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
	if got := ada.Version(); got.Get(1) == 0 {
		t.Fatalf("Version() = %v, want the local edits to be recorded", got)
	}
}

// Changes coalesces: a participant that is not watching does not build up a
// queue of wake-ups, and one that starts watching still learns something
// changed.
func TestChangesCoalesce(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	ada := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	grace := join(t, conn, collab.ClientConfig{Document: "doc", Site: 2})

	for i := range 20 {
		if err := ada.Insert(i, "x"); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}
	awaitText(t, grace, strings.Repeat("x", 20))

	// Drain whatever is queued; there must be at most one.
	drained := 0
	for {
		select {
		case <-grace.Changes():
			drained++
			continue
		default:
		}
		break
	}
	if drained > 1 {
		t.Fatalf("Changes queued %d wake-ups, want at most 1", drained)
	}
}

// fakeConn hands out a stream the test controls, so the failures that happen
// while a session is still being set up can be provoked deliberately instead of
// waited for.
type fakeConn struct {
	stream grpc.ClientStream
	err    error
}

func (c fakeConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return c.err
}

func (c fakeConn) NewStream(ctx context.Context, _ *grpc.StreamDesc, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.stream, nil
}

type fakeStream struct {
	ctx     context.Context
	sendErr error
}

func (s fakeStream) Header() (metadata.MD, error) { return nil, nil }
func (s fakeStream) Trailer() metadata.MD         { return nil }
func (s fakeStream) CloseSend() error             { return nil }
func (s fakeStream) Context() context.Context     { return s.ctx }
func (s fakeStream) SendMsg(any) error            { return s.sendErr }
func (s fakeStream) RecvMsg(any) error            { return io.EOF }

func TestJoinReportsAConnectionThatWillNotOpen(t *testing.T) {
	refused := errors.New("no route to host")
	tests := []struct {
		name string
		conn grpc.ClientConnInterface
		want error
	}{
		{"the stream cannot be opened", fakeConn{err: refused}, refused},
		{
			"the join cannot be sent",
			fakeConn{stream: fakeStream{ctx: t.Context(), sendErr: refused}},
			refused,
		},
		{
			"the server hangs up before answering",
			fakeConn{stream: fakeStream{ctx: t.Context()}},
			io.EOF,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := collab.Join(t.Context(), tt.conn, collab.ClientConfig{Document: "doc", Site: 1})
			if !errors.Is(err, tt.want) {
				t.Fatalf("Join = %v, want %v", err, tt.want)
			}
		})
	}
}
