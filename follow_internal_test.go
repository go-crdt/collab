//go:build !js

package collab

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// A transport that fails where it is told to, so that every way a link can end
// is a case rather than a hope. The failure paths are most of Follow, and they
// are the ones an operator meets: a peer that goes away mid-stream is the
// normal condition of a link between datacentres, not an exceptional one.
type brokenPeer struct {
	openErr  error
	sendErr  error // fails the join
	recvErr  error // fails the welcome
	firstMsg func() (byte, any, error)
	then     func() (byte, any, error)
	sends    func(kind byte, msg any) error
	closed   bool
	// ctx is the one the link was opened with, so that Recv blocks the way a
	// real carrier does — until there is something, or until the link is torn
	// down. A fake that blocks forever regardless would hang the test rather
	// than test anything.
	ctx context.Context
}

func (p *brokenPeer) open(ctx context.Context) (carrierConn, error) {
	if p.openErr != nil {
		return nil, p.openErr
	}
	p.ctx = ctx
	return p, nil
}

func (p *brokenPeer) Send(kind byte, msg any) error {
	if p.sendErr != nil {
		return p.sendErr
	}
	if p.sends != nil {
		return p.sends(kind, msg)
	}
	return nil
}

func (p *brokenPeer) Recv() (byte, any, error) {
	if p.recvErr != nil {
		return 0, nil, p.recvErr
	}
	if p.firstMsg != nil {
		f := p.firstMsg
		p.firstMsg = nil
		return f()
	}
	if p.then != nil {
		f := p.then
		p.then = nil
		return f()
	}
	// A peer with nothing more to say: block until the link is torn down.
	<-p.ctx.Done()
	return 0, nil, p.ctx.Err()
}

func (p *brokenPeer) Close() error { p.closed = true; return nil }

func followed(t *testing.T, peer *brokenPeer) error {
	t.Helper()
	s := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	return s.Follow(ctx, peer, "doc", 42)
}

func welcomeWith(w welcomeMsg) func() (byte, any, error) {
	return func() (byte, any, error) { return kindWelcome, w, nil }
}

func TestFollowEndsOnEveryWayThePeerCanFail(t *testing.T) {
	boom := errors.New("boom")

	// Operations from a replica of its own, so a welcome can carry something
	// real and a later message can carry something unusable.
	made := crdt.NewComposite(3)
	text, err := made.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	ops, err := text.Insert(0, "x")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := crdt.AppendPartOps(nil, []crdt.PartOps{{
		Part: crdt.Part{Kind: crdt.PartText, Name: "body"}, Text: ops,
	}})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		peer *brokenPeer
	}{
		{"the connection cannot be opened", &brokenPeer{openErr: boom}},
		{"the join cannot be sent", &brokenPeer{sendErr: boom}},
		{"the welcome never arrives", &brokenPeer{recvErr: boom}},
		{"something other than a welcome arrives", &brokenPeer{
			firstMsg: func() (byte, any, error) { return kindOperation, opsMsg{}, nil },
		}},
		{"a welcome that is not one", &brokenPeer{
			firstMsg: func() (byte, any, error) { return kindWelcome, opsMsg{}, nil },
		}},
		{"a welcome carrying a snapshot, which was not what was asked for", &brokenPeer{
			firstMsg: welcomeWith(welcomeMsg{Snapshot: []byte("whatever")}),
		}},
		{"a welcome carrying operations that are not operations", &brokenPeer{
			firstMsg: welcomeWith(welcomeMsg{Operations: []byte{0x09, 0x09}}),
		}},
		{"the stream ends after the welcome", &brokenPeer{
			firstMsg: welcomeWith(welcomeMsg{}),
			then:     func() (byte, any, error) { return 0, nil, boom },
		}},
		{"an operations message that is not one", &brokenPeer{
			firstMsg: welcomeWith(welcomeMsg{}),
			then:     func() (byte, any, error) { return kindOperation, welcomeMsg{}, nil },
		}},
		{"operations that cannot be applied", &brokenPeer{
			firstMsg: welcomeWith(welcomeMsg{}),
			then: func() (byte, any, error) {
				return kindOperation, opsMsg{Operations: []byte{0x09, 0x09}}, nil
			},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := followed(t, tt.peer); err == nil {
				t.Fatal("Follow returned no error")
			}
		})
	}

	// And one that is not a failure: a peer that sends presence, which a link
	// has no use for and steps over rather than refusing — a peer running a
	// later build must not be able to end a link by saying something new.
	t.Run("presence is ignored rather than refused", func(t *testing.T) {
		delivered := false
		peer := &brokenPeer{
			firstMsg: welcomeWith(welcomeMsg{Operations: raw}),
			then: func() (byte, any, error) {
				delivered = true
				return kindPresence, presenceMsg{Update: []byte("ignored")}, nil
			},
		}
		// The link steps over the presence message and blocks for the next,
		// which never comes: it ends on the context rather than on the
		// message, which is the point.
		if err := followed(t, peer); err == nil {
			t.Fatal("Follow returned no error when the link was torn down")
		}
		if !delivered {
			t.Fatal("the presence message was never delivered")
		}
	})
}

// A store that will not give a document up, so that the link fails where it
// asks for the local replica rather than where it talks to the peer.
type refusingStore struct{ err error }

func (s refusingStore) Load(context.Context, string) ([]byte, error) { return nil, s.err }
func (s refusingStore) Save(context.Context, string, []byte) error   { return s.err }

func TestFollowEndsWhenTheLocalDocumentCannotBeOpened(t *testing.T) {
	srv := NewServer(Config{Store: refusingStore{err: errors.New("no")}})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	if err := srv.Follow(t.Context(), &brokenPeer{}, "doc", 42); err == nil {
		t.Fatal("Follow returned no error when the document could not be opened")
	}
}

// A link whose peer stops reading ends, rather than filling a queue nobody
// drains — and it ends promptly, which is the part worth testing.
//
// The failure is on the way out, and the link is blocked waiting for the peer
// to say something on the way in. A peer that has stopped reading has usually
// stopped writing too, so waiting for it would be waiting forever. The send
// side cancels on its way out, which is what turns a broken link into a
// returned error instead of a goroutine nobody notices.
//
// The local edit is made by somebody other than the link, because a broadcast
// skips the participant it came from: an operation the link itself delivered
// would never come back to it, and the test would pass without sending
// anything.
func TestFollowEndsPromptlyWhenItCannotSendOnwards(t *testing.T) {
	boom := errors.New("boom")
	sent := 0
	peer := &brokenPeer{
		firstMsg: welcomeWith(welcomeMsg{}),
		sends: func(byte, any) error {
			sent++
			if sent == 1 { // the join
				return nil
			}
			return boom
		},
	}

	s := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	// A generous deadline: if the link only ended on it, this would take that
	// long, and the point is that it does not.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- s.Follow(ctx, peer, "doc", 42) }()

	// Wait for the link to be holding the document, then write to it as
	// somebody else.
	var doc *document
	deadline := time.Now().Add(5 * time.Second)
	for {
		var err error
		doc, err = s.open(t.Context(), "doc")
		if err != nil {
			t.Fatal(err)
		}
		doc.mu.Lock()
		n := len(doc.subs)
		doc.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the link never joined the document")
		}
		time.Sleep(5 * time.Millisecond)
	}

	made := crdt.NewComposite(3)
	text, err := made.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	ops, err := text.Insert(0, "x")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := crdt.AppendPartOps(nil, []crdt.PartOps{{
		Part: crdt.Part{Kind: crdt.PartText, Name: "body"}, Text: ops,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.applyOperations(nil, raw); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Follow returned no error when it could not send onwards")
		}
		if !errors.Is(err, boom) {
			t.Fatalf("Follow reported %v, want the send failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Follow did not end when its send failed")
	}
	if took := time.Since(started); took > 2*time.Second {
		t.Fatalf("Follow took %v to notice a failed send", took)
	}
}
