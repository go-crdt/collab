//go:build (js && wasm) || !js

package collab

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/go-crdt/crdt"
)

// A snapshot in a format this build has no reader for is recognised, and one it
// can read is not.
//
// The question is asked by trying, because a composite's own version byte does
// not say what the parts inside it are written in -- a composite whose text is
// in a dead format has a perfectly current outer version.
func TestAnUnreadableSnapshotIsRecognisedByTrying(t *testing.T) {
	seed := crdt.NewComposite(1)
	part, err := seed.Text("t")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Insert(0, "written by a server older than this"); err != nil {
		t.Fatal(err)
	}
	good := seed.Snapshot()
	old := stampTextVersion(t, good, 6)

	local := crdt.NewComposite(2)
	for _, c := range []struct {
		name string
		w    welcomeMsg
		want bool
	}{
		{"a snapshot this build reads", welcomeMsg{Snapshot: good}, false},
		{"a snapshot in a format that is gone", welcomeMsg{Snapshot: old}, true},
		{"operations, which carry no version", welcomeMsg{Operations: []byte{1, 2, 3}}, false},
		{"an empty welcome", welcomeMsg{}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := unreadable(local, c.w); got != c.want {
				t.Fatalf("unreadable = %v, want %v", got, c.want)
			}
		})
	}

	// Damaged bytes are not an old format: a participant must not ask again for
	// a document the server cannot send it either way.
	damaged := append([]byte(nil), good...)
	damaged = damaged[:len(damaged)-3]
	if unreadable(local, welcomeMsg{Snapshot: damaged}) {
		t.Fatal("truncated bytes were taken for an old format")
	}
}

// Saying "I hold nothing" is not the same as saying nothing: only the first
// takes the operations branch, on any server, old or new.
func TestAnEmptyVersionAsksForTheHistory(t *testing.T) {
	have, err := crdt.CompositeVersion{}.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if len(have) == 0 {
		t.Fatal("an empty version encodes to nothing, so it cannot be told from silence")
	}

	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(t.Context()) })
	doc, _, err := srv.openAndJoin(t.Context(), joinMsg{Document: "d", Site: 9})
	if err != nil {
		t.Fatal(err)
	}
	text, err := doc.doc.Text("t")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := text.Insert(0, "a document worth fetching"); err != nil {
		t.Fatal(err)
	}

	// Said nothing: a snapshot, because this build's Mine() is not sent here.
	quiet, err := doc.join(joinMsg{Site: 2})
	if err != nil {
		t.Fatal(err)
	}
	if w := (<-quiet.out).msg.(welcomeMsg); len(w.Operations) == 0 {
		t.Fatal("a participant that said nothing was sent no operations")
	}

	// Said "I hold nothing": operations, and they carry the whole document.
	asked, err := doc.join(joinMsg{Site: 3, Have: have})
	if err != nil {
		t.Fatal(err)
	}
	w := (<-asked.out).msg.(welcomeMsg)
	if len(w.Snapshot) > 0 {
		t.Fatal("saying it holds nothing still got a snapshot")
	}
	ops, err := crdt.ParsePartOps(w.Operations)
	if err != nil {
		t.Fatal(err)
	}
	fresh := crdt.NewComposite(4)
	if err := fresh.Apply(ops...); err != nil {
		t.Fatal(err)
	}
	got, err := fresh.Text("t")
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != "a document worth fetching" {
		t.Fatalf("replaying the history reads %q", got.String())
	}
}

func stampTextVersion(t *testing.T, snapshot []byte, version byte) []byte {
	t.Helper()
	out := append([]byte(nil), snapshot...)
	at := bytes.Index(out[4:], []byte("crdt"))
	if at < 0 {
		t.Fatal("no text part in the snapshot")
	}
	out[4+at+4] = version
	if _, err := crdt.LoadComposite(2, out); !errors.Is(err, crdt.ErrUnknownFormat) {
		t.Fatalf("the fixture is not unreadable: %v", err)
	}
	return out
}

// And the whole sequence: an old server sends a snapshot this build cannot read,
// the participant asks again saying it holds nothing, and the document arrives
// as operations on a second connection.
//
// A second join is not allowed on one connection -- the server closes it -- so
// the count of connections opened is part of what is asserted.
func TestAParticipantAsksAgainOnAFreshConnection(t *testing.T) {
	seed := crdt.NewComposite(1)
	part, err := seed.Text("t")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Insert(0, "written by a server older than this"); err != nil {
		t.Fatal(err)
	}
	ops, err := crdt.AppendPartOps(nil, seed.OpsSince(nil))
	if err != nil {
		t.Fatal(err)
	}
	version, err := seed.Version().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	tp := &scriptedTransport{welcomes: []welcomeMsg{
		{Snapshot: stampTextVersion(t, seed.Snapshot(), 6), Version: version},
		{Operations: ops, Version: version},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	c, err := joinOn(ctx, cancel, tp, ClientConfig{Document: "d", Site: 2})
	if err != nil {
		cancel()
		t.Fatalf("the participant was locked out instead of asking again: %v", err)
	}
	// What Join does after joinOn; without it nothing ever finishes.
	go c.receive(c.conn, c.finished)
	t.Cleanup(func() { _ = c.Close() })

	text, err := c.Text("t")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := text.String(), "written by a server older than this"; got != want {
		t.Fatalf("the participant reads %q, want %q", got, want)
	}
	if tp.opened != 2 {
		t.Fatalf("opened %d connections, want 2", tp.opened)
	}
	if len(tp.joins) != 2 || len(tp.joins[0].Have) != 0 || len(tp.joins[1].Have) == 0 {
		t.Fatalf("joins carried Have %v then %v; want silence then 'I hold nothing'",
			tp.joins[0].Have, tp.joins[1].Have)
	}
}

// A transport that hands out the welcomes it was given, one per connection, and
// remembers the joins it was sent.
type scriptedTransport struct {
	welcomes []welcomeMsg
	// attempts counts every open, opened only the ones that succeeded. The two
	// differ exactly when a retry finds nothing to open, which is a case worth
	// telling apart from never having retried.
	attempts int
	opened   int
	joins    []joinMsg
}

func (t *scriptedTransport) open(context.Context) (carrierConn, error) {
	t.attempts++
	if t.opened >= len(t.welcomes) {
		return nil, ErrTransport
	}
	c := &scriptedConn{t: t, welcome: t.welcomes[t.opened]}
	t.opened++
	return c, nil
}

type scriptedConn struct {
	t       *scriptedTransport
	welcome welcomeMsg
	sent    bool
}

func (c *scriptedConn) Send(kind byte, msg any) error {
	if kind == kindJoin {
		c.t.joins = append(c.t.joins, msg.(joinMsg))
	}
	return nil
}

func (c *scriptedConn) Recv() (byte, any, error) {
	if !c.sent {
		c.sent = true
		return kindWelcome, c.welcome, nil
	}
	// One welcome and nothing after it: what is under test is the join, not the
	// session that follows it.
	return 0, nil, ErrTransport
}

func (c *scriptedConn) Close() error { return nil }

// And when the second connection cannot be opened, the participant is told why
// rather than left holding a document it could not read.
func TestAskingAgainCanItselfFail(t *testing.T) {
	seed := crdt.NewComposite(1)
	part, err := seed.Text("t")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Insert(0, "unreachable after the first try"); err != nil {
		t.Fatal(err)
	}
	// One welcome only: the retry finds nothing to open.
	tp := &scriptedTransport{welcomes: []welcomeMsg{
		{Snapshot: stampTextVersion(t, seed.Snapshot(), 6)},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := joinOn(ctx, cancel, tp, ClientConfig{Document: "d", Site: 2})
	if !errors.Is(err, ErrTransport) {
		if c != nil {
			_ = c.Close()
		}
		t.Fatalf("joinOn = %v, want the transport error from the second attempt", err)
	}
	if tp.attempts != 2 {
		t.Fatalf("tried %d connections, want 2: the second attempt is the retry", tp.attempts)
	}
}
