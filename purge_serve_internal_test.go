//go:build !js

package collab

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// A purge is what a replica does to a document nobody can read any more: the
// deleted characters go and their identities stay. What goes with them is the
// history — a purged run is in no OpsSince at all — so a replica that has
// purged cannot answer a peer that is behind the purge by sending it the
// difference, and crdt says so through [crdt.Composite.CanServe].
//
// Nothing in collab could purge and nothing in collab asked. A purged document
// reaches a server through its store — an operator's gitstore, a MultiStore
// merge, a pgstore row, a Resume snapshot from a crdt-level tool — and from
// there every join of it was answered with a history full of holes. The peer
// parked everything that followed and said nothing, for ever, and the pile grew
// with every keystroke on the server that had purged.
//
// These are the doors that were open, and what closes each of them.

// purgedSnapshot is a composite whose "body" once read "hello world", had
// "hello " deleted by the site that wrote it, and then purged — so the "world"
// block that survives names an origin that is gone. It is what an operator's
// store holds after a purge, and CanServe refuses to answer the empty version
// for it, which is the version a fresh replica asks with.
func purgedSnapshot(t *testing.T) []byte {
	t.Helper()
	return purgedDoc(t).Snapshot()
}

// purgedDoc is that document, before it is written down, for a probe that needs
// a second replica holding it and more.
func purgedDoc(t *testing.T) *crdt.Composite {
	t.Helper()
	one := crdt.NewComposite(1)
	body, err := one.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	two := crdt.NewComposite(2)
	other, err := two.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	hello, err := body.Insert(0, "hello ")
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Apply(hello...); err != nil {
		t.Fatal(err)
	}
	// "world" from the second site, so that its origin is a character of the
	// first site's — which is what a purge of "hello " takes away.
	world, err := other.Insert(6, "world")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Apply(world...); err != nil {
		t.Fatal(err)
	}
	if _, err := body.Delete(0, 6); err != nil {
		t.Fatal(err)
	}
	if n := body.Purge(); n != 6 {
		t.Fatalf("the purge discarded %d characters, want 6", n)
	}
	// The premise, checked rather than assumed: this document cannot answer a
	// replica that holds nothing.
	if err := one.CanServe(nil); !errors.Is(err, crdt.ErrPurged) {
		t.Fatalf("CanServe(nil) on the purged document = %v, want ErrPurged", err)
	}
	return one
}

// purgedAndAhead is the same document with more typed into it, for the peer a
// replica holding the purged one can still be caught up by.
func purgedAndAhead(t *testing.T, more string) []byte {
	t.Helper()
	c := purgedDoc(t)
	body, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(body.Len(), more); err != nil {
		t.Fatal(err)
	}
	return c.Snapshot()
}

// writtenSnapshot is an ordinary document from a site of its own, for the
// replica a seeding link must not discard.
func writtenSnapshot(t *testing.T, site crdt.SiteID, text string) []byte {
	t.Helper()
	c := crdt.NewComposite(site)
	body, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, text); err != nil {
		t.Fatal(err)
	}
	return c.Snapshot()
}

// serverText asks a server's own replica what it holds and how much it has
// parked, which is the fact a witness's echo cannot tell you.
func serverText(s *Server, name, part string) (string, int) {
	s.mu.Lock()
	d := s.docs[name]
	s.mu.Unlock()
	if d == nil {
		return "", -1
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	b, err := d.doc.Text(part)
	if err != nil {
		return "", -1
	}
	return b.String(), d.doc.Pending()
}

// serverSubs is how many sessions a server holds for a document.
func serverSubs(s *Server, name string) int {
	s.mu.Lock()
	d := s.docs[name]
	s.mu.Unlock()
	if d == nil {
		return -1
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.subs)
}

// dialPipe dials a server in this process, one session per call.
func dialPipe(s *Server, sessions context.Context) Dialer {
	return func(context.Context) (Transport, error) {
		transport, serverEnd := Pipe()
		go func() { _ = s.ServePipe(sessions, serverEnd) }()
		return transport, nil
	}
}

// awaitTrue waits for something to become true, and says what it was waiting
// for when it does not.
func awaitTrue(t *testing.T, what string, within time.Duration, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(within); time.Now().Before(deadline); {
		if ok() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// awaitHandle waits for a handle to read exactly this, and names what it saw.
func awaitHandle(t *testing.T, what string, h *Text, want string, within time.Duration) {
	t.Helper()
	for deadline := time.Now().Add(within); time.Now().Before(deadline); {
		if h.String() == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: want %q, have %q", what, want, h.String())
}

// textHandle takes the "body" handle a probe reads or types in.
func textHandle(t *testing.T, c *Client) *Text {
	t.Helper()
	body, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// joinedTo opens a document on a server and joins it, for a probe that needs
// the document and its own subscriber rather than a client.
func joinedTo(t *testing.T, s *Server, name string, site crdt.SiteID) (*document, *subscriber) {
	t.Helper()
	d, sub, err := s.openAndJoin(t.Context(), joinMsg{Document: name, Site: uint64(site)})
	if err != nil {
		t.Fatalf("openAndJoin(%q, site %d): %v", name, site, err)
	}
	t.Cleanup(func() { d.leave(context.WithoutCancel(t.Context()), sub) })
	return d, sub
}

// A second datacentre following a peer that has purged is seeded with the
// peer's document instead of being sent a history it cannot use.
//
// A link always says what it holds, so it always took the difference branch,
// and the difference across a purge does not exist. Before this the link came
// up, reported itself up, and parked everything for ever: a witness on the
// follower never saw the document, and every later keystroke on the peer joined
// the pile.
//
// The barrier here is a witness on the FOLLOWER reading what an author typed on
// the PEER — a second participant on the far server, not a client reading back
// its own echo — and the follower's own replica is asked what it holds and how
// much it has parked.
func TestALinkIsSeededByAPeerThatHasPurgedPastIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewMemoryStore()
	if err := store.Save(ctx, "paper", purgedSnapshot(t)); err != nil {
		t.Fatal(err)
	}
	paris := NewServer(Config{Store: store})
	lyon := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = paris.Close(context.Background()); _ = lyon.Close(context.Background()) })

	go func() {
		_ = lyon.FollowWithRetry(ctx, dialPipe(paris, ctx), "paper", 9001,
			RetryPolicy{Wait: time.Millisecond, Ceiling: 10 * time.Millisecond})
	}()
	awaitTrue(t, "Lyon's own replica to hold the peer's document", 10*time.Second, func() bool {
		text, _ := serverText(lyon, "paper", "body")
		return text == "world"
	})

	witness := textHandle(t, participant(t, lyon, ClientConfig{Document: "paper", Site: 2}))
	awaitHandle(t, "a witness joining Lyon after the seed", witness, "world", 10*time.Second)

	author := textHandle(t, participant(t, paris, ClientConfig{Document: "paper", Site: 3}))
	if err := author.Insert(author.Len(), "!!!"); err != nil {
		t.Fatal(err)
	}
	awaitHandle(t, "the witness on Lyon to see what was typed on Paris", witness, "world!!!", 10*time.Second)

	text, pending := serverText(lyon, "paper", "body")
	if pending != 0 {
		t.Fatalf("Lyon holds %q with %d operations parked, want none", text, pending)
	}
}

// A participant resuming with a replica of its own that holds nothing is sent
// the document, not a hole.
//
// Resume is how an editor opened offline hands back what it had, and an empty
// replica still says "I hold nothing" out loud — which is a version, which took
// the difference branch. It said what snapshot formats it reads in the same
// breath, and there is nothing of its own to lose, so it is sent one.
func TestAResumingParticipantWithNothingOfItsOwnIsSeeded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewMemoryStore()
	if err := store.Save(ctx, "paper", purgedSnapshot(t)); err != nil {
		t.Fatal(err)
	}
	paris := NewServer(Config{Store: store})
	t.Cleanup(func() { _ = paris.Close(context.Background()) })

	transport, serverEnd := Pipe()
	go func() { _ = paris.ServePipe(ctx, serverEnd) }()
	resumed, err := Join(ctx, transport, ClientConfig{
		Document: "paper", Site: 6, Resume: crdt.NewComposite(6).Snapshot(),
	})
	if err != nil {
		t.Fatalf("Join with Resume: %v", err)
	}
	t.Cleanup(func() { _ = resumed.Close() })
	awaitHandle(t, "the resuming participant", textHandle(t, resumed), "world", 10*time.Second)
}

// A participant resuming with work of its own is refused, and the refusal names
// the purge.
//
// This is the half that makes the fallback safe. A snapshot REPLACES a replica,
// so answering this one with a snapshot would drop whatever it wrote while
// there was nowhere to send it — silently, which is the failure being fixed
// rather than a second version of it. The server sends one only to a peer whose
// version it already contains, and refuses everybody else out loud.
func TestAResumingParticipantWithWorkIsRefusedRatherThanServedAHole(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewMemoryStore()
	if err := store.Save(ctx, "paper", purgedSnapshot(t)); err != nil {
		t.Fatal(err)
	}
	paris := NewServer(Config{Store: store})
	t.Cleanup(func() { _ = paris.Close(context.Background()) })

	transport, serverEnd := Pipe()
	served := make(chan error, 1)
	go func() { served <- paris.ServePipe(ctx, serverEnd) }()
	c, err := Join(ctx, transport, ClientConfig{
		Document: "paper", Site: 6, Resume: writtenSnapshot(t, 6, "written while offline"),
	})
	if err == nil {
		_ = c.Close()
		t.Fatal("the join was accepted, so the offline work was about to be discarded or parked")
	}
	select {
	case serverErr := <-served:
		if !errors.Is(serverErr, crdt.ErrPurged) {
			t.Fatalf("the server ended the session with %v, want an error carrying crdt.ErrPurged", serverErr)
		}
		if !strings.Contains(serverErr.Error(), "purged") || !strings.Contains(serverErr.Error(), "reseed") {
			t.Fatalf("the refusal does not name the purge and the remedy: %v", serverErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the server session did not end")
	}
}

// A joiner that says nothing at all is refused rather than sent a hole.
//
// It said neither what it holds nor what it reads, so there is no difference to
// send it and no snapshot it has said it can read. Before this it took the
// third branch, OpsSince(nil), and was handed the whole history minus what the
// purge took.
func TestAJoinerThatSaysNothingIsRefusedByAPurgedDocument(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewMemoryStore()
	if err := store.Save(ctx, "paper", purgedSnapshot(t)); err != nil {
		t.Fatal(err)
	}
	paris := NewServer(Config{Store: store})
	t.Cleanup(func() { _ = paris.Close(context.Background()) })

	transport, serverEnd := Pipe()
	served := make(chan error, 1)
	go func() { served <- paris.ServePipe(ctx, serverEnd) }()
	conn, err := transport.open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.Send(kindJoin, joinMsg{Document: "paper", Site: 7}); err != nil {
		t.Fatal(err)
	}
	if kind, msg, err := conn.Recv(); err == nil {
		t.Fatalf("the join was answered with kind=%d (%T) instead of being refused", kind, msg)
	}
	select {
	case serverErr := <-served:
		if !errors.Is(serverErr, crdt.ErrPurged) {
			t.Fatalf("the server ended the session with %v, want an error carrying crdt.ErrPurged", serverErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the server session did not end")
	}
}

// A joiner that says what it holds but not what it reads is refused too.
//
// That is an older build rejoining, or a carrier that dropped the
// advertisement. There is no difference to send it and no snapshot it has said
// it can read, so the answer is a refusal that names the purge — the same rule
// as [Capabilities.Accepts]: silence is not acceptance.
func TestARejoinerThatSaysNothingItReadsIsRefusedByAPurgedDocument(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewMemoryStore()
	if err := store.Save(ctx, "paper", purgedSnapshot(t)); err != nil {
		t.Fatal(err)
	}
	paris := NewServer(Config{Store: store})
	t.Cleanup(func() { _ = paris.Close(context.Background()) })

	transport, serverEnd := Pipe()
	served := make(chan error, 1)
	go func() { served <- paris.ServePipe(ctx, serverEnd) }()
	conn, err := transport.open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	// "I hold nothing", said out loud, and nothing about what it reads.
	have, err := crdt.CompositeVersion{}.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Send(kindJoin, joinMsg{Document: "paper", Site: 7, Have: have}); err != nil {
		t.Fatal(err)
	}
	if kind, msg, err := conn.Recv(); err == nil {
		t.Fatalf("the join was answered with kind=%d (%T) instead of being refused", kind, msg)
	}
	select {
	case serverErr := <-served:
		if !errors.Is(serverErr, crdt.ErrPurged) {
			t.Fatalf("the server ended the session with %v, want an error carrying crdt.ErrPurged", serverErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the server session did not end")
	}
}

// A link into a peer that has purged, on a server that already holds
// participants, reports the purge instead of sitting up for ever.
//
// A session already welcomed cannot be re-seeded: the protocol has no message
// that carries a snapshot after a welcome, so a server that swapped its
// document under live participants would strand every one of them. So it
// refuses, and the refusal reaches the operator through [LinkStatus.Err] with
// crdt.ErrPurged in it — which is what [RetryPolicy.Permanent] is asked about.
func TestALinkIntoAPurgedPeerReportsThePurgeWhenItCannotBeSeeded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewMemoryStore()
	if err := store.Save(ctx, "paper", purgedSnapshot(t)); err != nil {
		t.Fatal(err)
	}
	paris := NewServer(Config{Store: store})
	lyon := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = paris.Close(context.Background()); _ = lyon.Close(context.Background()) })

	participant(t, lyon, ClientConfig{Document: "paper", Site: 2})
	awaitTrue(t, "Lyon to hold the participant", 10*time.Second, func() bool {
		return serverSubs(lyon, "paper") == 1
	})

	reported := make(chan error, 8)
	go func() {
		_ = lyon.FollowWithRetry(ctx, dialPipe(paris, ctx), "paper", 9001,
			RetryPolicy{Wait: time.Millisecond, Ceiling: 10 * time.Millisecond,
				Notify: func(s LinkStatus) {
					if !s.Up {
						select {
						case reported <- s.Err:
						default:
						}
					}
				}})
	}()
	select {
	case err := <-reported:
		if !errors.Is(err, crdt.ErrPurged) {
			t.Fatalf("the link reported %v, want an error carrying crdt.ErrPurged", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the link never reported anything; it is up and parking")
	}
}

// A replica that has purged past the peer it follows refuses to follow it,
// rather than pushing a history with a hole into it.
//
// This is the mirror direction, which adopt does not do and offer does: a link
// carries no snapshot — only a welcome does — so there is nothing to fall back
// to here and the honest answer is to stop.
func TestAReplicaThatHasPurgedRefusesToFollowAPeerBehindIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewMemoryStore()
	if err := store.Save(ctx, "paper", purgedSnapshot(t)); err != nil {
		t.Fatal(err)
	}
	lyon := NewServer(Config{Store: store})
	paris := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = paris.Close(context.Background()); _ = lyon.Close(context.Background()) })

	transport, serverEnd := Pipe()
	go func() { _ = paris.ServePipe(ctx, serverEnd) }()
	// Bounded, and read through a channel rather than by calling Follow here:
	// a link that hangs must not be able to pass for a link that refused.
	ended := make(chan error, 1)
	go func() { ended <- lyon.Follow(ctx, transport, "paper", 9001) }()
	select {
	case err := <-ended:
		if !errors.Is(err, crdt.ErrPurged) {
			t.Fatalf("Follow into a peer this replica has purged past = %v, want an error carrying crdt.ErrPurged", err)
		}
		// From this end and not the other. A server refusing itself its own
		// purged replica would also carry crdt.ErrPurged, and would pass this
		// test while stopping a link that ought to work -- see the next one.
		if !strings.Contains(err.Error(), "purged past the peer it follows") {
			t.Fatalf("the refusal came from somewhere else: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the link never ended; it is up and pushing a history with a hole in it")
	}
}

// A seeding link refuses a snapshot it cannot read, and says so as a snapshot
// rather than as a protocol error.
func TestASeedingLinkRefusesASnapshotItCannotRead(t *testing.T) {
	lyon := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = lyon.Close(context.Background()) })
	d, sub := joinedTo(t, lyon, "paper", 9001)

	err := d.adopt(t.Context(), sub, welcomeMsg{Snapshot: []byte("not a snapshot at all")})
	if err == nil {
		t.Fatal("a snapshot of nonsense was adopted")
	}
	if !strings.Contains(err.Error(), "snapshot") {
		t.Fatalf("the refusal does not name the snapshot: %v", err)
	}
	if text, _ := serverText(lyon, "paper", "body"); text != "" {
		t.Fatalf("the replica was changed by an unreadable snapshot: %q", text)
	}
}

// A seeding link refuses a snapshot that does not contain what this replica
// already holds, rather than discarding that work.
//
// The server checked this before it sent the snapshot; this end checks it
// again, because a participant can join, type and leave between the peer's
// decision and this one.
func TestASeedingLinkRefusesASnapshotThatWouldDiscardWork(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.Save(ctx, "paper", writtenSnapshot(t, 4, "work only this replica holds")); err != nil {
		t.Fatal(err)
	}
	lyon := NewServer(Config{Store: store})
	t.Cleanup(func() { _ = lyon.Close(context.Background()) })
	d, sub := joinedTo(t, lyon, "paper", 9001)

	err := d.adopt(t.Context(), sub, welcomeMsg{Snapshot: purgedSnapshot(t)})
	if !errors.Is(err, crdt.ErrPurged) {
		t.Fatalf("adopting a snapshot that drops this replica's work = %v, want an error carrying crdt.ErrPurged", err)
	}
	if text, _ := serverText(lyon, "paper", "body"); text != "work only this replica holds" {
		t.Fatalf("the replica lost its work anyway: %q", text)
	}
}

// A client refuses a snapshot that does not contain what its replica holds,
// wherever the server got the idea it could send one.
//
// The server asks the same question before it answers, so against a correct
// server this never fires. It is here because this is where the loss would
// happen, and a guard at the point of loss costs one comparison.
func TestAClientRefusesASnapshotThatWouldDiscardItsWork(t *testing.T) {
	own, err := crdt.LoadComposite(6, writtenSnapshot(t, 6, "written while offline"))
	if err != nil {
		t.Fatal(err)
	}
	c := &Client{site: 6, doc: own}
	if err := c.absorbWelcome(welcomeMsg{Snapshot: purgedSnapshot(t)}); !errors.Is(err, crdt.ErrPurged) {
		t.Fatalf("absorbing a snapshot that drops this replica's work = %v, want an error carrying crdt.ErrPurged", err)
	}
	body, err := c.doc.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if body.String() != "written while offline" {
		t.Fatalf("the replica lost its work anyway: %q", body.String())
	}
}

// A supervised participant that was away while its server was reseeded from a
// purged snapshot comes back and is seeded, instead of being locked out.
//
// This is why a rejoin says what it reads as well as what it holds. It changes
// no branch on any server that can answer — Have is asked about first — and it
// is the whole difference between this participant coming back and it retrying
// against a refusal until somebody notices.
func TestASupervisedParticipantIsSeededWhenItsServerWasReseededWhileItWasAway(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewMemoryStore()
	paris := NewServer(Config{Store: store})
	t.Cleanup(func() { _ = paris.Close(context.Background()) })

	peer := &breakable{srv: paris, ctx: ctx}
	c, err := JoinWithRetry(ctx, peer.dial, ClientConfig{Document: "paper", Site: 6},
		RetryPolicy{Wait: time.Millisecond, Ceiling: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	body := textHandle(t, c)
	if body.String() != "" {
		t.Fatalf("the participant joined holding %q", body.String())
	}

	// Away, and while it is away an operator replaces this document with one
	// that has been purged — which is the only way a purged document ever
	// reaches a server.
	peer.away()
	awaitTrue(t, "the server to be left alone with the document", 10*time.Second, func() bool {
		return serverSubs(paris, "paper") == 0
	})
	paris.Housekeep(ctx, time.Nanosecond)
	awaitTrue(t, "the server to let the document go", 10*time.Second, func() bool {
		return paris.Documents() == 0
	})
	if err := store.Save(ctx, "paper", purgedSnapshot(t)); err != nil {
		t.Fatal(err)
	}

	peer.back()
	awaitHandle(t, "the participant to come back to the reseeded document", body, "world", 20*time.Second)
}

// A replica that holds a purged document still follows a peer that is ahead of
// it, and converges.
//
// A link joins its own server's document before it opens the session, and it
// says nothing there because it has not read the replica yet. Answering that
// join is what a server does for a participant, and a link is not one: it takes
// what it needs from the replica directly. So the answer is composed and thrown
// away — and, once a purged document started refusing what it cannot serve, the
// answer nobody reads would have refused a link its own replica and stopped a
// server following the peer that could have caught it up.
//
// This is the case the refusal must not touch: both replicas hold the purge,
// the peer is simply further on, and there is a perfectly good difference to
// send.
func TestAReplicaHoldingAPurgedDocumentStillFollowsAPeerAheadOfIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	behind := NewMemoryStore()
	if err := behind.Save(ctx, "paper", purgedSnapshot(t)); err != nil {
		t.Fatal(err)
	}
	ahead := NewMemoryStore()
	if err := ahead.Save(ctx, "paper", purgedAndAhead(t, "!!!")); err != nil {
		t.Fatal(err)
	}
	lyon := NewServer(Config{Store: behind})
	paris := NewServer(Config{Store: ahead})
	t.Cleanup(func() { _ = paris.Close(context.Background()); _ = lyon.Close(context.Background()) })

	go func() {
		_ = lyon.FollowWithRetry(ctx, dialPipe(paris, ctx), "paper", 9001,
			RetryPolicy{Wait: time.Millisecond, Ceiling: 10 * time.Millisecond})
	}()
	awaitTrue(t, "Lyon to be caught up by the peer ahead of it", 10*time.Second, func() bool {
		text, _ := serverText(lyon, "paper", "body")
		return text == "world!!!"
	})
	if _, pending := serverText(lyon, "paper", "body"); pending != 0 {
		t.Fatalf("Lyon caught up with %d operations parked", pending)
	}
}
