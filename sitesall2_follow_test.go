//go:build !js

package collab

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// follow.go:117. A link that cannot be what it was asked to be is refused before
// the peer is dialled, and by its own reason: not by whatever the peer says.
func TestFollowRefusesWhatItCannotFollowBeforeDialling(t *testing.T) {
	boom := errors.New("the peer was dialled")
	s := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	// Control: a document and a site that can be followed reach the peer.
	if err := s.Follow(t.Context(), &brokenPeer{openErr: boom}, "doc", 42); !errors.Is(err, boom) {
		t.Fatalf("Follow of a followable document = %v, want the peer's error", err)
	}
	for _, tt := range []struct {
		name     string
		document string
		as       crdt.SiteID
		want     string
	}{
		{"no document", "", 42, "needs a document name"},
		{"the server's own site", "doc", serverSite, "server's own replica"},
	} {
		err := s.Follow(t.Context(), &brokenPeer{openErr: boom}, tt.document, tt.as)
		if err == nil || errors.Is(err, boom) || !strings.Contains(err.Error(), tt.want) {
			t.Fatalf("%s: Follow = %v, want a refusal saying %q", tt.name, err, tt.want)
		}
	}
}

// follow.go:364. Presence from the peer is ignored, not refused: a link is not a
// participant, and a peer running ahead of this build must not break it.
func TestALinkIgnoresPresenceFromItsPeer(t *testing.T) {
	peer := &brokenPeer{
		firstMsg: welcomeWith(welcomeMsg{}),
		then:     func() (byte, any, error) { return kindPresence, presenceMsg{}, nil },
	}
	s := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- s.Follow(ctx, peer, "doc", 42) }()
	select {
	case err := <-done:
		t.Fatalf("the link ended on presence: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Follow after cancel = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the link did not end when its context did")
	}
}

// quietLink follows a document with a participant, site 7, that has joined and
// never said what it holds: the server cannot promise anything to its peer.
type quietLink struct {
	doc   *document
	link  *subscriber
	other *subscriber
	mine  *crdt.Composite
	ops   []byte
	ctx   context.Context

	mu   sync.Mutex
	acks int
	got  []byte
}

func (q *quietLink) acked() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.acks
}

func (q *quietLink) sentOps() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.got)
}

func newQuietLink(t *testing.T, then func() (byte, any, error)) *quietLink {
	t.Helper()
	return newGatedLink(t, then, nil)
}

// newGatedLink is a quietLink whose peer takes an acknowledgement or an
// operation only when the test lets it, so a test can stop the link between one
// send and the next. A nil gate lets everything through.
func newGatedLink(t *testing.T, then func() (byte, any, error), gate chan struct{}) *quietLink {
	t.Helper()
	q := &quietLink{}
	peer := &brokenPeer{
		firstMsg: welcomeWith(welcomeMsg{}),
		then:     then,
		sends: func(kind byte, msg any) error {
			if gate != nil && (kind == kindAcknowledge || kind == kindOperation) {
				select {
				case <-gate:
				case <-q.ctx.Done():
					return q.ctx.Err()
				}
			}
			q.mu.Lock()
			defer q.mu.Unlock()
			switch kind {
			case kindAcknowledge:
				q.acks++
			case kindOperation:
				q.got = append(q.got, kind)
			}
			return nil
		},
	}
	s := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	q.ctx = ctx
	go func() { _ = s.Follow(ctx, peer, "doc", 42) }()

	until(t, "the link to register itself", func() bool {
		s.mu.Lock()
		q.doc = s.docs["doc"]
		s.mu.Unlock()
		if q.doc == nil {
			return false
		}
		q.doc.mu.Lock()
		defer q.doc.mu.Unlock()
		for sub := range q.doc.links {
			q.link = sub
			return true
		}
		return false
	})

	other, err := q.doc.enrol(joinMsg{Document: "doc", Site: 7}, false)
	if err != nil {
		t.Fatal(err)
	}
	q.other = other
	t.Cleanup(func() { q.doc.leave(context.WithoutCancel(ctx), other) })

	q.mine = crdt.NewComposite(7)
	cells, err := q.mine.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cells.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	q.ops, err = crdt.AppendPartOps(nil, q.mine.OpsSince(nil))
	if err != nil {
		t.Fatal(err)
	}
	return q
}

// speak is site 7 saying what it holds, which is what lets the server promise.
func (q *quietLink) speak(t *testing.T) {
	t.Helper()
	version, err := q.mine.Version().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	clocks, err := q.mine.Clocks().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := q.doc.acknowledge(q.other, version, clocks); err != nil {
		t.Fatal(err)
	}
}

// catchUp has site 7's operations reach the document and the link, so the link
// holds everything there is and only site 7's word is missing.
func (q *quietLink) catchUp(t *testing.T) {
	t.Helper()
	if err := q.doc.applyOperations(q.ctx, q.other, q.ops); err != nil {
		t.Fatal(err)
	}
	until(t, "the operation to reach the peer", func() bool { return q.sentOps() == 1 })
	time.Sleep(50 * time.Millisecond) // acknowledgeSelf follows the send
}

func (q *quietLink) wake() {
	q.doc.mu.Lock()
	select {
	case q.link.promiseMoved <- struct{}{}:
	default:
	}
	q.doc.mu.Unlock()
}

// follow.go:253, 323 and 408. While somebody behind the link has not said what
// it holds, the server cannot promise its peer anything, and says nothing: an
// acknowledgement of nothing would hold the peer's floor at nothing.
func TestALinkWithNothingToPromiseSaysNothing(t *testing.T) {
	t.Run("woken by an acknowledgement behind it", func(t *testing.T) { // 253
		q := newQuietLink(t, nil)
		for range 3 {
			q.wake()
			time.Sleep(30 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
		if got := q.acked(); got != 0 {
			t.Fatalf("the link promised %d times with a participant it had not heard from", got)
		}
		// Control: once the participant speaks, the same wake is a promise.
		q.catchUp(t)
		q.speak(t)
		q.wake()
		until(t, "the link to promise once it can", func() bool { return q.acked() >= 1 })
	})

	t.Run("relaying an operation", func(t *testing.T) { // 323
		q := newQuietLink(t, nil)
		if err := q.doc.applyOperations(q.ctx, q.other, q.ops); err != nil {
			t.Fatal(err)
		}
		// Control: the operation was relayed to the peer, so the branch was run.
		until(t, "the operation to reach the peer", func() bool { return q.sentOps() == 1 })
		time.Sleep(100 * time.Millisecond)
		if got := q.acked(); got != 0 {
			t.Fatalf("the link promised %d times after relaying, with a participant it had not heard from", got)
		}
	})

	t.Run("applying the peer's operations", func(t *testing.T) { // 408
		// Held until the participant is enrolled: the operations are the peer's,
		// and what they leave behind is a document with a quiet site in it.
		release := make(chan struct{})
		ops := peerOps(t)
		q := newQuietLink(t, func() (byte, any, error) {
			<-release
			return kindOperation, opsMsg{Operations: ops}, nil
		})
		close(release)
		// Control: the peer's operations were applied, so the branch was run.
		until(t, "the peer's operations to be applied", func() bool {
			q.doc.mu.Lock()
			defer q.doc.mu.Unlock()
			return len(q.doc.doc.OpsSince(nil)) > 0
		})
		time.Sleep(100 * time.Millisecond)
		if got := q.acked(); got != 0 {
			t.Fatalf("the link promised %d times after applying, with a participant it had not heard from", got)
		}
	})
}

func peerOps(t *testing.T) []byte {
	t.Helper()
	c := crdt.NewComposite(9)
	cells, err := c.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cells.Set("p", []byte("x")); err != nil {
		t.Fatal(err)
	}
	raw, err := crdt.AppendPartOps(nil, c.OpsSince(nil))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// follow.go:457. What a server can collect against needs the clocks as well as
// the versions. A participant that said what it holds and nothing of its clocks
// leaves the clock floor unknown, and nothing is promised.
func TestAPromiseNeedsClocksAsWellAsVersions(t *testing.T) {
	q := newQuietLink(t, nil)
	q.catchUp(t)

	version, err := q.mine.Version().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := q.doc.acknowledge(q.other, version, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := q.doc.promise(); ok {
		t.Fatal("a promise was made with a participant whose clocks nobody has heard")
	}

	// Control: with its clocks as well, the same participant lets the server promise.
	q.speak(t)
	if _, _, ok := q.doc.promise(); !ok {
		t.Fatal("no promise once every participant had said what it holds")
	}
}

// follow.go:314. A link records that it holds what the document holds only
// after it has sent the operations that put it there. Recording it after an
// acknowledgement as well claims the link has operations still queued behind
// that acknowledgement, and the server then promises its peer a floor over
// work the peer has not been sent.
func TestALinkIsNotCaughtUpByAcknowledgingBeforeItRelays(t *testing.T) {
	gate := make(chan struct{})
	q := newGatedLink(t, nil, gate)

	held := func() []byte {
		q.doc.mu.Lock()
		defer q.doc.mu.Unlock()
		raw, _ := q.doc.seen[42].MarshalBinary()
		return raw
	}
	whole := func() []byte {
		q.doc.mu.Lock()
		defer q.doc.mu.Unlock()
		raw, _ := q.doc.doc.Version().MarshalBinary()
		return raw
	}

	// The first operation is relayed, and the link is then caught up with it.
	if err := q.doc.applyOperations(q.ctx, q.other, q.ops); err != nil {
		t.Fatal(err)
	}
	gate <- struct{}{}
	first := whole()
	until(t, "the link to hold the first operation", func() bool { return string(held()) == string(first) })

	// Site 7 speaks, so the link has a promise to send, and the send is held.
	q.speak(t)
	time.Sleep(100 * time.Millisecond)

	// A second operation now queues behind the held acknowledgement.
	v1 := q.mine.Version()
	cells, err := q.mine.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cells.Set("second", []byte("v")); err != nil {
		t.Fatal(err)
	}
	ops2, err := crdt.AppendPartOps(nil, q.mine.OpsSince(v1))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.doc.applyOperations(q.ctx, q.other, ops2); err != nil {
		t.Fatal(err)
	}
	second := whole()
	if string(second) == string(first) {
		t.Fatal("the second operation did not change the document")
	}

	// Let the acknowledgement go, and nothing after it: the link has not sent
	// the second operation, so it does not hold it.
	gate <- struct{}{}
	time.Sleep(150 * time.Millisecond)
	if string(held()) != string(first) {
		t.Fatal("the link claimed to hold an operation it had queued and not sent")
	}

	// Control: let everything through, and it does hold it.
	close(gate)
	until(t, "the link to hold the second operation once it was sent", func() bool {
		return string(held()) == string(second)
	})
}
