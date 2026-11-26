//go:build !js

package collab

import (
	"context"
	"errors"
	"testing"

	"github.com/go-crdt/crdt"
)

// noPanicF runs f and turns a panic into a failure, so that a guard whose
// deletion makes the code after it dereference nil is caught by an assertion
// rather than by the process dying.
func noPanicF(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s panicked: %v", what, r)
		}
	}()
	f()
}

// follow.go:131. A link whose context is already over is refused with that
// context's error, not run against a replica that was never opened.
func TestFollowOnADeadContextReturnsItsError(t *testing.T) {
	s := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	noPanicF(t, "Follow on a cancelled context", func() {
		if err := s.Follow(ctx, &brokenPeer{}, "doc", 42); !errors.Is(err, context.Canceled) {
			t.Errorf("Follow = %v, want context.Canceled", err)
		}
	})
}

// follow.go:151. A peer that cannot be opened ends the link with its error.
func TestFollowWithAPeerThatCannotBeOpenedReturnsItsError(t *testing.T) {
	boom := errors.New("cannot open")
	s := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	noPanicF(t, "Follow with an unopenable peer", func() {
		if err := s.Follow(t.Context(), &brokenPeer{openErr: boom}, "doc", 42); !errors.Is(err, boom) {
			t.Errorf("Follow = %v, want the peer's error", err)
		}
	})
}

// follow.go:438 and 442. A document built without its maps is given them rather
// than written through nil.
func TestAcknowledgeSelfOnADocumentWithoutMaps(t *testing.T) {
	d := &document{doc: crdt.NewComposite(serverSite)}
	noPanicF(t, "acknowledgeSelf on a bare document", func() {
		d.acknowledgeSelf(&subscriber{site: 5})
	})
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[5]; !ok {
		t.Error("seen does not record the link")
	}
	if _, ok := d.reached[5]; !ok {
		t.Error("reached does not record the link")
	}
}

// follow.go:644. A snapshot that cannot be read is refused, not adopted.
func TestSeedRefusesAnUnreadableSnapshot(t *testing.T) {
	s := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	d, link := joinedTo(t, s, "paper", 9001)
	noPanicF(t, "seed of garbage", func() {
		if err := d.seed(link, []byte("not a snapshot")); err == nil {
			t.Error("an unreadable snapshot was accepted")
		}
	})
}

// wire.go:173. A length that runs past the buffer is a failed read, not a slice
// out of range.
func TestFrameBytesRefusesALengthPastTheBuffer(t *testing.T) {
	f := &frame{buf: []byte{5, 1}}
	noPanicF(t, "frame.bytes past the end", func() {
		if _, ok := f.bytes(); ok {
			t.Error("a length past the buffer was read")
		}
	})
}

// wire.go:285 and 345. An empty message is a protocol error, not an index out
// of range.
func TestDecodeOfAnEmptyMessageIsAProtocolError(t *testing.T) {
	noPanicF(t, "decodeClient of nothing", func() {
		if _, _, err := decodeClient(nil); !errors.Is(err, ErrProtocol) {
			t.Errorf("decodeClient(nil) = %v, want ErrProtocol", err)
		}
	})
	noPanicF(t, "decodeServer of nothing", func() {
		if _, _, err := decodeServer(nil); !errors.Is(err, ErrProtocol) {
			t.Errorf("decodeServer(nil) = %v, want ErrProtocol", err)
		}
	})
}

// client.go:137 and 179. A join whose transport cannot be opened returns its
// error and no client.
func TestJoinWithAnUnopenableTransportReturnsItsError(t *testing.T) {
	boom := errors.New("cannot open")
	noPanicF(t, "Join with an unopenable transport", func() {
		c, err := Join(t.Context(), &brokenPeer{openErr: boom}, ClientConfig{Document: "d", Site: 2})
		if !errors.Is(err, boom) || c != nil {
			t.Errorf("Join = %v, %v; want nil and the transport's error", c, err)
		}
	})
}

// client.go:170. A resume that cannot be read is refused, not used as the
// replica.
func TestJoinRefusesAnUnreadableResume(t *testing.T) {
	noPanicF(t, "Join with garbage resume", func() {
		c, err := Join(t.Context(), &brokenPeer{}, ClientConfig{Document: "d", Site: 2, Resume: []byte("not a snapshot")})
		if err == nil || c != nil {
			t.Errorf("Join = %v, %v; want nil and an error", c, err)
		}
	})
}

// secondOpenFails welcomes the first connection with a snapshot this build
// cannot read and refuses the second.
type secondOpenFails struct {
	welcome welcomeMsg
	opened  int
	boom    error
}

func (s *secondOpenFails) open(ctx context.Context) (carrierConn, error) {
	s.opened++
	if s.opened > 1 {
		return nil, s.boom
	}
	return &brokenPeer{firstMsg: welcomeWith(s.welcome), ctx: ctx}, nil
}

// client.go:206. If asking again on a fresh connection fails, the join fails
// with that error.
func TestJoinReturnsTheErrorOfTheSecondOpen(t *testing.T) {
	seed := crdt.NewComposite(1)
	part, err := seed.Text("t")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Insert(0, "written by an older server"); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("second open failed")
	tp := &secondOpenFails{welcome: welcomeMsg{Snapshot: stampTextVersion(t, seed.Snapshot(), 6)}, boom: boom}
	noPanicF(t, "joinOn whose retry cannot open", func() {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		c, err := joinOn(ctx, cancel, tp, ClientConfig{Document: "d", Site: 2})
		if !errors.Is(err, boom) || c != nil {
			t.Errorf("joinOn = %v, %v; want nil and the second open's error", c, err)
		}
	})
	if tp.opened != 2 {
		t.Errorf("opened %d connections, want 2", tp.opened)
	}
}

// client.go:264. A carrier that cannot be opened is its error, with no
// connection to write a join to.
func TestOpenAndBeWelcomedReturnsTheOpenError(t *testing.T) {
	boom := errors.New("cannot open")
	noPanicF(t, "openAndBeWelcomed", func() {
		conn, _, err := openAndBeWelcomed(t.Context(), &brokenPeer{openErr: boom}, joinMsg{Document: "d"})
		if !errors.Is(err, boom) || conn != nil {
			t.Errorf("openAndBeWelcomed = %v, %v; want nil and the open error", conn, err)
		}
	})
}

// client.go:309. A snapshot that is damaged, rather than newer, is refused with
// the decoder's error and the replica is left alone.
func TestAbsorbWelcomeRefusesADamagedSnapshot(t *testing.T) {
	c := &Client{site: 3, doc: crdt.NewComposite(3)}
	noPanicF(t, "absorbWelcome of garbage", func() {
		err := c.absorbWelcome(welcomeMsg{Snapshot: []byte("not a snapshot")})
		if err == nil || errors.Is(err, crdt.ErrUnknownFormat) {
			t.Errorf("absorbWelcome = %v, want a decoding error", err)
		}
	})
}
