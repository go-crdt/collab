//go:build !js

package collab

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// The survivors of a second `mutsweep -sites all`. Each test carries its control
// first, then the line it holds.

// server.go:959, 965, 971, 1089. The old test accepted any error from the
// session, a timeout included, so a refusal worded as another one passed. Each
// is asked for by kind and by the words it uses.
func TestASessionSaysWhichMessageWasMalformed(t *testing.T) {
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	for _, tt := range []struct {
		name string
		in   scripted
		want string
	}{
		{"operations", scripted{kindOperation, "not a message"}, "collab: malformed operations"},
		{"presence", scripted{kindPresence, 42}, "collab: malformed presence"},
		{"acknowledgement", scripted{kindAcknowledge, "x"}, "collab: malformed acknowledgement"},
		// server.go:1089: the body is the right type and its update unreadable.
		{"an unreadable update", scripted{kindPresence, presenceMsg{Update: []byte("junk")}}, "collab: malformed presence"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			c := &scriptedCarrier{ctx: ctx, hangsUp: true, in: []scripted{
				{kind: kindJoin, msg: joinMsg{Document: "doc", Site: 1}},
				tt.in,
			}}
			err := srv.session(c)
			var se *sessionError
			if !errors.As(err, &se) {
				t.Fatalf("session = %v, want a session error", err)
			}
			if se.kind != errInvalid || se.msg != tt.want {
				t.Fatalf("session error = (%v, %q), want (errInvalid, %q)", se.kind, se.msg, tt.want)
			}
		})
	}
}

// wire.go:330. An acknowledgement whose version is empty and whose clocks stop
// short is refused. A failed read leaves the buffer after the length, so the
// frame looks finished and a decoder that only asked "is there more?" accepts it.
func TestAnAcknowledgementWithTruncatedClocksIsRefused(t *testing.T) {
	// Control: an empty version and no clocks is the legacy form and is accepted.
	if _, _, err := decodeClient([]byte{kindAcknowledge, 0}); err != nil {
		t.Fatalf("a version with no clocks = %v, want it accepted", err)
	}
	// wire.go:330: the clocks say five bytes and there are none.
	if _, _, err := decodeClient([]byte{kindAcknowledge, 0, 5}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("truncated clocks = %v, want ErrProtocol", err)
	}
}

// wire.go:355. A presence count larger than the bytes left is refused before
// anything is allocated for it, and by an error rather than a panic.
func TestAWelcomeCountingMorePresenceThanItHoldsIsRefused(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("decodeServer panicked on a huge presence count: %v", r)
		}
	}()
	// Control: zero presence entries and nothing after is a welcome that parses
	// as far as the presence count; the tail decides the rest.
	huge := binary.AppendUvarint([]byte{kindWelcome, 0, 0, 0}, 1<<62)
	if _, _, err := decodeServer(huge); !errors.Is(err, ErrProtocol) {
		t.Fatalf("a count of 1<<62 = %v, want ErrProtocol", err)
	}
}

// sites.go:151. A participants file is refused when either of its sized fields
// is cut short: the version alone, or the clocks alone.
func TestParticipantsWithACutFieldAreRefused(t *testing.T) {
	// Control: one site, empty version, empty clocks, is read.
	if _, _, err := decodeSites([]byte{1, 5, 0, 0}); err != nil {
		t.Fatalf("a site with nothing to say = %v, want it read", err)
	}
	for name, in := range map[string][]byte{
		"a version cut short": {1, 5, 9},
		"clocks cut short":    {1, 5, 0, 9},
	} {
		if _, _, err := decodeSites(in); !errors.Is(err, crdt.ErrMalformed) {
			t.Fatalf("%s = %v, want crdt.ErrMalformed", name, err)
		}
	}
}

// durability.go:96. A collection that ran inside the interval is not a
// collection, and must not move the clock the next one is measured from.
func TestACollectionInsideTheIntervalDoesNotRestartIt(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := t0
	s := NewServer(Config{CollectEvery: time.Minute, Clock: func() time.Time { return now }})
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	s.collectStable() // control: the first one runs, and is stamped
	s.mu.Lock()
	first := s.lastCollect
	s.mu.Unlock()
	if !first.Equal(t0) {
		t.Fatalf("lastCollect after the first pass = %v, want %v", first, t0)
	}

	now = t0.Add(time.Second)
	s.collectStable() // inside the interval: nothing runs, nothing is stamped
	s.mu.Lock()
	got := s.lastCollect
	s.mu.Unlock()
	if !got.Equal(t0) {
		t.Fatalf("lastCollect after a pass inside the interval = %v, want %v", got, t0)
	}
}
