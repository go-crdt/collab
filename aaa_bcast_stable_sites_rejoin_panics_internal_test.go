//go:build !js

package collab

import (
	"context"
	"errors"
	"testing"
	"time"
)

// These hold, by assertion, guards that mutsweep -sites all found were held
// only by the process panicking: delete the guard and the code after it
// dereferences nil, the suite goes red, and nothing said why. Each drives the
// guard's failing input directly, in the test goroutine, so that a deleted guard
// is a failed assertion and not a crash. Refs go-crdt/collab#293.

func noPanicB(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s panicked: %v", what, r)
		}
	}()
	f()
}

func TestBcastEnvelopeGuardsHoldWithoutPanicking(t *testing.T) {
	overflow := []byte{0}
	for i := 0; i < 11; i++ {
		overflow = append(overflow, 0xff)
	}
	for _, c := range []struct {
		name string
		raw  []byte
	}{
		{"an empty frame", nil},
		{"a from identifier that overflows", overflow},
	} {
		noPanicB(t, c.name, func() {
			if _, ok := decodeEnvelope(c.raw); ok {
				t.Errorf("%s was decoded", c.name)
			}
		})
	}
}

func TestStableAndAcknowledgedDeclineAnUnopenedDocumentWithoutPanicking(t *testing.T) {
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	noPanicB(t, "Acknowledged", func() {
		if got, ok := srv.Acknowledged("never opened"); ok || got != nil {
			t.Errorf("Acknowledged = %v, %v", got, ok)
		}
	})
	noPanicB(t, "Stable", func() {
		if got, ok := srv.Stable("never opened"); ok || got != nil {
			t.Errorf("Stable = %v, %v", got, ok)
		}
	})
}

func TestAnAcknowledgementIsRecordedOnADocumentBuiltWithoutItsMaps(t *testing.T) {
	d := &document{}
	sub := &subscriber{site: 7}
	noPanicB(t, "acknowledge", func() {
		if err := d.acknowledge(sub, nil, nil); err != nil {
			t.Errorf("acknowledge: %v", err)
		}
		if _, ok := d.seen[7]; !ok {
			t.Error("the participant was not recorded in seen")
		}
		if _, ok := d.reached[7]; !ok {
			t.Error("the participant was not recorded in reached")
		}
	})
}

func TestDecodeSitesRefusesWhatItCannotReadWithoutPanicking(t *testing.T) {
	magic := append([]byte(nil), sitesMagic[:]...)
	for _, c := range []struct {
		name string
		in   []byte
	}{
		{"a body too short for a checksum", append(append([]byte(nil), magic...), 1, 2, 3)},
		{"a length that outruns the bytes", []byte{1, 1, 100}},
	} {
		noPanicB(t, c.name, func() {
			if _, _, err := decodeSites(c.in); err == nil {
				t.Errorf("%s was accepted", c.name)
			}
		})
	}
}

func TestPersistLoopTimesItsPassOnTheShortestIntervalWithoutPanicking(t *testing.T) {
	srv := &Server{stop: make(chan struct{}), stopped: make(chan struct{})}
	close(srv.stop)
	noPanicB(t, "persistLoop", func() { srv.persistLoop(time.Hour, 0, 0) })
	select {
	case <-srv.stopped:
	default:
		t.Error("persistLoop did not finish")
	}
}

// loadFails is a store whose reads fail, and which is not a ConditionalStore.
type loadFails struct{ Store }

func (loadFails) Load(context.Context, string) ([]byte, error) {
	return nil, errors.New("the disk is gone")
}

func TestOpenAndEnrolReturnsTheErrorsItIsGivenWithoutPanicking(t *testing.T) {
	ctx := context.Background()
	broken := NewServer(Config{Store: loadFails{NewMemoryStore()}})
	t.Cleanup(func() { _ = broken.Close(ctx) })
	noPanicB(t, "a store that cannot be read", func() {
		doc, sub, err := broken.openAndEnrol(ctx, joinMsg{Document: "d", Site: 1}, true)
		if err == nil || doc != nil || sub != nil {
			t.Errorf("got %v, %v, %v", doc, sub, err)
		}
	})

	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(ctx) })
	noPanicB(t, "a join as the server's own site", func() {
		doc, sub, err := srv.openAndEnrol(ctx, joinMsg{Document: "d", Site: uint64(serverSite)}, true)
		if err == nil || sub != nil {
			t.Errorf("got %v, %v, %v", doc, sub, err)
		}
	})
}

func TestJoinWithRetryReturnsItsErrorsWithoutPanicking(t *testing.T) {
	ctx := context.Background()
	cfg := ClientConfig{Document: "d", Site: 1}
	noPanicB(t, "no dialler", func() {
		if _, err := JoinWithRetry(ctx, nil, cfg, RetryPolicy{}); err == nil {
			t.Error("no dialler was accepted")
		}
	})
	noPanicB(t, "a dialler that fails", func() {
		_, err := JoinWithRetry(ctx, func(context.Context) (Transport, error) { return nil, errors.New("nobody") }, cfg, RetryPolicy{})
		if err == nil {
			t.Error("a failed dial was accepted")
		}
	})
	noPanicB(t, "a first join that is refused", func() {
		_, err := JoinWithRetry(ctx, func(context.Context) (Transport, error) { return &alwaysBad{}, nil }, cfg, RetryPolicy{})
		if err == nil {
			t.Error("a refused join was accepted")
		}
	})
}

// openFails is a transport whose carrier can never be opened.
type openFails struct{ opened func() }

func (o openFails) open(context.Context) (carrierConn, error) {
	o.opened()
	return nil, errors.New("nothing is listening")
}

// stayJoined is driven here in the test goroutine, which is what lets a panic
// in it be caught: spawned by JoinWithRetry, the same panic could not be.
func TestStayJoinedKeepsAnAttemptThatFailedWithoutPanicking(t *testing.T) {
	for _, at := range []string{"dial", "attach"} {
		t.Run(at, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			srv := NewServer(Config{Store: NewMemoryStore()})
			defer func() { _ = srv.Close(context.Background()) }()
			link := &breakable{srv: srv, ctx: ctx}
			c, err := joinOn(ctx, cancel, link, ClientConfig{Document: "d", Site: 1})
			if err != nil {
				t.Fatal(err)
			}
			c.supervised = true
			link.cut()
			dial := func(context.Context) (Transport, error) {
				if at == "dial" {
					cancel()
					return nil, errors.New("that name does not resolve")
				}
				return openFails{opened: cancel}, nil
			}
			noPanicB(t, "stayJoined after a failed "+at, func() {
				c.stayJoined(ctx, dial, RetryPolicy{Wait: time.Millisecond, Ceiling: time.Millisecond})
			})
			select {
			case <-c.Done():
			default:
				t.Error("stayJoined returned without finishing the client")
			}
		})
	}
}

// attach hands back the error of a carrier that cannot be opened, and no
// carrier, rather than a nil one for the caller to use.
func TestAttachReturnsTheErrorOfACarrierThatCannotBeOpenedWithoutPanicking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()
	link := &breakable{srv: srv, ctx: ctx}
	c, err := joinOn(ctx, cancel, link, ClientConfig{Document: "d", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	noPanicB(t, "attach", func() {
		conn, _, err := c.attach(ctx, openFails{opened: func() {}})
		if err == nil || conn != nil {
			t.Errorf("attach = %v, %v", conn, err)
		}
	})
}
