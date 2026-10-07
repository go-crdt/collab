//go:build !js

package collab

import (
	"context"
	"errors"
	"testing"
	"time"
)

// [RetryPolicy.Permanent] "stops the link by returning true — the error it was
// asked about is then what [Server.FollowWithRetry] returns".
//
// Nothing asserted it. Deleting the line that consults it did not make the
// suite fail; it made the suite HANG, because the loop went back to dialling a
// peer that would never answer, for as long as anybody was willing to wait.
//
// What makes a stop a stop, rather than a slow retry, is that nothing waits
// first. So the sleep here fails the test if it is called at all: an assertion
// about the absence of a wait, which is the thing a hang cannot distinguish.
func TestAPermanentErrorStopsTheLinkWithoutWaiting(t *testing.T) {
	refused := errors.New("the peer will not have us")

	t.Run("permanent: it stops, and does not sleep", func(t *testing.T) {
		s := NewServer(Config{Store: NewMemoryStore()})
		t.Cleanup(func() { _ = s.Close(context.Background()) })

		link, err := s.reconnecting(
			func(context.Context) (Transport, error) { return nil, refused },
			"doc", 42,
			RetryPolicy{
				Wait:      time.Second,
				Ceiling:   8 * time.Second,
				Permanent: func(err error) bool { return errors.Is(err, refused) },
			})
		if err != nil {
			t.Fatal(err)
		}
		link.back.sleep = func(context.Context, time.Duration) error {
			t.Error("the link waited before stopping: a permanent failure is not a slow one")
			return nil
		}

		done := make(chan error, 1)
		go func() { done <- link.run(t.Context()) }()
		select {
		case got := <-done:
			if !errors.Is(got, refused) {
				t.Fatalf("the link ended with %v, want the error Permanent was asked about", got)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the link did not stop: it is still retrying a failure it was told was permanent")
		}
	})

	// The control. With the same dial and no Permanent, the link DOES wait --
	// so the case above is about the policy and not about a loop that never
	// retries anything.
	t.Run("not permanent: it waits and tries again", func(t *testing.T) {
		s := NewServer(Config{Store: NewMemoryStore()})
		t.Cleanup(func() { _ = s.Close(context.Background()) })

		link, err := s.reconnecting(
			func(context.Context) (Transport, error) { return nil, refused },
			"doc", 42,
			RetryPolicy{Wait: time.Second, Ceiling: 8 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		slept := 0
		link.back.sleep = func(context.Context, time.Duration) error {
			slept++
			if slept == 3 {
				cancel()
				return ctx.Err()
			}
			return nil
		}
		if err := link.run(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("the link ended with %v, want the cancellation", err)
		}
		if slept != 3 {
			t.Fatalf("the link waited %d times before it was stopped, want 3", slept)
		}
	})
}
