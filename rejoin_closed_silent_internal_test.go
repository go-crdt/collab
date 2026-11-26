//go:build !js

package collab

import (
	"context"
	"sync"
	"testing"
	"time"
)

// A client closed on purpose ends the supervisor at once, and says nothing to
// the operator. The session that Close ends is a session that ended while the
// context was still live (Close cancels it only after waiting for the session),
// so the supervisor reaches rejoin.go's ErrClosed check with the context
// intact: without that check it counts the deliberate close as an outage, tells
// Notify about it and sits in the backoff until Close's grace runs out.
func TestAClientClosedOnPurposeDoesNotReportAnOutage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()

	link := &breakable{srv: srv, ctx: ctx}
	var mu sync.Mutex
	var told []LinkStatus
	c, err := JoinWithRetry(ctx, link.dial, ClientConfig{Document: "d", Site: 1}, RetryPolicy{
		Wait:    time.Second,
		Ceiling: 10 * time.Second,
		Notify: func(s LinkStatus) {
			mu.Lock()
			told = append(told, s)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	_ = c.Close()
	if took := time.Since(start); took >= closeGrace/2 {
		t.Errorf("Close took %v: the supervisor sat out a backoff instead of stopping", took)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(told) != 0 {
		t.Errorf("a deliberate Close was reported to Notify: %+v", told)
	}
}
