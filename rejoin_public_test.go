//go:build !js

package collab_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-crdt/collab"
)

// A policy that calls a refused dial permanent stops a supervised client — said
// here through the exported API and nothing else.
//
// # Why this is not a duplicate
//
// The other tests of this behaviour live inside the package and reach for an
// unexported fake, because they have to: a [collab.Dialer] can be written from
// outside, but [collab.Transport]'s only method is unexported, so an outside
// caller cannot hand a client a carrier of its own — and the carrier it does
// get, a *collab.PipeConn, has no methods at all, so it cannot be cut either.
//
// What is left is the lever the package documents. [collab.ErrPipeClosed] is
// what a pipe carrier's Recv returns when "either end was closed, or the
// session's context was cancelled", so the session is given a context of its
// own and cancelling that ends the session the way a dropped socket does.
// Finding that was the work; the test is short because of it.
//
// It exists because a defect in the engine is not the same thing as exposure
// through the API. The fix this checks was proved by internal tests and by a
// mutation sweep, and neither of those would have noticed if the behaviour had
// failed to reach a caller who only has the exported names. Run against the
// previous release the same sequence asks the policy about the dead session
// thousands of times and about the refused dial never once.
func TestAPolicyStopsASupervisedClientThroughTheExportedAPI(t *testing.T) {
	gone := errors.New("this endpoint is not coming back")

	srv := collab.NewServer(collab.Config{Store: collab.NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()

	sessionCtx, endSession := context.WithCancel(context.Background())
	defer endSession()
	transport, server := collab.Pipe()
	go func() { _ = srv.ServePipe(sessionCtx, server) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	dials := 0
	var asked []error

	// One real session, then every attempt to replace it is refused with an
	// error no amount of waiting will change.
	dial := func(context.Context) (collab.Transport, error) {
		mu.Lock()
		dials++
		first := dials == 1
		mu.Unlock()
		if first {
			return transport, nil
		}
		return nil, gone
	}

	c, err := collab.JoinWithRetry(ctx, dial, collab.ClientConfig{Document: "d", Site: 1}, collab.RetryPolicy{
		Wait:    time.Millisecond,
		Ceiling: 2 * time.Millisecond,
		Permanent: func(err error) bool {
			mu.Lock()
			asked = append(asked, err)
			mu.Unlock()
			return errors.Is(err, gone)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	endSession()

	select {
	case <-c.Done():
	case <-time.After(15 * time.Second):
		mu.Lock()
		n := len(asked)
		mu.Unlock()
		t.Fatalf("the policy called the refused dial permanent and the client kept retrying; it was asked %d time(s)", n)
	}

	if !errors.Is(c.Err(), gone) {
		t.Errorf("Err() is %v; it gave up because of %v", c.Err(), gone)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, err := range asked {
		if errors.Is(err, gone) {
			return
		}
	}
	t.Errorf("the policy was asked about %v and never about the refused dial", asked)
}
