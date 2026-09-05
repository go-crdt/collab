//go:build !js

package collab_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// A link's outbound goroutine may be inside Send when the link ends. Closing
// the carrier from the returning Follow, as a plain deferred Close did, is a
// data race on the stream -- and defer runs last-in-first-out, so that Close
// ran BEFORE the cancel meant to stop the goroutine. Under -race this catches
// it; without -race it still asserts the link ends cleanly.
func TestALinkDoesNotCloseItsCarrierUnderASendInFlight(t *testing.T) {
	for round := range 8 {
		// The link must end for a reason OTHER than its own context: cancelling
		// that makes the outbound goroutine notice and leave before the close,
		// which is exactly the window this is about. So the PEER is stopped
		// mid-relay: the inbound Recv fails while a Send is in flight.
		_, peer, peerServer := serveGRPCWith(t, collab.Config{Store: collab.NewMemoryStore()},
			collab.GRPCServerOptions()...)
		local, localConn, _ := serveGRPCWith(t, collab.Config{Store: collab.NewMemoryStore()},
			collab.GRPCServerOptions()...)

		ctx, cancel := context.WithCancel(context.Background())
		linked := make(chan error, 1)
		go func() { linked <- local.Follow(ctx, collab.GRPC(peer), "doc", 9001) }()

		// An editor on the followed side, typing without pause, so the link's
		// outbound goroutine is busy relaying when the link is cut.
		c, err := collab.Join(context.Background(), collab.GRPC(localConn),
			collab.ClientConfig{Document: "doc", Site: crdt.SiteID(100 + round)})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		body, err := c.Text("body")
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		typing := make(chan struct{})
		go func() {
			defer close(typing)
			for i := 0; i < 4000; i++ {
				if err := body.Insert(0, "x"); err != nil {
					return
				}
			}
		}()
		time.Sleep(5 * time.Millisecond) // let the relay get going
		peerServer.Stop()                // cut the peer under an in-flight Send

		select {
		case <-linked:
		case <-time.After(20 * time.Second):
			cancel()
			t.Fatal("the link did not end when its peer went away")
		}
		<-typing
		_ = c.Close()
		cancel()
	}
}
