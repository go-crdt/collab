//go:build !js

package collab_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/collab/collabpb"
	"github.com/go-crdt/crdt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// serveGRPCWith builds a peer over gRPC with exactly the server options given,
// so a test can say what a stock server does and what one built as the
// documentation says does.
func serveGRPCWith(t *testing.T, cfg collab.Config, opts ...grpc.ServerOption) (*collab.Server, *grpc.ClientConn, *grpc.Server) {
	t.Helper()
	srv := collab.NewServer(cfg)
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer(opts...)
	collabpb.RegisterCollabServer(gs, collab.GRPCService(srv))
	go func() { _ = gs.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///collab",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		gs.Stop()
		_ = srv.Close(context.Background())
	})
	return srv, conn, gs
}

// An edit larger than gRPC's four-mebibyte default is what a participant sends
// after a spell offline. A server built without [collab.GRPCServerOptions]
// refuses it -- that is why the option exists and why GRPCService's doc comment
// names it; a server built with them carries it.
//
// The payload is multi-byte characters so the message passes four mebibytes on
// about a third as many operations, and the assertion is that the SENDER's
// session survives: a refusal ends it with ResourceExhausted. Nothing here
// waits on a second participant re-applying a million operations, which under
// -race on a shared runner is a measurement of the runner.
func TestAGRPCServerNeedsTheLibrarysOptionsForALargeEdit(t *testing.T) {
	// Each rune is three bytes in an operation's character field, so this
	// clears 4 MiB with room to spare.
	big := strings.Repeat("\u4e16", (4<<20)/3+(64<<10))

	// send inserts big and reports how it went: an error if the session was
	// ended, nil if the server holds the text -- read back from its store,
	// which is proof it took the message rather than proof it has not died yet.
	send := func(t *testing.T, srv *collab.Server, store collab.Store, conn *grpc.ClientConn) error {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		c, err := collab.Join(ctx, collab.GRPC(conn), collab.ClientConfig{Document: "d", Site: 1})
		if err != nil {
			return err
		}
		defer func() { _ = c.Close() }()
		body, err := c.Text("body")
		if err != nil {
			return err
		}
		if err := body.Insert(0, big); err != nil {
			return err
		}
		// The server holds it, read back from its own store: proof it took the
		// message, where "the session has not died yet" would only be proof
		// that nothing had happened. Flush persists what the document has NOW,
		// so this asks again until the answer is yes -- or until the session
		// ends, which is what a refusal does and which ends the wait at once.
		want := len([]rune(big))
		deadline := time.After(4 * time.Minute)
		for {
			select {
			case <-c.Done():
				return c.Err()
			case <-deadline:
				return fmt.Errorf("the server never came to hold the text")
			case <-time.After(50 * time.Millisecond):
			}
			if err := srv.Flush(ctx); err != nil {
				return err
			}
			raw, err := store.Load(ctx, "d")
			if err != nil {
				return err
			}
			if len(raw) == 0 {
				continue
			}
			doc, err := crdt.LoadComposite(99, raw)
			if err != nil {
				return err
			}
			text, err := doc.Text("body")
			if err != nil {
				return err
			}
			if text.Len() == want {
				return nil
			}
		}
	}

	t.Run("a stock server refuses it", func(t *testing.T) {
		store := collab.NewMemoryStore()
		srv, conn, _ := serveGRPCWith(t, collab.Config{Store: store})
		err := send(t, srv, store, conn)
		if err == nil {
			t.Fatal("a stock grpc.Server carried a message past its own limit")
		}
		if !strings.Contains(err.Error(), "larger than max") {
			t.Fatalf("the stock server ended the session with %v, want its receive limit", err)
		}
	})

	t.Run("a server built as documented carries it", func(t *testing.T) {
		store := collab.NewMemoryStore()
		srv, conn, _ := serveGRPCWith(t, collab.Config{Store: store}, collab.GRPCServerOptions()...)
		if err := send(t, srv, store, conn); err != nil {
			t.Fatalf("a server built with GRPCServerOptions ended the session: %v", err)
		}
	})
}
