//go:build !js

package collab_test

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/collab/collabpb"
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
// refuses it -- and that is the point of the option existing and of the doc
// comment naming it; a server built with them carries it.
func TestAGRPCServerNeedsTheLibrarysOptionsForALargeEdit(t *testing.T) {
	big := strings.Repeat("x", 5<<20) // one operations message past 4 MiB

	send := func(t *testing.T, conn *grpc.ClientConn) error {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
		// A second participant is the barrier: it sees the text only if the
		// server accepted the message and broadcast it.
		w, err := collab.Join(ctx, collab.GRPC(conn), collab.ClientConfig{Document: "d", Site: 2})
		if err != nil {
			return err
		}
		defer func() { _ = w.Close() }()
		wb, err := w.Text("body")
		if err != nil {
			return err
		}
		deadline := time.After(30 * time.Second)
		for wb.Len() != len(big) {
			select {
			case <-w.Changes():
			case <-w.Done():
				return w.Err()
			case <-c.Done():
				return c.Err()
			case <-deadline:
				return context.DeadlineExceeded
			}
		}
		return nil
	}

	t.Run("a stock server refuses it", func(t *testing.T) {
		_, conn, _ := serveGRPCWith(t, collab.Config{Store: collab.NewMemoryStore()})
		if err := send(t, conn); err == nil {
			t.Fatal("a stock grpc.Server carried a message past its own limit")
		}
	})

	t.Run("a server built as documented carries it", func(t *testing.T) {
		_, conn, _ := serveGRPCWith(t, collab.Config{Store: collab.NewMemoryStore()}, collab.GRPCServerOptions()...)
		if err := send(t, conn); err != nil {
			t.Fatalf("a server built with GRPCServerOptions refused a %d-byte edit: %v", len(big), err)
		}
	})
}
