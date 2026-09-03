//go:build !js

package collab

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/go-crdt/collab/collabpb"
	"github.com/go-crdt/crdt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// A participant that cannot read this build's snapshot format is sent the whole
// history instead, which is about three and a quarter times larger -- so the
// peer least able to be upgraded is the one that meets gRPC's four-mebibyte
// default and is told "received message larger than max".
//
// That names a resource rather than the version mismatch behind it, and it is
// the fallback path this build chose for that peer. The WebSocket carrier has
// raised the same limit since it was written.
func TestAWelcomeLargerThanGRPCsDefaultStillArrives(t *testing.T) {
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	// Enough text that its history passes four mebibytes. Inserted in chunks
	// because the size that matters is the operations, not the characters.
	doc, _, err := srv.openAndJoin(t.Context(), joinMsg{Document: "big", Site: 9})
	if err != nil {
		t.Fatal(err)
	}
	text, err := doc.doc.Text("t")
	if err != nil {
		t.Fatal(err)
	}
	chunk := strings.Repeat("a sentence somebody wrote, and thought about again. ", 40)
	for range 170 {
		if _, err := text.Insert(0, chunk); err != nil {
			t.Fatal(err)
		}
	}
	ops, err := crdt.AppendPartOps(nil, doc.doc.OpsSince(nil))
	if err != nil {
		t.Fatal(err)
	}
	const grpcDefault = 4 << 20
	if len(ops) <= grpcDefault {
		t.Fatalf("the history is %d bytes, which does not reach gRPC's %d-byte default: "+
			"this test would pass without the limit being raised", len(ops), grpcDefault)
	}
	t.Logf("history %d bytes against a snapshot's %d, and gRPC's default is %d",
		len(ops), len(doc.doc.Snapshot()), grpcDefault)

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer(grpc.MaxSendMsgSize(maxMessage))
	collabpb.RegisterCollabServer(gs, GRPCService(srv))
	go func() { _ = gs.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///collab",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		gs.Stop()
		_ = lis.Close()
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	carrier, err := GRPC(conn).open(ctx)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer carrier.Close()

	// No advertisement: this is the participant that gets the history.
	if err := carrier.Send(kindJoin, joinMsg{Document: "big", Site: 2}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	kind, msg, err := carrier.Recv()
	if err != nil {
		t.Fatalf("the welcome did not arrive, which is what gRPC's default does "+
			"to the fallback path: %v", err)
	}
	if kind != kindWelcome {
		t.Fatalf("got kind %d, want a welcome", kind)
	}
	w := msg.(welcomeMsg)
	if len(w.Snapshot) > 0 {
		t.Fatal("a participant that said nothing was sent a snapshot")
	}
	if len(w.Operations) <= grpcDefault {
		t.Fatalf("the welcome carried %d bytes, which does not exercise the limit",
			len(w.Operations))
	}
}
