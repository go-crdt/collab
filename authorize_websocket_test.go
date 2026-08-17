//go:build !js

package collab_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/collab/collabpb"
	"github.com/go-crdt/crdt"
	wstransport "github.com/grpc-transports/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Deciding who may open a document needs to know who is asking, and over a
// WebSocket that is a cookie: a browser cannot put a header on one. This is the
// whole chain, end to end — cookie, upgrade, transport credentials, Authorize —
// because each link works on its own and the question is whether they meet.

// documentOwner is the toy rule under test: a session may open the documents
// belonging to whoever the cookie says it is.
func documentOwner(document string) string {
	for i, r := range document {
		if r == '/' {
			return document[:i]
		}
	}
	return ""
}

// serveOverWebSocket starts a Collab server whose Authorize decides from the
// session cookie the HTTP handshake carried.
func serveOverWebSocket(t *testing.T) string {
	t.Helper()
	lis, err := wstransport.ListenWebSocket("127.0.0.1:0", wstransport.ServerConfig{
		OriginPatterns: []string{"*"},
		// Runs while the upgrade is still an HTTP request, which is the only
		// moment the cookie exists.
		OnUpgrade: func(r *http.Request) (any, error) {
			cookie, err := r.Cookie("session")
			if err != nil {
				return nil, &wstransport.UpgradeError{
					Code:    http.StatusUnauthorized,
					Message: "a session cookie is required",
					Err:     err,
				}
			}
			return cookie.Value, nil
		},
	})
	if err != nil {
		t.Fatalf("ListenWebSocket: %v", err)
	}

	srv := collab.NewServer(collab.Config{
		Authorize: func(ctx context.Context, document string, _ crdt.SiteID) error {
			user, ok := wstransport.FromContext(ctx)
			if !ok {
				return errors.New("no session")
			}
			if user != documentOwner(document) {
				return status.Errorf(codes.PermissionDenied,
					"%q is not %s's document", document, user)
			}
			return nil
		},
	})
	gs := grpc.NewServer(grpc.Creds(wstransport.ServerCredentials()))
	collabpb.RegisterCollabServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(func() {
		gs.Stop()
		_ = lis.Close()
	})
	return lis.Addr().String()
}

// dialAs connects carrying a session cookie, the way a browser would.
func dialAs(t *testing.T, addr, user string) *grpc.ClientConn {
	t.Helper()
	header := http.Header{}
	if user != "" {
		header.Set("Cookie", "session="+user)
	}
	opt, err := wstransport.DialOption("ws://"+addr, wstransport.ClientConfig{HTTPHeader: header})
	if err != nil {
		t.Fatalf("DialOption: %v", err)
	}
	conn, err := grpc.NewClient("passthrough:///collab",
		grpc.WithTransportCredentials(insecure.NewCredentials()), opt)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestAuthorizeFromTheUpgradeCookie(t *testing.T) {
	addr := serveOverWebSocket(t)

	// Ada opens her own document, and a second participant of hers joins it.
	ada := join(t, dialAs(t, addr, "ada"), collab.ClientConfig{Document: "ada/notes", Site: 1})
	if err := body(t, ada).Insert(0, "mine"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	second := join(t, dialAs(t, addr, "ada"), collab.ClientConfig{Document: "ada/notes", Site: 2})
	awaitText(t, second, "mine")

	// Grace may not, and is told so rather than being let in.
	_, err := collab.Join(t.Context(), dialAs(t, addr, "grace"),
		collab.ClientConfig{Document: "ada/notes", Site: 3})
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Fatalf("joining someone else's document = %v (%v), want PermissionDenied", got, err)
	}

	// Grace's own document is hers.
	hers := join(t, dialAs(t, addr, "grace"), collab.ClientConfig{Document: "grace/notes", Site: 4})
	if err := body(t, hers).Insert(0, "also mine"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if got, want := text(t, hers), "also mine"; got != want {
		t.Fatalf("Text() = %q, want %q", got, want)
	}

	// And a connection with no cookie never becomes a session at all: the
	// refusal happens during the handshake, before any gRPC stream exists.
	if _, err := collab.Join(t.Context(), dialAs(t, addr, ""),
		collab.ClientConfig{Document: "ada/notes", Site: 5}); err == nil {
		t.Fatal("a session with no cookie was accepted")
	}
}
