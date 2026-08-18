//go:build !js

package collab_test

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/collab/collabpb"
	wstransport "github.com/grpc-transports/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// This is the claim the whole project rests on: the merge logic that runs on the
// server is the same code that runs in a browser. Compiling for js/wasm proves
// nothing on its own — plenty of code compiles and then fails to run — so the
// test below actually runs it.
//
// A real grpc.Server listens over a real WebSocket. A native participant edits.
// Two more participants, compiled to WebAssembly and executed by Node through
// the same transport a browser would use, edit at the same time. All three must
// end up with the same text, and no one's character may be lost.

// serveWebSocket starts one Collab server behind two carriers and returns an
// address for each: gRPC tunnelled over a WebSocket, and the session's own
// framing over a plain one.
//
// One server, two doors. Which door a participant came through is not something
// the document can tell, and that is the claim these tests are here to check.
func serveWebSocket(t *testing.T) (grpcAddr, thinURL string) {
	t.Helper()
	srv := collab.NewServer(collab.Config{})

	lis, err := wstransport.ListenWebSocket("127.0.0.1:0", wstransport.ServerConfig{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		t.Fatalf("ListenWebSocket: %v", err)
	}
	gs := grpc.NewServer()
	collabpb.RegisterCollabServer(gs, collab.GRPCService(srv))
	go func() { _ = gs.Serve(lis) }()

	thin := httptest.NewServer(srv.ServeWebSocket("*"))
	t.Cleanup(func() {
		thin.Close()
		gs.Stop()
		_ = lis.Close()
	})
	return lis.Addr().String(), "ws" + strings.TrimPrefix(thin.URL, "http")
}

// dialWebSocket connects to the server the way a native participant would.
func dialWebSocket(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	opt, err := wstransport.DialOption("ws://"+addr, wstransport.ClientConfig{})
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

// A native participant over the real WebSocket transport, with no WebAssembly
// involved — the half of the proof that does not need Node.
func TestOverWebSocket(t *testing.T) {
	grpcAddr, thinURL := serveWebSocket(t)

	// One participant through each door, editing one document.
	ada := join(t, dialWebSocket(t, grpcAddr), collab.ClientConfig{Document: "ws", Site: 1})
	grace, err := collab.Join(t.Context(), collab.WebSocket(thinURL),
		collab.ClientConfig{Document: "ws", Site: 2})
	if err != nil {
		t.Fatalf("joining over the thin carrier: %v", err)
	}
	t.Cleanup(func() { _ = grace.Close() })

	if err := body(t, ada).Insert(0, "over a websocket"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	awaitText(t, grace, "over a websocket")
	// And back the other way, so neither carrier is only ever listening.
	if err := body(t, grace).Insert(16, ", both ways"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	awaitText(t, ada, "over a websocket, both ways")
}

// TestWasmConverges is the acceptance gate: two WebAssembly participants and one
// native participant editing one document at the same time, all three converging
// on the same text.
func TestWasmConverges(t *testing.T) {
	// A skipped test is not a passing one. CI sets COLLAB_REQUIRE_WASM on the job
	// that exists to run this, so a missing toolchain there is a failure rather
	// than a green tick; the emulated-architecture jobs set COLLAB_SKIP_WASM,
	// because running Node under qemu proves nothing about this package.
	required := os.Getenv("COLLAB_REQUIRE_WASM") != ""
	if os.Getenv("COLLAB_SKIP_WASM") != "" {
		t.Skip("COLLAB_SKIP_WASM is set")
	}
	node, err := exec.LookPath("node")
	if err != nil && required {
		t.Fatalf("COLLAB_REQUIRE_WASM is set but node is missing: %v", err)
	} else if err != nil {
		t.Skip("node not found; skipping the WebAssembly end-to-end test")
	}
	wasmExec := filepath.Join(runtime.GOROOT(), "lib", "wasm", "wasm_exec.js")
	if _, err := os.Stat(wasmExec); err != nil && required {
		t.Fatalf("COLLAB_REQUIRE_WASM is set but %s is missing: %v", wasmExec, err)
	} else if err != nil {
		t.Skipf("wasm_exec.js not found at %s; skipping", wasmExec)
	}

	wasmPath := filepath.Join(t.TempDir(), "client.wasm")
	build := exec.Command("go", "build", "-o", wasmPath, "./wasmtest")
	build.Env = append(os.Environ(), "GOOS=js", "GOARCH=wasm", "GOWORK=off")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building the WebAssembly client failed: %v\n%s", err, out)
	}

	grpcAddr, thinURL := serveWebSocket(t)
	// The native participant comes in over gRPC and the WebAssembly ones over the
	// framing a browser can afford, which is what makes this a test of both.
	native := join(t, dialWebSocket(t, grpcAddr), collab.ClientConfig{Document: "shared", Site: 1})
	if err := body(t, native).Insert(0, "A"); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	cmd := exec.Command(node, filepath.Join("wasmtest", "run.mjs"))
	cmd.Env = append(os.Environ(),
		"URL="+thinURL,
		"DOCUMENT=shared",
		"WASM="+wasmPath,
		"WASM_EXEC="+wasmExec,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("the WebAssembly participants failed: %v\n%s", err, out)
	}
	line := string(out)
	marker := strings.Index(line, "WASM_OK ")
	if marker < 0 {
		t.Fatalf("the WebAssembly participants did not report success:\n%s", out)
	}
	var wasm struct {
		First  string `json:"first"`
		Second string `json:"second"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(line[marker+len("WASM_OK "):])), &wasm); err != nil {
		t.Fatalf("unreadable result from the WebAssembly participants (%v):\n%s", err, out)
	}

	// The native participant has to reach the same text the two WebAssembly ones
	// did — that is what "the same merge logic everywhere" means.
	await(t, native, "convergence with the WebAssembly participants", func() bool {
		return body(t, native).Len() == 3
	})
	got := text(t, native)
	if wasm.First != got || wasm.Second != got {
		t.Fatalf("replicas disagree:\n  native      %q\n  wasm first  %q\n  wasm second %q",
			got, wasm.First, wasm.Second)
	}
	for _, char := range []string{"A", "B", "C"} {
		if !strings.Contains(got, char) {
			t.Fatalf("%q is missing from %q: a participant's character was lost", char, got)
		}
	}
	t.Logf("three replicas across two runtimes converged on %q", got)
}
