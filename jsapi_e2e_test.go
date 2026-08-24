//go:build !js

package collab_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt/awareness"
)

// The consumer this package is built for cannot call Go: it is a TypeScript
// editor in a page. So the binding in ./wasm is the surface that has to work,
// and this is the test of it — a page joining a document a native participant is
// already editing, driven entirely through globalThis.collab.
//
// It is here rather than under ./wasm because the half that is not the page has
// to be a real server with a real participant on it. A test of the binding
// against a document nobody else is touching would prove that the calls return,
// which is not the question.
func TestTheJavaScriptAPIConverges(t *testing.T) {
	// A skipped test is not a passing one: the job that exists to run this sets
	// COLLAB_REQUIRE_WASM, so a missing toolchain there fails rather than
	// quietly passing.
	required := os.Getenv("COLLAB_REQUIRE_WASM") != ""
	if os.Getenv("COLLAB_SKIP_WASM") != "" {
		t.Skip("COLLAB_SKIP_WASM is set")
	}
	node, err := exec.LookPath("node")
	if err != nil && required {
		t.Fatalf("COLLAB_REQUIRE_WASM is set but node is missing: %v", err)
	} else if err != nil {
		t.Skip("node not found; skipping the JavaScript binding test")
	}
	wasmExec := filepath.Join(runtime.GOROOT(), "lib", "wasm", "wasm_exec.js")
	if _, err := os.Stat(wasmExec); err != nil && required {
		t.Fatalf("COLLAB_REQUIRE_WASM is set but %s is missing: %v", wasmExec, err)
	} else if err != nil {
		t.Skipf("wasm_exec.js not found at %s; skipping", wasmExec)
	}

	wasmPath := filepath.Join(t.TempDir(), "collab.wasm")
	build := exec.Command("go", "build", "-o", wasmPath, "./wasm")
	build.Env = append(os.Environ(), "GOOS=js", "GOARCH=wasm", "GOWORK=off")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building the JavaScript binding failed: %v\n%s", err, out)
	}

	const document = "project:default"
	_, thinURL := serveWebSocket(t)
	native, err := collab.Join(t.Context(), collab.WebSocket(thinURL),
		collab.ClientConfig{Document: document, Site: 1})
	if err != nil {
		t.Fatalf("the native participant could not join: %v", err)
	}
	t.Cleanup(func() { _ = native.Close() })

	nativeBody, err := native.Text("file:main.tex")
	if err != nil {
		t.Fatal(err)
	}
	nativeChat, err := native.List("chat")
	if err != nil {
		t.Fatal(err)
	}
	nativeCells, err := native.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	// One character to join onto, chosen so the page can put an astral one in
	// front of it: from there runes and code units stop agreeing, which is the
	// whole of what the binding has to get right.
	if err := nativeBody.Insert(0, "A"); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	cmd := exec.Command(node, filepath.Join("wasm", "e2e.mjs"))
	cmd.Env = append(os.Environ(),
		"URL="+thinURL,
		"DOCUMENT="+document,
		"WASM="+wasmPath,
		"WASM_EXEC="+wasmExec,
	)
	var page strings.Builder
	cmd.Stdout, cmd.Stderr = &page, &page
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the page: %v", err)
	}
	finished := make(chan error, 1)
	go func() { finished <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	// The native half of the choreography. The page edits first; this waits for
	// its round, then makes two edits whose offsets differ between runes and
	// units — appending after an astral character, and deleting one — because
	// what those are reported as is the thing being tested.
	awaitPage(t, native, "the page's round", finished, &page, func() bool {
		return nativeBody.String() == "😀A" && nativeChat.Len() == 1 && nativeCells.Len() == 1
	})
	if err := nativeBody.Insert(nativeBody.Len(), "Z"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := nativeBody.Delete(0, 1); err != nil { // the emoji: one rune, two units
		t.Fatalf("Delete: %v", err)
	}
	if err := nativeCells.Set("C8", []byte("7")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := nativeChat.Append([]byte("depuis le serveur")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Presence exists for whoever publishes it, so a participant that never says
	// where it is is not on the list. The page expects to see this one.
	if err := native.SetCursor(awareness.Cursor{Anchor: 1, Head: 2},
		map[string]string{"name": "ada"}); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}

	// The page writes one last character and waits to be told it was seen, which
	// is how it knows authorship has settled: one run per author, and the run
	// boundaries are what a view colours by.
	awaitPage(t, native, "the page's last character", finished, &page, func() bool {
		return nativeBody.String() == "AZ!"
	})
	if err := nativeCells.Set("done", []byte("1")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := <-finished; err != nil {
		t.Fatalf("the page-side participant failed: %v\n%s", err, page.String())
	}
	out := page.String()
	marker := strings.Index(out, "WASM_OK ")
	if marker < 0 {
		t.Fatalf("the page-side participant did not report success:\n%s", out)
	}
	var result struct {
		Text     string            `json:"text"`
		Chat     []string          `json:"chat"`
		Cells    map[string]string `json:"cells"`
		Refusals map[string]string `json:"refusals"`
	}
	rest := strings.TrimSpace(out[marker+len("WASM_OK "):])
	if line, _, found := strings.Cut(rest, "\n"); found {
		rest = line
	}
	if err := json.Unmarshal([]byte(rest), &result); err != nil {
		t.Fatalf("unreadable result from the page (%v):\n%s", err, out)
	}

	// The page and the native participant hold the same document, which is what
	// "the same merge logic everywhere" means once a JavaScript API is in the
	// way of one of them.
	awaitFor(t, native, "convergence with the page", settleJS, func() bool {
		return nativeBody.String() == result.Text
	})
	if got := nativeBody.String(); got != result.Text {
		t.Fatalf("the page holds %q, the native participant %q", result.Text, got)
	}
	if len(result.Refusals) == 0 {
		t.Fatal("the page reported no refusals, so it tested none of them")
	}
	t.Logf("the page and a native participant converged on %q", result.Text)
	t.Logf("refusals the page saw: %v", result.Refusals)
}

// awaitPage waits for something only the page can produce, and gives up the
// moment there is no page left to produce it. Both waits in the choreography
// above are for an edit the page has yet to make, so a page that has exited —
// because an assertion in e2e.mjs failed — will never satisfy them, and waiting
// out settleJS only delays the report by a minute before blaming the native
// participant for a timeout.
//
// Worse, the page's own message is thrown away by that route: it is printed
// only where the exit is collected, which a timeout never reaches. That is why
// every failure in issue #41 said a native participant timed out and nothing
// about the assertion that actually failed, and why the fix to the timeout
// message alone would not have shown it either.
func awaitPage(t *testing.T, c *collab.Client, what string, finished <-chan error, page *strings.Builder, want func() bool) {
	t.Helper()
	deadline := time.After(settleJS)
	for !want() {
		select {
		case <-c.Changes():
		case err := <-finished:
			// Reading page is safe here and only here: Wait has returned, so
			// the goroutines copying into it are done.
			t.Fatalf("the page exited before %s (%v):\n%s", what, err, page.String())
		case <-c.Done():
			if !want() {
				t.Fatalf("session ended before %s: %v\n%s", what, c.Err(), describe(t, c))
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s\n%s", what, describe(t, c))
		}
	}
}
