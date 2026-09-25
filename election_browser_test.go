//go:build !js

package collab_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestTwoTabsElectOneHostWithNoWindow is the real-browser proof that a host
// election decided by a Web Lock leaves exactly one host.
//
// It needs two pages, which is why it is not folded into the WebRTC proof: a lock
// is exclusive across tabs and invisible inside one, so a single-page harness
// cannot ask the question at all.
//
// # What makes it discriminating
//
// The election window is ZERO. With a zero window electRole concludes at once that
// it heard no host and hosts, so BOTH tabs would host -- which is precisely the
// starvation electRole's own comment describes ("a machine loaded enough to leave
// the frames in a queue closes both windows having heard nothing, and both tabs
// elect themselves"), reached deliberately rather than by loading a machine. If the
// lock is taken, exactly one tab hosts whatever the window says.
//
// So this fails if the lock path is not taken, and it fails for the right reason:
// the driver reports how many tabs hosted.
//
// # What it does not claim
//
// That the fallback is gone. It is not: a browser without Web Locks gets the window
// election exactly as before, and [collab.ErrHostSuperseded] still heals a room that
// ends up with two hosts. Only Chrome is measured here, because only Chrome is what
// the browser lane pins; the driver reports whether the API was there at all, so a
// pass on a browser without it would be visible rather than silent.
func TestTwoTabsElectOneHostWithNoWindow(t *testing.T) {
	required := os.Getenv("COLLAB_REQUIRE_BROWSER") != ""
	need := func(what, path string, err error) string {
		t.Helper()
		if err != nil || path == "" {
			if required {
				t.Fatalf("COLLAB_REQUIRE_BROWSER is set but %s is missing: %v", what, err)
			}
			t.Skipf("%s not found; skipping the host-election proof", what)
		}
		return path
	}
	nodeBin, nodeErr := exec.LookPath("node")
	node := need("node", nodeBin, nodeErr)
	chromeBin, chromeErr := locateChrome()
	chrome := need("a Chrome binary", chromeBin, chromeErr)
	puppeteerDir, puppeteerErr := locatePuppeteer()
	nodePath := need("puppeteer-core", puppeteerDir, puppeteerErr)
	wasmExec := filepath.Join(runtime.GOROOT(), "lib", "wasm", "wasm_exec.js")
	if _, err := os.Stat(wasmExec); err != nil {
		need("wasm_exec.js", "", err)
	}

	root := t.TempDir()
	copyFile(t, wasmExec, filepath.Join(root, "wasm_exec.js"))
	copyFile(t, "electtest/index.html", filepath.Join(root, "index.html"))

	build := exec.Command("go", "build", "-o", filepath.Join(root, "elect.wasm"), "./electtest")
	build.Env = append(os.Environ(), "GOOS=js", "GOARCH=wasm", "GOWORK=off")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building the election wasm failed: %v\n%s", err, out)
	}

	srv := httptest.NewServer(wasmMIME(http.FileServer(http.Dir(root))))
	defer srv.Close()

	cmd := exec.Command(node, "electtest/driver.cjs")
	cmd.Env = append(os.Environ(),
		"PAGE_URL="+srv.URL+"/index.html",
		"CHROME="+chrome,
		"NODE_PATH="+nodePath,
	)
	out, err := cmd.CombinedOutput()
	t.Logf("browser driver output:\n%s", out)
	if err != nil {
		t.Fatalf("the two tabs did not elect one host: %v", err)
	}
	log := string(out)
	for _, want := range []string{
		`"hosts":1`,
		`"locksAvailable":true`,
		"RESULT ",
		`"ok":true`,
	} {
		if !strings.Contains(log, want) {
			t.Fatalf("the browser did not report %q in:\n%s", want, out)
		}
	}
}
