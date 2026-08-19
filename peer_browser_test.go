//go:build !js

package collab_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestPeerBrowserHandshake is the real-browser proof for the WebRTC Peer. The
// Node end-to-end test stubs the RTCPeerConnection; this one uses the browser's
// own. A headless Chrome loads ./browsertest compiled to wasm, which opens two
// real WebRTC connections to an in-page server, has two participants edit one
// document, and asserts each one's edit reached the other — over WebRTC, with
// nothing on either connection.
//
// It needs a browser, which CI does not have, so it skips unless one is found.
// COLLAB_REQUIRE_BROWSER turns a missing browser into a failure, for a machine
// that is meant to have one. Paths are discovered but overridable:
//
//	CHROME                    the Chrome (for Testing) binary
//	COLLAB_BROWSER_NODE_PATH  a node_modules directory holding puppeteer-core
func TestPeerBrowserHandshake(t *testing.T) {
	required := os.Getenv("COLLAB_REQUIRE_BROWSER") != ""
	need := func(what, path string, err error) string {
		if err != nil || path == "" {
			if required {
				t.Fatalf("COLLAB_REQUIRE_BROWSER is set but %s is missing: %v", what, err)
			}
			t.Skipf("%s not found; skipping the real-browser handshake", what)
		}
		return path
	}

	nodePathBin, nodeErr := exec.LookPath("node")
	node := need("node", nodePathBin, nodeErr)
	chromeBin, chromeErr := locateChrome()
	chrome := need("a Chrome binary", chromeBin, chromeErr)
	puppeteerDir, puppeteerErr := locatePuppeteer()
	nodePath := need("puppeteer-core", puppeteerDir, puppeteerErr)
	wasmExec := filepath.Join(runtime.GOROOT(), "lib", "wasm", "wasm_exec.js")
	if _, err := os.Stat(wasmExec); err != nil {
		need("wasm_exec.js", "", err)
	}

	// Assemble the directory the page is served from: the loader, the page, and
	// the wasm built from ./browsertest.
	root := t.TempDir()
	copyFile(t, wasmExec, filepath.Join(root, "wasm_exec.js"))
	copyFile(t, "browsertest/index.html", filepath.Join(root, "index.html"))

	build := exec.Command("go", "build", "-o", filepath.Join(root, "client.wasm"), "./browsertest")
	build.Env = append(os.Environ(), "GOOS=js", "GOARCH=wasm", "GOWORK=off")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building the browser wasm failed: %v\n%s", err, out)
	}

	srv := httptest.NewServer(wasmMIME(http.FileServer(http.Dir(root))))
	defer srv.Close()

	cmd := exec.Command(node, "browsertest/driver.cjs")
	cmd.Env = append(os.Environ(),
		"PAGE_URL="+srv.URL+"/index.html",
		"CHROME="+chrome,
		"NODE_PATH="+nodePath,
	)
	out, err := cmd.CombinedOutput()
	t.Logf("browser driver output:\n%s", out)
	if err != nil {
		t.Fatalf("the headless browser handshake failed: %v", err)
	}
	log := string(out)
	for _, want := range []string{
		"both channels open",
		"edit propagated between participants over WebRTC",
		"RESULT ",
		`"ok":true`,
	} {
		if !strings.Contains(log, want) {
			t.Fatalf("the browser did not report %q in:\n%s", want, out)
		}
	}
}

// wasmMIME serves .wasm as application/wasm, which instantiateStreaming insists
// on and http.FileServer does not always guess.
func wasmMIME(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".wasm") {
			w.Header().Set("Content-Type", "application/wasm")
		}
		next.ServeHTTP(w, r)
	})
}

// locateChrome finds a Chrome (for Testing) binary: the CHROME env if set, else
// the newest one puppeteer has downloaded into its cache.
func locateChrome() (string, error) {
	if env := os.Getenv("CHROME"); env != "" {
		_, err := os.Stat(env)
		return env, err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	patterns := []string{
		filepath.Join(home, ".cache/puppeteer/chrome/*/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing"),
		filepath.Join(home, ".cache/puppeteer/chrome/*/chrome-mac-x64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing"),
		filepath.Join(home, ".cache/puppeteer/chrome/*/chrome-linux64/chrome"),
	}
	return newestMatch(patterns)
}

// locatePuppeteer finds a node_modules with puppeteer-core in it: the override
// env if set, else the one the marp-vscode extension ships.
func locatePuppeteer() (string, error) {
	if env := os.Getenv("COLLAB_BROWSER_NODE_PATH"); env != "" {
		_, err := os.Stat(filepath.Join(env, "puppeteer-core"))
		return env, err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	matches, err := newestMatch([]string{
		filepath.Join(home, ".vscode/extensions/marp-team.marp-vscode-*/node_modules/puppeteer-core"),
	})
	if err != nil {
		return "", err
	}
	return filepath.Dir(matches), nil
}

// newestMatch returns the lexically greatest path any pattern matches, which for
// version-stamped directories is the newest.
func newestMatch(patterns []string) (string, error) {
	best := ""
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return "", err
		}
		for _, m := range matches {
			if m > best {
				best = m
			}
		}
	}
	if best == "" {
		return "", os.ErrNotExist
	}
	return best, nil
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatalf("open %s: %v", src, err)
	}
	defer func() { _ = in.Close() }()
	out, err := os.Create(dst)
	if err != nil {
		t.Fatalf("create %s: %v", dst, err)
	}
	defer func() { _ = out.Close() }()
	if _, err := io.Copy(out, in); err != nil {
		t.Fatalf("copy %s to %s: %v", src, dst, err)
	}
}
