package gitstore

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
)

// A git server that authenticates, so that a credential can be shown to reach
// the wire rather than merely to have been asked for.
//
// Everything before this file proved that Remote.Auth is consulted when it
// should be and that its error stops the operation. None of it proved that what
// the callback returns is what the far end receives, because the transport
// those tests use is a directory and a directory does not ask who you are.
//
// This is git's own smart-HTTP server — git-http-backend, the CGI program every
// git installation ships and every git host runs some form of — behind an
// http.Handler that demands Basic authentication and records exactly what
// arrived. What is asserted here is therefore not this package's idea of a push
// but git's.
type authenticatingGit struct {
	*httptest.Server
	user, pass string

	mu       sync.Mutex
	saw      []string // the Authorization headers that arrived, decoded
	refusals int
}

// gitHTTPBackend locates git's CGI server, or says why the test cannot run.
func gitHTTPBackend(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("git", "--exec-path").Output()
	if err == nil {
		path := filepath.Join(strings.TrimSpace(string(out)), "git-http-backend")
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	// A skipped test is not a passing one: the job that exists to run this sets
	// COLLAB_REQUIRE_GIT_HTTP, so a git without its CGI server fails there
	// rather than quietly proving nothing.
	if os.Getenv("COLLAB_REQUIRE_GIT_HTTP") != "" {
		t.Fatal("git-http-backend is not installed, and COLLAB_REQUIRE_GIT_HTTP says it must be")
	}
	t.Skip("git-http-backend is not installed")
	return ""
}

// serveAuthenticatedGit publishes dir over smart HTTP behind Basic auth.
func serveAuthenticatedGit(t *testing.T, dir, user, pass string) *authenticatingGit {
	t.Helper()
	backend := gitHTTPBackend(t)
	g := &authenticatingGit{user: user, pass: pass}
	handler := &cgi.Handler{
		Path: backend,
		Env: []string{
			"GIT_PROJECT_ROOT=" + dir,
			"GIT_HTTP_EXPORT_ALL=1",
			// Pushing over HTTP is refused by default, which is the whole
			// point of publishing it here.
			"REMOTE_USER=" + user,
		},
	}
	g.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		g.mu.Lock()
		if ok {
			g.saw = append(g.saw, u+":"+p)
		} else {
			g.saw = append(g.saw, "")
		}
		bad := !ok || u != g.user || p != g.pass
		if bad {
			g.refusals++
		}
		g.mu.Unlock()
		if bad {
			w.Header().Set("WWW-Authenticate", `Basic realm="git"`)
			http.Error(w, "who are you", http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(g.Close)
	return g
}

func (g *authenticatingGit) credentialsSeen() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.saw...)
}

func (g *authenticatingGit) refused() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.refusals
}

// bareServed makes a bare repository the CGI server will publish, and returns
// its path on disk and the URL it is reachable at.
func bareServed(t *testing.T, g *authenticatingGit, root, name string) (string, string) {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	runIn(t, path, "init", "--bare", "--initial-branch=main")
	// git-http-backend refuses a push to a repository that has not said it
	// accepts one. This is the server's decision, not the client's, which is
	// why it is made here.
	runIn(t, path, "config", "http.receivepack", "true")
	return path, g.URL + "/" + name
}

// The credential the caller returns is the credential the far end receives.
func TestTheCredentialReachesTheWire(t *testing.T) {
	root := t.TempDir()
	git := serveAuthenticatedGit(t, root, "ada", "correct-horse")
	served, url := bareServed(t, git, root, "papers.git")

	asked := 0
	store := instance(t, "here", url, WithRemote(Remote{
		URL: url,
		Auth: func(context.Context) (transport.AuthMethod, error) {
			asked++
			return &githttp.BasicAuth{Username: "ada", Password: "correct-horse"}, nil
		},
	}))
	if err := store.Save(t.Context(), "d", paper(t, 1, "one").Snapshot()); err != nil {
		t.Fatal(err)
	}

	// git itself, at the far end, holds what was pushed.
	if got := strings.TrimSpace(runIn(t, served, "show", "main:d/paper.tex")); got != "one" {
		t.Fatalf("the served repository holds %q", got)
	}
	// And it received the credential, not merely a request.
	seen := git.credentialsSeen()
	if len(seen) == 0 {
		t.Fatal("the server was never contacted")
	}
	var authenticated bool
	for _, s := range seen {
		if s == "ada:correct-horse" {
			authenticated = true
		}
	}
	if !authenticated {
		t.Fatalf("no request carried the credential; the server saw %q", seen)
	}
	if asked == 0 {
		t.Fatal("the callback was never asked")
	}
}

// A push with no credential is refused by the server, and the document is still
// here — which is the direction this store fails in.
func TestAPushWithNoCredentialIsRefusedAndLosesNothing(t *testing.T) {
	root := t.TempDir()
	git := serveAuthenticatedGit(t, root, "ada", "correct-horse")
	_, url := bareServed(t, git, root, "papers.git")

	var refused []error
	store := instance(t, "here", url, WithRemote(Remote{
		URL:        url,
		PushFailed: func(err error) { refused = append(refused, err) },
	}))
	if err := store.Save(t.Context(), "d", paper(t, 1, "one").Snapshot()); err != nil {
		t.Fatalf("a save failed because a push was refused: %v", err)
	}
	if git.refused() == 0 {
		t.Fatal("the server did not refuse an unauthenticated push")
	}
	if len(refused) != 1 {
		t.Fatalf("the refusal was reported %d times", len(refused))
	}
	if textOf(t, store, "d") != "one" {
		t.Fatal("the document did not survive a refused push")
	}
}

// A wrong credential is refused, and it is refused by the far end rather than
// by anything here.
func TestAWrongCredentialIsRefusedByTheServer(t *testing.T) {
	root := t.TempDir()
	git := serveAuthenticatedGit(t, root, "ada", "correct-horse")
	served, url := bareServed(t, git, root, "papers.git")

	var refused []error
	store := instance(t, "here", url, WithRemote(Remote{
		URL: url,
		Auth: func(context.Context) (transport.AuthMethod, error) {
			return &githttp.BasicAuth{Username: "ada", Password: "battery-staple"}, nil
		},
		PushFailed: func(err error) { refused = append(refused, err) },
	}))
	if err := store.Save(t.Context(), "d", paper(t, 1, "one").Snapshot()); err != nil {
		t.Fatal(err)
	}
	if len(refused) != 1 {
		t.Fatalf("a wrong credential was reported %d times", len(refused))
	}
	if got := git.credentialsSeen(); len(got) == 0 || !strings.Contains(strings.Join(got, " "), "ada:battery-staple") {
		t.Fatalf("the server did not see the wrong credential: %q", got)
	}
	// Nothing arrived: a refused push leaves the far end as it was.
	if out, err := gitFails(served, "show", "main:d/paper.tex"); err == nil {
		t.Fatalf("a refused push wrote to the remote anyway: %q", out)
	}
}

// The callback is asked on every operation, because a token good for an hour
// outlives neither a server nor a document.
func TestTheCredentialIsAskedForEveryOperation(t *testing.T) {
	root := t.TempDir()
	git := serveAuthenticatedGit(t, root, "ada", "correct-horse")
	_, url := bareServed(t, git, root, "papers.git")

	asked := 0
	store := instance(t, "here", url, WithRemote(Remote{
		URL: url,
		Auth: func(context.Context) (transport.AuthMethod, error) {
			asked++
			return &githttp.BasicAuth{Username: "ada", Password: "correct-horse"}, nil
		},
	}))
	if err := store.Save(t.Context(), "d", paper(t, 1, "one").Snapshot()); err != nil {
		t.Fatal(err)
	}
	afterSave := asked
	if afterSave == 0 {
		t.Fatal("a save that pushed did not ask for a credential")
	}
	if err := store.Pull(t.Context()); err != nil {
		t.Fatal(err)
	}
	if asked == afterSave {
		t.Fatal("a pull reused the credential the save was given")
	}
}

// A URL can carry a password, so it must not reach an error message.
func TestTheURLNeverAppearsInAnError(t *testing.T) {
	root := t.TempDir()
	git := serveAuthenticatedGit(t, root, "ada", "correct-horse")
	_, url := bareServed(t, git, root, "papers.git")

	// The shape a person actually pastes into a configuration file.
	withSecret := strings.Replace(url, "http://", "http://ada:hunter2@", 1)
	var refused []error
	store := instance(t, "here", withSecret, WithRemote(Remote{
		URL:        withSecret,
		PushFailed: func(err error) { refused = append(refused, err) },
	}))
	if err := store.Save(t.Context(), "d", paper(t, 1, "one").Snapshot()); err != nil {
		t.Fatal(err)
	}
	if len(refused) != 1 {
		t.Fatalf("the refusal was reported %d times", len(refused))
	}
	if strings.Contains(refused[0].Error(), "hunter2") {
		t.Fatalf("the password reached an error message: %v", refused[0])
	}
	// And a pull, which fails through a different path, says no more.
	err := store.Pull(t.Context())
	if err == nil {
		t.Fatal("a pull with a bad credential succeeded")
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("the password reached a pull's error: %v", err)
	}
	_ = git
}

// gitFails runs git where the test expects it not to work.
func gitFails(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	return string(out), nil
}

var _ = errors.New
