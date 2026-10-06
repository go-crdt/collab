//go:build !js

package collab_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// A version number written into prose rots at the next dependency bump, and
// rots silently: nothing builds the documents. This one did. `docs/design.md`
// named `grpc-transports/websocket` v0.2.0 for two releases after the module
// moved to v0.4.0.
//
// The rule is not that the two must be equal. Naming the release a capability
// arrived in is useful and stays true -- "it has carried a cookie since v0.2.0"
// is a fact about v0.2.0, not about the build. What is never true is naming a
// version NEWER than the one this module compiles against: that documents an
// API nobody here has.
//
// The search is driven by go.mod rather than by a pattern guessed from the
// prose. The first version of this test was a regular expression requiring a
// domain, and it missed the very line that prompted it, because the documents
// write "grpc-transports/websocket" without the host.
func TestAVersionNamedInTheDocsIsNotAheadOfTheBuild(t *testing.T) {
	gomod, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	// A pseudo-version (v0.0.0-20260706201446-f0a921348800) is excluded by the
	// anchored end of this: it is not a release and nothing names it in prose.
	require := regexp.MustCompile(`(?m)^\s*([a-z0-9.-]+\.[a-z]{2,}/[A-Za-z0-9._/-]+)\s+v(\d+)\.(\d+)\.(\d+)\s*(//.*)?$`)
	type version struct{ major, minor, patch int }
	built := map[string]version{} // every name prose might use -> the version built
	var modules int
	for _, m := range require.FindAllStringSubmatch(string(gomod), -1) {
		path := m[1]
		n := func(s string) int { v, _ := strconv.Atoi(s); return v }
		v := version{n(m[2]), n(m[3]), n(m[4])}
		modules++
		// The full path and the path without its host, which is how the
		// documents write it. NOT the last segment alone: "rpc" is a substring
		// of "grpc", and the first version of this test compared a mention of
		// grpc against genproto/googleapis/rpc because of it.
		parts := strings.Split(path, "/")
		for _, name := range []string{path, strings.Join(parts[1:], "/")} {
			if strings.Contains(name, "/") {
				built[name] = v
			}
		}
	}
	if modules < 5 {
		t.Fatalf("read %d requirements from go.mod, which is too few to be this "+
			"module: the parse is broken, not the documents", modules)
	}

	var read, checked int
	err = filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".md") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		read++
		for name, have := range built {
			// The name, then anything short of a newline, then a version: the
			// documents write "`grpc-transports/websocket` v0.2.0" with the
			// backticks in between.
			// A word boundary in front, so a name is not matched inside a
			// longer one, and at most a backtick and a space in between.
			near := regexp.MustCompile(`(^|[^A-Za-z0-9._/-])` + regexp.QuoteMeta(name) + "[^\n]{0,4}?\\bv(\\d+)\\.(\\d+)\\.(\\d+)")
			for _, m := range near.FindAllStringSubmatch(string(body), -1) {
				n := func(s string) int { v, _ := strconv.Atoi(s); return v }
				said := version{n(m[2]), n(m[3]), n(m[4])}
				checked++
				ahead := said.major > have.major ||
					(said.major == have.major && said.minor > have.minor) ||
					(said.major == have.major && said.minor == have.minor && said.patch > have.patch)
				if ahead {
					t.Errorf("%s names %s v%d.%d.%d, and go.mod builds against v%d.%d.%d: "+
						"the documents describe an API this module does not have",
						path, name, said.major, said.minor, said.patch,
						have.major, have.minor, have.patch)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if read < 2 || checked == 0 {
		t.Fatalf("read %d markdown files and found %d version mentions, which is "+
			"too few to be this repository: the scan is broken", read, checked)
	}
	t.Logf("%d requirements, %d markdown files, %d version mentions checked", modules, read, checked)
}
