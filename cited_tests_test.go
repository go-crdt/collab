//go:build !js

package collab_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// This package ties a claim to the test that holds it by NAME, in doc comments
// and in the two documents people read -- README.md and docs/design.md. A name
// is a link with nothing checking it: rename the test and the prose still reads
// as if the guarantee were pinned, which is worse than saying nothing.
//
// The rule is: a cited test is defined in this module, or the prose says whose
// it is. One citation is crdt's, deliberately, because the loss it describes is
// crdt's to demonstrate; when that module's source can be found, this resolves
// the name there too rather than taking the attribution on trust.
func TestEveryTestThisPackageCitesExists(t *testing.T) {
	cited := regexp.MustCompile(`\b((?:Test|Fuzz|Benchmark)[A-Z][A-Za-z0-9_]{6,})\b`)
	defined := regexp.MustCompile(`\bfunc ((?:Test|Fuzz|Benchmark)[A-Za-z0-9_]+)\b`)

	here := map[string]bool{}
	type site struct{ file, context string }
	mentions := map[string][]site{}
	var read int

	err := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" || d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		isGo, isMD := strings.HasSuffix(path, ".go"), strings.HasSuffix(path, ".md")
		if !isGo && !isMD {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		read++
		for _, m := range defined.FindAllStringSubmatch(string(body), -1) {
			here[m[1]] = true
		}
		if strings.HasSuffix(path, "_test.go") {
			// A test file naming another test is code, not a claim in prose.
			return nil
		}
		for _, loc := range cited.FindAllStringSubmatchIndex(string(body), -1) {
			name := string(body[loc[2]:loc[3]])
			from := max(loc[2]-40, 0)
			mentions[name] = append(mentions[name], site{path, string(body[from:loc[2]])})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// A scan that cannot read reports nothing wrong. These two numbers are what
	// the repository holds today; they are a floor, not an assertion about the
	// exact count.
	if read < 100 || len(mentions) < 20 {
		t.Fatalf("read %d files and found %d cited names, which is too few to be "+
			"this repository: the scan is broken, not the prose", read, len(mentions))
	}

	// Best effort, and never a skip: without it the attribution below is taken
	// on trust, which is still a check.
	elsewhere := map[string]bool{}
	if dir, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/go-crdt/crdt").Output(); err == nil {
		root := strings.TrimSpace(string(dir))
		if entries, err := os.ReadDir(root); err == nil {
			for _, e := range entries {
				if !strings.HasSuffix(e.Name(), "_test.go") {
					continue
				}
				body, err := os.ReadFile(filepath.Join(root, e.Name()))
				if err != nil {
					continue
				}
				for _, m := range defined.FindAllStringSubmatch(string(body), -1) {
					elsewhere[m[1]] = true
				}
			}
		}
	}
	t.Logf("%d files read, %d names cited in prose, %d defined here, %d found in crdt",
		read, len(mentions), len(here), len(elsewhere))

	for name, sites := range mentions {
		if here[name] {
			continue
		}
		for _, s := range sites {
			if !strings.Contains(s.context, "crdt") {
				t.Errorf("%s cites %s, which is not defined in this module and is not "+
					"said to be anybody else's: either the test was renamed or the prose "+
					"has to say where it lives", s.file, name)
				continue
			}
			if len(elsewhere) > 0 && !elsewhere[name] {
				t.Errorf("%s cites crdt's %s, and the crdt source this module builds "+
					"against has no such test: it was renamed there", s.file, name)
			}
		}
	}
}
