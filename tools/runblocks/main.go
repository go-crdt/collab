// Command runblocks checks that every `run:` block in a workflow parses as a
// shell script.
//
// A workflow whose shell is broken is still valid YAML. An edit to the sibling
// repository split a run block in half, between `failed="$failed $t"` and the
// `done` that closed its loop: it parsed as YAML, passed a yaml.safe_load
// check, and failed on the runner with "unexpected end of file"
// (go-crdt/crdt#138).
//
// The oracle is bash itself, through `bash -n`, and deliberately not a shell
// parser written in Go. The question this answers is not whether some parser
// accepts the script, it is whether the shell that will RUN it does -- and only
// that shell can say.
//
// With one exception, measured rather than assumed: `bash -n` accepts an
// UNTERMINATED HEREDOC, silently, exit 0 and no output. So the delimiter is
// checked here. It is not a hypothetical shape either -- a heredoc whose
// terminator is indented does not terminate, and a step added to this very
// workflow on 2026-10-04 had to be checked by hand for exactly that.
//
// An empty selection is an error: a workflow with no run block to check means
// the file moved or the parse changed, and that reads as a clean pass unless it
// is said out loud.
package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"gopkg.in/yaml.v3"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout)) }

type workflow struct {
	Jobs map[string]struct {
		Steps []struct {
			Name string `yaml:"name"`
			Run  string `yaml:"run"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
}

func run(args []string, out io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(out, "usage: runblocks .github/workflows/ci.yml")
		return 2
	}
	src, err := os.ReadFile(args[0])
	if err != nil {
		fmt.Fprintf(out, "::error::runblocks cannot read %s: %v\n", args[0], err)
		return 1
	}
	return check(src, out)
}

func check(src []byte, out io.Writer) int {
	var wf workflow
	if err := yaml.Unmarshal(src, &wf); err != nil {
		fmt.Fprintf(out, "::error::runblocks cannot parse the workflow: %v\n", err)
		return 1
	}

	bad, checked := 0, 0
	for job, j := range wf.Jobs {
		for _, st := range j.Steps {
			if strings.TrimSpace(st.Run) == "" {
				continue
			}
			checked++
			if err := parsesAsShell(st.Run); err != nil {
				fmt.Fprintf(out, "::error::job %q, step %q does not parse as shell: %v\n", job, st.Name, err)
				bad++
			}
		}
	}
	if checked == 0 {
		fmt.Fprintln(out, "::error::no run block found to check: the workflow moved, or this stopped reading it")
		return 1
	}
	fmt.Fprintf(out, "%d run blocks parse as shell\n", checked)
	if bad > 0 {
		return 1
	}
	return 0
}

func parsesAsShell(script string) error {
	cmd := exec.Command("bash", "-n")
	cmd.Stdin = strings.NewReader(script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s", strings.TrimSpace(string(out)))
	}
	return heredocsTerminate(script)
}

// heredocsTerminate reports a heredoc whose delimiter never arrives, which
// `bash -n` does not: measured, it exits 0 and says nothing.
//
// The delimiter has to be a line of its own. For <<- it may be preceded by
// tabs, and for << it may not be preceded by anything at all -- which is how an
// indented terminator silently swallows the rest of a script.
func heredocsTerminate(script string) error {
	lines := strings.Split(script, "\n")
	for i := 0; i < len(lines); i++ {
		word, dash, ok := heredocWord(lines[i])
		if !ok {
			continue
		}
		closed := false
		for j := i + 1; j < len(lines); j++ {
			end := lines[j]
			if dash {
				end = strings.TrimLeft(end, "\t")
			}
			if end == word {
				closed, i = true, j
				break
			}
		}
		if !closed {
			return fmt.Errorf("a heredoc opened with %q is never closed by a line that is exactly %q "+
				"(bash -n does not notice this: it exits 0 and says nothing)", word, word)
		}
	}
	return nil
}

// heredocWord finds the delimiter a line opens a heredoc with, if it does.
func heredocWord(line string) (word string, dash, ok bool) {
	i := strings.Index(line, "<<")
	if i < 0 || strings.HasPrefix(line[i:], "<<<") {
		return "", false, false
	}
	rest := line[i+2:]
	if strings.HasPrefix(rest, "-") {
		dash, rest = true, rest[1:]
	}
	rest = strings.TrimLeft(rest, " \t")
	// The delimiter may be quoted, which changes expansion and not the word.
	switch {
	case strings.HasPrefix(rest, "'"):
		if end := strings.Index(rest[1:], "'"); end >= 0 {
			return rest[1 : 1+end], dash, true
		}
	case strings.HasPrefix(rest, `"`):
		if end := strings.Index(rest[1:], `"`); end >= 0 {
			return rest[1 : 1+end], dash, true
		}
	default:
		if f := strings.FieldsFunc(rest, func(r rune) bool { return r == ' ' || r == '\t' || r == ';' || r == '|' }); len(f) > 0 {
			return f[0], dash, true
		}
	}
	return "", false, false
}
