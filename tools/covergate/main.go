// Command covergate reports the coverage of code the native gate never sees.
//
// The 100% statement gate runs natively, so it compiles none of the files built
// only for the browser: five of them, 780 lines, 15.8% of the package on
// 2026-10-07. They are TESTED -- by the wasm lane and by the real-browser lane
// -- and they were never MEASURED, which is how a gate at a hundred per cent
// and a blind spot come to live side by side.
//
// A floor rather than a gate, because 100% is not where that code is today:
// 427 of 510 statements, with websocket_js.go the thinnest at 54.8%. The floor
// catches a collapse and puts the number in every run's log. Closing the gap is
// work; pretending the existing gate already covers it would be worse than
// saying so.
//
// An empty selection is an error rather than a clean pass: renaming a file out
// of *_js.go is enough to make this measure nothing.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout)) }

func run(argv []string, out io.Writer) int {
	fs := flag.NewFlagSet("covergate", flag.ContinueOnError)
	fs.SetOutput(out)
	floor := fs.Float64("floor", 75, "fail below this percentage")
	suffix := fs.String("suffix", "_js.go", "the files to report on, by filename suffix")
	if err := fs.Parse(argv); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(out, "usage: covergate [-floor 75] [-suffix _js.go] cover.out")
		return 2
	}
	f, err := os.Open(fs.Arg(0))
	if err != nil {
		fmt.Fprintf(out, "::error::covergate cannot read %s: %v\n", fs.Arg(0), err)
		return 1
	}
	defer f.Close()
	return judge(f, *suffix, *floor, out)
}

type counts struct{ total, covered int }

func judge(r io.Reader, suffix string, floor float64, out io.Writer) int {
	per := map[string]*counts{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "mode:") {
			continue
		}
		// name.go:line.col,line.col numberOfStatements count
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		stmts, err1 := strconv.Atoi(fields[1])
		hits, err2 := strconv.Atoi(fields[2])
		if err1 != nil || err2 != nil {
			continue
		}
		name := path.Base(strings.SplitN(fields[0], ":", 2)[0])
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		c := per[name]
		if c == nil {
			c = &counts{}
			per[name] = c
		}
		c.total += stmts
		if hits > 0 {
			c.covered += stmts
		}
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintf(out, "::error::covergate cannot read the profile: %v\n", err)
		return 1
	}
	if len(per) == 0 {
		fmt.Fprintf(out, "::error::no file ending in %q appears in the profile: this measure has "+
			"stopped measuring anything, which a rename is enough to cause\n", suffix)
		return 1
	}

	names := make([]string, 0, len(per))
	for n := range per {
		names = append(names, n)
	}
	sort.Strings(names)

	var total, covered int
	fmt.Fprintf(out, "code ending in %q, which the native 100%% gate never sees:\n", suffix)
	for _, n := range names {
		c := per[n]
		total += c.total
		covered += c.covered
		fmt.Fprintf(out, "  %-24s %4d/%-4d %5.1f%%\n", n, c.covered, c.total, pct(c.covered, c.total))
	}
	fmt.Fprintf(out, "  %-24s %4d/%-4d %5.1f%%\n", "total", covered, total, pct(covered, total))

	if p := pct(covered, total); p < floor {
		fmt.Fprintf(out, "::error::coverage is %.1f%%, below the %.1f%% floor\n", p, floor)
		return 1
	}
	fmt.Fprintf(out, "::notice::coverage %.1f%% over %d statements, floor %.1f%%\n", pct(covered, total), total, floor)
	return 0
}

func pct(covered, total int) float64 {
	if total == 0 {
		return 0
	}
	return 100 * float64(covered) / float64(total)
}
