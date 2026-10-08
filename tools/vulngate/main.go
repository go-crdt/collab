// Command vulngate reads govulncheck's JSON and decides what to do about it.
//
// The exit status is not the gate. Measured on 2026-10-04, govulncheck exits 0
// over an advisory it has decided this module does not call -- GO-2026-6443, a
// gRPC server that panics on a request with no :authority or Host header, in a
// version this module requires. A lane that reads the status is therefore
// silent about exactly the finding somebody has to decide about.
//
// So this reads the findings:
//
//	reachable from this module                      error
//	in a required module, with a RELEASED fix       error -- upgrade rather than carry it
//	anything else                                   a notice, named in the log
//	no config block, or no advisories at all        error: the scan did not run
//
// The last one matters most: a scan that could not run reports nothing wrong.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout)) }

// govulncheck writes a stream of JSON objects, one per line group; only these
// fields matter here.
type message struct {
	Config *struct{} `json:"config"`
	OSV    *struct {
		ID string `json:"id"`
	} `json:"osv"`
	Finding *struct {
		OSV          string `json:"osv"`
		FixedVersion string `json:"fixed_version"`
		Trace        []struct {
			Module   string `json:"module"`
			Function string `json:"function"`
		} `json:"trace"`
	} `json:"finding"`
}

var released = regexp.MustCompile(`^v\d+\.\d+\.\d+$`)

func run(args []string, out io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(out, "usage: vulngate findings.json")
		return 2
	}
	f, err := os.Open(args[0])
	if err != nil {
		fmt.Fprintf(out, "::error::vulngate cannot read %s: %v\n", args[0], err)
		return 1
	}
	defer f.Close()
	return judge(f, out)
}

func judge(r io.Reader, out io.Writer) int {
	dec := json.NewDecoder(r)
	var configs, advisories int
	type seen struct {
		osv    string
		called bool
	}
	said := map[seen]bool{}
	bad := false

	for {
		var m message
		if err := dec.Decode(&m); err != nil {
			if err == io.EOF {
				break
			}
			fmt.Fprintf(out, "::error::vulngate cannot parse govulncheck's output: %v\n", err)
			return 1
		}
		if m.Config != nil {
			configs++
		}
		if m.OSV != nil {
			advisories++
		}
		if m.Finding == nil {
			continue
		}
		called := false
		var where string
		for _, fr := range m.Finding.Trace {
			if fr.Function != "" {
				called, where = true, fr.Module+"."+fr.Function
				break
			}
		}
		key := seen{m.Finding.OSV, called}
		if said[key] {
			continue // govulncheck reports a finding once per trace; say it once
		}
		said[key] = true

		switch {
		case called:
			fmt.Fprintf(out, "::error::%s is reachable from this module: %s (fixed in %s)\n",
				m.Finding.OSV, where, m.Finding.FixedVersion)
			bad = true
		case released.MatchString(m.Finding.FixedVersion):
			fmt.Fprintf(out, "::error::%s is not called from here, and a RELEASED fix exists (%s): upgrade rather than carry it\n",
				m.Finding.OSV, m.Finding.FixedVersion)
			bad = true
		default:
			fmt.Fprintf(out, "::notice::%s is in a module this one requires, not called from here, and has no released fix yet (upstream fix: %s)\n",
				m.Finding.OSV, fixOrNone(m.Finding.FixedVersion))
		}
	}

	if configs == 0 || advisories == 0 {
		fmt.Fprintln(out, "::error::govulncheck produced no config or no advisories: it did not run")
		return 1
	}
	fmt.Fprintf(out, "advisories in the database: %d; findings judged: %d\n", advisories, len(said))
	if bad {
		return 1
	}
	return 0
}

func fixOrNone(v string) string {
	if strings.TrimSpace(v) == "" {
		return "none published"
	}
	return v
}
