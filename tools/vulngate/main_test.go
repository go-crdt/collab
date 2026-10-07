package main

import (
	"strings"
	"testing"
)

// The four answers, each on the smallest input that produces it.
//
// The shape of these fixtures is govulncheck's: a stream of objects, a config
// first, then one per advisory in the database, then the findings. A finding
// whose trace names a FUNCTION is one this module calls; a trace that names
// only a module is not.
func TestWhatEachFindingIsWorth(t *testing.T) {
	const head = `{"config":{"scanner_name":"govulncheck"}}
{"osv":{"id":"GO-2026-0001"}}
`
	for _, tt := range []struct {
		name string
		in   string
		want int
		says string
	}{
		{
			name: "carried: not called, and no released fix",
			in:   head + `{"finding":{"osv":"GO-2026-6443","fixed_version":"v1.85.0-dev.0.2026","trace":[{"module":"google.golang.org/grpc"}]}}`,
			want: 0,
			says: "::notice::GO-2026-6443 is in a module this one requires",
		},
		{
			name: "reachable: a frame names a function",
			in:   head + `{"finding":{"osv":"GO-2026-6443","fixed_version":"v1.85.0-dev.0.2026","trace":[{"module":"google.golang.org/grpc","function":"Serve"}]}}`,
			want: 1,
			says: "is reachable from this module: google.golang.org/grpc.Serve",
		},
		{
			name: "a released fix exists, so carrying it is a choice nobody made",
			in:   head + `{"finding":{"osv":"GO-2026-6443","fixed_version":"v1.85.0","trace":[{"module":"google.golang.org/grpc"}]}}`,
			want: 1,
			says: "RELEASED fix exists (v1.85.0)",
		},
		{
			name: "no advisories at all: the scan did not run",
			in:   `{"config":{"scanner_name":"govulncheck"}}`,
			want: 1,
			says: "it did not run",
		},
		{
			name: "nothing at all: the scan did not run",
			in:   "",
			want: 1,
			says: "it did not run",
		},
		{
			name: "clean: a database, no findings",
			in:   head,
			want: 0,
			says: "advisories in the database: 1",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var out strings.Builder
			got := judge(strings.NewReader(tt.in), &out)
			if got != tt.want {
				t.Errorf("exit %d, want %d (said %q)", got, tt.want, out.String())
			}
			if !strings.Contains(out.String(), tt.says) {
				t.Errorf("does not say %q; it said %q", tt.says, out.String())
			}
		})
	}
}

// One advisory reported through several traces is one decision, not several.
func TestOneAdvisoryIsSaidOnce(t *testing.T) {
	in := `{"config":{}}
{"osv":{"id":"GO-2026-0001"}}
{"finding":{"osv":"GO-2026-6443","fixed_version":"v1.85.0-dev","trace":[{"module":"m"}]}}
{"finding":{"osv":"GO-2026-6443","fixed_version":"v1.85.0-dev","trace":[{"module":"m"}]}}
`
	var out strings.Builder
	if code := judge(strings.NewReader(in), &out); code != 0 {
		t.Fatalf("exit %d, want 0", code)
	}
	if n := strings.Count(out.String(), "GO-2026-6443"); n != 1 {
		t.Errorf("said it %d times, want once: %q", n, out.String())
	}
}

// Unparseable output is not a clean run.
func TestGarbageIsNotAPass(t *testing.T) {
	var out strings.Builder
	if code := judge(strings.NewReader("{not json"), &out); code != 1 {
		t.Errorf("exit %d, want 1", code)
	}
	if !strings.Contains(out.String(), "cannot parse") {
		t.Errorf("does not say it could not parse: %q", out.String())
	}
}
