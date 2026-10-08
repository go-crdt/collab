package main

import (
	"strings"
	"testing"
)

func TestTheThreeAnswers(t *testing.T) {
	const profile = `mode: set
github.com/go-crdt/collab/peer_js.go:10.2,12.3 2 1
github.com/go-crdt/collab/peer_js.go:20.2,22.3 2 0
github.com/go-crdt/collab/server.go:30.2,31.3 1 1
`
	for _, tt := range []struct {
		name  string
		in    string
		floor float64
		want  int
		says  string
	}{
		{
			name:  "above the floor",
			in:    profile,
			floor: 40,
			want:  0,
			says:  "peer_js.go",
		},
		{
			name:  "below the floor",
			in:    profile,
			floor: 75,
			want:  1,
			says:  "below the 75.0% floor",
		},
		{
			// A rename out of the suffix is enough to cause this, and it reads
			// as a clean pass unless it is an error.
			name:  "nothing matched the suffix",
			in:    "mode: set\ngithub.com/go-crdt/collab/server.go:30.2,31.3 1 1\n",
			floor: 0,
			want:  1,
			says:  "stopped measuring anything",
		},
		{
			name:  "an empty profile is not a clean one",
			in:    "",
			floor: 0,
			want:  1,
			says:  "stopped measuring anything",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var out strings.Builder
			if got := judge(strings.NewReader(tt.in), "_js.go", tt.floor, &out); got != tt.want {
				t.Errorf("exit %d, want %d (said %q)", got, tt.want, out.String())
			}
			// Normalised, so a column width is not an assertion.
			flat := strings.Join(strings.Fields(out.String()), " ")
			if !strings.Contains(flat, strings.Join(strings.Fields(tt.says), " ")) {
				t.Errorf("does not say %q; it said:\n%s", tt.says, out.String())
			}
			if tt.name == "above the floor" && !strings.Contains(flat, "peer_js.go 2/4 50.0%") {
				t.Errorf("the per-file line is not what was measured: %s", flat)
			}
		})
	}
}

// The native gate counts only what builds natively, so a file that is in the
// profile but not browser-only must not be counted here.
func TestOnlyTheChosenSuffixCounts(t *testing.T) {
	in := `mode: set
github.com/go-crdt/collab/peer_js.go:1.1,2.2 10 10
github.com/go-crdt/collab/server.go:1.1,2.2 90 0
`
	var out strings.Builder
	if code := judge(strings.NewReader(in), "_js.go", 99, &out); code != 0 {
		t.Fatalf("exit %d, want 0: server.go's zero coverage was counted", code)
	}
	if !strings.Contains(out.String(), "10/10") {
		t.Errorf("the total is not the browser-only one: %q", out.String())
	}
}
