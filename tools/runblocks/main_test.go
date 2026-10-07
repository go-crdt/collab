package main

import (
	"strings"
	"testing"
)

func TestWhatItCatches(t *testing.T) {
	for _, tt := range []struct {
		name string
		yaml string
		want int
		says string
	}{
		{
			name: "a sound workflow",
			yaml: "jobs:\n  a:\n    steps:\n      - name: fine\n        run: |\n          echo hello\n          for i in 1 2; do echo $i; done\n",
			want: 0,
			says: "1 run blocks parse as shell",
		},
		{
			// The shape that broke the sibling repository: a loop split in
			// half. Valid YAML, and bash will not run it.
			name: "a loop with no done",
			yaml: "jobs:\n  a:\n    steps:\n      - name: broken\n        run: |\n          for i in 1 2; do\n            echo $i\n",
			want: 1,
			says: "does not parse as shell",
		},
		{
			// bash reports this as a WARNING and exits 0, so a check that reads
			// only the status calls it sound.
			name: "an unterminated heredoc",
			yaml: "jobs:\n  a:\n    steps:\n      - name: heredoc\n        run: |\n          cat <<'EOF'\n          one\n",
			want: 1,
			says: "does not parse as shell",
		},
		{
			name: "nothing to check is not a pass",
			yaml: "jobs:\n  a:\n    steps:\n      - uses: actions/checkout@v7\n",
			want: 1,
			says: "no run block found",
		},
		{
			name: "unreadable yaml",
			yaml: "jobs: [unclosed\n",
			want: 1,
			says: "cannot parse the workflow",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var out strings.Builder
			if got := check([]byte(tt.yaml), &out); got != tt.want {
				t.Errorf("exit %d, want %d (said %q)", got, tt.want, out.String())
			}
			if !strings.Contains(out.String(), tt.says) {
				t.Errorf("does not say %q; it said %q", tt.says, out.String())
			}
		})
	}
}

// A step with no run block is skipped rather than failed: most steps are `uses`.
func TestUsesStepsAreNotShell(t *testing.T) {
	y := "jobs:\n  a:\n    steps:\n      - uses: actions/checkout@v7\n      - name: real\n        run: echo ok\n"
	var out strings.Builder
	if got := check([]byte(y), &out); got != 0 {
		t.Fatalf("exit %d, want 0: %s", got, out.String())
	}
	if !strings.Contains(out.String(), "1 run blocks") {
		t.Errorf("counted something other than the one run block: %q", out.String())
	}
}
