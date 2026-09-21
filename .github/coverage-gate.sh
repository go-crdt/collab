#!/usr/bin/env bash
# A 100% coverage gate that means 100%.
#
# The gate this replaces read the total `go tool cover -func` prints, which is
# rounded to one decimal: one uncovered statement in a package of two thousand
# is 99.95%, prints as "100.0%", and passes a gate named for 100%. It did, for
# the clamp in dircompress.go's expansionLimit.
#
# So this counts statements in the profile instead, where the numbers are exact.
# Each line is
#     file.go:fromLine.col,toLine.col numberOfStatements timesExecuted
# and a block executed zero times is the thing a 100% gate exists to refuse.
#
# Usage: coverage-gate.sh [--report] <profile> [awk-regexp over the file:line field]
#
# The regexp is how a split gate names the part it holds to 100% (see the
# gitstore job). Matching nothing is a FAILURE in both modes, not an empty pass,
# so a rename cannot quietly empty the gate -- which is the property the gate
# this replaces was careful about and worth keeping.
#
# --report prints the count and does not fail on uncovered statements: for the
# part of a split gate that is measured and not held, where the number is news
# and not a verdict.
set -euo pipefail

report=0
if [ "${1:-}" = "--report" ]; then
  report=1
  shift
fi
profile=${1:?usage: coverage-gate.sh [--report] <profile> [pattern]}
pattern=${2:-}

# Two passes over the profile, because it may carry the SAME block more than
# once. `go test -coverpkg=X ./...` writes one section per test binary, each
# listing every block of X with that binary's counts -- so a block the root
# package's tests cover and another package's tests do not appears twice, once
# with a count and once with a zero. `go tool cover` merges those by block; a
# script that reads each line on its own does not, and reports thousands of
# uncovered statements that are covered. Measured: 4646 of them, on a profile the
# rounded gate called 100.0%.
#
# So the first pass sums the counts per block and the second prints the ones that
# are still zero, in file order.
awk -v pat="$pattern" -v report="$report" '
  FNR == 1 { next }                    # the "mode:" header, in each pass
  pat != "" && $1 !~ pat { next }
  NR == FNR {
    if (!($1 in stmts)) { stmts[$1] = $2; order[++n] = $1 }
    hits[$1] += $3
    next
  }
  # Second pass: nothing to do per line, the work is in END.
  { }
  END {
    for (i = 1; i <= n; i++) {
      key = order[i]
      total += stmts[key]
      if (hits[key] + 0 == 0 && stmts[key] + 0 > 0) {
        uncovered += stmts[key]
        if (!report) print "uncovered (" stmts[key] " statements): " key
      }
    }
    if (total == 0) {
      print "::error::the coverage gate matched no statements" \
            (pat == "" ? "" : " for /" pat "/") ", which is not a pass"
      exit 1
    }
    printf "%d of %d statements covered\n", total - uncovered, total
    if (report) exit 0
    if (uncovered > 0) {
      print "::error::" uncovered " statement(s) uncovered; the gate is 100%"
      exit 1
    }
  }
' "$profile" "$profile"
