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

awk -v pat="$pattern" -v report="$report" '
  NR == 1 { next }                     # the "mode:" header
  pat != "" && $1 !~ pat { next }
  {
    total += $2
    if ($3 + 0 == 0 && $2 + 0 > 0) {
      uncovered += $2
      if (!report) print "uncovered (" $2 " statements): " $1
    }
  }
  END {
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
' "$profile"
