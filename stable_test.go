//go:build !js

package collab_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// totalSeen adds up a version, so two of them can be compared as "how much of
// the history is in here".
func totalSeen(v crdt.CompositeVersion) uint64 {
	var n uint64
	for _, vv := range v {
		for _, seq := range vv {
			n += seq
		}
	}
	return n
}

// The measurement the collection work needs before anything is collected: in a
// room that is actually being used, does what everybody has certainly seen
// advance, or does it sit at nothing?
//
// It is asked here rather than assumed because the answer decides whether a
// policy for collecting is worth writing. A meet that never moves is a feature
// nobody can use.

// perAcknowledger reports what each participant has told the server it applied,
// totalled across every sender — the axis the meet is a minimum OVER.
//
// ⛔ This is the one perSite cannot give. perSite answers "how far has the meet
// got with site k's operations", which shows a single straggler as one "behind"
// line per sender. This answers "how much of the history has participant k
// applied", so a lagging participant is one short line and everybody else is
// complete. The two together account for the same stall from both ends.
func perAcknowledger(acks []collab.Acknowledgement, known bool, editors, rounds int) string {
	// ⛔ TWO WAYS THERE IS NO TABLE, AND THEY ARE DIFFERENT FINDINGS. "The
	// server does not know this document" is about the test's own premise — a
	// wrong name, or a room evicted under it — and "the room emptied" is about
	// the subject. An earlier version of this printed the second for both.
	if !known {
		return "\tthe server does not know this document, so the premise of this wait is gone\n"
	}
	if len(acks) == 0 {
		return "\tthe server reports no connected participant, which is itself the finding\n"
	}
	want := uint64(editors * rounds)
	var b strings.Builder
	b.WriteString("\tand by ACKNOWLEDGER, which is the axis the meet is a minimum over:\n")
	for _, a := range acks {
		if a.Have == nil {
			fmt.Fprintf(&b, "\tparticipant %d: has acknowledged NOTHING, which takes the meet to nothing\n", a.Site)
			continue
		}
		applied := totalSeen(a.Have)
		mark := ""
		if applied < want {
			mark = fmt.Sprintf("  <-- behind by %d", want-applied)
		}
		fmt.Fprintf(&b, "\tparticipant %d: applied %d of %d%s\n", a.Site, applied, want, mark)
	}
	return b.String()
}

// ⛔⛔ THE FAILURE MESSAGE IS CODE NOTHING RUNS. perAcknowledger exists only on
// a path this suite does not take when it passes, which is how a diagnostic
// rots: the next occurrence prints a panic, or nothing, instead of the answer.
// So it is asserted directly, against the SECOND occurrence's own numbers.
func TestTheAcknowledgerTableNamesTheStraggler(t *testing.T) {
	const editors, rounds = 8, 20
	part := crdt.Part{Kind: crdt.PartText, Name: "body"}
	ver := func(from map[int]uint64) crdt.CompositeVersion {
		vv := crdt.VersionVector{}
		for i := 1; i <= editors; i++ {
			n := uint64(rounds)
			if v, ok := from[i]; ok {
				n = v
			}
			vv[crdt.SiteID(i)] = n
		}
		return crdt.CompositeVersion{part: vv}
	}
	// ⛔ go-crdt/collab#204's second occurrence, reconstructed so the numbers
	// are ITS numbers. A first version of this fixture gave the straggler 18 of
	// its OWN twenty, which cannot happen — a site authors its operations, so
	// it holds all of them — and totalled 144 against the occurrence's 145. A
	// review caught the discrepancy, and a fixture off by one from the incident
	// it names is a fixture that proves something else.
	//
	// The published per-sender table was 18×6, 17, 20 = 145. That is this: one
	// participant short of everybody ELSE's last two (three for site 7), and
	// complete on its own.
	straggler := ver(map[int]uint64{1: 18, 2: 18, 3: 18, 4: 18, 5: 18, 6: 18, 7: 17})
	acks := make([]collab.Acknowledgement, 0, editors)
	for i := 1; i < editors; i++ {
		acks = append(acks, collab.Acknowledgement{Site: crdt.SiteID(i), Have: ver(nil)})
	}
	acks = append(acks, collab.Acknowledgement{Site: crdt.SiteID(editors), Have: straggler})

	out := perAcknowledger(acks, true, editors, rounds)
	if n := strings.Count(out, "<-- behind"); n != 1 {
		t.Errorf("ONE straggler must read as one line, %d did:\n%s", n, out)
	}
	if !strings.Contains(out, "participant 8: applied 145 of 160  <-- behind by 15") {
		t.Errorf("it must name the participant and the gap, with the occurrence's own total; it said:\n%s", out)
	}
	// ⛔ And the complete ones must be LISTED and unmarked. Without this, a
	// formatter that printed only the stragglers would pass every assertion
	// above — the reader would see one line and no idea whether the other seven
	// were fine or simply omitted.
	for i := 1; i < editors; i++ {
		want := fmt.Sprintf("participant %d: applied 160 of 160\n", i)
		if !strings.Contains(out, want) {
			t.Errorf("a complete participant must be listed unmarked as %q; it said:\n%s", want, out)
		}
	}

	// A participant that has said nothing is the case that takes the meet to
	// nothing, and it must not read as "applied 0" among the others.
	silent := perAcknowledger([]collab.Acknowledgement{{Site: 3}}, true, editors, rounds)
	if !strings.Contains(silent, "participant 3: has acknowledged NOTHING") {
		t.Errorf("silence is not zero; it said %q", silent)
	}
	// ⛔ AND THE TWO WAYS THERE IS NO TABLE ARE DIFFERENT ANSWERS. An open room
	// that emptied is a finding about this test; a document the server does not
	// know is a finding about the test's own premise, and reading the second as
	// the first would send the next reader after the wrong thing. A review
	// caught this message asserting the first for both.
	if empty := perAcknowledger(nil, true, editors, rounds); !strings.Contains(empty, "no connected participant") {
		t.Errorf("an emptied room must say so; it said %q", empty)
	}
	gone := perAcknowledger(nil, false, editors, rounds)
	if !strings.Contains(gone, "does not know this document") {
		t.Errorf("an unknown document must say THAT, not that the room is empty; it said %q", gone)
	}
	if strings.Contains(gone, "no connected participant") {
		t.Errorf("the two must not read alike; it said %q", gone)
	}
}

// perSite reports what each participant is acknowledged up to, and how far
// short of its own contribution that is.
//
// The total alone cannot say what stalled. This failed once on arch amd64 at
// 124 of 160 (go-crdt/collab#204): the arithmetic rules out one participant
// having stopped -- a single site stuck would leave at least 140 -- and cannot
// choose among the ways several could be a little behind. A breakdown can.
//
// ⚠ What it cannot do is name the ACKNOWLEDGER, and the second occurrence
// showed why that matters: see perAcknowledger above.
func perSite(v crdt.CompositeVersion, editors, rounds int) string {
	var b strings.Builder
	for i := 1; i <= editors; i++ {
		site := crdt.SiteID(i)
		var at uint64
		for _, vv := range v {
			if seq, held := vv[site]; held && seq > at {
				at = seq
			}
		}
		fmt.Fprintf(&b, "\tsite %d: acknowledged up to %d of %d%s\n",
			site, at, rounds, map[bool]string{true: "  <-- behind", false: ""}[at < uint64(rounds)])
	}
	return b.String()
}

func TestWhatEverybodyHasSeenAdvances(t *testing.T) {
	const editors = 8
	const rounds = 20

	srv, conn := serve(t, collab.Config{})
	clients := make([]*collab.Client, 0, editors)
	for i := 0; i < editors; i++ {
		clients = append(clients, join(t, conn, collab.ClientConfig{
			Document: "paper", Site: crdt.SiteID(i + 1),
		}))
	}

	// Nobody has acknowledged anything they have not been sent, so at the very
	// start the meet is either nothing or the empty version.
	if v, ok := srv.Stable("paper"); ok && totalSeen(v) != 0 {
		t.Fatalf("before anybody typed, %d operations were already called stable", totalSeen(v))
	}

	for round := 0; round < rounds; round++ {
		for i, c := range clients {
			if err := body(t, c).Insert(0, fmt.Sprintf("%d", i)); err != nil {
				t.Fatalf("participant %d: %v", i, err)
			}
		}
	}
	want := editors * rounds

	// Everyone converges first: the meet cannot pass what the slowest has.
	for _, c := range clients {
		await(t, c, "the whole text", func() bool {
			return len(body(t, c).String()) == want
		})
	}

	// And then what everybody has certainly seen has to reach the same place. It
	// arrives after convergence rather than with it: an acknowledgement
	// describes what its sender had applied when it was sent, so it is always a
	// round-trip behind the state it describes.
	start := time.Now()
	deadline := start.Add(20 * time.Second)
	var seen uint64
	// WHEN the meet last moved, and the last few places it moved to.
	//
	// The per-site breakdown below says who is behind. It cannot say whether
	// they were still catching up or had stopped, because it is a snapshot
	// taken at the deadline and a snapshot has no time in it. That is the open
	// question in go-crdt/collab#204 -- "something stops rather than slows" is
	// a claim about time, so the test has to keep some.
	//
	// Both occurrences so far were argued from the MARGIN between this test's
	// local duration and its deadline, about 650x, which is an argument that a
	// loaded runner cannot spend it. This measures the thing itself instead:
	// frozen at its final value for nearly the whole deadline is a stall,
	// reaching it moments before the deadline is slowness.
	moved := start
	var steps []string
	const keep = 8
	for {
		v, ok := srv.Stable("paper")
		if ok {
			if got := totalSeen(v); got != seen {
				steps = append(steps, fmt.Sprintf("%d at %v", got, time.Since(start).Round(time.Millisecond)))
				seen, moved = got, time.Now()
			}
			if seen >= uint64(want) {
				break
			}
		}
		if time.Now().After(deadline) {
			// WHICH sites are behind, not merely how many operations are
			// missing. This stalled once on arch amd64 at 124 of 160
			// (go-crdt/collab#204) and the number alone could not say whether
			// one participant had stopped acknowledging or several were a
			// little behind -- the arithmetic rules out the first (one site
			// stuck would leave at least 140) and cannot choose among the
			// rest. A per-site breakdown can.
			v, _ := srv.Stable("paper")
			// ⛔⛔ THE SENDER TABLE IS THE WRONG AXIS ON ITS OWN, which the
			// SECOND occurrence showed: 2026-10-09, run 37896495193, same
			// lane, 145 of 160, reading `site 1..6: 18 of 20 <-- behind`,
			// `site 7: 17` and `site 8: 20`. Seven "behind" lines for ONE
			// lagging acknowledger: Stable is a minimum over the
			// acknowledgers, so a participant two operations behind caps it
			// for every sender but itself. Site 8's own operations had
			// reached everybody; site 8 had not reached everybody else's
			// last two. The sender table stays, because it accounts for the
			// total; the acknowledger table goes beside it, because it is
			// the axis that names the culprit and only the server knows it.
			acks, known := srv.Acknowledged("paper")
			last := "and it never moved at all"
			if len(steps) > 0 {
				from := max(len(steps)-keep, 0)
				last = fmt.Sprintf("the meet moved %d time(s); the last of them: %s",
					len(steps), strings.Join(steps[from:], ", "))
			}
			t.Fatalf("what everybody has certainly seen stalled at %d of %d operations, and had not moved for %v\n%s%s%s",
				seen, want, time.Since(moved).Round(time.Millisecond),
				perSite(v, editors, rounds), perAcknowledger(acks, known, editors, rounds), last)
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Logf("%d participants, %d operations: everybody has certainly seen all of them", editors, want)
}

// A participant that says nothing holds the answer back, rather than being
// counted as having everything. That is the safe direction: a replica nobody has
// heard from is exactly the replica that might not have seen the operation.
func TestOneSilentParticipantHoldsTheAnswerBack(t *testing.T) {
	srv, conn := serve(t, collab.Config{})
	ada := join(t, conn, collab.ClientConfig{Document: "paper", Site: 1})
	if err := body(t, ada).Insert(0, "hello"); err != nil {
		t.Fatal(err)
	}
	await(t, ada, "her own text", func() bool {
		return body(t, ada).String() == "hello"
	})
	// Wait for her acknowledgement to land, so the meet is hers alone.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if v, ok := srv.Stable("paper"); ok && totalSeen(v) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("one participant alone never acknowledged anything")
		}
		time.Sleep(2 * time.Millisecond)
	}

	// A second participant arrives and is sent a snapshot. Until it says so,
	// the meet must not claim it has anything.
	grace := join(t, conn, collab.ClientConfig{Document: "paper", Site: 2})
	await(t, grace, "the text", func() bool {
		return body(t, grace).String() == "hello"
	})
	// It will acknowledge, and the meet comes back — the point being that it
	// went through zero rather than staying at Ada's version while a
	// participant that had said nothing was in the room.
	deadline = time.Now().Add(10 * time.Second)
	for {
		v, ok := srv.Stable("paper")
		if ok && totalSeen(v) >= 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the meet never recovered once the second participant acknowledged")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// A participant that only reads is heard from too, which is the whole point of
// acknowledging from the goroutine that receives.
//
// It is also what a room mostly is. One person writing and thirty watching is
// the ordinary shape of a document being reviewed, and if the thirty said
// nothing the answer would sit where the writer last was — which is to say, one
// message behind, for ever.
func TestAParticipantThatOnlyReadsIsHeardFrom(t *testing.T) {
	srv, conn := serve(t, collab.Config{})
	writer := join(t, conn, collab.ClientConfig{Document: "paper", Site: 1})
	readers := make([]*collab.Client, 0, 4)
	for i := 0; i < 4; i++ {
		readers = append(readers, join(t, conn, collab.ClientConfig{
			Document: "paper", Site: crdt.SiteID(i + 2),
		}))
	}

	const want = 12
	for i := 0; i < want; i++ {
		if err := body(t, writer).Insert(0, "x"); err != nil {
			t.Fatal(err)
		}
	}
	for i, r := range readers {
		await(t, r, "the text", func() bool { return len(body(t, r).String()) == want })
		_ = i
	}

	// Nobody but the writer has written anything, so if a reader could not
	// acknowledge, the meet would stop at whatever the writer last reported —
	// which is before it received its own last broadcast.
	deadline := time.Now().Add(20 * time.Second)
	var seen uint64
	for {
		if v, ok := srv.Stable("paper"); ok {
			seen = totalSeen(v)
			if seen >= want {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("with four readers and one writer, the answer stalled at %d of %d", seen, want)
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Logf("one writer and %d readers: all %d operations are certainly seen", len(readers), want)
}
