package storetest

import (
	"context"
	"strings"
	"sync"
	"testing"
)

// recorder stands in for *testing.T so a case can be run against a store that
// is deliberately wrong and the result read rather than reported.
//
// Fatal and Fatalf panic, because that is what they do: a case that has said
// the store is wrong must not carry on as though it were not. runCase recovers.
type recorder struct {
	failed  bool
	skipped bool
	says    []string
}

type stopped struct{}

func (r *recorder) Helper() {}
func (r *recorder) Logf(format string, args ...any) {
	r.says = append(r.says, format)
}
func (r *recorder) Errorf(format string, args ...any) {
	r.failed = true
	r.says = append(r.says, format)
}
func (r *recorder) Fatal(args ...any) {
	r.failed = true
	panic(stopped{})
}
func (r *recorder) Fatalf(format string, args ...any) {
	r.failed = true
	r.says = append(r.says, format)
	panic(stopped{})
}
func (r *recorder) Skip(args ...any) {
	r.skipped = true
	panic(stopped{})
}

// runCase runs one case by name and says whether it failed.
func runCase(t *testing.T, name string, h Harness) (failed, skipped bool) {
	t.Helper()
	for _, c := range cases {
		if c.name != name {
			continue
		}
		r := &recorder{}
		func() {
			defer func() {
				if p := recover(); p != nil {
					if _, ours := p.(stopped); !ours {
						panic(p)
					}
				}
			}()
			c.run(r, h)
		}()
		return r.failed, r.skipped
	}
	t.Fatalf("no case named %q", name)
	return false, false
}

// A store that keeps the contract passes every case, and one that breaks a rule
// fails the case about that rule.
//
// Both halves matter. A suite that no store can pass is noise; a suite that
// every store passes is decoration. These are the cases that would have caught
// the three divergences this package was written for, put to stores built to
// have exactly those defects.
func TestTheSuiteFailsAStoreThatBreaksTheRule(t *testing.T) {
	for _, c := range []struct {
		rule  string
		store func() *broken
	}{
		{"LoadOfADocumentNobodyHasSavedIsNilAndNotAnError", func() *broken {
			// Answers zero bytes rather than nil for a document nobody saved.
			return &broken{emptyForUnknown: true}
		}},
		{"WhatWasSavedIsWhatComesBack", func() *broken {
			return &broken{loseATrailingByte: true}
		}},
		{"SavingAgainReplaces", func() *broken {
			return &broken{keepTheFirst: true}
		}},
		{"TwoNamesAreTwoDocuments", func() *broken {
			return &broken{foldCase: true}
		}},
		{"NamesAreEitherKeptApartOrRefused", func() *broken {
			// Refuses a name on save and answers it on load, which is the
			// inconsistency a store that encodes names badly falls into.
			return &broken{refuseColonsButAnswerThem: true}
		}},
	} {
		t.Run(c.rule, func(t *testing.T) {
			b := c.store()
			failed, skipped := runCase(t, c.rule, Harness{Store: b})
			if skipped {
				t.Fatal("the case skipped, so it asked nothing")
			}
			if !failed {
				t.Error("a store that breaks this rule passed the case for it")
			}
			// And the same case on a store that keeps the contract passes,
			// so the failure above is about the defect and not about the case
			// being impossible.
			if failed, skipped := runCase(t, c.rule, Harness{Store: &broken{}}); failed || skipped {
				t.Errorf("a correct store did not pass this case (failed=%v skipped=%v)", failed, skipped)
			}
		})
	}
}

// The two cases that reach the medium fail a store that hands back what changed.
//
// These are the ones that went vacuous once already, so they are the ones the
// self-test most needs: a store is given the hooks and made to answer a
// truncated document as a new one, and to serve bytes that were changed
// underneath it.
func TestTheSuiteFailsAStoreThatServesWhatChangedUnderneathIt(t *testing.T) {
	// A store whose medium can be reached, and which reports whatever is in it
	// without looking. Truncating gives an empty document; corrupting gives
	// bytes that still open, because the corruption is in the text rather than
	// in the header.
	medium := func() (*broken, Harness) {
		b := &broken{servesWhatIsThere: true}
		h := Harness{
			Store: b,
			Truncate: func(document string) error {
				b.mu.Lock()
				defer b.mu.Unlock()
				b.docs[document] = []byte{}
				return nil
			},
			Corrupt: func(document string, nth int) (bool, error) {
				b.mu.Lock()
				defer b.mu.Unlock()
				held := b.docs[document]
				if nth >= len(held) {
					return false, nil
				}
				held[nth] ^= 1
				return true, nil
			},
		}
		return b, h
	}

	_, h := medium()
	if failed, skipped := runCase(t, "AZeroLengthSnapshotIsRefusedAndNotCalledANewDocument", h); !failed || skipped {
		t.Errorf("a store that answers a truncated document passed (failed=%v skipped=%v)", failed, skipped)
	}
	_, h = medium()
	if failed, skipped := runCase(t, "AStoredSnapshotThatChangedIsRefused", h); !failed || skipped {
		t.Errorf("a store that serves what changed passed (failed=%v skipped=%v)", failed, skipped)
	}
}

// The two cases that need to reach the medium skip when they cannot, rather
// than passing. A skip that reads as a pass is how a suite quietly stops asking.
func TestTheCasesThatNeedTheMediumSkipRatherThanPass(t *testing.T) {
	for _, name := range []string{
		"AZeroLengthSnapshotIsRefusedAndNotCalledANewDocument",
		"AStoredSnapshotThatChangedIsRefused",
	} {
		failed, skipped := runCase(t, name, Harness{Store: &broken{}})
		if failed {
			t.Errorf("%s failed a store it could not reach", name)
		}
		if !skipped {
			t.Errorf("%s passed without a medium to ask about", name)
		}
	}
}

// broken is a store with whichever defect a case is being tested against, and
// none when they are all false.
type broken struct {
	mu   sync.Mutex
	docs map[string][]byte

	emptyForUnknown           bool
	servesWhatIsThere         bool
	loseATrailingByte         bool
	keepTheFirst              bool
	foldCase                  bool
	refuseColonsButAnswerThem bool
}

func (b *broken) key(document string) string {
	if b.foldCase {
		return strings.ToLower(document)
	}
	return document
}

func (b *broken) Load(_ context.Context, document string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	held, ok := b.docs[b.key(document)]
	if ok && b.servesWhatIsThere {
		// No refusal of any kind: whatever the medium holds is the answer,
		// zero bytes included.
		return held, nil
	}
	if !ok {
		if b.emptyForUnknown {
			return []byte{}, nil
		}
		return nil, nil
	}
	if b.loseATrailingByte && len(held) > 0 {
		return held[:len(held)-1], nil
	}
	return append([]byte(nil), held...), nil
}

func (b *broken) Save(_ context.Context, document string, snapshot []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.refuseColonsButAnswerThem && strings.Contains(document, ":") {
		// Refused, and yet kept: the shape a store falls into when its name
		// check and its writer disagree.
		if b.docs == nil {
			b.docs = map[string][]byte{}
		}
		b.docs[b.key(document)] = append([]byte(nil), snapshot...)
		return errStore
	}
	if b.docs == nil {
		b.docs = map[string][]byte{}
	}
	if _, held := b.docs[b.key(document)]; held && b.keepTheFirst {
		return nil
	}
	b.docs[b.key(document)] = append([]byte(nil), snapshot...)
	return nil
}

var errStore = errBroken("collab: this store will not keep that name")

type errBroken string

func (e errBroken) Error() string { return string(e) }
