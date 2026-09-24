//go:build !js

package collab

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-crdt/crdt"
)

// What a policy costs on the path every batch takes.
//
// [Config.AuthorizeOperations] is asked about every batch a session sends, so its
// cost is paid per message rather than per document or per join, and there was no
// benchmark for it.
//
// It exists because go-crdt/collab#182 proposes replacing the walk inside
// [OwnSiteOnly] with an exported enumerator so the same rule stops living in
// three places, and a proposal like that needs a number. Measured on an Apple
// M4 Max, medians of five runs, against both candidate signatures:
//
//	                       1 op      64 ops     20 000 ops   allocated
//	inlined, as shipped    5.8 ns     97.5 ns      32.6 µs      0 B
//	append into a slice   16.9 ns    273 ns       105 µs      8 B / 960 B / 660 KB
//	a callback             8.1 ns    146 ns        48.7 µs      0 B
//
// So the append shape is out: three times the time, and 660 KB of garbage on a
// batch a rejoining participant sends. The callback costs about half again as
// much and allocates nothing -- its closure does not escape -- which on a
// keystroke is 2.3 ns and on a twenty-thousand-operation resend is 16 µs, next
// to a merge that takes hundreds. Affordable, and the decision is whose API
// surface it is rather than whose machine.
//
// The shapes are the ones that happen. One operation is somebody typing, and it
// is the only column where the per-call overhead shows: at sixty-four the walk is
// already down to 1.5 ns an operation.
func BenchmarkOwnSiteOnly(b *testing.B) {
	for _, n := range []int{1, 64, 20000} {
		batches := benchBatches(b, 7, n)
		b.Run(fmt.Sprint(n, " operations"), func(b *testing.B) {
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := OwnSiteOnly(ctx, "d", 7, batches); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/op/operation")
		})
	}
}

// benchBatches is one text batch of n operations, all from one site, which is
// what a session sends.
func benchBatches(b *testing.B, site crdt.SiteID, n int) []crdt.PartOps {
	b.Helper()
	c := crdt.NewComposite(site)
	text, err := c.Text("body")
	if err != nil {
		b.Fatal(err)
	}
	runes := make([]rune, n)
	for i := range runes {
		runes[i] = rune('a' + i%26)
	}
	if _, err := text.Insert(0, string(runes)); err != nil {
		b.Fatal(err)
	}
	return c.OpsSince(nil)
}
