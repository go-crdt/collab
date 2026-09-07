//go:build !js

package collab_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// Resuming means coming back as the site that wrote the work, under a policy
// that says a session speaks for itself.
//
// [ClientConfig.Resume] carries what a participant did while it was away, and
// [OwnSiteOnly] refuses operations a session did not make. Put together, a
// participant that resumes under a FRESH site is carrying somebody else's
// operations as far as the policy is concerned -- its own, from before -- and
// they are refused. The tab keeps showing them, because they are in its own
// replica; nowhere else has them. That is the shape worth a test: it looks like
// it worked.
func TestResumingUnderAFreshSiteLosesTheWorkToOwnSiteOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, c := range []struct {
		name     string
		policy   func(context.Context, string, crdt.SiteID, []crdt.PartOps) error
		resumeAs crdt.SiteID
		arrives  bool
	}{
		{"no policy, a fresh site", nil, 99, true},
		{"OwnSiteOnly, a fresh site", collab.OwnSiteOnly, 99, false},
		{"OwnSiteOnly, the site that wrote it", collab.OwnSiteOnly, 7, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			join := func(srv *collab.Server, site crdt.SiteID, resume []byte) (*collab.Client, error) {
				tr, sc := collab.Pipe()
				go func() { _ = srv.ServePipe(ctx, sc) }()
				return collab.Join(ctx, tr, collab.ClientConfig{Document: "d", Site: site, Resume: resume})
			}

			// Work written as site 7, somewhere else.
			was := collab.NewServer(collab.Config{Store: collab.NewMemoryStore()})
			t.Cleanup(func() { _ = was.Close(context.Background()) })
			mine, err := join(was, 7, nil)
			if err != nil {
				t.Fatal(err)
			}
			body, err := mine.Text("body")
			if err != nil {
				t.Fatal(err)
			}
			if err := body.Insert(0, "WORK"); err != nil {
				t.Fatal(err)
			}
			snapshot := mine.Snapshot()
			_ = mine.Close()

			// Brought to a server that already holds something of its own.
			now := collab.NewServer(collab.Config{Store: collab.NewMemoryStore(), AuthorizeOperations: c.policy})
			t.Cleanup(func() { _ = now.Close(context.Background()) })
			there, err := join(now, 1, nil)
			if err != nil {
				t.Fatal(err)
			}
			theirs, err := there.Text("body")
			if err != nil {
				t.Fatal(err)
			}
			if err := theirs.Insert(0, "THEIRS"); err != nil {
				t.Fatal(err)
			}

			if _, err := join(now, c.resumeAs, snapshot); err != nil {
				t.Fatalf("the resuming join was refused outright: %v", err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) && !strings.Contains(theirs.String(), "WORK") {
				time.Sleep(20 * time.Millisecond)
			}

			got := strings.Contains(theirs.String(), "WORK")
			if got != c.arrives {
				t.Errorf("the server holds %q; the resumed work arriving = %v, want %v",
					theirs.String(), got, c.arrives)
			}
			t.Logf("the server holds %q", theirs.String())
		})
	}
}
