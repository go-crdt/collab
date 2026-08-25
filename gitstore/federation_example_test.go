// This file is a consumer of the package rather than part of it: it may only
// use what an operator can use, so anything it needs and cannot reach is a gap
// in the exported surface rather than something to reach around.
package gitstore_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/collab/gitstore"
	"github.com/go-crdt/crdt"
)

// institution is one site of a federation: a server, the documents it keeps,
// and the repository a person can read them out of.
type institution struct {
	name   string
	scope  string // what eduGAIN calls the part after the @
	server *collab.Server
	git    *gitstore.Store
	dir    string
}

// siteFor derives a replica identity from a federated identifier.
//
// The identifier must be scoped, and this is the whole of what GÉANT settles
// for this design: eduGAIN hands over an identifier that is globally unique
// because only the home organisation issues inside its own scope, so two
// institutions that have never spoken cannot mint the same one.
//
// A bare identifier would be the one failure this design cannot merge its way
// out of: DeriveSiteID is a hash, so "42" is the same site on every instance in
// the world, and two institutions each with a user "42" would silently share a
// replica.
func siteFor(eppn string) crdt.SiteID { return crdt.DeriveSiteID([]byte(eppn)) }

// federates says whether an institution accepts work carried on behalf of a
// site. Real deployments would ask their federation metadata; this one holds
// the scopes it has agreed with, which is the same question asked of a smaller
// register.
func federates(name string, with ...string) func(context.Context, string, crdt.SiteID, []crdt.PartOps) error {
	allowed := map[crdt.SiteID]bool{}
	for _, scope := range with {
		for _, who := range []string{"ada", "grace", "link"} {
			allowed[siteFor(who+"@"+scope)] = true
		}
	}
	return func(_ context.Context, _ string, _ crdt.SiteID, batches []crdt.PartOps) error {
		// Every operation in the batch, not the sender: a participant speaks
		// for itself, but a link speaks for an institution and a batch it
		// carries names whichever sites wrote the work.
		for _, batch := range batches {
			for _, site := range sitesIn(batch) {
				if !allowed[site] {
					return fmt.Errorf("site %d is not in a scope %s federates with", site, name)
				}
			}
		}
		return nil
	}
}

// sitesIn reports which replicas wrote the operations in a batch.
func sitesIn(batch crdt.PartOps) []crdt.SiteID {
	seen := map[crdt.SiteID]bool{}
	var out []crdt.SiteID
	for _, op := range batch.Text {
		if !seen[op.ID.Site] {
			seen[op.ID.Site] = true
			out = append(out, op.ID.Site)
		}
	}
	return out
}

// open builds one institution: a server whose store is both a database-shaped
// one and a git repository, so that the same save answers "what is it now" and
// "what did it say, and who wrote this sentence".
func open(t *testing.T, name, scope string, authorize func(context.Context, string, crdt.SiteID, []crdt.PartOps) error) *institution {
	t.Helper()
	dir := t.TempDir()
	stamp := time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC)
	repo, err := gitstore.New(dir,
		gitstore.WithAuthor(name, name+"@"+scope),
		gitstore.WithClock(func() time.Time { stamp = stamp.Add(time.Minute); return stamp }))
	if err != nil {
		t.Fatal(err)
	}
	server := collab.NewServer(collab.Config{
		Store:               collab.NewMultiStore(collab.NewMemoryStore(), repo),
		PersistEvery:        time.Millisecond,
		AuthorizeOperations: authorize,
	})
	// Closed here rather than left to the context: a server still writing into
	// a directory the framework is removing is a flake nobody should have to
	// diagnose twice.
	t.Cleanup(func() {
		stop, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = server.Close(stop)
	})
	return &institution{name: name, scope: scope, git: repo, dir: dir, server: server}
}

// editor joins a document on an institution's own server. Nothing leaves the
// process: collab.Pipe is a session with the carrier taken out, so an example
// needs no port and no loopback connection.
func editor(t *testing.T, ctx context.Context, in *institution, document string, site crdt.SiteID) *collab.Client {
	t.Helper()
	transport, conn := collab.Pipe()
	go func() { _ = in.server.ServePipe(ctx, conn) }()
	c, err := collab.Join(ctx, transport, collab.ClientConfig{Document: document, Site: site})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// link makes one institution a participant in another's copy of a document, and
// keeps it one: FollowWithRetry is the loop an operator would otherwise write,
// with the jitter and the ceiling they would otherwise leave out.
func link(ctx context.Context, from, to *institution, document string, as crdt.SiteID) {
	dial := func(context.Context) (collab.Transport, error) {
		transport, conn := collab.Pipe()
		go func() { _ = to.server.ServePipe(ctx, conn) }()
		return transport, nil
	}
	go func() {
		_ = from.server.FollowWithRetry(ctx, dial, document, as, collab.RetryPolicy{
			Wait:    50 * time.Millisecond,
			Ceiling: 2 * time.Second,
		})
	}()
}

// settle waits for something to become true, and says what it was waiting for
// if it never does.
func settle(t *testing.T, what string, want func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !want() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func textOf(t *testing.T, c *collab.Client) string {
	t.Helper()
	body, err := c.Text("file:paper.tex")
	if err != nil {
		t.Fatal(err)
	}
	return body.String()
}

// Two institutions, each holding the document its own people are editing, and
// each keeping a repository somebody can clone.
//
// It is the whole story in one place: an identity that two instances cannot
// both mint, a link that comes back on its own, a rule about what that link may
// carry, and a record that is readable without this library.
func TestTwoInstitutionsFederate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	paris := open(t, "paris", "paris.example.ac", nil)
	lyon := open(t, "lyon", "lyon.example.ac", nil)

	// Lyon follows Paris. The link is a participant of Paris like any other, so
	// what Lyon learns it tells Paris and the other way round.
	link(ctx, lyon, paris, "project:paper", siteFor("link@lyon.example.ac"))

	ada := editor(t, ctx, paris, "project:paper", siteFor("ada@paris.example.ac"))
	grace := editor(t, ctx, lyon, "project:paper", siteFor("grace@lyon.example.ac"))

	body, err := ada.Text("file:paper.tex")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, "On rivers."); err != nil {
		t.Fatal(err)
	}
	settle(t, "Ada's sentence to reach Lyon", func() bool { return textOf(t, grace) == "On rivers." })

	// And back the other way, which a one-directional relay would not give.
	graceBody, err := grace.Text("file:paper.tex")
	if err != nil {
		t.Fatal(err)
	}
	if err := graceBody.Insert(graceBody.Len(), " They run downhill."); err != nil {
		t.Fatal(err)
	}
	settle(t, "Grace's sentence to reach Paris", func() bool {
		return textOf(t, ada) == "On rivers. They run downhill."
	})

	// Both institutions hold the same document, not merely the same text: the
	// identities and the authorship are the same on both sides.
	settle(t, "the two replicas to hold the same state", func() bool {
		return string(ada.Snapshot()) == string(grace.Snapshot())
	})

	// Each of them can be read by a person, out of its own repository, with git
	// and nothing else.
	for _, in := range []*institution{paris, lyon} {
		settle(t, in.name+" to have written the document to its repository", func() bool {
			out, err := gitIn(in.dir, "show", "HEAD:project%3Apaper/paper.tex")
			return err == nil && strings.TrimSpace(out) == "On rivers. They run downhill."
		})
	}

	// A release names a state that already exists rather than making one.
	if err := paris.git.Release(ctx, "v1.0", "the first version we showed anybody"); err != nil {
		t.Fatal(err)
	}
	if out, err := gitIn(paris.dir, "tag", "-l"); err != nil || strings.TrimSpace(out) != "v1.0" {
		t.Fatalf("tags = %q, %v", out, err)
	}
	// And the release is readable at its name, which is what a release is for.
	kept, err := paris.git.At("project:paper", "v1.0")
	if err != nil {
		t.Fatal(err)
	}
	restored, err := crdt.LoadComposite(1, kept)
	if err != nil {
		t.Fatal(err)
	}
	released, err := restored.Text("file:paper.tex")
	if err != nil {
		t.Fatal(err)
	}
	if released.String() != "On rivers. They run downhill." {
		t.Fatalf("v1.0 reads %q", released.String())
	}
}

// What a link may carry is not what its institution may join.
//
// Config.Authorize runs once, when a participant joins, which is right for a
// participant because a participant speaks for itself. A link speaks for an
// institution, and after the join a batch it carries could name any site — so
// the question has to be asked of the operations rather than of the sender.
func TestALinkCarriesOnlyWhatItsInstitutionMayCarry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Paris federates with Lyon and with nobody else.
	paris := open(t, "paris", "paris.example.ac",
		federates("paris", "paris.example.ac", "lyon.example.ac"))
	elsewhere := open(t, "elsewhere", "elsewhere.example.ac", nil)

	link(ctx, elsewhere, paris, "project:paper", siteFor("link@lyon.example.ac"))

	ada := editor(t, ctx, paris, "project:paper", siteFor("ada@paris.example.ac"))
	stranger := editor(t, ctx, elsewhere, "project:paper", siteFor("ada@elsewhere.example.ac"))

	body, err := stranger.Text("file:paper.tex")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, "from a scope Paris does not federate with"); err != nil {
		t.Fatal(err)
	}

	// The link carries it, Paris refuses it, and refusing changes nothing:
	// Paris's document is what it was. Waiting for a thing not to happen needs
	// a bound, so this asks a participant that arrives afterwards, which can
	// only see what was actually applied.
	time.Sleep(300 * time.Millisecond)
	if got := textOf(t, ada); got != "" {
		t.Fatalf("Paris applied work from a scope it does not federate with: %q", got)
	}
	late := editor(t, ctx, paris, "project:paper", siteFor("grace@paris.example.ac"))
	if got := textOf(t, late); got != "" {
		t.Fatalf("a participant joining afterwards found %q", got)
	}

	// And the institution it does federate with is unaffected: the rule is
	// about scopes, not about links.
	parisBody, err := ada.Text("file:paper.tex")
	if err != nil {
		t.Fatal(err)
	}
	if err := parisBody.Insert(0, "On rivers."); err != nil {
		t.Fatal(err)
	}
	settle(t, "Paris's own work to be accepted", func() bool { return textOf(t, late) == "On rivers." })
}

// A bare identifier is the same replica everywhere, and that is the one thing
// this design cannot merge its way out of.
func TestABareIdentifierIsTheSameReplicaEverywhere(t *testing.T) {
	if siteFor("42") != siteFor("42") {
		t.Fatal("DeriveSiteID is not a function")
	}
	if siteFor("ada@paris.example.ac") == siteFor("ada@lyon.example.ac") {
		t.Fatal("two scopes derived the same replica")
	}
	// Which is why the rule is: derive from something scoped, never bare.
	if siteFor("ada") == siteFor("ada@paris.example.ac") {
		t.Fatal("a scope made no difference")
	}
}

func gitIn(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), errors.New(strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// Two institutions federating: what an operator writes.
//
// Everything here is deliberate. The identity comes from a scoped federated
// identifier, because a bare one is the same replica in every datacentre in the
// world. The store is two stores, so the same save answers both "what is it
// now" and "what did it say last Tuesday". The link is FollowWithRetry rather
// than Follow, because a link that drops stays dropped and the loop that brings
// it back is the one thing everybody writes and most people write without
// jitter. And AuthorizeOperations, rather than Authorize alone, because a
// participant speaks for itself while a link speaks for an institution.
//
// The carrier here is collab.Pipe, which keeps the example to one process. A
// real deployment dials the other institution — collab.GRPC over a connection,
// or a WebSocket — and changes nothing else: the dialler is the only part that
// knows there is a network.
func Example_federation() {
	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	// Deferred first so it runs last: the directories go once the servers have
	// stopped writing into them, not while.
	parisDir, lyonDir := tempDir(), tempDir()
	defer func() { _ = os.RemoveAll(parisDir); _ = os.RemoveAll(lyonDir) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// One institution. Its store is a database-shaped one beside a git
	// repository; MultiStore writes both and merges what both hold, so adding
	// the repository to a server that was already running backfills it.
	open := func(dir, name, scope string, rule func(context.Context, string, crdt.SiteID, []crdt.PartOps) error) (*collab.Server, *gitstore.Store) {
		repo, err := gitstore.New(dir, gitstore.WithAuthor(name, name+"@"+scope))
		must(err)
		return collab.NewServer(collab.Config{
			Store:               collab.NewMultiStore(collab.NewMemoryStore(), repo),
			PersistEvery:        time.Millisecond,
			AuthorizeOperations: rule,
		}), repo
	}

	paris, parisGit := open(parisDir, "paris", "paris.example.ac",
		federates("paris", "paris.example.ac", "lyon.example.ac"))
	lyon, _ := open(lyonDir, "lyon", "lyon.example.ac", nil)
	// A server is closed, not merely cancelled: it has documents to write out,
	// and a directory removed while it is still writing is a race an operator
	// would inherit from this example.
	defer func() {
		stop, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = paris.Close(stop)
		_ = lyon.Close(stop)
	}()

	// Lyon follows Paris, and keeps following: the policy for coming back
	// belongs to the operator, and this is one written down.
	dial := func(context.Context) (collab.Transport, error) {
		transport, conn := collab.Pipe()
		go func() { _ = paris.ServePipe(ctx, conn) }()
		return transport, nil
	}
	go func() {
		_ = lyon.FollowWithRetry(ctx, dial, "project:paper",
			crdt.DeriveSiteID([]byte("link@lyon.example.ac")),
			collab.RetryPolicy{Wait: 50 * time.Millisecond, Ceiling: 2 * time.Second})
	}()

	// A person at each institution, editing the document their own server holds.
	join := func(s *collab.Server, eppn string) *collab.Client {
		transport, conn := collab.Pipe()
		go func() { _ = s.ServePipe(ctx, conn) }()
		c, err := collab.Join(ctx, transport, collab.ClientConfig{
			Document: "project:paper",
			Site:     crdt.DeriveSiteID([]byte(eppn)),
		})
		must(err)
		return c
	}
	ada, grace := join(paris, "ada@paris.example.ac"), join(lyon, "grace@lyon.example.ac")
	defer func() { _ = ada.Close(); _ = grace.Close() }()

	adaBody, err := ada.Text("file:paper.tex")
	must(err)
	must(adaBody.Insert(0, "On rivers."))

	graceBody, err := grace.Text("file:paper.tex")
	must(err)
	waitUntil(func() bool { return graceBody.String() == "On rivers." })
	must(graceBody.Insert(graceBody.Len(), " They run downhill."))
	waitUntil(func() bool { return adaBody.String() == "On rivers. They run downhill." })

	fmt.Println("paris:", adaBody.String())
	fmt.Println("lyon: ", graceBody.String())

	// The repository holds the text a person reads and the state a document is
	// restored from, in the same commit, so the two cannot drift.
	waitUntil(func() bool {
		out, err := gitIn(parisDir, "show", "HEAD:project%3Apaper/paper.tex")
		return err == nil && strings.TrimSpace(out) == "On rivers. They run downhill."
	})
	out, err := gitIn(parisDir, "show", "--stat", "--format=", "HEAD")
	must(err)
	fmt.Println("committed:", strings.Join(filesIn(out), " and "))

	// A release names a commit that already exists.
	must(parisGit.Release(ctx, "v1.0", "the first version we showed anybody"))
	kept, err := parisGit.At("project:paper", "v1.0")
	must(err)
	restored, err := crdt.LoadComposite(1, kept)
	must(err)
	released, err := restored.Text("file:paper.tex")
	must(err)
	fmt.Println("v1.0:  ", released.String())

	// Output:
	// paris: On rivers. They run downhill.
	// lyon:  On rivers. They run downhill.
	// committed: paper.tex and state.crdt
	// v1.0:   On rivers. They run downhill.
}

// tempDir is what an example has instead of t.TempDir. Its caller removes it:
// an example somebody copies should not teach them to leave directories behind.
func tempDir() string {
	dir, err := os.MkdirTemp("", "gitstore-example")
	if err != nil {
		panic(err)
	}
	return dir
}

// waitUntil is what an example has instead of a test's deadline helper. A
// program would wait on Client.Changes rather than poll.
func waitUntil(want func() bool) {
	deadline := time.Now().Add(15 * time.Second)
	for !want() {
		if time.Now().After(deadline) {
			panic("the two replicas did not converge")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// filesIn reads the paths out of a git --stat, so the example can say what a
// commit held without printing a hash that changes every run.
func filesIn(stat string) []string {
	var out []string
	for _, line := range strings.Split(stat, "\n") {
		name, _, found := strings.Cut(strings.TrimSpace(line), "|")
		if !found {
			continue
		}
		out = append(out, strings.TrimSpace(name[strings.LastIndex(name, "/")+1:]))
	}
	return out
}
