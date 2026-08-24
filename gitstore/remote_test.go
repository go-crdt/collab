package gitstore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-crdt/crdt"
	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
)

// Nothing here reaches a network, and everything here reaches a real remote.
// go-git speaks to a bare repository on this machine over the same transport
// machinery it speaks to a host over, so a push that has arrived has been sent
// and received rather than mocked into existence.

// bare is the repository two instances share.
func bare(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if _, err := git.PlainInit(dir, true); err != nil {
		t.Fatal(err)
	}
	return dir
}

// instance is one server's store, pointed at a shared repository.
func instance(t *testing.T, name, url string, opts ...Option) *Store {
	t.Helper()
	s, err := New(t.TempDir(), append([]Option{
		WithAuthor(name, name+"@example"),
		WithClock(stamps()),
		WithRemote(Remote{URL: url}),
	}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// textOf is what a document reads as, loaded back from what a store holds.
func textOf(t *testing.T, s *Store, document string) string {
	t.Helper()
	raw, err := s.Load(t.Context(), document)
	if err != nil {
		t.Fatal(err)
	}
	if raw == nil {
		t.Fatalf("%s holds no %q", s.author.Name, document)
	}
	doc, err := crdt.LoadComposite(99, raw)
	if err != nil {
		t.Fatal(err)
	}
	text, _ := doc.Text("file:paper.tex")
	return text.String()
}

// Two servers, one repository, and neither of them ever speaking to the other.
//
// This is the whole of what a remote is for: the repository is the channel, the
// latency is however often somebody pulls, and what the two of them disagree
// about is the state file — which is the conflict this design never needs a
// person for.
func TestTwoInstancesFederateThroughARepository(t *testing.T) {
	ctx := t.Context()
	url := bare(t)
	paris := instance(t, "paris", url)
	lyon := instance(t, "lyon", url)

	// Paris writes the opening. Saving pushed it, so it is in the shared
	// repository before anybody asks for it.
	opening := crdt.NewComposite(crdt.DeriveSiteID([]byte("ada@paris.ac.example")))
	body, err := opening.Text("file:paper.tex")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, "On rivers."); err != nil {
		t.Fatal(err)
	}
	if err := paris.Save(ctx, "project:paper", opening.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if got := runIn(t, url, "show", "main:project%3Apaper/paper.tex"); got != "On rivers." {
		t.Fatalf("the shared repository holds %q", got)
	}

	// Lyon has never seen the document, and a pull is how it gets one.
	if err := lyon.Pull(ctx); err != nil {
		t.Fatal(err)
	}
	if got := textOf(t, lyon, "project:paper"); got != "On rivers." {
		t.Fatalf("lyon pulled %q", got)
	}

	// Now both of them work, having heard nothing from each other.
	lyonRaw, err := lyon.Load(ctx, "project:paper")
	if err != nil {
		t.Fatal(err)
	}
	lyonDoc, err := crdt.LoadComposite(crdt.DeriveSiteID([]byte("grace@lyon.ac.example")), lyonRaw)
	if err != nil {
		t.Fatal(err)
	}
	lyonBody, _ := lyonDoc.Text("file:paper.tex")
	if _, err := lyonBody.Insert(0, "PS. "); err != nil {
		t.Fatal(err)
	}
	if err := lyon.Save(ctx, "project:paper", lyonDoc.Snapshot()); err != nil {
		t.Fatal(err)
	}

	if _, err := body.Insert(body.Len(), " They run downhill."); err != nil {
		t.Fatal(err)
	}
	// Paris's push cannot arrive: lyon has committed since, so this is not a
	// fast-forward. The save succeeds anyway, and the commit is here.
	var refused []error
	paris.remote.PushFailed = func(err error) { refused = append(refused, err) }
	if err := paris.Save(ctx, "project:paper", opening.Snapshot()); err != nil {
		t.Fatalf("a save failed because a push did not arrive: %v", err)
	}
	if len(refused) != 1 {
		t.Fatalf("the refused push was reported %d times", len(refused))
	}
	if got := textOf(t, paris, "project:paper"); got != "On rivers. They run downhill." {
		t.Fatalf("paris holds %q after a push that did not arrive", got)
	}

	// Which is what a pull is for. It merges, and the merge holds both.
	before := len(gitLog(t, paris))
	if err := paris.Pull(ctx); err != nil {
		t.Fatal(err)
	}
	if got := textOf(t, paris, "project:paper"); got != "PS. On rivers. They run downhill." {
		t.Fatalf("the merge reads %q", got)
	}
	if after := len(gitLog(t, paris)); after <= before {
		t.Fatalf("the merge made no commit: %d before, %d after", before, after)
	}
	// And it is a merge: two parents, which is what puts lyon's history in
	// this branch and what makes the push below a fast-forward.
	parents := strings.Fields(run(t, paris, "log", "-1", "--format=%P"))
	if len(parents) != 2 {
		t.Fatalf("the merge commit has %d parents: %v", len(parents), parents)
	}
	if err := paris.Push(ctx); err != nil {
		t.Fatal(err)
	}

	// Lyon pulls it back and the two of them read the same document.
	if err := lyon.Pull(ctx); err != nil {
		t.Fatal(err)
	}
	if got := textOf(t, lyon, "project:paper"); got != "PS. On rivers. They run downhill." {
		t.Fatalf("lyon reads %q", got)
	}
	if got := runIn(t, url, "show", "main:project%3Apaper/paper.tex"); got != "PS. On rivers. They run downhill." {
		t.Fatalf("the shared repository reads %q", got)
	}
}

// A store with no remote is the store as it was before there was one.
func TestAStoreWithNoRemoteIsLocalOnly(t *testing.T) {
	ctx := t.Context()
	s := store(t)
	if err := s.Save(ctx, "d", paper(t, 1, "one").Snapshot()); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, "v1", "the first"); err != nil {
		t.Fatal(err)
	}
	if err := s.Push(ctx); !errors.Is(err, ErrNoRemote) {
		t.Fatalf("Push with no remote = %v", err)
	}
	if err := s.Pull(ctx); !errors.Is(err, ErrNoRemote) {
		t.Fatalf("Pull with no remote = %v", err)
	}
	if got := textOf(t, s, "d"); got != "one" {
		t.Fatalf("the document reads %q", got)
	}
}

// A push that does not arrive does not fail the save, does not undo the commit,
// and is tried again by the next save even though that save changes nothing.
func TestAPushThatDoesNotArrive(t *testing.T) {
	ctx := t.Context()
	// A directory that is not a repository, which is what an address that is
	// wrong or a host that is down comes to here.
	nowhere := t.TempDir()
	var heard []error
	s := instance(t, "alone", nowhere, WithRemote(Remote{
		URL:        nowhere,
		PushFailed: func(err error) { heard = append(heard, err) },
	}))

	snapshot := paper(t, 1, "kept anyway").Snapshot()
	if err := s.Save(ctx, "d", snapshot); err != nil {
		t.Fatalf("a save failed because its push did not arrive: %v", err)
	}
	if len(heard) != 1 {
		t.Fatalf("the failure was reported %d times", len(heard))
	}
	if !strings.Contains(heard[0].Error(), "gitstore:") {
		t.Errorf("the failure does not say where it came from: %v", heard[0])
	}
	if got := len(gitLog(t, s)); got != 1 {
		t.Fatalf("the commit was not kept: %d commits", got)
	}
	if got := textOf(t, s, "d"); got != "kept anyway" {
		t.Fatalf("the document reads %q", got)
	}

	// The same snapshot again: no commit, and the push is tried again — that
	// save IS the retry, which is the only reason a failed push needs nothing
	// queued anywhere.
	if err := s.Save(ctx, "d", snapshot); err != nil {
		t.Fatal(err)
	}
	if got := len(gitLog(t, s)); got != 1 {
		t.Fatalf("an unchanged save committed: %d commits", got)
	}
	if len(heard) != 2 {
		t.Fatalf("a save that changed nothing did not try the push again: %d reports", len(heard))
	}

	// And a release says nothing about it either, for the same reason.
	if err := s.Release(ctx, "v1", "released into the void"); err != nil {
		t.Fatalf("a release failed because its push did not arrive: %v", err)
	}
	if len(heard) != 3 {
		t.Fatalf("the release did not try to push: %d reports", len(heard))
	}

	// Nobody listening is a choice too, and it is not a crash.
	quiet := instance(t, "quiet", nowhere)
	if err := quiet.Save(ctx, "d", snapshot); err != nil {
		t.Fatal(err)
	}
	// Asked outright, it says so.
	if err := quiet.Push(ctx); err == nil {
		t.Fatal("a push to somewhere that is not a repository succeeded")
	}
}

// The operator says what to authenticate as, and this package does not guess.
func TestTheOperatorSaysWhatToAuthenticateAs(t *testing.T) {
	ctx := t.Context()
	url := bare(t)

	asked := 0
	s := instance(t, "asked", url, WithRemote(Remote{
		URL: url,
		Auth: func(context.Context) (transport.AuthMethod, error) {
			asked++
			return &http.BasicAuth{Username: "x", Password: "y"}, nil
		},
	}))
	snapshot := paper(t, 1, "one").Snapshot()
	if err := s.Save(ctx, "d", snapshot); err != nil {
		t.Fatal(err)
	}
	if err := s.Pull(ctx); err != nil {
		t.Fatal(err)
	}
	// Once for the push the save made and once for the fetch. Asked every
	// time, because a credential expires and a store that captured one at
	// startup would federate until it quietly did not.
	if asked != 2 {
		t.Fatalf("the credentials were asked for %d times", asked)
	}

	// And a save with nothing owed says nothing to anybody: the last push
	// arrived, so this one has nothing to carry and does not go asking.
	if err := s.Save(ctx, "d", snapshot); err != nil {
		t.Fatal(err)
	}
	if asked != 2 {
		t.Fatalf("a save with nothing to send opened a connection anyway: %d", asked)
	}

	// And when there are none to be had, both operations say so rather than
	// trying the remote as nobody in particular.
	refused := errors.New("the vault is sealed")
	locked := instance(t, "locked", url, WithRemote(Remote{
		URL:  url,
		Auth: func(context.Context) (transport.AuthMethod, error) { return nil, refused },
	}))
	if err := locked.Push(ctx); !errors.Is(err, refused) {
		t.Fatalf("Push with no credentials = %v", err)
	}
	if err := locked.Pull(ctx); !errors.Is(err, refused) {
		t.Fatalf("Pull with no credentials = %v", err)
	}
}

// A release travels, so a version somebody decided on is one the other
// instances can name.
func TestAReleaseTravels(t *testing.T) {
	ctx := t.Context()
	url := bare(t)
	here, there := instance(t, "here", url), instance(t, "there", url)

	if err := here.Save(ctx, "d", paper(t, 1, "the first draft").Snapshot()); err != nil {
		t.Fatal(err)
	}
	if err := here.Release(ctx, "v1", "the first"); err != nil {
		t.Fatal(err)
	}
	if err := there.Pull(ctx); err != nil {
		t.Fatal(err)
	}
	raw, err := there.At("d", "v1")
	if err != nil {
		t.Fatalf("the release did not travel: %v", err)
	}
	doc, err := crdt.LoadComposite(2, raw)
	if err != nil {
		t.Fatal(err)
	}
	text, _ := doc.Text("file:paper.tex")
	if text.String() != "the first draft" {
		t.Fatalf("v1 reads %q on the other instance", text.String())
	}
}

// A pull that finds nothing new commits nothing, and a remote nobody has
// pushed to yet is not a failure — it is what the first instance to start sees.
func TestAPullWithNothingToPull(t *testing.T) {
	ctx := t.Context()
	url := bare(t)
	first := instance(t, "first", url)
	if err := first.Pull(ctx); err != nil {
		t.Fatalf("pulling from an empty remote: %v", err)
	}
	if _, err := first.repo.head(); err == nil {
		t.Fatal("pulling from an empty remote made a commit")
	}

	if err := first.Save(ctx, "d", paper(t, 1, "one").Snapshot()); err != nil {
		t.Fatal(err)
	}
	// Its own commit, come back around: already held, so nothing to merge.
	before := len(gitLog(t, first))
	if err := first.Pull(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.Pull(ctx); err != nil {
		t.Fatal(err)
	}
	if after := len(gitLog(t, first)); after != before {
		t.Fatalf("pulling what this instance wrote made %d commits", after-before)
	}
}

// A merge takes nothing away. What this store writes is written again from the
// merged state, but a repository is also where somebody puts a README, and a
// pull is not allowed to be what removes one.
func TestAPullDoesNotDeleteWhatTheOtherSideAdded(t *testing.T) {
	ctx := t.Context()
	url := bare(t)
	here, there := instance(t, "here", url), instance(t, "there", url)

	if err := here.Save(ctx, "d", paper(t, 1, "one").Snapshot()); err != nil {
		t.Fatal(err)
	}
	if err := here.write("README", []byte("what this repository is")); err != nil {
		t.Fatal(err)
	}
	if err := here.repo.add("README"); err != nil {
		t.Fatal(err)
	}
	if err := here.Save(ctx, "d", paper(t, 1, "one and a half").Snapshot()); err != nil {
		t.Fatal(err)
	}

	// The other instance has its own document and has never seen any of that,
	// so the pull is a real merge and not a fast-forward.
	if err := there.Save(ctx, "other", paper(t, 2, "elsewhere").Snapshot()); err != nil {
		t.Fatal(err)
	}
	if err := there.Pull(ctx); err != nil {
		t.Fatal(err)
	}
	files := gitShow(t, there, "HEAD")
	if files["README"] != "what this repository is" {
		t.Fatalf("the merge lost the README: %v", keys(files))
	}
	// And both documents are in it.
	if got := textOf(t, there, "d"); got != "one and a half" {
		t.Fatalf("the pulled document reads %q", got)
	}
	if got := textOf(t, there, "other"); got != "elsewhere" {
		t.Fatalf("its own document reads %q", got)
	}
}

// A directory the other side holds a state file in, named something this store
// would never have written, is not a document and is not read as one.
func TestADirectoryTheOtherSideCallsSomethingElse(t *testing.T) {
	ctx := t.Context()
	url := bare(t)
	here, there := instance(t, "here", url), instance(t, "there", url)

	if err := here.write("not!a!document/"+stateFile, []byte("whatever this is")); err != nil {
		t.Fatal(err)
	}
	if err := here.repo.add("not!a!document/" + stateFile); err != nil {
		t.Fatal(err)
	}
	if err := here.Save(ctx, "d", paper(t, 1, "one").Snapshot()); err != nil {
		t.Fatal(err)
	}

	if err := there.Save(ctx, "other", paper(t, 2, "elsewhere").Snapshot()); err != nil {
		t.Fatal(err)
	}
	if err := there.Pull(ctx); err != nil {
		t.Fatalf("a directory this store did not write stopped the pull: %v", err)
	}
	docs, err := there.Documents()
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 2 || docs[0] != "d" || docs[1] != "other" {
		t.Fatalf("Documents = %v", docs)
	}
}

// What the other side sends has to be a snapshot. It came out of a repository
// anybody with push rights can write to, so it is not trusted for being there.
func TestPullingSomethingThatIsNotASnapshot(t *testing.T) {
	ctx := t.Context()
	url := bare(t)
	here, there := instance(t, "here", url), instance(t, "there", url)

	// Both instances hold the document, and then one of them puts something
	// in the state file that is not a state.
	if err := here.Save(ctx, "d", paper(t, 1, "one").Snapshot()); err != nil {
		t.Fatal(err)
	}
	if err := there.Pull(ctx); err != nil {
		t.Fatal(err)
	}
	dir, err := dirFor("d")
	if err != nil {
		t.Fatal(err)
	}
	if err := here.write(dir+"/"+stateFile, []byte("not a snapshot")); err != nil {
		t.Fatal(err)
	}
	if err := here.repo.add(dir + "/" + stateFile); err != nil {
		t.Fatal(err)
	}
	who := here.author
	who.When = here.now()
	if err := here.repo.commit("d: nonsense", who); err != nil {
		t.Fatal(err)
	}
	if err := here.Push(ctx); err != nil {
		t.Fatal(err)
	}

	// The other instance has moved too, so this is a merge and not a
	// fast-forward, and the merge is where the nonsense is noticed.
	if err := there.Save(ctx, "d", paper(t, 2, "two").Snapshot()); err != nil {
		t.Fatal(err)
	}
	if err := there.Pull(ctx); err == nil {
		t.Fatal("a pull merged bytes that are not a snapshot")
	}
	if got := textOf(t, there, "d"); got != "two" {
		t.Fatalf("the document reads %q after a pull that failed", got)
	}
}

// Everything the far end can refuse, said through the seam rather than by
// unplugging anything. See failing_test.go for why.
func TestEveryWayTheRemoteCanRefuse(t *testing.T) {
	ctx := t.Context()

	// Each case gets two instances that have both committed and diverged, so
	// a pull has something to fetch, something to merge and something to
	// commit — and every step of it can be made to fail on its own.
	setUp := func(t *testing.T, at breaks) *Store {
		t.Helper()
		url := bare(t)
		here, there := instance(t, "here", url), instance(t, "there", url)
		if err := here.Save(ctx, "d", paper(t, 1, "one").Snapshot()); err != nil {
			t.Fatal(err)
		}
		if err := there.Save(ctx, "other", paper(t, 2, "elsewhere").Snapshot()); err != nil {
			t.Fatal(err)
		}
		there.repo = &broken{repository: there.repo, at: at}
		return there
	}

	cases := []struct {
		at   breaks
		what string
		do   func(s *Store) error
	}{
		{atPush, "pushing", func(s *Store) error { return s.Push(ctx) }},
		{atFetch, "fetching", func(s *Store) error { return s.Pull(ctx) }},
		{atContains, "asking what this instance holds", func(s *Store) error { return s.Pull(ctx) }},
		{atAdopt, "taking what only they have", func(s *Store) error { return s.Pull(ctx) }},
		{atDocsAt, "listing their documents", func(s *Store) error { return s.Pull(ctx) }},
		{atFileAt, "reading their state", func(s *Store) error { return s.Pull(ctx) }},
		{atOpen, "reading our own state", func(s *Store) error { return s.Pull(ctx) }},
		{atCreate, "writing the merged state", func(s *Store) error { return s.Pull(ctx) }},
		{atMerge, "committing the merge", func(s *Store) error { return s.Pull(ctx) }},
	}
	for _, c := range cases {
		t.Run(string(c.at), func(t *testing.T) {
			s := setUp(t, c.at)
			err := c.do(s)
			if err == nil {
				t.Fatalf("%s did not fail", c.what)
			}
			if !errors.Is(err, errBroken) {
				t.Fatalf("%s failed as %v, which does not carry what went wrong", c.what, err)
			}
			if !strings.Contains(err.Error(), "gitstore:") {
				t.Errorf("the error does not say where it came from: %v", err)
			}
			// Whatever failed, this instance still holds its own document.
			s.repo = s.repo.(*broken).repository
			if got := textOf(t, s, "other"); got != "elsewhere" {
				t.Fatalf("this instance's own document reads %q after %s failed", got, c.what)
			}
		})
	}
}

// Two things an operator can arrange that are not this store's fault, and that
// it has to say plainly rather than fail somewhere further in.
func TestWhatTheRepositoryItselfCanBeInNoStateFor(t *testing.T) {
	ctx := t.Context()
	url := bare(t)
	here := instance(t, "here", url)
	if err := here.Save(ctx, "d", paper(t, 1, "one").Snapshot()); err != nil {
		t.Fatal(err)
	}

	// The shared repository calls its branch something else — master, here, which
	// is what a repository older than this package is on. Which branch is
	// pushed and fetched is the one this repository is on, so there is nothing
	// to line up with and saying so is the whole of what can be done.
	runIn(t, url, "branch", "-m", "main", "master")
	newcomer := instance(t, "newcomer", url)
	err := newcomer.Pull(ctx)
	if err == nil {
		t.Fatal("a pull found a branch the remote does not have")
	}
	if !strings.Contains(err.Error(), "main") {
		t.Errorf("the error does not name the branch it wanted: %v", err)
	}

	// And a repository somebody left on a detached head has no branch to push
	// at all.
	run(t, here, "checkout", "--detach")
	if err := here.Push(ctx); err == nil || !strings.Contains(err.Error(), "detached") {
		t.Fatalf("Push on a detached head = %v", err)
	}
	if err := here.Pull(ctx); err == nil || !strings.Contains(err.Error(), "detached") {
		t.Fatalf("Pull on a detached head = %v", err)
	}
}

func TestARepositoryThisPackageCreatesStartsOnMain(t *testing.T) {
	ctx := t.Context()
	url := bare(t)
	here := instance(t, "here", url)
	if err := here.Save(ctx, "d", paper(t, 1, "one").Snapshot()); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(run(t, here, "rev-parse", "--abbrev-ref", "HEAD")); got != "main" {
		t.Fatalf("a repository this package created is on %q", got)
	}
	// And that is the branch the shared repository ends up with, rather than a
	// master branch beside a main nobody asked for.
	if got := strings.TrimSpace(runIn(t, url, "rev-parse", "--abbrev-ref", "main")); got != "main" {
		t.Fatalf("the shared repository does not have a main branch: %q", got)
	}
}

func TestARepositoryThatAlreadyExistsKeepsItsOwnBranch(t *testing.T) {
	ctx := t.Context()
	url := bare(t)

	// A repository older than this package, on master, with a commit already on
	// it. Nothing here renames anybody's branch: what is pushed and fetched is
	// read from HEAD, so the store simply works on master.
	dir := t.TempDir()
	runIn(t, dir, "init", "--initial-branch=master")
	// The identity is given on the command rather than left to the machine's
	// git configuration, which a CI runner does not have.
	runIn(t, dir, "-c", "user.name=Ada", "-c", "user.email=ada@example",
		"commit", "--allow-empty", "-m", "older than this package")

	s, err := New(dir, WithAuthor("here", "here@example"), WithClock(stamps()),
		WithRemote(Remote{URL: url}))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, "d", paper(t, 1, "one").Snapshot()); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(run(t, s, "rev-parse", "--abbrev-ref", "HEAD")); got != "master" {
		t.Fatalf("an existing repository was moved to %q", got)
	}
	if got := strings.TrimSpace(runIn(t, url, "rev-parse", "--abbrev-ref", "master")); got != "master" {
		t.Fatalf("the push did not go to master: %q", got)
	}
}
