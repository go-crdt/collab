package gitstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport"
)

// gitRepo is the repository interface over a real repository, and the only
// place in this package that names go-git.
//
// Everything above it is about documents; everything here is about git. The
// split is what lets a test say "staging fails" without having to find a way to
// make go-git believe it.
type gitRepo struct {
	repo *git.Repository
	tree *git.Worktree
}

// openRepo opens the repository at dir, initialising one if there is none.
//
// A repository that is already there is used exactly as it is, on whatever
// branch its HEAD names — everything that pushes and fetches here reads the
// branch from HEAD rather than assuming one, so an operator whose repository is
// on master, or on anything else, is not affected by the line below.
//
// What the line below decides is only the branch a repository this package
// creates from nothing starts on, and go-git's own default is master. That is
// the wrong answer here: a store that is given a remote will almost always meet
// a host whose default branch is main, and a first push would then put a master
// branch beside it that nobody asked for and nobody reads. An operator who
// wants something else creates the repository themselves, which is one command
// and is respected.
func openRepo(dir string) (*gitRepo, error) {
	repo, err := git.PlainOpen(dir)
	if errors.Is(err, git.ErrRepositoryNotExists) {
		repo, err = git.PlainInitWithOptions(dir, &git.PlainInitOptions{
			InitOptions: git.InitOptions{DefaultBranch: plumbing.Main},
		})
	}
	if err != nil {
		return nil, fmt.Errorf("gitstore: opening %s: %w", dir, err)
	}
	tree, err := repo.Worktree()
	if err != nil {
		return nil, fmt.Errorf("gitstore: %s has no worktree: %w", dir, err)
	}
	return &gitRepo{repo: repo, tree: tree}, nil
}

func (g *gitRepo) root() string { return g.tree.Filesystem.Root() }

func (g *gitRepo) create(name string) (io.WriteCloser, error) {
	f, err := g.tree.Filesystem.Create(name)
	if err != nil {
		return nil, fmt.Errorf("creating %s: %w", name, err)
	}
	return f, nil
}

func (g *gitRepo) open(name string) (io.ReadCloser, error) {
	f, err := g.tree.Filesystem.Open(name)
	if err != nil {
		return nil, err // the caller distinguishes absent from unreadable
	}
	return f, nil
}

func (g *gitRepo) add(name string) error {
	if _, err := g.tree.Add(name); err != nil {
		return fmt.Errorf("staging %s: %w", name, err)
	}
	return nil
}

// clean reports whether the worktree holds nothing to commit.
func (g *gitRepo) clean() (bool, error) {
	status, err := g.tree.Status()
	if err != nil {
		return false, fmt.Errorf("reading the worktree: %w", err)
	}
	return status.IsClean(), nil
}

func (g *gitRepo) commit(message string, who object.Signature) error {
	if _, err := g.tree.Commit(message, &git.CommitOptions{Author: &who, Committer: &who}); err != nil {
		return fmt.Errorf("committing: %w", err)
	}
	return nil
}

func (g *gitRepo) head() (plumbing.Hash, error) {
	ref, err := g.repo.Head()
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("nothing has been saved yet: %w", err)
	}
	return ref.Hash(), nil
}

func (g *gitRepo) tag(name string, at plumbing.Hash, tagger object.Signature, message string) error {
	_, err := g.repo.CreateTag(name, at, &git.CreateTagOptions{Tagger: &tagger, Message: message})
	if err != nil {
		return fmt.Errorf("tagging %q: %w", name, err)
	}
	return nil
}

// tags returns the commit each tag names, annotated tags resolved through their
// own object and lightweight ones taken as they are.
func (g *gitRepo) tags() (map[plumbing.Hash]string, error) {
	out := map[plumbing.Hash]string{}
	iter, err := g.repo.Tags()
	if err != nil {
		return nil, fmt.Errorf("reading tags: %w", err)
	}
	defer iter.Close()
	err = iter.ForEach(func(ref *plumbing.Reference) error {
		name := ref.Name().Short()
		if tag, err := g.repo.TagObject(ref.Hash()); err == nil {
			out[tag.Target] = name
			return nil
		}
		out[ref.Hash()] = name
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reading tags: %w", err)
	}
	return out, nil
}

// log returns the commits that touched anything under a directory, newest
// first. An empty repository has an empty history rather than an error.
func (g *gitRepo) log(under string) ([]Revision, error) {
	head, err := g.head()
	if err != nil {
		return nil, nil
	}
	tags, err := g.tags()
	if err != nil {
		return nil, err
	}
	iter, err := g.repo.Log(&git.LogOptions{From: head, PathFilter: func(p string) bool {
		return strings.HasPrefix(p, under+"/")
	}})
	if err != nil {
		return nil, fmt.Errorf("reading the history: %w", err)
	}
	defer iter.Close()
	var out []Revision
	err = iter.ForEach(func(c *object.Commit) error {
		out = append(out, Revision{
			Hash:    c.Hash.String(),
			When:    c.Author.When,
			Message: strings.SplitN(c.Message, "\n", 2)[0],
			Release: tags[c.Hash],
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reading the history: %w", err)
	}
	return out, nil
}

func (g *gitRepo) fileAt(hash plumbing.Hash, name string) ([]byte, error) {
	commit, err := g.repo.CommitObject(hash)
	if err != nil {
		return nil, fmt.Errorf("reading revision %s: %w", hash, err)
	}
	file, err := commit.File(path.Clean(name))
	if err != nil {
		return nil, fmt.Errorf("%s holds no %s: %w", hash, name, err)
	}
	content, err := file.Contents()
	if err != nil {
		return nil, fmt.Errorf("reading %s at %s: %w", name, hash, err)
	}
	return []byte(content), nil
}

// resolve turns a release name or a commit hash into a commit.
func (g *gitRepo) resolve(revision string) (plumbing.Hash, error) {
	if ref, err := g.repo.Tag(revision); err == nil {
		if tag, err := g.repo.TagObject(ref.Hash()); err == nil {
			return tag.Target, nil
		}
		return ref.Hash(), nil
	}
	hash, err := g.repo.ResolveRevision(plumbing.Revision(revision))
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("no revision %q: %w", revision, err)
	}
	return *hash, nil
}

// dirs lists the directories at the top of the worktree.
func (g *gitRepo) dirs() ([]string, error) {
	entries, err := g.tree.Filesystem.ReadDir(".")
	if err != nil {
		return nil, fmt.Errorf("reading the repository: %w", err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

// --- the remote

// remoteName is what the fetched branch is filed under, in refs/remotes. It is
// not written to the repository's config and it is not "origin": a repository
// this store shares may well have an origin of its own, belonging to whoever
// clones it, and taking that name would mean answering for somebody else's.
const remoteName = "collab"

// remoteFor is the remote as go-git wants it, built for the call and thrown
// away after it.
//
// Nothing is written to .git/config. The URL and the credentials come from the
// caller on every operation — a token expires, and an operator who moves the
// repository should not have to undo a line this package wrote behind them —
// so there is nothing worth persisting and a config nobody edited is a config
// nobody has to reconcile.
func (g *gitRepo) remoteFor(url string) *git.Remote {
	return git.NewRemote(g.repo.Storer, &config.RemoteConfig{Name: remoteName, URLs: []string{url}})
}

// branch is the branch this repository is on, which is the one that is pushed
// and the one that is fetched.
func (g *gitRepo) branch() (plumbing.ReferenceName, error) {
	ref, err := g.repo.Reference(plumbing.HEAD, false)
	if err != nil {
		return "", fmt.Errorf("reading HEAD: %w", err)
	}
	if ref.Type() != plumbing.SymbolicReference {
		return "", fmt.Errorf("HEAD is detached at %s, so there is no branch to push", ref.Hash())
	}
	return ref.Target(), nil
}

// push sends this repository's branch and its tags to the remote.
//
// The tags go with the branch because a release is a tag and a release that
// never leaves the machine is not one. Neither refspec is forced: a branch that
// is not a fast-forward means somebody else has committed and this instance has
// not pulled yet, and a tag that already names a different commit means two
// instances decided different things about one release — which is a
// disagreement between people, not between replicas, and the only thing a
// program can usefully do with it is say so.
func (g *gitRepo) push(ctx context.Context, url string, auth transport.AuthMethod) error {
	branch, err := g.branch()
	if err != nil {
		return err
	}
	err = g.remoteFor(url).PushContext(ctx, &git.PushOptions{
		RemoteName: remoteName,
		RefSpecs: []config.RefSpec{
			config.RefSpec(branch + ":" + branch),
			"refs/tags/*:refs/tags/*",
		},
		Auth: auth,
	})
	if errors.Is(err, git.NoErrAlreadyUpToDate) {
		return nil // the remote has it already, which is what a push is for
	}
	if err != nil {
		return fmt.Errorf("pushing: %w", err)
	}
	return nil
}

// fetch brings the remote's branch and tags into this repository and returns
// the commit the branch points at, or the zero hash if there is nothing there.
//
// The branch is fetched into refs/remotes, so nothing this instance has
// committed is touched by a fetch; deciding what to do with what arrived is
// [Store.Pull]'s, and it is a decision, not a ref update. Tags are fetched
// forced, because a tag is a name for a release and the remote is where the
// instances agree what a name means — and a local tag that disagreed with it
// could not have been pushed in the first place.
func (g *gitRepo) fetch(ctx context.Context, url string, auth transport.AuthMethod) (plumbing.Hash, error) {
	branch, err := g.branch()
	if err != nil {
		return plumbing.ZeroHash, err
	}
	tracking := plumbing.NewRemoteReferenceName(remoteName, branch.Short())
	err = g.remoteFor(url).FetchContext(ctx, &git.FetchOptions{
		RemoteName: remoteName,
		RefSpecs: []config.RefSpec{
			config.RefSpec("+" + branch + ":" + tracking),
			"+refs/tags/*:refs/tags/*",
		},
		Auth: auth,
	})
	switch {
	case err == nil, errors.Is(err, git.NoErrAlreadyUpToDate):
	case errors.Is(err, transport.ErrEmptyRemoteRepository):
		// A remote nobody has pushed to yet holds nothing to merge, which is
		// the ordinary state of the first instance to start.
		return plumbing.ZeroHash, nil
	default:
		return plumbing.ZeroHash, fmt.Errorf("fetching: %w", err)
	}
	ref, err := g.repo.Reference(tracking, true)
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("the remote has no %s: %w", branch.Short(), err)
	}
	return ref.Hash(), nil
}

// contains reports whether this repository's history already holds a commit.
func (g *gitRepo) contains(hash plumbing.Hash) (bool, error) {
	head, err := g.head()
	if err != nil {
		return false, nil // nothing has been committed here, so it holds nothing
	}
	if head == hash {
		return true, nil
	}
	ours, err := g.repo.CommitObject(head)
	if err != nil {
		return false, fmt.Errorf("reading %s: %w", head, err)
	}
	theirs, err := g.repo.CommitObject(hash)
	if err != nil {
		return false, fmt.Errorf("reading %s: %w", hash, err)
	}
	held, err := theirs.IsAncestor(ours)
	if err != nil {
		return false, fmt.Errorf("comparing %s with %s: %w", hash, head, err)
	}
	return held, nil
}

// documentsAt lists the directories a commit holds a document in, which is the
// ones with a state file: a directory without one is not something this store
// wrote, whatever it is called.
func (g *gitRepo) documentsAt(hash plumbing.Hash) ([]string, error) {
	commit, err := g.repo.CommitObject(hash)
	if err != nil {
		return nil, fmt.Errorf("reading revision %s: %w", hash, err)
	}
	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("reading revision %s: %w", hash, err)
	}
	var out []string
	for _, e := range tree.Entries {
		if e.Mode != filemode.Dir {
			continue
		}
		sub, err := tree.Tree(e.Name)
		if err != nil {
			return nil, fmt.Errorf("reading %s at %s: %w", e.Name, hash, err)
		}
		if _, err := sub.File(stateFile); err != nil {
			continue
		}
		out = append(out, e.Name)
	}
	return out, nil
}

// adopt copies into the worktree every file a commit holds that this side does
// not, and stages them.
//
// A merge commit's tree is built from the index, which holds this side's
// content, so a file that exists only on the other side would be missing from
// the merge — and a tree missing a file is git saying the merge deleted it.
// Everything this store writes is written again from the merged state and so
// is never missing; a repository is also where somebody puts a README, and a
// pull must not be the thing that removes one.
func (g *gitRepo) adopt(from plumbing.Hash) error {
	commit, err := g.repo.CommitObject(from)
	if err != nil {
		return fmt.Errorf("reading revision %s: %w", from, err)
	}
	tree, err := commit.Tree()
	if err != nil {
		return fmt.Errorf("reading revision %s: %w", from, err)
	}
	iter := tree.Files()
	defer iter.Close()
	return iter.ForEach(func(f *object.File) error {
		if _, err := g.tree.Filesystem.Stat(f.Name); err == nil {
			return nil // this side has one, and this side's is the one that stays
		}
		content, err := f.Contents()
		if err != nil {
			return fmt.Errorf("reading %s at %s: %w", f.Name, from, err)
		}
		out, err := g.create(f.Name)
		if err != nil {
			return err
		}
		if _, err := io.WriteString(out, content); err != nil {
			out.Close()
			return fmt.Errorf("writing %s: %w", f.Name, err)
		}
		if err := out.Close(); err != nil {
			return fmt.Errorf("writing %s: %w", f.Name, err)
		}
		return g.add(f.Name)
	})
}

// mergeCommit records the worktree as a commit with two parents, this side's
// and the other's.
//
// The second parent is the point of the whole operation: it is what puts the
// other instance's history into this branch, and a branch that does not hold
// theirs cannot be pushed. It is made even when the tree is unchanged, because
// what the commit records is not that the documents moved but that this
// instance now holds what the other one committed.
func (g *gitRepo) mergeCommit(message string, who object.Signature, other plumbing.Hash) error {
	parents := []plumbing.Hash{}
	if head, err := g.head(); err == nil {
		parents = append(parents, head)
	}
	parents = append(parents, other)
	_, err := g.tree.Commit(message, &git.CommitOptions{
		Author:            &who,
		Committer:         &who,
		Parents:           parents,
		AllowEmptyCommits: true,
	})
	if err != nil {
		return fmt.Errorf("committing the merge: %w", err)
	}
	return nil
}
