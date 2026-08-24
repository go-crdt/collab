package gitstore

import (
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
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
func openRepo(dir string) (*gitRepo, error) {
	repo, err := git.PlainOpen(dir)
	if errors.Is(err, git.ErrRepositoryNotExists) {
		repo, err = git.PlainInit(dir, false)
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
