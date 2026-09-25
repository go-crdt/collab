//go:build (js && wasm) || !js

package collab

import (
	"context"
	"fmt"
	"iter"

	"github.com/go-crdt/crdt"
)

// OwnSiteOnly is a [Config.AuthorizeOperations] that refuses a batch carrying
// any site but the one the session joined as.
//
//	srv := collab.NewServer(collab.Config{
//		Store:               store,
//		AuthorizeOperations: collab.OwnSiteOnly,
//	})
//
// # Why a server would want it
//
// Without a policy, a session may hand over operations another site made, and
// the server applies them. That is not merely wrong attribution. A site
// identity is the half of an operation's name that makes it unique, so two
// writers using one site id produce different characters with the same ID --
// and a CRDT converges on names.
//
// Measured, and it is the guarantee this whole package rests on: a forger
// writing "FORGED" as site 2, and the real site 2 working offline writing
// "GENUINE", give two replicas that received both batches two different
// documents. The divergence is permanent.
//
// Since crdt v0.48.0 it is no longer silent: the second batch each replica sees
// is refused with [github.com/go-crdt/crdt.ErrCollidingID]. That changes the
// announcement and not the outcome — refusing the second batch does not undo the
// first, so the replicas still disagree — and it does not reach a forger who
// reproduces what the replica already holds before diverging. A diagnostic that
// arrives after the document is wrong is why this policy is still the thing that
// prevents it. Measured in TestTwoWritersOnOneSiteIdentityDiverge.
//
// # Why it is not the default
//
// A link is a session too, and it is meant to carry other sites: [Server.Follow]
// joins as one site and relays the work of everyone on the server it follows,
// which is thousands of sites this server never authorised. Refusing them would
// refuse federation. Nothing on the wire says which kind of session is
// speaking, so the server cannot choose for a deployment -- which is why this
// is a policy to install rather than a rule.
//
// The side that must not have it is the FOLLOWER. What arrives over its link
// names sites it never authorised, so this refuses them and the link ends. The
// server being FOLLOWED sees an ordinary session and is untroubled: it may run
// this policy while federating. Measured all three ways in
// TestALinkCarryingOtherSitesMeetsOwnSiteOnly.
//
// When it does refuse a link, it refuses loudly -- the link's session ends at
// once and the error names both sites, "a session of site 9001 sent an
// operation made by site 1", rather than leaving a link that retries forever
// and a document that never fills.
//
// A deployment that federates wants a policy about the relationship instead:
// which sites a given link may speak for. See [Config.AuthorizeOperations].
//
// # It refuses a participant that resumes under a fresh site
//
// [ClientConfig.Resume] carries what a participant did while it was away, and
// those operations were written by the site it had THEN. Coming back under a
// new one makes them somebody else's as far as this policy can tell, so they
// are refused -- and the tab keeps showing them, because they are in its own
// replica and nowhere else. It looks like it worked.
//
// So resume as the site that wrote the work. Measured, in
// TestResumingUnderAFreshSiteLosesTheWorkToOwnSiteOnly: with no policy a fresh
// site's resume arrives, with this policy it does not, and with the authoring
// site it does.
// Sites is every site named by the operations in batches, in the order they
// appear and with repeats: a batch of a hundred characters from one site yields
// it a hundred times.
//
// It exists because the question "which sites does this batch name" had three
// answers in this repository and one of them was wrong. [OwnSiteOnly] walks all
// three kinds of operation; so does the helper in this package's own tests; and
// gitstore's federation example walked a batch's text and nothing else, so an
// institution it did not federate with was refused for a character and ALLOWED
// for a map entry. A policy that can be stepped around by writing to a different
// part of the document is not one. So the walk is exported, once, and a
// deployment writing its own policy has nothing to get wrong.
//
// All three kinds, and a valid batch carries exactly one of them, so two of the
// three loops do nothing on any given call.
func Sites(batches ...crdt.PartOps) iter.Seq[crdt.SiteID] {
	return func(yield func(crdt.SiteID) bool) {
		for _, b := range batches {
			for _, op := range b.Text {
				if !yield(op.ID.Site) {
					return
				}
			}
			for _, op := range b.List {
				if !yield(op.ID.Site) {
					return
				}
			}
			for _, op := range b.Map {
				if !yield(op.ID.Site) {
					return
				}
			}
		}
	}
}

// SpeaksFor builds a [Config.AuthorizeOperations] from one question: may this
// session hand over work that site made?
//
//	srv := collab.NewServer(collab.Config{
//		Store: store,
//		AuthorizeOperations: collab.SpeaksFor(
//			func(_ context.Context, _ string, _, carried crdt.SiteID) bool {
//				return lyon[carried] // the scopes this server federates with
//			}),
//	})
//
// # What it gets right so a deployment does not have to
//
// Every operation in the batch rather than the sender, because that difference is
// the whole point: a participant speaks for itself, a link speaks for a server,
// and what a link carries is not what it joined as. Every KIND of operation, for
// the reason [Sites] gives. And a session may ALWAYS speak for the site it joined
// as, without being asked — which is [OwnSiteOnly]'s rule, and is the part that is
// easy to get wrong in the dangerous direction.
//
// That last one is worth spelling out. A policy written as "the scopes I federate
// with" invites listing your own among them, and gitstore's example did: a link
// from Lyon was then allowed to write as one of Paris's own users, which is the
// attack in go-crdt/collab#175 and not a refinement of it. Here the question is
// never asked about the session's own site, so there is nothing to list.
//
// # What it deliberately does not know
//
// How a site maps to whoever may speak for it. That is the deployment's, and it
// is why this takes a predicate rather than a set of scopes: a real one asks its
// federation metadata, which may be per document and may want the context's
// deadline, so both are passed through.
//
// [OwnSiteOnly] is the same rule as this with a predicate that always says no, and
// it is written out rather than built from this. Measured on a one-operation batch,
// which is what a keystroke is: 3.50 ns inlined against 53.1 ns through here, so
// fifteen times, with the inlined arm measured before and after the other and
// agreeing to three decimals. Two indirect calls an operation -- the range-over-func
// yield and the predicate -- is what that buys.
//
// For a deployment that federates the same 50 ns is nothing: the fan-out of one
// edit costs 2.5 µs a participant, and what this saves is writing the walk by hand
// and getting one of its three kinds wrong, which is what happened twice in this
// repository. For the policy every server installs it would be a cost on every
// batch for a convenience nobody there needs, which is why OwnSiteOnly does not
// pay it.
func SpeaksFor(mayCarry func(ctx context.Context, document string, session, carried crdt.SiteID) bool) func(context.Context, string, crdt.SiteID, []crdt.PartOps) error {
	return func(ctx context.Context, document string, from crdt.SiteID, batches []crdt.PartOps) error {
		for site := range Sites(batches...) {
			if site == from {
				continue // speaking for itself, which needs no agreement
			}
			if !mayCarry(ctx, document, from, site) {
				return mayNotSpeakFor(from, site)
			}
		}
		return nil
	}
}

func OwnSiteOnly(_ context.Context, _ string, from crdt.SiteID, batches []crdt.PartOps) error {
	for _, batch := range batches {
		for _, op := range batch.Text {
			if op.ID.Site != from {
				return foreignSite(from, op.ID.Site)
			}
		}
		for _, op := range batch.List {
			if op.ID.Site != from {
				return foreignSite(from, op.ID.Site)
			}
		}
		for _, op := range batch.Map {
			if op.ID.Site != from {
				return foreignSite(from, op.ID.Site)
			}
		}
	}
	return nil
}

// mayNotSpeakFor names the relation rather than the mismatch, which is what a
// federating operator needs to read: [OwnSiteOnly]'s refusal means a session wrote
// as somebody else, and this one means a session wrote as somebody it is not
// entitled to speak for. The two are worth telling apart in a log, which is why
// they are not the same sentence. See [Config.OnOperationsRefused].
func mayNotSpeakFor(session, carried crdt.SiteID) error {
	return fmt.Errorf("collab: a session of site %d may not speak for site %d", session, carried)
}

// foreignSite names both sites, because which one was expected is the whole
// content of the refusal.
func foreignSite(from, found crdt.SiteID) error {
	return fmt.Errorf("collab: a session of site %d sent an operation made by site %d", from, found)
}
