//go:build (js && wasm) || !js

package collab

import (
	"context"
	"fmt"

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
// "GENUINE", give two replicas that received both batches "FORGEDE" and
// "GENUINE". Neither Apply returned an error. The divergence is silent and it
// is permanent.
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
// A deployment that federates wants a policy about the relationship instead:
// which sites a given link may speak for. See [Config.AuthorizeOperations].
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

// foreignSite names both sites, because which one was expected is the whole
// content of the refusal.
func foreignSite(from, found crdt.SiteID) error {
	return fmt.Errorf("collab: a session of site %d sent an operation made by site %d", from, found)
}
