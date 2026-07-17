package runner

import (
	"context"
	"fmt"
	"log"

	"github.com/osrs-ge/ge-orchestrator/internal/brief"
	"github.com/osrs-ge/ge-orchestrator/internal/eval"
	"github.com/osrs-ge/ge-orchestrator/internal/store"
)

// vet applies the ship-time rules a strategy must pass to enter the book.
// The agent is told these rules in its directive; vetting is the mechanical
// backstop for when it ships one anyway. Order matters: the cheapest DB-only
// rules run before the price snapshot, and capital is charged only by
// strategies that survived the earlier rules.
//
//  1. dedup — the live book already trades this item under this archetype;
//  2. kill pre-breach — the stop is already crossed at ship time (the
//     Dragon-dart failure: entry 1180, kill 1050, market already at 1028);
//  3. capital — the live book's committed capital plus this strategy must
//     fit the run brief's bankroll.
//
// Vet errors fail open (accept + log): a flaky price lookup must not turn
// into a dropped research run.
func (r *Runner) vet(ctx context.Context, p brief.Params, list []store.SidecarStrategy) (accepted []store.SidecarStrategy, vetoed []store.Vetoed) {
	committed, err := r.Store.CommittedCapital(ctx)
	if err != nil {
		log.Printf("vet: committed capital: %v (skipping capital rule)", err)
		committed = -1
	}
	for _, st := range list {
		if reason := r.vetOne(ctx, p, st, committed); reason != "" {
			vetoed = append(vetoed, store.Vetoed{Strategy: st, Reason: reason})
			continue
		}
		if committed >= 0 {
			committed += st.CapitalRequired
		}
		accepted = append(accepted, st)
	}
	return accepted, vetoed
}

func (r *Runner) vetOne(ctx context.Context, p brief.Params, st store.SidecarStrategy, committed int64) string {
	if len(st.Items) == 0 {
		return "" // InsertStrategies rejects the whole sidecar with a real error
	}
	itemID := st.PrimaryItemID()

	dup, err := r.Store.HasOpenStrategyForItem(ctx, itemID, st.Archetype)
	if err != nil {
		log.Printf("vet %s: dedup lookup: %v", st.ID, err)
	} else if dup {
		return fmt.Sprintf("vetoed at ship time: item %d already has an open %s strategy", itemID, st.Archetype)
	}

	if st.KillPrice != nil && r.Prices != nil {
		snap, err := r.Prices.Snapshot(ctx, itemID)
		if err != nil {
			log.Printf("vet %s: snapshot item %d: %v", st.ID, itemID, err)
		} else if ref := refPrice(snap); ref != nil && stopCrossed(st, *ref) {
			return fmt.Sprintf("vetoed at ship time: kill_price %d already breached (live price %d, entry %d)",
				*st.KillPrice, *ref, st.EntryPrice)
		}
	}

	if committed >= 0 && committed+st.CapitalRequired > p.CapitalGp {
		return fmt.Sprintf("vetoed at ship time: over capital budget (%d committed + %d required > %d)",
			committed, st.CapitalRequired, p.CapitalGp)
	}
	return ""
}

// refPrice mirrors eval.killBreached's reference choice: the high leg, else
// the low leg.
func refPrice(snap *eval.Snap) *int64 {
	if snap == nil {
		return nil
	}
	if snap.High != nil {
		return snap.High
	}
	return snap.Low
}

// stopCrossed applies the same directional stop rule the evaluator uses: a
// kill above entry means "price rose too far", below means "fell too far".
func stopCrossed(st store.SidecarStrategy, ref int64) bool {
	if *st.KillPrice >= st.EntryPrice {
		return ref >= *st.KillPrice
	}
	return ref <= *st.KillPrice
}
