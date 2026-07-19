package runner

import (
	"context"
	"fmt"
	"log"

	"github.com/osrs-ge/ge-orchestrator/internal/brief"
	"github.com/osrs-ge/ge-orchestrator/internal/eval"
	"github.com/osrs-ge/ge-orchestrator/internal/store"
)

// Absolute-gp ship floors (flips-first redesign). These mirror ge-agent's
// validator constants; the vet is the mechanical backstop for a model that
// ships below them anyway.
const (
	floorFPerCycleGp = 200_000
	floorBPerCycleGp = 100_000
	// evSanityFactor caps how far a claimed per-cycle EV may exceed the
	// live recomputation (margin x fillable units) before it is vetoed.
	evSanityFactor = 2
)

// vet applies the ship-time rules a strategy must pass to enter the book.
// The agent is told these rules in its directive; vetting is the mechanical
// backstop for when it ships one anyway. Order matters: the cheapest DB-only
// rules run before the price snapshot.
//
//  1. dedup — the live book already trades this item under this archetype;
//  2. kill pre-breach — the stop is already crossed at ship time (the
//     Dragon-dart failure: entry 1180, kill 1050, market already at 1028);
//  3. capital + floor — each strategy independently fits the research
//     budget (a per-opportunity sizing scale, NOT a shared pool the book
//     drains) and clears its lane's absolute-gp floor;
//  4. EV sanity (F/B) — the claimed per-cycle gp must be within
//     evSanityFactor of a live recomputation from the current margin and
//     fillable size. Projections are the denominator of every scoreboard
//     ratio; an inflated claim poisons the track record.
//
// Vet errors fail open (accept + log): a flaky price lookup must not turn
// into a dropped research run.
func (r *Runner) vet(ctx context.Context, p brief.Params, list []store.SidecarStrategy) (accepted []store.SidecarStrategy, vetoed []store.Vetoed) {
	for _, st := range list {
		if reason := r.vetOne(ctx, p, st); reason != "" {
			vetoed = append(vetoed, store.Vetoed{Strategy: st, Reason: reason})
			continue
		}
		accepted = append(accepted, st)
	}
	return accepted, vetoed
}

func (r *Runner) vetOne(ctx context.Context, p brief.Params, st store.SidecarStrategy) string {
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

	var snap *eval.Snap
	if r.Prices != nil {
		if snap, err = r.Prices.Snapshot(ctx, itemID); err != nil {
			log.Printf("vet %s: snapshot item %d: %v", st.ID, itemID, err)
			snap = nil
		}
	}

	if st.KillPrice != nil {
		if ref := refPrice(snap); ref != nil && stopCrossed(st, *ref) {
			return fmt.Sprintf("vetoed at ship time: kill_price %d already breached (live price %d, entry %d)",
				*st.KillPrice, *ref, st.EntryPrice)
		}
	}

	// Per-opportunity capital: every strategy is sized against the full
	// research budget on its own — no shared committed-capital remainder.
	if st.CapitalRequired > p.CapitalGp {
		return fmt.Sprintf("vetoed at ship time: capital_required %d exceeds the %d research budget",
			st.CapitalRequired, p.CapitalGp)
	}

	switch st.Archetype {
	case "F":
		if st.ExpectedValue.PerCycleGp < floorFPerCycleGp {
			return fmt.Sprintf("vetoed at ship time: per_cycle_gp %d below the %d lane-F floor",
				st.ExpectedValue.PerCycleGp, floorFPerCycleGp)
		}
	case "B":
		if st.ExpectedValue.PerCycleGp < floorBPerCycleGp {
			return fmt.Sprintf("vetoed at ship time: per_cycle_gp %d below the %d lane-B floor",
				st.ExpectedValue.PerCycleGp, floorBPerCycleGp)
		}
	}

	if reason := evSanity(st, snap); reason != "" {
		return reason
	}
	return ""
}

// evSanity recomputes a flip strategy's per-cycle ceiling from the live
// snapshot: post-tax margin x min(units_used, 15% participation of a
// 4h-equivalent volume (Vol30m x 8)). A claim more than evSanityFactor above
// that ceiling is vetoed. Fail open when the snapshot is missing a leg or
// volume — freshness problems are the agent's quote-before-ship duty, and
// the paper-trader re-prices everything anyway.
func evSanity(st store.SidecarStrategy, snap *eval.Snap) string {
	if st.Archetype != "F" && st.Archetype != "B" {
		return ""
	}
	if snap == nil || snap.Margin == nil || snap.Vol30m <= 0 || st.Size.UnitsUsed <= 0 {
		return ""
	}
	fillable := int64(float64(snap.Vol30m*8) * 0.15)
	if fillable < 1 {
		fillable = 1
	}
	units := st.Size.UnitsUsed
	if fillable < units {
		units = fillable
	}
	ceiling := *snap.Margin * units
	if ceiling < 0 {
		ceiling = 0
	}
	if st.ExpectedValue.PerCycleGp > ceiling*evSanityFactor {
		return fmt.Sprintf("vetoed at ship time: claimed per_cycle_gp %d exceeds %dx the live recomputation %d (margin %d x %d fillable units)",
			st.ExpectedValue.PerCycleGp, evSanityFactor, ceiling, *snap.Margin, units)
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
