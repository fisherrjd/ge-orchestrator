package eval

import (
	"context"

	"github.com/osrs-ge/ge-orchestrator/internal/store"
)

// computeHold evaluates an H strategy: a 1-4 week reversion hold. Everything
// relaxes to match the horizon — hourly cadence (Policy.MinTickGap), wider
// entry/exit bands, 6h freshness (illiquid multi-week items trade sparsely;
// flip-grade staleness is normal here), a gentler volume gate, and a 6-tick
// kill run (~6h sustained at the hourly cadence). Realized value is
// mark-to-market per hour held, slipped and participation-capped.
func (ev *Evaluator) computeHold(ctx context.Context, st store.Strategy) (store.Evaluation, map[string]bool, error) {
	now := ev.now()
	snap, err := ev.source().Snapshot(ctx, st.PrimaryItemID)
	if err != nil {
		return store.Evaluation{}, nil, err
	}

	checks := map[string]bool{}
	checks["kill_price_ok"] = !killBreached(st, snap)
	checks["legs_fresh"] = legsFresh(snap, freshMaxAgeSHold)
	checks["entry_reachable"] = snap.Low != nil && float64(*snap.Low) <= float64(st.EntryPrice)*holdEntryBand
	checks["exit_reachable"] = snap.High != nil && float64(*snap.High) >= float64(st.ExitPrice)*holdExitBand
	if st.UnitsUsed != nil && *st.UnitsUsed > 0 {
		checks["vol_ok"] = snap.Vol30m >= *st.UnitsUsed/holdVolDivisor
	} else {
		checks["vol_ok"] = true
	}

	var realizedPer1h, rawPer1h *int64
	var cur *int64
	switch {
	case snap.High != nil && snap.Low != nil:
		m := (*snap.High + *snap.Low) / 2
		cur = &m
	case snap.High != nil:
		cur = snap.High
	case snap.Low != nil:
		cur = snap.Low
	}
	if cur != nil {
		units := int64(0)
		if st.UnitsUsed != nil {
			units = *st.UnitsUsed
		}
		capped := ev.haircutUnits(units, snap.Vol30m*8)
		hours := now.Sub(clockAnchor(st)).Hours()
		if hours < 1 {
			hours = 1
		}
		perUnitRaw := *cur - st.EntryPrice - sellTax(*cur)
		perUnit := ev.slipSell(*cur) - ev.slipBuy(st.EntryPrice) - sellTax(ev.slipSell(*cur))
		raw := int64(float64(units*perUnitRaw) / hours)
		hc := int64(float64(capped*perUnit) / hours)
		rawPer1h, realizedPer1h = &raw, &hc
	}

	verdict := "healthy"
	switch {
	case !checks["kill_price_ok"]:
		verdict = "kill_signal"
	case !checks["legs_fresh"] || !checks["vol_ok"]:
		verdict = "degraded"
	}

	e := baseEvaluation(st, now, snap)
	e.RealizedPer1hGp = realizedPer1h
	finishEvaluation(&e, checks, verdict, map[string]any{
		"cur_mid": cur, "hours_held": now.Sub(clockAnchor(st)).Hours(),
		"realized_raw_per_1h": rawPer1h, "realized_haircut_per_1h": realizedPer1h,
	})
	return e, checks, nil
}
