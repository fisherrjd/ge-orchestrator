package eval

import (
	"context"

	"github.com/osrs-ge/ge-orchestrator/internal/store"
)

// Flip lanes (flips-first redesign). F = volume flip: one 4h buy-limit cycle,
// realized value is the LIVE post-tax spread times participation-capped
// units, per cycle hour. B = high-value flip: a 1-4 day hold on a 10M+ item,
// marked to market per hour held like H was. Both kill when the live cycle
// capacity collapses below the lane floor (sustained over KillConsecutive
// evals) — the floor that ships a strategy is the floor that keeps it alive.
const (
	freshMaxAgeSFlip = 3600 // both legs within 60 min — flip markets print constantly
	flipCycleHours   = 4    // F: one buy-limit cycle
	floorFCapacityGp = 200_000
	floorBCapacityGp = 100_000
)

func (ev *Evaluator) computeFlip(ctx context.Context, st store.Strategy) (store.Evaluation, map[string]bool, error) {
	now := ev.now()
	snap, err := ev.source().Snapshot(ctx, st.PrimaryItemID)
	if err != nil {
		return store.Evaluation{}, nil, err
	}

	units := int64(0)
	if st.UnitsUsed != nil {
		units = *st.UnitsUsed
	}
	capped := ev.haircutUnits(units, snap.Vol30m*8)

	checks := map[string]bool{}
	checks["kill_price_ok"] = !killBreached(st, snap)
	checks["legs_fresh"] = legsFresh(snap, freshMaxAgeSFlip)
	checks["margin_alive"] = snap.Margin != nil && *snap.Margin > 0
	checks["entry_reachable"] = snap.Low != nil && float64(*snap.Low) <= float64(st.EntryPrice)*entryBand
	checks["exit_reachable"] = snap.High != nil && float64(*snap.High) >= float64(st.ExitPrice)*exitBand
	if units > 0 {
		checks["vol_ok"] = snap.Vol30m >= units/volDivisor
	} else {
		checks["vol_ok"] = true
	}

	// floor_ok: the live cycle capacity (post-tax margin x capped units)
	// still clears the lane's absolute-gp floor. A missing leg is a
	// freshness problem, not a floor breach — only a live margin can fail
	// this check.
	floor := int64(floorFCapacityGp)
	if st.Archetype == "B" {
		floor = floorBCapacityGp
	}
	var capacity *int64
	if snap.Margin != nil {
		c := *snap.Margin * capped
		capacity = &c
		checks["floor_ok"] = c >= floor
	} else {
		checks["floor_ok"] = true
	}

	var realizedPer1h, rawPer1h *int64
	if st.Archetype == "F" {
		// Spread capture: what a full cycle pays at the live spread, per
		// cycle hour. Raw = stored post-tax margin; haircut = slipped both
		// sides and participation-capped.
		if snap.Margin != nil {
			raw := *snap.Margin * units / flipCycleHours
			rawPer1h = &raw
		}
		if snap.High != nil && snap.Low != nil {
			perUnit := ev.slipSell(*snap.High) - ev.slipBuy(*snap.Low) - sellTax(ev.slipSell(*snap.High))
			hc := perUnit * capped / flipCycleHours
			realizedPer1h = &hc
		}
	} else {
		// B: mark-to-market per hour held, like a short hold.
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
	}

	verdict := "healthy"
	switch {
	case !checks["kill_price_ok"] || !checks["floor_ok"]:
		verdict = "kill_signal"
	case !checks["legs_fresh"] || !checks["margin_alive"] || !checks["vol_ok"]:
		verdict = "degraded"
	}

	e := baseEvaluation(st, now, snap)
	e.RealizedPer1hGp = realizedPer1h
	finishEvaluation(&e, checks, verdict, map[string]any{
		"live_cycle_capacity_gp": capacity, "lane_floor_gp": floor,
		"units_capped": capped, "hours_held": now.Sub(clockAnchor(st)).Hours(),
		"realized_raw_per_1h": rawPer1h, "realized_haircut_per_1h": realizedPer1h,
	})
	return e, checks, nil
}
