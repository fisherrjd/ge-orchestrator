package eval

import (
	"context"
	"time"

	"github.com/osrs-ge/ge-orchestrator/internal/store"
)

// computeVolume evaluates a V strategy.
//
// ARMED: only the trigger is evaluated (the same computation as ge-mcp's
// volume_zscore, trailing baseline). Armed ticks emit lightweight healthy
// evaluations with armed_waiting so the timeline is visible; they are
// excluded from confirmation stats by the triggered_at anchor. transition()
// flips the strategy open when trigger_fired, and expires it if it never
// fires within armedTTL.
//
// OPEN (post-trigger): a directional mark-to-market. ride profits when price
// rises from entry; fade when it falls. Realized = capped units × per-unit
// MTM delta ÷ hours since trigger (position basis includes slippage; the
// exit side pays tax when the direction implies a sell-out).
func (ev *Evaluator) computeVolume(ctx context.Context, st store.Strategy) (store.Evaluation, map[string]bool, error) {
	now := ev.now()

	if st.State == "armed" {
		window := time.Hour
		if st.Trigger != nil {
			if d, err := time.ParseDuration(st.Trigger.Window); err == nil && d > 0 {
				window = d
			}
		}
		z, n, move, err := ev.source().VolumeZ(ctx, st.PrimaryItemID, window)
		if err != nil {
			return store.Evaluation{}, nil, err
		}
		fired := false
		if st.Trigger != nil && n >= 3 {
			metric := z
			if st.Trigger.Metric == "price_move_pct" && move != nil {
				metric = *move
			}
			if st.Trigger.Direction == "above" {
				fired = metric >= st.Trigger.Threshold
			} else {
				fired = metric <= -st.Trigger.Threshold ||
					(st.Trigger.Metric == "price_move_pct" && metric <= st.Trigger.Threshold && st.Trigger.Threshold < 0)
			}
		}
		checks := map[string]bool{"armed_waiting": !fired, "trigger_fired": fired}
		e := baseEvaluation(st, now, nil)
		finishEvaluation(&e, checks, "healthy", map[string]any{
			"z": z, "n_baseline": n, "price_move_pct": move, "window": window.String(),
		})
		return e, checks, nil
	}

	snap, err := ev.source().Snapshot(ctx, st.PrimaryItemID)
	if err != nil {
		return store.Evaluation{}, nil, err
	}

	checks := map[string]bool{}
	checks["kill_price_ok"] = !killBreached(st, snap)
	checks["legs_fresh"] = legsFresh(snap, freshMaxAgeS)
	if st.UnitsUsed != nil && *st.UnitsUsed > 0 {
		checks["vol_ok"] = snap.Vol30m >= *st.UnitsUsed/volDivisor
	} else {
		checks["vol_ok"] = true
	}

	// Directional MTM off the mid (both sides when present, else the live one).
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

	fade := st.Direction != nil && *st.Direction == "fade"
	var realizedPer1h, rawPer1h *int64
	exitPrinted := false
	if cur != nil {
		units := int64(0)
		if st.UnitsUsed != nil {
			units = *st.UnitsUsed
		}
		// Post-shock fills: cap at participation of a 4h-equivalent of the
		// observed 30m volume (anomaly volume decays — this still flatters,
		// and detail says so).
		capped := ev.haircutUnits(units, snap.Vol30m*8)

		perUnitRaw := *cur - st.EntryPrice - sellTax(*cur)
		perUnit := ev.slipSell(*cur) - ev.slipBuy(st.EntryPrice) - sellTax(ev.slipSell(*cur))
		if fade {
			// Fade: profit when price falls from entry (short-equivalent on
			// paper; tax still models the eventual unwind).
			perUnitRaw = st.EntryPrice - *cur - sellTax(st.EntryPrice)
			perUnit = ev.slipSell(st.EntryPrice) - ev.slipBuy(*cur) - sellTax(ev.slipSell(st.EntryPrice))
		}
		hours := now.Sub(clockAnchor(st)).Hours()
		if hours < 1 {
			hours = 1
		}
		raw := int64(float64(units*perUnitRaw) / hours)
		hc := int64(float64(capped*perUnit) / hours)
		rawPer1h, realizedPer1h = &raw, &hc

		if fade {
			exitPrinted = *cur <= int64(float64(st.ExitPrice)/exitBand)
		} else {
			exitPrinted = *cur >= int64(float64(st.ExitPrice)*exitBand)
		}
	}
	checks["exit_reachable"] = exitPrinted || cur == nil // pre-exit is not a failure; only trend against kill is

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
		"direction": st.Direction, "cur_mid": cur, "exit_printed": exitPrinted,
		"realized_raw_per_1h": rawPer1h, "realized_haircut_per_1h": realizedPer1h,
		"note": "post-shock volume overstates fillable share; haircut applied",
	})
	return e, checks, nil
}
