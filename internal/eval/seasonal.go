package eval

import (
	"context"

	"github.com/osrs-ge/ge-orchestrator/internal/store"
)

// computeSeasonal evaluates an S strategy: buy in one hour-of-week window,
// sell in the other. Checks are TIME-GATED — entry reachability only matters
// inside the buy window, exit only inside the sell window, and outside both
// the strategy is healthy-by-default (out_of_window), never degraded: an
// unreachable entry on a Wednesday says nothing about a Saturday window.
//
// Realized value = what the windows actually printed since opening: median
// observed low in the buy window (slipped up) vs median observed high in the
// sell window (slipped down, taxed), units capped at Participation of the
// smaller window's traded volume. per_1h = per_cycle / 168 (one cycle/week).
func (ev *Evaluator) computeSeasonal(ctx context.Context, st store.Strategy) (store.Evaluation, map[string]bool, error) {
	now := ev.now()
	snap, err := ev.source().Snapshot(ctx, st.PrimaryItemID)
	if err != nil {
		return store.Evaluation{}, nil, err
	}

	buyW := Window{FromHow: st.BuyWindow.FromHow, ToHow: st.BuyWindow.ToHow}
	sellW := Window{FromHow: st.SellWindow.FromHow, ToHow: st.SellWindow.ToHow}
	nowHow := HourOfWeek(now)
	inBuy, inSell := buyW.Contains(nowHow), sellW.Contains(nowHow)

	checks := map[string]bool{}
	checks["kill_price_ok"] = !killBreached(st, snap)

	switch {
	case inBuy:
		checks["buy_leg_fresh"] = snap.LowAgeS != nil && *snap.LowAgeS <= freshMaxAgeS
		checks["entry_reachable"] = snap.Low != nil && float64(*snap.Low) <= float64(st.EntryPrice)*entryBand
		if st.UnitsUsed != nil && *st.UnitsUsed > 0 {
			checks["vol_ok"] = snap.Vol30m >= *st.UnitsUsed/volDivisor
		}
	case inSell:
		checks["sell_leg_fresh"] = snap.HighAgeS != nil && *snap.HighAgeS <= freshMaxAgeS
		checks["exit_reachable"] = snap.High != nil && float64(*snap.High) >= float64(st.ExitPrice)*exitBand
	default:
		checks["out_of_window"] = true
	}

	// Window realities since opening — the honest realized number.
	anchor := clockAnchor(st)
	buyStats, err := ev.source().WindowStats(ctx, st.PrimaryItemID, anchor, buyW)
	if err != nil {
		return store.Evaluation{}, nil, err
	}
	sellStats, err := ev.source().WindowStats(ctx, st.PrimaryItemID, anchor, sellW)
	if err != nil {
		return store.Evaluation{}, nil, err
	}

	// Enough in-window prints to judge the cycle (>=3 5m rows each side).
	checks["windows_observed"] = buyStats.Obs >= 3 && sellStats.Obs >= 3

	var realizedPer1h *int64
	var rawPer1h *int64
	windowGapDead := false
	if buyStats.MedLow != nil && sellStats.MedHigh != nil {
		units := int64(0)
		if st.UnitsUsed != nil {
			units = *st.UnitsUsed
		}
		volRef := buyStats.Volume
		if sellStats.Volume < volRef {
			volRef = sellStats.Volume
		}
		capped := ev.haircutUnits(units, volRef)

		buyRaw, sellRaw := *buyStats.MedLow, *sellStats.MedHigh
		rawCycle := units * (sellRaw - sellTax(sellRaw) - buyRaw)
		raw1h := rawCycle / 168
		rawPer1h = &raw1h

		buyH, sellH := ev.slipBuy(buyRaw), ev.slipSell(sellRaw)
		cycle := capped * (sellH - sellTax(sellH) - buyH)
		r := cycle / 168
		realizedPer1h = &r

		// The projection is dead when the observed window gap can't clear
		// tax even before the haircut.
		windowGapDead = sellRaw-sellTax(sellRaw)-buyRaw <= 0
	}

	verdict := "healthy"
	switch {
	case !checks["kill_price_ok"], checks["windows_observed"] && windowGapDead:
		verdict = "kill_signal"
	case inBuy && (!checks["buy_leg_fresh"] || !checks["entry_reachable"] || (st.UnitsUsed != nil && !checks["vol_ok"])),
		inSell && (!checks["sell_leg_fresh"] || !checks["exit_reachable"]):
		verdict = "degraded"
	}

	e := baseEvaluation(st, now, snap)
	e.RealizedPer1hGp = realizedPer1h
	finishEvaluation(&e, checks, verdict, map[string]any{
		"now_how": nowHow, "in_buy": inBuy, "in_sell": inSell,
		"med_buy": buyStats.MedLow, "med_sell": sellStats.MedHigh,
		"obs_buy": buyStats.Obs, "obs_sell": sellStats.Obs,
		"vol_buy_window": buyStats.Volume, "vol_sell_window": sellStats.Volume,
		"realized_raw_per_1h": rawPer1h, "realized_haircut_per_1h": realizedPer1h,
	})
	return e, checks, nil
}
