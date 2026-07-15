package eval

import (
	"context"
	"time"

	"github.com/osrs-ge/ge-orchestrator/internal/store"
)

// computeUpdate evaluates a U strategy in two phases around event.date (UTC
// midnight): PRE-EVENT is position building — only entry reachability,
// freshness and the kill stop matter; POST-EVENT is the same directional
// mark-to-market as an open V. transition() handles confirm/expire on the
// eval window; a hard stop closes anything still open 7 days past the event
// (the dislocation, if it was coming, came).
func (ev *Evaluator) computeUpdate(ctx context.Context, st store.Strategy) (store.Evaluation, map[string]bool, error) {
	now := ev.now()
	snap, err := ev.source().Snapshot(ctx, st.PrimaryItemID)
	if err != nil {
		return store.Evaluation{}, nil, err
	}

	var eventAt time.Time
	if st.Event != nil {
		eventAt, _ = time.Parse("2006-01-02", st.Event.Date)
	}
	postEvent := !eventAt.IsZero() && now.After(eventAt)

	checks := map[string]bool{}
	checks["kill_price_ok"] = !killBreached(st, snap)
	checks["legs_fresh"] = legsFresh(snap, freshMaxAgeS)

	var realizedPer1h, rawPer1h *int64
	if !postEvent {
		checks["entry_reachable"] = snap.Low != nil && float64(*snap.Low) <= float64(st.EntryPrice)*entryBand
	} else {
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
			fade := st.Direction != nil && *st.Direction == "fade"
			perUnitRaw := *cur - st.EntryPrice - sellTax(*cur)
			perUnit := ev.slipSell(*cur) - ev.slipBuy(st.EntryPrice) - sellTax(ev.slipSell(*cur))
			if fade {
				perUnitRaw = st.EntryPrice - *cur - sellTax(st.EntryPrice)
				perUnit = ev.slipSell(st.EntryPrice) - ev.slipBuy(*cur) - sellTax(ev.slipSell(st.EntryPrice))
			}
			hours := now.Sub(eventAt).Hours()
			if hours < 1 {
				hours = 1
			}
			raw := int64(float64(units*perUnitRaw) / hours)
			hc := int64(float64(capped*perUnit) / hours)
			rawPer1h, realizedPer1h = &raw, &hc
		}
	}

	verdict := "healthy"
	switch {
	case !checks["kill_price_ok"]:
		verdict = "kill_signal"
	case !checks["legs_fresh"] || (!postEvent && !checks["entry_reachable"]):
		verdict = "degraded"
	}

	e := baseEvaluation(st, now, snap)
	e.RealizedPer1hGp = realizedPer1h
	finishEvaluation(&e, checks, verdict, map[string]any{
		"event_date": st.Event, "post_event": postEvent, "direction": st.Direction,
		"realized_raw_per_1h": rawPer1h, "realized_haircut_per_1h": realizedPer1h,
	})

	// Hard stop: 7 days past the event, whatever was going to happen happened.
	if postEvent && now.Sub(eventAt) > 7*24*time.Hour && st.State == "open" {
		if err := ev.Store.CloseStrategy(ctx, st.StrategyID, "expired", "event passed 7+ days ago"); err != nil {
			return e, checks, err
		}
	}
	return e, checks, nil
}
