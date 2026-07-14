// Package eval paper-trades open strategies against the live price tables.
// v1 interprets only the structured fields (entry/exit/kill price, units) —
// free-text invalidation is never parsed. Every evaluation snapshots exactly
// what it saw, so the scoreboard reflects what was observable, not a later
// re-query.
package eval

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/osrs-ge/ge-orchestrator/internal/store"
)

const (
	freshMaxAgeS      = 900  // both legs traded within 15 min
	marginOKFraction  = 0.5  // A/B/C: current margin >= 50% of projected per-unit
	entryBand         = 1.02 // entry still reachable within +2%
	exitBand          = 0.98 // exit still prints within -2%
	volDivisor        = 8    // 30m volume must cover units/8
	killConsecutive   = 3    // kill_signal on N consecutive evals -> killed
	confirmHealthyPct = 0.80
	confirmRatioMin   = 0.5
	natureRuneID      = 561
)

type Evaluator struct {
	Store *store.Store
}

type snapshot struct {
	high, low, margin       *int64
	highAgeS, lowAgeS       *int
	vol30m                  int64
	natLow                  *int64 // latest nature rune low (archetype G)
	highalch                *int64 // items.highalch of the primary item
}

// Tick evaluates every open strategy once and applies state transitions.
func (ev *Evaluator) Tick(ctx context.Context) {
	open, err := ev.Store.OpenStrategies(ctx)
	if err != nil {
		log.Printf("eval: list open: %v", err)
		return
	}
	for _, st := range open {
		if err := ev.evaluate(ctx, st); err != nil {
			log.Printf("eval: strategy %d (%s): %v", st.StrategyID, st.Sid, err)
		}
	}
}

func (ev *Evaluator) evaluate(ctx context.Context, st store.Strategy) error {
	e, checks, err := ev.Compute(ctx, st)
	if err != nil {
		return err
	}
	if err := ev.Store.InsertEvaluation(ctx, e); err != nil {
		return err
	}
	return ev.transition(ctx, st, e.At, e.Verdict, checks)
}

// Compute runs the checks against live prices WITHOUT persisting — shared by
// the 5-min ticker (which stores the result) and the API's live=1 view.
func (ev *Evaluator) Compute(ctx context.Context, st store.Strategy) (store.Evaluation, map[string]bool, error) {
	now := time.Now().UTC()

	snap, err := ev.snapshot(ctx, st.PrimaryItemID)
	if err != nil {
		return store.Evaluation{}, nil, err
	}

	checks := map[string]bool{}
	// legs_fresh: both legs traded recently.
	checks["legs_fresh"] = snap.highAgeS != nil && snap.lowAgeS != nil &&
		*snap.highAgeS <= freshMaxAgeS && *snap.lowAgeS <= freshMaxAgeS

	// margin_ok + realized value, archetype-dependent.
	var realizedPer4h *int64
	unitMargin := int64(0)
	if st.PerCycleGp != nil && st.UnitsUsed != nil && *st.UnitsUsed > 0 {
		unitMargin = *st.PerCycleGp / *st.UnitsUsed
	}
	switch st.Archetype {
	case "G":
		// Alch gap: highalch - nature_low - cur_high still positive.
		ok := false
		if snap.highalch != nil && snap.natLow != nil && snap.high != nil {
			gap := *snap.highalch - *snap.natLow - *snap.high
			ok = gap > 0
			if st.UnitsUsed != nil {
				r := gap * *st.UnitsUsed
				realizedPer4h = &r
			}
		}
		checks["margin_ok"] = ok
	default:
		ok := false
		if snap.margin != nil {
			ok = float64(*snap.margin) >= marginOKFraction*float64(unitMargin)
			if st.UnitsUsed != nil {
				r := *snap.margin * *st.UnitsUsed
				realizedPer4h = &r
			}
		}
		checks["margin_ok"] = ok
	}

	// entry/exit reachable.
	checks["entry_reachable"] = snap.low != nil && float64(*snap.low) <= float64(st.EntryPrice)*entryBand
	if st.Archetype == "G" {
		checks["exit_reachable"] = true // exit is the alch value, always "prints"
	} else {
		checks["exit_reachable"] = snap.high != nil && float64(*snap.high) >= float64(st.ExitPrice)*exitBand
	}

	// kill_price: the model's own stop, on the primary item's relevant leg.
	breached := false
	if st.KillPrice != nil {
		ref := snap.high
		if ref == nil {
			ref = snap.low
		}
		if ref != nil {
			// Direction: a kill above entry means "price rose too far", below
			// means "fell too far". Breach = crossed away from entry.
			if *st.KillPrice >= st.EntryPrice {
				breached = *ref >= *st.KillPrice
			} else {
				breached = *ref <= *st.KillPrice
			}
		}
	}
	// Uniform polarity: true = good, so every check renders the same way.
	checks["kill_price_ok"] = !breached

	// vol_ok: 30m volume supports the stated size.
	if st.UnitsUsed != nil && *st.UnitsUsed > 0 {
		checks["vol_ok"] = snap.vol30m >= *st.UnitsUsed/volDivisor
	} else {
		checks["vol_ok"] = true
	}

	verdict := "healthy"
	switch {
	case breached || !checks["margin_ok"]:
		verdict = "kill_signal"
	case !checks["legs_fresh"] || !checks["vol_ok"] || !checks["entry_reachable"] || !checks["exit_reachable"]:
		verdict = "degraded"
	}

	checksJSON, _ := json.Marshal(checks)
	return store.Evaluation{
		StrategyID: st.StrategyID, At: now,
		CurHigh: snap.high, CurLow: snap.low,
		HighAgeS: snap.highAgeS, LowAgeS: snap.lowAgeS,
		CurMargin: snap.margin, Vol30m: &snap.vol30m,
		RealizedPer4hGp: realizedPer4h, Checks: checksJSON, Verdict: verdict,
	}, checks, nil
}

// transition applies the open -> killed/confirmed/expired state machine.
func (ev *Evaluator) transition(ctx context.Context, st store.Strategy, now time.Time, verdict string, checks map[string]bool) error {
	// killed: kill_signal on N consecutive evaluations (~15 min sustained).
	if verdict == "kill_signal" {
		last, err := ev.Store.LastVerdicts(ctx, st.StrategyID, killConsecutive)
		if err != nil {
			return err
		}
		kills := 0
		for _, v := range last {
			if v == "kill_signal" {
				kills++
			}
		}
		if kills >= killConsecutive {
			reason := fmt.Sprintf("kill_signal sustained %d evaluations (checks: %v)", kills, failing(checks))
			log.Printf("eval: strategy %d (%s) KILLED: %s", st.StrategyID, st.Sid, reason)
			return ev.Store.CloseStrategy(ctx, st.StrategyID, "killed", reason)
		}
	}

	// window elapsed -> confirmed or expired.
	if now.Sub(st.OpenedAt) >= 48*time.Hour {
		total, healthy, ratio, err := ev.Store.EvalStats(ctx, st.StrategyID)
		if err != nil {
			return err
		}
		if total > 0 && float64(healthy)/float64(total) >= confirmHealthyPct &&
			ratio != nil && *ratio >= confirmRatioMin {
			reason := fmt.Sprintf("48h window: %d/%d healthy, median realized/projected %.2f", healthy, total, *ratio)
			return ev.Store.CloseStrategy(ctx, st.StrategyID, "confirmed", reason)
		}
		r := "48h window elapsed without meeting confirmation"
		if ratio != nil {
			r = fmt.Sprintf("%s (%d/%d healthy, ratio %.2f)", r, healthy, total, *ratio)
		}
		return ev.Store.CloseStrategy(ctx, st.StrategyID, "expired", r)
	}
	return nil
}

func failing(checks map[string]bool) []string {
	var out []string
	for k, ok := range checks {
		if !ok {
			out = append(out, k)
		}
	}
	return out
}

// snapshot reads everything one evaluation needs in a single round trip.
func (ev *Evaluator) snapshot(ctx context.Context, itemID int) (*snapshot, error) {
	var s snapshot
	err := ev.Store.Pool.QueryRow(ctx, `
		WITH q AS (
		  SELECT DISTINCT ON (item_id) high, low, margin,
		         extract(epoch from now() - high_time)::int AS high_age_s,
		         extract(epoch from now() - low_time)::int  AS low_age_s
		  FROM prices_1m WHERE item_id = $1 ORDER BY item_id, ts DESC
		),
		v AS (
		  SELECT coalesce(sum(coalesce(high_volume,0)+coalesce(low_volume,0)),0) AS vol_30m
		  FROM prices_5m WHERE item_id = $1 AND ts > now() - interval '30 min'
		),
		nat AS (
		  SELECT low FROM prices_1m WHERE item_id = $2 AND low IS NOT NULL
		  ORDER BY ts DESC LIMIT 1
		),
		alch AS (SELECT highalch FROM items WHERE item_id = $1)
		SELECT q.high, q.low, q.margin, q.high_age_s, q.low_age_s, v.vol_30m,
		       (SELECT low FROM nat), (SELECT highalch::bigint FROM alch)
		FROM q, v`, itemID, natureRuneID).
		Scan(&s.high, &s.low, &s.margin, &s.highAgeS, &s.lowAgeS, &s.vol30m, &s.natLow, &s.highalch)
	if err != nil {
		return nil, fmt.Errorf("snapshot item %d: %w", itemID, err)
	}
	return &s, nil
}
