// Package eval paper-trades strategies against live prices with a per-kind
// evaluator (S seasonal window / V armed volume trigger / C multi-leg
// conversion / U event-anchored / H swing hold).
//
// Honesty rules, common to every kind:
//   - Each evaluation is a frozen snapshot; verdicts come only from what the
//     tick saw. Free-text invalidation is never parsed — only the structured
//     fields trade.
//   - Paper results are UPPER BOUNDS: we never act, so observed prices embed
//     zero market impact from our own hypothetical orders. Every realized
//     number therefore takes the self-impact haircut: units capped at
//     Participation (default 15%) of the observed relevant volume, and fills
//     modeled at entry×(1+Slippage) / exit×(1−Slippage) (default 0.5% —
//     crossing part of the spread to actually fill). detail carries both
//     realized_raw and realized_haircut; the scalar column (and so the
//     scoreboard) uses the haircut.
package eval

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/osrs-ge/ge-orchestrator/internal/store"
)

const (
	freshMaxAgeS       = 900  // both legs traded within 15 min (S/V/C/U)
	freshMaxAgeSHold   = 6 * 3600 // H: multi-week items trade sparsely
	marginOKFraction   = 0.5  // current margin >= 50% of projected
	entryBand          = 1.02 // entry reachable within +2%
	exitBand           = 0.98 // exit printing within -2%
	holdEntryBand      = 1.05 // H: wider bands, coarser horizon
	holdExitBand       = 0.95
	volDivisor         = int64(8)  // 30m volume >= units/8
	holdVolDivisor     = int64(32) // H: relaxed
	confirmHealthyPct  = 0.80
	confirmRatioMin    = 0.5
	holdConfirmRatio   = 0.3 // H: long-horizon MTM is noisy
	armedTTL           = 7 * 24 * time.Hour
	sellTaxCap         = int64(5_000_000)

	defaultParticipation = 0.15
	defaultSlippage      = 0.005
)

// Policy is a kind's transition tuning.
type Policy struct {
	KillConsecutive int
	ConfirmHealthy  float64
	ConfirmRatioMin float64
	MinTickGap      time.Duration
}

func policyFor(archetype string) Policy {
	switch archetype {
	case "H":
		return Policy{KillConsecutive: 6, ConfirmHealthy: confirmHealthyPct, ConfirmRatioMin: holdConfirmRatio, MinTickGap: time.Hour}
	default:
		return Policy{KillConsecutive: 3, ConfirmHealthy: confirmHealthyPct, ConfirmRatioMin: confirmRatioMin}
	}
}

type Evaluator struct {
	Store  *store.Store
	Source PriceSource // nil = pg-backed on Store.Pool

	Participation float64 // 0 = default 0.15
	Slippage      float64 // 0 = default 0.005
	Now           func() time.Time
}

func (ev *Evaluator) source() PriceSource {
	if ev.Source == nil {
		ev.Source = NewPgSource(ev.Store.Pool)
	}
	return ev.Source
}

func (ev *Evaluator) now() time.Time {
	if ev.Now != nil {
		return ev.Now()
	}
	return time.Now().UTC()
}

func (ev *Evaluator) participation() float64 {
	if ev.Participation > 0 {
		return ev.Participation
	}
	return defaultParticipation
}

func (ev *Evaluator) slippage() float64 {
	if ev.Slippage > 0 {
		return ev.Slippage
	}
	return defaultSlippage
}

// Tick evaluates every open/armed strategy once, honoring per-kind cadence.
func (ev *Evaluator) Tick(ctx context.Context) {
	list, err := ev.Store.EvaluableStrategies(ctx)
	if err != nil {
		log.Printf("eval: list: %v", err)
		return
	}
	for _, st := range list {
		pol := policyFor(st.Archetype)
		if pol.MinTickGap > 0 {
			last, err := ev.Store.LastEvalAt(ctx, st.StrategyID)
			if err == nil && last != nil && ev.now().Sub(*last) < pol.MinTickGap {
				continue
			}
		}
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

// Compute runs the per-kind checks against live prices WITHOUT persisting —
// shared by the ticker (which stores the result) and the API's live=1 view.
func (ev *Evaluator) Compute(ctx context.Context, st store.Strategy) (store.Evaluation, map[string]bool, error) {
	switch st.Archetype {
	case "S":
		return ev.computeSeasonal(ctx, st)
	case "V":
		return ev.computeVolume(ctx, st)
	case "C":
		return ev.computeCombo(ctx, st)
	case "U":
		return ev.computeUpdate(ctx, st)
	case "H":
		return ev.computeHold(ctx, st)
	default:
		// Legacy rows are all closed by migration 004; nothing evaluable.
		return ev.computeHold(ctx, st)
	}
}

// clockAnchor is when the strategy's evaluation window started: the trigger
// moment for a fired V, opened_at otherwise.
func clockAnchor(st store.Strategy) time.Time {
	if st.TriggeredAt != nil {
		return *st.TriggeredAt
	}
	return st.OpenedAt
}

// transition applies the state machine.
// armed:  expire when the trigger never fires within armedTTL.
// open:   kill on N consecutive kill_signals; at anchor+eval_window ->
//         confirmed (healthy share + median realized/projected) else expired.
func (ev *Evaluator) transition(ctx context.Context, st store.Strategy, now time.Time, verdict string, checks map[string]bool) error {
	if st.State == "armed" {
		if checks["trigger_fired"] {
			return ev.Store.MarkTriggered(ctx, st.StrategyID, now)
		}
		if now.Sub(st.OpenedAt) >= armedTTL {
			return ev.Store.CloseStrategy(ctx, st.StrategyID, "expired",
				"armed trigger never fired within 7 days")
		}
		return nil
	}

	pol := policyFor(st.Archetype)
	if verdict == "kill_signal" {
		last, err := ev.Store.LastVerdicts(ctx, st.StrategyID, pol.KillConsecutive)
		if err != nil {
			return err
		}
		kills := 0
		for _, v := range last {
			if v != "kill_signal" {
				break
			}
			kills++
		}
		if kills >= pol.KillConsecutive {
			reason := "kill signal sustained over " + itoa(pol.KillConsecutive) + " consecutive evaluations"
			if fails := failing(checks); len(fails) > 0 {
				reason += " (failing: " + join(fails) + ")"
			}
			return ev.closeAndScore(ctx, st, "killed", reason)
		}
		return nil
	}

	anchor := clockAnchor(st)
	window := st.EvalWindow
	if window <= 0 {
		window = 48 * time.Hour
	}
	if now.Sub(anchor) >= window {
		total, healthy, ratio, err := ev.Store.EvalStats(ctx, st.StrategyID, anchor)
		if err != nil {
			return err
		}
		// S additionally needs at least one observed cycle in each window —
		// a week where the market never printed in your windows proves nothing.
		if st.Archetype == "S" && !checks["windows_observed"] {
			return ev.closeAndScore(ctx, st, "expired",
				"eval window elapsed without enough in-window observations to judge")
		}
		if total > 0 && float64(healthy)/float64(total) >= pol.ConfirmHealthy &&
			ratio != nil && *ratio >= pol.ConfirmRatioMin {
			reason := sprintf("window %s: %d/%d healthy, median realized/projected %.2f (haircut)", window, healthy, total, *ratio)
			return ev.closeAndScore(ctx, st, "confirmed", reason)
		}
		r := sprintf("window %s elapsed without meeting confirmation", window)
		if ratio != nil {
			r += sprintf(" (%d/%d healthy, ratio %.2f)", healthy, total, *ratio)
		}
		return ev.closeAndScore(ctx, st, "expired", r)
	}
	return nil
}

// closeAndScore closes the strategy and feeds the outcome into the watch
// portfolio (confirmations promote the item; kills and expiries decay it).
// Scoring failure is logged, not returned — the close already happened and
// must not be retried as if it hadn't.
func (ev *Evaluator) closeAndScore(ctx context.Context, st store.Strategy, state, reason string) error {
	if err := ev.Store.CloseStrategy(ctx, st.StrategyID, state, reason); err != nil {
		return err
	}
	name := firstItemName(st.Items)
	if err := ev.Store.RecordStrategyOutcome(ctx, st.PrimaryItemID, name, st.Archetype, st.Sid, state); err != nil {
		log.Printf("eval: watchlist outcome for %s (%s): %v", st.Sid, state, err)
	}
	return nil
}

// firstItemName digs the display name out of the stored items JSON.
func firstItemName(raw json.RawMessage) string {
	var items []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &items); err != nil || len(items) == 0 || items[0].Name == "" {
		return "unknown item"
	}
	return items[0].Name
}

// --- shared helpers ---

// sellTax is the GE tax on one unit sold at price p (the ingest margin
// formula applied to a sell fill): floor(p/50) capped at 5M.
func sellTax(p int64) int64 {
	t := p / 50
	if t > sellTaxCap {
		return sellTaxCap
	}
	return t
}

// haircutUnits caps hypothetical fills at a participation share of the
// volume that actually traded.
func (ev *Evaluator) haircutUnits(units int64, observedVolume int64) int64 {
	cap64 := int64(ev.participation() * float64(observedVolume))
	if units < cap64 {
		return units
	}
	return cap64
}

// slipBuy / slipSell model crossing part of the spread to actually fill.
func (ev *Evaluator) slipBuy(p int64) int64  { return int64(float64(p) * (1 + ev.slippage())) }
func (ev *Evaluator) slipSell(p int64) int64 { return int64(float64(p) * (1 - ev.slippage())) }

// killBreached applies the model's stop: a kill above entry means "price rose
// too far", below means "fell too far"; breach = crossed away from entry.
func killBreached(st store.Strategy, snap *Snap) bool {
	if st.KillPrice == nil {
		return false
	}
	ref := snap.High
	if ref == nil {
		ref = snap.Low
	}
	if ref == nil {
		return false
	}
	if *st.KillPrice >= st.EntryPrice {
		return *ref >= *st.KillPrice
	}
	return *ref <= *st.KillPrice
}

func legsFresh(snap *Snap, maxAgeS int) bool {
	return snap.HighAgeS != nil && snap.LowAgeS != nil &&
		*snap.HighAgeS <= maxAgeS && *snap.LowAgeS <= maxAgeS
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

func baseEvaluation(st store.Strategy, at time.Time, snap *Snap) store.Evaluation {
	e := store.Evaluation{StrategyID: st.StrategyID, At: at}
	if snap != nil {
		e.CurHigh, e.CurLow, e.CurMargin = snap.High, snap.Low, snap.Margin
		e.HighAgeS, e.LowAgeS = snap.HighAgeS, snap.LowAgeS
		v := snap.Vol30m
		e.Vol30m = &v
	}
	return e
}

func finishEvaluation(e *store.Evaluation, checks map[string]bool, verdict string, detail map[string]any) {
	e.Checks, _ = json.Marshal(checks)
	e.Verdict = verdict
	if detail != nil {
		e.Detail, _ = json.Marshal(detail)
	}
}

func sprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }
func itoa(n int) string                         { return fmt.Sprintf("%d", n) }
func join(ss []string) string                   { return strings.Join(ss, ", ") }
