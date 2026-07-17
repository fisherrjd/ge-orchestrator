// Package brief renders the per-run brief: user constraints + the harness's
// own paper-trade track record + the collector's assigned signals and latest
// market sweep. The rendered text is stored on the run and shown by the
// preview endpoint, so the human always sees exactly what the model will see.
// The feedback loop closes here with zero LLM involvement.
package brief

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/osrs-ge/ge-orchestrator/internal/store"
)

// Params is the structured brief body (stored as runs.brief).
type Params struct {
	CapitalGp     int64              `json:"capital_gp"`
	Risk          string             `json:"risk"` // low | medium | high
	Members       *bool              `json:"members"`
	MinConfidence string             `json:"min_confidence"`
	Archetypes    map[string]float64 `json:"archetypes"` // S/V/C/U/H -> weight 0..2
	Notes         string             `json:"notes"`
}

var archetypeOrder = []string{"S", "V", "C", "U", "H"}

var archetypeNames = map[string]string{
	"S": "seasonal window", "V": "volume anomaly", "C": "conversion",
	"U": "update/event", "H": "swing hold",
}

func Defaults() Params {
	return Params{
		// The operator runs low-touch on a 10-30M bankroll and wants reliable
		// gp/day: repeatable mechanical edges (S/C/H) over variance plays (V/U).
		CapitalGp: 25_000_000, Risk: "low", MinConfidence: "medium",
		Archetypes: map[string]float64{"S": 1.5, "V": 0.5, "C": 1.2, "U": 0.5, "H": 1},
	}
}

func (p *Params) Validate() error {
	switch p.Risk {
	case "low", "medium", "high":
	default:
		return fmt.Errorf("risk must be low|medium|high")
	}
	switch p.MinConfidence {
	case "high", "medium", "low", "insufficient_history":
	default:
		return fmt.Errorf("min_confidence must be a confidence level")
	}
	if p.CapitalGp <= 0 {
		return fmt.Errorf("capital_gp must be positive")
	}
	for k, w := range p.Archetypes {
		if _, ok := archetypeNames[k]; !ok {
			return fmt.Errorf("archetypes: unknown archetype %q (S/V/C/U/H)", k)
		}
		if w < 0 || w > 2 {
			return fmt.Errorf("archetypes[%s]: weight must be 0..2", k)
		}
	}
	return nil
}

// Render produces the brief text from params + the current scoreboard +
// the signals assigned to this run + the latest market sweep.
func Render(ctx context.Context, s *store.Store, p Params, at time.Time, assigned []store.Signal) (string, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "_Generated %s UTC._\n\n", at.Format("2006-01-02 15:04"))

	b.WriteString("### Constraints\n")
	fmt.Fprintf(&b, "- Capital available: %s gp total. Do NOT ship any strategy whose capital_required exceeds this.\n", group(p.CapitalGp))
	fmt.Fprintf(&b, "- Risk appetite: %s.", p.Risk)
	if p.Members != nil {
		if *p.Members {
			b.WriteString(" Members items: allowed.")
		} else {
			b.WriteString(" F2P items only (members=false).")
		}
	}
	b.WriteString("\n")
	fmt.Fprintf(&b, "- Minimum confidence to ship a strategy: %s.\n", p.MinConfidence)
	if len(p.Archetypes) > 0 {
		b.WriteString("- Archetype weights (0 = do not pitch it at all; >1 = favor):")
		for _, a := range archetypeOrder {
			if w, ok := p.Archetypes[a]; ok {
				fmt.Fprintf(&b, " %s(%s) %.1f,", a, archetypeNames[a], w)
			}
		}
		b.WriteString("\n")
	}
	b.WriteString("- Archetype S is the baseline: ship at least one S strategy or document in Discarded why every seasonal candidate failed falsification.\n")
	b.WriteString("- Execution is LOW-TOUCH: the operator places offers at most twice a day. Every strategy must work with offers placed in advance and checked on that cadence — nothing that needs a fast reaction.\n")
	b.WriteString("- Objective: reliable gp/day. Prefer the boring, repeatable, high-confidence edge over the bigger speculative one; ship fewer, stronger strategies.\n")

	writeOpenBook(ctx, &b, s, p)
	writeWatchlist(ctx, &b, s)

	if len(assigned) > 0 {
		b.WriteString("\n### Assigned candidates (from the collector's work queue — investigate these FIRST)\n")
		b.WriteString("Every signal below needs a verdict in submit_report's `signal_verdicts`: `shipped` (name the strategy id) or `dismissed` (name the falsification that killed it).\n")
		for _, sig := range assigned {
			fmt.Fprintf(&b, "- signal_id %d [%s] %s (item_id %d): %s\n",
				sig.SignalID, sig.Kind, sig.ItemName, sig.ItemID, compactJSON(sig.Metrics))
		}
	}

	writeSweep(ctx, &b, s)

	rows, err := s.Scoreboard(ctx)
	if err != nil {
		return "", err
	}
	// Only the live letter set goes to the model — retired A-G history stays
	// in the DB/dashboard but would just confuse the directive.
	live := map[string]bool{}
	for _, a := range archetypeOrder {
		live[a] = true
	}
	var liveRows []store.ScoreboardRow
	for _, r := range rows {
		if live[r.Archetype] {
			liveRows = append(liveRows, r)
		}
	}
	if len(liveRows) > 0 {
		b.WriteString("\n### Track record (paper-traded by the harness with a self-impact haircut — weight your search by it)\n")
		for _, r := range liveRows {
			closed := r.N - r.Open - r.Armed
			line := fmt.Sprintf("- %s: n=%d (%d open, %d armed)", r.Archetype, r.N, r.Open, r.Armed)
			if r.Vetoed > 0 {
				line += fmt.Sprintf(", %d vetoed at ship time — check kill_price and capital against a live quote before shipping", r.Vetoed)
			}
			if closed > 0 {
				surv := float64(r.Confirmed) / float64(closed) * 100
				line += fmt.Sprintf(", %.0f%% of closed confirmed", surv)
			}
			if r.RealizedVsProjected != nil {
				line += fmt.Sprintf(", realized/projected %.2f", *r.RealizedVsProjected)
				switch {
				case closed < 5:
					line += " — insufficient sample, treat as unproven."
				case *r.RealizedVsProjected >= 0.8:
					line += " — holding up, keep mining."
				case *r.RealizedVsProjected <= 0.5:
					line += " — projections have not held; require stronger evidence before pitching."
				}
			}
			b.WriteString(line + "\n")
		}
	}

	closedList, err := s.RecentlyClosed(ctx, 8)
	if err != nil {
		return "", err
	}
	if len(closedList) > 0 {
		b.WriteString("\n### Recently killed/expired (do not re-pitch without materially new evidence)\n")
		for _, st := range closedList {
			reason := ""
			if st.StateReason != nil {
				reason = *st.StateReason
			}
			fmt.Fprintf(&b, "- %s [%s] %s: %s\n", st.Title, st.Archetype, st.State, reason)
		}
	}

	if strings.TrimSpace(p.Notes) != "" {
		b.WriteString("\n### Operator notes\n" + strings.TrimSpace(p.Notes) + "\n")
	}
	return b.String(), nil
}

// writeOpenBook appends the live book (open + armed strategies) with its
// committed capital, so the model dedups against it and sizes into the
// remaining bankroll instead of re-committing the same gp every run. The
// ship-time vetter enforces both rules mechanically; telling the model here
// saves it from wasting a strategy slot on a doomed pitch. Best-effort: a
// query failure just omits the section (the vetter still has the last word).
func writeOpenBook(ctx context.Context, b *strings.Builder, s *store.Store, p Params) {
	open, err := s.EvaluableStrategies(ctx)
	if err != nil {
		return
	}
	var committed int64
	for _, st := range open {
		if st.Capital != nil {
			committed += *st.Capital
		}
	}
	remaining := p.CapitalGp - committed
	if remaining < 0 {
		remaining = 0
	}
	b.WriteString("\n### Open book (already paper-trading — dedup and capital rules are enforced at ingest)\n")
	fmt.Fprintf(b, "- Committed capital: %s gp of %s gp; remaining for new strategies: %s gp. A strategy that does not fit the remainder is vetoed.\n",
		group(committed), group(p.CapitalGp), group(remaining))
	if len(open) == 0 {
		b.WriteString("- The book is empty — the full bankroll is available.\n")
		return
	}
	b.WriteString("- Do NOT pitch an item that already has an open/armed strategy of the same archetype — it is vetoed at ingest.\n")
	const maxLines = 20
	for i, st := range open {
		if i == maxLines {
			fmt.Fprintf(b, "- …and %d more open strategies.\n", len(open)-maxLines)
			break
		}
		capital := int64(0)
		if st.Capital != nil {
			capital = *st.Capital
		}
		fmt.Fprintf(b, "- [%s] %s (item_id %d, %s): %s gp committed, opened %s\n",
			st.Archetype, firstItemName(st.Items), st.PrimaryItemID, st.State, group(capital),
			st.OpenedAt.Format("01-02"))
	}
}

// writeWatchlist appends the ranked watch portfolio: ideas that earned their
// place (operator conviction or a confirmed paper-trade) with a score that
// decays unless revalidation keeps re-proving it. Best-effort like the other
// intelligence sections.
func writeWatchlist(ctx context.Context, b *strings.Builder, s *store.Store) {
	watches, err := s.WatchRanked(ctx, 10)
	if err != nil || len(watches) == 0 {
		return
	}
	b.WriteString("\n### Watch portfolio (validated-good ideas, ranked by decayed score — strong hunting ground, but revalidate with live tools; scores decay unless re-confirmed)\n")
	for _, w := range watches {
		arch := "any"
		if w.Archetype != nil {
			arch = *w.Archetype
		}
		line := fmt.Sprintf("- %s (item_id %d, %s, score %.2f, %d/%d confirmed",
			w.ItemName, w.ItemID, arch, w.EffScore, w.TimesConfirmed, w.TimesValidated)
		if w.LastResult != nil {
			line += ", last: " + *w.LastResult
		}
		line += ")"
		if w.Note != nil && strings.TrimSpace(*w.Note) != "" {
			line += " — " + strings.TrimSpace(*w.Note)
		}
		b.WriteString(line + "\n")
	}
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

// writeSweep appends a compact view of the collector's latest sweep so the
// model starts from accumulated intelligence instead of re-scanning cold.
// Best-effort: a missing sweep (fresh install) just omits the section.
func writeSweep(ctx context.Context, b *strings.Builder, s *store.Store) {
	lenses := []struct{ lens, title string }{
		{"seasonal", "hour-of-week amplitude"},
		{"volume", "volume anomalies"},
		{"band", "below 21d band"},
	}
	wrote := false
	for _, l := range lenses {
		rows, err := s.LatestTrends(ctx, l.lens, 5)
		if err != nil || len(rows) == 0 {
			continue
		}
		if !wrote {
			b.WriteString("\n### Latest market sweep (collector, no-LLM — re-verify anything you act on with live tools)\n")
			wrote = true
		}
		fmt.Fprintf(b, "- %s (as of %s):\n", l.title, rows[0].AsOf.Format("01-02 15:04"))
		for _, r := range rows {
			fmt.Fprintf(b, "  - %s\n", compactJSON(r.Metrics))
		}
	}
}

func compactJSON(raw json.RawMessage) string {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return string(raw)
	}
	s := buf.String()
	if len(s) > 220 {
		s = s[:220] + "…"
	}
	return s
}

func MarshalParams(p Params) json.RawMessage {
	raw, _ := json.Marshal(p)
	return raw
}

// group renders 157464000 as 157,464,000 (display only — never sent as data).
func group(n int64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return string(out)
}
