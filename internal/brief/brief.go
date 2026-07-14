// Package brief renders the per-run brief: user constraints + the harness's
// own paper-trade track record. The rendered text is stored on the run and
// shown by the preview endpoint, so the human always sees exactly what the
// model will see. The feedback loop closes here with zero LLM involvement.
package brief

import (
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
	Archetypes    map[string]float64 `json:"archetypes"` // A..F -> weight 0..2
	Notes         string             `json:"notes"`
}

func Defaults() Params {
	return Params{
		CapitalGp: 100_000_000, Risk: "medium", MinConfidence: "medium",
		Archetypes: map[string]float64{"A": 1, "B": 1, "C": 1, "D": 1, "E": 1, "F": 1},
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
		if len(k) != 1 || k[0] < 'A' || k[0] > 'F' {
			return fmt.Errorf("archetypes: unknown archetype %q", k)
		}
		if w < 0 || w > 2 {
			return fmt.Errorf("archetypes[%s]: weight must be 0..2", k)
		}
	}
	return nil
}

// Render produces the brief text from params + the current scoreboard.
func Render(ctx context.Context, s *store.Store, p Params, at time.Time) (string, error) {
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
		for _, a := range []string{"A", "B", "C", "D", "E", "F"} {
			if w, ok := p.Archetypes[a]; ok {
				fmt.Fprintf(&b, " %s %.1f,", a, w)
			}
		}
		b.WriteString("\n")
	}

	rows, err := s.Scoreboard(ctx)
	if err != nil {
		return "", err
	}
	if len(rows) > 0 {
		b.WriteString("\n### Track record (paper-traded by the harness — weight your search by it)\n")
		for _, r := range rows {
			closed := r.N - r.Open
			line := fmt.Sprintf("- %s: n=%d (%d still open)", r.Archetype, r.N, r.Open)
			if closed > 0 {
				surv := float64(r.Confirmed) / float64(closed) * 100
				line += fmt.Sprintf(", %.0f%% of closed confirmed", surv)
			}
			if r.RealizedVsProjected != nil {
				line += fmt.Sprintf(", realized/projected %.2f", *r.RealizedVsProjected)
				switch {
				case r.N-r.Open < 5:
					line += " — insufficient sample, treat as unproven."
				case *r.RealizedVsProjected >= 0.8:
					line += " — holding up, keep mining."
				case *r.RealizedVsProjected <= 0.5:
					line += " — projections have not held; require stronger persistence evidence before pitching."
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
