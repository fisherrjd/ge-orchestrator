package store

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

// TestSidecarRoundTrip inserts one strategy per kind through the real ingest
// path and scans them back, asserting the structured fields survive. Needs a
// migrated scratch DB: set STORE_TEST_DSN (see the Makefile-less invocation
// in the repo docs); skipped otherwise so `go test ./...` stays hermetic.
func TestSidecarRoundTrip(t *testing.T) {
	dsn := os.Getenv("STORE_TEST_DSN")
	if dsn == "" {
		t.Skip("STORE_TEST_DSN not set")
	}
	ctx := context.Background()
	s, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Pool.Close()

	runID, err := s.CreateRun(ctx, json.RawMessage(`{}`), "test brief")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Pool.Exec(ctx, `DELETE FROM orchestrator.evaluations WHERE strategy_id IN
		(SELECT strategy_id FROM orchestrator.strategies WHERE run_id=$1);
		DELETE FROM orchestrator.strategies WHERE run_id=$1`, runID)

	raw := `{
	  "run_started_at": "2026-07-14T12:00:00Z", "report_path": "x.md",
	  "strategies": [
	    {"id":"S-yew-logs-20260714","archetype":"S","title":"t","thesis":"t",
	     "items":[{"name":"Yew logs","id":1515}],"entry":"e","exit":"x",
	     "entry_price":240,"exit_price":265,"kill_price":210,"horizon":"h",
	     "capital_required":1,"size":{"buy_limit":1,"vol_constrained":1,"units_used":1},
	     "expected_value":{"per_cycle_gp":1,"per_1h_gp":1,"per_day_gp":1,"roi_pct":1},
	     "confidence":"medium","invalidation":"i",
	     "buy_window":{"from_how":50,"to_how":53},"sell_window":{"from_how":162,"to_how":165}},
	    {"id":"V-scales-20260714","archetype":"V","title":"t","thesis":"t",
	     "items":[{"name":"Zulrah's scales","id":12934}],"entry":"e","exit":"x",
	     "entry_price":95,"exit_price":120,"kill_price":80,"horizon":"h",
	     "capital_required":1,"size":{"buy_limit":1,"vol_constrained":1,"units_used":1},
	     "expected_value":{"per_cycle_gp":1,"per_1h_gp":1,"per_day_gp":1,"roi_pct":1},
	     "confidence":"low","invalidation":"i",
	     "trigger":{"metric":"volume_zscore","threshold":4,"direction":"above","window":"1h"},
	     "direction":"ride","eval_window_hours":96},
	    {"id":"C-prayer-decant-20260714","archetype":"C","title":"t","thesis":"t",
	     "items":[{"name":"Prayer potion(3)","id":139},{"name":"Prayer potion(4)","id":2434}],
	     "entry":"e","exit":"x","entry_price":27600,"exit_price":27930,"horizon":"h",
	     "capital_required":1,"size":{"buy_limit":1,"vol_constrained":1,"units_used":1},
	     "expected_value":{"per_cycle_gp":1,"per_1h_gp":1,"per_day_gp":1,"roi_pct":1},
	     "confidence":"high","invalidation":"i",
	     "legs":[{"item_id":139,"name":"Prayer potion(3)","side":"buy","qty":4,"price":6900},
	             {"item_id":2434,"name":"Prayer potion(4)","side":"sell","qty":3,"price":9500}],
	     "relation_id":1},
	    {"id":"U-bones-20260714","archetype":"U","title":"t","thesis":"t",
	     "items":[{"name":"Dragon bones","id":536}],"entry":"e","exit":"x",
	     "entry_price":2500,"exit_price":3000,"kill_price":2200,"horizon":"h",
	     "capital_required":1,"size":{"buy_limit":1,"vol_constrained":1,"units_used":1},
	     "expected_value":{"per_cycle_gp":1,"per_1h_gp":1,"per_day_gp":1,"roi_pct":1},
	     "confidence":"low","invalidation":"i",
	     "event":{"date":"2026-07-15","description":"update"},"direction":"ride"},
	    {"id":"H-scim-20260714","archetype":"H","title":"t","thesis":"t",
	     "items":[{"name":"Rune scimitar","id":1333}],"entry":"e","exit":"x",
	     "entry_price":14800,"exit_price":15900,"kill_price":13500,"horizon":"h",
	     "capital_required":1,"size":{"buy_limit":1,"vol_constrained":1,"units_used":1},
	     "expected_value":{"per_cycle_gp":1,"per_1h_gp":1,"per_day_gp":1,"roi_pct":1},
	     "confidence":"medium","invalidation":"i","eval_window_hours":336}
	  ],
	  "signal_verdicts":[{"signal_id":1,"verdict":"dismissed","reason":"test"}]
	}`
	var sc Sidecar
	if err := json.Unmarshal([]byte(raw), &sc); err != nil {
		t.Fatal(err)
	}
	if len(sc.SignalVerdicts) != 1 {
		t.Fatal("signal_verdicts should parse")
	}
	if err := s.InsertStrategies(ctx, runID, time.Now().UTC(), sc.Strategies, nil); err != nil {
		t.Fatal(err)
	}

	got, err := s.StrategiesForRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("want 5 strategies, got %d", len(got))
	}
	byKind := map[string]Strategy{}
	for _, st := range got {
		byKind[st.Archetype] = st
	}

	if s := byKind["S"]; s.BuyWindow == nil || s.BuyWindow.FromHow != 50 ||
		s.SellWindow == nil || s.SellWindow.ToHow != 165 ||
		s.EvalWindow != 168*time.Hour || s.State != "open" {
		t.Fatalf("S round trip: %+v", s)
	}
	if v := byKind["V"]; v.State != "armed" || v.Trigger == nil ||
		v.Trigger.Threshold != 4 || v.Direction == nil || *v.Direction != "ride" ||
		v.EvalWindow != 96*time.Hour {
		t.Fatalf("V round trip: %+v", v)
	}
	if c := byKind["C"]; len(c.Legs) != 2 || c.PrimaryItemID != 2434 ||
		c.RelationID == nil || *c.RelationID != 1 {
		t.Fatalf("C round trip: primary=%d legs=%v", c.PrimaryItemID, c.Legs)
	}
	if u := byKind["U"]; u.Event == nil || u.Event.Date != "2026-07-15" ||
		u.EvalWindow != 72*time.Hour {
		t.Fatalf("U round trip: %+v", u)
	}
	if h := byKind["H"]; h.EvalWindow != 336*time.Hour || h.State != "open" {
		t.Fatalf("H round trip: %+v", h)
	}
}
