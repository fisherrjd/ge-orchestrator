// ge-orchestrator: triggers/schedules ge-agent runs with a rendered brief,
// ingests their structured strategies, paper-trades open strategies against
// the live price tables, and serves the JSON/SSE API the dashboard consumes.
package main

import (
	"bufio"
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/osrs-ge/ge-orchestrator/internal/api"
	"github.com/osrs-ge/ge-orchestrator/internal/brief"
	"github.com/osrs-ge/ge-orchestrator/internal/eval"
	"github.com/osrs-ge/ge-orchestrator/internal/runner"
	"github.com/osrs-ge/ge-orchestrator/internal/store"
)

func main() {
	log.SetPrefix("ge-orchestrator: ")
	ctx := context.Background()

	dsn := os.Getenv("GE_ORCH_DSN")
	if dsn == "" {
		log.Fatal("GE_ORCH_DSN not set")
	}
	st, err := store.New(ctx, dsn)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer st.Pool.Close()
	if n, err := st.OrphanRunningRuns(ctx); err != nil {
		log.Fatalf("orphan recovery: %v", err)
	} else if n > 0 {
		log.Printf("marked %d orphaned run(s) failed", n)
	}

	apiKey := os.Getenv("MINIMAX_API_KEY")
	if apiKey == "" {
		apiKey = keyFromDotEnv(".env", "minimax")
	}
	r := &runner.Runner{
		Cfg: runner.Config{
			AgentPath: mustEnv("GE_AGENT_PATH"),
			McpPath:   mustEnv("GE_MCP_PATH"),
			McpDSN:    mustEnv("GE_MCP_DSN"),
			APIKey:    apiKey,
			Directive: mustEnv("GE_AGENT_DIRECTIVE"),
			StateDir:  getenv("GE_ORCH_STATE", "state"),
		},
		Store: st,
		Hub:   runner.NewHub(),
	}
	ev := &eval.Evaluator{Store: st}

	// Evaluation ticker (+ optional empty-portfolio auto-trigger).
	evalEvery := durEnv("GE_ORCH_EVAL_INTERVAL", 5*time.Minute)
	emptyCooldown := durEnv("GE_ORCH_TRIGGER_ON_EMPTY", 0) // 0 = disabled
	go func() {
		t := time.NewTicker(evalEvery)
		defer t.Stop()
		for range t.C {
			ev.Tick(ctx)
			if emptyCooldown > 0 {
				maybeTriggerOnEmpty(ctx, st, r, emptyCooldown)
			}
		}
	}()
	log.Printf("evaluator: every %s (trigger-on-empty cooldown: %s)", evalEvery, emptyCooldown)

	// Optional run schedule.
	if sched := os.Getenv("GE_ORCH_SCHEDULE"); sched != "" {
		every, err := time.ParseDuration(sched)
		if err != nil {
			log.Fatalf("GE_ORCH_SCHEDULE: %v", err)
		}
		go func() {
			t := time.NewTicker(every)
			defer t.Stop()
			for range t.C {
				if _, err := r.Trigger(ctx, brief.Defaults()); err == runner.ErrBusy {
					log.Print("schedule: skipped, run in progress")
				} else if err != nil {
					log.Printf("schedule: trigger failed: %v", err)
				}
			}
		}()
		log.Printf("schedule: every %s", every)
	}

	srv := &api.Server{Store: st, Runner: r, Evaluator: ev}
	addr := getenv("GE_ORCH_ADDR", "127.0.0.1:8410")
	log.Printf("listening on %s", addr)
	if err := http.ListenAndServe(addr, srv.Handler()); err != nil {
		log.Fatal(err)
	}
}

// maybeTriggerOnEmpty starts a new research cycle when the portfolio has gone
// empty — the natural re-research moment: nothing left to trade, and the
// scoreboard has just absorbed whatever killed the last strategies. The
// cooldown is anchored to the last run's start time in the DB, so restarts
// don't re-trigger and manual runs count toward it. In a market where every
// strategy dies in minutes, this converges to one run per cooldown period.
func maybeTriggerOnEmpty(ctx context.Context, st *store.Store, r *runner.Runner, cooldown time.Duration) {
	if r.ActiveRunID() != 0 {
		return
	}
	open, err := st.OpenCount(ctx)
	if err != nil || open > 0 {
		return
	}
	last, err := st.LastRunStart(ctx)
	if err != nil {
		log.Printf("trigger-on-empty: %v", err)
		return
	}
	if last != nil && time.Since(*last) < cooldown {
		return
	}
	runID, err := r.Trigger(ctx, brief.Defaults())
	if err == runner.ErrBusy {
		return
	}
	if err != nil {
		log.Printf("trigger-on-empty: %v", err)
		return
	}
	log.Printf("trigger-on-empty: portfolio empty, started run %d", runID)
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("%s not set", key)
	}
	return v
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func durEnv(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		log.Fatalf("%s: bad duration %q", key, v)
	}
	return def
}

func keyFromDotEnv(path, key string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(strings.TrimSpace(sc.Text()), key+"="); ok {
			return strings.Trim(strings.TrimSpace(v), `"'`)
		}
	}
	return ""
}
