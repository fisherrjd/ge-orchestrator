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

	// Evaluation ticker.
	evalEvery := durEnv("GE_ORCH_EVAL_INTERVAL", 5*time.Minute)
	go func() {
		t := time.NewTicker(evalEvery)
		defer t.Stop()
		for range t.C {
			ev.Tick(ctx)
		}
	}()
	log.Printf("evaluator: every %s", evalEvery)

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
