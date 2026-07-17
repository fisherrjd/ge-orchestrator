// Package runner owns ge-agent execution: one run at a time, per-run
// workspace, brief delivery, stderr -> SSE events, sidecar ingestion.
package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/osrs-ge/ge-orchestrator/internal/brief"
	"github.com/osrs-ge/ge-orchestrator/internal/eval"
	"github.com/osrs-ge/ge-orchestrator/internal/store"
)

type Config struct {
	AgentPath  string // GE_AGENT_PATH
	McpPath    string // GE_MCP_PATH
	McpDSN     string // GE_MCP_DSN
	APIKey     string // MINIMAX_API_KEY passthrough
	Directive  string // GE_AGENT_DIRECTIVE (absolute path)
	StateDir   string // GE_ORCH_STATE — run workspaces live under here
	ExtraEnv   []string
}

type Runner struct {
	Cfg    Config
	Store  *store.Store
	Hub    *Hub
	Prices eval.PriceSource // ship-time vetting; nil skips the kill-price rule

	mu     sync.Mutex
	active *activeRun
}

type activeRun struct {
	runID  int64
	params brief.Params
	done   chan struct{}
}

// ActiveRunID returns the in-flight run id, or 0.
func (r *Runner) ActiveRunID() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active == nil {
		return 0
	}
	return r.active.runID
}

// Trigger starts a run with the given brief params. Returns the run id, or
// ErrBusy if one is already in flight.
var ErrBusy = fmt.Errorf("a run is already in progress")

func (r *Runner) Trigger(ctx context.Context, p brief.Params) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active != nil {
		return r.active.runID, ErrBusy
	}

	// Pull the oldest pending signals into this run's brief — the work-queue
	// mechanism that makes consecutive runs investigate different candidates.
	assigned, err := r.Store.PendingSignals(ctx, 10)
	if err != nil {
		return 0, err
	}
	briefText, err := brief.Render(ctx, r.Store, p, time.Now().UTC(), assigned)
	if err != nil {
		return 0, err
	}
	runID, err := r.Store.CreateRun(ctx, brief.MarshalParams(p), briefText)
	if err != nil {
		return 0, err
	}
	if len(assigned) > 0 {
		ids := make([]int64, len(assigned))
		for i, sig := range assigned {
			ids[i] = sig.SignalID
		}
		if err := r.Store.AssignSignals(ctx, runID, ids); err != nil {
			return 0, err
		}
	}

	workspace := filepath.Join(r.Cfg.StateDir, "runs", fmt.Sprintf("%d", runID))
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		return 0, err
	}
	briefPath := filepath.Join(workspace, "brief.md")
	if err := os.WriteFile(briefPath, []byte(briefText), 0o644); err != nil {
		return 0, err
	}

	run := &activeRun{runID: runID, params: p, done: make(chan struct{})}
	r.active = run
	go r.execute(runID, workspace, briefPath, run)
	return runID, nil
}

// execute runs the child to completion (detached from the trigger ctx — an
// HTTP disconnect must not kill a research run).
func (r *Runner) execute(runID int64, workspace, briefPath string, run *activeRun) {
	defer func() {
		r.mu.Lock()
		r.active = nil
		r.mu.Unlock()
		close(run.done)
	}()
	ctx := context.Background()

	cmd := exec.Command(r.Cfg.AgentPath)
	cmd.Dir = workspace
	cmd.Env = append([]string{
		"MINIMAX_API_KEY=" + r.Cfg.APIKey,
		"GE_MCP_PATH=" + r.Cfg.McpPath,
		"GE_MCP_DSN=" + r.Cfg.McpDSN,
		"GE_AGENT_DIRECTIVE=" + r.Cfg.Directive,
		"GE_AGENT_BRIEF_FILE=" + briefPath,
	}, r.Cfg.ExtraEnv...)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		r.fail(ctx, runID, "stderr pipe: "+err.Error())
		return
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		r.fail(ctx, runID, "stdout pipe: "+err.Error())
		return
	}
	if err := cmd.Start(); err != nil {
		r.fail(ctx, runID, "start ge-agent: "+err.Error())
		return
	}
	r.Hub.Publish(runID, Event{Type: "started", Data: map[string]any{"run_id": runID}})

	// stderr -> events
	var lastLines []string
	go func() {
		sc := bufio.NewScanner(stderr)
		sc.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
		for sc.Scan() {
			line := sc.Text()
			lastLines = append(lastLines, line)
			if len(lastLines) > 20 {
				lastLines = lastLines[1:]
			}
			r.Hub.Publish(runID, parseLine(line))
		}
	}()

	var stdoutBuf strings.Builder
	scOut := bufio.NewScanner(stdout)
	for scOut.Scan() {
		stdoutBuf.WriteString(scOut.Text() + "\n")
	}

	err = cmd.Wait()
	reportPath := strings.TrimSpace(stdoutBuf.String())
	if err != nil || reportPath == "" {
		reason := "ge-agent exited without a report"
		if err != nil {
			reason = fmt.Sprintf("ge-agent failed: %v; tail: %s", err, strings.Join(lastLines, " | "))
		}
		r.fail(ctx, runID, reason)
		return
	}

	if err := r.ingest(ctx, runID, run.params, workspace, reportPath); err != nil {
		r.fail(ctx, runID, "ingest: "+err.Error())
		return
	}
	r.Hub.Publish(runID, Event{Type: "finished", Data: map[string]any{"run_id": runID, "status": "succeeded"}})
	log.Printf("run %d succeeded: %s", runID, reportPath)
}

// ingest stores the report markdown + parses the sidecar strategies.
func (r *Runner) ingest(ctx context.Context, runID int64, p brief.Params, workspace, reportPath string) error {
	full := reportPath
	if !filepath.IsAbs(full) {
		full = filepath.Join(workspace, reportPath)
	}
	md, err := os.ReadFile(full)
	if err != nil {
		return fmt.Errorf("read report: %w", err)
	}
	scPath := strings.TrimSuffix(full, ".md") + ".strategies.json"
	raw, err := os.ReadFile(scPath)
	if err != nil {
		return fmt.Errorf("read sidecar: %w", err)
	}
	var sc store.Sidecar
	if err := json.Unmarshal(raw, &sc); err != nil {
		return fmt.Errorf("parse sidecar: %w", err)
	}
	now := time.Now().UTC()
	accepted, vetoed := r.vet(ctx, p, sc.Strategies)
	for _, v := range vetoed {
		log.Printf("run %d: strategy %s %s", runID, v.Strategy.ID, v.Reason)
		r.Hub.Publish(runID, Event{Type: "vetoed", Data: map[string]any{"sid": v.Strategy.ID, "reason": v.Reason}})
	}
	if err := r.Store.InsertStrategies(ctx, runID, now, accepted, vetoed); err != nil {
		return err
	}
	// Apply the run's verdicts on its assigned signals, then return any it
	// ignored to the queue (a verdict-less signal must not rot as 'assigned').
	for _, v := range sc.SignalVerdicts {
		if err := r.Store.ResolveSignal(ctx, int64(v.SignalID), v.Verdict, v.Reason); err != nil {
			log.Printf("run %d: resolve signal %d: %v", runID, v.SignalID, err)
		}
	}
	if err := r.Store.ReleaseRunSignals(ctx, runID); err != nil {
		log.Printf("run %d: release unanswered signals: %v", runID, err)
	}
	return r.Store.FinishRun(ctx, runID, "succeeded", full, string(md), "")
}

func (r *Runner) fail(ctx context.Context, runID int64, reason string) {
	log.Printf("run %d failed: %s", runID, reason)
	if err := r.Store.ReleaseRunSignals(ctx, runID); err != nil {
		log.Printf("run %d: release signals: %v", runID, err)
	}
	if err := r.Store.FinishRun(ctx, runID, "failed", "", "", reason); err != nil {
		log.Printf("run %d: record failure: %v", runID, err)
	}
	r.Hub.Publish(runID, Event{Type: "finished", Data: map[string]any{"run_id": runID, "status": "failed", "reason": reason}})
}

// --- stderr line parsing (the loop's known formats; fallback = log event) ---

var (
	turnRe   = regexp.MustCompile(`turn (\d+): stop=(\S+) in=(\d+) out=(\d+)`)
	toolRe   = regexp.MustCompile(`\s+tool (\S+) \(err=(\w+), (\d+) bytes\)`)
	acceptRe = regexp.MustCompile(`report (accepted|rejected): (.*)`)
)

func parseLine(line string) Event {
	if m := turnRe.FindStringSubmatch(line); m != nil {
		return Event{Type: "turn", Data: map[string]any{"n": m[1], "stop": m[2], "in": m[3], "out": m[4]}}
	}
	if m := toolRe.FindStringSubmatch(line); m != nil {
		return Event{Type: "tool", Data: map[string]any{"name": m[1], "err": m[2] == "true", "bytes": m[3]}}
	}
	if m := acceptRe.FindStringSubmatch(line); m != nil {
		return Event{Type: "report_" + m[1], Data: map[string]any{"detail": m[2]}}
	}
	return Event{Type: "log", Data: map[string]any{"line": line}}
}
