// Package api is the orchestrator's HTTP surface (JSON + SSE), consumed by
// ge-dashboard. Localhost-only by default; the dashboard is the public face.
package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/osrs-ge/ge-orchestrator/internal/brief"
	"github.com/osrs-ge/ge-orchestrator/internal/eval"
	"github.com/osrs-ge/ge-orchestrator/internal/runner"
	"github.com/osrs-ge/ge-orchestrator/internal/store"
)

type Server struct {
	Store     *store.Store
	Runner    *runner.Runner
	Evaluator *eval.Evaluator
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.health)
	mux.HandleFunc("GET /api/runs", s.listRuns)
	mux.HandleFunc("POST /api/runs", s.triggerRun)
	mux.HandleFunc("GET /api/runs/{id}", s.getRun)
	mux.HandleFunc("GET /api/runs/{id}/report", s.getReport)
	mux.HandleFunc("GET /api/runs/{id}/events", s.streamEvents)
	mux.HandleFunc("GET /api/strategies", s.listStrategies)
	mux.HandleFunc("GET /api/strategies/{id}", s.getStrategy)
	mux.HandleFunc("GET /api/scoreboard", s.scoreboard)
	mux.HandleFunc("GET /api/pnl", s.pnl)
	mux.HandleFunc("GET /api/watchlist", s.listWatchlist)
	mux.HandleFunc("POST /api/watchlist", s.addWatch)
	mux.HandleFunc("DELETE /api/watchlist/{id}", s.retireWatch)
	mux.HandleFunc("GET /api/brief/preview", s.briefPreview)
	mux.HandleFunc("GET /api/signals", s.listSignals)
	mux.HandleFunc("GET /api/trends", s.listTrends)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	dbOK := s.Store.Pool.Ping(r.Context()) == nil
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": dbOK, "db": dbOK, "active_run_id": s.Runner.ActiveRunID(),
	})
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 200 {
			limit = n
		}
	}
	runs, err := s.Store.Runs(r.Context(), limit)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, runs)
}

func (s *Server) triggerRun(w http.ResponseWriter, r *http.Request) {
	p := brief.Defaults()
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		writeErr(w, 400, "bad brief body: "+err.Error())
		return
	}
	if err := p.Validate(); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	runID, err := s.Runner.Trigger(r.Context(), p)
	if err == runner.ErrBusy {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "run in progress", "active_run_id": runID})
		return
	}
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"run_id": runID})
}

func (s *Server) runID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeErr(w, 400, "bad id")
		return 0, false
	}
	return id, true
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	id, ok := s.runID(w, r)
	if !ok {
		return
	}
	run, err := s.Store.Run(r.Context(), id)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if run == nil {
		writeErr(w, 404, "no such run")
		return
	}
	strategies, err := s.Store.StrategiesForRun(r.Context(), id)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"run": run, "strategies": strategies})
}

func (s *Server) getReport(w http.ResponseWriter, r *http.Request) {
	id, ok := s.runID(w, r)
	if !ok {
		return
	}
	md, err := s.Store.ReportMarkdown(r.Context(), id)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if md == "" {
		writeErr(w, 404, "no report for this run")
		return
	}
	w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
	w.Write([]byte(md))
}

// streamEvents is the SSE feed for a run, with Last-Event-ID replay.
func (s *Server) streamEvents(w http.ResponseWriter, r *http.Request) {
	id, ok := s.runID(w, r)
	if !ok {
		return
	}
	lastID := 0
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		lastID, _ = strconv.Atoi(v)
	}

	flusher, canFlush := w.(http.Flusher)
	if !canFlush {
		writeErr(w, 500, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	send := func(e runner.Event) {
		fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", e.ID, e.Type, e.Marshal())
	}

	replay, live := s.Runner.Hub.Subscribe(id, lastID)
	for _, e := range replay {
		send(e)
	}
	flusher.Flush()
	if live == nil {
		return // run already finished; replay was everything
	}
	defer s.Runner.Hub.Unsubscribe(id, live)

	heartbeat := time.NewTicker(25 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case e, open := <-live:
			if !open {
				return
			}
			send(e)
			flusher.Flush()
		case <-heartbeat.C:
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) listStrategies(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var (
		list []store.Strategy
		err  error
	)
	switch q.Get("scope") {
	case "", "latest_run":
		list, err = s.Store.LatestRunStrategies(r.Context())
	case "open":
		list, err = s.Store.EvaluableStrategies(r.Context()) // open + armed
	default:
		writeErr(w, 400, "scope must be latest_run or open")
		return
	}
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	type liveStrategy struct {
		store.Strategy
		LiveChecks  map[string]bool `json:"live_checks,omitempty"`
		LiveVerdict string          `json:"live_verdict,omitempty"`
		Live        *store.Evaluation `json:"live,omitempty"`
	}
	out := make([]liveStrategy, 0, len(list))
	for _, st := range list {
		ls := liveStrategy{Strategy: st}
		if q.Get("live") == "1" {
			e, checks, err := s.Evaluator.Compute(r.Context(), st)
			if err != nil {
				log.Printf("api: live check %d: %v", st.StrategyID, err)
			} else {
				ls.LiveChecks, ls.LiveVerdict, ls.Live = checks, e.Verdict, &e
			}
		}
		out = append(out, ls)
	}
	writeJSON(w, 200, out)
}

func (s *Server) getStrategy(w http.ResponseWriter, r *http.Request) {
	id, ok := s.runID(w, r)
	if !ok {
		return
	}
	st, err := s.Store.StrategyByID(r.Context(), id)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if st == nil {
		writeErr(w, 404, "no such strategy")
		return
	}
	evals, err := s.Store.Evaluations(r.Context(), id, 600)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"strategy": st, "evaluations": evals})
}

func (s *Server) scoreboard(w http.ResponseWriter, r *http.Request) {
	rows, err := s.Store.Scoreboard(r.Context())
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, rows)
}

// pnl is the proof-of-work view: what following every paper-traded strategy
// would have printed, rolled up. Estimates inherit the evaluator's haircut
// and are upper bounds — but they are the honest answer to "is the research
// making money on paper yet".
func (s *Server) pnl(w http.ResponseWriter, r *http.Request) {
	rows, err := s.Store.PnL(r.Context())
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	type bucket struct {
		N             int   `json:"n"`
		EstRealizedGp int64 `json:"est_realized_gp"`
		ProjectedGp   int64 `json:"projected_gp"`
	}
	total := bucket{}
	byState := map[string]*bucket{}
	byArchetype := map[string]*bucket{}
	add := func(m map[string]*bucket, key string, row store.PnLRow) {
		b := m[key]
		if b == nil {
			b = &bucket{}
			m[key] = b
		}
		b.N++
		if row.EstRealizedGp != nil {
			b.EstRealizedGp += *row.EstRealizedGp
		}
		if row.ProjectedGp != nil {
			b.ProjectedGp += *row.ProjectedGp
		}
	}
	for _, row := range rows {
		total.N++
		if row.EstRealizedGp != nil {
			total.EstRealizedGp += *row.EstRealizedGp
		}
		if row.ProjectedGp != nil {
			total.ProjectedGp += *row.ProjectedGp
		}
		add(byState, row.State, row)
		add(byArchetype, row.Archetype, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		vi, vj := int64(0), int64(0)
		if rows[i].EstRealizedGp != nil {
			vi = *rows[i].EstRealizedGp
		}
		if rows[j].EstRealizedGp != nil {
			vj = *rows[j].EstRealizedGp
		}
		return vi > vj
	})

	writeJSON(w, 200, map[string]any{
		"as_of":        time.Now().UTC(),
		"total":        total,
		"by_state":     byState,
		"by_archetype": byArchetype,
		"strategies":   rows,
	})
}

func (s *Server) briefPreview(w http.ResponseWriter, r *http.Request) {
	p := brief.Defaults()
	if v := r.URL.Query().Get("params"); v != "" {
		if err := json.Unmarshal([]byte(v), &p); err != nil {
			writeErr(w, 400, "bad params: "+err.Error())
			return
		}
	}
	if err := p.Validate(); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	// Preview with the signals a real trigger would assign right now.
	pending, err := s.Store.PendingSignals(r.Context(), 10)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	text, err := brief.Render(r.Context(), s.Store, p, time.Now().UTC(), pending)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"brief_text": text})
}

func (s *Server) listWatchlist(w http.ResponseWriter, r *http.Request) {
	list, err := s.Store.WatchRanked(r.Context(), 100)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, list)
}

// addWatch is the operator's entry point: "this item is good, keep it on the
// stack." Accepts an item id or an exact (case-insensitive) name; archetype
// optional (empty = the idea isn't tied to one kind).
func (s *Server) addWatch(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ItemID    int    `json:"item_id"`
		Name      string `json:"name"`
		Archetype string `json:"archetype"`
		Note      string `json:"note"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad body: "+err.Error())
		return
	}
	switch body.Archetype {
	case "", "S", "V", "C", "U", "H":
	default:
		writeErr(w, 400, "archetype must be S, V, C, U, H or empty")
		return
	}
	var (
		itemID int
		name   string
		err    error
	)
	switch {
	case body.ItemID != 0:
		itemID = body.ItemID
		if name, err = s.Store.ItemName(r.Context(), itemID); err != nil {
			writeErr(w, 404, fmt.Sprintf("unknown item_id %d", itemID))
			return
		}
	case body.Name != "":
		if itemID, name, err = s.Store.LookupItem(r.Context(), body.Name); err != nil {
			writeErr(w, 404, fmt.Sprintf("no item named %q (exact match required)", body.Name))
			return
		}
	default:
		writeErr(w, 400, "item_id or name required")
		return
	}
	var archetype, note *string
	if body.Archetype != "" {
		archetype = &body.Archetype
	}
	if body.Note != "" {
		note = &body.Note
	}
	id, err := s.Store.UpsertWatch(r.Context(), itemID, name, archetype, note, "operator")
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"watch_id": id, "item_id": itemID, "item_name": name})
}

func (s *Server) retireWatch(w http.ResponseWriter, r *http.Request) {
	id, ok := s.runID(w, r)
	if !ok {
		return
	}
	if err := s.Store.RetireWatch(r.Context(), id); err != nil {
		writeErr(w, 404, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"retired": id})
}

func (s *Server) listSignals(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	list, err := s.Store.Signals(r.Context(), limit)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, list)
}

func (s *Server) listTrends(w http.ResponseWriter, r *http.Request) {
	lens := r.URL.Query().Get("lens")
	switch lens {
	case "seasonal", "volume", "band", "flip":
	default:
		writeErr(w, 400, "lens must be seasonal, volume, band or flip")
		return
	}
	list, err := s.Store.LatestTrends(r.Context(), lens, 25)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, list)
}
