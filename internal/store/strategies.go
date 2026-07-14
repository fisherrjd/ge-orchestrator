package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// SidecarStrategy mirrors ge-agent's internal/strategy.Strategy JSON shape
// (kept as a local copy: the wire contract is the sidecar file, not a shared
// Go module — the repos deploy independently).
type SidecarStrategy struct {
	ID        string `json:"id"`
	Archetype string `json:"archetype"`
	Title     string `json:"title"`
	Thesis    string `json:"thesis"`
	Items     []struct {
		Name     string `json:"name"`
		ID       int    `json:"id"`
		BuyLimit *int64 `json:"buy_limit"`
		Members  *bool  `json:"members"`
	} `json:"items"`
	Entry           string `json:"entry"`
	Exit            string `json:"exit"`
	EntryPrice      int64  `json:"entry_price"`
	ExitPrice       int64  `json:"exit_price"`
	KillPrice       *int64 `json:"kill_price"`
	Horizon         string `json:"horizon"`
	CapitalRequired int64  `json:"capital_required"`
	Size            struct {
		BuyLimit       int64 `json:"buy_limit"`
		VolConstrained int64 `json:"vol_constrained"`
		UnitsUsed      int64 `json:"units_used"`
	} `json:"size"`
	ExpectedValue struct {
		PerCycleGp int64   `json:"per_cycle_gp"`
		Per4hGp    int64   `json:"per_4h_gp"`
		PerDayGp   int64   `json:"per_day_gp"`
		RoiPct     float64 `json:"roi_pct"`
	} `json:"expected_value"`
	Confidence    string   `json:"confidence"`
	ConfidenceWhy string   `json:"confidence_why"`
	Evidence      string   `json:"evidence"`
	Invalidation  string   `json:"invalidation"`
	Risks         []string `json:"risks"`
	PaperTrade    string   `json:"paper_trade"`
}

type Sidecar struct {
	RunStartedAt time.Time         `json:"run_started_at"`
	ReportPath   string            `json:"report_path"`
	Strategies   []SidecarStrategy `json:"strategies"`
}

// InsertStrategies ingests a run's sidecar strategies in one transaction.
func (s *Store) InsertStrategies(ctx context.Context, runID int64, openedAt time.Time, list []SidecarStrategy) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, st := range list {
		if len(st.Items) == 0 {
			return fmt.Errorf("strategy %s: no items", st.ID)
		}
		items, _ := json.Marshal(st.Items)
		risks, _ := json.Marshal(st.Risks)
		if _, err := tx.Exec(ctx, `INSERT INTO orchestrator.strategies
			(run_id, sid, archetype, title, thesis, items, primary_item_id,
			 entry_text, exit_text, entry_price, exit_price, kill_price, horizon_text,
			 capital_required, units_used, per_cycle_gp, per_4h_gp, per_day_gp, roi_pct,
			 confidence, confidence_why, evidence, invalidation, risks, paper_trade, opened_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)`,
			runID, st.ID, st.Archetype, st.Title, st.Thesis, items, st.Items[0].ID,
			st.Entry, st.Exit, st.EntryPrice, st.ExitPrice, st.KillPrice, st.Horizon,
			st.CapitalRequired, st.Size.UnitsUsed,
			st.ExpectedValue.PerCycleGp, st.ExpectedValue.Per4hGp, st.ExpectedValue.PerDayGp, st.ExpectedValue.RoiPct,
			st.Confidence, st.ConfidenceWhy, st.Evidence, st.Invalidation, risks, st.PaperTrade, openedAt,
		); err != nil {
			return fmt.Errorf("insert strategy %s: %w", st.ID, err)
		}
	}
	return tx.Commit(ctx)
}

type Strategy struct {
	StrategyID    int64           `json:"strategy_id"`
	RunID         int64           `json:"run_id"`
	Sid           string          `json:"sid"`
	Archetype     string          `json:"archetype"`
	Title         string          `json:"title"`
	Thesis        string          `json:"thesis"`
	Items         json.RawMessage `json:"items"`
	PrimaryItemID int             `json:"primary_item_id"`
	EntryText     string          `json:"entry"`
	ExitText      string          `json:"exit"`
	EntryPrice    int64           `json:"entry_price"`
	ExitPrice     int64           `json:"exit_price"`
	KillPrice     *int64          `json:"kill_price"`
	HorizonText   string          `json:"horizon"`
	Capital       *int64          `json:"capital_required"`
	UnitsUsed     *int64          `json:"units_used"`
	PerCycleGp    *int64          `json:"per_cycle_gp"`
	Per4hGp       *int64          `json:"per_4h_gp"`
	PerDayGp      *int64          `json:"per_day_gp"`
	RoiPct        *float64        `json:"roi_pct"`
	Confidence    string          `json:"confidence"`
	ConfidenceWhy *string         `json:"confidence_why"`
	Invalidation  string          `json:"invalidation"`
	Risks         json.RawMessage `json:"risks"`
	PaperTrade    *string         `json:"paper_trade"`
	State         string          `json:"state"`
	StateReason   *string         `json:"state_reason"`
	OpenedAt      time.Time       `json:"opened_at"`
	ClosedAt      *time.Time      `json:"closed_at"`
}

const strategyCols = `strategy_id, run_id, sid, archetype, title, thesis, items, primary_item_id,
	entry_text, exit_text, entry_price, exit_price, kill_price, horizon_text,
	capital_required, units_used, per_cycle_gp, per_4h_gp, per_day_gp, roi_pct,
	confidence, confidence_why, invalidation, risks, paper_trade,
	state, state_reason, opened_at, closed_at`

func scanStrategy(row pgx.Row) (*Strategy, error) {
	var st Strategy
	err := row.Scan(&st.StrategyID, &st.RunID, &st.Sid, &st.Archetype, &st.Title, &st.Thesis,
		&st.Items, &st.PrimaryItemID, &st.EntryText, &st.ExitText, &st.EntryPrice, &st.ExitPrice,
		&st.KillPrice, &st.HorizonText, &st.Capital, &st.UnitsUsed,
		&st.PerCycleGp, &st.Per4hGp, &st.PerDayGp, &st.RoiPct,
		&st.Confidence, &st.ConfidenceWhy, &st.Invalidation, &st.Risks, &st.PaperTrade,
		&st.State, &st.StateReason, &st.OpenedAt, &st.ClosedAt)
	if err != nil {
		return nil, err
	}
	return &st, nil
}

func (s *Store) collectStrategies(ctx context.Context, query string, args ...any) ([]Strategy, error) {
	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Strategy
	for rows.Next() {
		st, err := scanStrategy(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *st)
	}
	return out, rows.Err()
}

func (s *Store) StrategiesForRun(ctx context.Context, runID int64) ([]Strategy, error) {
	return s.collectStrategies(ctx,
		`SELECT `+strategyCols+` FROM orchestrator.strategies WHERE run_id=$1 ORDER BY strategy_id`, runID)
}

// LatestRunStrategies returns the strategies of the most recent succeeded run.
func (s *Store) LatestRunStrategies(ctx context.Context) ([]Strategy, error) {
	return s.collectStrategies(ctx, `SELECT `+strategyCols+` FROM orchestrator.strategies
		WHERE run_id = (SELECT max(run_id) FROM orchestrator.runs WHERE status='succeeded')
		ORDER BY per_4h_gp DESC NULLS LAST`)
}

func (s *Store) OpenStrategies(ctx context.Context) ([]Strategy, error) {
	return s.collectStrategies(ctx,
		`SELECT `+strategyCols+` FROM orchestrator.strategies WHERE state='open' ORDER BY strategy_id`)
}

func (s *Store) StrategyByID(ctx context.Context, id int64) (*Strategy, error) {
	st, err := scanStrategy(s.Pool.QueryRow(ctx,
		`SELECT `+strategyCols+` FROM orchestrator.strategies WHERE strategy_id=$1`, id))
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return st, err
}

func (s *Store) CloseStrategy(ctx context.Context, id int64, state, reason string) error {
	_, err := s.Pool.Exec(ctx, `UPDATE orchestrator.strategies
		SET state=$2, state_reason=$3, closed_at=now() WHERE strategy_id=$1 AND state='open'`,
		id, state, reason)
	return err
}
