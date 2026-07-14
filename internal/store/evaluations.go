package store

import (
	"context"
	"encoding/json"
	"time"
)

type Evaluation struct {
	StrategyID      int64           `json:"strategy_id"`
	At              time.Time       `json:"at"`
	CurHigh         *int64          `json:"cur_high"`
	CurLow          *int64          `json:"cur_low"`
	HighAgeS        *int            `json:"high_age_s"`
	LowAgeS         *int            `json:"low_age_s"`
	CurMargin       *int64          `json:"cur_margin"`
	Vol30m          *int64          `json:"vol_30m"`
	RealizedPer4hGp *int64          `json:"realized_per_4h_gp"`
	Checks          json.RawMessage `json:"checks"`
	Verdict         string          `json:"verdict"`
}

func (s *Store) InsertEvaluation(ctx context.Context, e Evaluation) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO orchestrator.evaluations
		(strategy_id, at, cur_high, cur_low, high_age_s, low_age_s, cur_margin, vol_30m,
		 realized_per_4h_gp, checks, verdict)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		e.StrategyID, e.At, e.CurHigh, e.CurLow, e.HighAgeS, e.LowAgeS, e.CurMargin, e.Vol30m,
		e.RealizedPer4hGp, e.Checks, e.Verdict)
	return err
}

func (s *Store) Evaluations(ctx context.Context, strategyID int64, limit int) ([]Evaluation, error) {
	rows, err := s.Pool.Query(ctx, `SELECT strategy_id, at, cur_high, cur_low, high_age_s,
		low_age_s, cur_margin, vol_30m, realized_per_4h_gp, checks, verdict
		FROM orchestrator.evaluations WHERE strategy_id=$1 ORDER BY at DESC LIMIT $2`,
		strategyID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Evaluation
	for rows.Next() {
		var e Evaluation
		if err := rows.Scan(&e.StrategyID, &e.At, &e.CurHigh, &e.CurLow, &e.HighAgeS,
			&e.LowAgeS, &e.CurMargin, &e.Vol30m, &e.RealizedPer4hGp, &e.Checks, &e.Verdict); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// LastVerdicts returns the most recent n verdicts, newest first.
func (s *Store) LastVerdicts(ctx context.Context, strategyID int64, n int) ([]string, error) {
	rows, err := s.Pool.Query(ctx, `SELECT verdict FROM orchestrator.evaluations
		WHERE strategy_id=$1 ORDER BY at DESC LIMIT $2`, strategyID, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// EvalStats supports the confirmation rule: share of healthy evals + median
// realized/projected over the strategy's whole evaluation history.
func (s *Store) EvalStats(ctx context.Context, strategyID int64) (total, healthy int, medianRatio *float64, err error) {
	err = s.Pool.QueryRow(ctx, `SELECT count(*),
		count(*) FILTER (WHERE verdict='healthy'),
		(percentile_cont(0.5) WITHIN GROUP (ORDER BY realized_per_4h_gp)
		 / nullif((SELECT per_4h_gp FROM orchestrator.strategies WHERE strategy_id=$1), 0))::float8
		FROM orchestrator.evaluations WHERE strategy_id=$1`, strategyID).
		Scan(&total, &healthy, &medianRatio)
	return
}

type ScoreboardRow struct {
	Archetype           string   `json:"archetype"`
	N                   int      `json:"n"`
	Confirmed           int      `json:"confirmed"`
	Killed              int      `json:"killed"`
	Expired             int      `json:"expired"`
	Open                int      `json:"open"`
	RealizedVsProjected *float64 `json:"realized_vs_projected"`
}

func (s *Store) Scoreboard(ctx context.Context) ([]ScoreboardRow, error) {
	rows, err := s.Pool.Query(ctx, `SELECT archetype, n, confirmed, killed, expired, open,
		realized_vs_projected::float8 FROM orchestrator.scoreboard ORDER BY archetype`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ScoreboardRow
	for rows.Next() {
		var r ScoreboardRow
		if err := rows.Scan(&r.Archetype, &r.N, &r.Confirmed, &r.Killed, &r.Expired, &r.Open, &r.RealizedVsProjected); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RecentlyClosed lists recently killed/expired strategies for the brief's
// do-not-re-pitch section.
func (s *Store) RecentlyClosed(ctx context.Context, limit int) ([]Strategy, error) {
	return s.collectStrategies(ctx, `SELECT `+strategyCols+` FROM orchestrator.strategies
		WHERE state IN ('killed','expired') ORDER BY closed_at DESC LIMIT $1`, limit)
}
