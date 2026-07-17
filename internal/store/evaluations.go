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
	RealizedPer1hGp *int64          `json:"realized_per_1h_gp"`
	Checks          json.RawMessage `json:"checks"`
	Verdict         string          `json:"verdict"`
	Detail          json.RawMessage `json:"detail,omitempty"`
}

func (s *Store) InsertEvaluation(ctx context.Context, e Evaluation) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO orchestrator.evaluations
		(strategy_id, at, cur_high, cur_low, high_age_s, low_age_s, cur_margin, vol_30m,
		 realized_per_1h_gp, checks, verdict, detail)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		e.StrategyID, e.At, e.CurHigh, e.CurLow, e.HighAgeS, e.LowAgeS, e.CurMargin, e.Vol30m,
		e.RealizedPer1hGp, e.Checks, e.Verdict, e.Detail)
	return err
}

func (s *Store) Evaluations(ctx context.Context, strategyID int64, limit int) ([]Evaluation, error) {
	rows, err := s.Pool.Query(ctx, `SELECT strategy_id, at, cur_high, cur_low, high_age_s,
		low_age_s, cur_margin, vol_30m, realized_per_1h_gp, checks, verdict, detail
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
			&e.LowAgeS, &e.CurMargin, &e.Vol30m, &e.RealizedPer1hGp, &e.Checks, &e.Verdict, &e.Detail); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// LastEvalAt returns the newest evaluation time for a strategy (nil if none).
// The ticker uses it to honor per-kind cadences (H evaluates hourly).
func (s *Store) LastEvalAt(ctx context.Context, strategyID int64) (*time.Time, error) {
	var t *time.Time
	err := s.Pool.QueryRow(ctx, `SELECT max(at) FROM orchestrator.evaluations
		WHERE strategy_id=$1`, strategyID).Scan(&t)
	return t, err
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
// realized/projected since the given anchor (a V strategy's armed ticks must
// not count toward its confirmation — pass triggered_at; zero time = all).
func (s *Store) EvalStats(ctx context.Context, strategyID int64, since time.Time) (total, healthy int, medianRatio *float64, err error) {
	err = s.Pool.QueryRow(ctx, `SELECT count(*),
		count(*) FILTER (WHERE verdict='healthy'),
		(percentile_cont(0.5) WITHIN GROUP (ORDER BY realized_per_1h_gp)
		 / nullif((SELECT per_1h_gp FROM orchestrator.strategies WHERE strategy_id=$1), 0))::float8
		FROM orchestrator.evaluations WHERE strategy_id=$1 AND at >= $2`, strategyID, since).
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
	Armed               int      `json:"armed"`
	Vetoed              int      `json:"vetoed"`
	RealizedVsProjected *float64 `json:"realized_vs_projected"`
}

func (s *Store) Scoreboard(ctx context.Context) ([]ScoreboardRow, error) {
	rows, err := s.Pool.Query(ctx, `SELECT archetype, n, confirmed, killed, expired, open, armed, vetoed,
		realized_vs_projected::float8 FROM orchestrator.scoreboard ORDER BY archetype`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ScoreboardRow
	for rows.Next() {
		var r ScoreboardRow
		if err := rows.Scan(&r.Archetype, &r.N, &r.Confirmed, &r.Killed, &r.Expired, &r.Open, &r.Armed, &r.Vetoed, &r.RealizedVsProjected); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// PnLRow is one strategy's paper-trade estimate: the median haircut
// realized_per_1h over its evaluated life × hours in that life. It is an
// ESTIMATE of what following the strategy would have printed, not a ledger —
// upper-bounded like every paper number (see package eval's honesty rules).
type PnLRow struct {
	StrategyID    int64      `json:"strategy_id"`
	Sid           string     `json:"sid"`
	Title         string     `json:"title"`
	Archetype     string     `json:"archetype"`
	State         string     `json:"state"`
	OpenedAt      time.Time  `json:"opened_at"`
	ClosedAt      *time.Time `json:"closed_at,omitempty"`
	Hours         float64    `json:"hours"`
	MedRealized1h *float64   `json:"med_realized_per_1h_gp"`
	EstRealizedGp *int64     `json:"est_realized_gp"`
	ProjectedGp   *int64     `json:"projected_gp"`
	Capital       *int64     `json:"capital_required"`
}

// PnL returns per-strategy paper-trade estimates for every strategy that has
// actually been evaluated: vetoed rows and never-triggered armed rows are
// excluded (they were never trading). Hours run from the eval-clock anchor
// (triggered_at for fired Vs, opened_at otherwise) to closed_at or now.
func (s *Store) PnL(ctx context.Context) ([]PnLRow, error) {
	rows, err := s.Pool.Query(ctx, `SELECT s.strategy_id, s.sid, s.title, s.archetype, s.state,
		s.opened_at, s.closed_at,
		greatest(extract(epoch from (coalesce(s.closed_at, now()) - coalesce(s.triggered_at, s.opened_at)))/3600.0, 0)::float8 AS hours,
		est.med_1h::float8, s.per_1h_gp, s.capital_required
		FROM orchestrator.strategies s
		JOIN LATERAL (
			SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY e.realized_per_1h_gp) AS med_1h
			FROM orchestrator.evaluations e
			WHERE e.strategy_id = s.strategy_id
			  AND (s.triggered_at IS NULL OR e.at >= s.triggered_at)
		) est ON true
		WHERE s.state <> 'vetoed' AND NOT (s.state = 'armed' AND s.triggered_at IS NULL)
		ORDER BY s.strategy_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PnLRow
	for rows.Next() {
		var r PnLRow
		var per1h *int64
		if err := rows.Scan(&r.StrategyID, &r.Sid, &r.Title, &r.Archetype, &r.State,
			&r.OpenedAt, &r.ClosedAt, &r.Hours, &r.MedRealized1h, &per1h, &r.Capital); err != nil {
			return nil, err
		}
		if r.MedRealized1h != nil {
			v := int64(*r.MedRealized1h * r.Hours)
			r.EstRealizedGp = &v
		}
		if per1h != nil {
			v := int64(float64(*per1h) * r.Hours)
			r.ProjectedGp = &v
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
