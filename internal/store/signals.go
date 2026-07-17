package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
)

// Signal is one row of the collector's work queue: something the sweeps
// flagged that a research run should investigate. Lifecycle:
// pending -> assigned (attached to a run's brief) -> investigated/dismissed
// (the run's signal_verdicts), or dismissed by staleness.
type Signal struct {
	SignalID   int64           `json:"signal_id"`
	Kind       string          `json:"kind"` // seasonal | volume | band
	ItemID     int             `json:"item_id"`
	ItemName   string          `json:"item_name"`
	Metrics    json.RawMessage `json:"metrics"`
	Status     string          `json:"status"`
	RunID      *int64          `json:"run_id,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	ResolvedAt *time.Time      `json:"resolved_at,omitempty"`
	Reason     *string         `json:"reason,omitempty"`
}

// UpsertSignal queues a detection. A live (pending/assigned) signal for the
// same (kind, item) just gets its metrics refreshed — detections don't stack.
// Returns true when a NEW pending signal was created.
func (s *Store) UpsertSignal(ctx context.Context, kind string, itemID int, itemName string, metrics any) (bool, error) {
	m, err := json.Marshal(metrics)
	if err != nil {
		return false, err
	}
	var inserted bool
	err = s.Pool.QueryRow(ctx, `INSERT INTO orchestrator.signals (kind, item_id, item_name, metrics)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (kind, item_id) WHERE status IN ('pending','assigned')
		DO UPDATE SET metrics = EXCLUDED.metrics
		RETURNING (xmax = 0)`, kind, itemID, itemName, m).Scan(&inserted)
	return inserted, err
}

// PendingSignals returns the oldest pending signals up to limit.
func (s *Store) PendingSignals(ctx context.Context, limit int) ([]Signal, error) {
	return s.collectSignals(ctx, `SELECT signal_id, kind, item_id, item_name, metrics,
		status, run_id, created_at, resolved_at, reason
		FROM orchestrator.signals WHERE status='pending' ORDER BY created_at LIMIT $1`, limit)
}

// Signals lists recent signals of any status, newest first.
func (s *Store) Signals(ctx context.Context, limit int) ([]Signal, error) {
	return s.collectSignals(ctx, `SELECT signal_id, kind, item_id, item_name, metrics,
		status, run_id, created_at, resolved_at, reason
		FROM orchestrator.signals ORDER BY signal_id DESC LIMIT $1`, limit)
}

// AssignSignals marks the given pending signals as assigned to a run.
func (s *Store) AssignSignals(ctx context.Context, runID int64, ids []int64) error {
	_, err := s.Pool.Exec(ctx, `UPDATE orchestrator.signals
		SET status='assigned', run_id=$1 WHERE signal_id = ANY($2) AND status='pending'`, runID, ids)
	return err
}

// ResolveSignal applies a run's verdict: shipped -> investigated,
// dismissed -> dismissed, both with the model's reason. A dismissed 'watch'
// signal is a failed revalidation, so it decays the item's portfolio entry
// (shipped watches score later, through the strategy's own outcome).
func (s *Store) ResolveSignal(ctx context.Context, signalID int64, verdict, reason string) error {
	status := "dismissed"
	if verdict == "shipped" {
		status = "investigated"
	}
	var kind string
	var itemID int
	err := s.Pool.QueryRow(ctx, `UPDATE orchestrator.signals
		SET status=$2, reason=$3, resolved_at=now()
		WHERE signal_id=$1 AND status IN ('pending','assigned')
		RETURNING kind, item_id`, signalID, status, reason).Scan(&kind, &itemID)
	if err == pgx.ErrNoRows {
		return nil // already resolved (or unknown id) — same as the old no-op
	}
	if err != nil {
		return err
	}
	if kind == "watch" && status == "dismissed" {
		return s.RecordWatchDismissal(ctx, itemID)
	}
	return nil
}

// ReleaseRunSignals returns a failed run's assigned signals to the queue so
// the next run picks them up.
func (s *Store) ReleaseRunSignals(ctx context.Context, runID int64) error {
	_, err := s.Pool.Exec(ctx, `UPDATE orchestrator.signals
		SET status='pending', run_id=NULL WHERE run_id=$1 AND status='assigned'`, runID)
	return err
}

// ExpireStaleSignals dismisses pending signals older than ttl — a week-old
// anomaly is no longer a reaction-speed edge.
func (s *Store) ExpireStaleSignals(ctx context.Context, ttl time.Duration) (int64, error) {
	tag, err := s.Pool.Exec(ctx, `UPDATE orchestrator.signals
		SET status='dismissed', reason='expired unassigned', resolved_at=now()
		WHERE status='pending' AND created_at < now() - $1::interval`, ttl.String())
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) collectSignals(ctx context.Context, query string, args ...any) ([]Signal, error) {
	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Signal
	for rows.Next() {
		var sig Signal
		if err := rows.Scan(&sig.SignalID, &sig.Kind, &sig.ItemID, &sig.ItemName, &sig.Metrics,
			&sig.Status, &sig.RunID, &sig.CreatedAt, &sig.ResolvedAt, &sig.Reason); err != nil {
			return nil, err
		}
		out = append(out, sig)
	}
	return out, rows.Err()
}

// --- trend snapshots (the durable market-intelligence base) ---

type TrendRow struct {
	AsOf    time.Time       `json:"as_of"`
	Lens    string          `json:"lens"`
	ItemID  int             `json:"item_id"`
	Metrics json.RawMessage `json:"metrics"`
}

// InsertTrendSnapshots stores one sweep's top rows for a lens.
func (s *Store) InsertTrendSnapshots(ctx context.Context, asOf time.Time, lens string, rows []TrendRow) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, r := range rows {
		if _, err := tx.Exec(ctx, `INSERT INTO orchestrator.trend_snapshots (as_of, lens, item_id, metrics)
			VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`, asOf, lens, r.ItemID, r.Metrics); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// LatestTrends returns the most recent sweep's rows for a lens.
func (s *Store) LatestTrends(ctx context.Context, lens string, limit int) ([]TrendRow, error) {
	rows, err := s.Pool.Query(ctx, `SELECT as_of, lens, item_id, metrics
		FROM orchestrator.trend_snapshots
		WHERE lens=$1 AND as_of = (SELECT max(as_of) FROM orchestrator.trend_snapshots WHERE lens=$1)
		LIMIT $2`, lens, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TrendRow
	for rows.Next() {
		var r TrendRow
		if err := rows.Scan(&r.AsOf, &r.Lens, &r.ItemID, &r.Metrics); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
