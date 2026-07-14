// Package store owns the orchestrator schema: embedded migrations, and typed
// access to runs / strategies / evaluations / the scoreboard view.
package store

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Store struct {
	Pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	s := &Store{Pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// migrate applies embedded migrations in filename order, tracked in
// orchestrator.schema_version. The orchestrator role owns the schema, so DDL
// is permitted there (and nowhere else).
func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.Pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS orchestrator.schema_version
		(version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("schema_version: %w", err)
	}
	files, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, f := range files {
		var done bool
		if err := s.Pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM orchestrator.schema_version WHERE version=$1)`, f).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		sql, err := migrations.ReadFile(f)
		if err != nil {
			return err
		}
		tx, err := s.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("migration %s: %w", f, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO orchestrator.schema_version(version) VALUES ($1)`, f); err != nil {
			tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// --- runs ---

type Run struct {
	RunID      int64           `json:"run_id"`
	StartedAt  time.Time       `json:"started_at"`
	FinishedAt *time.Time      `json:"finished_at"`
	Status     string          `json:"status"`
	Brief      json.RawMessage `json:"brief"`
	BriefText  string          `json:"brief_text"`
	ReportPath *string         `json:"report_path"`
	FailReason *string         `json:"fail_reason"`
	NStrats    int             `json:"n_strategies"`
}

func (s *Store) CreateRun(ctx context.Context, brief json.RawMessage, briefText string) (int64, error) {
	var id int64
	err := s.Pool.QueryRow(ctx,
		`INSERT INTO orchestrator.runs (brief, brief_text) VALUES ($1, $2) RETURNING run_id`,
		brief, briefText).Scan(&id)
	return id, err
}

func (s *Store) FinishRun(ctx context.Context, runID int64, status, reportPath, reportMd, failReason string) error {
	_, err := s.Pool.Exec(ctx, `UPDATE orchestrator.runs
		SET finished_at = now(), status = $2,
		    report_path = nullif($3,''), report_md = nullif($4,''), fail_reason = nullif($5,'')
		WHERE run_id = $1`, runID, status, reportPath, reportMd, failReason)
	return err
}

// OrphanRunningRuns marks leftover 'running' rows failed (startup recovery).
func (s *Store) OrphanRunningRuns(ctx context.Context) (int64, error) {
	tag, err := s.Pool.Exec(ctx, `UPDATE orchestrator.runs
		SET status='failed', finished_at=now(), fail_reason='orphaned by orchestrator restart'
		WHERE status='running'`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) Runs(ctx context.Context, limit int) ([]Run, error) {
	rows, err := s.Pool.Query(ctx, `SELECT r.run_id, r.started_at, r.finished_at, r.status,
		r.brief, r.brief_text, r.report_path, r.fail_reason,
		(SELECT count(*) FROM orchestrator.strategies st WHERE st.run_id = r.run_id)
		FROM orchestrator.runs r ORDER BY r.run_id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		var r Run
		if err := rows.Scan(&r.RunID, &r.StartedAt, &r.FinishedAt, &r.Status,
			&r.Brief, &r.BriefText, &r.ReportPath, &r.FailReason, &r.NStrats); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Run(ctx context.Context, runID int64) (*Run, error) {
	var r Run
	err := s.Pool.QueryRow(ctx, `SELECT r.run_id, r.started_at, r.finished_at, r.status,
		r.brief, r.brief_text, r.report_path, r.fail_reason,
		(SELECT count(*) FROM orchestrator.strategies st WHERE st.run_id = r.run_id)
		FROM orchestrator.runs r WHERE r.run_id = $1`, runID).
		Scan(&r.RunID, &r.StartedAt, &r.FinishedAt, &r.Status,
			&r.Brief, &r.BriefText, &r.ReportPath, &r.FailReason, &r.NStrats)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return &r, err
}

// OpenCount returns how many strategies are still open.
func (s *Store) OpenCount(ctx context.Context) (int, error) {
	var n int
	err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM orchestrator.strategies WHERE state='open'`).Scan(&n)
	return n, err
}

// LastRunStart returns the most recent run's start time (any status), nil if
// no runs exist. Used as the auto-trigger cooldown anchor — DB-based so it
// survives restarts and counts manual runs too.
func (s *Store) LastRunStart(ctx context.Context) (*time.Time, error) {
	var t *time.Time
	err := s.Pool.QueryRow(ctx, `SELECT max(started_at) FROM orchestrator.runs`).Scan(&t)
	return t, err
}

func (s *Store) ReportMarkdown(ctx context.Context, runID int64) (string, error) {
	var md *string
	err := s.Pool.QueryRow(ctx, `SELECT report_md FROM orchestrator.runs WHERE run_id=$1`, runID).Scan(&md)
	if err == pgx.ErrNoRows || md == nil {
		return "", nil
	}
	return *md, err
}
