package store

import (
	"context"
	"fmt"
	"time"
)

// Watch scoring model. `score` is the accumulated evidence: each confirmed
// strategy on the entry adds a full point (capped), each negative outcome
// multiplies it down — a kill hits harder than an expiry, because "the stop
// blew out" is stronger evidence against the idea than "the window elapsed
// unconfirmed". `eff_score` (the ranking key) additionally halves for every
// 14 days since the entry was last validated, so a onetime winner sinks
// unless revalidation keeps re-proving it. Below retireBelow the entry
// auto-retires: the idea had its chances.
const (
	watchConfirmBoost  = 1.0
	watchScoreCap      = 6.0
	watchKilledFactor  = 0.4
	watchExpiredFactor = 0.7
	watchDismissFactor = 0.6
	watchRetireBelow   = 0.25
)

// 14-day half-life; keep in sync with the doc comment above.
const watchEffScore = `w.score * power(0.5,
	extract(epoch from (now() - coalesce(w.last_validated_at, w.created_at))) / 86400.0 / 14.0)`

type WatchEntry struct {
	WatchID         int64      `json:"watch_id"`
	ItemID          int        `json:"item_id"`
	ItemName        string     `json:"item_name"`
	Archetype       *string    `json:"archetype,omitempty"`
	Note            *string    `json:"note,omitempty"`
	Source          string     `json:"source"`
	Score           float64    `json:"score"`
	EffScore        float64    `json:"eff_score"`
	TimesValidated  int        `json:"times_validated"`
	TimesConfirmed  int        `json:"times_confirmed"`
	LastResult      *string    `json:"last_result,omitempty"`
	LastValidatedAt *time.Time `json:"last_validated_at,omitempty"`
	Status          string     `json:"status"`
	CreatedAt       time.Time  `json:"created_at"`
}

const watchCols = `w.watch_id, w.item_id, w.item_name, w.archetype, w.note, w.source,
	w.score, ` + watchEffScore + ` AS eff_score,
	w.times_validated, w.times_confirmed, w.last_result, w.last_validated_at, w.status, w.created_at`

// UpsertWatch adds an idea to the portfolio, or boosts the existing live
// entry for the same (item, archetype). Operator entries keep the operator's
// note; a confirming strategy refreshes the note with its sid trail.
func (s *Store) UpsertWatch(ctx context.Context, itemID int, itemName string, archetype, note *string, source string) (int64, error) {
	var id int64
	err := s.Pool.QueryRow(ctx, `INSERT INTO orchestrator.watchlist
		(item_id, item_name, archetype, note, source)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (item_id, coalesce(archetype, '*')) WHERE status = 'active'
		DO UPDATE SET score = least(orchestrator.watchlist.score + $6, $7),
		              note = coalesce(EXCLUDED.note, orchestrator.watchlist.note)
		RETURNING watch_id`,
		itemID, itemName, archetype, note, source, watchConfirmBoost, watchScoreCap).Scan(&id)
	return id, err
}

// RecordStrategyOutcome feeds a closed strategy's result back into the
// portfolio. A confirmation promotes the item (creating the entry if it
// wasn't watched yet — confirmed ideas are exactly what the portfolio is
// for) and boosts its score; kills and expiries decay every live entry
// matching the item (archetype-specific or any-kind), retiring entries that
// fall below the floor.
func (s *Store) RecordStrategyOutcome(ctx context.Context, itemID int, itemName, archetype, sid, outcome string) error {
	if outcome == "confirmed" {
		note := "confirmed by " + sid
		if _, err := s.UpsertWatch(ctx, itemID, itemName, &archetype, &note, "confirmed"); err != nil {
			return err
		}
		_, err := s.Pool.Exec(ctx, `UPDATE orchestrator.watchlist
			SET times_validated = times_validated + 1,
			    times_confirmed = times_confirmed + 1,
			    last_result = 'confirmed', last_validated_at = now()
			WHERE status='active' AND item_id=$1 AND (archetype IS NULL OR archetype=$2)`,
			itemID, archetype)
		return err
	}

	factor := watchExpiredFactor
	if outcome == "killed" {
		factor = watchKilledFactor
	}
	_, err := s.Pool.Exec(ctx, `UPDATE orchestrator.watchlist
		SET score = score * $3,
		    times_validated = times_validated + 1,
		    last_result = $4, last_validated_at = now(),
		    status = CASE WHEN score * $3 < $5 THEN 'retired' ELSE status END
		WHERE status='active' AND item_id=$1 AND (archetype IS NULL OR archetype=$2)`,
		itemID, archetype, factor, outcome, watchRetireBelow)
	return err
}

// RecordWatchDismissal decays an item's live entries when a research run
// investigated a watch signal and dismissed it — a revalidation that failed.
func (s *Store) RecordWatchDismissal(ctx context.Context, itemID int) error {
	_, err := s.Pool.Exec(ctx, `UPDATE orchestrator.watchlist
		SET score = score * $2,
		    times_validated = times_validated + 1,
		    last_result = 'dismissed', last_validated_at = now(),
		    status = CASE WHEN score * $2 < $3 THEN 'retired' ELSE status END
		WHERE status='active' AND item_id=$1`,
		itemID, watchDismissFactor, watchRetireBelow)
	return err
}

// RetireWatch is the operator saying "stop watching this".
func (s *Store) RetireWatch(ctx context.Context, watchID int64) error {
	tag, err := s.Pool.Exec(ctx, `UPDATE orchestrator.watchlist
		SET status='retired' WHERE watch_id=$1 AND status='active'`, watchID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("no active watchlist entry %d", watchID)
	}
	return nil
}

// WatchRanked returns the live portfolio ordered by decayed effective score.
func (s *Store) WatchRanked(ctx context.Context, limit int) ([]WatchEntry, error) {
	return s.collectWatches(ctx, `SELECT `+watchCols+` FROM orchestrator.watchlist w
		WHERE w.status='active' ORDER BY 8 DESC LIMIT $1`, limit)
}

// WatchDueForRevalidation returns live entries whose last validation (or
// creation) is older than the interval — the collector queues these as
// 'watch' signals so research runs re-prove or decay them.
func (s *Store) WatchDueForRevalidation(ctx context.Context, olderThan time.Duration, limit int) ([]WatchEntry, error) {
	return s.collectWatches(ctx, `SELECT `+watchCols+` FROM orchestrator.watchlist w
		WHERE w.status='active'
		  AND coalesce(w.last_validated_at, w.created_at) < now() - $1::interval
		ORDER BY 8 DESC LIMIT $2`, olderThan.String(), limit)
}

func (s *Store) collectWatches(ctx context.Context, query string, args ...any) ([]WatchEntry, error) {
	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WatchEntry
	for rows.Next() {
		var w WatchEntry
		if err := rows.Scan(&w.WatchID, &w.ItemID, &w.ItemName, &w.Archetype, &w.Note, &w.Source,
			&w.Score, &w.EffScore, &w.TimesValidated, &w.TimesConfirmed,
			&w.LastResult, &w.LastValidatedAt, &w.Status, &w.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// LookupItem resolves an item name (exact, case-insensitive) against the
// ingester's items table — the operator adds watches by name.
func (s *Store) LookupItem(ctx context.Context, name string) (int, string, error) {
	var id int
	var canonical string
	err := s.Pool.QueryRow(ctx, `SELECT item_id, name FROM items
		WHERE lower(name) = lower($1) LIMIT 1`, name).Scan(&id, &canonical)
	return id, canonical, err
}

// ItemName looks up the canonical name for an item id.
func (s *Store) ItemName(ctx context.Context, itemID int) (string, error) {
	var name string
	err := s.Pool.QueryRow(ctx, `SELECT name FROM items WHERE item_id=$1`, itemID).Scan(&name)
	return name, err
}
