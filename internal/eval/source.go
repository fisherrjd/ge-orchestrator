package eval

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Snap is the frozen single-item view a tick evaluates against: latest
// both-leg quote from prices_1m + 30m volume from prices_5m.
type Snap struct {
	High, Low, Margin  *int64
	HighAgeS, LowAgeS  *int
	Vol30m             int64
}

// WindowStats is what actually printed inside an hour-of-week window since a
// strategy opened: median observed prices and the traded volume, from
// prices_5m. Obs = 5m rows seen in-window.
type WindowStats struct {
	MedLow  *int64 // median avg_low_price (what a buy offer fills near)
	MedHigh *int64 // median avg_high_price (what a sell offer fills near)
	Volume  int64
	Obs     int
}

// PriceSource abstracts the price DB so per-kind evaluators are unit-testable
// with a fake. The pg implementation is the only production one.
type PriceSource interface {
	Snapshot(ctx context.Context, itemID int) (*Snap, error)
	SnapshotMany(ctx context.Context, itemIDs []int) (map[int]*Snap, error)
	// WindowStats aggregates prices_5m rows since `since` whose hour-of-week
	// bucket falls inside w.
	WindowStats(ctx context.Context, itemID int, since time.Time, w Window) (WindowStats, error)
	// VolumeZ computes the same z-score as ge-mcp's volume_zscore (trailing
	// 7d hourly baseline) plus the price move over the window.
	VolumeZ(ctx context.Context, itemID int, window time.Duration) (z float64, n int, priceMovePct *float64, err error)
}

// Window mirrors store.HourWindow without importing store (keeps this
// package's fake-source tests dependency-light).
type Window struct{ FromHow, ToHow int }

func (w Window) Contains(b int) bool {
	if w.FromHow <= w.ToHow {
		return b >= w.FromHow && b <= w.ToHow
	}
	return b >= w.FromHow || b <= w.ToHow
}

// HourOfWeek returns t's UTC hour-of-week bucket (dow*24+hour, dow 0=Sunday) —
// the same convention as the SQL `extract` math; asserted equal by
// TestHourOfWeekConvention.
func HourOfWeek(t time.Time) int {
	u := t.UTC()
	return int(u.Weekday())*24 + u.Hour()
}

type pgSource struct{ pool *pgxpool.Pool }

func NewPgSource(pool *pgxpool.Pool) PriceSource { return &pgSource{pool: pool} }

const snapshotSQL = `
WITH latest AS (
  SELECT high, low, margin, high_time, low_time
  FROM prices_1m WHERE item_id = $1 ORDER BY ts DESC LIMIT 1
),
vol AS (
  SELECT coalesce(sum(coalesce(high_volume,0)+coalesce(low_volume,0)),0) AS v
  FROM prices_5m WHERE item_id = $1 AND ts > now() - interval '30 minutes'
)
SELECT l.high, l.low, l.margin,
       extract(epoch FROM now()-l.high_time)::int,
       extract(epoch FROM now()-l.low_time)::int,
       v.v
FROM vol v LEFT JOIN latest l ON true`

func (p *pgSource) Snapshot(ctx context.Context, itemID int) (*Snap, error) {
	var s Snap
	if err := p.pool.QueryRow(ctx, snapshotSQL, itemID).Scan(
		&s.High, &s.Low, &s.Margin, &s.HighAgeS, &s.LowAgeS, &s.Vol30m); err != nil {
		return nil, err
	}
	return &s, nil
}

const snapshotManySQL = `
WITH latest AS (
  SELECT DISTINCT ON (item_id) item_id, high, low, margin, high_time, low_time
  FROM prices_1m WHERE item_id = ANY($1) ORDER BY item_id, ts DESC
),
vol AS (
  SELECT item_id, coalesce(sum(coalesce(high_volume,0)+coalesce(low_volume,0)),0) AS v
  FROM prices_5m WHERE item_id = ANY($1) AND ts > now() - interval '30 minutes'
  GROUP BY item_id
)
SELECT ids.id, l.high, l.low, l.margin,
       extract(epoch FROM now()-l.high_time)::int,
       extract(epoch FROM now()-l.low_time)::int,
       coalesce(v.v, 0)
FROM unnest($1::int[]) AS ids(id)
LEFT JOIN latest l ON l.item_id = ids.id
LEFT JOIN vol v ON v.item_id = ids.id`

func (p *pgSource) SnapshotMany(ctx context.Context, itemIDs []int) (map[int]*Snap, error) {
	rows, err := p.pool.Query(ctx, snapshotManySQL, itemIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int]*Snap{}
	for rows.Next() {
		var id int
		var s Snap
		if err := rows.Scan(&id, &s.High, &s.Low, &s.Margin, &s.HighAgeS, &s.LowAgeS, &s.Vol30m); err != nil {
			return nil, err
		}
		out[id] = &s
	}
	return out, rows.Err()
}

// Same UTC bucket convention as ge-mcp (QUERIES #17): explicit AT TIME ZONE
// so a server timezone change can't shift windows.
const windowStatsSQL = `
SELECT (percentile_cont(0.5) WITHIN GROUP (ORDER BY avg_low_price))::bigint,
       (percentile_cont(0.5) WITHIN GROUP (ORDER BY avg_high_price))::bigint,
       coalesce(sum(coalesce(high_volume,0)+coalesce(low_volume,0)),0),
       count(*)
FROM prices_5m
WHERE item_id = $1 AND ts >= $2
  AND (avg_high_price IS NOT NULL OR avg_low_price IS NOT NULL)
  AND ($3::int <= $4::int
       AND (extract(dow from ts AT TIME ZONE 'utc')*24 + extract(hour from ts AT TIME ZONE 'utc'))::int BETWEEN $3 AND $4
    OR $3::int > $4::int
       AND ((extract(dow from ts AT TIME ZONE 'utc')*24 + extract(hour from ts AT TIME ZONE 'utc'))::int >= $3
         OR (extract(dow from ts AT TIME ZONE 'utc')*24 + extract(hour from ts AT TIME ZONE 'utc'))::int <= $4))`

func (p *pgSource) WindowStats(ctx context.Context, itemID int, since time.Time, w Window) (WindowStats, error) {
	var ws WindowStats
	err := p.pool.QueryRow(ctx, windowStatsSQL, itemID, since, w.FromHow, w.ToHow).
		Scan(&ws.MedLow, &ws.MedHigh, &ws.Volume, &ws.Obs)
	return ws, err
}

// Trailing-7d hourly baseline — the same computation as ge-mcp's
// volume_zscore(baseline=trailing); keep the two in sync.
const volumeZSQL = `
WITH cur AS (
  SELECT coalesce(sum(coalesce(high_volume,0)+coalesce(low_volume,0)),0) AS v
  FROM prices_5m WHERE item_id = $1 AND ts >= $2
),
hist AS (
  SELECT date_trunc('hour', ts) AS hb,
         sum(coalesce(high_volume,0)+coalesce(low_volume,0)) AS vol
  FROM prices_5m
  WHERE item_id = $1 AND ts < date_trunc('hour', now()) AND ts >= now() - interval '7 days'
  GROUP BY 1
),
base AS (SELECT avg(vol) AS mean, stddev_samp(vol) AS sd, count(*) AS n FROM hist),
px AS (
  SELECT (array_agg((coalesce(avg_high_price,avg_low_price)+coalesce(avg_low_price,avg_high_price))/2.0 ORDER BY ts ASC))[1]  AS p_start,
         (array_agg((coalesce(avg_high_price,avg_low_price)+coalesce(avg_low_price,avg_high_price))/2.0 ORDER BY ts DESC))[1] AS p_end
  FROM prices_5m
  WHERE item_id = $1 AND ts >= $2 AND (avg_high_price IS NOT NULL OR avg_low_price IS NOT NULL)
)
SELECT CASE WHEN b.sd > 0 THEN (c.v - b.mean) / b.sd ELSE 0 END,
       b.n,
       (100*(p.p_end - p.p_start)/nullif(p.p_start,0))::float8
FROM cur c CROSS JOIN base b LEFT JOIN px p ON true`

func (p *pgSource) VolumeZ(ctx context.Context, itemID int, window time.Duration) (float64, int, *float64, error) {
	var z float64
	var n int
	var move *float64
	err := p.pool.QueryRow(ctx, volumeZSQL, itemID, time.Now().UTC().Add(-window)).Scan(&z, &n, &move)
	return z, n, move, err
}
