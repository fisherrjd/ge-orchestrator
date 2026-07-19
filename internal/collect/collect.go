// Package collect is the no-LLM trend collector: on its own ticker it sweeps
// the ENTIRE market with the same SQL lenses the research tools use, persists
// the top rows as trend_snapshots (the durable market-intelligence base —
// pattern history stays queryable after prices move on), and queues signals
// (detections crossing significance) for research runs to investigate.
//
// This is the inversion at the heart of the re-architecture: observation and
// screening are deterministic and continuous; the LLM only interprets. A
// signal is an *assignment of attention*, so consecutive runs cover different
// ground instead of re-deriving the same top-of-scan.
package collect

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/osrs-ge/ge-orchestrator/internal/store"
)

type Config struct {
	// Signal thresholds — start conservative, tune from the queue's hit rate.
	VolZMin         float64       // |z| to queue a volume signal (default 8)
	SnapshotTopN    int           // rows persisted per lens per cycle (default 25)
	SignalTTL       time.Duration // pending signals expire after this (default 72h)
	RevalidateAfter time.Duration // watch entries re-queue after this (default 5d)

	// Lane F — volume flips: deep-market commodities where post-tax margin x
	// buy limit pays real gp per 4h cycle. Ranked and gated in absolute gp.
	FlipFreshAge   time.Duration // both legs traded within this (default 30m)
	FlipVolMin24h  int64         // 24h volume floor in units (default 100k)
	FlipGpCycleMin int64         // margin x buy_limit floor to queue a signal (default 200k)

	// Lane B — high-value flips: 10M+ items with a fresh two-sided market,
	// ranked by absolute post-tax margin, sized by what the budget affords.
	HighValueMinPrice   int64 // buy-leg price floor (default 10M)
	HighValueVolMin24h  int64 // 24h volume floor in units (default 200)
	HighValueGpCycleMin int64 // margin x affordable units floor to queue (default 100k)

	// ResearchBudgetGp caps lane B's affordable units (default 50M). A
	// per-opportunity sizing scale, not a shared pool.
	ResearchBudgetGp int64
}

func (c *Config) defaults() {
	if c.VolZMin == 0 {
		c.VolZMin = 8
	}
	if c.SnapshotTopN == 0 {
		c.SnapshotTopN = 25
	}
	if c.SignalTTL == 0 {
		c.SignalTTL = 72 * time.Hour
	}
	if c.RevalidateAfter == 0 {
		c.RevalidateAfter = 5 * 24 * time.Hour
	}
	if c.FlipFreshAge == 0 {
		c.FlipFreshAge = 30 * time.Minute
	}
	if c.FlipVolMin24h == 0 {
		c.FlipVolMin24h = 100_000
	}
	if c.FlipGpCycleMin == 0 {
		c.FlipGpCycleMin = 200_000
	}
	if c.HighValueMinPrice == 0 {
		c.HighValueMinPrice = 10_000_000
	}
	if c.HighValueVolMin24h == 0 {
		c.HighValueVolMin24h = 200
	}
	if c.HighValueGpCycleMin == 0 {
		c.HighValueGpCycleMin = 100_000
	}
	if c.ResearchBudgetGp == 0 {
		c.ResearchBudgetGp = 50_000_000
	}
}

type Collector struct {
	Store *store.Store
	Cfg   Config
}

// Cycle runs one full sweep. Returns how many NEW signals were queued (the
// caller uses that to decide whether to trigger a research run).
func (c *Collector) Cycle(ctx context.Context) int {
	c.Cfg.defaults()
	asOf := time.Now().UTC()
	newSignals := 0
	newSignals += c.sweepVolume(ctx, asOf)
	newSignals += c.sweepSeasonal(ctx, asOf) // snapshots only — timing evidence, no signals
	newSignals += c.sweepBand(ctx, asOf)     // snapshots only — qualification evidence, no signals
	newSignals += c.sweepFlip(ctx, asOf)
	newSignals += c.sweepHighValue(ctx, asOf)
	newSignals += c.sweepWatch(ctx)
	if n, err := c.Store.ExpireStaleSignals(ctx, c.Cfg.SignalTTL); err != nil {
		log.Printf("collect: expire stale: %v", err)
	} else if n > 0 {
		log.Printf("collect: expired %d stale signal(s)", n)
	}
	return newSignals
}

// --- volume lens: trailing-7d hourly z-score, whole market ---
// (Same computation as ge-mcp volume_zscore baseline=trailing.)

const volumeSweepSQL = `
WITH cur AS (
  SELECT item_id,
         sum(coalesce(high_volume,0)+coalesce(low_volume,0)) AS cur_vol,
         sum(coalesce(high_volume,0)) AS buys,
         sum(coalesce(low_volume,0)) AS sells
  FROM prices_5m WHERE ts >= now() - interval '1 hour'
  GROUP BY 1
),
hist AS (
  SELECT item_id, date_trunc('hour', ts) AS hb,
         sum(coalesce(high_volume,0)+coalesce(low_volume,0)) AS vol
  FROM prices_5m
  WHERE ts < date_trunc('hour', now()) AND ts >= now() - interval '7 days'
  GROUP BY 1, 2
),
base AS (
  SELECT item_id, avg(vol) AS mean, stddev_samp(vol) AS sd, count(*) AS n
  FROM hist GROUP BY 1
),
px AS (
  SELECT item_id,
         (array_agg((coalesce(avg_high_price,avg_low_price)+coalesce(avg_low_price,avg_high_price))/2.0 ORDER BY ts ASC))[1]  AS p_start,
         (array_agg((coalesce(avg_high_price,avg_low_price)+coalesce(avg_low_price,avg_high_price))/2.0 ORDER BY ts DESC))[1] AS p_end
  FROM prices_5m
  WHERE ts >= now() - interval '1 hour' AND (avg_high_price IS NOT NULL OR avg_low_price IS NOT NULL)
  GROUP BY 1
)
SELECT c.item_id, i.name, c.cur_vol, c.buys, c.sells,
       round(((c.cur_vol - b.mean) / b.sd)::numeric, 2)::float8 AS z,
       b.n,
       round((100*(p.p_end - p.p_start)/nullif(p.p_start,0))::numeric, 2)::float8 AS price_move_pct,
       round(p.p_end)::bigint AS cur_price
FROM cur c
JOIN base b USING (item_id)
JOIN items i USING (item_id)
LEFT JOIN px p USING (item_id)
WHERE b.sd > 0 AND b.n >= 24 AND c.cur_vol >= 100
ORDER BY abs((c.cur_vol - b.mean) / b.sd) DESC
LIMIT $1`

func (c *Collector) sweepVolume(ctx context.Context, asOf time.Time) int {
	rows, err := c.Store.Pool.Query(ctx, volumeSweepSQL, c.Cfg.SnapshotTopN)
	if err != nil {
		log.Printf("collect: volume sweep: %v", err)
		return 0
	}
	defer rows.Close()
	type volRow struct {
		ItemID       int      `json:"item_id"`
		Name         string   `json:"name"`
		CurVol       int64    `json:"cur_vol"`
		Buys         int64    `json:"buys"`
		Sells        int64    `json:"sells"`
		Z            float64  `json:"z"`
		NBaseline    int64    `json:"n_baseline"`
		PriceMovePct *float64 `json:"price_move_pct"`
		CurPrice     *int64   `json:"cur_price"`
	}
	var parsed []volRow
	for rows.Next() {
		var r volRow
		if err := rows.Scan(&r.ItemID, &r.Name, &r.CurVol, &r.Buys, &r.Sells,
			&r.Z, &r.NBaseline, &r.PriceMovePct, &r.CurPrice); err != nil {
			log.Printf("collect: volume scan: %v", err)
			return 0
		}
		parsed = append(parsed, r)
	}
	return persist(c, ctx, asOf, "volume", parsed, func(r volRow) (int, string, bool) {
		return r.ItemID, r.Name, r.Z >= c.Cfg.VolZMin || r.Z <= -c.Cfg.VolZMin
	})
}

// --- seasonal lens: hour-of-week amplitude, whole market ---
// (Same computation as ge-mcp seasonal_scan; gates match its defaults.)

const seasonalSweepSQL = `
WITH raw5 AS (
  SELECT item_id,
         extract(dow from ts AT TIME ZONE 'utc')::int AS d,
         extract(hour from ts AT TIME ZONE 'utc')::int AS h,
         sum((coalesce(avg_high_price, avg_low_price) + coalesce(avg_low_price, avg_high_price)) / 2.0) AS sum_mid,
         count(*) AS n_mid,
         sum(coalesce(high_volume,0) + coalesce(low_volume,0)) AS vol
  FROM prices_5m
  WHERE avg_high_price IS NOT NULL OR avg_low_price IS NOT NULL
  GROUP BY 1, 2, 3
),
item_stats AS (
  SELECT item_id, sum(sum_mid)/sum(n_mid) AS mean_mid, sum(vol) AS tot_vol, sum(n_mid) AS tot_n
  FROM raw5 GROUP BY 1
),
pooled AS (
  SELECT r.item_id, (r.d*24 + r.h)::int AS b,
         sum(p.sum_mid)/nullif(sum(p.n_mid),0) AS mid, sum(p.n_mid) AS obs
  FROM raw5 r
  JOIN raw5 p ON p.item_id = r.item_id AND p.d = r.d
             AND (p.h = r.h OR p.h = (r.h+1)%24 OR p.h = (r.h+23)%24)
  GROUP BY 1, 2, r.d, r.h
),
gated AS (
  SELECT p.item_id, p.b, p.mid / s.mean_mid AS idx, p.obs
  FROM pooled p JOIN item_stats s USING (item_id)
  WHERE s.tot_vol::numeric / s.tot_n >= 500 AND s.mean_mid >= 250 AND p.obs >= 9
),
agg AS (
  SELECT item_id,
         min(idx) AS cheap_idx, max(idx) AS dear_idx,
         (array_agg(b ORDER BY idx ASC))[1]  AS cheap_bucket,
         (array_agg(b ORDER BY idx DESC))[1] AS dear_bucket,
         min(obs) AS min_bucket_obs
  FROM gated GROUP BY item_id HAVING count(*) = 168
)
SELECT a.item_id, i.name,
       round((a.dear_idx - a.cheap_idx)*100, 2)::float8 AS amplitude_pct,
       a.cheap_bucket, a.dear_bucket, a.min_bucket_obs,
       round(s.tot_vol::numeric / s.tot_n)::bigint AS avg_vol5m,
       round(s.mean_mid::numeric)::bigint AS mean_price
FROM agg a JOIN item_stats s USING (item_id) JOIN items i USING (item_id)
ORDER BY amplitude_pct DESC
LIMIT $1`

func (c *Collector) sweepSeasonal(ctx context.Context, asOf time.Time) int {
	// Full-history scan (~12s): run uncapped.
	tx, err := c.Store.Pool.Begin(ctx)
	if err != nil {
		log.Printf("collect: seasonal begin: %v", err)
		return 0
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = 0"); err != nil {
		log.Printf("collect: seasonal timeout off: %v", err)
		return 0
	}
	rows, err := tx.Query(ctx, seasonalSweepSQL, c.Cfg.SnapshotTopN)
	if err != nil {
		log.Printf("collect: seasonal sweep: %v", err)
		return 0
	}
	type seaRow struct {
		ItemID       int     `json:"item_id"`
		Name         string  `json:"name"`
		AmplitudePct float64 `json:"amplitude_pct"`
		CheapBucket  int     `json:"cheap_bucket"`
		DearBucket   int     `json:"dear_bucket"`
		MinBucketObs int64   `json:"min_bucket_obs"`
		AvgVol5m     int64   `json:"avg_vol5m"`
		MeanPrice    int64   `json:"mean_price"`
	}
	var parsed []seaRow
	for rows.Next() {
		var r seaRow
		if err := rows.Scan(&r.ItemID, &r.Name, &r.AmplitudePct, &r.CheapBucket,
			&r.DearBucket, &r.MinBucketObs, &r.AvgVol5m, &r.MeanPrice); err != nil {
			rows.Close()
			log.Printf("collect: seasonal scan: %v", err)
			return 0
		}
		parsed = append(parsed, r)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		log.Printf("collect: seasonal commit: %v", err)
		return 0
	}
	// Flips-first redesign: seasonal structure is timing evidence for the
	// flip lanes, not a strategy source — persist snapshots, queue nothing.
	return persist(c, ctx, asOf, "seasonal", parsed, func(r seaRow) (int, string, bool) {
		return r.ItemID, r.Name, false
	})
}

// --- band lens: current price vs 21d range (archetype-H entries) ---

const bandSweepSQL = `
WITH series AS (
  SELECT item_id,
         (coalesce(avg_high_price,avg_low_price)+coalesce(avg_low_price,avg_high_price))/2.0 AS mid,
         ts,
         coalesce(high_volume,0)+coalesce(low_volume,0) AS vol
  FROM prices_5m
  WHERE ts >= now() - interval '21 days'
    AND (avg_high_price IS NOT NULL OR avg_low_price IS NOT NULL)
),
agg AS (
  SELECT item_id, min(mid) AS lo, max(mid) AS hi,
         (array_agg(mid ORDER BY ts DESC))[1] AS cur,
         sum(vol) AS vol, count(*) AS obs
  FROM series GROUP BY 1
  HAVING count(*) >= 500 AND sum(vol) >= 100000 AND min(mid) >= 100
)
SELECT a.item_id, i.name,
       round(((a.cur - a.lo) / nullif(a.hi - a.lo, 0))::numeric, 3)::float8 AS range_pos,
       round((100*(a.hi - a.lo)/nullif(a.lo,0))::numeric, 1)::float8 AS width_pct,
       round(a.cur)::bigint AS cur_price, round(a.lo)::bigint AS range_low,
       round(a.hi)::bigint AS range_high, a.obs
FROM agg a JOIN items i USING (item_id)
WHERE a.hi > a.lo
ORDER BY ((a.cur - a.lo) / nullif(a.hi - a.lo, 0)) ASC
LIMIT $1`

func (c *Collector) sweepBand(ctx context.Context, asOf time.Time) int {
	rows, err := c.Store.Pool.Query(ctx, bandSweepSQL, c.Cfg.SnapshotTopN)
	if err != nil {
		log.Printf("collect: band sweep: %v", err)
		return 0
	}
	defer rows.Close()
	type bandRow struct {
		ItemID    int     `json:"item_id"`
		Name      string  `json:"name"`
		RangePos  float64 `json:"range_pos"`
		WidthPct  float64 `json:"width_pct"`
		CurPrice  int64   `json:"cur_price"`
		RangeLow  int64   `json:"range_low"`
		RangeHigh int64   `json:"range_high"`
		Obs       int64   `json:"obs"`
	}
	var parsed []bandRow
	for rows.Next() {
		var r bandRow
		if err := rows.Scan(&r.ItemID, &r.Name, &r.RangePos, &r.WidthPct,
			&r.CurPrice, &r.RangeLow, &r.RangeHigh, &r.Obs); err != nil {
			log.Printf("collect: band scan: %v", err)
			return 0
		}
		parsed = append(parsed, r)
	}
	// Flips-first redesign: band position is lane-B qualification evidence,
	// not a strategy source — persist snapshots, queue nothing.
	return persist(c, ctx, asOf, "band", parsed, func(r bandRow) (int, string, bool) {
		return r.ItemID, r.Name, false
	})
}

// --- lane F: volume flips ---
// Both legs traded within FlipFreshAge (a margin between a stale high and a
// fresh low was never simultaneously real), 24h volume >= FlipVolMin24h
// (deep commodity markets only), ranked by gp_cycle = post-tax margin x
// buy_limit (prices_1m.margin — already tax-adjusted, never recomputed).
// The signal gate is the same number: absolute gp per 4h cycle, no ratios.

const flipSweepSQL = `
WITH latest AS (
  SELECT DISTINCT ON (item_id) item_id, high, low, margin, high_time, low_time
  FROM prices_1m WHERE ts >= now() - interval '2 hours'
  ORDER BY item_id, ts DESC
),
vol AS (
  SELECT item_id, sum(coalesce(high_volume,0)+coalesce(low_volume,0)) AS vol24h
  FROM prices_5m WHERE ts >= now() - interval '24 hours' GROUP BY 1
)
SELECT l.item_id, i.name, l.high, l.low, l.margin,
       i.buy_limit,
       l.margin * i.buy_limit AS gp_cycle,
       extract(epoch from (now()-l.high_time))::int AS high_age_s,
       extract(epoch from (now()-l.low_time))::int  AS low_age_s,
       v.vol24h
FROM latest l JOIN vol v USING (item_id) JOIN items i USING (item_id)
WHERE l.margin IS NOT NULL AND l.margin > 0
  AND i.buy_limit > 0
  AND l.high_time >= now() - $1::interval
  AND l.low_time  >= now() - $1::interval
  AND v.vol24h >= $2
ORDER BY l.margin * i.buy_limit DESC
LIMIT $3`

func (c *Collector) sweepFlip(ctx context.Context, asOf time.Time) int {
	rows, err := c.Store.Pool.Query(ctx, flipSweepSQL,
		c.Cfg.FlipFreshAge.String(), c.Cfg.FlipVolMin24h, c.Cfg.SnapshotTopN)
	if err != nil {
		log.Printf("collect: flip sweep: %v", err)
		return 0
	}
	defer rows.Close()
	type flipRow struct {
		ItemID   int    `json:"item_id"`
		Name     string `json:"name"`
		High     int64  `json:"high"`
		Low      int64  `json:"low"`
		Margin   int64  `json:"margin"`
		BuyLimit int64  `json:"buy_limit"`
		GpCycle  int64  `json:"gp_cycle"`
		HighAgeS int    `json:"high_age_s"`
		LowAgeS  int    `json:"low_age_s"`
		Vol24h   int64  `json:"vol24h"`
	}
	var parsed []flipRow
	for rows.Next() {
		var r flipRow
		if err := rows.Scan(&r.ItemID, &r.Name, &r.High, &r.Low, &r.Margin,
			&r.BuyLimit, &r.GpCycle, &r.HighAgeS, &r.LowAgeS, &r.Vol24h); err != nil {
			log.Printf("collect: flip scan: %v", err)
			return 0
		}
		parsed = append(parsed, r)
	}
	return persist(c, ctx, asOf, "vflip", parsed, func(r flipRow) (int, string, bool) {
		return r.ItemID, r.Name, r.GpCycle >= c.Cfg.FlipGpCycleMin
	})
}

// --- lane B: high-value flips ---
// 10M+ items with a fresh two-sided market. Ranked by absolute post-tax
// margin; gp_cycle = margin x the units the research budget affords
// (bounded by buy_limit). Items the budget cannot buy one unit of are out.

const highValueSweepSQL = `
WITH latest AS (
  SELECT DISTINCT ON (item_id) item_id, high, low, margin, high_time, low_time
  FROM prices_1m WHERE ts >= now() - interval '2 hours'
  ORDER BY item_id, ts DESC
),
vol AS (
  SELECT item_id, sum(coalesce(high_volume,0)+coalesce(low_volume,0)) AS vol24h
  FROM prices_5m WHERE ts >= now() - interval '24 hours' GROUP BY 1
)
SELECT l.item_id, i.name, l.high, l.low, l.margin,
       i.buy_limit,
       least(greatest(i.buy_limit,1), $4::bigint / l.low) AS units_affordable,
       l.margin * least(greatest(i.buy_limit,1), $4::bigint / l.low) AS gp_cycle,
       extract(epoch from (now()-l.high_time))::int AS high_age_s,
       extract(epoch from (now()-l.low_time))::int  AS low_age_s,
       v.vol24h
FROM latest l JOIN vol v USING (item_id) JOIN items i USING (item_id)
WHERE l.margin IS NOT NULL AND l.margin > 0
  AND l.low >= $3
  AND l.low <= $4
  AND l.high_time >= now() - $1::interval
  AND l.low_time  >= now() - $1::interval
  AND v.vol24h >= $2
ORDER BY l.margin DESC
LIMIT $5`

func (c *Collector) sweepHighValue(ctx context.Context, asOf time.Time) int {
	rows, err := c.Store.Pool.Query(ctx, highValueSweepSQL,
		c.Cfg.FlipFreshAge.String(), c.Cfg.HighValueVolMin24h,
		c.Cfg.HighValueMinPrice, c.Cfg.ResearchBudgetGp, c.Cfg.SnapshotTopN)
	if err != nil {
		log.Printf("collect: high-value sweep: %v", err)
		return 0
	}
	defer rows.Close()
	type hvRow struct {
		ItemID          int    `json:"item_id"`
		Name            string `json:"name"`
		High            int64  `json:"high"`
		Low             int64  `json:"low"`
		Margin          int64  `json:"margin"`
		BuyLimit        int64  `json:"buy_limit"`
		UnitsAffordable int64  `json:"units_affordable"`
		GpCycle         int64  `json:"gp_cycle"`
		HighAgeS        int    `json:"high_age_s"`
		LowAgeS         int    `json:"low_age_s"`
		Vol24h          int64  `json:"vol24h"`
	}
	var parsed []hvRow
	for rows.Next() {
		var r hvRow
		if err := rows.Scan(&r.ItemID, &r.Name, &r.High, &r.Low, &r.Margin,
			&r.BuyLimit, &r.UnitsAffordable, &r.GpCycle, &r.HighAgeS, &r.LowAgeS, &r.Vol24h); err != nil {
			log.Printf("collect: high-value scan: %v", err)
			return 0
		}
		parsed = append(parsed, r)
	}
	return persist(c, ctx, asOf, "hvflip", parsed, func(r hvRow) (int, string, bool) {
		return r.ItemID, r.Name, r.GpCycle >= c.Cfg.HighValueGpCycleMin
	})
}

// --- watch lens: revalidation of the ranked portfolio ---

// sweepWatch queues a 'watch' signal for portfolio entries that haven't been
// validated recently, so research runs re-prove them (ship a fresh strategy)
// or decay them (dismiss). This is the loop that keeps the "good" stack
// honest: without it an entry's score would just coast on its history.
func (c *Collector) sweepWatch(ctx context.Context) int {
	due, err := c.Store.WatchDueForRevalidation(ctx, c.Cfg.RevalidateAfter, 10)
	if err != nil {
		log.Printf("collect: watch due: %v", err)
		return 0
	}
	newSignals := 0
	for _, w := range due {
		metrics := map[string]any{
			"watch_id": w.WatchID, "score": w.Score, "eff_score": w.EffScore,
			"source": w.Source, "times_confirmed": w.TimesConfirmed,
			"times_validated": w.TimesValidated,
		}
		if w.Archetype != nil {
			metrics["archetype"] = *w.Archetype
		}
		if w.Note != nil {
			metrics["note"] = *w.Note
		}
		if w.LastResult != nil {
			metrics["last_result"] = *w.LastResult
		}
		isNew, err := c.Store.UpsertSignal(ctx, "watch", w.ItemID, w.ItemName, metrics)
		if err != nil {
			log.Printf("collect: watch signal %d: %v", w.ItemID, err)
			continue
		}
		if isNew {
			newSignals++
			log.Printf("collect: revalidation due: %s (eff score %.2f)", w.ItemName, w.EffScore)
		}
	}
	return newSignals
}

// persist stores the sweep's rows as trend snapshots and queues signals for
// rows the lens flags as significant (flag returns item_id, name,
// significant). Returns the count of NEW signals.
func persist[T any](c *Collector, ctx context.Context, asOf time.Time, lens string, rows []T, flag func(T) (int, string, bool)) int {
	trend := make([]store.TrendRow, 0, len(rows))
	for _, r := range rows {
		id, _, _ := flag(r)
		m, _ := json.Marshal(r)
		trend = append(trend, store.TrendRow{AsOf: asOf, Lens: lens, ItemID: id, Metrics: m})
	}
	if err := c.Store.InsertTrendSnapshots(ctx, asOf, lens, trend); err != nil {
		log.Printf("collect: %s snapshots: %v", lens, err)
	}
	newSignals := 0
	for _, r := range rows {
		id, name, significant := flag(r)
		if !significant {
			continue
		}
		isNew, err := c.Store.UpsertSignal(ctx, lens, id, name, r)
		if err != nil {
			log.Printf("collect: %s signal %d: %v", lens, id, err)
			continue
		}
		if isNew {
			newSignals++
			log.Printf("collect: new %s signal: %s", lens, name)
		}
	}
	return newSignals
}
