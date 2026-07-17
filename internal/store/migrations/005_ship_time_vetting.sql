-- v5: ship-time vetting. Strategies rejected at ingest (kill price already
-- breached at ship time, duplicate of an open item+archetype, or over the
-- run's capital budget) are recorded as state='vetoed' instead of silently
-- dropped, so the dashboard and the next run's brief can see what bounced.

-- 1. State machine gains 'vetoed' (named by 004, drop by name).
ALTER TABLE orchestrator.strategies DROP CONSTRAINT strategies_state_check;
ALTER TABLE orchestrator.strategies
  ADD CONSTRAINT strategies_state_check
  CHECK (state IN ('armed','open','confirmed','killed','expired','vetoed')) NOT VALID;

-- 2. Scoreboard: vetoed rows get their own column and stay out of n, so
--    "closed" (n - open - armed) still means "closed by paper-trading".
DROP VIEW orchestrator.scoreboard;
CREATE VIEW orchestrator.scoreboard AS
SELECT s.archetype,
       count(*) FILTER (WHERE s.state <> 'vetoed')             AS n,
       count(*) FILTER (WHERE s.state = 'confirmed')           AS confirmed,
       count(*) FILTER (WHERE s.state = 'killed')              AS killed,
       count(*) FILTER (WHERE s.state = 'expired')             AS expired,
       count(*) FILTER (WHERE s.state = 'open')                AS open,
       count(*) FILTER (WHERE s.state = 'armed')               AS armed,
       count(*) FILTER (WHERE s.state = 'vetoed')              AS vetoed,
       round(avg(r.ratio) FILTER (WHERE s.state NOT IN ('open','armed','vetoed'))::numeric, 2) AS realized_vs_projected
FROM orchestrator.strategies s
LEFT JOIN LATERAL (
  SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY e.realized_per_1h_gp)
         / nullif(s.per_1h_gp, 0) AS ratio
  FROM orchestrator.evaluations e
  WHERE e.strategy_id = s.strategy_id
    AND (s.triggered_at IS NULL OR e.at >= s.triggered_at)  -- V: armed ticks don't count
) r ON true
GROUP BY s.archetype;
