-- v4 (2026-07 re-architecture): archetypes A-F retired, replaced by
-- S (seasonal window) / V (volume anomaly, armed) / C (conversion, multi-leg)
-- / U (update/event) / H (swing hold). Adds the kind-specific structured
-- columns the per-kind evaluator interprets, the 'armed' state for V, the
-- evaluations detail column, and the collector's trend/signal tables.

-- 1. Close every open legacy strategy (003 pattern: history rows keep their
--    letters via NOT VALID checks; nothing old is ever re-evaluated).
UPDATE orchestrator.strategies
   SET state = 'expired',
       state_reason = 'archetype set A-F retired (replaced by S/V/C/U/H)',
       closed_at = now()
 WHERE state = 'open';

-- 2. Archetype CHECK: named by 003, drop by name; NOT VALID keeps closed A-G rows.
ALTER TABLE orchestrator.strategies DROP CONSTRAINT strategies_archetype_check;
ALTER TABLE orchestrator.strategies
  ADD CONSTRAINT strategies_archetype_check
  CHECK (archetype IN ('S','V','C','U','H')) NOT VALID;

-- 3. State machine gains 'armed' (V ships armed; the trigger firing opens it).
--    001's state CHECK is inline/auto-named -> look it up (003 pattern).
DO $$
DECLARE
  cname text;
BEGIN
  SELECT conname INTO cname
  FROM pg_constraint
  WHERE conrelid = 'orchestrator.strategies'::regclass
    AND contype = 'c'
    AND pg_get_constraintdef(oid) LIKE '%state%'
    AND pg_get_constraintdef(oid) NOT LIKE '%archetype%';
  IF cname IS NOT NULL THEN
    EXECUTE format('ALTER TABLE orchestrator.strategies DROP CONSTRAINT %I', cname);
  END IF;
END $$;
ALTER TABLE orchestrator.strategies
  ADD CONSTRAINT strategies_state_check
  CHECK (state IN ('armed','open','confirmed','killed','expired')) NOT VALID;

-- 4. Kind-specific structured fields (validated upstream at the agent gate;
--    nullable because each belongs to one kind).
ALTER TABLE orchestrator.strategies
  ADD COLUMN buy_window   jsonb,        -- S: {from_how, to_how} 0-167 UTC
  ADD COLUMN sell_window  jsonb,        -- S
  ADD COLUMN trigger      jsonb,        -- V: {metric, threshold, direction, window}
  ADD COLUMN direction    text,         -- V/U: ride | fade
  ADD COLUMN legs         jsonb,        -- C: [{item_id, name, side, qty, price}]
  ADD COLUMN relation_id  int,          -- C: item_relations row
  ADD COLUMN event        jsonb,        -- U: {date, description}
  ADD COLUMN triggered_at timestamptz;  -- V: armed -> open moment; eval clock anchor

-- 5. The evaluator now walks armed rows too, and honors eval_window.
DROP INDEX orchestrator.strategies_open_idx;
CREATE INDEX strategies_open_idx ON orchestrator.strategies (state)
  WHERE state IN ('open','armed');

-- 6. Per-kind evaluation detail (window stats, trigger values, per-leg
--    snapshots, raw-vs-haircut realized) — frozen like the scalar columns.
ALTER TABLE orchestrator.evaluations ADD COLUMN detail jsonb;

-- 7. Scoreboard learns the armed state. Column list changes -> recreate.
DROP VIEW orchestrator.scoreboard;
CREATE VIEW orchestrator.scoreboard AS
SELECT s.archetype,
       count(*)                                                AS n,
       count(*) FILTER (WHERE s.state = 'confirmed')           AS confirmed,
       count(*) FILTER (WHERE s.state = 'killed')              AS killed,
       count(*) FILTER (WHERE s.state = 'expired')             AS expired,
       count(*) FILTER (WHERE s.state = 'open')                AS open,
       count(*) FILTER (WHERE s.state = 'armed')               AS armed,
       round(avg(r.ratio) FILTER (WHERE s.state NOT IN ('open','armed'))::numeric, 2) AS realized_vs_projected
FROM orchestrator.strategies s
LEFT JOIN LATERAL (
  SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY e.realized_per_1h_gp)
         / nullif(s.per_1h_gp, 0) AS ratio
  FROM orchestrator.evaluations e
  WHERE e.strategy_id = s.strategy_id
    AND (s.triggered_at IS NULL OR e.at >= s.triggered_at)  -- V: armed ticks don't count
) r ON true
GROUP BY s.archetype;

-- 8. Collector: full-market sweep snapshots (the durable market-intelligence
--    base) and the signal work queue that feeds run briefs.
CREATE TABLE orchestrator.trend_snapshots (
  as_of    timestamptz NOT NULL,
  lens     text        NOT NULL CHECK (lens IN ('seasonal','volume','band')),
  item_id  int         NOT NULL,
  metrics  jsonb       NOT NULL,
  PRIMARY KEY (as_of, lens, item_id)
);
CREATE INDEX trend_snapshots_lens_idx ON orchestrator.trend_snapshots (lens, item_id, as_of DESC);

CREATE TABLE orchestrator.signals (
  signal_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  kind        text        NOT NULL CHECK (kind IN ('seasonal','volume','band')),
  item_id     int         NOT NULL,
  item_name   text        NOT NULL,
  metrics     jsonb       NOT NULL,
  status      text        NOT NULL DEFAULT 'pending'
              CHECK (status IN ('pending','assigned','investigated','dismissed')),
  run_id      bigint REFERENCES orchestrator.runs,
  created_at  timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz,
  reason      text
);
-- One live (pending/assigned) signal per (kind, item): re-detections refresh
-- metrics instead of stacking duplicates.
CREATE UNIQUE INDEX signals_live_idx ON orchestrator.signals (kind, item_id)
  WHERE status IN ('pending','assigned');
CREATE INDEX signals_status_idx ON orchestrator.signals (status, created_at);
