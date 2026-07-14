-- orchestrator schema v1: runs, strategies, evaluations, scoreboard.
-- Applied by the embedded migrator at startup (idempotent via schema_version).

CREATE TABLE orchestrator.runs (
  run_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  started_at   timestamptz NOT NULL DEFAULT now(),
  finished_at  timestamptz,
  status       text NOT NULL DEFAULT 'running'
               CHECK (status IN ('running','succeeded','failed')),
  brief        jsonb NOT NULL,
  brief_text   text  NOT NULL,
  report_path  text,
  report_md    text,
  fail_reason  text
);

-- One running run ever, even across orchestrator restarts / double-starts.
CREATE UNIQUE INDEX runs_one_running ON orchestrator.runs ((true)) WHERE status = 'running';

CREATE TABLE orchestrator.strategies (
  strategy_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_id           bigint NOT NULL REFERENCES orchestrator.runs,
  sid              text   NOT NULL,
  archetype        text   NOT NULL CHECK (archetype IN ('A','B','C','D','E','F','G')),
  title            text   NOT NULL,
  thesis           text   NOT NULL,
  items            jsonb  NOT NULL,
  primary_item_id  int    NOT NULL,
  entry_text       text   NOT NULL,
  exit_text        text   NOT NULL,
  entry_price      bigint NOT NULL,
  exit_price       bigint NOT NULL,
  kill_price       bigint,
  horizon_text     text   NOT NULL,
  eval_window      interval NOT NULL DEFAULT interval '48 hours',
  capital_required bigint,
  units_used       bigint,
  per_cycle_gp     bigint,
  per_4h_gp        bigint,
  per_day_gp       bigint,
  roi_pct          numeric,
  confidence       text NOT NULL,
  confidence_why   text,
  evidence         text,
  invalidation     text NOT NULL,
  risks            jsonb,
  paper_trade      text,
  state            text NOT NULL DEFAULT 'open'
                   CHECK (state IN ('open','confirmed','killed','expired')),
  state_reason     text,
  opened_at        timestamptz NOT NULL,
  closed_at        timestamptz,
  UNIQUE (run_id, sid)
);
CREATE INDEX strategies_open_idx ON orchestrator.strategies (state) WHERE state = 'open';

CREATE TABLE orchestrator.evaluations (
  strategy_id  bigint NOT NULL REFERENCES orchestrator.strategies,
  at           timestamptz NOT NULL,
  -- frozen snapshot of exactly what the evaluator saw; never recomputed later
  cur_high     bigint,
  cur_low      bigint,
  high_age_s   int,
  low_age_s    int,
  cur_margin   bigint,
  vol_30m      bigint,
  realized_per_4h_gp bigint,
  checks       jsonb NOT NULL,
  verdict      text NOT NULL CHECK (verdict IN ('healthy','degraded','kill_signal')),
  PRIMARY KEY (strategy_id, at)
);

CREATE VIEW orchestrator.scoreboard AS
SELECT s.archetype,
       count(*)                                                AS n,
       count(*) FILTER (WHERE s.state = 'confirmed')           AS confirmed,
       count(*) FILTER (WHERE s.state = 'killed')              AS killed,
       count(*) FILTER (WHERE s.state = 'expired')             AS expired,
       count(*) FILTER (WHERE s.state = 'open')                AS open,
       round(avg(r.ratio) FILTER (WHERE s.state <> 'open')::numeric, 2) AS realized_vs_projected
FROM orchestrator.strategies s
LEFT JOIN LATERAL (
  SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY e.realized_per_4h_gp)
         / nullif(s.per_4h_gp, 0) AS ratio
  FROM orchestrator.evaluations e WHERE e.strategy_id = s.strategy_id
) r ON true
GROUP BY s.archetype;
