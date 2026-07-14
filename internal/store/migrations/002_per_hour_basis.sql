-- v2: money rates move from a per-4h (buy-limit window) basis to per-hour.
-- Renames propagate into the scoreboard view automatically; its ratio
-- (median realized / projected) is scale-invariant, so no view change.
ALTER TABLE orchestrator.strategies  RENAME COLUMN per_4h_gp TO per_1h_gp;
ALTER TABLE orchestrator.evaluations RENAME COLUMN realized_per_4h_gp TO realized_per_1h_gp;
UPDATE orchestrator.strategies  SET per_1h_gp = per_1h_gp / 4;
UPDATE orchestrator.evaluations SET realized_per_1h_gp = realized_per_1h_gp / 4;
