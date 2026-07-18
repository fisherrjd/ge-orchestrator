-- v7: the operator's "good pattern" as a collector lens. Screen: both legs
-- traded recently (fresh instabuy AND instasell), real daily volume, ranked
-- by post-tax margin, capped at prices the bankroll can actually trade.
-- Snapshots persist under lens 'flip'; strong hits queue as 'flip' signals
-- for research runs to investigate (the margin is the SYMPTOM to explain,
-- never the thesis — the directive's no-passive-spread-flips rule stands).

DO $$
DECLARE
  cname text;
BEGIN
  SELECT conname INTO cname
  FROM pg_constraint
  WHERE conrelid = 'orchestrator.trend_snapshots'::regclass
    AND contype = 'c'
    AND pg_get_constraintdef(oid) LIKE '%lens%';
  IF cname IS NOT NULL THEN
    EXECUTE format('ALTER TABLE orchestrator.trend_snapshots DROP CONSTRAINT %I', cname);
  END IF;
END $$;
ALTER TABLE orchestrator.trend_snapshots
  ADD CONSTRAINT trend_snapshots_lens_check
  CHECK (lens IN ('seasonal','volume','band','flip'));

ALTER TABLE orchestrator.signals DROP CONSTRAINT signals_kind_check;
ALTER TABLE orchestrator.signals
  ADD CONSTRAINT signals_kind_check
  CHECK (kind IN ('seasonal','volume','band','watch','flip'));
