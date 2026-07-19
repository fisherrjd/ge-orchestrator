-- v8: flips-first redesign. Two discovery lanes replace the ratio-ranked
-- funnel: 'vflip' (lane F — volume flips: vol24h >= 100k, ranked and gated
-- by gp_cycle = post-tax margin x buy_limit) and 'hvflip' (lane B —
-- high-value flips: 10M+ items, ranked by absolute margin, sized by what
-- the research budget affords). Seasonal/band lenses keep their snapshots
-- (timing/qualification evidence) but no longer queue signals. Strategy
-- archetypes gain F and B; S and H are retired at the directive level but
-- stay valid here so historical rows keep evaluating.
-- Old 'flip' lens/kind values stay in the CHECKs for existing rows.

ALTER TABLE orchestrator.trend_snapshots DROP CONSTRAINT trend_snapshots_lens_check;
ALTER TABLE orchestrator.trend_snapshots
  ADD CONSTRAINT trend_snapshots_lens_check
  CHECK (lens IN ('seasonal','volume','band','flip','vflip','hvflip'));

ALTER TABLE orchestrator.signals DROP CONSTRAINT signals_kind_check;
ALTER TABLE orchestrator.signals
  ADD CONSTRAINT signals_kind_check
  CHECK (kind IN ('seasonal','volume','band','watch','flip','vflip','hvflip'));

ALTER TABLE orchestrator.strategies DROP CONSTRAINT strategies_archetype_check;
ALTER TABLE orchestrator.strategies
  ADD CONSTRAINT strategies_archetype_check
  CHECK (archetype IN ('F','B','S','V','C','U','H')) NOT VALID;

-- watchlist.archetype records the source strategy's kind on outcome.
DO $$
DECLARE
  cname text;
BEGIN
  SELECT conname INTO cname
  FROM pg_constraint
  WHERE conrelid = 'orchestrator.watchlist'::regclass
    AND contype = 'c'
    AND pg_get_constraintdef(oid) LIKE '%archetype%';
  IF cname IS NOT NULL THEN
    EXECUTE format('ALTER TABLE orchestrator.watchlist DROP CONSTRAINT %I', cname);
  END IF;
END $$;
ALTER TABLE orchestrator.watchlist
  ADD CONSTRAINT watchlist_archetype_check
  CHECK (archetype IN ('F','B','S','V','C','U','H'));

-- The F/B execution contract: offer cadence, longest safe unattended
-- window, reaction risk. Replaces the retired global low-touch constraint.
ALTER TABLE orchestrator.strategies ADD COLUMN IF NOT EXISTS attention text;

