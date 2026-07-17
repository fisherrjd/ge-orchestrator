-- v6: the watch portfolio. Ideas judged good — by the operator or by a
-- confirmed paper-trade — persist here as ranked entries instead of vanishing
-- when their strategy closes. Each entry carries a score that outcomes update
-- (confirm boosts, kill/expiry/dismissal decays) and that the ranking decays
-- by time since last validation, so a onetime winner sinks unless it keeps
-- re-proving itself. The collector re-queues stale entries as 'watch' signals
-- for research runs to revalidate.

CREATE TABLE orchestrator.watchlist (
  watch_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  item_id           int         NOT NULL,
  item_name         text        NOT NULL,
  archetype         text        CHECK (archetype IN ('S','V','C','U','H')),  -- NULL = idea not tied to one kind
  note              text,       -- why this is on the list (operator words or the confirming sid)
  source            text        NOT NULL CHECK (source IN ('operator','confirmed')),
  score             double precision NOT NULL DEFAULT 1.0,
  times_validated   int         NOT NULL DEFAULT 0,
  times_confirmed   int         NOT NULL DEFAULT 0,
  last_result       text,       -- confirmed | killed | expired | dismissed
  last_validated_at timestamptz,
  status            text        NOT NULL DEFAULT 'active' CHECK (status IN ('active','retired')),
  created_at        timestamptz NOT NULL DEFAULT now()
);
-- One live entry per (item, archetype-or-any): re-promotions boost the
-- existing entry instead of stacking duplicates.
CREATE UNIQUE INDEX watchlist_live_idx ON orchestrator.watchlist (item_id, coalesce(archetype, '*'))
  WHERE status = 'active';

-- Signals gain the 'watch' kind (revalidation assignments). 004 created the
-- kind CHECK inline -> look it up by definition, drop, re-add (003 pattern).
DO $$
DECLARE
  cname text;
BEGIN
  SELECT conname INTO cname
  FROM pg_constraint
  WHERE conrelid = 'orchestrator.signals'::regclass
    AND contype = 'c'
    AND pg_get_constraintdef(oid) LIKE '%kind%';
  IF cname IS NOT NULL THEN
    EXECUTE format('ALTER TABLE orchestrator.signals DROP CONSTRAINT %I', cname);
  END IF;
END $$;
ALTER TABLE orchestrator.signals
  ADD CONSTRAINT signals_kind_check
  CHECK (kind IN ('seasonal','volume','band','watch'));
