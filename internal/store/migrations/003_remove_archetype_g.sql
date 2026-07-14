-- Alching dropped as a strategy (2026-07-14): retire archetype G.
-- Historical G rows are kept for scoreboard history; the new CHECK is added
-- NOT VALID so it only gates new rows.

UPDATE orchestrator.strategies
   SET state = 'expired',
       state_reason = 'archetype G (alch) retired',
       closed_at = now()
 WHERE archetype = 'G' AND state = 'open';

-- The original CHECK was declared inline (auto-named); look it up rather than
-- assume the name.
DO $$
DECLARE
  cname text;
BEGIN
  SELECT conname INTO cname
  FROM pg_constraint
  WHERE conrelid = 'orchestrator.strategies'::regclass
    AND contype = 'c'
    AND pg_get_constraintdef(oid) LIKE '%archetype%';
  IF cname IS NOT NULL THEN
    EXECUTE format('ALTER TABLE orchestrator.strategies DROP CONSTRAINT %I', cname);
  END IF;
END $$;

ALTER TABLE orchestrator.strategies
  ADD CONSTRAINT strategies_archetype_check
  CHECK (archetype IN ('A','B','C','D','E','F')) NOT VALID;
