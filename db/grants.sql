-- ge-orchestrator role: owner of its own schema, read-only on the price data.
--
-- The orchestrator paper-trades strategies against prices_1m/prices_5m (public
-- schema, owned by the ingester's ge-data role) and stores runs / strategies /
-- evaluations in its own `orchestrator` schema. It must never write to public.
--
-- Run on eldo as a superuser, once, against the (quoted, hyphenated) "ge-data"
-- database. Password set out-of-band, same discipline as "ge-mcp":
--   ALTER ROLE "ge-orchestrator" PASSWORD '...';

CREATE ROLE "ge-orchestrator" LOGIN;                       -- password set out-of-band

GRANT CONNECT ON DATABASE "ge-data" TO "ge-orchestrator";

-- Own schema: full control, tables created by the service's migrations.
CREATE SCHEMA orchestrator AUTHORIZATION "ge-orchestrator";

-- Read-only on the price data.
GRANT USAGE ON SCHEMA public TO "ge-orchestrator";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO "ge-orchestrator";
-- FOR ROLE "ge-data": default privileges attach to the creating role, and the
-- ingester's ge-data role owns/creates the public tables.
ALTER DEFAULT PRIVILEGES FOR ROLE "ge-data" IN SCHEMA public GRANT SELECT ON TABLES TO "ge-orchestrator";
