-- The windowing demo's destination tables: one per processor, so the wasm
-- and plugin paths land in visibly separate places.
--
-- `PostgresSink` never issues CREATE TABLE, so these have to exist before
-- the sinks' first write. Compose mounts this file into the container's
-- /docker-entrypoint-initdb.d/, which PostgreSQL runs once on first
-- initialisation of an empty data directory.
--
-- Columns match the `WindowTotal` component's schema field for field, in
-- order: window_id, symbol, count, sum. The primary key makes the sinks'
-- `upsert` on (window_id, symbol) idempotent across re-runs and late
-- re-fires.

CREATE TABLE wasm_window_totals (
    window_id BIGINT NOT NULL,
    symbol    TEXT NOT NULL,
    count     BIGINT NOT NULL,
    sum       DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_id, symbol)
);

CREATE TABLE plugin_window_totals (
    window_id BIGINT NOT NULL,
    symbol    TEXT NOT NULL,
    count     BIGINT NOT NULL,
    sum       DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_id, symbol)
);
