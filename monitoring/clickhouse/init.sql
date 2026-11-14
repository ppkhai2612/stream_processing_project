CREATE DATABASE IF NOT EXISTS stream_processing_project;

-- Windowed event-count rollup: one row per (window, category, country) fired by
-- the PyFlink windowed-revenue job, which processes ALL event types (a
-- full-throughput stress test, not just purchases -- no revenue/amount field
-- since most event types don't carry one). window_start/window_end let you
-- measure window trigger latency against wall-clock insert time.
CREATE TABLE IF NOT EXISTS stream_processing_project.windowed_revenue
(
    window_start DateTime64(3),
    window_end   DateTime64(3),
    category     String,
    country      String,
    order_count  UInt32,
    inserted_at  DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (window_start, category, country);

-- Raw event passthrough: one row per event (no windowing/aggregation),
-- written by raw_event_sink.py. Preserves the original event_time so
-- event_time -> inserted_at gives TRUE single-event pipeline latency,
-- unlike windowed_revenue's window_end -> inserted_at (which bakes in the
-- window's deliberate wait time on top of real pipeline overhead). See
-- latency_breakdown.py for decomposing the two into real overhead vs
-- windowing wait.
CREATE TABLE IF NOT EXISTS stream_processing_project.raw_events
(
    event_id    String,
    event_time  DateTime64(3),
    event_type  String,
    category    String,
    country     String,
    inserted_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (event_time);