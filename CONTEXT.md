# Project Context (read this first)

This file exists so a fresh session (e.g. Claude Code) can pick up exactly where the design conversation left off, without re-deriving decisions that were already made and reasoned through.

## Goal

A personal learning project for a data engineer to learn **Kafka operations and performance tuning** hands-on, with **Apache Flink** planned as the next phase (end-to-end stream processing: Kafka produces → Flink consumes/transforms → some sink).

## Environment

- Dell Latitude 5480 (4-core, 8GB RAM)
- Docker Desktop
- Python (`confluent-kafka`, not `kafka-python` — chosen because it wraps librdkafka and exposes more real tuning knobs, and its performance profile is closer to production clients)

## Decision history (why the repo looks the way it does)

1. **Kafka runs in KRaft mode** (no ZooKeeper) — simpler docker-compose, and it's the direction the Kafka project has moved.

2. **Built a reusable load-testing harness first** (`producer.py`, `consumer.py`) rather than one-off scripts, specifically so the same tooling can be reused across every tuning experiment (partitions, `linger.ms`, compression, acks, resource limits, slow consumers).

3. **Throughput ceiling math was worked out before running anything**, to have a ceiling to sanity-check real results against:

   - Single Python producer process: Python/librdkafka call overhead (~3-8us/call) is usually the binding constraint for small messages, not Kafka or Docker's network layer, not disk.
   - 5 MB/s: trivial, no tuning needed.
   - 5,000,000 msgs/sec: not achievable on this hardware (would need ~33 concurrent CPU-bound producer processes on 10 cores, and exceeds Docker Desktop's virtualized network throughput).
   - "High" production throughput is domain-dependent -- most real systems (transactional events, mid-size clickstream, IoT) run at 1K-50K msgs/sec / low-tens-of-MB/s. Only ad tech, observability platforms, and large-scale clickstream at big-tech scale need GB/s-class numbers. **Most real-time systems are latency-bound, not throughput-bound** -- that's the actual point of Kafka + stream processing for most use cases, not raw volume.

4. **The payload was redesigned from arbitrary padded bytes into a real e-commerce clickstream/order event schema** (`event_gen.py`), because the original random-padding payload was explicitly called out as "nonsense" once Flink entered the picture -- a real Flink pipeline needs a real schema to do sessionization, funnel analysis, windowed aggregation, etc. against.

5. **Schema evolved in two passes**:

   - **First pass**: flat JSON event (event_id, event_time, event_type, user_id, session_id, device, referrer, country, product_id, category, price, quantity, search_query) -- average ~328 bytes.
   - **Added `amount` field** (`price * quantity`, populated on `add_to_cart`/`purchase` only) as an explicit measure/metric field, specifically so a later Flink windowed-revenue job doesn't need to recompute it -- avg moved to ~344 bytes.
   - **Second pass, to reach ~1KB**: added nested `device` (type, os, browser, full user-agent string, screen_resolution), `geo` (city, region, lat/lon), `marketing` (referrer_url, utm_source, utm_campaign), `page_url`, `ab_test_variant`, richer product data (name, brand, tags, description, image_url), and a **running `cart` snapshot + `cart_value`** included on every event (not just cart events) -- this last one specifically enables cart-abandonment detection directly from the stream (a session with cart contents but no matching `purchase` within N minutes), without needing a join. Measured weighted average: **~1,099 bytes/event** (min ~871, max ~1,567, varies by event type -- `add_to_cart` and `purchase` are heaviest due to the full cart array).
   - These fields were deliberately chosen to mirror real clickstream platforms (Snowplow/Segment/Amplitude-style events), not chosen to hit a byte target -- the ~1KB outcome is a side effect of adding genuinely useful context, not padding.

6. **Kafka message key defaults to `session_id`** (`--key-field`, also supports `user_id` / `product_id` / `none`) -- chosen because the most natural first Flink job on this data is session-window sessionization, and Kafka's per-partition ordering guarantee only pays off if same-key events are already co-located on arrival. **This default hasn't been revisited since -- worth confirming it's still the right choice once the actual first Flink job is picked.**

7. **Measured (not assumed) that a single Python producer process cannot sustain 50 MB/s with this schema**:

   - 50 MB/s / ~1,099 bytes/event ~= 45,500 events/sec needed.
   - Benchmarked pure event-generation + serialization (no Kafka involved) on the reference container: **~29,600 events/sec (~32.5 MB/s)** -- already short of the target, and this excludes the actual `produce()`/network cost, so the real single-process ceiling talking to Kafka is lower still.
   - The bottleneck is `next_event()` itself (~21,000/sec alone -- weighted `random.choices()` for device/geo/referrer/AB-variant, nested dict construction, cart list copying), not `json.dumps()` (~103,000/sec alone) and not Kafka.
   - **Conclusion: hitting 50 MB/s requires multiple parallel producer processes** (see `multi_producer.py`), not tuning a single process harder. Rough math: need ~1.5-2x one process's capacity at minimum; 3 processes recommended for real headroom once `produce()`/network overhead is included.
   - This number (~29,600/sec) was measured **inside a sandboxed dev container**, not on the user's actual MacBook -- it's a reference point to validate against, not a guarantee. Re-run `benchmark_generator.py` on the real machine before trusting it.

## Current repo state

| File | Purpose | Status |
|---|---|---|
| `docker-compose.yml` | Single-broker Kafka (KRaft) + Redpanda Console UI | Done, not yet run against real broker |
| `event_gen.py` | Clickstream/order event generator (see schema evolution above) | Done |
| `producer.py` | Configurable producer using `event_gen.py`; `--key-field`, `--rate`, `--acks`, `--linger-ms`, `--compression`, `--seed`, etc. | Done |
| `consumer.py` | Configurable consumer; reports throughput, consumer lag, end-to-end latency (parses `event_time`, epoch ms) | Done |
| `benchmark_generator.py` | Isolates generator+serialization speed from Kafka/network cost | Done, added to formalize the ad-hoc benchmark run during design |
| `multi_producer.py` | Spawns N parallel `producer.py` processes to get past the single-process ceiling | Done, **not yet run/validated end-to-end** |
| `setup_topic.sh` | Creates/recreates the topic with a given partition count | Done |
| `requirements.txt` | `confluent-kafka==2.5.0` | Done |
| `README.md` | Setup instructions, experiment plan, schema docs, Flink-readiness notes | Done, may need updating to reference `multi_producer.py` and `benchmark_generator.py` |

## Not yet done / open items

- **Nothing has been run against a live broker yet.** All measurements so far are either theoretical (Python/network math) or pure-Python benchmarks (`benchmark_generator.py`-style) with no Kafka involved. `docker compose up` + the actual baseline run is the next concrete step.
- `multi_producer.py` exists but hasn't been executed or validated -- worth confirming it actually achieves the ~45K events/sec combined rate needed for the 50 MB/s stress case once run for real.
- The six planned tuning experiments (partition scaling, `linger.ms` sweep, compression cost, acks/durability + broker-kill, resource starvation, slow-consumer/backpressure) are documented in `README.md` but **none have been executed yet**.
- Schema was left at ~1,099 bytes average rather than trimmed to exactly 1,024 -- if exact 1KB matters, the two cheapest trims identified were: shortening `description` templates, and/or reducing `cart` line items to `{product_id, quantity}` only (drop `name`/`price`, re-joinable from `product_id` downstream).
- Flink integration itself hasn't started -- this repo is Kafka-only so far. The "Looking ahead: Flink" section in `README.md` lists the candidate first jobs (sessionization, funnel/conversion, windowed revenue, cart abandonment, hot-key skew) based on what the schema was specifically designed to support.

## Phase 2: Flink + ClickHouse (decided, not yet built)

### Goal

Extend Kafka-only lab into a complete Kafka -> Flink -> ClickHouse pipeline, so tuning/learning covers all three tools, not just Kafka. Two concrete success criteria were set:

1. Sustain **>=30 MB/s** into Kafka through the full pipeline (not just at the producer -- Flink and ClickHouse must keep up without unbounded lag).
2. Be able to **visibly watch revenue update** end-to-end (producer -> Kafka -> Flink window fires -> ClickHouse row appears), i.e. window trigger latency and sink flush latency are both observable, not just theoretical.

### Decisions made

- **Sink: ClickHouse**, not Postgres/Redis/sink-to-Kafka. Chosen because it's purpose-built for "insert a stream of aggregated rows, query fast," and it has a native Grafana datasource -- reuses the existing dashboard instead of standing up a separate query tool.
- **First Flink job: windowed revenue** -- tumbling window sum of
  `amount` by `category`/`country`. Chosen over sessionization/cart
  abandonment as the *first* job specifically because it's pure
  windowing (no `KeyedProcessFunction`/timer state), so it exercises
  Kafka source -> window -> sink plumbing before adding stateful
  complexity. Cart abandonment (the job the running `cart`
  snapshot field was originally added for) is the intended second job,
  and is expected to need RocksDB state backend + timers where
  windowed-revenue does not.
  **Redis was flagged as the better long-term sink for cart-abandonment
  specifically** (point-lookup shaped, not aggregation-shaped) -- not
  yet decided whether to add it or keep everything in ClickHouse.
- **Language: PyFlink**, not Java. Chosen to match the rest of the
  repo's Python-first tooling and lower friction to start; Table/
  DataStream API is expected to cover windowed-revenue fine. Java was
  flagged as the fallback if cart-abandonment's stateful timer logic
  later feels too constrained in PyFlink -- not yet needed.
- **Deployment: Flink standalone session cluster** (jobmanager +
  taskmanager) added to the existing `docker-compose.yml`, same pattern
  as the Kafka stack, specifically so Flink's own Prometheus reporter
  and ClickHouse's Grafana datasource plug into the *existing*
  Prometheus/Grafana rather than standing up parallel monitoring.
- **`experiments/scenarios.yml` / `run_experiment.py` mechanism is
  being kept and extended**, not replaced. The original 12 Kafka-only
  scenarios were explicitly called out as disposable content and have
  since been deleted (2026-07-29) -- `scenarios.yml` now holds only the
  4 Kafka+Flink+ClickHouse scenarios (see "All four scenarios... done"
  below), reindexed 1-4. New scenarios need `flink:` and `clickhouse:`
  config sections (job/parallelism/checkpoint-interval; sink batch/flush
  config) and corresponding setup/teardown logic (submit/cancel Flink job,
  truncate
  ClickHouse table between runs), same shape as the existing
  broker/producer/consumer sections.

### Tuning research (trustworthy sources, gathered before writing any Flink/ClickHouse code)

- **Kafka producer** (already close to the 30 MB/s goal per README's
  measured ~110 MB/s w/ 3 processes): `linger.ms` + `batch.size` tradeoff
  is the main lever --
  [Confluent producer config reference](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html),
  [Confluent throughput tutorial](https://developer.confluent.io/confluent-tutorials/optimize-producer-throughput/kafka/).
- **Flink parallelism vs Kafka partition count**: misaligning the two
  stalls watermark progress (idle subtasks if parallelism > partitions,
  merged-partition watermarks if parallelism < partitions). Starting
  point: parallelism = 6 to match the existing topic's partition count.
  [Flink Kafka connector docs](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/datastream/kafka/).
- **Flink state backend**: `HashMapStateBackend` (heap, faster/lower
  latency, heap-bounded) vs `RocksDBStateBackend` (disk-bounded,
  serialization overhead, needed once state won't fit in heap).
  Windowed-revenue's state is small enough for heap; RocksDB becomes
  relevant at cart-abandonment. [Flink state backend docs](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/).
- **Flink checkpointing**: interval + incremental checkpoints are the
  durability/latency dial, directly relevant to "how fast do you see a
  revenue update." [Tuning Checkpoints and Large State](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/large_state_tuning/),
  [Getting into Low-Latency Gears with Apache Flink, part 1](https://flink.apache.org/2022/05/18/getting-into-low-latency-gears-with-apache-flink-part-one/) / [part 2](https://flink.apache.org/2022/05/23/getting-into-low-latency-gears-with-apache-flink-part-two/).
- **Flink watermarks**: delay controls how long the window waits for
  late `purchase`/`add_to_cart` events before firing -- too tight drops
  late data, too loose delays the revenue update.
  [Confluent watermarks course](https://developer.confluent.io/courses/apache-flink/timely-stream-processing/).
- **ClickHouse insert throughput**: batch size is the dominant lever --
  every INSERT creates an immutable part, so small/frequent inserts
  (a naive per-record sink) exhaust the merge queue ("Too many parts").
  Recommended: 10K-100K+ rows/insert, or async inserts if the client
  can't batch. [Selecting an insert strategy (ClickHouse docs)](https://clickhouse.com/docs/best-practices/selecting-an-insert-strategy),
  [Async inserts (Altinity)](https://altinity.com/blog/using-async-inserts-for-peak-data-loading-rates-in-clickhouse).

### Status: scenario 2 (Kafka -> Flink -> ClickHouse) is done and validated

`flink_jobs/windowed_revenue.py`, the Docker infra, and
`experiments/scenarios.yml` scenario 2 are complete and have been run
end-to-end for real (not just dry-run): producer sustained ~36-40 MB/s
(comfortably past the 30 MB/s goal), the Flink job stayed RUNNING, and
ClickHouse rows landed with correct revenue sums ~7-8s after each 30s
window closed (window fire + 5s watermark + sink latency).

**Written against the DataStream API (imperative Python), not Table
API/SQL** -- the user explicitly asked for this after the first draft was
SQL-based Table API. `key_by(category, country).window(...)` still keeps
state (one accumulator per key+window); it's just Flink-managed and
implicit, unlike the explicit `ValueState`/timers cart-abandonment will need.

**Three non-obvious bugs hit and fixed while getting this to actually
run** (worth knowing before touching `monitoring/flink/Dockerfile` or
`flink_jobs/windowed_revenue.py` again):

1. **Flink's JDBC connector is incompatible with PyFlink's DataStream
   `JdbcSink` Python wrapper** in this version pairing -- the wrapper
   reflectively calls a `JdbcOutputFormat` method
   (`createRowJdbcStatementBuilder`) that doesn't exist in the externally
   versioned `flink-connector-jdbc:3.2.0-1.19` jar. Fix: don't use Flink's
   JDBC connector at all -- the sink is a plain Python `MapFunction` using
   `clickhouse-connect` directly, terminated with `.print()` to give the
   job graph a sink node.
2. **The Kafka source's operator coordinator (FLIP-27) is deserialized on
   the JobManager using the job's own attached-jar classloader, not the
   static system classpath** -- connector jars dropped in `/opt/flink/lib`
   are visible to normal task execution but NOT to this specific
   coordinator-deserialization step, causing `ClassNotFoundException` for
   `KafkaSource` even though the jar is provably on the daemon's own
   classpath. Fix: `flink run -j <jar>` attaches a jar directly to the
   job's classloader. `-j` only accepts one jar, so
   `flink-connector-kafka` + `kafka-clients` are merged into one bundle at
   image-build time (Python `zipfile`, see Dockerfile) and referenced via
   `-j /opt/flink/connector-jars/flink-kafka-connector-bundle.jar`.
3. **Corollary of #2: if the same jar is ALSO left in `/opt/flink/lib`,
   you get a worse bug** -- two independent classloaders (system parent +
   job-attached) each load their own copy of Kafka's classes, so
   `instanceof` checks between them fail (`ByteArrayDeserializer is not an
   instance of Deserializer`) even though the classes have the same name.
   Fix: keep connector jars OUT of `/opt/flink/lib` entirely; the bundle
   lives only in `/opt/flink/connector-jars/`, loaded exactly once via `-j`.
4. **ClickHouse's official Docker image disables network access for the
   `default` user if no password env var is set** -- `clickhouse-client`
   run via `docker exec` (local/in-container) works fine even with no
   password configured, which masked this for a while; any network client
   (Flink's `clickhouse-connect`, Grafana's datasource) gets
   `AUTHENTICATION_FAILED` until `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD`
   are set on the container and threaded through to every client
   (`windowed_revenue.py` args, `run_experiment.py`, Grafana datasource
   config).

### Grafana dashboard: done, then pruned to 5 panels (2026-07-30)

Originally added 4 panels (19 total) to the existing Kafka Lab dashboard.
After deleting the 12 original Kafka-only scenarios, pruned the dashboard
down to only what the remaining 4 scenarios actually reference: **Broker
Bytes In/sec (JMX)** plus the 4 Flink/ClickHouse panels. Everything else
(Consumer Lag, Partition Offset Rate, Broker CPU%, JVM GC Pause Rate,
Producer Delivery Errors, Consumer Group Members, etc.) belonged to the
deleted scenarios and is gone.

Both datasources were given fixed `uid`s (`prometheus-uid`,
`clickhouse-uid`) in
`monitoring/grafana/provisioning/datasources/datasources.yml` so the
dashboard JSON can reference them explicitly.

**Two Grafana/Docker gotchas hit and fixed, both the same root cause**:
`grafana-data:/var/lib/grafana` is a named volume that overlays the
*entire* `/var/lib/grafana` directory at container start, including
`/dashboards` and the datasource DB -- baked-in files from the Docker
image (`COPY dashboards /var/lib/grafana/dashboards` in the Dockerfile)
only take effect on the volume's very first creation; every rebuild after
that is silently shadowed by whatever's already in the volume, no matter
how many times you rebuild the image (`--no-cache` included) or edit the
source file. First hit this with a stale datasource UID (crashed Grafana
entirely); hit it again editing the dashboard JSON (edits kept not
appearing, panel count stayed at 19 through multiple rebuilds). Fix both
times: `docker rm -f grafana && docker volume rm
stream_processing_project_grafana-data && docker compose up -d grafana`
-- config-only volume, safe to wipe, no real data lost.

**Also fixed a real metrics bug** while rebuilding the Flink panel: the
panel was using `numRecordsInPerSecond` on the *source* task and
`numRecordsOutPerSecond` on the *window+sink* task. Both are structurally
always 0 -- a source has no upstream task so its own `numRecordsIn` never
moves (its real output is `numRecordsOut`); the final operator has no
downstream task so its `numRecordsOut` never moves (its real input is
`numRecordsIn`). On top of that, Flink's own derived `*PerSecond` gauges
apparently don't get populated reliably for this PyFlink DataStream job
(stayed 0 throughout multiple real runs with confirmed nonzero ClickHouse
output). Fixed by swapping to the correct counter per task
(`numRecordsOut` for source, `numRecordsIn` for window+sink) and computing
`rate(...[30s])` over the raw cumulative counter in PromQL instead of
trusting Flink's gauge -- confirmed showing real ~3000/sec throughput
historically once fixed.

### Scenario categorization (decided): infra vs correctness

Scenarios are being split into two kinds, not just difficulty tiers:

- **Infra/"intra" scenarios** -- non-functional (throughput ceiling,
  end-to-end latency, parallelism/checkpoint/watermark/sink-batching
  tuning). Use the real randomized `event_gen.py` producer at scale.
- **Stream-processing-logic correctness scenarios** -- assert exact
  expected output (window sums, late-event inclusion/drop) against a
  small, fully-controlled event sequence. These need `event_injector.py`
  (new tool, see below), not the randomized generator.

### `event_injector.py`: deterministic event tool for correctness tests (done)

New script, separate from `producer.py`/`event_gen.py`. Takes a JSON list
of event specs (`event_time_offset`, `category`, `country`, `amount`,
optional `sleep_before` to control real send order independent of the
timestamp) and publishes them as real `purchase` events with exact,
hand-computable values -- makes assertions like "window X should show
revenue=35.00, count=3" possible, which is meaningless against the
randomized generator.

**Critical gotcha discovered while validating it**: Flink's
`TumblingEventTimeWindows` align to absolute epoch multiples of
`window_seconds`, *not* to whenever the test happens to start. A naive
first attempt (`event_time_offset=0` assumed to be a window start) landed
mid-window and split a "single window" test across two windows. Fixed by
adding `--align-window-seconds`, which snaps T0 forward to the next real
window boundary (+2s buffer) so offsets are predictable. Always pass the
same value as the job's `--window-seconds` when writing a correctness
test.

**Real bug found and fixed via this tool, in `windowed_revenue.py`
itself**: watermarks were assigned *after* the purchase-only filter, so
any Kafka partition that happened to get zero purchase events never
advanced its watermark at all -- and Flink merges per-partition watermarks
by taking the *minimum*, so one stalled partition blocked every window
from firing, permanently. This didn't surface during scenario 2's
high-volume random-load test (all 6 partitions got purchase traffic
quickly) but broke immediately with the injector's tiny 4-event test
(which only hits 1-2 partitions). Fixed by: (1) moving
`.assign_timestamps_and_watermarks()` before the filter, so watermarks
advance on any event type, not just purchases; (2) adding
`.with_idleness(Duration)` (new `--idleness-seconds` arg, default 60s,
threaded through `run_experiment.py`'s `submit_flink_job`) as a safety
net so a genuinely idle partition gets excluded from the watermark merge
instead of blocking forever.

Validated end-to-end: `experiments/events/basic_window.json` (3 events,
amounts 10/20/5 in one window + a 4th event to push the watermark past
it) produced exactly `revenue=35, order_count=3` in ClickHouse.

### Not yet done

- Both planned correctness scenarios are now built and validated:
  **basic window correctness** (`experiments/events/basic_window.json`)
  and **late-event threshold** (`experiments/events/late_event.json`).
  The late-event test sends 6 events on one Kafka partition (pinned via
  explicit `session_id`) in this order: two on-time events (100, 50),
  a "pusher" event that advances the watermark to just short of closing
  the window, a late-arriving-in-send-order-but-still-on-time event
  (999, included because the window hadn't fired yet), a second pusher
  that actually closes the window, then a truly-late event (12345) sent
  after the window already fired. Result matched exactly:
  `revenue=1149, order_count=3` (100+50+999) -- 999 included, 12345
  silently dropped, exactly as Flink's default no-`allowedLateness`
  behavior predicts. Both correctness scenarios from the 2+2 brainstorm
  are done; only the two infra scenarios remain.
### All four scenarios from the 2+2 brainstorm: done

- `category: infra | correctness` field added to `scenarios.yml` scenarios
  (1-4), alongside the existing `tier`.
- **Scenario 1** (infra): throughput+latency, formalized via a new
  `report_pipeline_metrics()` in `run_experiment.py` -- queries ClickHouse
  for row count, total revenue, and avg/max `window_end -> inserted_at`
  latency, printed automatically after any scenario with a `clickhouse:`
  block (or per-step, for scenarios with `steps`).
- **Scenario 2** (infra): ClickHouse sink batching tuning. Added
  `--sink-batch-rows` to `windowed_revenue.py`'s `ClickHouseSink` (buffers
  N rows per parallel sink instance before inserting; 1 = original
  per-row behavior). Required restructuring `run_scenario()` in
  `run_experiment.py`: Flink job reset/resubmit moved from
  once-before-the-step-loop to once-per-step (merging step overrides into
  `flink_cfg`), so a sweep scenario can test a different Flink job config
  per step, not just a different producer config. Validated with real
  numbers: batch_rows=1 -> avg_latency=2s max=4s; batch_rows=50 ->
  avg_latency=8.57s max=21s -- confirms the predicted latency/throughput
  tradeoff in the right direction. Caveat found: `flink cancel` is a hard
  stop, so rows still sitting in an unfilled batch buffer are lost (1218
  rows landed at batch_rows=1 vs 1050 at batch_rows=50, same producer
  load) -- fine for a tuning demo, but a real deployment would want a
  time-based flush too, not just a row-count threshold.
- **Scenarios 3/4** (correctness): basic window sum and late-event
  threshold, both registered as scenarios (pointing at
  `experiments/events/*.json`) even though `run_experiment.py` doesn't
  yet drive `event_injector.py` automatically -- `producers:` is
  intentionally omitted from these two (added a guard in `run_scenario()`
  so it skips launching a producer when absent, since a correctness
  scenario must NOT run the randomized producer at all). Steps 2-3 stay
  manual (`manual_steps` documents the exact `event_injector.py` command
  and the expected exact numbers).

### `windowed_revenue.py` changed to process ALL events, not just purchases (2026-07-30)

Discovered while debugging a mislabeled Grafana panel: the job's Kafka
ingest rate (all event types, ~36-55K/sec at 40-60MB/s) was nowhere near
the job's actual throughput (~2-2.5K/sec), because the `is_purchase`
filter meant only ~5-7% of events ever reached the window+sink stage --
the pipeline was never actually tested against full Kafka-level volume,
only the purchase subset. Decision: remove the `is_purchase` filter
entirely (deliberately, not behind a flag) so the job now processes every
event type. `order_count` is now "count of all events of any type" per
window/key, and `revenue` sums `amount` where present (0 for types like
`page_view` that don't carry one, per `event_gen.py`'s schema) --
`to_purchase_tuple`/`is_purchase` renamed to `to_event_tuple`/removed.
This makes scenario 2 an honest full-throughput stress test: Flink's true
ingest rate should now track Broker Bytes In/sec 1:1 (previously only the
purchase-filtered trickle was ever exercised end-to-end through the
window+sink stage). Scenarios 3/4 (correctness) are unaffected since their
injected test events are explicitly typed `purchase` and run against an
otherwise-empty topic -- no other traffic to conflate them with.

Also fixed alongside this: the Grafana "Flink Records In/Out per sec"
panel was using the wrong metric direction (see next section) --
discovered because the *true* ingest number, once correctly plumbed,
didn't match Broker Bytes In/sec at all, which is what led to finding the
`is_purchase`-filter issue in the first place.

### Grafana Flink panel bug: wrong metric per task role, plus a Docker volume gotcha (2026-07-30)

The panel was reading `numRecordsIn` on the *source* task and
`numRecordsOut` on the *final sink* task -- both are structurally always 0
by Flink's own metric semantics (a source has no upstream task, so its
`numRecordsIn` never moves; the final operator has no downstream task, so
its `numRecordsOut` never moves). Also, Flink's own derived
`numRecordsInPerSecond`/`numRecordsOutPerSecond` gauges didn't populate
reliably for this PyFlink job (stayed 0 despite confirmed real throughput
in ClickHouse). Fixed by using the correct counter per task
(`numRecordsOut` for source, `numRecordsIn` for window+sink) and computing
`rate(...[30s])` over the raw cumulative counter in PromQL instead of
trusting Flink's gauge. Further fixed to use the **operator-scoped**
metric (`flink_taskmanager_job_task_operator_numRecordsIn{operator_name="Source:_kafka_clickstream"}`)
for the "true kafka ingest" line specifically, since the *task*-scoped
source metric already reflects the fused chain's output post-filter, not
the raw pre-filter ingest -- operator-level metrics exist precisely to
distinguish between operators fused into the same task, which task-level
metrics can't.

**Also hit the exact same Docker volume gotcha twice more**: any edit to
`monitoring/grafana/dashboards/charts.json` (named `kafka-lab.json` at the
time) was silently ignored
through multiple rebuilds (`--no-cache` included) because
`grafana-data:/var/lib/grafana` shadows the image's baked-in dashboard
file with whatever was already in the volume. Same fix every time:
`docker rm -f grafana && docker volume rm
stream_processing_project_grafana-data && docker compose up -d grafana`.
Worth remembering as a standing rule for *any* future dashboard JSON edit,
not just datasource changes.

### Dashboard panels updated for the all-events job (2026-07-30)

Two panels went stale the moment `windowed_revenue.py` stopped filtering to
purchase-only:
- "Flink Records In/Out per sec" had a "purchase events out/sec
  (post-filter)" line -- removed. With no filter left in the job, that
  task-scoped metric is now nearly identical to the operator-scoped "true
  kafka ingest" line next to it (they used to differ ~15-20x when the
  filter existed), so it added nothing but a confusing label. Kept just
  "kafka ingest/sec" (operator-scoped, comparable to Broker Bytes In/sec)
  vs "window+sink out/sec" -- a growing gap between these two now
  specifically means the window+sink/ClickHouse stage can't keep up with
  full Kafka volume.
- "ClickHouse: Revenue per window" renamed to "ClickHouse: Event count per
  window", now summing `order_count` instead of `revenue` -- revenue
  summed across all-event-types is mostly zeros (only `add_to_cart`/
  `purchase` carry an amount) and isn't a meaningful number for the
  stress-test framing; event count is.
- Confirmed via real run: `sum(order_count)` reached ~73% of total events
  sent within ~1 minute of producer stop, still climbing toward 100% as
  the remaining open windows wait out `idleness_seconds` before firing --
  not a bug, just normal event-time window closing behavior once traffic
  stops.

### `revenue` field removed from windowed_revenue.py entirely (2026-07-30)

Follow-up to processing ALL events: since revenue summed across all-event
types was mostly zeros and no longer meaningful, removed it completely
rather than leaving it as dead weight. `RevenueWindowFunction` renamed to
`EventCountWindowFunction` (just counts elements, no `Decimal` sum),
`to_event_tuple` no longer extracts `amount`, `Row`/ClickHouse schema
dropped to 5 columns (`window_start, window_end, category, country,
order_count`). `revenue` column dropped from the live table via `ALTER
TABLE ... DROP COLUMN` and from `init.sql`. `report_pipeline_metrics()` in
`run_experiment.py` now sums `order_count` instead of `revenue`.
Scenarios 3/4's expected-output text updated similarly -- their
"Validated 2026-07-29: revenue=X" notes are now explicitly marked as
historical (from before this change), with the current assertion being
`order_count` only. The event JSON files
(`experiments/events/*.json`) still specify `amount` per event; it's
simply unused by the job now, harmless to leave.

**Debugging note (worth remembering)**: while validating this, a test run
showed zero rows in ClickHouse for several minutes despite the job
genuinely processing 1.8M+ records (confirmed via Flink's own
read-records/write-records counters). Root cause: the `clickstream`
topic had somehow been auto-recreated with **1 partition** instead of 6
(Kafka's `auto.create.topics.enable=true` will silently recreate a
topic with default settings the moment a client references a
just-deleted topic name -- in this case, the running Flink job's own
active consumer connection triggered the recreation the instant
`kafka-topics --delete` was run, before the following `--create
--partitions 6` could land first). Fix: **cancel the Flink job before
deleting/recreating the topic**, not after -- an active consumer
reference is enough to race the delete/create sequence. Always verify
`kafka-topics --describe --topic clickstream` shows `PartitionCount: 6`
before trusting a "no rows yet" result is just normal window-timing
and not a topic-configuration problem.

### Removed benchmark_generator.py and consumer.py, kept multi_producer.py (2026-07-30)

Both were leftover from the original Kafka-only phase and unused by any
of the current 4 scenarios (none run a consumer; producer-side
benchmarking is no longer a focus). Removed: the files themselves, their
`pyproject.toml` entries, the now-dead consumer-launching code in
`run_experiment.py` (`build_consumer_cmd`, the `consumer_count`/
`consumer_procs` logic, and the `consumer.py` reference in
`kill_leftover_processes`'s pkill pattern), the inert `consumers: {count:
0}` blocks in `scenarios.yml`, and the stale dashboard-table/"Running the
consumer manually" section in `README.md`.

### Grafana: 4 more Flink observability panels added (2026-07-30)

Brainstormed "what Flink metrics are we still missing" and added the
highest-value ones, all confirmed working against real metric names
(checked via Prometheus's `/api/v1/query` first, not guessed):
- **Late Records Dropped/sec** (`numLateRecordsDropped` on the window
  operator) -- validated live: injecting scenario 4's late-event sequence
  produced exactly `1`, matching the single truly-late event. This makes
  the late-event mechanism continuously visible, not just checkable via a
  one-off manual assertion.
- **Network Buffer Pool Usage** (`buffers_inPoolUsage`/`outPoolUsage`) --
  Flink's own docs call this the canonical backpressure signal, reacting
  earlier than busy/backpressured time.
- **Flink TaskManager JVM Heap Used vs Max** and **GC Pause Rate** --
  closes an asymmetry: we've monitored the Kafka broker's JVM via JMX
  since early on, but never Flink's own taskmanager JVM, which is just as
  capable of GC-thrashing under load.

Other metrics surveyed but not yet added: checkpointing (entirely dark
since checkpointing isn't enabled in the job at all -- more of a design
decision than a panel), watermark value as a graphed time series (only
ever checked via the Flink UI's Watermarks tab, not graphed), job
restart/failure count, Kafka-source per-partition offset metrics (Flink's
own view, complementing kafka-exporter's consumer lag). Also discussed
but not built: side-output for late data (route dropped events to their
own ClickHouse table to see *which* events were late, not just the
count), and early/continuous-firing triggers or a custom gauge metric for
true "current in-progress window" visibility (Flink has no built-in way
to inspect an open window's state before it fires).

### Scenario 2 (sink batching) removed; checkpointing enabled by default + 2 new scenarios (2026-07-30)

Scenario list is now 1-5 (removed the sink-batching scenario per request,
reindexed the two correctness scenarios down to 2/3, added two new ones):

- **Checkpointing enabled by default** in `windowed_revenue.py`
  (`env.enable_checkpointing(checkpoint_interval_ms)`, default 10s,
  `FileSystemCheckpointStorage` at `file:///opt/flink/checkpoints`). This
  closes the gap flagged earlier ("entirely dark since checkpointing isn't
  enabled at all"). Checkpoints live on a new `flink-checkpoints` Docker
  volume mounted on **both** flink-jobmanager and flink-taskmanager (both
  need filesystem access -- jobmanager for checkpoint metadata/coordination,
  taskmanager for the actual state data), so they survive a taskmanager
  container restart, not just an in-process retry.
- **Validated for real, not just submitted**: ran a job with
  `--checkpoint-interval-ms 5000`, confirmed 40 completed checkpoints (0
  failed, ~10ms duration each) via the REST API, then `docker restart
  flink-taskmanager` mid-run -- job automatically recovered to `RUNNING`
  within ~15s, `numRestarts` incremented, confirming checkpointing
  actually provides real fault tolerance here, not just configured-but-
  untested.
- **Scenario 4** ("Checkpointing: interval tradeoff + kill-and-recover"):
  sweeps `checkpoint_interval_ms` (1000ms vs 30000ms) via steps, with a
  manual `docker restart flink-taskmanager` mid-run to validate recovery
  -- combines the tradeoff and the recovery test into one scenario per
  request, not two.
- **Scenario 5** ("Backpressure: reduced Flink resources + artificial sink
  delay"): combines both requested mechanisms rather than picking one --
  constrains `flink-taskmanager` to 0.5 CPUs for the whole scenario (new
  `apply_flink_taskmanager_resources()` in `run_experiment.py`, mirroring
  the existing Kafka-broker-CPU-limit pattern), then two steps at that
  same constrained level: `sink_delay_ms=0` (resource constraint alone)
  vs `sink_delay_ms=50` (constraint + slow sink compounding). New
  `--sink-delay-ms` flag on `ClickHouseSink` (artificial `time.sleep()`
  per insert) makes the slow-sink half reproducible on demand, rather
  than hoping real load happens to trigger it -- deliberately avoids
  needing to fire more Kafka producers to induce backpressure, per
  request.
- Two new Grafana panels: **Checkpoint Duration/Size** and **Job
  Restarts** (`numRestarts`) -- both were completely dark before since
  checkpointing was disabled.

### Grafana dashboard organized into rows: Business vs Operational (2026-07-31)

All 11 panels were a flat list mixing business-facing metrics with pure
infra/runtime signals. Added 4 Grafana row panels (collapsible section
headers, `"type": "row"`) to group them:
- **Business Metrics** -- ClickHouse event count per window, insert rate.
  The only two panels answering "what is the pipeline producing," not
  "is the infra healthy."
- **Kafka — Operational** -- Broker Bytes In/sec.
- **Flink — Throughput & Backpressure (Operational)** -- Records In/Out,
  Busy vs Backpressured Time, Buffer Pool Usage, Late Records Dropped.
- **Flink — Runtime Health (Operational)** -- JVM Heap, GC Pause Rate,
  Checkpoint Duration/Size, Job Restarts.

Row panels need sequential `gridPos.y` positioning to visually group the
panels beneath them (each row's child panels must have `y` greater than
the row's `y` and less than the next row's `y`) -- Grafana doesn't nest
them via a parent/child reference for `collapsed: false` rows, it's
purely position-based.

### Two more panels: live end-to-end throughput + latency (2026-07-31)

Closed two real gaps identified while reasoning through "how do I measure
scenario 2's end-to-end throughput/latency from Grafana alone":
- **"ClickHouse: Events/sec (derived end-to-end throughput)"** --
  `sum(order_count)/window_seconds` per window. Distinct from the
  existing "insert rate" panel (counts ROWS -- category/country groups
  -- not events) and "Event count per window" (raw per-window totals,
  not a rate). This is the first panel directly comparable, apples-to-
  apples, with Broker Bytes In/sec and the Flink ingest rate panel.
  Hardcodes `/30.0` assuming scenario 2's default `window_seconds=30` --
  needs recomputing if used against scenario 5/6's different window
  sizes.
- **"ClickHouse: End-to-end Latency over time"** -- a live time-series
  version of `report_pipeline_metrics()`'s avg/max latency, which
  previously only ever existed as a one-shot number printed after a
  scenario finished. Now you can watch latency trend *during* a run.
  Validated against real leftover data: showed latency climbing 10s ->
  30s across consecutive windows, consistent with the backlog-catch-up
  behavior noted earlier in this doc -- confirms the panel actually
  catches real degradation live, not just cosmetic.

### New scenario 1: raw event passthrough for TRUE single-event latency (2026-07-31)

Prompted by the observation that windowed_revenue's latency
(`window_end -> inserted_at`) conflates real pipeline overhead with the
window's *deliberate* wait time (`window_seconds + watermark_seconds`) --
built a way to isolate the two. Scenario list reindexed: this is now
**scenario 1** (the more fundamental measurement, deliberately placed
before the windowed scenario it's meant to be compared against), pushing
the old 1-5 to 2-6.

- **`flink_jobs/raw_event_sink.py`** (new) -- no `key_by`/`window`/
  watermark logic at all, just Kafka -> parse -> ClickHouse, one row per
  event, preserving `event_time`. Accepts (but ignores)
  `--window-seconds`/`--watermark-seconds`/`--idleness-seconds` purely so
  `run_experiment.py`'s existing `submit_flink_job()` can build one
  identical command-line-construction path for both this job and
  `windowed_revenue.py`, without needing job-type-specific branches.
- **New `raw_events` ClickHouse table** (`event_id, event_time,
  event_type, category, country, inserted_at`) -- added to `init.sql` and
  created live via `ALTER`/`CREATE` on the running container (init.sql
  only runs on a volume's first creation).
- **`latency_breakdown.py`** (new, repo root) -- queries both tables over
  a lookback window and prints the decomposition: true pipeline latency
  (raw_events) vs windowed latency (windowed_revenue) vs the gap between
  them (windowing wait).
- **`report_pipeline_metrics()` in `run_experiment.py` made schema-aware**
  -- branches on `table == "raw_events"` vs the windowed schema, since the
  two tables have genuinely different columns/latency definitions
  (`event_time` vs `window_end` as the latency start point).
- **Validated for real, end-to-end**: raw_events showed **avg latency
  0.916s** (max 2.305s) on a real run -- dramatically lower than
  windowed_revenue's ~10-30s+ measured earlier. Full decomposition via
  `latency_breakdown.py` on a paired run: true pipeline overhead ≈2.28s,
  windowing wait ≈4.9s -- confirms most of what looked like "pipeline
  latency" before was actually just the deliberate window wait, not the
  system being slow.

### Not yet done

- Automating scenarios 3/4 fully (running `event_injector.py` from
  `run_experiment.py` and asserting the ClickHouse result programmatically
  instead of a human eyeballing it).
- A time-based (not just row-count-based) flush for `ClickHouseSink`,
  given the "rows lost on cancel" caveat above.
- Consumer-lag panel for Flink's own consumer group (`flink-windowed-revenue`)
  and a restored "total Messages In/sec" panel -- discussed as verification
  signals for "can Flink absorb the full 40MB/s" but not yet added.
- Sessionization, funnel/conversion, cart-abandonment jobs -- only
  windowed-revenue exists so far.
- Whether Redis gets added later for cart-abandonment is still open.