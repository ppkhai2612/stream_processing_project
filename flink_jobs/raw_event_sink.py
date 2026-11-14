"""
Raw event passthrough job — no windowing, no aggregation. Written against
the DataStream API (imperative Python), matching windowed_revenue.py's style.

Reads `clickstream` from Kafka, parses each event, and writes it straight to
ClickHouse as its own row (one row per event), preserving the original
`event_time`. No key_by, no window, no watermark strategy needed at all --
there's no window to close, so there's nothing to wait for.

Why this job exists: windowed_revenue.py's latency measurement
(window_end -> inserted_at) bakes in the window's *deliberate* wait time
(window_seconds + watermark_seconds) on top of real pipeline overhead --
it answers "how long until the aggregated result is visible," not "how
long does a single event take to travel through Kafka -> Flink ->
ClickHouse." This job isolates the second number. Run both jobs against
the same producer load and compare with latency_breakdown.py to decompose
observed latency into real pipeline overhead vs windowing wait.

Sink note: same as windowed_revenue.py -- plain Python side-effect
(clickhouse-connect) inside a MapFunction, not Flink's JDBC connector (see
that file's docstring for why).

Volume note: one row per event is a much higher insert rate than
windowed_revenue.py's aggregated output (which only writes once per window
fire). Run this against a throttled producer rate, not uncapped -- see
scenario 1 in scenarios.yml, which deliberately caps at --rate 5000 for
this reason. sink_batch_rows defaults higher here than in
windowed_revenue.py for the same reason (ClickHouse's own guidance:
insert-per-row at high volume exhausts the merge queue).

Submitted with:
    flink run -d -py raw_event_sink.py --bootstrap kafka:29092 ...
(see experiments/scenarios.yml scenario 1 / run_experiment.py for the full
command the runner builds -- reuses the same generic submit_flink_job()
as windowed_revenue.py, which is why this file accepts --window-seconds/
--watermark-seconds/--idleness-seconds even though it ignores them: keeps
the submission command-building code in run_experiment.py identical for
both jobs).
"""
import argparse
import json
import time
from datetime import datetime, timezone

from pyflink.common import Row, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import MapFunction, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="kafka:29092",
                    help="Kafka bootstrap address reachable from inside the Flink cluster")
    p.add_argument("--topic", default="clickstream")
    p.add_argument("--group-id", default="flink-raw-events")
    p.add_argument("--clickhouse-host", default="clickhouse")
    p.add_argument("--clickhouse-port", type=int, default=8123)
    p.add_argument("--clickhouse-database", default="stream_processing_project")
    p.add_argument("--clickhouse-user", default="default")
    p.add_argument("--clickhouse-password", default="clickhouse")
    # Unused by this job (no windowing at all) but accepted so
    # run_experiment.py's submit_flink_job() can build one identical
    # command line for both this job and windowed_revenue.py.
    p.add_argument("--window-seconds", type=int, default=30)
    p.add_argument("--watermark-seconds", type=int, default=5)
    p.add_argument("--idleness-seconds", type=int, default=60)
    p.add_argument("--parallelism", type=int, default=6)
    p.add_argument("--sink-batch-rows", type=int, default=200,
                    help="Rows to buffer per sink instance before inserting into ClickHouse "
                         "as one batch. Higher default than windowed_revenue.py's since raw "
                         "per-event volume is much higher than windowed output volume.")
    p.add_argument("--sink-delay-ms", type=int, default=0,
                    help="Artificial delay per sink insert, simulating a slow downstream.")
    p.add_argument("--checkpoint-interval-ms", type=int, default=10000)
    return p.parse_args()


def parse_event(raw: str) -> dict | None:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def to_row(event: dict) -> Row:
    event_time = datetime.fromtimestamp(
        int(event["event_time"]) / 1000.0, tz=timezone.utc
    ).replace(tzinfo=None)
    category = event.get("category") or "unknown"
    country = (event.get("geo") or {}).get("country") or "unknown"
    return Row(
        event.get("event_id", ""),
        event_time,
        event.get("event_type", "unknown"),
        category,
        country,
    )


class RawEventClickHouseSink(MapFunction):
    """Same batching/delay pattern as windowed_revenue.py's ClickHouseSink,
    against the raw_events table instead."""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str,
                 batch_rows: int = 200, delay_ms: int = 0):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.batch_rows = max(1, batch_rows)
        self.delay_ms = delay_ms
        self.client = None
        self.buffer = []

    def open(self, runtime_context):
        import clickhouse_connect
        self.client = clickhouse_connect.get_client(
            host=self.host, port=self.port, database=self.database,
            username=self.user, password=self.password,
        )
        self.buffer = []

    def _flush(self):
        if not self.buffer:
            return
        self.client.insert(
            "raw_events",
            self.buffer,
            column_names=["event_id", "event_time", "event_type", "category", "country"],
        )
        flushed = len(self.buffer)
        self.buffer = []
        return flushed

    def map(self, row: Row):
        if self.delay_ms:
            time.sleep(self.delay_ms / 1000.0)
        self.buffer.append([row[0], row[1], row[2], row[3], row[4]])
        flushed = None
        if len(self.buffer) >= self.batch_rows:
            flushed = self._flush()
        return f"flushed {flushed} rows" if flushed else f"buffered {len(self.buffer)}/{self.batch_rows}"

    def close(self):
        self._flush()
        if self.client is not None:
            self.client.close()


def main():
    args = parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(args.bootstrap)
        .set_topics(args.topic)
        .set_group_id(args.group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-clickstream")

    rows = (
        raw.map(parse_event, output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda e: e is not None)
        .map(to_row,
             output_type=Types.ROW([Types.STRING(), Types.SQL_TIMESTAMP(), Types.STRING(),
                                    Types.STRING(), Types.STRING()])
        )
    )

    (
        rows
        .map(RawEventClickHouseSink(args.clickhouse_host, args.clickhouse_port,
                                   args.clickhouse_database, args.clickhouse_user,
                                   args.clickhouse_password, args.sink_batch_rows,
                                   args.sink_delay_ms),
            output_type=Types.STRING())
        .print()
    )

    env.execute("raw-event-sink")


if __name__ == "__main__":
    main()