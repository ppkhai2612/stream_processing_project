"""
Kafka producer for a synthetic e-commerce clickstream/order stream.

Emits realistic events (page_view, product_view, search, add_to_cart,
remove_from_cart, purchase) from a pool of concurrent sessions, suitable
for later Flink processing (sessionization, funnel analysis, windowed
revenue aggregation, etc). See event_gen.py for the generation model.

Examples
--------
# Realistic steady-state track (sleep 0.2ms between sends)
python producer.py --topic clickstream --delay-ms 0.2 --duration 60

# Uncapped stress track (no delay)
python producer.py --topic clickstream --delay-ms 0 --duration 30

# Key by user_id instead of session_id (changes downstream partitioning /
# what a Flink keyBy would naturally group on)
python producer.py --topic clickstream --key-field user_id
"""
import argparse
import json
import os
import time

from confluent_kafka import Producer
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from event_gen import ClickstreamGenerator

# Shared dict updated by librdkafka stats callback, read by main loop for push
_librdkafka_stats: dict = {}


def _make_stats_cb(topic: str):
    """Returns a librdkafka stats callback that parses the JSON and updates _librdkafka_stats."""
    prev = {"txmsgs": 0, "txmsg_bytes": 0, "ts": 0}

    def stats_cb(stats_json: str):
        try:
            s = json.loads(stats_json)
            ts = s.get("ts", 0)  # microseconds since epoch

            if prev["ts"] > 0:
                dt = (ts - prev["ts"]) / 1e6  # seconds
                if dt > 0:
                    _librdkafka_stats["msgs_per_sec"] = (s.get("txmsgs", 0) - prev["txmsgs"]) / dt
                    _librdkafka_stats["throughput_mbps"] = (s.get("txmsg_bytes", 0) - prev["txmsg_bytes"]) / dt / 1e6

            prev["txmsgs"] = s.get("txmsgs", 0)
            prev["txmsg_bytes"] = s.get("txmsg_bytes", 0)
            prev["ts"] = ts

            # Batch size from topic stats
            topic_stats = s.get("topics", {}).get(topic, {})
            _librdkafka_stats["batch_size_avg"] = topic_stats.get("batchsize", {}).get("avg", 0)

            # RTT and internal queue latency (linger.ms effect) from first UP broker
            for broker in s.get("brokers", {}).values():
                if broker.get("state") == "UP":
                    _librdkafka_stats["rtt_avg_us"] = broker.get("rtt", {}).get("avg", 0)
                    _librdkafka_stats["queue_latency_avg_us"] = broker.get("int_latency", {}).get("avg", 0)
                    break
        except Exception:
            pass


    return stats_cb


def main():
    ap = argparse.ArgumentParser(description="Clickstream producer load generator")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="clickstream")
    ap.add_argument("--delay-ms", type=float, default=0,
                     help="fixed delay (in ms) slept after each send; 0 means uncapped "
                          "(no artificial delay, real speed governed by event "
                          "generation + Kafka send overhead, not this flag)")
    ap.add_argument("--duration", type=float, default=30, help="seconds to run")
    ap.add_argument("--key-field", choices=["session_id", "user_id", "product_id", "none"],
                     default="session_id",
                     help="which event field to use as the Kafka message key "
                          "(determines partition + downstream keyBy affinity)")
    ap.add_argument("--num-users", type=int, default=5000)
    ap.add_argument("--concurrent-sessions", type=int, default=300)
    ap.add_argument("--num-products", type=int, default=200)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--acks", default="1", choices=["0", "1", "all"])
    ap.add_argument("--linger-ms", type=int, default=5)
    ap.add_argument("--batch-size", type=int, default=16384, help="batch.size in bytes")
    ap.add_argument("--compression", default="none",
                    choices=["none", "gzip", "snappy", "lz4", "zstd"])
    ap.add_argument("--report-interval", type=float, default=1.0)
    ap.add_argument("--pushgateway", default="",
                     help="Prometheus pushgateway address e.g. localhost:9091; empty = disabled")

    args = ap.parse_args()

    conf = {
        "bootstrap.servers": args.bootstrap,
        "acks": args.acks,
        "linger.ms": args.linger_ms,
        "batch.size": args.batch_size,
        "compression.type": args.compression,
        "queue.buffering.max.messages": 500000,
        "queue.buffering.max.kbytes": 1048576,
        "statistics.interval.ms": 1000,
        "stats_cb": _make_stats_cb(args.topic),
    }
    producer = Producer(conf)

    # Prometheus gauges — created once, reused on every push
    _pg_registry   = CollectorRegistry()
    _pg_throughput = Gauge("kafka_producer_throughput_mbps",      "Producer throughput MB/s",                registry=_pg_registry)
    _pg_msgs       = Gauge("kafka_producer_msgs_per_sec",         "Producer messages/sec",                   registry=_pg_registry)
    _pg_batch_size = Gauge("kafka_producer_batch_size_avg_bytes", "Avg batch size bytes",                    registry=_pg_registry)
    _pg_rtt        = Gauge("kafka_producer_rtt_avg_us",           "Broker RTT avg microseconds",             registry=_pg_registry )
    _pg_queue_lat  = Gauge("kafka_producer_queue_latency_avg_us", "Internal queue latency avg microseconds", registry=_pg_registry)
    _pg_errors     = Gauge("kafka_producer_errors_total",         "Cumulative delivery errors",              registry=_pg_registry)

    generator = ClickstreamGenerator(
        num_users=args.num_users,
        concurrent_sessions=args.concurrent_sessions,
        num_products=args.num_products,
        seed=args.seed,
    )

    sent = 0
    errors = 0
    interval_sent = 0
    interval_bytes = 0

    def delivery_cb(err, msg):
        nonlocal errors
        if err is not None:
            errors += 1

    start = time.time()
    next_report = start + args.report_interval
    sleep_per_msg = args.delay_ms / 1000.0

    print(f"Producing clickstream events to '{args.topic}' @ {args.bootstrap} | "
          f"delay_ms={'uncapped' if args.delay_ms <= 0 else args.delay_ms} "
          f"key_field={args.key_field} acks={args.acks} "
          f"linger.ms={args.linger_ms} compression={args.compression}")

    try:
        while True:
            now = time.time()
            if now - start >= args.duration:
                break

            event = generator.next_event()
            payload = json.dumps(event).encode()

            key = None
            if args.key_field != "none":
                key_val = event.get(args.key)
                key = key_val.encode() if key_val is not None else None
            
            try:
                producer.produce(args.topic, key=key, value=payload, callback=delivery_cb)
            except BufferError:
                producer.poll(0.1)
                continue

            producer.poll(0)
            sent += 1
            interval_sent += 1
            interval_bytes += len(payload)

            if sleep_per_msg:
                time.sleep(sleep_per_msg)

            if now >= next_report:
                elapsed = now - next_report + args.report_interval
                rate_msgs = interval_sent / elapsed
                rate_mb = interval_bytes / elapsed / 1_000_000
                print(f"[{now - start:6.1f}s] {rate_msgs:>10.0f} events/s  "
                      f"{rate_mb:>8.2f} MB/s  total_sent={sent}  errors={errors}")
                interval_sent = 0
                interval_bytes = 0
                next_report = now + args.report_interval

                if args.pushgateway and _librdkafka_stats:
                    _pg_throughput.set(_librdkafka_stats.get("throughput_mbps", 0))
                    _pg_msgs.set(_librdkafka_stats.get("msgs_per_sec", 0))
                    _pg_batch_size.set(_librdkafka_stats.get("batch_size_avg", 0))
                    _pg_rtt.set(_librdkafka_stats.get("rtt_avg_us", 0))
                    _pg_queue_lat.set(_librdkafka_stats.get("queue_latency_avg_us", 0))
                    _pg_errors.set(errors)
                    try:
                        push_to_gateway(
                            args.pushgateway,
                            job="kafka_producer",
                            grouping_key={"pid": str(os.getpid())},
                            registry=_pg_registry,
                        )
                    except Exception:
                        pass

    except KeyboardInterrupt:
        print("Interrupted, flushing...")

    remaining = producer.flush(30)
    total_elapsed = time.time() - start
    print("\n--- Summary ---")
    print(f"Sent:           {sent}")
    print(f"Errors:         {errors}")
    print(f"Unflushed:      {remaining}")
    print(f"Elapsed:        {total_elapsed:.2f}s")
    print(f"Avg throughput: {sent / total_elapsed:,.0f} events/s")
    

if __name__ == "__main__":
    main()