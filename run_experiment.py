"""
Experiment runner — reads experiments/scenarios.yml and executes a scenario.

Usage
-----
# List all scenarios
python run_experiment.py --list

# Run scenario 1
python run_experiment.py --id 1

# Run scenario 2, step 2
python run_experiment.py --id 2 --step 2

# Dry run — print commands without executing
python run_experiment.py --id 1 --dry-run
"""
import argparse
import base64
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import yaml


SCENARIOS_FILE = Path(__file__).parent / "experiments" / "scenarios.yml"
GRAFANA_URL    = "http://localhost:3000"
GRAFANA_AUTH   = base64.b64encode(b"admin:admin").decode()
PUSHGATEWAY    = "http://localhost:9091"
FLINK_URL      = "http://localhost:8081"


def _http(method: str, url: str, body: dict | None = None, auth: str | None = None) -> bool:
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json"}
    if auth:
        headers["Authorization"] = f"Basic {auth}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        urllib.request.urlopen(req, timeout=5)
        return True
    except urllib.error.URLError:
        return False


def _http_get_json(url: str) -> dict | None:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return json.loads(resp.read())
    except (urllib.error.URLError, json.JSONDecodeError):
        return None


def kill_leftover_processes(dry_run: bool):
    """
    Kill any producer processes left over from a previous run
    (e.g. a Ctrl+C'd scenario, or a background run that got killed without
    letting subprocess.Popen children get cleaned up).
    """
    cmd = "pkill -f 'python (producer|multi_producer).py' || true"
    print(f"[cleanup]  {cmd}")
    if not dry_run:
        subprocess.run(cmd, shell=True)
        time.sleep(1)


def clear_pushgateway(dry_run: bool):
    """Delete all producer and consumer metrics from pushgateway."""
    for job in ("kafka_producer", "kafka_consumer"):
        url = f"{PUSHGATEWAY}/metrics/job/{job}"
        print(f"[cleanup]  DELETE {url}")
        if not dry_run:
            _http("DELETE", url)


def post_grafana_annotation(text: str, tags: list[str], dry_run: bool):
    """Drop a labelled vertical line on the Grafana dashboard."""
    url = f"{GRAFANA_URL}/api/annotations"
    body = {
        "dashboardUID": "kafka-lab",
        "time": int(time.time() * 1000),
        "tags": tags,
        "text": text,
    }
    print(f"[grafana]  annotation: {text}")
    if not dry_run:
        ok = _http("POST", url, body=body, auth=GRAFANA_AUTH)
        if not ok:
            print("           (warning: could not post annotation — Grafana unreachable?)")


def load_scenarios():
    with open(SCENARIOS_FILE) as f:
        data = yaml.safe_load(f)
        return data["global"], {s["id"]: s for s in data["scenarios"]}


def print_banner(text: str):
    print("\n" + "─" * 60)
    print(f"  {text}")
    print("─" * 60)


def apply_broker_config(broker: dict, dry_run: bool):
    cpus = broker.get("cpus", "4")
    memory = broker.get("memory", "2g")
    cmd = f"docker update --cpus={cpus} --memory={memory} kafka"
    print(f"\n[broker]  {cmd}")
    if not dry_run:
        subprocess.run(cmd, shell=True, check=True)


def apply_flink_taskmanager_resources(resources: dict, dry_run: bool):
    cpus = resources.get("cpus", "4")
    memory = resources.get("memory", "2g")
    cmd = f"docker update --cpus={cpus} --memory={memory} flink-taskmanager"
    print(f"\n[flink-taskmanager]  {cmd}")
    if not dry_run:
        subprocess.run(cmd, shell=True, check=True)


def setup_topic(g: dict, topic_cfg: dict, dry_run: bool):
    partitions = topic_cfg.get("partitions", 6)
    retention_ms = topic_cfg.get("retention_minutes", topic_cfg.get("retention_hours", 1) * 60) * 60 * 1000
    cmd = (
        f"docker exec kafka kafka-topics --bootstrap-server localhost:9092 "
        f"--delete --topic {g['topic']} --if-exists ; "
        f"docker exec kafka kafka-topics --bootstrap-server localhost:9092 "
        f"--create --topic {g['topic']} "
        f"--partitions {partitions} --replication-factor 1 "
        f"--config retention.ms={retention_ms}"
    )
    print(f"\n[topic]   partitions={partitions}  retention={retention_ms}ms")
    if not dry_run:
        subprocess.run(cmd, shell=True)
        # Poll until the topic actually has the right partition count instead
        # of a fixed sleep — avoids a race against auto.create.topics (or a
        # slow delete/recreate) where a producer starts before the topic is
        # really ready and gets the wrong partition count or a stale topic.
        describe_cmd = (
            f"docker exec kafka kafka-topics --bootstrap-server localhost:9092 "
            f"--describe --topic {g['topic']}"
        )
        for i in range(30):
            result = subprocess.run(describe_cmd, shell=True, capture_output=True, text=True)
            if f"PartitionCount: {partitions}" in result.stdout:
                print("f[topic]   ready (confirmed {partitions} partitions)")
                break
            time.sleep(1)
        else:
            print(f"[topic]   WARNING: could not confirm {partitions} partitions after 30s")


def cancel_running_flink_jobs(dry_run: bool):
    """Cancel any Flink jobs left running from a previous scenario run."""
    print(f"\n[flink]   checking {FLINK_URL}/jobs for running jobs to cancel")
    if dry_run:
        print("          (dry-run: would GET /jobs, PATCH /jobs/<id>?mode=cancel for each RUNNING job)")
        return
    data = _http_get_json(f"{FLINK_URL}/jobs")
    if data is None:
        print("          (warning: could not reach Flink REST API — is the cluster up?)")
        return
    for job in data.get("jobs", []):
        if job.get("status") == "RUNNING":
            job_id = job["id"]
            print(f"          cancelling job {job_id}")
            _http("PATCH", f"{FLINK_URL}/jobs/{job_id}?mode=cancel")
            time.sleep(2)


def submit_flink_job(flink_cfg: dict, g: dict, dry_run: bool):
    job = flink_cfg.get("job", "windowed_revenue.py")
    bootstrap = g.get("internal_bootstrap", "kafka:29092")
    topic = g["topic"]
    window_seconds = flink_cfg.get("window_seconds", 30)
    watermark_seconds = flink_cfg.get("watermark_seconds", 5)
    idleness_seconds = flink_cfg.get("idleness_seconds", 60)
    parallelism = flink_cfg.get("parallelism", 6)
    group_id = flink_cfg.get("group_id", "flink-windowed-revenue")
    ch_host = flink_cfg.get("clickhouse_host", "clickhouse")
    ch_port = flink_cfg.get("clickhouse_port", 8123)
    ch_database = flink_cfg.get("clickhouse_database", "stream_processing_project")
    ch_user = flink_cfg.get("clickhouse_user", "default")
    ch_password = flink_cfg.get("clickhouse_password", "clickhouse")
    sink_batch_rows = flink_cfg.get("sink_batch_rows", 1)
    sink_delay_ms = flink_cfg.get("sink_delay_ms", 0)
    checkpoint_interval_ms = flink_cfg.get("checkpoint_interval_ms", 10000)

    cmd = (
        f"docker exec flink-jobmanager /opt/flink/bin/flink run -d "
        f"-j /opt/flink/connector-jars/flink-kafka-connector-bundle.jar "
        f"-py /opt/flink/usrlib/{job} "
        f"--bootstrap {bootstrap} "
        f"--topic {topic} "
        f"--group-id {group_id} "
        f"--clickhouse-host {ch_host} "
        f"--clickhouse-port {ch_port} "
        f"--clickhouse-database {ch_database} "
        f"--clickhouse-user {ch_user} "
        f"--clickhouse-password {ch_password} "
        f"--window-seconds {window_seconds} "
        f"--watermark-seconds {watermark_seconds} "
        f"--idleness-seconds {idleness_seconds} "
        f"--parallelism {parallelism} "
        f"--sink-batch-rows {sink_batch_rows} "
        f"--sink-delay-ms {sink_delay_ms} "
        f"--checkpoint-interval-ms {checkpoint_interval_ms}"
    )
    print(f"\n[flink]   {cmd}")
    if not dry_run:
        subprocess.run(cmd, shell=True, check=True)


def clear_clickhouse_table(clickhouse_cfg: dict, dry_run: bool):
    database = clickhouse_cfg.get("database", "stream_processing_project")
    table = clickhouse_cfg.get("table", "windowed_revenue")
    cmd = (
        f'docker exec clickhouse clickhouse-client '
        f'--query "TRUNCATE TABLE {database}.{table}"'
    )
    print(f"\n[clickhouse]  {cmd}")
    if not dry_run:
        subprocess.run(cmd, shell=True)


def report_pipeline_metrics(clickhouse_cfg: dict, dry_run: bool):
    """Formalized end-to-end throughput+latency measurement straight from
    ClickHouse. Schema-aware: windowed_revenue's latency is window_end ->
    inserted_at (window fire + watermark wait + sink flush, all folded
    together); raw_events' latency is event_time -> inserted_at (TRUE
    single-event pipeline latency, no windowing wait baked in — see
    latency_breakdown.py to compare the two directly)."""
    database = clickhouse_cfg.get("database", "stream_processing_project")
    table = clickhouse_cfg.get("table", "windowed_revenue")

    if table == "raw_events":
        query = (
            f"SELECT count(*), "
            f"round(avg(dateDiff('millisecond', event_time, inserted_at)) / 1000.0, 3), "
            f"round(max(dateDiff('millisecond', event_time, inserted_at)) / 1000.0, 3) "
            f"FROM {database}.{table}"
        )
        expected_fields = 3
    else:
        query = (
            f"SELECT count(*), sum(order_count), "
            f"round(avg(dateDiff('second', window_end, inserted_at)), 2), "
            f"max(dateDiff('second', window_end, inserted_at)) "
            f"FROM {database}.{table}"
        )
        expected_fields = 4

    cmd = f'docker exec clickhouse clickhouse-client --query "{query}"'
    print_banner("Pipeline measurement (ClickHouse)")
    print(f"  {cmd}")
    if dry_run:
        return
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    parts = result.stdout.strip().split("\t")
    if len(parts) != expected_fields:
        print(f"  (unexpected output: {result.stdout!r} {result.stderr!r})")
        return
    if table == "raw_events":
        rows, avg_latency_s, max_latency_s = parts
        print(f"  rows={rows}  avg_latency={avg_latency_s}s  max_latency={max_latency_s}s "
              f"(TRUE per-event latency, no windowing wait)")
    else:
        rows, total_events, avg_latency_s, max_latency_s = parts
        print(f"  rows={rows}  total_events={total_events}  "
              f"avg_latency={avg_latency_s}s  max_latency={max_latency_s}s")


def build_producer_cmd(g: dict, p: dict, step: dict | None = None) -> str:
    cfg = {**p, **(step or {})}
    # Remove non-producer keys
    for k in ["processes", "count", "label"]:
        cfg.pop(k, None)

    delay_ms    = cfg.get("delay_ms", 0)
    duration    = cfg.get("duration", 60)
    acks        = cfg.get("acks", "1")
    linger_ms   = cfg.get("linger_ms", 5)
    batch_size  = cfg.get("batch_size", 524288)
    compression = cfg.get("compression", "none")
    key_field   = cfg.get("key_field", "session_id")

    return (
        f"python producer.py "
        f"--topic {g['topic']} "
        f"--bootstrap {g['bootstrap']} "
        f"--delay-ms {delay_ms} "
        f"--duration {duration} "
        f"--acks {acks} "
        f"--linger-ms {linger_ms} "
        f"--batch-size {batch_size} "
        f"--compression {compression} "
        f"--key-field {key_field} "
        f"--pushgateway {g['pushgateway']}"
    )


def build_multi_producer_cmd(g: dict, p: dict, step: dict | None = None) -> str:
    cfg = {**p, **(step or {})}
    processes  = cfg.get("processes", 1)
    delay_ms   = cfg.get("delay_ms", 0)
    duration   = cfg.get("duration", 60)
    acks       = cfg.get("acks", "1")
    linger_ms  = cfg.get("linger_ms", 5)
    batch_size = cfg.get("batch_size", 524288)
    compression= cfg.get("compression", "none")
    key_field  = cfg.get("key_field", "session_id")

    return (
        f"python multi_producer.py "
        f"--topic {g['topic']} "
        f"--bootstrap {g['bootstrap']} "
        f"--processes {processes} "
        f"--delay-ms {delay_ms} "
        f"--duration {duration} "
        f"--acks {acks} "
        f"--linger-ms {linger_ms} "
        f"--batch-size {batch_size} "
        f"--compression {compression} "
        f"--key-field {key_field} "
        f"--pushgateway {g['pushgateway']}"
    )


def run_scenario(scenario: dict, g: dict, step_num: int | None, dry_run: bool):
    sid   = scenario["id"]
    name  = scenario["name"]
    tier  = scenario["tier"]
    steps = scenario.get("steps", [])

    print_banner(f"Scenario {sid} (Tier {tier}): {name}")
    print(f"\n{scenario.get('description', '').strip()}")

    # ── Pre-run cleanup ───────────────────────────────────
    kill_leftover_processes(dry_run)
    clear_pushgateway(dry_run)
    post_grafana_annotation(
        text=f"Scenario {sid} start: {name}",
        tags=["experiment", f"scenario-{sid}"],
        dry_run=dry_run,
    )

    # ── Broker config ─────────────────────────────────────
    apply_broker_config(scenario.get("broker", g["default_broker"]), dry_run)

    # ── Flink taskmanager resources (optional, for backpressure-via-undersized-cluster scenarios) ──
    if "flink_taskmanager_resources" in scenario:
        apply_flink_taskmanager_resources(scenario["flink_taskmanager_resources"], dry_run)

    # ── Cancel any leftover Flink job BEFORE touching the topic ──
    # Must happen first: deleting/recreating a topic while a running job's
    # Kafka consumer still references it can trigger auto.create.topics.enable
    # to instantly recreate the topic with the Kafka default of 1 partition,
    # racing (and winning) against the --create --partitions N below.
    flink_cfg = scenario.get("flink")
    clickhouse_cfg = scenario.get("clickhouse")
    if flink_cfg or clickhouse_cfg:
        cancel_running_flink_jobs(dry_run)

    # ── Topic setup ───────────────────────────────────────
    setup_topic(g, scenario.get("topic", g["default_topic"]), dry_run)

    # ── Steps or single run ───────────────────────────────
    runs = []
    if steps:
        if step_num is not None:
            if step_num < 1 or step_num > len(steps):
                print(f"Error: --step must be 1-{len(steps)} for this scenario")
                sys.exit(1)
            runs = [(step_num, steps[step_num - 1])]
        else:
            runs = list(enumerate(steps, start=1))
    else:
        runs = [(None, None)]

    producers_cfg = scenario.get("producers", {})

    for step_idx, step in runs:
        if step:
            label = step.get("label", f"step {step_idx}")
            print_banner(f"  Step {step_idx}/{len(steps)}: {label}")
            post_grafana_annotation(
                text=f"S{sid} step {step_idx}: {label}",
                tags=["experiment", f"scenario-{sid}", f"step-{step_idx}"],
                dry_run=dry_run,
            )

            if flink_cfg or clickhouse_cfg:
                cancel_running_flink_jobs(dry_run)
            if clickhouse_cfg:
                clear_clickhouse_table(clickhouse_cfg, dry_run)
            if flink_cfg:
                submit_flink_job({**flink_cfg, **(step or {})}, g, dry_run)

            proc = None
            if producers_cfg:
                processes = producers_cfg.get("processes", 1)
                if processes > 1:
                    cmd = build_multi_producer_cmd(g, producers_cfg, step)
                else:
                    cmd = build_producer_cmd(g, producers_cfg, step)

                print(f"\n[producer] {cmd}")
                if not dry_run:
                    proc = subprocess.Popen(cmd, shell=True)
            else:
                print("\n[producer] none configured for this scenario "
                    "(correctness scenario — see manual_steps for the event_injector.py command)")

            if not dry_run and step:
                if proc:
                    proc.wait()
                if clickhouse_cfg:
                    report_pipeline_metrics(clickhouse_cfg, dry_run)
                print(f"\n  ✓ Step {step_idx} complete. Waiting 5s before next step...")
                time.sleep(5)

    if not dry_run and not steps:
        if proc:
            proc.wait()
            
    # ── Manual steps ──────────────────────────────────────
    manual = scenario.get("manual_steps", [])
    if manual:
        print_banner("Manual steps (do these yourself)")
        for i, s in enumerate(manual, 1):
            print(f"  {i}. {s}")

    # ── What to watch ─────────────────────────────────────
    print_banner("Watch in Grafana → http://localhost:3000")
    for w in scenario.get("watch", []):
        print(f"  • {w}")

    # ── Expected outcome ──────────────────────────────────
    expected = scenario.get("expected", "")
    if expected:
        print_banner("Expected outcome")
        print(f"  {expected.strip()}")

    # ── Reset instructions ─────────────────────────────────
    reset = scenario.get("reset", {})
    if reset:
        print_banner("Reset after experiment")
        if "broker_cpus" in reset:
            print(f"  docker update --cpus={reset['broker_cpus']} kafka")
        if "broker_memory" in reset:
            print(f"  docker update --memory={reset['broker_memory']} kafka")
        if "flink_taskmanager_cpus" in reset:
            print(f"  docker update --cpus={reset['flink_taskmanager_cpus']} flink-taskmanager")
        if "flink_taskmanager_memory" in reset:
            print(f"  docker update --memory={reset['flink_taskmanager_memory']} flink-taskmanager")

    notes = scenario.get("notes", "")
    if notes:
        print(f"\n  NOTE: {notes.strip()}")

    # ── Pipeline measurement (Flink/ClickHouse scenarios only) ─────
    if clickhouse_cfg:
        report_pipeline_metrics(clickhouse_cfg, dry_run)


def main():
    ap = argparse.ArgumentParser(description="Kafka experiment runner")
    ap.add_argument("--list", action="store_true", help="List all scenarios")
    ap.add_argument("--id", type=int, help="Scenario ID to run")
    ap.add_argument("--step", type=int, default=None,
                    help="Run a specific step (for sweep scenarios); omit to run all steps")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print commands without executing")
    args = ap.parse_args()

    g, scenarios = load_scenarios()

    if args.list:
        print(f"\n{'ID':>3}  {'Tier':>4}  Name")
        print("─" * 60)
        for sid, s in sorted(scenarios.items()):
            steps = f"  ({len(s['steps'])} steps)" if s.get("steps") else ""
            print(f"{sid:>3}  {s['tier']:>4}  {s['name']}{steps}")
        return

    if args.id is None:
        ap.print_help()
        sys.exit(1)

    if args.id not in scenarios:
        print(f"Unknown scenario ID {args.id}. Use --list to see available scenarios.")
        sys.exit(1)

    run_scenario(scenarios[args.id], g, args.step, args.dry_run)


if __name__ == "__main__":
    main()