# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = ["cassandra-driver==3.30.1"]
# ///
r"""MovieLens: compare a filtered scan against a partition-key lookup.

Windows PowerShell:
  uv run --python 3.12 benchmark.py load --csv .\ml-25m\ratings.csv --rows 100000
  uv run --python 3.12 benchmark.py run --manifest results\ml_bench_100000_manifest.json

Uses NEW benchmark keyspaces only. No DROP/TRUNCATE. No fabricated measurements.
"""

import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import hashlib
from itertools import islice
import json
from pathlib import Path
import re
import statistics
import struct
import sys
import time

import cassandra
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.concurrent import execute_concurrent
from cassandra.policies import FallthroughRetryPolicy, WhiteListRoundRobinPolicy
from cassandra.query import tuple_factory


COLUMNS = "user_id, rating_time, movie_id, rating"
QUERIES = {
    "filtered_scan": f"SELECT {COLUMNS} FROM ratings_by_user WHERE movie_id=? ALLOW FILTERING",
    "partition_lookup": f"SELECT {COLUMNS} FROM ratings_by_movie WHERE movie_id=?",
}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding="utf-8")
    temporary.replace(path)


def keyspace_name(value):
    if not re.fullmatch(r"ml_bench_[a-z0-9_]{1,30}", value):
        raise ValueError("Keyspace must start with ml_bench_ and use lowercase letters/digits/_.")
    return value


def read_ratings(path, limit):
    """Stream the first N CSV records; tuples match COLUMNS above."""
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not {"userId", "movieId", "rating", "timestamp"}.issubset(reader.fieldnames or []):
            raise ValueError("Expected MovieLens header: userId,movieId,rating,timestamp")
        for row in islice(reader, limit):
            yield (int(row["userId"]), int(row["timestamp"]),
                   int(row["movieId"]), float(row["rating"]))


def inspect_csv(path, limit, movie=None):
    digest, counts, expected = hashlib.sha256(), Counter(), {}
    total = 0
    for record in read_ratings(path, limit):
        total += 1
        digest.update(struct.pack("<iqif", *record))
        counts[record[2]] += 1
        if record[2] == movie:
            # One primary key maps to one final value, just as an INSERT upsert does.
            expected[record[:3]] = struct.unpack("<f", struct.pack("<f", record[3]))[0]
    if total != limit:
        raise ValueError(f"CSV has only {total:,} records; requested {limit:,}.")
    return digest.hexdigest(), counts, sorted((*key, value) for key, value in expected.items())


def connect(args):
    # Only cassandra1:9042 is published to Windows in the student's Compose file.
    # Do not let the driver try Docker-private peer IPs from the Windows host.
    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy([args.host]),
        consistency_level=ConsistencyLevel.ONE,
        request_timeout=args.timeout,
        retry_policy=FallthroughRetryPolicy(),  # Surface failures; no hidden retries.
        row_factory=tuple_factory,
    )
    cluster = Cluster([args.host], port=args.port, protocol_version=4,
                      execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    try:
        session = cluster.connect()
        session.default_fetch_size = args.fetch_size
        version = session.execute("SELECT release_version FROM system.local").one()[0]
        return cluster, session, version
    except BaseException:
        cluster.shutdown()
        raise


def load_data(args):
    csv_path = args.csv.resolve()
    keyspace = keyspace_name(args.keyspace or f"ml_bench_{args.rows}")
    manifest_path = args.out / f"{keyspace}_manifest.json"
    if manifest_path.exists():
        raise ValueError(f"Manifest already exists: {manifest_path}. Use a NEW --keyspace.")
    print(f"Checking first {args.rows:,} CSV records...", flush=True)
    source_hash, counts, _ = inspect_csv(csv_path, args.rows)
    cluster, session, version = connect(args)
    try:
        existing = session.execute(
            "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name=%s", (keyspace,)
        ).one()
        if existing:
            raise ValueError(f"{keyspace} already exists. Use a NEW --keyspace; no data was overwritten.")
        session.execute(f"CREATE KEYSPACE {keyspace} WITH replication = "
                        "{'class':'NetworkTopologyStrategy','datacenter1':3}")
        session.set_keyspace(keyspace)
        session.execute("""CREATE TABLE ratings_by_user (
            user_id int, rating_time bigint, movie_id int, rating float,
            PRIMARY KEY ((user_id), rating_time, movie_id)
        ) WITH CLUSTERING ORDER BY (rating_time DESC)""")
        session.execute("""CREATE TABLE ratings_by_movie (
            user_id int, rating_time bigint, movie_id int, rating float,
            PRIMARY KEY ((movie_id), rating_time, user_id)
        ) WITH CLUSTERING ORDER BY (rating_time DESC)""")
        metadata = {
            "keyspace": keyspace, "csv": str(csv_path), "rows": args.rows,
            "source_sha256": source_hash, "complete": False,
            "created_utc": utc_now(), "write_consistency": "ALL", "replication_factor": 3,
            "cassandra_version": version, "driver_version": cassandra.__version__,
            "top_movies": counts.most_common(10),
        }
        write_json(manifest_path, metadata)
        inserts = [session.prepare(f"INSERT INTO {table} ({COLUMNS}) VALUES (?, ?, ?, ?)")
                   for table in ("ratings_by_user", "ratings_by_movie")]
        for statement in inserts:
            # After import succeeds, all three replicas acknowledged each table's writes.
            statement.consistency_level = ConsistencyLevel.ALL
        iterator = iter(read_ratings(csv_path, args.rows))
        done, loaded_hash = 0, hashlib.sha256()
        start = time.perf_counter()
        print("Importing into TWO tables, RF=3, writes=ALL; keep all 3 nodes running.", flush=True)
        while chunk := list(islice(iterator, 500)):
            requests = [(statement, record) for record in chunk for statement in inserts]
            results = execute_concurrent(session, requests, concurrency=args.concurrency,
                                         raise_on_first_error=False)
            errors = [str(result) for success, result in results if not success]
            if errors:
                raise RuntimeError(f"Import incomplete ({len(errors)} failed writes). First: {errors[0]}")
            for record in chunk:
                loaded_hash.update(struct.pack("<iqif", *record))
            done += len(chunk)
            if done % 10000 == 0 or done == args.rows:
                print(f"Imported {done:,}/{args.rows:,} source rows into each table "
                      f"({time.perf_counter() - start:.1f}s)", flush=True)
        if done != args.rows or loaded_hash.hexdigest() != source_hash:
            raise RuntimeError("CSV changed during import. This experiment is incomplete.")
        metadata.update(complete=True, completed_utc=utc_now(),
                        import_seconds=time.perf_counter() - start)
        write_json(manifest_path, metadata)
        print(f"DONE. Manifest: {manifest_path}")
        print("Popular movie IDs (movie_id, count):", counts.most_common(5))
        print("Wait for background activity to settle, then run the benchmark.")
    finally:
        cluster.shutdown()


def measure(session, statement, expected):
    """Time ALL pages. Validate outside the measured interval, ignoring row order."""
    start = time.perf_counter()
    try:
        rows = list(session.execute(statement))  # Iteration fetches subsequent pages too.
    except Exception as error:
        return {"status": "ERROR", "elapsed_ms": (time.perf_counter() - start) * 1000,
                "returned_rows": None, "detail": f"{type(error).__name__}: {error}"}
    elapsed = (time.perf_counter() - start) * 1000
    valid = sorted(tuple(row) for row in rows) == expected
    return {"status": "OK" if valid else "MISMATCH", "elapsed_ms": elapsed,
            "returned_rows": len(rows), "detail": "" if valid else "Results differ from source CSV"}


def run_benchmark(args):
    metadata = json.loads(args.manifest.read_text(encoding="utf-8"))
    if not metadata.get("complete"):
        raise ValueError("Import is incomplete. Do not benchmark partial data; load into a NEW keyspace.")
    keyspace = keyspace_name(metadata["keyspace"])
    csv_path = (args.csv or Path(metadata["csv"])).resolve()
    source_hash, counts, expected = inspect_csv(csv_path, metadata["rows"], args.movie)
    if source_hash != metadata["source_sha256"]:
        raise ValueError("CSV subset differs from the imported data.")
    if not expected:
        raise ValueError(f"Movie {args.movie} has no ratings in this subset. "
                         f"Try --movie with one of: {counts.most_common(5)}")
    args.out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%f")
    prefix = f"{keyspace}_movie{args.movie}_{stamp}"
    csv_output = args.out / f"{prefix}_runs.csv"
    json_output = args.out / f"{prefix}_summary.json"
    cluster, session, version = connect(args)
    records = []
    try:
        session.set_keyspace(keyspace)
        statements = {}
        for label, query in QUERIES.items():
            statement = session.prepare(query).bind((args.movie,))
            statement.consistency_level = getattr(ConsistencyLevel, args.consistency)
            statement.fetch_size = args.fetch_size
            statements[label] = statement
        print(f"Keyspace={keyspace}; imported={metadata['rows']:,}; movie={args.movie}; "
              f"expected={len(expected):,} ratings")
        print(f"Read CL={args.consistency}; page size={args.fetch_size}; "
              f"rounds={args.rounds}; Cassandra={version}")
        print("One warm-up per query, then alternating query order; tracing disabled.", flush=True)
        fields = ["phase", "round", "query", "status", "elapsed_ms", "returned_rows", "detail"]
        with csv_output.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for index in range(args.rounds + 1):
                phase = "warmup" if index == 0 else "measured"
                order = list(statements)
                if index % 2:
                    order.reverse()
                for label in order:
                    result = measure(session, statements[label], expected)
                    record = dict(phase=phase, round=index, query=label, **result)
                    records.append(record)
                    writer.writerow(record)
                    handle.flush()
                    print(f"{phase:8} {index:2} | {label:16} | {result['status']:8} | "
                          f"{result['elapsed_ms']:10.2f} ms | rows={result['returned_rows']}", flush=True)
                    if result["detail"]:
                        print("  " + result["detail"], flush=True)
                if index < args.rounds:
                    time.sleep(args.pause)
        summary = {
            "created_utc": utc_now(), "keyspace": keyspace, "imported_source_rows": metadata["rows"],
            "movie_id": args.movie, "expected_result_rows": len(expected),
            "read_consistency": args.consistency, "write_consistency": metadata["write_consistency"],
            "replication_factor": 3, "fetch_size": args.fetch_size, "rounds": args.rounds,
            "host": args.host, "port": args.port, "client_timeout_seconds": args.timeout,
            "pause_seconds": args.pause, "cassandra_version": version,
            "driver_version": cassandra.__version__, "python_version": sys.version,
            "queries": QUERIES, "runs_csv": str(csv_output.resolve()),
            "warmup": [r for r in records if r["phase"] == "warmup"],
            "query_statistics": {}, "scan_over_lookup_ratio": None,
        }
        for label in statements:
            measured = [r for r in records if r["phase"] == "measured" and r["query"] == label]
            times = [r["elapsed_ms"] for r in measured if r["status"] == "OK"]
            entry = {"ok": len(times), "errors_or_mismatches": len(measured) - len(times),
                     "median_ms": statistics.median(times) if times else None,
                     "min_ms": min(times) if times else None, "max_ms": max(times) if times else None}
            summary["query_statistics"][label] = entry
            print(f"{label}: {json.dumps(entry)}")
        # A timeout is NOT a successful query taking timeout_seconds.
        if all(r["status"] == "OK" for r in records):
            stats = summary["query_statistics"]
            ratio = stats["filtered_scan"]["median_ms"] / stats["partition_lookup"]["median_ms"]
            summary["scan_over_lookup_ratio"] = ratio
            print(f"VERIFIED: both queries match CSV on every run. Scan/lookup median ratio = {ratio:.2f}x")
        else:
            print("INCOMPLETE COMPARISON: errors/mismatches occurred; no overall speed ratio reported.")
        write_json(json_output, summary)
        print(f"Saved: {csv_output}\nSaved: {json_output}")
        return 0 if summary["scan_over_lookup_ratio"] is not None else 2
    finally:
        cluster.shutdown()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument("--host", default="127.0.0.1")
    shared.add_argument("--port", type=int, default=9042)
    shared.add_argument("--timeout", type=float, default=60, help="Client request timeout; server has its own limits")
    shared.add_argument("--fetch-size", type=int, default=1000)
    shared.add_argument("--out", type=Path, default=Path("results"))
    commands = parser.add_subparsers(dest="command", required=True)
    load = commands.add_parser("load", parents=[shared], help="Create NEW tables and import the same subset into both")
    load.add_argument("--csv", type=Path, required=True)
    load.add_argument("--rows", type=int, default=100000)
    load.add_argument("--keyspace", help="Default: ml_bench_<rows>; must be new")
    load.add_argument("--concurrency", type=int, default=16)
    run = commands.add_parser("run", parents=[shared], help="Compare query latency and validate results against CSV")
    run.add_argument("--manifest", type=Path, required=True)
    run.add_argument("--csv", type=Path, help="Override CSV path if you moved it")
    run.add_argument("--movie", type=int, default=101)
    run.add_argument("--rounds", type=int, default=8)
    run.add_argument("--consistency", choices=["ONE", "QUORUM", "ALL"], default="ONE")
    run.add_argument("--pause", type=float, default=1, help="Seconds between pairs of queries")
    args = parser.parse_args()
    if args.timeout <= 0 or args.fetch_size <= 0:
        parser.error("--timeout and --fetch-size must be positive")
    if args.command == "load" and (args.rows <= 0 or args.concurrency <= 0):
        parser.error("--rows and --concurrency must be positive")
    if args.command == "run" and (args.rounds < 2 or args.pause < 0):
        parser.error("Use --rounds >= 2 and --pause >= 0")
    try:
        return load_data(args) if args.command == "load" else run_benchmark(args)
    except KeyboardInterrupt:
        print("Stopped. Partial imports remain marked incomplete; no tables were deleted.", file=sys.stderr)
        return 130
    except Exception as error:
        print(f"ERROR: {type(error).__name__}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
