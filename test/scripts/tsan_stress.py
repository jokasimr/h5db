#!/usr/bin/env python3
"""Stress runner for TSAN-instrumented h5db builds.

This script repeatedly exercises the higher-risk h5_read paths:
- regular chunk cache boundaries
- projection/filter mismatches
- index + RSE pushdown range intersections
- sparse pushdown over cached columns
- logical partition ownership / batch-index paths
- large UNION ALL parallel scans
- randomized query interrupts

It is intended to be run against a TSAN reldebug DuckDB shell, e.g.

    ./venv/bin/python test/scripts/tsan_stress.py \
        --duckdb-binary ./build/reldebug/duckdb
"""

from __future__ import annotations

import argparse
import os
import platform
import random
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TSAN_OPTIONS = "halt_on_error=1 second_deadlock_stack=1 history_size=7"


class StressFailure(RuntimeError):
    """Raised when a scenario fails."""


@dataclass(frozen=True)
class Scenario:
    name: str
    sql_factory: Callable[[random.Random, Path], str]
    timeout_seconds: float = 120.0
    interrupt_window: Optional[tuple[float, float]] = None
    weight: int = 1


@dataclass
class ScenarioResult:
    scenario: Scenario
    duration_seconds: float
    interrupted: bool


def sql_escape(path: Path) -> str:
    return str(path).replace("'", "''")


def common_prelude(
    rng: random.Random, *, batch_size: Optional[str] = None, preserve_order: Optional[bool] = None
) -> str:
    threads = rng.choice([1, 2, 4, 8])
    statements = [
        "PRAGMA disable_progress_bar;",
        f"PRAGMA threads={threads};",
    ]
    if preserve_order is not None:
        statements.append(f"SET preserve_insertion_order={'true' if preserve_order else 'false'};")
    if batch_size is not None:
        statements.append(f"SET h5db_batch_size='{batch_size}';")
    return "\n".join(statements)


def scenario_cache_boundaries(rng: random.Random, temp_dir: Path) -> str:
    offset = rng.choice([14, 15, 16, 30, 31, 32])
    return f"""
    {common_prelude(rng, batch_size='1KB')}

    SELECT contig[1][1], chunked[4][4]
    FROM h5_read('test/data/cache_boundaries.h5', '/rows_33/contig', '/rows_33/chunked')
    ORDER BY contig[1][1]
    LIMIT 3 OFFSET {offset};

    CREATE TEMP TABLE cache_boundary_tmp AS
    SELECT * FROM h5_read('test/data/cache_boundaries.h5', '/rows_33/contig', '/rows_33/chunked');

    SELECT COUNT(*), SUM(contig[1][1]), MIN(chunked[1][1]), MAX(chunked[1][1])
    FROM cache_boundary_tmp;

    DROP TABLE cache_boundary_tmp;
    RESET h5db_batch_size;
    """


def scenario_projection_filter_mismatch(rng: random.Random, temp_dir: Path) -> str:
    filter_sql = rng.choice(
        [
            "WHERE floats > 0.1",
            "WHERE strings IN ('hello', 'test')",
            "WHERE strings = 'test' AND floats > 0.5",
        ]
    )
    return f"""
    {common_prelude(rng, batch_size=rng.choice(['64KB', '256KB', '1MB']))}

    SELECT SUM(integers)
    FROM h5_read('test/data/simple.h5', '/integers', '/floats', '/strings')
    {filter_sql};

    SELECT integers
    FROM h5_read('test/data/simple.h5', h5_alias('idx', h5_index()), '/integers', '/floats')
    WHERE idx BETWEEN 2 AND 7
    ORDER BY floats DESC
    LIMIT 3;

    RESET h5db_batch_size;
    """


def scenario_large_index_rse_intersection(rng: random.Random, temp_dir: Path) -> str:
    run = rng.choice(
        [
            (10, 1.5, 0, 1_999_999),
            (20, 2.5, 2_000_000, 3_999_999),
            (30, 3.5, 4_000_000, 5_999_999),
            (40, 4.5, 6_000_000, 7_999_999),
            (50, 5.5, 8_000_000, 9_999_999),
        ]
    )
    pad_left = rng.randint(0, 8192)
    pad_right = rng.randint(0, 8192)
    idx_start = max(0, run[2] - pad_left)
    idx_end = min(9_999_999, run[3] + pad_right)
    return f"""
    {common_prelude(rng, batch_size=rng.choice(['128KB', '512KB', '2MB']))}

    SELECT COUNT(*), MIN(regular), MAX(regular)
    FROM h5_read(
        'test/data/large/large_pushdown_test.h5',
        h5_alias('idx', h5_index()),
        '/regular',
        h5_rse('/int_rse_starts', '/int_rse_values'),
        h5_rse('/float_rse_starts', '/float_rse_values')
    )
    WHERE idx BETWEEN {idx_start} AND {idx_end}
      AND int_rse_values = {run[0]}
      AND float_rse_values = {run[1]};

    RESET h5db_batch_size;
    """


def scenario_sparse_cached_pushdown(rng: random.Random, temp_dir: Path) -> str:
    return f"""
    {common_prelude(rng, batch_size='1KB')}

    SELECT COUNT(*), SUM(regular_contig[1][1]), SUM(regular_chunked[1][1])
    FROM h5_read(
        'test/data/sparse_pushdown_cache.h5',
        '/regular_contig',
        '/regular_chunked',
        h5_rse('/selector_starts', '/selector_values')
    ) WHERE selector_values = {rng.choice([0, 1])};

    SELECT MIN(label), MAX(label)
    FROM h5_read(
        'test/data/sparse_pushdown_cache.h5',
        '/regular_contig',
        '/label',
        h5_rse('/selector_starts', '/selector_values')
    ) WHERE selector_values = 1;

    RESET h5db_batch_size;
    """


def scenario_sparse_partition_copy(rng: random.Random, temp_dir: Path) -> str:
    output_path = temp_dir / f"sparse_partition_{rng.randint(0, 1_000_000)}.csv"
    return f"""
    {common_prelude(rng, preserve_order=True)}

    COPY (
        SELECT value_chunked
        FROM h5_read(
            'test/data/sparse_partition_pushdown.h5',
            '/value_chunked',
            h5_rse('/selector_starts', '/selector_values')
        ) WHERE selector_values = 1
    ) TO '{sql_escape(output_path)}' (FORMAT csv, HEADER false);

    SELECT COUNT(*), SUM(value_chunked), MAX(value_chunked)
    FROM read_csv('{sql_escape(output_path)}', columns={{'value_chunked': 'INTEGER'}}, header=false);
    """


def scenario_partition_ownership(rng: random.Random, temp_dir: Path) -> str:
    output_path = temp_dir / f"partition_ownership_{rng.randint(0, 1_000_000)}.csv"
    limit_offset = rng.choice([20478, 20479, 20480])
    return f"""
    {common_prelude(rng, preserve_order=True)}

    SELECT COUNT(*), MIN(index), MAX(index), MIN(rows_40961), MAX(rows_40961)
    FROM h5_read('test/data/partition_ownership.h5', h5_index(), '/rows_40961')
    WHERE index >= 20478 AND index <= 20482;

    COPY (
        SELECT *
        FROM h5_read('test/data/partition_ownership.h5', '/rows_20481')
        LIMIT 3 OFFSET {limit_offset}
    ) TO '{sql_escape(output_path)}' (FORMAT csv, HEADER false);

    SELECT COUNT(*), MIN(rows_20481), MAX(rows_20481)
    FROM read_csv('{sql_escape(output_path)}', columns={{'rows_20481': 'INTEGER'}}, header=false);
    """


def scenario_nd_cache_index_slice(rng: random.Random, temp_dir: Path) -> str:
    start = rng.randint(0, 999_500)
    end = min(start + rng.randint(64, 4096), 999_999)
    dataset = rng.choice(
        [
            ("/array_2d_chunked_small", "array_2d_chunked_small[1]"),
            ("/array_2d_chunked_partial", "array_2d_chunked_partial[5]"),
            ("/tensor_3d_chunked_large", "tensor_3d_chunked_large[1][1]"),
            ("/tensor_4d_chunked_small", "tensor_4d_chunked_small[1][1][1]"),
        ]
    )
    return f"""
    {common_prelude(rng, batch_size=rng.choice(['64KB', '256KB', '1MB']))}

    SELECT COUNT(*), SUM(val)
    FROM (
        SELECT {dataset[1]} AS val
        FROM h5_read('test/data/nd_cache_test.h5', h5_alias('idx', h5_index()), '{dataset[0]}')
        WHERE idx BETWEEN {start} AND {end}
    );

    RESET h5db_batch_size;
    """


def scenario_parallel_union_rse(rng: random.Random, temp_dir: Path) -> str:
    detector_count = rng.choice([3, 5])
    subqueries = []
    for detector in range(1, detector_count + 1):
        subqueries.append(
            f"""
            SELECT *
            FROM h5_read(
                'test/data/large/large_multithreading.h5',
                '/detector_{detector}/event_id',
                h5_rse('/detector_{detector}/event_index', '/detector_{detector}/event_time_zero')
            )
            """
        )
    union_query = "\nUNION ALL\n".join(textwrap.dedent(part).strip() for part in subqueries)
    return f"""
    {common_prelude(rng, batch_size=rng.choice(['256KB', '1MB', '4MB']))}

    SELECT COUNT(*), COUNT(DISTINCT event_time_zero), MIN(event_id), MAX(event_id)
    FROM (
        {union_query}
    );

    RESET h5db_batch_size;
    """


def scenario_interrupt_large_union(rng: random.Random, temp_dir: Path) -> str:
    detector_count = rng.choice([5, 10])
    subqueries = []
    for detector in range(1, detector_count + 1):
        subqueries.append(
            f"SELECT event_id FROM h5_read('test/data/large/large_multithreading.h5', '/detector_{detector}/event_id')"
        )
    union_query = "\nUNION ALL\n".join(subqueries)
    return f"""
    {common_prelude(rng, batch_size=rng.choice(['128KB', '512KB', '2MB']))}

    SELECT SUM(event_id)
    FROM (
        {union_query}
    );
    """


def scenario_interrupt_pushdown(rng: random.Random, temp_dir: Path) -> str:
    threshold = rng.choice([10, 20, 30, 40, 50])
    return f"""
    {common_prelude(rng, batch_size=rng.choice(['64KB', '256KB', '1MB']))}

    SELECT SUM(regular)
    FROM h5_read(
        'test/data/large/large_pushdown_test.h5',
        '/regular',
        h5_rse('/int_rse_starts', '/int_rse_values')
    )
    WHERE int_rse_values >= {threshold};
    """


SCENARIOS = [
    Scenario("cache_boundaries", scenario_cache_boundaries, weight=2),
    Scenario("projection_filter_mismatch", scenario_projection_filter_mismatch, weight=2),
    Scenario("large_index_rse_intersection", scenario_large_index_rse_intersection, weight=3),
    Scenario("sparse_cached_pushdown", scenario_sparse_cached_pushdown, weight=2),
    Scenario("sparse_partition_copy", scenario_sparse_partition_copy, weight=2),
    Scenario("partition_ownership", scenario_partition_ownership, weight=2),
    Scenario("nd_cache_index_slice", scenario_nd_cache_index_slice, weight=3),
    Scenario("parallel_union_rse", scenario_parallel_union_rse, timeout_seconds=180.0, weight=2),
    Scenario(
        "interrupt_large_union", scenario_interrupt_large_union, timeout_seconds=180.0, interrupt_window=(0.02, 0.20)
    ),
    Scenario("interrupt_pushdown", scenario_interrupt_pushdown, timeout_seconds=120.0, interrupt_window=(0.01, 0.15)),
]


def build_command(binary: Path, disable_aslr: bool) -> list[str]:
    command = []
    if disable_aslr:
        setarch = shutil.which("setarch")
        if setarch and platform.system() == "Linux" and platform.machine() in {"x86_64", "amd64"}:
            command.extend([setarch, platform.machine(), "-R"])
    command.append(str(binary))
    command.extend(["-csv", "-c"])
    return command


def normalize_sql(sql: str) -> str:
    return textwrap.dedent(sql).strip() + "\n"


def tsan_detected(stdout: str, stderr: str) -> bool:
    haystack = f"{stdout}\n{stderr}"
    return "threadsanitizer" in haystack.lower()


def record_failure(output_dir: Path, iteration: int, scenario: Scenario, sql: str, stdout: str, stderr: str) -> None:
    failure_dir = output_dir / f"failure_{iteration:04d}_{scenario.name}"
    failure_dir.mkdir(parents=True, exist_ok=True)
    (failure_dir / "query.sql").write_text(sql, encoding="utf-8")
    (failure_dir / "stdout.txt").write_text(stdout, encoding="utf-8")
    (failure_dir / "stderr.txt").write_text(stderr, encoding="utf-8")


def run_scenario(
    binary: Path,
    scenario: Scenario,
    rng: random.Random,
    output_dir: Path,
    iteration: int,
    tsan_options: str,
    disable_aslr: bool,
) -> ScenarioResult:
    sql = normalize_sql(scenario.sql_factory(rng, output_dir))
    command = build_command(binary, disable_aslr)
    env = os.environ.copy()
    env["TSAN_OPTIONS"] = tsan_options
    start = time.monotonic()

    if scenario.interrupt_window is None:
        completed = subprocess.run(
            command + [sql],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=scenario.timeout_seconds,
            check=False,
        )
        duration = time.monotonic() - start
        if completed.returncode != 0 or tsan_detected(completed.stdout, completed.stderr):
            record_failure(output_dir, iteration, scenario, sql, completed.stdout, completed.stderr)
            raise StressFailure(
                f"Scenario {scenario.name} failed with exit code {completed.returncode}. " f"Artifacts: {output_dir}"
            )
        return ScenarioResult(scenario, duration, interrupted=False)

    process = subprocess.Popen(
        command + [sql],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    interrupted = False
    try:
        interrupt_after = rng.uniform(*scenario.interrupt_window)
        try:
            stdout, stderr = process.communicate(timeout=interrupt_after)
        except subprocess.TimeoutExpired:
            interrupted = True
            os.killpg(process.pid, signal.SIGINT)
            try:
                stdout, stderr = process.communicate(timeout=scenario.timeout_seconds)
            except subprocess.TimeoutExpired as exc:
                os.killpg(process.pid, signal.SIGKILL)
                stdout, stderr = process.communicate()
                record_failure(output_dir, iteration, scenario, sql, stdout, stderr)
                raise StressFailure(f"Scenario {scenario.name} did not exit after SIGINT (pid={process.pid}).") from exc
    finally:
        duration = time.monotonic() - start

    if tsan_detected(stdout, stderr):
        record_failure(output_dir, iteration, scenario, sql, stdout, stderr)
        raise StressFailure(f"Scenario {scenario.name} reported TSAN diagnostics. Artifacts: {output_dir}")
    if process.returncode == 66:
        record_failure(output_dir, iteration, scenario, sql, stdout, stderr)
        raise StressFailure(f"Scenario {scenario.name} aborted with TSAN runtime error. Artifacts: {output_dir}")
    return ScenarioResult(scenario, duration, interrupted=interrupted)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duckdb-binary",
        type=Path,
        default=REPO_ROOT / "build" / "reldebug" / "duckdb",
        help="Path to the TSAN-instrumented DuckDB shell.",
    )
    parser.add_argument("--iterations", type=int, default=25, help="Number of randomized iterations to run.")
    parser.add_argument("--seed", type=int, default=None, help="Random seed. Defaults to current time.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=REPO_ROOT / "test" / ".tsan_stress",
        help="Directory for stress-run artifacts.",
    )
    parser.add_argument(
        "--scenario",
        action="append",
        default=[],
        help="Scenario name to run. May be specified multiple times. Defaults to all scenarios.",
    )
    parser.add_argument(
        "--tsan-options",
        default=DEFAULT_TSAN_OPTIONS,
        help="TSAN_OPTIONS passed to the stress subprocesses.",
    )
    parser.add_argument(
        "--allow-aslr",
        action="store_true",
        help="Do not wrap the DuckDB shell in 'setarch -R'.",
    )
    return parser.parse_args()


def ensure_test_data() -> None:
    subprocess.run(
        ["bash", str(REPO_ROOT / "test" / "data" / "ensure_test_data.sh")],
        cwd=REPO_ROOT,
        check=True,
    )


def select_scenarios(requested: list[str]) -> list[Scenario]:
    if not requested:
        return SCENARIOS
    lookup = {scenario.name: scenario for scenario in SCENARIOS}
    missing = sorted(set(requested) - lookup.keys())
    if missing:
        raise SystemExit(f"Unknown scenario(s): {', '.join(missing)}")
    return [lookup[name] for name in requested]


def main() -> int:
    args = parse_args()
    if not args.duckdb_binary.exists():
        raise SystemExit(f"DuckDB binary not found: {args.duckdb_binary}")

    seed = args.seed if args.seed is not None else int(time.time())
    rng = random.Random(seed)
    scenarios = select_scenarios(args.scenario)
    weighted_scenarios = [scenario for scenario in scenarios for _ in range(max(scenario.weight, 1))]

    args.output_dir.mkdir(parents=True, exist_ok=True)
    run_dir = args.output_dir / time.strftime("run_%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    print(f"Seed: {seed}")
    print(f"DuckDB binary: {args.duckdb_binary}")
    print(f"Artifacts: {run_dir}")
    print(f"Disable ASLR: {'no' if args.allow_aslr else 'yes'}")
    print(f"TSAN_OPTIONS: {args.tsan_options}")
    print(f"Scenarios: {', '.join(s.name for s in scenarios)}")
    ensure_test_data()

    results: list[ScenarioResult] = []
    for iteration in range(1, args.iterations + 1):
        scenario = rng.choice(weighted_scenarios)
        print(f"[{iteration:03d}/{args.iterations:03d}] {scenario.name}")
        try:
            result = run_scenario(
                args.duckdb_binary,
                scenario,
                rng,
                run_dir,
                iteration,
                args.tsan_options,
                disable_aslr=not args.allow_aslr,
            )
        except subprocess.TimeoutExpired as exc:
            raise SystemExit(f"Scenario {scenario.name} timed out after {exc.timeout} seconds") from exc
        except StressFailure as exc:
            print(str(exc), file=sys.stderr)
            return 1
        results.append(result)

    interrupted_count = sum(1 for result in results if result.interrupted)
    total_duration = sum(result.duration_seconds for result in results)
    print(
        f"Completed {len(results)} iterations in {total_duration:.1f}s "
        f"({interrupted_count} interrupted queries, no TSAN findings)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
