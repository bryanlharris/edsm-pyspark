#!/usr/bin/env python3
"""Run ingest pipelines for all bronze and silver tables.

This utility discovers all settings files in the bronze and silver layer
folders and sequentially invokes ``scripts/run_ingest.py`` for each table.
Output from each ingest run is captured in the ``logs`` directory while
progress messages are printed to the console. The script stops on the first
failure.
"""

from __future__ import annotations

from pathlib import Path
import sys

# Ensure the repository root is on the Python path so the ``functions``
# package can be imported when this script is executed directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from functions.sanity import validate_settings, initialize_empty_tables
from functions.utility import create_spark_session

import argparse
from glob import glob
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[1]
RUN_INGEST = REPO_ROOT / "scripts" / "run_ingest.py"
DEFAULT_LOG_DIR = REPO_ROOT / "logs"


def discover_tables(color: str) -> list[str]:
    """Return sorted table names for the given layer color."""
    paths = glob(str(REPO_ROOT / f"layer_*_{color}" / "*.json"))
    return sorted(Path(p).stem for p in paths)


def run_ingest(color: str, table: str, master: str, verbose: bool, log_dir: Path) -> int:
    """Invoke ``run_ingest.py`` for ``table`` and capture output."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{color}_{table}.log"
    cmd = [sys.executable, str(RUN_INGEST), color, table, "--master", master]
    if verbose:
        cmd.append("-v")

    print(f"Running {color} ingest for {table}... (log: {log_file})")
    with log_file.open("wb") as f:
        result = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)
    return result.returncode


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ingest for all tables")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    parser.add_argument("-v", "--verbose", action="store_true", help="Pass -v to run_ingest")
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR, help="Directory to store logs")
    args = parser.parse_args()

    spark = create_spark_session(args.master, "edsm-pipeline")

    try:
        validate_settings()
        initialize_empty_tables(spark)

        for color in ["bronze", "silver"]:
            for table in discover_tables(color):
                code = run_ingest(color, table, args.master, args.verbose, args.log_dir)
                if code != 0:
                    print(
                        f"Ingest failed for {color}/{table}. See {args.log_dir/(color + '_' + table + '.log')}"
                    )
                    sys.exit(code)

        print("All ingest tasks completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
