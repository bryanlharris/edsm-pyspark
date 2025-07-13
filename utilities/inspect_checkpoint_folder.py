#!/usr/bin/env python3
"""Inspect a Delta checkpoint folder for a given table."""

import argparse
import json
from glob import glob
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from functions.utility import (
    inspect_checkpoint_folder,
    apply_job_type,
)
from functions.config import PROJECT_ROOT


def main() -> None:
    layer_map = {
        "silver": PROJECT_ROOT / "layer_02_silver",
        "bronze": PROJECT_ROOT / "layer_01_bronze",
    }

    parser = argparse.ArgumentParser(description="Inspect checkpoint directory")
    parser.add_argument(
        "color",
        choices=layer_map.keys(),
        help="Table color (bronze or silver)",
    )
    parser.add_argument("table", help="Table name")
    args = parser.parse_args()

    settings_dir = layer_map[args.color]
    paths = glob(str(settings_dir / "*.json"))
    table_map = {Path(p).stem: p for p in paths}
    if args.table not in table_map:
        parser.error(f"Unknown table '{args.table}' for color '{args.color}'")

    settings_path = table_map[args.table]
    settings = json.loads(Path(settings_path).read_text())
    settings = apply_job_type(settings)

    inspect_checkpoint_folder(args.table, settings, spark=None)


if __name__ == "__main__":
    main()
