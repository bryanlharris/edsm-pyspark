#!/usr/bin/env python3
"""Run the downloader shell script outside of Databricks."""
import argparse
import subprocess
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the nightly dump downloader")
    parser.add_argument(
        "--script",
        type=Path,
        default=Path(__file__).with_name("downloader.sh"),
        help="Path to downloader.sh",
    )
    args = parser.parse_args()
    subprocess.check_call(["bash", str(args.script)])


if __name__ == "__main__":
    main()
