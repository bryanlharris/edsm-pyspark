#!/usr/bin/env python3
"""Remove all files from the landing zone."""

import argparse
import shutil
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Erase landing zone directory")
    parser.add_argument(
        "path",
        type=Path,
        nargs="?",
        default=Path("/Volumes/edsm/bronze/landing"),
        help="Landing zone path",
    )
    args = parser.parse_args()

    if args.path.exists():
        for child in args.path.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()


if __name__ == "__main__":
    main()
