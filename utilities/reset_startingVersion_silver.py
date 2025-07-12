#!/usr/bin/env python3
"""Remove ``startingVersion`` from silver settings files."""

import argparse
import json
from pathlib import Path
from glob import glob


def main() -> None:
    parser = argparse.ArgumentParser(description="Remove startingVersion from settings")
    parser.add_argument(
        "--settings-path",
        type=Path,
        default=Path("../layer_02_silver"),
        help="Directory containing settings JSON files",
    )
    args = parser.parse_args()

    paths = glob(str(args.settings_path / "*.json"))
    for path in paths:
        p = Path(path)
        print(f"Checking: {p}")
        settings = json.loads(p.read_text())
        if "startingVersion" in settings.get("readStreamOptions", {}):
            print(f"Removing startingVersion from: {p.name}")
            del settings["readStreamOptions"]["startingVersion"]
            p.write_text(json.dumps(settings, indent=4))
        else:
            print(f"No startingVersion found in: {p.name}")


if __name__ == "__main__":
    main()
