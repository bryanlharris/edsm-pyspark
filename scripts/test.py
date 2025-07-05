import json
from pathlib import Path
from functions.utility import get_function


def main():
    merge = get_function("functions.utility._merge_dicts")
    result = merge({"a": 1}, {"b": 2})
    print("Merge result:", result)

    json_path = Path(__file__).resolve().parents[1] / "layer_01_bronze" / "codex.json"
    with open(json_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    print("Loaded JSON job_type:", data.get("job_type"))


if __name__ == "__main__":
    main()
