import argparse
import json
from pathlib import Path
from glob import glob
from pyspark.sql import SparkSession
from functions.history import build_and_merge_file_history, transaction_history
from functions.utility import schema_exists


def discover_tables(base_dir: Path) -> dict:
    """Return mapping of table name to config path under ``layer_01_bronze``."""
    paths = glob(str(base_dir / 'layer_01_bronze' / '*.json'))
    return {Path(p).stem: p for p in paths}


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    table_map = discover_tables(repo_root)
    if not table_map:
        raise SystemExit('No configuration files found in layer_01_bronze')

    parser = argparse.ArgumentParser(
        description='Build file and transaction history for a Delta table'
    )
    parser.add_argument(
        '--table',
        choices=sorted(table_map.keys()),
        required=True,
        help='Table name with JSON settings in layer_01_bronze'
    )
    args = parser.parse_args()

    settings_path = Path(table_map[args.table])
    with open(settings_path) as f:
        config = json.load(f)

    full_table_name = config['dst_table_name']
    history_schema = config.get('history_schema')
    catalog = full_table_name.split('.')[0]

    settings_msg = (
        f"\n\nContents of {settings_path.name}:\n\n" + json.dumps(config, indent=4)
    )
    print(settings_msg)

    spark = SparkSession.builder.getOrCreate()

    if history_schema is None:
        print('Skipping history build: no history_schema provided')
    elif schema_exists(catalog, history_schema, spark):
        build_and_merge_file_history(full_table_name, history_schema, spark)
        transaction_history(full_table_name, history_schema, spark)
    else:
        print(f'Skipping history build: schema {catalog}.{history_schema} not found')


if __name__ == '__main__':
    main()
