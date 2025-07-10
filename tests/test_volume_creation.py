import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from functions import utility, config

class DummySpark:
    def __init__(self):
        self.queries = []
    def sql(self, query):
        self.queries.append(query)

def test_create_landing_volume_uses_landing_root():
    spark = DummySpark()
    utility.create_volume_if_not_exists('cat', 'sch', 'landing', spark)
    assert spark.queries[-1] == (
        f"CREATE EXTERNAL VOLUME IF NOT EXISTS cat.sch.landing LOCATION '{config.S3_ROOT_LANDING}cat/sch/landing'"
    )

def test_create_utility_volume_uses_utility_root():
    spark = DummySpark()
    utility.create_volume_if_not_exists('cat', 'sch', 'utility', spark)
    assert spark.queries[-1] == (
        f"CREATE EXTERNAL VOLUME IF NOT EXISTS cat.sch.utility LOCATION '{config.S3_ROOT_UTILITY}cat/sch/utility'"
    )

def test_volume_root_without_trailing_slash(monkeypatch):
    spark = DummySpark()
    root_no_slash = config.S3_ROOT_LANDING.rstrip('/')
    monkeypatch.setattr(config, 'S3_ROOT_LANDING', root_no_slash)
    utility.create_volume_if_not_exists('cat', 'sch', 'landing', spark)
    assert spark.queries[-1] == (
        f"CREATE EXTERNAL VOLUME IF NOT EXISTS cat.sch.landing LOCATION '{root_no_slash}/cat/sch/landing'"
    )
