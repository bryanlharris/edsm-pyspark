import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from functions import utility, config

class DummySpark:
    def __init__(self):
        self.queries = []
        self.schemas = set()
        self.volumes = set()

    class _Result:
        def __init__(self, count):
            self._count = count

        def count(self):
            return self._count

    def sql(self, query):
        self.queries.append(query)

        if query.startswith("SHOW SCHEMAS IN"):
            catalog = query.split()[3]
            schema = query.split()[-1].strip("'")
            count = 1 if (catalog, schema) in self.schemas else 0
            return self._Result(count)

        if query.startswith("SHOW VOLUMES IN"):
            cat_schema = query.split()[3]
            catalog, schema = cat_schema.split(".")
            volume = query.split()[-1].strip("'")
            count = 1 if (catalog, schema, volume) in self.volumes else 0
            return self._Result(count)

        return None


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



def test_create_volume_message_depends_on_existence(capsys):
    spark = DummySpark()
    # Volume doesn't exist - should print message
    utility.create_volume_if_not_exists('cat', 'sch', 'landing', spark)
    out = capsys.readouterr().out
    assert "Volume did not exist and was created" in out

    # Mark volume as existing and call again - no message expected
    spark.volumes.add(('cat', 'sch', 'landing'))
    utility.create_volume_if_not_exists('cat', 'sch', 'landing', spark)
    out = capsys.readouterr().out
    assert out == ""


def test_create_schema_message_depends_on_existence(capsys):
    spark = DummySpark()
    # Schema not present
    utility.create_schema_if_not_exists('cat', 'sch', spark)
    out = capsys.readouterr().out
    assert "Schema did not exist and was created" in out

    # Already present
    spark.schemas.add(('cat', 'sch'))
    utility.create_schema_if_not_exists('cat', 'sch', spark)
    out = capsys.readouterr().out
    assert out == ""

def test_volume_root_without_trailing_slash(monkeypatch):
    spark = DummySpark()
    root_no_slash = config.S3_ROOT_LANDING.rstrip('/')
    monkeypatch.setattr(config, 'S3_ROOT_LANDING', root_no_slash)
    utility.create_volume_if_not_exists('cat', 'sch', 'landing', spark)
    assert spark.queries[-1] == (
        f"CREATE EXTERNAL VOLUME IF NOT EXISTS cat.sch.landing LOCATION '{root_no_slash}/cat/sch/landing'"
    )

