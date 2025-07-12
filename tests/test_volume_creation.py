import pathlib
import sys
from pathlib import Path

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from functions import utility, config

class DummySpark:
    def __init__(self):
        self.queries = []
        self.schemas = set()

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

        return None




def test_create_landing_volume_uses_landing_root(tmp_path, monkeypatch):
    spark = DummySpark()
    root = tmp_path / "landing_root"
    monkeypatch.setattr(config, 'S3_ROOT_LANDING', f"{root}/")
    monkeypatch.setattr(utility, 'S3_ROOT_LANDING', f"{root}/")
    utility.create_volume_if_not_exists('cat', 'sch', 'landing', spark)
    assert (root / 'cat' / 'sch' / 'landing').is_dir()

def test_create_utility_volume_uses_utility_root(tmp_path, monkeypatch):
    spark = DummySpark()
    root = tmp_path / "utility_root"
    monkeypatch.setattr(config, 'S3_ROOT_UTILITY', f"{root}/")
    monkeypatch.setattr(utility, 'S3_ROOT_UTILITY', f"{root}/")
    utility.create_volume_if_not_exists('cat', 'sch', 'utility', spark)
    assert (root / 'cat' / 'sch' / 'utility').is_dir()



def test_create_volume_message_depends_on_existence(tmp_path, monkeypatch, capsys):
    spark = DummySpark()
    root = tmp_path / "landing_root"
    monkeypatch.setattr(config, 'S3_ROOT_LANDING', f"{root}/")
    monkeypatch.setattr(utility, 'S3_ROOT_LANDING', f"{root}/")

    # Volume doesn't exist - should print message
    utility.create_volume_if_not_exists('cat', 'sch', 'landing', spark)
    out = capsys.readouterr().out
    assert "Volume did not exist and was created" in out

    # Mark volume as existing and call again - no message expected
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
    monkeypatch.setattr(utility, 'S3_ROOT_LANDING', root_no_slash)
    utility.create_volume_if_not_exists('cat', 'sch', 'landing', spark)
    assert Path(f"{root_no_slash}/cat/sch/landing").is_dir()

