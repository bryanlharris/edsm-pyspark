import sys
import pathlib
import importlib.util
from tests import utils


sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

# Provide minimal pyspark stubs required by utility
utils.install_fake_pyspark(function_names=["col", "row_number"], type_names=["StructType"])
import types
sys.modules["pyspark.sql.window"] = types.ModuleType("pyspark.sql.window")
setattr(sys.modules["pyspark.sql.window"], "Window", type("Window", (), {}))
StructType = sys.modules["pyspark.sql.types"].StructType
setattr(StructType, "fromJson", classmethod(lambda cls, data: cls()))

# Create a minimal 'functions' package so relative imports work
pkg_path = utils.install_functions_package()

# Load utility dynamically
utility_path = pkg_path / 'utility.py'
utility = utils.load_module('functions.utility', utility_path)
read_path = pkg_path / 'read.py'
read = utils.load_module('functions.read', read_path)


def test_path_glob_appended_to_load():
    settings = {
        'simple_settings': 'true',
        'job_type': 'bronze_standard_streaming',
        'dst_table_name': 'cat.bronze.tbl',
        'file_format': 'json',
        'readStreamOptions': {
            'pathGlobFilter': 'stations.json'
        },
        'file_schema': []
    }
    result = utility.apply_job_type(settings)
    assert result['readStream_load'].endswith('/**/stations.json')
    assert 'pathGlobFilter' not in result['readStreamOptions']
    assert result['readStreamOptions'].get('recursiveFileLookup') == 'true'


class DummyReader:
    def __init__(self):
        self.calls = []
        self.opts = {}

    def format(self, fmt):
        self.calls.append(('format', fmt))
        return self

    def options(self, **opts):
        self.calls.append(('options', opts))
        self.opts.update(opts)
        return self

    def schema(self, schema):
        self.calls.append(('schema', schema))
        return self

    def load(self, path):
        self.calls.append(('load', path))
        return self


class DummySpark:
    def __init__(self):
        self.readStream = DummyReader()


def test_stream_read_files_recursive_option():
    spark = DummySpark()
    settings = {
        'readStreamOptions': {
            'format': 'json',
            'recursiveFileLookup': 'true',
        },
        'readStream_load': 'landing/data/**/stations.json',
        'file_schema': [],
    }
    read.stream_read_files(settings, spark)
    assert spark.readStream.opts.get('recursiveFileLookup') == 'true'
