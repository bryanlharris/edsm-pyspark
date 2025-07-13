import sys
import pathlib
import importlib.util
from tests import utils


sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

# Provide minimal pyspark stubs required by utility
utils.install_fake_pyspark(type_names=["StructType"])

# Create a minimal 'functions' package so relative imports work
pkg_path = utils.install_functions_package()

# Load utility dynamically
utility_path = pkg_path / 'utility.py'
utility = utils.load_module('functions.utility', utility_path)


def test_path_glob_appended_to_load():
    settings = {
        'simple_settings': 'true',
        'job_type': 'bronze_standard_streaming',
        'dst_table_name': 'cat.bronze.tbl',
        'readStreamOptions': {
            'cloudFiles.format': 'json',
            'pathGlobFilter': 'stations.json'
        },
        'file_schema': []
    }
    result = utility.apply_job_type(settings)
    assert result['readStream_load'].endswith('/**/stations.json')
    assert 'pathGlobFilter' not in result['readStreamOptions']
