import sys
import types
import pathlib
import importlib.util

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

# Provide minimal pyspark stubs required by utility
pyspark = types.ModuleType('pyspark')
sql = types.ModuleType('pyspark.sql')
func_mod = types.ModuleType('pyspark.sql.functions')
types_mod = types.ModuleType('pyspark.sql.types')
setattr(types_mod, 'StructType', type('StructType', (), {}))
sql.types = types_mod
sql.functions = func_mod
pyspark.sql = sql
sys.modules['pyspark'] = pyspark
sys.modules['pyspark.sql'] = sql
sys.modules['pyspark.sql.types'] = types_mod
sys.modules['pyspark.sql.functions'] = func_mod

# Create a minimal 'functions' package so relative imports work
pkg_path = pathlib.Path(__file__).resolve().parents[1] / 'functions'
functions_pkg = types.ModuleType('functions')
functions_pkg.__path__ = [str(pkg_path)]
sys.modules.setdefault('functions', functions_pkg)

# Load utility dynamically
utility_path = pkg_path / 'utility.py'
spec = importlib.util.spec_from_file_location('functions.utility', utility_path)
utility = importlib.util.module_from_spec(spec)
spec.loader.exec_module(utility)


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
