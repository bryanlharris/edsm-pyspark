import sys
import pathlib
import types
import importlib.util
import json
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

pkg_path = pathlib.Path(__file__).resolve().parents[1] / 'functions'
functions_pkg = types.ModuleType('functions')
functions_pkg.__path__ = [str(pkg_path)]
sys.modules.setdefault('functions', functions_pkg)

# Load modules dynamically
for name in ['sanity', 'config']:
    path = pkg_path / f'{name}.py'
    spec = importlib.util.spec_from_file_location(f'functions.{name}', path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    setattr(functions_pkg, name, mod)

sanity = functions_pkg.sanity
config = functions_pkg.config

def make_settings(tmp_path, catalog):
    d = tmp_path / 'layer_01_bronze'
    d.mkdir()
    path = d / 'tbl.json'
    data = {
        'dst_table_name': f'{catalog}.bronze.tbl',
        'read_function': 'f',
        'transform_function': 'f',
        'write_function': 'f',
        'file_schema': []
    }
    with open(path, 'w') as f:
        json.dump(data, f)
    return tmp_path


def test_catalog_matches(monkeypatch, tmp_path):
    root = make_settings(tmp_path, 'dev')
    monkeypatch.setattr(config, 'PROJECT_ROOT', root)
    monkeypatch.setattr(sanity, 'PROJECT_ROOT', root)
    monkeypatch.setenv('DATABRICKS_HOST', 'https://dev.cloud.databricks.com')
    sanity.check_host_name_matches_catalog()


def test_catalog_mismatch(monkeypatch, tmp_path):
    root = make_settings(tmp_path, 'prod')
    monkeypatch.setattr(config, 'PROJECT_ROOT', root)
    monkeypatch.setattr(sanity, 'PROJECT_ROOT', root)
    monkeypatch.setenv('DATABRICKS_HOST', 'https://dev.cloud.databricks.com')
    with pytest.raises(RuntimeError):
        sanity.check_host_name_matches_catalog()


def test_exempt_host(monkeypatch, tmp_path):
    root = make_settings(tmp_path, 'foo')
    monkeypatch.setattr(config, 'PROJECT_ROOT', root)
    monkeypatch.setattr(sanity, 'PROJECT_ROOT', root)
    monkeypatch.setenv('DATABRICKS_HOST', 'https://dbc-bde2b6e3-4903.cloud.databricks.com')
    sanity.check_host_name_matches_catalog()
