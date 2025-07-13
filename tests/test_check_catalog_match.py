import sys
import pathlib
import types
import importlib.util
import json
import pytest
from tests import utils

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

pkg_path = utils.install_functions_package()
functions_pkg = sys.modules['functions']

# Load modules dynamically
for name in ['sanity', 'config']:
    path = pkg_path / f'{name}.py'
    mod = utils.load_module(f'functions.{name}', path)
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
