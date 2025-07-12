import sys
import pathlib
import types
import importlib.util
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

# Create minimal 'functions' package to load sanity without importing other modules
pkg_path = pathlib.Path(__file__).resolve().parents[1] / 'functions'
functions_pkg = types.ModuleType('functions')
functions_pkg.__path__ = [str(pkg_path)]
sys.modules.setdefault('functions', functions_pkg)

sanity_path = pkg_path / 'sanity.py'
spec = importlib.util.spec_from_file_location('functions.sanity', sanity_path)
sanity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sanity)


def test_check_host_name_env(monkeypatch, capsys):
    monkeypatch.setenv("DATABRICKS_HOST", "https://dev.cloud.databricks.com")
    host = sanity.check_host_name()
    assert host == "dev"
    out = capsys.readouterr().out
    assert "Host name recognized as dev" in out


def test_check_host_name_not_allowed(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://foo.cloud.databricks.com")
    with pytest.raises(RuntimeError):
        sanity.check_host_name()


def test_check_host_name_no_host(monkeypatch):
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    with pytest.raises(RuntimeError):
        sanity.check_host_name()
