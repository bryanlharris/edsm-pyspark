import sys
import types
import pathlib
import importlib.util


def install_fake_pyspark(function_names=None, type_names=None):
    """Install minimal pyspark stubs into ``sys.modules`` for tests."""
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    funcs = types.ModuleType("pyspark.sql.functions")
    types_mod = types.ModuleType("pyspark.sql.types")
    sql.functions = funcs
    sql.types = types_mod
    pyspark.sql = sql
    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.sql"] = sql
    sys.modules["pyspark.sql.functions"] = funcs
    sys.modules["pyspark.sql.types"] = types_mod

    for name in type_names or []:
        setattr(types_mod, name, type(name, (), {}))

    def dummy(*args, **kwargs):
        return None

    for name in function_names or []:
        setattr(funcs, name, dummy)

    return pyspark


def install_functions_package():
    """Create a ``functions`` namespace package pointing at repo code."""
    pkg_path = pathlib.Path(__file__).resolve().parents[1] / "functions"
    functions_pkg = types.ModuleType("functions")
    functions_pkg.__path__ = [str(pkg_path)]
    sys.modules.setdefault("functions", functions_pkg)
    return pkg_path


def load_module(mod_name, path):
    """Load a module from ``path`` under the provided name."""
    spec = importlib.util.spec_from_file_location(mod_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
