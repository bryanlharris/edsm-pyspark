import sys
import types
import pathlib
import importlib.util
import unittest

# Insert repo root into path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

# Import quality module dynamically
quality_path = pathlib.Path(__file__).resolve().parents[1] / 'functions' / 'quality.py'
spec = importlib.util.spec_from_file_location('functions.quality', quality_path)
quality = importlib.util.module_from_spec(spec)
spec.loader.exec_module(quality)

class DummyDF:
    def __init__(self, schema=None):
        self.schema = schema

class DummySpark:
    def __init__(self):
        self.created = []
    def createDataFrame(self, data, schema):
        df = DummyDF(schema)
        self.created.append((data, schema))
        return df

class QualityTests(unittest.TestCase):
    def test_returns_input_when_no_checks(self):
        df = DummyDF(schema='schema')
        spark = DummySpark()
        good, bad = quality.apply_dqx_checks(df, {}, spark)
        self.assertIs(good, df)
        self.assertEqual(bad.schema, 'schema')
        self.assertEqual(len(spark.created), 1)
        self.assertEqual(spark.created[0][0], [])

if __name__ == '__main__':
    unittest.main()
