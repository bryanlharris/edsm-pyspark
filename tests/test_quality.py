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

class DummyStreamingDF(DummyDF):
    def __init__(self, rows, spark):
        super().__init__(schema='schema')
        self.rows = rows
        self.isStreaming = True
        self.writeStream = DummyWriteStream(self, spark)

class DummyStreamingQuery:
    def awaitTermination(self):
        pass

class DummyWriteStream:
    def __init__(self, df, spark):
        self.df = df
        self.spark = spark
        self.name = None
    def format(self, fmt):
        return self
    def queryName(self, name):
        self.name = name
        return self
    def trigger(self, **kwargs):
        return self
    def start(self):
        self.spark.storage[self.name] = getattr(self.df, "rows", [])
        return DummyStreamingQuery()

class DummySpark:
    def __init__(self):
        self.created = []
        self.storage = {}
        self.catalog = types.SimpleNamespace(dropTempView=self.storage.pop)
    def createDataFrame(self, data, schema):
        df = DummyDF(schema)
        self.created.append((data, schema))
        return df
    def sql(self, query):
        if query.startswith("SELECT COUNT(*) FROM"):
            name = query.split()[-1]
            count = len(self.storage.get(name, []))
            return types.SimpleNamespace(collect=lambda: [[count]])
        raise NotImplementedError

class QualityTests(unittest.TestCase):
    def test_returns_input_when_no_checks(self):
        df = DummyDF(schema='schema')
        spark = DummySpark()
        good, bad = quality.apply_dqx_checks(df, {}, spark)
        self.assertIs(good, df)
        self.assertEqual(bad.schema, 'schema')
        self.assertEqual(len(spark.created), 1)
        self.assertEqual(spark.created[0][0], [])

    def test_count_records_batch(self):
        class CountDF(DummyDF):
            def count(self_inner):
                return 7

        spark = DummySpark()
        df = CountDF()
        result = quality.count_records(df, spark)
        self.assertEqual(result, 7)

    def test_count_records_streaming(self):
        spark = DummySpark()
        df = DummyStreamingDF([1, 2, 3], spark)
        result = quality.count_records(df, spark)
        self.assertEqual(result, 3)
        self.assertEqual(spark.storage, {})

if __name__ == '__main__':
    unittest.main()
