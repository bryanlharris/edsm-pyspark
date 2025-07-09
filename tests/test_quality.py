import sys
import types
import pathlib
import importlib.util
import unittest
import unittest.mock

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
        self.opts = {}
    def format(self, fmt):
        return self
    def queryName(self, name):
        self.name = name
        return self
    def trigger(self, **kwargs):
        return self
    def option(self, *args, **kwargs):
        if len(args) == 2 and not kwargs:
            self.opts[args[0]] = args[1]
        self.opts.update(kwargs)
        return self
    def options(self, **kwargs):
        self.opts.update(kwargs)
        return self
    def start(self):
        self.spark.storage[self.name] = getattr(self.df, "rows", [])
        return DummyStreamingQuery()
    def table(self, table_name):
        self.spark.tables.setdefault(table_name, []).extend(
            getattr(self.df, "rows", [])
        )
        return DummyStreamingQuery()

class DummySpark:
    def __init__(self):
        self.created = []
        self.storage = {}
        self.tables = {}
        self.catalog = types.SimpleNamespace(
            dropTempView=self.storage.pop,
            tableExists=lambda name: name in self.tables,
        )
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
        with unittest.mock.patch.object(quality.uuid, 'uuid4') as mock_uuid, \
             unittest.mock.patch.object(quality.shutil, 'rmtree') as mock_rm:
            mock_uuid.return_value.hex = 'abcd'
            result = quality.count_records(
                df, spark, checkpoint_location='/tmp/_dqx_checkpoints/'
            )
            self.assertEqual(result, 3)
            self.assertEqual(
                spark.storage, {}
            )
            self.assertEqual(
                df.writeStream.opts.get('checkpointLocation'),
                '/tmp/_dqx_checkpoints/abcd/'
            )
        mock_rm.assert_called_with('/tmp/_dqx_checkpoints/abcd/', ignore_errors=True)

    def test_create_dqx_bad_records_table_streaming(self):
        spark = DummySpark()
        df = DummyDF(schema='schema')
        bad = DummyStreamingDF([1], spark)

        settings = {
            'dst_table_name': 'foo',
            'writeStreamOptions': {'checkpointLocation': '/tmp/base/_checkpoints/'}
        }

        spark.catalog.tableExists = lambda name: False

        with unittest.mock.patch.object(quality, 'apply_dqx_checks', return_value=(df, bad)), \
             unittest.mock.patch.object(quality.uuid, 'uuid4') as mock_uuid, \
             unittest.mock.patch.object(quality, 'count_records', return_value=1), \
             unittest.mock.patch.object(quality.shutil, 'rmtree') as mock_rm:
            mock_uuid.return_value.hex = 'abcd'
            result = quality.create_dqx_bad_records_table(df, settings, spark)
            self.assertIs(result, df)
            self.assertEqual(
                bad.writeStream.opts.get('checkpointLocation'),
                '/tmp/base/_dqx_checkpoints/abcd/'
            )
            self.assertEqual(
                spark.tables.get('foo_dqx_bad_records'),
                [1]
            )
            mock_rm.assert_called_with('/tmp/base/_dqx_checkpoints/abcd/', ignore_errors=True)

if __name__ == '__main__':
    unittest.main()
