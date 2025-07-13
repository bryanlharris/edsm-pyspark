import sys
import pathlib
from unittest import mock
import importlib.util
import unittest
from tests import utils

# Create minimal fake pyspark modules before importing transform
utils.install_fake_pyspark(
    function_names=[
        'concat', 'regexp_extract', 'date_format', 'current_timestamp', 'when', 'col',
        'to_timestamp', 'to_date', 'regexp_replace', 'sha2', 'lit', 'trim',
        'struct', 'to_json', 'expr', 'transform', 'array'
    ],
    type_names=[
        'StructType', 'StructField', 'StringType', 'LongType',
        'TimestampType', 'ArrayType', 'MapType'
    ]
)

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

# Import the module under test after faking pyspark
pkg_path = utils.install_functions_package()
transform_path = pkg_path / 'transform.py'
transform = utils.load_module('functions.transform', transform_path)

class DummyDF:
    def __init__(self, columns=None):
        self.columns = columns or []
        self.calls = []

    def transform(self, func, *args, **kwargs):
        self.calls.append(func.__name__)
        return self

    def withColumn(self, *args, **kwargs):
        self.calls.append('withColumn')
        return self

class BronzeTransformTests(unittest.TestCase):
    def test_skip_add_rescued_when_schema_contains_column(self):
        df = DummyDF(['foo'])
        settings = {
            'file_schema': [
                {'name': 'foo', 'type': 'string'},
                {'name': '_rescued_data', 'type': 'string'}
            ],
            'use_metadata': 'true'
        }
        with mock.patch.object(transform, 'clean_column_names', new=lambda df_in: df_in), \
             mock.patch.object(transform, 'add_source_metadata', new=lambda df_in, settings: df_in), \
             mock.patch.object(transform, 'add_rescued_data', new=mock.Mock(side_effect=lambda df_in: df_in)) as add_mock:
            transform.bronze_standard_transform(df, settings, spark=None)
            add_mock.assert_not_called()


if __name__ == '__main__':
    unittest.main()
