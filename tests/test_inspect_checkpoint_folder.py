import json
import pathlib
import sys
from tests import utils
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

pkg_path = utils.install_functions_package()
utility = utils.load_module('functions.utility', pkg_path / 'utility.py')


def test_inspect_checkpoint_folder_lists_all_batches(tmp_path, capsys):
    root = tmp_path / '_checkpoints'
    src0 = root / 'sources' / '0'
    src1 = root / 'sources' / '1'
    src0.mkdir(parents=True)
    src1.mkdir(parents=True)

    (src0 / '0').write_text('v1\n' + json.dumps({
        'path': 'file:///data/20250712/powerPlay.json',
        'batchId': 0
    }))
    (src1 / '1').write_text('v1\n' + json.dumps({
        'path': 'file:///data/20250713/powerPlay.json',
        'batchId': 1
    }))

    settings = {'writeStreamOptions': {'checkpointLocation': str(root)}}
    utility.inspect_checkpoint_folder('powerPlay', settings, spark=None)
    out = capsys.readouterr().out
    assert 'Bronze Batch 0' in out
    assert 'Bronze Batch 1' in out
