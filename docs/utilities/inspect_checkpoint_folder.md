# Inspecting checkpoint folders

`utilities/inspect_checkpoint_folder.py` inspects a Delta checkpoint directory for a given table.

```bash
python utilities/inspect_checkpoint_folder.py TABLE_NAME [--master MASTER_URL]
```

`TABLE_NAME` should match one of the JSON files in `layer_02_silver` without the extension.
Use `--master` to specify the Spark master URL.
