# Inspecting checkpoint folders

`utilities/inspect_checkpoint_folder.py` inspects a Delta checkpoint directory for a given table.

```bash
python utilities/inspect_checkpoint_folder.py COLOR TABLE_NAME [--master MASTER_URL]
```

`COLOR` must be either `bronze` or `silver` and selects the layer of settings to inspect. `TABLE_NAME` should match one of the JSON files in the corresponding layer without the extension. Use `--master` to specify the Spark master URL.

The script applies the `job_type` defaults from the settings file, which usually
populate `writeStreamOptions.checkpointLocation`. If a checkpoint location
cannot be determined after applying the job type, a descriptive error is raised.

When inspecting a silver table the output shows which bronze version was read by
each streaming batch. When inspecting a bronze table the script lists the
directories processed in each batch based on the information stored in the
checkpoint's `sources` directory.
