# Ingest all tables

`scripts/run_pipeline.py` invokes `scripts/run_ingest.py` for every table defined in the settings files. It first ingests all bronze tables and then all silver tables.

```bash
python scripts/run_pipeline.py
```

The stdout and stderr from each ingest run are written to files in the `logs` directory. Use `--master` to override the Spark master URL and `-v` to pass the verbose flag to `run_ingest.py`.
