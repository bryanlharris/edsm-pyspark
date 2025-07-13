# Ingest all tables

`scripts/run_pipeline.py` invokes `scripts/run_ingest.py` for every table defined in the settings files. Before any ingest jobs run it validates the JSON settings and initialises empty tables if needed. It then ingests all bronze tables followed by all silver tables. Use `--color bronze` or `--color silver` to restrict ingestion to a single layer.

```bash
python scripts/run_pipeline.py
```

The stdout and stderr from each ingest run are written to files in the `logs` directory. Use `--master` to override the Spark master URL, `--log-level` to adjust Spark logging, and `-v` to pass the verbose flag to `run_ingest.py`.
