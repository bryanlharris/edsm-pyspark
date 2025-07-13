## Running a pipeline locally

The `scripts` directory contains a `run_ingest.py` utility that emulates the
`03_ingest` notebook so jobs can be executed with `spark-submit` or directly
from the command line.

```bash
python scripts/run_ingest.py bronze stations
```

Specify the layer (`bronze`, `silver`, or `gold`) and the table name (without the
`.json` extension). Use the `--master` argument to override the Spark master URL
if necessary. Pass `-v` to include the full settings including the file schema.
## Running the full job

Use the `utilities/run_job.py` script to execute the downloader and all ingest tasks without relying on Databricks APIs.

```bash
python utilities/run_job.py
```

Use `--master` to override the Spark master URL if necessary. Include `-v` to
print the full settings for each table.

## Ingest all tables

`utilities/run_ingest_pipeline.py` runs `scripts/run_ingest.py` for every table
defined in the settings files. Bronze tables are ingested first followed by
silver tables.

```bash
python utilities/run_ingest_pipeline.py
```

All output from the ingest commands is written to the `logs` directory.
