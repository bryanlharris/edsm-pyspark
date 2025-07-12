# EDSM

This repository contains notebooks and utilities for ingesting data from the Elite Dangerous Star Map (EDSM) nightly dumps.

For documentation, see [here](https://github.com/bryanlharris/Documentation).

## Running a pipeline locally

The `scripts` directory contains a `run_ingest.py` utility that emulates the
`03_ingest` notebook so jobs can be executed with `spark-submit` or directly
from the command line.

```bash
python scripts/run_ingest.py bronze stations
```

Specify the layer (`bronze`, `silver`, or `gold`) and the table name (without the
`.json` extension). Use the `--master` argument to override the Spark master URL
if necessary.
## Running the full job

Use the `utilities/run_job.py` script to execute the downloader and all ingest tasks without relying on Databricks APIs.

```bash
python utilities/run_job.py
```

Use `--master` to override the Spark master URL if necessary.
