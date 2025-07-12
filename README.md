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