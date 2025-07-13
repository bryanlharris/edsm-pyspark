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

If you override ``PYSPARK_DRIVER_HOST`` to bind the driver to a specific
interface, ``foreachBatch`` callbacks may fail to start unless PySpark is
allowed to use a non-local port. Set ``PYSPARK_ALLOW_INSECURE_PORT=1`` before
creating the session. The snippet below determines the machine's active IP
address automatically rather than hard-coding one:

```python
import os
import socket

def get_ip() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except OSError:
        return socket.gethostbyname(socket.gethostname())

os.environ["PYSPARK_DRIVER_HOST"] = get_ip()
os.environ["PYSPARK_ALLOW_INSECURE_PORT"] = "1"
```
## Running the full job

Use the `utilities/run_job.py` script to execute the downloader and all ingest tasks without relying on Databricks APIs.

```bash
python utilities/run_job.py
```

Use `--master` to override the Spark master URL if necessary. Include `-v` to
print the full settings for each table.

## Ingest all tables

`scripts/run_pipeline.py` runs `scripts/run_ingest.py` for every table defined
in the settings files. Before ingestion starts it validates the settings files
and creates any missing empty tables based on the JSON definitions. Bronze
tables are ingested first followed by silver tables.

```bash
python scripts/run_pipeline.py
```

Add `--color bronze` or `--color silver` to ingest only one layer. All output from the ingest commands is written to the `logs` directory.
