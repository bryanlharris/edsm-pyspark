# Running the job

`utilities/run_job.py` executes the downloader and then ingests every table
defined in the settings files. It does not rely on Databricks APIs and can be
used to run the full pipeline locally.

```bash
python utilities/run_job.py
```

Use `--master` to override the Spark master URL if required.
`--log-level` controls the Spark log level and `-v` prints the full settings for each table.

