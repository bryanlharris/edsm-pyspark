# EDSM

This repository contains notebooks and utilities for ingesting data from the Elite Dangerous Star Map (EDSM).

For documentation, see [here](https://github.com/bryanlharris/Documentation).

## Data Quality Checks

The `functions.quality` module provides an optional helper to run
Databricks [DQX](https://pypi.org/project/databricks-labs-dqx/) checks
as part of a pipeline.  Use `apply_dqx_checks(df, settings, spark)` to
validate a DataFrame using checks defined inline in the job settings.
The standard ingest notebook executes these checks between the transform
and write steps and stops the job when any rows fail validation.

When counting rows from a streaming DataFrame, first derive a sibling
`_dqx_checkpoints` directory next to the job's checkpoint location:

```python
base = checkpoint.rstrip('/')
parent = os.path.dirname(base)
chkpt = f"{parent}/_dqx_checkpoints/"
count_records(df, spark, checkpoint_location=chkpt)
```

The helper appends a unique run ID inside this directory and removes the
folder after counting.  You can also set the Spark configuration
`spark.dqx.checkpointLocation` instead of passing the argument.
