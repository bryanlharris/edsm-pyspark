# EDSM Delta Pipeline

This repository ingests [EDSM](https://www.edsm.net) dumps into Delta tables.  
The notebooks in this project were originally used to run each step, but the
same logic can be executed from a regular Python script.

## Ingesting a table

Use `scripts/ingest.py` to run the read/transform/write pipeline for a single
settings file.  The script expects the layer colour and table name so it can
locate `layer_*_<color>/<table>.json`.

```bash
spark-submit scripts/ingest.py --color bronze --table stations
```

The script loads the JSON settings, applies `apply_job_type` to expand any
`simple_settings` values and then runs the pipeline defined by the functions
listed in the file. If the colour is `bronze` the script will also check for bad
records using `create_bad_records_table`.

## Migrating from the Notebook

Running `scripts/ingest.py` replaces executing the `03_ingest.ipynb` notebook.
Any workflow that previously passed widget values to the notebook can instead
supply the `--color` and `--table` arguments to the script.
