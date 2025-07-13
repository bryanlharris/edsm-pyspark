# Profiling a silver table

`utilities/profile_silver_table.py` generates a data quality profile for a silver table
using `databricks-labs-dqx`.

```bash
python utilities/profile_silver_table.py TABLE_NAME [--master MASTER_URL]
```

`TABLE_NAME` is the base name of the settings file in `layer_02_silver`.
