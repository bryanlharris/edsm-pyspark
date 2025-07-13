# Optimizing and vacuuming tables

`utilities/optimize_and_vacuum.py` iterates over all schemas in a catalog and runs `OPTIMIZE`
and `VACUUM` on every table.

```bash
python utilities/optimize_and_vacuum.py [--catalog CATALOG] [--master MASTER_URL]
```

The default catalog is `edsm` and the default master is `local[*]`.
