# Resetting a catalog

`utilities/reset_catalog.py` drops all tables from a catalog and deletes utility
volumes in the mounted workspace.

```bash
python utilities/reset_catalog.py CATALOG [--volume-root PATH] [--master MASTER_URL]
```
