# Rescuing silver tables by schema

`utilities/rescue_silver_schema.py` iterates over all silver table settings and runs the rescue process for each one.

```bash
python utilities/rescue_silver_schema.py [--master MASTER_URL]
```

Tables listed in the script's `SKIP` set are ignored.
