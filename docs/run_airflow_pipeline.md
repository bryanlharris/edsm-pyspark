# Running the EDSM pipeline with Airflow

`airflow_dags/edsm_pipeline.py` defines an Airflow DAG that downloads the raw EDSM data and ingests each table. The DAG is unscheduled by default (`schedule_interval=None`) so it must be triggered manually.

1. Install Apache Airflow and clone this repository on the machine that will run the scheduler.
2. Add the project root to your `PYTHONPATH` or run Airflow from this directory so that the modules and scripts can be imported correctly.
3. Copy `airflow_dags/edsm_pipeline.py` to your Airflow DAGs directory (or create a symlink).
4. Start the Airflow scheduler and webserver:

```bash
airflow scheduler &
airflow webserver &
```

5. Open the Airflow UI and trigger the `edsm_pipeline` DAG.
   It will execute the downloader, initialise the schemas and then ingest the bronze and silver tables.

Monitor task logs through the Airflow interface to track progress.
