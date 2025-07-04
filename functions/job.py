import json
import os
from pathlib import Path

import requests


def save_job_configuration(dbutils, path="/Volumes/edsm/bronze/utility/jobs"):
    """Retrieve the current job's configuration as JSON and save it.

    Parameters
    ----------
    dbutils : Databricks dbutils
        Utility object used to fetch context information.
    path : str or Path, optional
        Destination directory for the saved JSON. Defaults to
        ``/Volumes/edsm/bronze/utility/jobs``.
    """
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    job_id = ctx.jobId().getOrElse(None)
    if job_id is None:
        raise RuntimeError("Not running inside a job")

    host = os.environ.get("DATABRICKS_HOST") or ctx.apiUrl().get()
    token = os.environ.get("DATABRICKS_TOKEN") or ctx.apiToken().get()

    url = f"{host}/api/2.1/jobs/get"
    resp = requests.get(
        url, headers={"Authorization": f"Bearer {token}"}, params={"job_id": job_id}
    )
    resp.raise_for_status()

    job_config = resp.json()
    job_name = job_config.get("settings", {}).get("name", f"job-{job_id}")

    dst = Path(path) / f"{job_name}.json"
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(json.dumps(job_config, indent=4))
