# import requests

# def get_previous_task_lookup(job_id, dbutils):
#     token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
#     host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
#     url = f"{host}/api/2.1/jobs/get?job_id={job_id}"
#     headers = {"Authorization": f"Bearer {token}"}
#     response = requests.get(url, headers=headers)
#     tasks = response.json()["settings"]["tasks"]
#     return {
#         task["task_key"]: task["depends_on"][0]["task_key"]
#         for task in tasks
#         if "depends_on" in task and task["depends_on"]
#     }
