import os
import toml
import json
import requests
from pathlib import Path
from typing import Any, Dict

class ApiHandler:

    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    cluster_name = "dbs-cls-data-engineering"

    def __init__(self, name:str) -> None:
        
        # Job name
        self.name = name.replace(" ","_")

    def cluster_id(self) -> str:
        url = f"{self.host}/api/2.0/clusters/list"
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(url, headers=headers)
        clusters = response.json()
        for c in clusters["clusters"]:
            if c["cluster_name"]==self.cluster_name:
                return c["cluster_id"]
        return ""
    
    def job_id(self) -> str:
        url = f"{self.host}/api/2.2/jobs/list"
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(url, headers=headers)
        jobs = response.json().get("jobs", [])
        job = next((j for j in jobs if j["settings"]["name"] == self.name), None)
        if job:
            return job["job_id"]
        else:
            return None
        
    def json_payload(self, sharepoint_url:str, db_table:str, mode:str, **kwargs) -> Dict[str, Any]:
    
        # Resolve path
        notebook_path ="/Workspace/Users/tailan.rg@cpflenergia.onmicrosoft.com/Apps/sharepoint-app/notebooks/ingest"
        # notebook_path = #Path(os.getcwd()).parent/"notebooks"/"ingest"

        # Cluster ID
        cluster_id = self.cluster_id()

        # Job name
        payload = dict(name=self.name)

        # Schedule
        payload.update(dict(
            schedule = dict(
                quartz_cron_expression = kwargs.get("quartz_cron_expression", "0 0 6 * * ?"),
                timezone_id = "America/Sao_Paulo",
                pause_status = "UNPAUSED",
            )
        ))

        # Max Concurrent Runs
        payload.update(dict(max_concurrent_runs=1))

        # Set task
        payload.update(dict(
                task_key = db_table,
                notebook_task = dict(
                    notebook_path = notebook_path,
                    base_parameters = dict(
                        db_table = db_table,
                        sharepoint_url = sharepoint_url,
                        mode = mode,
                    ),
                ),
                depends_on = [],
                existing_cluster_id=cluster_id,
                max_retries = kwargs.get("task_retries", 3),
                min_retry_interval_millis = kwargs.get("min_retry_interval_millis", int(300*1e3)),
                retry_on_timeout = kwargs.get("retry_on_timeout", True)
            ))
        
        # Insert tags
        payload.update(
            tags = {"SharePoint APP": ""}
        )

        return payload
    
    def create_job(self, sharepoint_url:str, db_table:str, mode:str, **kwargs) -> str:
        url = f"{self.host}/api/2.2/jobs/"
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        job_id = self.job_id()
        if job_id is not None:
            payload = dict(
                    job_id = job_id,
                    new_settings = self.json_payload(sharepoint_url=sharepoint_url, db_table=db_table, mode=mode, **kwargs)
                )
            response = requests.post(url+"reset", headers=headers, data=json.dumps(payload))
        else:
            payload = self.json_payload(sharepoint_url=sharepoint_url, db_table=db_table, mode=mode, **kwargs)
            response = requests.post(url+"create", headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        if response.status_code == 200:
            job_id = self.job_id()
            return True, f"Job criado/atualizado: {self.host}jobs/{job_id}"
        else:
            return False, f"Problema ao criar/atualizar o job: {response.text}"

if __name__=="__main__":

    quartz_cron_expression = "0 0 6 * * ?"
    name = "TesteAPI"
    db_table = "silver.sap_ccs.teste_tgarcia"
    sharepoint_url = "https://cpflenergia.sharepoint.com/:x:/s/CARE-Subtransmisso/IQDVfoYXx8EaQLuQ5we_LsqcAdCHMh9sP1rYVPTBrUteAHE?e=DfREDc"
    mode = "overwrite"
    kwargs = {}

    scheduler = ApiHandler(db_table)
    success, _ = scheduler.create_job(
        sharepoint_url=sharepoint_url, 
        db_table=db_table, 
        mode=mode, 
        quartz_cron_expression=quartz_cron_expression
    )
    print(_)

