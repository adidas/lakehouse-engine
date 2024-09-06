# Databricks notebook source
# imports
import enum
from typing import Tuple
from uuid import UUID

import requests


# COMMAND ----------

class BearerAuth:
    """Create authorisation object to be used in the requests header."""

    def __init__(self, token):
        """Create auth object with personal access token."""
        self.token = token

    def __call__(self, r):
        """Add bearer token to header.

        This function is internally called by get or post method of requests.
        """
        r.headers["authorization"] = "Bearer " + self.token
        return r


class ResultState(str, enum.Enum):
    """Possible values for result state of a job run."""

    SUCCESS = "SUCCESS"
    CANCELED = "CANCELED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class DatabricksJobs:
    """Class with methods to execute databricks jobs API commands.
        Refer documentation for details: https://docs.databricks.com/dev-tools/api/latest/jobs.html#.
    """

    # api endpoints
    RUN_NOW = "/2.1/jobs/run-now"
    GET_OUTPUT = "/2.1/jobs/runs/get-output"
    GET_JOB = "/2.1/jobs/runs/get"
    GET_LIST_JOBS = "/2.1/jobs/list"
    CANCEL_JOB = "/2.1/jobs/runs/cancel"

    headers = {"Content-type": "application/json"}

    def __init__(self, databricks_instance: str, auth: str):
        """
        Construct a databricks jobs object using databricks instance and api token.

        Parameters:
            databricks_instance: domain name of databricks deployment. Use the form <account>.cloud.databricks.com
            auth: personal access token
        """
        self.databricks_instance = databricks_instance
        self.auth = BearerAuth(auth)

    @staticmethod
    def _check_response(response):
        if response.status_code != 200:
            raise Exception(f"Response Code: {response.status_code} \n {response.content}")

    def list_jobs(self, name: str = None, limit: int = 20, offset: int = 0, expand_tasks: bool = False) -> dict:
        """
        List the databricks jobs corresponding to given `name`.

        for details refer API documentation:
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsList

        Parameters:
            name: optional, to filter jobs as per name (case-insensitive)
            limit: optional, The number of jobs to return, valid range 0 to 25.
            offset: The offset of the first job to return, relative to the most recently created job
            expand_tasks: Whether to include task and cluster details in the response.
        Returns:
            A dictionary of job ids matching the name (if provided) else returns in chunks
        """
        params = {"limit": limit, "offset": offset, "expand_tasks": expand_tasks}

        if name:
            params.update({"name": name})
        response = requests.get(
            f"https://{self.databricks_instance}/api{self.GET_LIST_JOBS}",
            params=params,
            headers=self.headers,
            auth=self.auth,
        )
        self._check_response(response)  # Raises exception if not successful
        return response.json()

    def run_now(self, job_id: int, notebook_params: dict, idempotency_token: UUID = None) -> dict:
        """
        Trigger the job specified by the job id.

        Note: currently it expects notebook tasks in a job, but can be extended for other tasks

        Parameters:
            job_id: databricks job identifier
            notebook_params: key value pairs of the parameter name and its value to be passed to the job
            idempotency_token: An optional token to guarantee the idempotency of job run requests,
                it should have at most 64 characters
        Returns:
            A dictionary consisting of run_id and number_in_job

        """
        data = {"job_id": job_id, "notebook_params": notebook_params}
        if idempotency_token:
            data.update({"idempotency_token": str(idempotency_token)})

        response = requests.post(
            f"https://{self.databricks_instance}/api{self.RUN_NOW}",
            json=data,
            headers=self.headers,
            auth=self.auth,
        )
        self._check_response(response)  # Raises exception if not successful
        return response.json()

    def get_output(self, run_id: int) -> dict:
        """
        Fetch the single job run output and metadata for a single task.

        Reference: https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsGetOutput

        Parameters:
            run_id: identifier for the job run
        Returns:
            A dictionary containing the output and metadata from task
        """
        params = {}

        if run_id:
            params.update({"run_id": run_id})
        response = requests.get(
            f"https://{self.databricks_instance}/api{self.GET_OUTPUT}",
            params=params,
            headers=self.headers,
            auth=self.auth,
        )
        self._check_response(response)  # Raises exception if not successful
        return response.json()

    def get_job(self, run_id: int) -> dict:
        """
        Retrieve the metadata of a job run identified by run_id.

        Parameters:
            run_id: identifier for the job run
        Returns:
            A dictionary containing the metadata of a job
        """
        params = {}

        if run_id:
            params.update({"run_id": run_id})
        response = requests.get(
            f"https://{self.databricks_instance}/api{self.GET_JOB}", params=params, headers=self.headers, auth=self.auth
        )
        self._check_response(response)  # Raises exception if not successful
        return response.json()

    def cancel_job(self, run_id: int) -> dict:
        """
        Cancel job specified by run_id.

        Parameters:
            run_id: job run identifier

        Returns:
            Response received from endpoint
        """
        response = requests.post(
            f"https://{self.databricks_instance}/api{self.CANCEL_JOB}",
            json={"run_id": run_id},
            headers=self.headers,
            auth=self.auth,
        )
        self._check_response(response)  # Raises exception if not successful
        return response.json()

    def trigger_job_by_name(self, job_name: str, notebook_params: dict, idempotency_token: UUID = None) -> dict:
        """
        Triggers a job as specified by the job name, if found.

        Parameters:
            job_name: name of the job
            notebook_params: key value pairs of the parameter name and its value to be passed to the job
            idempotency_token: Optional token to guarantee the idempotency of job run requests, 64 characters max
        Returns:
            A dictionary consisting of run_id and number_in_job
        """
        result = self.list_jobs(name=job_name)
        if result.get("jobs") is None:
            raise Exception(f"job with name {job_name} not found.")

        return self.run_now(int(result.get("jobs")[0].get("job_id")), notebook_params, idempotency_token)

    def get_job_status(self, run_id: int) -> Tuple[bool, dict]:
        """
        Fetch the status of the job run id.

        Parameters:
            run_id: identifier for the job run
        Returns:
            Tuple bool and dict containing whether the job run has succeeded and its state
        """
        state = self.get_job(run_id)["state"]
        result_state = state.get("result_state") or state.get("life_cycle_state")
        return result_state == ResultState.SUCCESS, state

    def job_id_extraction(self, job_name: str) -> int:
        """Extract the job id from the job run.

        Args:
            job_name: Job name.

        Returns:
            Job ID number.
        """
        jobs_list = self.list_jobs(name=job_name)
        if jobs_list.get("jobs") is None:
            raise Exception("No jobs found.")
        return int(jobs_list.get("jobs")[0].get("job_id"))
