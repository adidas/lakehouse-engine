# Databricks notebook source
import os

NOTEBOOK_CONTEXT = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

# Import the required libraries
import datetime
import json
import time
import uuid
import ast

from pyspark.sql.functions import col, lit, upper

# COMMAND ----------

# MAGIC %run ../utils/databricks_job_utils

# COMMAND ----------

AUTH_TOKEN = NOTEBOOK_CONTEXT.apiToken().getOrElse(None)

HOST_NAME = spark.conf.get("spark.databricks.workspaceUrl")

DATABRICKS_JOB_UTILS = DatabricksJobs(databricks_instance=HOST_NAME, auth=AUTH_TOKEN)

# COMMAND ----------

dbutils.widgets.text("gab_job_schedule", "{'hour': {07: 'GLOBAL'}}")
gab_job_schedule = ast.literal_eval(dbutils.widgets.get("gab_job_schedule"))

dbutils.widgets.text("source_database", "")
source_database = dbutils.widgets.get("source_database")

dbutils.widgets.text("target_database", "")
target_database = dbutils.widgets.get("target_database")

dbutils.widgets.text("gab_base_path", "")
gab_base_path = dbutils.widgets.get("gab_base_path")

dbutils.widgets.text("gab_max_jobs_limit_high_job", "")
gab_max_jobs_limit_high_job = dbutils.widgets.get("gab_max_jobs_limit_high_job")

dbutils.widgets.text("gab_max_jobs_limit_medium_job", "")
gab_max_jobs_limit_medium_job = dbutils.widgets.get("gab_max_jobs_limit_medium_job")

dbutils.widgets.text("gab_max_jobs_limit_low_job", "")
gab_max_jobs_limit_low_job = dbutils.widgets.get("gab_max_jobs_limit_low_job")


# COMMAND ----------

# functions


def divide_chunks(input_list: list, max_number_of_jobs: int) -> list:
    """Split list into predefined chunks, accordingly to the number of jobs.

        This function reads the maximum job limit defined by the parameter for each queue type in order to determine
            the number of parallel runs for each queue and divides the use cases into chunks for each run.
        For example, if the maximum job limit is set to 30 for the high queue and there are 60 use cases for the
            high queue, then each run will handle 2 use cases.
    Args:
        input_list: Input list to be split.
        max_number_of_jobs: Max job number.

    Returns:
        Split chunk list.
    """
    avg_chunk_size = len(input_list) // max_number_of_jobs
    remainder = len(input_list) % max_number_of_jobs

    chunks = [
        input_list[i * avg_chunk_size + min(i, remainder) : (i + 1) * avg_chunk_size + min(i + 1, remainder)]
        for i in range(max_number_of_jobs)
    ]
    chunks = list(filter(None, chunks))
    return chunks


def get_run_regions(job_schedule: dict, job_info: dict) -> list:
    """Get run regions accordingly to job_manager trigger time.

    Args:
        job_schedule: Markets schedule list from the parameter `gab_job_schedule`.
        job_info: Job manager info to match.

    Returns:
        Markets run list.
    """
    q_type_match = ""
    for keys in job_schedule["hour"].keys():
        if keys == int(datetime.datetime.fromtimestamp(job_info["start_time"] / 1000).strftime("%H")):
            q_type_match = job_schedule["hour"][keys]
    try:
        print("Matched regions are: ", q_type_match)
        return list(q_type_match.split(","))
    except Exception:
        raise Exception("None of the query types are configured to be run at this time")


# COMMAND ----------


context_json = json.loads(NOTEBOOK_CONTEXT.safeToJson())

run_id = ""
if context_json.get("attributes") and context_json["attributes"].get("rootRunId"):
    run_id = context_json["attributes"]["rootRunId"]
print(f"Job Run Id: {run_id}")

job_status = DATABRICKS_JOB_UTILS.get_job(run_id)
print("Job Status: ", job_status)

# COMMAND ----------

list_q_type_match = get_run_regions(gab_job_schedule, job_status)

job_queues = {
    "High": {"queue": "gab_high_queue", "max_jobs": gab_max_jobs_limit_high_job},
    "Medium": {
        "queue": "gab_medium_queue",
        "max_jobs": gab_max_jobs_limit_medium_job,
    },
    "Low": {"queue": "gab_low_queue", "max_jobs": gab_max_jobs_limit_low_job},
}

df = spark.read.table(f"{target_database}.lkp_query_builder")

for queue_type, queue_config in job_queues.items():
    lst = (
        df.filter(upper(col("queue")) == lit(queue_type.upper()))
        .filter(col("query_type").isin(list_q_type_match))
        .select(col("query_label"))
        .collect()
    )
    query_list = [job_queues[0] for job_queues in lst]

    chunk = divide_chunks(query_list, int(queue_config["max_jobs"]))
    chunk = [i for i in chunk if i]

    if chunk:
        for i in range(0, len(chunk)):
            chunk_split = ",".join(chunk[i])
            print(chunk_split)
            time.sleep(2)

            idempotency_token = uuid.uuid4()
            print(idempotency_token)

            result = DATABRICKS_JOB_UTILS.run_now(
                DATABRICKS_JOB_UTILS.job_id_extraction(queue_config["queue"]),
                {
                    "query_label_filter": chunk_split,
                    "start_date": "",
                    "look_back": "",
                    "end_date": "",
                    "cadence_filter": "All",
                    "queue_filter": queue_type,
                    "rerun_flag": "N",
                    "target_database": target_database,
                    "source_database": source_database,
                    "gab_base_path": gab_base_path,
                },
                idempotency_token=idempotency_token,
            )
            print(f"{result}\n")
