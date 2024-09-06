# Databricks notebook source
from datetime import datetime, timedelta

from lakehouse_engine.engine import execute_gab
from pyspark.sql.functions import collect_list, collect_set, lit

# COMMAND ----------

dbutils.widgets.text("lookup_table", "lkp_query_builder")
lookup_table = dbutils.widgets.get("lookup_table")
dbutils.widgets.text("source_database", "source_database")
source_database = dbutils.widgets.get("source_database")
dbutils.widgets.text("target_database", "target_database")
target_database = dbutils.widgets.get("target_database")

# COMMAND ----------


def flatten_extend(list_to_flatten: list) -> list:
    """Flatten python list.

    Args:
        list_to_flatten: list to be flattened.
    Returns:
        A list containing the flatten values.
    """
    flat_list = []
    for row in list_to_flatten:
        flat_list.extend(row)
    return flat_list


lkp_query_builder_df = spark.read.table(
    "{}.{}".format(target_database, lookup_table)
)

query_label_and_queue = (
    lkp_query_builder_df.groupBy(lit(1)).agg(collect_list("query_label"), collect_set("queue")).collect()
)
query_list = flatten_extend(query_label_and_queue)[1]
queue_list = flatten_extend(query_label_and_queue)[2]

# COMMAND ----------

dbutils.widgets.text("start_date", "", label="Start Date")
dbutils.widgets.text("end_date", "", label="End Date")
dbutils.widgets.text("rerun_flag", "N", label="Re-Run Flag")
dbutils.widgets.text("look_back", "1", label="Look Back Window")
dbutils.widgets.multiselect(
    "cadence_filter",
    "All",
    ["All", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR"],
    label="Cadence",
)
dbutils.widgets.multiselect("query_label_filter", "All", query_list + ["All"], label="Use Case")
dbutils.widgets.multiselect("queue_filter", "All", queue_list + ["All"], label="Query Categorization")
dbutils.widgets.text("gab_base_path", "", label="Base Path Use Cases")
dbutils.widgets.text("target_table", "", label="Target Table")

# Input Parameters
lookback_days = "1" if dbutils.widgets.get("look_back") == "" else dbutils.widgets.get("look_back")

# COMMAND ----------

end_date_str = (
    datetime.today().strftime("%Y-%m-%d") if dbutils.widgets.get("end_date") == "" else dbutils.widgets.get("end_date")
)
end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

# As part of daily run, when no end_date is given, program always runs
# for yesterday date (Unless custom end date is given)
if dbutils.widgets.get("end_date") == "":
    end_date = end_date - timedelta(days=1)

start_date_str = (
    datetime.date(end_date - timedelta(days=int(lookback_days))).strftime("%Y-%m-%d")
    if dbutils.widgets.get("start_date") == ""
    else dbutils.widgets.get("start_date")
)
start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")
rerun_flag = dbutils.widgets.get("rerun_flag")

query_label_filter = dbutils.widgets.get("query_label_filter")
recon_filter = dbutils.widgets.get("cadence_filter")
queue_filter = dbutils.widgets.get("queue_filter")
gab_base_path = dbutils.widgets.get("gab_base_path")

# COMMAND ----------

query_label_filter = [x.strip() for x in list(set(query_label_filter.split(",")))]
queue_filter = list(set(queue_filter.split(",")))
recon_filter = list(set(recon_filter.split(",")))

if "All" in query_label_filter:
    query_label_filter = query_list

if "All" in queue_filter:
    queue_filter = queue_list

# COMMAND ----------

target_table = (
    "gab_use_case_results" if dbutils.widgets.get("target_table") == "" else dbutils.widgets.get("target_table")
)

# COMMAND ----------

print(f"Query Label: {query_label_filter}")
print(f"Queue Filter: {queue_filter}")
print(f"Cadence Filter: {recon_filter}")
print(f"Target Database: {target_database}")
print(f"Start Date: {start_date}")
print(f"End Date: {end_date}")
print(f"Look Back Days: {lookback_days}")
print(f"Re-run Flag: {rerun_flag}")
print(f"Target Table: {target_table}")
print(f"Source Database: {source_database}")
print(f"Path Use Cases: {gab_base_path}")

# COMMAND ----------

gab_acon = {
    "query_label_filter": query_label_filter,
    "queue_filter": queue_filter,
    "cadence_filter": recon_filter,
    "target_database": target_database,
    "start_date": start_date,
    "end_date": end_date,
    "rerun_flag": rerun_flag,
    "target_table": target_table,
    "source_database": source_database,
    "gab_base_path": gab_base_path,
    "lookup_table": lookup_table,
    "calendar_table": "dim_calendar",
}

# COMMAND ----------

execute_gab(acon=gab_acon)
