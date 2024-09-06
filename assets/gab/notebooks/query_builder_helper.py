# Databricks notebook source
# MAGIC %md
# MAGIC # Import Utils

# COMMAND ----------

# MAGIC %run ../utils/query_builder_utils

# COMMAND ----------

QUERY_BUILDER_UTILS = QueryBuilderUtils()

# COMMAND ----------

# MAGIC %md
# MAGIC <h1>Use Case Setup

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The Global Asset Builder (GAB) has been developed to help you automate the creations of aggregate tables for
# MAGIC dashboards on top of base fact tables. It reduce the efforts and time to production for new aggregate tables.
# MAGIC Users don't need to create separate pipeline for all such cases.
# MAGIC
# MAGIC This notebook has been developed to help users to create their use cases configurations easily.
# MAGIC
# MAGIC There is some mandatory information that must be completed for the use case to work correctly:
# MAGIC
# MAGIC **Use case name:** This parameter must not contain spaces or special characters.
# MAGIC The suggestion is to use lowercase and underlined alphanumeric characters.
# MAGIC
# MAGIC **Market:** Related to the job schedule, example: GLOBAL starting at 07AM UTC
# MAGIC It gets the complete coverage of last day for the market.
# MAGIC - GLOBAL - 07AM UTC
# MAGIC
# MAGIC **Reference date:** Reference date of the use case. The parameter should be the column name.
# MAGIC The selected column should have the date/datetime format.
# MAGIC
# MAGIC **To date:** This parameter is used in the template, by default its value must be "to_date".
# MAGIC You can change it if you have managed this in your SQL files.
# MAGIC The values stored in this column depend on the use case behavior:
# MAGIC - if snapshots are enabled, it will contain the snapshot end day.
# MAGIC - If snapshot is not enabled, it will contain the last day of the cadence.
# MAGIC The snapshot behaviour is set in the reconciliation steps.
# MAGIC
# MAGIC **How many dimensions?** An integer input of the number of dimensions (columns) expected in the use case.
# MAGIC Do not consider the reference date or metrics here, as they have their own parameters.
# MAGIC
# MAGIC **Time Offset:** The time zone offset that you want to apply to the reference date column.
# MAGIC It should be a number to decrement or add to the date (e.g., -8 or 8). The default value is zero,
# MAGIC which means that any time zone transformation will be applied to the date.
# MAGIC
# MAGIC **Week start:** The start of the business week of the use case. Two options are available SUNDAY or MONDAY.
# MAGIC
# MAGIC **Is Active:** Flag to make the use case active or not. Default value is "Y".
# MAGIC
# MAGIC **How many views?** Defines how many consumption views you want to have for the use case.
# MAGIC You can have as many as you want. However, they will have exactly the same structure
# MAGIC (metrics, columns, timelines, etc.), the only change will be the filter applied to them.
# MAGIC The default value is 1.
# MAGIC
# MAGIC **Complexity:** Defines the complexity of your use case. You should mainly consider the volume of data.
# MAGIC This parameter directly affects the number of workers that will be spin up to execute the use case.
# MAGIC - High
# MAGIC - Medium
# MAGIC - Low
# MAGIC
# MAGIC **SQL File Names:** Name of the SQL files used in the use case.
# MAGIC You can combine different layers of dependencies between them as shown in the example,
# MAGIC where the "2_combined.sql" file depends on "1_product_category.sql" file.
# MAGIC The file name should follow the pattern x_file_name (where x is an integer digit) and be separated by a comma
# MAGIC (e.g.: 1_first_query.sql, 2_second_query.sql).
# MAGIC
# MAGIC **DEV - Database Schema Name** Refers to the name of the development environment database where the
# MAGIC "lkp_query_builder" table resides. This parameter is used at the end of the notebook to insert data into
# MAGIC the "lkp_query_builder" table.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text(name="usecase_name", defaultValue="", label="Use Case Name")
dbutils.widgets.dropdown(
    name="market", defaultValue="GLOBAL", label="Market", choices=["APAC", "GLOBAL", "NAM", "NIGHTLY"]
)
dbutils.widgets.text(name="from_date", defaultValue="", label="Reference Date")
dbutils.widgets.text(name="to_date", defaultValue="to_date", label="Snapshot End Date")
dbutils.widgets.text(name="num_dimensions", defaultValue="", label="How many dimensions?")
dbutils.widgets.text(name="time_offset", defaultValue="0", label="Time Offset")
dbutils.widgets.dropdown(name="week_start", defaultValue="MONDAY", label="Week start", choices=["SUNDAY", "MONDAY"])
dbutils.widgets.dropdown(name="is_active", defaultValue="Y", label="Is Active", choices=["Y", "N"])
dbutils.widgets.text(name="num_of_views", defaultValue="1", label="How many views?")
dbutils.widgets.dropdown(
    name="complexity", defaultValue="Medium", label="Complexity", choices=["Low", "Medium", "High"]
)
dbutils.widgets.text(name="sql_files", defaultValue="", label="SQL File Names")
dbutils.widgets.text(name="db_schema", defaultValue="", label="DEV - Database Schema Name")

# COMMAND ----------

# MAGIC %md
# MAGIC Set configurations and validate.

# COMMAND ----------

usecase_name = dbutils.widgets.get("usecase_name").lower().strip()
market = dbutils.widgets.get("market")
from_date = dbutils.widgets.get("from_date")
to_date = dbutils.widgets.get("to_date")
num_dimensions = dbutils.widgets.get("num_dimensions")
time_offset = dbutils.widgets.get("time_offset")
week_start = dbutils.widgets.get("week_start")
is_active = dbutils.widgets.get("is_active")
num_of_views = dbutils.widgets.get("num_of_views")
complexity = dbutils.widgets.get("complexity")
sql_files = dbutils.widgets.get("sql_files").replace(".sql", "")
db_schema = dbutils.widgets.get("db_schema")
num_of_metrics = ""

QUERY_BUILDER_UTILS.check_config_inputs(
    usecase_name, from_date, num_dimensions, sql_files, num_of_views, to_date, time_offset, db_schema
)

# COMMAND ----------

# MAGIC %md
# MAGIC Set Dimensions.
# MAGIC
# MAGIC In this step you will have to map the dimension columns with their respective order.
# MAGIC The options available in the widgets to fill are based on the number of dimensions previously defined.
# MAGIC For example, if you have two dimensions to analyze, such as country and category,
# MAGIC values must be set to D1 and D2.
# MAGIC For example:
# MAGIC D1. Dimension name = country
# MAGIC D2. Dimension name = category

# COMMAND ----------

QUERY_BUILDER_UTILS.set_dimensions(num_dimensions)

# COMMAND ----------

dimensions = QUERY_BUILDER_UTILS.get_dimensions(num_dimensions)

# COMMAND ----------

QUERY_BUILDER_UTILS.print_definitions(
    usecase_name=usecase_name,
    market=market,
    from_date=from_date,
    to_date=to_date,
    dimensions=dimensions,
    time_offset=time_offset,
    week_start=week_start,
    is_active=is_active,
    num_of_views=num_of_views,
    complexity=complexity,
    sql_files=sql_files,
    db_schema=db_schema,
)

# COMMAND ----------

# MAGIC %md
# MAGIC <h1> 1 - Configure view(s) name(s) and filter(s)

# COMMAND ----------

# MAGIC %md
# MAGIC The filters defined in this step will be based on the dimensions defined in the previous step.
# MAGIC
# MAGIC So, if you have set the country as D1, the filter here should be D1 = "Germany".
# MAGIC The commands allowed for the filter step are the same as those used in the where clause in SQL language.

# COMMAND ----------

QUERY_BUILDER_UTILS.set_views(num_of_views)

# COMMAND ----------

dims_dict = QUERY_BUILDER_UTILS.get_view_information(num_of_views)

# COMMAND ----------

QUERY_BUILDER_UTILS.print_definitions(
    usecase_name=usecase_name,
    market=market,
    from_date=from_date,
    to_date=to_date,
    dimensions=dimensions,
    time_offset=time_offset,
    week_start=week_start,
    is_active=is_active,
    num_of_views=num_of_views,
    complexity=complexity,
    sql_files=sql_files,
    db_schema=db_schema,
    dims_dict=dims_dict,
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2 - Configure Reconciliation

# COMMAND ----------

# MAGIC %md
# MAGIC The reconciliation configuration (recon) is mandatory.
# MAGIC In this section you will set the cadence, recon and snapshot behaviour of your use case.
# MAGIC
# MAGIC CADENCE - The cadence sets how often the data will be calculated. E.g: DAY, WEEK, MONTH, QUARTER, YEAR.
# MAGIC
# MAGIC RECON - The reconciliation for the cadence set.
# MAGIC
# MAGIC IS SNAPSHOT? - Set yes or no for the combination of cadence and reconciliation.
# MAGIC
# MAGIC Combination examples:
# MAGIC - DAILY CADENCE = DAY - This configuration means that only daily data will be refreshed.
# MAGIC - MONTHLY CADENCE - WEEKLY RECONCILIATION - WITHOUT SNAPSHOT = MONTH-WEEK-N -
# MAGIC This means after every week, the whole month data is refreshed without snapshot.
# MAGIC - WEEKLY CADENCE - DAY RECONCILIATION - WITH SNAPSHOT = WEEK-DAY-Y -
# MAGIC This means that every day, the entire week's data (week to date) is refreshed with snapshot.
# MAGIC It will generate a record for each day with the specific position of the value for the week.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.multiselect(
    name="recon_cadence",
    defaultValue="DAY",
    label="Recon Cadence",
    choices=QUERY_BUILDER_UTILS.get_recon_choices(),
)

# COMMAND ----------

recon_list = list(filter(None, dbutils.widgets.get(name="recon_cadence").split(",")))
print(f"List of chosen reconciliation values: {recon_list}")

# COMMAND ----------

recon_dict = QUERY_BUILDER_UTILS.get_recon_config(recon_list)

# COMMAND ----------

QUERY_BUILDER_UTILS.print_definitions(
    usecase_name=usecase_name,
    market=market,
    from_date=from_date,
    to_date=to_date,
    dimensions=dimensions,
    time_offset=time_offset,
    week_start=week_start,
    is_active=is_active,
    num_of_views=num_of_views,
    complexity=complexity,
    sql_files=sql_files,
    db_schema=db_schema,
    dims_dict=dims_dict,
    recon_dict=recon_dict,
)

# COMMAND ----------

# MAGIC %md
# MAGIC <h1> 3 - Configure METRICS

# COMMAND ----------

# MAGIC %md
# MAGIC Define how many metrics your SQL files contain. For example, you have a sum (amount) as total_amount
# MAGIC and a count(*) as total_records, you will need to set 2 here.
# MAGIC
# MAGIC The metrics column must be configured in the same order they appear in the sql files.
# MAGIC
# MAGIC For example:
# MAGIC 1. Metric name = total_amount
# MAGIC 2. Metric name = total_records

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text(name="num_of_metrics", defaultValue="1", label="How many metrics?")

# COMMAND ----------

num_of_metrics = dbutils.widgets.get("num_of_metrics")

QUERY_BUILDER_UTILS.set_metric(num_of_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC Based on the metric setup, it is possible to derive 4 new columns based on each metric.
# MAGIC Those new columns will be based on cadences like last_cadence, last_year_cadence and window function.
# MAGIC But also, you can create a derived column, which is a SQL statement that you can write on your own
# MAGIC by selecting the option of "derived_metric".

# COMMAND ----------

metrics_dict = QUERY_BUILDER_UTILS.get_metric_configuration(num_of_metrics)

# COMMAND ----------

QUERY_BUILDER_UTILS.set_extra_metric_config(num_of_metrics, metrics_dict)

# COMMAND ----------

QUERY_BUILDER_UTILS.print_definitions(
    usecase_name=usecase_name,
    market=market,
    from_date=from_date,
    to_date=to_date,
    dimensions=dimensions,
    time_offset=time_offset,
    week_start=week_start,
    is_active=is_active,
    num_of_views=num_of_views,
    complexity=complexity,
    sql_files=sql_files,
    db_schema=db_schema,
    dims_dict=dims_dict,
    recon_dict=recon_dict,
    metrics_dict=metrics_dict,
)

# COMMAND ----------

# MAGIC %md
# MAGIC <h1> 4 - Configure STAGES

# COMMAND ----------

# MAGIC %md
# MAGIC The parameters available for this step are:
# MAGIC
# MAGIC - Filter Date Column - This column will be used to filter the data of your use case.
# MAGIC This information will be replaced in the placeholder of the GAB template.
# MAGIC - Project Date Column - This column will be used as reference date for the query given.
# MAGIC This information will be replaced in the placeholder of the GAB template.
# MAGIC - Repartition Value - This parameter only has effect when used with Repartition Type parameter.
# MAGIC It sets the way of repartitioning the data while processing.
# MAGIC - Repartition Type - Type of repartitioning the data of the query.
# MAGIC Available values are Key and Number. When use Key, it expects column names separated by a comma.
# MAGIC When set number it expects and integer of how many partitions the user want.
# MAGIC - Storage Level - Defines the type of spark persistence storage levels you want to define
# MAGIC (e.g. Memory Only, Memory and Disk etc).
# MAGIC - Table Alias - The alias name of the sql file that will run.
# MAGIC

# COMMAND ----------

sql_files_list = QUERY_BUILDER_UTILS.set_stages(sql_files=sql_files)

# COMMAND ----------

# MAGIC %md
# MAGIC According to the number of sql files provided in the use case, a set of widgets will appear to be configured.
# MAGIC Remember that the configuration index matches the given sql file order.
# MAGIC
# MAGIC For example: 1_categories.sql, 2_fact_kpi.sql. Settings starting with index “1”.
# MAGIC will be set to sql file 1_categories.sql. The same will happen with index “2.”.

# COMMAND ----------

stages_dict = QUERY_BUILDER_UTILS.get_stages(sql_files_list, usecase_name)

# COMMAND ----------

# MAGIC %md
# MAGIC <h1> BUILD AND INSERT SQL INSTRUCTION

# COMMAND ----------

delete_sttmt, insert_sttmt = QUERY_BUILDER_UTILS.create_sql_statement(
    usecase_name,
    market,
    stages_dict,
    recon_dict,
    time_offset,
    week_start,
    is_active,
    complexity,
    db_schema,
    dims_dict,
    dimensions,
    from_date,
    to_date,
    metrics_dict,
)

print(delete_sttmt + "\n" + insert_sttmt)

# COMMAND ----------

# MAGIC %md
# MAGIC <h1> INSERT CONFIGURATION DATA
# MAGIC
# MAGIC **Note:** This insert will have effect just on dev/uat, to execute it on prod
# MAGIC it will need to use the Table/SQL Manager or another job.

# COMMAND ----------

QUERY_BUILDER_UTILS.insert_data_into_lkp_query_builder(delete_sttmt, insert_sttmt)
