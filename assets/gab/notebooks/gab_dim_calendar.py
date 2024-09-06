# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook holds the calendar used as part of the GAB framework.

# COMMAND ----------

# Import the required libraries
from datetime import datetime, timedelta

from pyspark.sql.functions import to_date
from pyspark.sql.types import StringType

# COMMAND ----------

DIM_CALENDAR_LOCATION = "s3://my-data-product-bucket/dim_calendar"

# COMMAND ----------

initial_date = datetime.strptime("1990-01-01", "%Y-%m-%d")

dates_list = [datetime.strftime(initial_date, "%Y-%m-%d")]

for _ in range(1, 200000):
    initial_date = initial_date + timedelta(days=1)
    next_date = datetime.strftime(initial_date, "%Y-%m-%d")
    dates_list.append(next_date)

# COMMAND ----------

df_date_completed = spark.createDataFrame(dates_list, StringType())
df_date_completed = df_date_completed.withColumn("calendar_date", to_date(df_date_completed.value, "yyyy-MM-dd")).drop(
    df_date_completed.value
)
df_date_completed.createOrReplaceTempView("dates_completed")

# COMMAND ----------

df_cal = spark.sql(
    """
    WITH monday_calendar AS (
        SELECT
            calendar_date,
            WEEKOFYEAR(calendar_date) AS weeknum_mon,
            DATE_FORMAT(calendar_date, 'E') AS day_en,
            MIN(calendar_date) OVER (PARTITION BY CONCAT(DATE_PART('YEAROFWEEK', calendar_date),
            WEEKOFYEAR(calendar_date)) ORDER BY calendar_date) AS weekstart_mon
        FROM dates_completed
        ORDER BY
            calendar_date
    ),
    monday_calendar_plus_week_num_sunday AS (
        SELECT
            monday_calendar.*,
            LEAD(weeknum_mon) OVER(ORDER BY calendar_date) AS weeknum_sun
        FROM monday_calendar
    ),
    calendar_complementary_values AS (
        SELECT
            calendar_date,
            weeknum_mon,
            day_en,
            weekstart_mon,
            weekstart_mon+6 AS weekend_mon,
            LEAD(weekstart_mon-1) OVER(ORDER BY calendar_date) AS weekstart_sun,
            DATE(DATE_TRUNC('MONTH', calendar_date)) AS month_start,
            DATE(DATE_TRUNC('QUARTER', calendar_date)) AS quarter_start,
            DATE(DATE_TRUNC('YEAR', calendar_date)) AS year_start
        FROM monday_calendar_plus_week_num_sunday
    )
    SELECT
        calendar_date,
        day_en,
        weeknum_mon,
        weekstart_mon,
        weekend_mon,
        weekstart_sun,
        weekstart_sun+6 AS weekend_sun,
        month_start,
        add_months(month_start, 1)-1 AS month_end,
        quarter_start,
        ADD_MONTHS(quarter_start, 3)-1 AS quarter_end,
        year_start,
        ADD_MONTHS(year_start, 12)-1 AS year_end
    FROM calendar_complementary_values
    """
)
df_cal.createOrReplaceTempView("df_cal")

# COMMAND ----------

df_cal.write.format("delta").mode("overwrite").save(DIM_CALENDAR_LOCATION)
