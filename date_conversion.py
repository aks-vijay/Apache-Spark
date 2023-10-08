from pyspark.sql.functions import to_date, lit, to_timestamp
dummy_df.select(
    to_date(lit("20200128"), "yyyyMMdd")) \
.show()

dummy_df.select(
    to_timestamp(lit("20200128 1725"), "yyyyMMdd HHmm")) \
.show()

from pyspark.sql.functions import date_add, date_sub

employees_dates = employees_df.select("*") \
    .withColumn("date_add", date_add(employees_df.date_joined, 10)) \
    .withColumn("date_sub", date_sub(employees_df.date_joined, 10))

from pyspark.sql.functions import datediff, months_between, current_date, round

employees_dates \
    .select(
        datediff(current_date(), employees_dates.date_joined).alias("difference"),
        round(months_between(current_date(), employees_dates.date_sub), 2).alias("months")
    ) \
    .show()

# to get beginning date of the month
from pyspark.sql.functions import trunc, date_trunc
dummy_df.select(
    trunc(current_date(), "MM").alias("Month"),  # adds date but syntax as column, format
    date_trunc("YY", current_date()).alias("Year")) \ # adds timestamp but syntax is format, column
    .show()

# to extract individual dates
from pyspark.sql.functions import day, month, year, weekofyear, dayofmonth, dayofyear
dummy_df.select(
    day(current_date()).alias("current_day"),
    month(current_date()).alias("current_month"),
    year(current_date()).alias("current_year"),
    weekofyear(current_date()).alias("week_of_year"),
    dayofmonth(current_date()).alias("day_of_month"),
    dayofyear(current_date()).alias("day_of_year")
) \
.show(truncate=False)
