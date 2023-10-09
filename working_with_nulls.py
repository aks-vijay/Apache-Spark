#dealing with nulls
from pyspark.sql.functions import coalesce, col, lit
employees_df \
    .withColumn("bonus", coalesce(col("bonus"), lit(0))) \
    .show()

# cast the empty values as NULL
#dealing with nulls
from pyspark.sql.functions import coalesce, col, lit
employees_df \
    .withColumn("bonus_renamed", col("bonus").cast("int")) \
    .show()

# with nvl, nullif in expr
from pyspark.sql.functions import expr
employees_df \
    .withColumn("bonus", expr("nvl(nullif(bonus, ''),0)")) \
    .show()

# filling null values with fillna
# the below won't work because it will only with same datatypes
employees_df.fillna(0).show()

# this will work
employees_df.fillna("0").show()

# filling individual columns
employees_df.fillna("0", "bonus").show()
