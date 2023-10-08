from pyspark.sql.functions import expr, ltrim, rtrim, trim
dummy_df \
    .select("*") \
    .withColumn("ltrim", expr("ltrim(dummy)")) \
    .withColumn("rtrim", rtrim(dummy_df.dummy)) \
    .withColumn("trim", rtrim(dummy_df.dummy)) \
    .show()
