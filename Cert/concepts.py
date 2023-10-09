repartition
- BOTH INCREASE/DECREASE PARTITIONS
- When shuffle happens, data is partioned which is expensive.
- If we set repartition(5), its reduced to 5 paritions which is faster
- This way we can Increase/Decrease the parition but the parallelism remains

Coalesce
- ONLY DECREASES
- It completely eliminates the partition
- Eliminates full shuffle and executes as narrow transformation

# to check paritions on the df
print(df.rdd.getNumPartitions())

# coalesce
df.coalesce(1) \
    .write.mode("overwrite").format("csv").save(
        f"dbfs:/FileStore/{user}/courses/")

df.coalesce(1) \
    .write.mode("overwrite") \
    .csv(
        f"dbfs:/FileStore/{user}/courses/",
        compression="gzip" #uncompression happens automatically
        )
# reparition
df.repartition(1) \
    .write.mode("overwrite").format("csv").save(
        f"dbfs:/FileStore/{user}/courses/")

df.repartition(1) \
    .write.mode("overwrite") \
    .csv(
        f"dbfs:/FileStore/{user}/courses/",
        compression="snappy" #uncompression happens automatically
        )

print(df.rdd.getNumPartitions())
