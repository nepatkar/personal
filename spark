Here is a sample PySpark code to copy a file from HDFS to Snowflake using the Snowflake Spark Connector:

```python
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("HDFS to Snowflake") \
    .getOrCreate()

# HDFS file path (replace with your actual file path)
hdfs_path = "hdfs:///user/yourusername/yourfile.csv"

# Read data from HDFS (assuming CSV; adjust for other formats as needed)
df = spark.read \
    .option("header", "true") \
    .csv(hdfs_path)

# Snowflake connection options
sfOptions = {
    "sfURL": "<account>.snowflakecomputing.com",
    "sfUser": "<username>",
    "sfPassword": "<password>",
    "sfDatabase": "<database>",
    "sfSchema": "<schema>",
    "sfWarehouse": "<warehouse>",
    "dbtable": "<destination_table>"
}

# Write DataFrame to Snowflake
df.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .mode("overwrite") \  # or "append"
    .save()

spark.stop()
```

Replace the placeholder values in sfOptions with your actual Snowflake configuration.

**Requirements:**
- Install the Snowflake Spark Connector and its dependencies in your Spark environment.
- Your Spark cluster must have network connectivity to Snowflake.

Let me know if you need help with the connector setup or code customization!
