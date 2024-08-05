import pyspark
from delta import *
import time

# Create a builder with the Delta extensions
builder = (
    pyspark.sql.SparkSession.builder
    .appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

# Create a Spark instance with the builder
# As a result, we now can read and write Delta files
spark = configure_spark_with_delta_pip(builder).getOrCreate()
print(f"Hello, Spark version: {spark.version}")

# Create a range, and save it in Delta format to ensure
# that our Delta extensions are indeed working
df = spark.range(0, 109)
(
    df.write
    .format("delta")
    .mode("overwrite")
    .save("/data/range")
)
print(f"The number of partitions is: {df.rdd.getNumPartitions()}")

# Keep the Spark session alive
print("Access the Spark Web UI at http://localhost:4040")
# or you can use: time.sleep(3600) # Keeps the Spark session alive for an hour
