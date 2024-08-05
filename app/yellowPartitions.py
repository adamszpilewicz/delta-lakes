import pyspark
from delta import *
from delta.tables import *
from pyspark.sql.functions import col, avg

# Initialize Spark session with Delta Lake configurations
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Define input path and Delta Lake path
INPUT_PATH = '/input/YellowTaxi/'
DELTALAKE_PATH = '/data/taxidb/yellow_partitions'

# Read the Parquet files into a DataFrame
df = spark.read.format("parquet").load(INPUT_PATH)

# Write the DataFrame as a Delta table, specifying the path and partition column
df.write.format("delta").mode("overwrite").partitionBy("VendorID").option('path', DELTALAKE_PATH).saveAsTable('taxidb.yellow')

# Verify the number of records
print("Number of records: ", df.count())
