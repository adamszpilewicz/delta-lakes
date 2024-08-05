# Make sure to import the functions you want to use
import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, avg, desc

# Initialize Spark session with Delta Lake configurations
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Read YellowTaxis into our DataFrame
df = spark.read.format("delta").load("/data/taxidb/green/")

print("Number of records: ", df.count())
print("Columns: ", df.columns)

results = df.groupBy("VendorID") \
    .agg(avg("fare_amount").alias("AverageFare")) \
    .filter(col("AverageFare") > 1) \
    .sort(desc("AverageFare")) \
    .take(5)

[print(result) for result in results]

