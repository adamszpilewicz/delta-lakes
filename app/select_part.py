import pyspark
from delta import *
from delta.tables import *

# Initialize Spark session with Delta Lake configurations
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.format("delta").load("/data/taxidb/yellow_partitions/")
df.show()

print("Number of records: ", df.count())