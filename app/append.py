import pyspark
from delta import *
from delta.tables import *

# Initialize Spark session with Delta Lake configurations
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.format("delta").load("/data/taxidb/yellow/")
yellowTaxiSchema = df.schema
print(yellowTaxiSchema)

print(df.columns)

df_for_append = spark.read \
.option("header", "false") \
.schema(yellowTaxiSchema) \
.csv("/input/append_yellow/data.csv")


df_for_append.write.format("delta").mode("append").save("/data/taxidb/yellow/")