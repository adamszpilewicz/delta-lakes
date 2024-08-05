import pyspark
from delta import *
from delta.tables import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Create Database if it does not exist
spark.sql("CREATE DATABASE IF NOT EXISTS taxidb")

# Switch to the taxidb schema
spark.catalog.setCurrentDatabase("taxidb")

INPUT_PATH = '/input/taxi_rate_code.csv'

# Read the DataFrame from the input path
df_rate_codes = spark \
    .read \
    .format("csv") \
    .option("inferSchema", True) \
    .option("header", True) \
    .load(INPUT_PATH)

# Save our DataFrame as a managed Hive table
df_rate_codes.write.format("delta").saveAsTable('rateCard2')

# Show the contents of the table to verify
df = spark.table("rateCard2")
df.show()

# Describe the table extended
description_df = spark.sql("DESCRIBE TABLE EXTENDED rateCard2")
description_df.show(truncate=False)

# List all databases
databases = spark.sql("SHOW DATABASES").collect()
print("Databases:")
for db in databases:
    print(db[0])

# Create Database if it does not exist
spark.sql("SELECT * FROM taxidb.rateCard2").show()