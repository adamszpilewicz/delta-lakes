import pyspark
from delta import *
from delta.tables import *

# Initialize Spark session with Delta Lake configurations
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Create Database if it does not exist
spark.sql("CREATE DATABASE IF NOT EXISTS taxidb")

# Switch to the taxidb schema
spark.catalog.setCurrentDatabase("taxidb")

# Define input and Delta Lake paths
INPUT_PATH = '/input/green_tripdata_2019-12.csv'
DELTALAKE_PATH = '/data/taxidb/green'

# Read the DataFrame from the input path
df_rate_codes = spark \
    .read \
    .format("csv") \
    .option("inferSchema", True) \
    .option("header", True) \
    .load(INPUT_PATH)

# Save the DataFrame as a Delta table, specifying the path and table name
df_rate_codes \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option('path', DELTALAKE_PATH) \
    .saveAsTable('taxidb.green')

# Show the contents of the table to verify
df = spark.table("taxidb.green")
df.show()

# Describe the table extended
description_df = spark.sql("DESCRIBE TABLE EXTENDED taxidb.rateCard")
description_df.show(truncate=False)

# List all databases
databases = spark.sql("SHOW DATABASES").collect()
print("Databases:")
for db in databases:
    print(db[0])

# List all tables in the current database
tables = spark.sql("SHOW TABLES").collect()
print("\nTables in taxidb:")
for table in tables:
    print(f"Table: {table.tableName}, isTemporary: {table.isTemporary}")


