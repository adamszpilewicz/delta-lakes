import pyspark
from delta import *
from delta.tables import *

# Initialize Spark session
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# List all databases
databases = spark.sql("SHOW DATABASES").collect()
print("Databases:")
for db in databases:
    print(db[0])

# Switch to the taxidb schema
spark.catalog.setCurrentDatabase("taxidb")

# List all tables in the current database
tables = spark.sql("SHOW TABLES").collect()
print("\nTables in taxidb:")
for table in tables:
    print(f"Table: {table.tableName}, isTemporary: {table.isTemporary}")

# Describe the table extended
description_df = spark.sql("DESCRIBE TABLE EXTENDED taxidb.rateCard")
description_df.show(truncate=False)
