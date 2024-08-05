import pyspark
from delta import *
from delta.tables import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Create Database
spark.sql("CREATE DATABASE IF NOT EXISTS taxidb")

# Create Table
spark.sql("""
    CREATE TABLE IF NOT EXISTS taxidb.rateCard (
        rateCodeId INT,
        rateCodeDesc STRING
    )
    USING DELTA
    LOCATION '/data/taxidb/rateCard'
""")

spark.sql("DESCRIBE TABLE EXTENDED taxidb.rateCard").show()