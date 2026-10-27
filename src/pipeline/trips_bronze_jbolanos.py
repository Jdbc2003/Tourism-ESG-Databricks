from pyspark import pipelines as dp
import pyspark.sql.functions as F

catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")
name = spark.conf.get("student_name")

source = f"/Volumes/{catalog}/{schema}/raw_data"

@dp.table(name = f"tourism_esg_bronze_trips_{name}")
def tourism_esg_bronze_trips():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", True)
        .load(f"{source}")
        .select(
            "*",
            F.current_timestamp().alias("ingest_timestamp"),
            "_metadata.file_name"
        )
    )