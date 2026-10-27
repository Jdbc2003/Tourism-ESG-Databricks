from pyspark import pipelines as dp
import pyspark.sql.functions as F

catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")
name = spark.conf.get("student_name")

dp.create_streaming_table(name = f"tourism_esg_silver_transport_{name}")

dp.create_auto_cdc_flow(
    target = f"tourism_esg_silver_transport_{name}",
    source = f"tourism_esg_bronze_transport_{name}",
    keys = ["transport_mode"],
    sequence_by = F.to_timestamp(F.col("event_timestamp"), "MM-dd-yyyy HH:mm:ss"),
    apply_as_deletes = F.expr("operation = 'DELETE'"),
    except_column_list = ["operation", "event_timestamp"],
    stored_as_scd_type = 2
)