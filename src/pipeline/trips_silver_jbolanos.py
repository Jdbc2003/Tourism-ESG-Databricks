from pyspark import pipelines as dp
import pyspark.sql.functions as F

catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")
name = spark.conf.get("student_name")

@dp.table(name = f"tourism_esg_silver_trips_{name}")

@dp.expect_or_drop("no_missing_trip_id", "trip_id IS NOT NULL")
@dp.expect("negative_emissions", "carbon_footprint_kg_co2 > 0")
@dp.expect_or_drop("valid_num_travelers", "num_travelers IS NOT NULL")

#justificacion constraints

def tourism_esg_silver_trips():
    return (
        spark.readStream.table(f"tourism_esg_bronze_trips_{name}")
        .withColumn(
            "emission_per_traveler",
            F.col("carbon_footprint_kg_co2") / F.col("num_travelers")
        )
        .withColumn(
            "spend_without_food",
            F.col("total_trip_spend_usd") - F.col("restaurant_spend_per_day_usd") * F.col("duration_nights"))
        .select(
            F.trim(F.col("trip_id")).alias("trip_id"),
            "year",
            "month",
            F.initcap(F.col("origin_country")).alias("origin_country"),
            F.initcap(F.col("destination_country")).alias("destination_country"),
            "num_travelers",
            "carbon_footprint_kg_co2",
            "emission_per_traveler",
            "transport_mode",
            "duration_nights",
            "accommodation_type",
            "total_trip_spend_usd",
            "spend_without_food",
            "local_transport_used",
            "eco_friendly_choices"
            )
        )


