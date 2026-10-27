from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.window import Window

catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")
name = spark.conf.get("student_name")

@dp.materialized_view(name=f"tourism_esg_gold_{name}")
def tourism_esg_gold():
    trips = spark.read.table(f"tourism_esg_silver_trips_{name}")
    transport = spark.read.table(f"tourism_esg_silver_transport_{name}").filter(F.col("__END_AT").isNull())

    return (
        trips
        .join(transport, ["transport_mode"], "left")
        .withColumn(
            "spend_per_traveler",
            F.col("total_trip_spend_usd") / F.col("num_travelers")
        )
        .withColumn(
            "spend_per_traveler_night",
            F.col("total_trip_spend_usd") / F.col("num_travelers") / F.col("duration_nights")
        )
        .withColumn(
            "emission_per_traveler_night",
            F.col("carbon_footprint_kg_co2") / F.col("num_travelers") / F.col("duration_nights")
        )
        .select(
            "trip_id",
            "year",
            "month",
            "origin_country",
            "destination_country",
            "transport_mode",
            "esg_category",
            "num_travelers",
            F.col("carbon_footprint_kg_co2").alias("carbon_emissions"),
            "emission_per_traveler",
            "spend_per_traveler_night",
            "emission_per_traveler_night",
            "duration_nights",
            "accommodation_type",
            F.col("total_trip_spend_usd").alias("total_spend"),
            "spend_without_food",
            "spend_per_traveler",
            "local_transport_used",
            "eco_friendly_choices"
        )
    )
        