from pyspark import pipelines as dp
import pyspark.sql.functions as F

catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")
name = spark.conf.get("student_name")

@dp.table(name = f"tourism_esg_silver_trips_{name}")

@dp.expect_or_drop("no_missing_trip_id", "trip_id IS NOT NULL")
@dp.expect("negative_emissions", "carbon_footprint_kg_co2 > 0")
@dp.expect_or_drop("valid_num_travelers", "num_travelers IS NOT NULL")

# =============================================================================
# JUSTIFICACIÓN DE EXPECTATIONS (calidad de datos)
# -----------------------------------------------------------------------------
# 1. no_missing_trip_id — comportamiento: DROP (expect_or_drop)
#    Regla: trip_id IS NOT NULL
#    Justificación: trip_id es la llave primaria del hecho. Un registro sin
#    trip_id no puede unirse con dimensiones ni agregarse de forma confiable.
#    Se descarta la fila porque un dato sin identificador no aporta valor
#    analítico y contaminaría las métricas de gold.
#
# 2. negative_emissions — comportamiento: WARN (expect)
#    Regla: carbon_footprint_kg_co2 > 0
#    Justificación: las emisiones deberían ser positivas, pero un valor de
#    cero o negativo podría indicar un error de captura o un caso atípico
#    (p. ej. un viaje compensado 100%). Se deja pasar la fila y solo se advierte
#    para no perder el registro completo, que puede tener gasto y otros
#    atributos válidos. Permite investigar el caso sin descartar información.
#
# 3. valid_num_travelers — comportamiento: DROP (expect_or_drop)
#    Regla: num_travelers IS NOT NULL
#    Justificación: num_travelers se usa como denominador en columnas derivadas
#    (emission_per_traveler, spend_per_traveler). Un NULL provocaría división
#    por cero o NULL en gold. Se descarta la fila porque el cálculo de
#    métricas por viajero no es posible sin este campo.
# =============================================================================

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
        .withWatermark("ingest_timestamp", "1 hour")
        .dropDuplicates(["trip_id"])
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


