# Databricks notebook source
import pyspark.sql.functions as F

dbutils.widgets.text("catalog", "tourism-esg")
dbutils.widgets.text("schema", "tourism-esg-dev")
dbutils.widgets.text("min_rows", "1")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
min_rows = int(dbutils.widgets.get("min_rows"))


# COMMAND ----------

# DBTITLE 1,Cell 2
row_validation = spark.sql(f"""
SELECT COUNT(*) AS row_count
FROM `{catalog}`.`{schema}`.tourism_esg_gold_jbolanos
""").collect()[0]["row_count"]

print(f"Filas en gold: {row_validation:,} | Umbral mínimo: {min_rows:,}")

# COMMAND ----------

# DBTITLE 1,Cell 3
# Publica el conteo para que la condition task lo compare.
# Sin try/except: si fallara, el run falla aquí con el error real visible
# (taskValues siempre está disponible en una ejecución de Job).
dbutils.jobs.taskValues.set(key="row_validation", value=row_validation)
print(f"Task value 'row_validation' establecido: {row_validation:,}")

# COMMAND ----------

# DBTITLE 1,Cell 4
if row_validation <= min_rows:
    # El raise se movió a la rama 'else' (Gold_validation_failed) para que el If/Else sea real
    print(f"ADVERTENCIA: {row_validation} filas <= umbral {min_rows}. La rama 'else' manejará el fallo.")
else:
    print(f"Validación OK: {row_validation:,} filas > umbral {min_rows:,}. Continuando hacia el dashboard.")