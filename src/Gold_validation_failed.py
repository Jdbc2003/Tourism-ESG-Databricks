# Databricks notebook source
# DBTITLE 1,Recibir parámetros de validación
# Rama "else" del If/Else: la validación de Gold NO cumplió el umbral mínimo
# Recibe el conteo real (task value) y el umbral (job parameter reutilizado)
dbutils.widgets.text("row_validation", "0")
dbutils.widgets.text("min_rows", "0")

row_validation = dbutils.widgets.get("row_validation")
min_rows = dbutils.widgets.get("min_rows")

# COMMAND ----------

# DBTITLE 1,Marcar el run como fallido
print(f"Validación Gold FALLIDA: {row_validation} filas no superan el umbral mínimo de {min_rows}.")

raise Exception(
    f"Validación Gold fallida: el conteo de filas ({row_validation}) "
    f"no es mayor que el umbral mínimo ({min_rows}). "
    "El dashboard NO se actualiza y el run queda marcado como FAILED."
)