# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Cap 4 — Modelo Híbrido TCROC-NNLS
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:** Validación predictiva con Non-Negative Least Squares (NNLS) sobre la matriz de transición y los centros de estado.

# COMMAND ----------

import numpy as np
import pandas as pd
from scipy.optimize import nnls
from pyspark.sql import functions as F

CATALOG = "combustibles_hn"
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Aplicación NNLS

# COMMAND ----------

def predict_next_state_nnls(P_matrix, current_state_vector):
    A = P_matrix.T
    b = current_state_vector
    x, residual = nnls(A, b)
    if np.sum(x) > 0:
        x = x / np.sum(x)
    return x

try:
    df_matrices = spark.table(f"{CATALOG}.gold.tesis_cap3_matrices_transicion").toPandas()
except Exception as e:
    print("Por favor, ejecuta tesis_02 primero para generar las matrices de transición.")
    dbutils.notebook.exit("Faltan dependencias")

# Construir matrices P por combustible y predecir
resultados_nnls = []
combustibles = df_matrices['Combustible'].unique()
k_optimo = 4

for fuel in combustibles:
    # Extraer la matriz P
    P = np.zeros((k_optimo, k_optimo))
    df_fuel = df_matrices[df_matrices['Combustible'] == fuel]
    for _, row in df_fuel.iterrows():
        P[int(row['Estado_Origen']), int(row['Estado_Destino'])] = row['Probabilidad']
    
    # Asumir que el estado actual es un vector uniforme o el último estado conocido
    # Para la tesis, validamos que la prediccion matemática de NNLS no arroje negativos
    curr_state = np.array([1.0, 0.0, 0.0, 0.0]) # Ejemplo estático: estado más bajo
    
    next_state = predict_next_state_nnls(P, curr_state)
    
    for i, prob in enumerate(next_state):
        resultados_nnls.append({
            'Combustible': fuel,
            'Estado_Actual_Simulado': 0,
            'Estado_Predicho': i,
            'Probabilidad_NNLS': float(prob)
        })

df_nnls = spark.createDataFrame(pd.DataFrame(resultados_nnls))
df_nnls.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap4_predicciones_nnls")
print("Predicciones NNLS guardadas en combustibles_hn.gold.tesis_cap4_predicciones_nnls")
