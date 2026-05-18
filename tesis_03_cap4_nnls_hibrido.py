# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Cap 4 — Modelo Híbrido TCROC-NNLS
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:** Validación predictiva con Non-Negative Least Squares (NNLS) sobre la matriz de transición.
# MAGIC
# MAGIC **Salidas Gold:** Predicciones NNLS por cada estado inicial posible, RMSE Markov base.

# COMMAND ----------

import numpy as np
import pandas as pd
from scipy.optimize import nnls
from sklearn.metrics import mean_squared_error
from pyspark.sql import functions as F

CATALOG = "combustibles_hn"
spark.sql(f"USE CATALOG {CATALOG}")

SEED = 42
np.random.seed(SEED)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Carga de Dependencias (Cap 3)

# COMMAND ----------

try:
    df_matrices = spark.table(f"{CATALOG}.gold.tesis_cap3_matrices_transicion").toPandas()
    df_centroides = spark.table(f"{CATALOG}.gold.tesis_cap3_centroides").toPandas()
    df_alphas = spark.table(f"{CATALOG}.gold.tesis_alphas_con_estados").toPandas()
    print("Datos cargados correctamente desde Cap 3.")
except Exception as e:
    print(f"ERROR: {e}")
    print("Por favor, ejecuta tesis_02 primero.")
    dbutils.notebook.exit("Faltan dependencias")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Función NNLS

# COMMAND ----------

def predict_next_state_nnls(P_matrix, current_state_vector):
    """Aplica optimización NNLS para predecir el próximo vector de probabilidades de estado."""
    A = P_matrix.T
    b = current_state_vector
    x, residual = nnls(A, b)
    if np.sum(x) > 0:
        x = x / np.sum(x)
    return x, residual

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Predicciones NNLS y Cálculo de RMSE Markov Base

# COMMAND ----------

resultados_nnls = []
resultados_markov_rmse = []
combustibles = df_matrices['Combustible'].unique()
k_optimo = 4

for fuel in combustibles:
    # Reconstruir la matriz P
    P = np.zeros((k_optimo, k_optimo))
    df_fuel = df_matrices[df_matrices['Combustible'] == fuel]
    for _, row in df_fuel.iterrows():
        P[int(row['Estado_Origen']), int(row['Estado_Destino'])] = row['Probabilidad']

    # Obtener centroides ordenados
    df_fuel_centroides = df_centroides[df_centroides['Combustible'] == fuel].sort_values('Estado')
    centroids = df_fuel_centroides['Centroide_Alpha'].values

    # Predicciones NNLS desde cada estado inicial posible
    for s in range(k_optimo):
        curr_state = np.zeros(k_optimo)
        curr_state[s] = 1.0
        next_state, residual = predict_next_state_nnls(P, curr_state)
        for i, prob in enumerate(next_state):
            resultados_nnls.append({
                'Combustible': fuel,
                'Estado_Actual': s,
                'Estado_Predicho': i,
                'Probabilidad_NNLS': float(prob),
                'Residual_NNLS': float(residual)
            })

    # Calcular RMSE Markov base (predicción 1 paso adelante)
    col_state = f'{fuel}_State'
    col_price = fuel
    if col_state in df_alphas.columns and col_price in df_alphas.columns:
        prices = np.array(df_alphas[col_price].fillna(0).values, dtype=float)
        states = df_alphas[col_state].values

        actual_prices = []
        predicted_prices = []

        for t in range(len(states) - 1):
            current_s = int(states[t])
            # Predecir el siguiente estado más probable
            next_s = np.argmax(P[current_s, :])
            pred_alpha = centroids[next_s] if next_s < len(centroids) else 0
            pred_price = prices[t] * (1 + pred_alpha)

            actual_prices.append(prices[t + 1])
            predicted_prices.append(pred_price)

        if len(actual_prices) > 0:
            rmse_markov = np.sqrt(mean_squared_error(actual_prices, predicted_prices))
            resultados_markov_rmse.append({
                'Combustible': fuel,
                'RMSE_Markov': float(rmse_markov),
                'N_Predicciones': len(actual_prices)
            })
            print(f"{fuel}: RMSE Markov Base = {rmse_markov:.6f}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Guardar Resultados en Gold

# COMMAND ----------

# 4a. Predicciones NNLS completas
df_nnls = spark.createDataFrame(pd.DataFrame(resultados_nnls))
df_nnls.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap4_predicciones_nnls")
print("Predicciones NNLS guardadas en combustibles_hn.gold.tesis_cap4_predicciones_nnls")

# 4b. RMSE Markov Base (necesario para el barplot comparativo Cap 6)
df_markov_rmse = spark.createDataFrame(pd.DataFrame(resultados_markov_rmse))
df_markov_rmse.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap4_rmse_markov_base")
print("RMSE Markov base guardado en combustibles_hn.gold.tesis_cap4_rmse_markov_base")
