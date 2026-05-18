# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Cap 3 — Cadenas de Markov y Discretización K-Medias
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:** Tomar los alphas generados y discretizarlos en estados mediante K-Medias para estimar la Matriz de Transición de Markov.
# MAGIC
# MAGIC **Salidas Gold:** centroides, matrices de transición, y estados por semana.

# COMMAND ----------

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from pyspark.sql import functions as F

CATALOG = "combustibles_hn"
spark.sql(f"USE CATALOG {CATALOG}")

SEED = 42
np.random.seed(SEED)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Carga de Alphas

# COMMAND ----------

df_alphas = spark.table(f"{CATALOG}.gold.tesis_alphas_combustibles").toPandas()
print(f"Columnas disponibles: {list(df_alphas.columns)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Funciones de Discretización y Transición

# COMMAND ----------

def get_markov_states_kmeans(alphas_series, k=4):
    """Aplica K-Medias para discretizar las series continuas de Alphas en k estados."""
    valid_alphas = np.array(alphas_series.fillna(0).values, dtype=float).reshape(-1, 1)
    kmeans = KMeans(n_clusters=k, random_state=SEED, n_init=10)
    states = kmeans.fit_predict(valid_alphas)
    return states, kmeans.cluster_centers_

def estimate_transition_matrix(states, k=4):
    """Estima la matriz de transición de Markov basada en la secuencia de estados."""
    P = np.zeros((k, k))
    for i in range(len(states) - 1):
        P[states[i], states[i+1]] += 1
    row_sums = P.sum(axis=1)
    for i in range(k):
        if row_sums[i] > 0:
            P[i, :] = P[i, :] / row_sums[i]
    return P

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Procesamiento de Todos los Combustibles

# COMMAND ----------

k_optimo = 4
resultados_centroides = []
resultados_matrices = []

combustibles = ['Super', 'Regular', 'Diesel', 'Kerosene']

for fuel in combustibles:
    col_alpha = f'{fuel}_Alpha'
    if col_alpha not in df_alphas.columns:
        print(f"ADVERTENCIA: {col_alpha} no encontrada, saltando {fuel}.")
        continue

    # K-Medias
    states, centroids = get_markov_states_kmeans(df_alphas[col_alpha], k=k_optimo)

    # Guardar estados por semana directamente en el DataFrame
    df_alphas[f'{fuel}_State'] = states
    print(f"{fuel}: {k_optimo} estados asignados. Centroides: {centroids.flatten()}")

    # Guardar centroides
    for state_idx, center in enumerate(centroids.flatten()):
        resultados_centroides.append({
            'Combustible': fuel,
            'Estado': state_idx,
            'Centroide_Alpha': float(center)
        })

    # Estimar Matriz de Transición
    P = estimate_transition_matrix(states, k=k_optimo)
    for i in range(k_optimo):
        for j in range(k_optimo):
            resultados_matrices.append({
                'Combustible': fuel,
                'Estado_Origen': i,
                'Estado_Destino': j,
                'Probabilidad': float(P[i, j])
            })

    # Imprimir la Matriz P
    print(f"Matriz de Transicion (P) para {fuel}:")
    print(np.round(P, 3))
    print()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Guardar Resultados en Gold

# COMMAND ----------

# 4a. Guardar Centroides en Gold
df_centroides = spark.createDataFrame(pd.DataFrame(resultados_centroides))
df_centroides.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap3_centroides")
print("Centroides guardados en combustibles_hn.gold.tesis_cap3_centroides")

# 4b. Guardar Matrices en Gold
df_matrices = spark.createDataFrame(pd.DataFrame(resultados_matrices))
df_matrices.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap3_matrices_transicion")
print("Matrices de transicion guardadas en combustibles_hn.gold.tesis_cap3_matrices_transicion")

# 4c. Guardar Alphas con Estados por semana (para gráficos de regímenes y sombreado)
df_alphas_con_estados = spark.createDataFrame(df_alphas)
df_alphas_con_estados.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_alphas_con_estados")
print("Alphas con estados guardados en combustibles_hn.gold.tesis_alphas_con_estados")
print(f"Columnas finales: {list(df_alphas.columns)}")
