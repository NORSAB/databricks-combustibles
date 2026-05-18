# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Cap 3 — Cadenas de Markov y Discretización K-Medias
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:** Tomar los alphas generados y discretizarlos en estados mediante K-Medias para estimar la Matriz de Transición de Markov.

# COMMAND ----------

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from pyspark.sql import functions as F

CATALOG = "combustibles_hn"
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Carga de Alphas

# COMMAND ----------

df_alphas = spark.table(f"{CATALOG}.gold.tesis_alphas_combustibles").toPandas()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Discretización K-Medias

# COMMAND ----------

def get_markov_states_kmeans(alphas_series, k=4):
    valid_alphas = np.array(alphas_series.fillna(0).values, dtype=float).reshape(-1, 1)
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    states = kmeans.fit_predict(valid_alphas)
    return states, kmeans.cluster_centers_

def estimate_transition_matrix(states, k=4):
    P = np.zeros((k, k))
    for i in range(len(states) - 1):
        P[states[i], states[i+1]] += 1
    row_sums = P.sum(axis=1)
    for i in range(k):
        if row_sums[i] > 0:
            P[i, :] = P[i, :] / row_sums[i]
    return P

# Procesar todos los combustibles
k_optimo = 4 # Valor estandar de la tesis
resultados_centroides = []
resultados_matrices = []

combustibles = ['Super_Alpha', 'Regular_Alpha', 'Diesel_Alpha', 'Kerosene_Alpha']

for col in combustibles:
    if col not in df_alphas.columns: continue
    fuel_name = col.replace('_Alpha', '')
    
    # K-Medias
    states, centroids = get_markov_states_kmeans(df_alphas[col], k=k_optimo)
    df_alphas[f'{fuel_name}_State'] = states
    
    # Guardar centroides
    for state, center in enumerate(centroids.flatten()):
        resultados_centroides.append({'Combustible': fuel_name, 'Estado': state, 'Centroide_Alpha': float(center)})
        
    # Matriz de Transicion
    P = estimate_transition_matrix(states, k=k_optimo)
    for i in range(k_optimo):
        for j in range(k_optimo):
            resultados_matrices.append({
                'Combustible': fuel_name, 
                'Estado_Origen': i, 
                'Estado_Destino': j, 
                'Probabilidad': float(P[i, j])
            })

# Guardar Centroides en Gold
df_centroides = spark.createDataFrame(pd.DataFrame(resultados_centroides))
df_centroides.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap3_centroides")
print("Centroides guardados en combustibles_hn.gold.tesis_cap3_centroides")

# Guardar Matrices en Gold
df_matrices = spark.createDataFrame(pd.DataFrame(resultados_matrices))
df_matrices.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap3_matrices_transicion")
print("Matrices de transición guardadas en combustibles_hn.gold.tesis_cap3_matrices_transicion")
