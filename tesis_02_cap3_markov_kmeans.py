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
    """Aplica K-Medias para discretizar las series continuas de Alphas en k estados."""
    valid_alphas = alphas_series.fillna(0).values.reshape(-1, 1)
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    states = kmeans.fit_predict(valid_alphas)
    return states, kmeans.cluster_centers_

df_alphas['Super_State'], super_centroids = get_markov_states_kmeans(df_alphas['Super_Alpha'], k=4)
print("Centroides K-Medias para Super:", super_centroids.flatten())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Estimación Matriz de Transición (P)

# COMMAND ----------

def estimate_transition_matrix(states, k=4):
    """Estima la matriz de transición de Markov basada en la secuencia de estados."""
    P = np.zeros((k, k))
    for i in range(len(states) - 1):
        P[states[i], states[i+1]] += 1
    
    # Normalizar por filas
    row_sums = P.sum(axis=1)
    for i in range(k):
        if row_sums[i] > 0:
            P[i, :] = P[i, :] / row_sums[i]
            
    return P

P_super = estimate_transition_matrix(df_alphas['Super_State'].values, k=4)
print("Matriz de Transición (P) para Super:")
print(np.round(P_super, 3))
