# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Cap 3 — Cadenas de Markov: K-Medias + Cuantiles (Benchmark)
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:** Discretizar alphas en estados via K-Medias (óptimo) y Cuantiles (benchmark).
# MAGIC
# MAGIC **Salidas Gold:** centroides, matrices P (K-Medias y Cuantiles), estados por semana, boundaries cuantiles.

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

def get_markov_states_kmeans(alphas_array, k=4):
    """Aplica K-Medias para discretizar las series continuas de Alphas en k estados."""
    valid_alphas = alphas_array.reshape(-1, 1)
    kmeans = KMeans(n_clusters=k, random_state=SEED, n_init=10)
    states = kmeans.fit_predict(valid_alphas)
    return states, kmeans.cluster_centers_.flatten()

def get_markov_states_quantiles(alphas_array, k=4):
    """Discretiza por cuantiles (benchmark de la tesis)."""
    boundaries = np.quantile(alphas_array, np.linspace(0, 1, k + 1)[1:-1])
    states = np.digitize(alphas_array, boundaries)
    return states, boundaries

def estimate_transition_matrix(states, k):
    """Estima la matriz de transición de Markov."""
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
# MAGIC ## 3. Procesamiento: K-Medias + Cuantiles para Todos los Combustibles

# COMMAND ----------

k_optimo = 4
k_cuantiles = 4  # benchmark fijo

resultados_centroides = []
resultados_matrices_km = []
resultados_matrices_q = []
resultados_cuantiles_boundaries = []
resultados_propiedades_espectrales = []

combustibles = ['Super', 'Regular', 'Diesel', 'Kerosene']

for fuel in combustibles:
    col_alpha = f'{fuel}_Alpha'
    if col_alpha not in df_alphas.columns:
        print(f"ADVERTENCIA: {col_alpha} no encontrada, saltando {fuel}.")
        continue

    alphas = np.array(df_alphas[col_alpha].fillna(0).values, dtype=float)

    # === K-MEDIAS ===
    states_km, centroids = get_markov_states_kmeans(alphas, k=k_optimo)
    df_alphas[f'{fuel}_State'] = states_km
    print(f"{fuel} K-Means: {k_optimo} estados. Centroides: {np.round(centroids, 6)}")

    for state_idx, center in enumerate(centroids):
        resultados_centroides.append({
            'Combustible': fuel, 'Estado': state_idx, 'Centroide_Alpha': float(center)
        })

    P_km = estimate_transition_matrix(states_km, k=k_optimo)
    for i in range(k_optimo):
        for j in range(k_optimo):
            resultados_matrices_km.append({
                'Combustible': fuel, 'Estado_Origen': i, 'Estado_Destino': j,
                'Probabilidad': float(P_km[i, j])
            })

    # === CUANTILES (BENCHMARK) ===
    states_q, boundaries = get_markov_states_quantiles(alphas, k=k_cuantiles)
    df_alphas[f'{fuel}_State_Quantile'] = states_q
    print(f"{fuel} Cuantiles: boundaries = {np.round(boundaries, 6)}")

    for b_idx, b_val in enumerate(boundaries):
        resultados_cuantiles_boundaries.append({
            'Combustible': fuel, 'Boundary_Index': b_idx, 'Boundary_Value': float(b_val)
        })

    P_q = estimate_transition_matrix(states_q, k=k_cuantiles)
    for i in range(k_cuantiles):
        for j in range(k_cuantiles):
            resultados_matrices_q.append({
                'Combustible': fuel, 'Estado_Origen': i, 'Estado_Destino': j,
                'Probabilidad': float(P_q[i, j])
            })

    # Varianza explicada (métrica de defensa: ~97.9%)
    from sklearn.metrics import calinski_harabasz_score
    total_variance = np.var(alphas) * len(alphas)
    within_variance = sum(np.var(alphas[states_km == c]) * np.sum(states_km == c) for c in range(k_optimo))
    explained_var_pct = (1 - within_variance / total_variance) * 100 if total_variance > 0 else 0

    resultados_centroides_meta = resultados_centroides  # reuse list

    # Prevalencia temporal (% tiempo en cada régimen)
    for k_idx in range(k_optimo):
        count = np.sum(states_km == k_idx)
        pct = count / len(states_km) * 100
        print(f"  Régimen {k_idx+1}: centroide={centroids[k_idx]:+.6f}, prevalencia={pct:.1f}%")

    print(f"  Varianza explicada: {explained_var_pct:.1f}%")

    # Propiedades espectrales de P
    eigenvalues = np.linalg.eigvals(P_km)
    sorted_eigs = np.sort(np.abs(eigenvalues))[::-1]
    # Distribución estacionaria (autovector izquierdo de eigenvalue=1)
    eig_vals, eig_vecs = np.linalg.eig(P_km.T)
    idx_one = np.argmin(np.abs(eig_vals - 1.0))
    pi_stationary = np.real(eig_vecs[:, idx_one])
    pi_stationary = pi_stationary / pi_stationary.sum()

    resultados_propiedades_espectrales.append({
        'Combustible': fuel,
        'Varianza_Explicada_Pct': float(explained_var_pct),
        'Eigenvalue_Dominante': float(sorted_eigs[0]),
        'Eigenvalue_2': float(sorted_eigs[1]) if len(sorted_eigs) > 1 else 0,
        'Mixing_Time_Approx': float(-1 / np.log(sorted_eigs[1])) if len(sorted_eigs) > 1 and sorted_eigs[1] > 0 else 0,
        'Pi_Estacionaria_0': float(pi_stationary[0]),
        'Pi_Estacionaria_1': float(pi_stationary[1]),
        'Pi_Estacionaria_2': float(pi_stationary[2]),
        'Pi_Estacionaria_3': float(pi_stationary[3]),
    })
    print()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Guardar Resultados en Gold

# COMMAND ----------

# 4a. Centroides K-Medias
spark.createDataFrame(pd.DataFrame(resultados_centroides)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap3_centroides")
print("✅ tesis_cap3_centroides")

# 4b. Matrices K-Medias
spark.createDataFrame(pd.DataFrame(resultados_matrices_km)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap3_matrices_transicion")
print("✅ tesis_cap3_matrices_transicion")

# 4c. Boundaries Cuantiles
spark.createDataFrame(pd.DataFrame(resultados_cuantiles_boundaries)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap5_cuantiles_boundaries")
print("✅ tesis_cap5_cuantiles_boundaries")

# 4d. Matrices Cuantiles
spark.createDataFrame(pd.DataFrame(resultados_matrices_q)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap5_cuantiles_matrices")
print("✅ tesis_cap5_cuantiles_matrices")

# 4e. Alphas con AMBOS tipos de estados (K-Medias + Cuantiles)
spark.createDataFrame(df_alphas).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_alphas_con_estados")
print("✅ tesis_alphas_con_estados")
print(f"Columnas finales: {list(df_alphas.columns)}")

# 4f. Propiedades espectrales + Varianza explicada
if resultados_propiedades_espectrales:
    spark.createDataFrame(pd.DataFrame(resultados_propiedades_espectrales)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap3_propiedades_espectrales")
    avg_var = np.mean([r['Varianza_Explicada_Pct'] for r in resultados_propiedades_espectrales])
    print(f"✅ tesis_cap3_propiedades_espectrales")
    print(f"\n=== VARIANZA EXPLICADA PROMEDIO: {avg_var:.1f}% ===")
