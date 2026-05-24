# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Cap 3 — Cadenas de Markov: K-Medias, Transiciones con Errores Estándar y Propiedades Espectrales
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:** 
# MAGIC 1. Leer los alphas óptimos del Capítulo 2 (`gold.tesis_alphas_combustibles_optimos`).
# MAGIC 2. Discretizar alphas en estados vía K-Medias (óptimo de la tesis) y Cuantiles (benchmark).
# MAGIC 3. Estimar las matrices de transición de Markov ($\hat{\mat{P}}$) y calcular analíticamente sus **errores estándar asintóticos** ($\hat{\sigma}_{ij}$) según el Corolario 3.1 de la tesis.
# MAGIC 4. Realizar el análisis espectral completo de cada matriz: autovalores, brecha espectral ($\gamma$), tiempo de mezcla ($t_{\text{mix}}$) y distribución estacionaria ($\boldsymbol{\pi}$).
# MAGIC
# MAGIC **Salidas Gold:**
# MAGIC - `combustibles_hn.gold.tesis_cap3_centroides`: Centroides de K-Medias para cada combustible.
# MAGIC - `combustibles_hn.gold.tesis_cap3_matrices_transicion`: Matrices de transición K-Medias con conteos, totales y errores estándar.
# MAGIC - `combustibles_hn.gold.tesis_cap5_cuantiles_boundaries`: Puntos de corte para la discretización de cuantiles.
# MAGIC - `combustibles_hn.gold.tesis_cap5_cuantiles_matrices`: Matrices de transición para la discretización de cuantiles con errores estándar.
# MAGIC - `combustibles_hn.gold.tesis_alphas_con_estados`: Alphas enriquecidos con los estados asignados por ambos métodos.
# MAGIC - `combustibles_hn.gold.tesis_cap3_propiedades_espectrales`: Autovalores, brecha espectral, tiempos de mezcla y distribuciones estacionarias.

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
# MAGIC ## 1. Carga de Alphas Óptimos (del Capítulo 2)

# COMMAND ----------

df_alphas = spark.table(f"{CATALOG}.gold.tesis_alphas_combustibles_optimos").orderBy("Fecha").toPandas()
print(f"Columnas disponibles: {list(df_alphas.columns)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Funciones de Discretización y Estimación con Errores Estándar

# COMMAND ----------

def get_markov_states_kmeans(alphas_array, k=4):
    """Aplica K-Medias para discretizar las series continuas de Alphas en k estados."""
    valid_alphas = alphas_array.reshape(-1, 1)
    kmeans = KMeans(n_clusters=k, random_state=SEED, n_init=10)
    states = kmeans.fit_predict(valid_alphas)
    
    # Reordenar estados para que el menor centroide sea el estado 0 y el mayor sea k-1
    centroids = kmeans.cluster_centers_.flatten()
    sort_idx = np.argsort(centroids)
    rank_map = {old_label: new_rank for new_rank, old_label in enumerate(sort_idx)}
    states_sorted = np.array([rank_map[s] for s in states])
    centroids_sorted = centroids[sort_idx]
    
    return states_sorted, centroids_sorted

def get_markov_states_quantiles(alphas_array, k=4):
    """Discretiza por cuantiles (benchmark de la tesis)."""
    boundaries = np.quantile(alphas_array, np.linspace(0, 1, k + 1)[1:-1])
    states = np.digitize(alphas_array, boundaries)
    return states, boundaries

def estimate_transition_matrix_with_errors(states, k):
    """Estima la matriz de transición de Markov y calcula sus errores estándar y estadísticas."""
    counts = np.zeros((k, k))
    for i in range(len(states) - 1):
        counts[states[i], states[i+1]] += 1
        
    row_sums = counts.sum(axis=1)
    P = np.zeros((k, k))
    std_errors = np.zeros((k, k))
    
    for i in range(k):
        n_i = row_sums[i]
        if n_i > 0:
            P[i, :] = counts[i, :] / n_i
            # Error estándar asintótico: sqrt( P_ij * (1 - P_ij) / n_i )
            for j in range(k):
                p_ij = P[i, j]
                std_errors[i, j] = np.sqrt(p_ij * (1.0 - p_ij) / n_i)
        else:
            P[i, :] = 1.0 / k
            std_errors[i, :] = 0.0
            
    return P, counts, row_sums, std_errors

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Procesamiento y Análisis Espectral

# COMMAND ----------

# K* óptimo por combustible según la tesis
K_OPTIMO_POR_FUEL = {'Super': 4, 'Regular': 3, 'Diesel': 3, 'Kerosene': 5}
k_cuantiles = 4  # benchmark fijo de cuantiles

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
    k_optimo = K_OPTIMO_POR_FUEL[fuel]

    # === A. K-MEDIAS (Óptimo) ===
    states_km, centroids = get_markov_states_kmeans(alphas, k=k_optimo)
    df_alphas[f'{fuel}_State'] = states_km
    print(f"\n=== {fuel} K-Means: {k_optimo} estados ===")
    print(f"Centroides: {np.round(centroids, 6)}")

    for state_idx, center in enumerate(centroids):
        resultados_centroides.append({
            'Combustible': fuel, 'Estado': state_idx, 'Centroide_Alpha': float(center)
        })

    # Estimar matriz con errores estándar
    P_km, C_km, n_orig_km, SE_km = estimate_transition_matrix_with_errors(states_km, k=k_optimo)
    for i in range(k_optimo):
        for j in range(k_optimo):
            resultados_matrices_km.append({
                'Combustible': fuel, 
                'Estado_Origen': i, 
                'Estado_Destino': j,
                'Probabilidad': float(P_km[i, j]),
                'Error_Estandar': float(SE_km[i, j]),
                'Conteo_Transiciones': int(C_km[i, j]),
                'Total_Origen': int(n_orig_km[i])
            })

    # === B. CUANTILES (Benchmark) ===
    states_q, boundaries = get_markov_states_quantiles(alphas, k=k_cuantiles)
    df_alphas[f'{fuel}_State_Quantile'] = states_q

    for b_idx, b_val in enumerate(boundaries):
        resultados_cuantiles_boundaries.append({
            'Combustible': fuel, 'Boundary_Index': b_idx, 'Boundary_Value': float(b_val)
        })

    P_q, C_q, n_orig_q, SE_q = estimate_transition_matrix_with_errors(states_q, k=k_cuantiles)
    for i in range(k_cuantiles):
        for j in range(k_cuantiles):
            resultados_matrices_q.append({
                'Combustible': fuel, 
                'Estado_Origen': i, 
                'Estado_Destino': j,
                'Probabilidad': float(P_q[i, j]),
                'Error_Estandar': float(SE_q[i, j]),
                'Conteo_Transiciones': int(C_q[i, j]),
                'Total_Origen': int(n_orig_q[i])
            })

    # === C. Varianza Explicada (Métrica de Agrupamiento) ===
    total_variance = np.var(alphas) * len(alphas)
    within_variance = sum(np.var(alphas[states_km == c]) * np.sum(states_km == c) for c in range(k_optimo))
    explained_var_pct = (1 - within_variance / total_variance) * 100 if total_variance > 0 else 0
    print(f"Varianza explicada por K-medias: {explained_var_pct:.2f}%")

    # === D. Propiedades Espectrales de P_km ===
    eigenvalues = np.linalg.eigvals(P_km)
    sorted_eigs = np.sort(np.abs(eigenvalues))[::-1]
    
    # Distribución estacionaria (autovector izquierdo de eigenvalue=1, que es derecho de P.T)
    eig_vals, eig_vecs = np.linalg.eig(P_km.T)
    idx_one = np.argmin(np.abs(eig_vals - 1.0))
    pi_stationary = np.real(eig_vecs[:, idx_one])
    pi_stationary = pi_stationary / pi_stationary.sum()
    
    # Brecha espectral y tiempo de mezcla (con epsilon = 0.01)
    lambda_2 = sorted_eigs[1] if len(sorted_eigs) > 1 else 0
    spectral_gap = 1.0 - lambda_2
    mixing_time = -np.log(0.01) / spectral_gap if spectral_gap > 0 and lambda_2 < 1.0 else 0

    resultados_propiedades_espectrales.append({
        'Combustible': fuel,
        'K_Optimo': k_optimo,
        'Varianza_Explicada_Pct': float(explained_var_pct),
        'Eigenvalue_Dominante': float(sorted_eigs[0]),
        'Eigenvalue_2': float(lambda_2),
        'Spectral_Gap': float(spectral_gap),
        'Mixing_Time_Approx': float(mixing_time),
        'Pi_Estacionaria': str([round(float(x), 6) for x in pi_stationary]),
    })
    
    print(f"Brecha espectral (gamma): {spectral_gap:.6f}")
    print(f"Tiempo de mezcla estimado: {mixing_time:.2f} semanas")
    print(f"Distribución Estacionaria pi: {[round(float(x), 4) for x in pi_stationary]}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Guardar Resultados en Capa Gold

# COMMAND ----------

# 4a. Centroides K-Medias
spark.createDataFrame(pd.DataFrame(resultados_centroides)).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap3_centroides")
print("✅ gold.tesis_cap3_centroides")

# 4b. Matrices K-Medias con Errores Estándar
spark.createDataFrame(pd.DataFrame(resultados_matrices_km)).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap3_matrices_transicion")
print("✅ gold.tesis_cap3_matrices_transicion")

# 4c. Boundaries Cuantiles
spark.createDataFrame(pd.DataFrame(resultados_cuantiles_boundaries)).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap5_cuantiles_boundaries")
print("✅ gold.tesis_cap5_cuantiles_boundaries")

# 4d. Matrices Cuantiles con Errores Estándar
spark.createDataFrame(pd.DataFrame(resultados_matrices_q)).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap5_cuantiles_matrices")
print("✅ gold.tesis_cap5_cuantiles_matrices")

# 4e. Alphas con Estados (K-Medias + Cuantiles)
spark.createDataFrame(df_alphas).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_alphas_con_estados")
print("✅ gold.tesis_alphas_con_estados")

# 4f. Propiedades Espectrales
spark.createDataFrame(pd.DataFrame(resultados_propiedades_espectrales)).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap3_propiedades_espectrales")
print("✅ gold.tesis_cap3_propiedades_espectrales")
