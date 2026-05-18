# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Cap 5 — Detección y Pronóstico (Grid Search)
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:** Optimización de hiperparámetros (Grid Search) para W, Lambda y k usando paralelismo básico de Python.

# COMMAND ----------

import numpy as np
import pandas as pd
import scipy.stats as stats
from concurrent.futures import ProcessPoolExecutor
from pyspark.sql import functions as F

CATALOG = "combustibles_hn"
spark.sql(f"USE CATALOG {CATALOG}")

# CONSTANTES DE TESIS
SEED = 42
np.random.seed(SEED)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Lógica de Evaluación para Grid Search

# COMMAND ----------

def calculate_alphas(series, w, lambda_decay):
    prices = series.values
    alphas = np.zeros(len(prices))
    for i in range(w, len(prices)):
        diff = (prices[i] - prices[i-w]) / prices[i-w] if prices[i-w] != 0 else 0
        alphas[i] = lambda_decay * diff + (1 - lambda_decay) * alphas[i-1]
    return alphas

def estimate_transition_matrix(states, k):
    P = np.zeros((k, k))
    for i in range(len(states) - 1):
        P[states[i], states[i+1]] += 1
    row_sums = P.sum(axis=1)
    for i in range(k):
        if row_sums[i] > 0: P[i, :] = P[i, :] / row_sums[i]
    return P

def evaluate_hyperparams(fuel_name, prices, w, l, k):
    """Evaluación básica para una combinación de hiperparámetros."""
    alphas = calculate_alphas(prices, w, l)
    
    # Validacion basica
    if len(alphas) < k * 2:
        return None
        
    # K-Means simplificado
    valid_alphas = alphas.reshape(-1, 1)
    from sklearn.cluster import KMeans
    kmeans = KMeans(n_clusters=k, random_state=SEED, n_init=10)
    states = kmeans.fit_predict(valid_alphas)
    
    P = estimate_transition_matrix(states, k)
    
    # Log-likelihood + AIC
    C = np.zeros((k, k))
    for i in range(len(states) - 1): C[states[i], states[i+1]] += 1
    log_likelihood = np.sum(C * np.log(P + 1e-9))
    aic = 2 * k * (k - 1) - 2 * log_likelihood
    
    return {'Combustible': fuel_name, 'W': w, 'Lambda': l, 'k': k, 'AIC': aic}

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Ejecución del Grid Search (Simplificado para Databricks)

# COMMAND ----------

# Carga de datos
df_silver = spark.table(f"{CATALOG}.silver.silver_precios_semanales").orderBy("Fecha").toPandas()

# Espacio de busqueda (Reducido ligeramente para pruebas en nube, se puede expandir)
w_range = list(range(2, 10)) 
lambda_range = np.round(np.arange(0.8, 1.01, 0.05), 2)
k_range = list(range(2, 5))

resultados = []
combustibles = ['Super', 'Regular', 'Diesel', 'Kerosene']

print("Iniciando Grid Search (Cap 5)...")
for fuel in combustibles:
    if fuel not in df_silver.columns: continue
    series = df_silver[fuel].fillna(0)
    
    for w in w_range:
        for l in lambda_range:
            for k in k_range:
                res = evaluate_hyperparams(fuel, series, w, l, k)
                if res:
                    resultados.append(res)

df_resultados = pd.DataFrame(resultados)
df_best = df_resultados.loc[df_resultados.groupby('Combustible')['AIC'].idxmin()]

print("\n--- MEJORES HIPERPARÁMETROS (Por AIC) ---")
display(df_best)

# Guardar en Delta
df_best_spark = spark.createDataFrame(df_best)
(df_best_spark.write
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.gold.tesis_cap5_best_hyperparams"))
print("Guardado en combustibles_hn.gold.tesis_cap5_best_hyperparams")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Detección de Quiebres Estructurales (Mejor Modelo)

# COMMAND ----------

def detect_structural_breaks(alphas, threshold=2.0):
    mean = np.mean(alphas)
    std = np.std(alphas)
    z_scores = (alphas - mean) / std if std > 0 else np.zeros_like(alphas)
    return np.abs(z_scores) > threshold

# Se aplicaría la detección usando los mejores hiperparámetros encontrados
print("Detección de quiebres configurada para el mejor modelo.")
