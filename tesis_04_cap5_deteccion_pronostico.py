# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Cap 5 — Detección y Pronóstico (Grid Search Completo)
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:** Optimización de hiperparámetros (Grid Search) para W, Lambda y k.
# MAGIC
# MAGIC **Salidas Gold:**
# MAGIC - `tesis_cap5_grid_completo`: TODAS las combinaciones evaluadas (para heatmaps, curvas, burbujas)
# MAGIC - `tesis_cap5_best_hyperparams`: Solo los ganadores por combustible

# COMMAND ----------

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import accuracy_score, mean_squared_error
from pyspark.sql import functions as F

CATALOG = "combustibles_hn"
spark.sql(f"USE CATALOG {CATALOG}")

# CONSTANTES DE TESIS
SEED = 42
np.random.seed(SEED)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Funciones de Evaluación

# COMMAND ----------

def calculate_alphas(series, w, lambda_decay):
    """Calcula los alphas TCRA con decaimiento exponencial."""
    prices = np.array(series.values, dtype=float)
    alphas = np.zeros(len(prices))
    for i in range(w, len(prices)):
        diff = (prices[i] - prices[i-w]) / prices[i-w] if prices[i-w] != 0 else 0
        alphas[i] = lambda_decay * diff + (1 - lambda_decay) * alphas[i-1]
    return alphas

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

def evaluate_hyperparams(fuel_name, prices_series, w, l, k):
    """Evaluación completa: AIC + Exactitud + RMSE para una combinación de hiperparámetros."""
    alphas = calculate_alphas(prices_series, w, l)
    prices = np.array(prices_series.values, dtype=float)

    if len(alphas) < k * 2:
        return None

    # K-Means
    valid_alphas = alphas.reshape(-1, 1)
    kmeans = KMeans(n_clusters=k, random_state=SEED, n_init=10)
    states = kmeans.fit_predict(valid_alphas)
    centroids = kmeans.cluster_centers_.flatten()

    # Matriz de Transición
    P = estimate_transition_matrix(states, k)

    # --- Métrica 1: AIC (Log-likelihood) ---
    C = np.zeros((k, k))
    for i in range(len(states) - 1):
        C[states[i], states[i+1]] += 1
    log_likelihood = np.sum(C * np.log(P + 1e-9))
    aic = 2 * k * (k - 1) - 2 * log_likelihood

    # --- Métrica 2 y 3: Exactitud y RMSE predictivo (validación particionada) ---
    # Usamos partición 80/20
    split_idx = int(len(alphas) * 0.8)
    if split_idx < k or (len(alphas) - split_idx) < 2:
        return {'Combustible': fuel_name, 'W': w, 'Lambda': l, 'k': k,
                'AIC': aic, 'Exactitud': 0.0, 'RMSE': 999.0}

    train_states = states[:split_idx]
    test_states = states[split_idx:]

    # Estimar P con datos de entrenamiento
    P_train = estimate_transition_matrix(train_states, k)

    # Predecir estados y precios
    predicted_states = []
    last_s = train_states[-1]
    for _ in range(len(test_states)):
        pred_s = np.argmax(P_train[last_s, :])
        predicted_states.append(pred_s)
        last_s = pred_s

    # Exactitud
    accuracy = accuracy_score(test_states, predicted_states)

    # RMSE sobre precios
    test_prices = prices[split_idx + w:]
    pred_prices = []
    for t_idx, pred_s in enumerate(predicted_states):
        actual_idx = split_idx + t_idx
        if actual_idx < len(prices):
            pred_alpha = centroids[pred_s] if pred_s < len(centroids) else 0
            pred_prices.append(prices[actual_idx] * (1 + pred_alpha))

    if len(pred_prices) > 0 and len(pred_prices) <= len(test_prices):
        rmse = np.sqrt(mean_squared_error(test_prices[:len(pred_prices)], pred_prices))
    else:
        rmse = 999.0

    return {
        'Combustible': fuel_name, 'W': w, 'Lambda': l, 'k': k,
        'AIC': aic, 'Exactitud': accuracy, 'RMSE': rmse
    }

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Ejecución del Grid Search Completo

# COMMAND ----------

# Carga de datos
df_silver = spark.table(f"{CATALOG}.silver.silver_precios_semanales").orderBy("Fecha").toPandas()

# Espacio de búsqueda
w_range = list(range(2, 10))
lambda_range = np.round(np.arange(0.5, 1.01, 0.05), 2)
k_range = list(range(2, 7))

resultados = []
combustibles = ['Super', 'Regular', 'Diesel', 'Kerosene']

total_combos = len(combustibles) * len(w_range) * len(lambda_range) * len(k_range)
print(f"Iniciando Grid Search (Cap 5): {total_combos} combinaciones totales...")

counter = 0
for fuel in combustibles:
    if fuel not in df_silver.columns:
        continue
    series = df_silver[fuel].fillna(0)

    for w in w_range:
        for l in lambda_range:
            for k in k_range:
                res = evaluate_hyperparams(fuel, series, w, l, k)
                if res:
                    resultados.append(res)
                counter += 1

    print(f"  {fuel} completado.")

print(f"Grid Search terminado: {len(resultados)} resultados válidos.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Resultados: Tabla Completa y Ganadores

# COMMAND ----------

df_resultados = pd.DataFrame(resultados)

# Ganadores por combustible (menor AIC)
df_best = df_resultados.loc[df_resultados.groupby('Combustible')['AIC'].idxmin()]

print("\n--- MEJORES HIPERPARAMETROS (Por AIC) ---")
display(df_best)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Detección de Quiebres Estructurales (Mejor Modelo)

# COMMAND ----------

def detect_structural_breaks(alphas, threshold=2.0):
    """Detecta quiebres estructurales usando z-scores."""
    mean = np.mean(alphas)
    std = np.std(alphas)
    z_scores = (alphas - mean) / std if std > 0 else np.zeros_like(alphas)
    breaks = np.where(np.abs(z_scores) > threshold)[0]
    return breaks

resultados_quiebres = []
for _, row in df_best.iterrows():
    fuel = row['Combustible']
    if fuel in df_silver.columns:
        alphas = calculate_alphas(df_silver[fuel].fillna(0), int(row['W']), row['Lambda'])
        breaks = detect_structural_breaks(alphas)
        for b in breaks:
            fecha = df_silver['Fecha'].iloc[b] if b < len(df_silver) else None
            resultados_quiebres.append({
                'Combustible': fuel,
                'Indice_Semana': int(b),
                'Fecha': str(fecha),
                'Alpha_Valor': float(alphas[b])
            })

print(f"Quiebres detectados: {len(resultados_quiebres)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Guardar en Gold

# COMMAND ----------

# 5a. Tabla COMPLETA (todas las combinaciones) — para heatmaps, curvas, burbujas
df_grid_spark = spark.createDataFrame(df_resultados)
(df_grid_spark.write
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.gold.tesis_cap5_grid_completo"))
print(f"Grid completo guardado: {len(df_resultados)} filas en tesis_cap5_grid_completo")

# 5b. Tabla GANADORES (solo el mejor por combustible)
df_best_spark = spark.createDataFrame(df_best)
(df_best_spark.write
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.gold.tesis_cap5_best_hyperparams"))
print("Mejores hiperparametros guardados en tesis_cap5_best_hyperparams")

# 5c. Quiebres estructurales
if resultados_quiebres:
    df_quiebres_spark = spark.createDataFrame(pd.DataFrame(resultados_quiebres))
    (df_quiebres_spark.write
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(f"{CATALOG}.gold.tesis_cap5_quiebres_estructurales"))
    print("Quiebres estructurales guardados en tesis_cap5_quiebres_estructurales")
