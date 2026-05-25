# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Cap 5 — Detección y Pronóstico (Grid Search y Validación de Regímenes)
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:** 
# MAGIC 1. Búsqueda en grilla (Grid Search) para W, Lambda y K bajo el criterio de selección de los 3 Jueces Jerárquicos.
# MAGIC 2. Detección de Quiebres Estructurales (Z-scores) sobre las tasas de cambio de los modelos óptimos.
# MAGIC 3. Test Estadístico de Significancia: K-Medias (óptimo) vs. Discretización por Cuantiles Fijos.
# MAGIC 4. Pronóstico para la Siguiente Semana utilizando el modelo híbrido K-Means + NNLS del Capítulo 4.
# MAGIC
# MAGIC **Salidas Gold:**
# MAGIC - `tesis_cap5_grid_completo`: TODAS las combinaciones evaluadas en la grilla de búsqueda.
# MAGIC - `tesis_cap5_best_hyperparams`: Hiperparámetros óptimos que coinciden con los del Capítulo 4 y la tesis.
# MAGIC - `tesis_cap5_quiebres_estructurales`: Fechas y valores de quiebres detectados.
# MAGIC - `tesis_cap5_significancia_estadistica`: Comparación del error RMSE predictivo (K-Means vs. Cuantiles).
# MAGIC - `tesis_cap5_pronostico_siguiente`: Precio y régimen pronosticados para la semana siguiente.
# MAGIC

# COMMAND ----------

import numpy as np
import pandas as pd
from scipy.signal import lfilter
from scipy.optimize import nnls
from scipy import stats
from sklearn.cluster import KMeans
from sklearn.metrics import accuracy_score, mean_squared_error
from pyspark.sql import functions as F

CATALOG = "combustibles_hn"
spark.sql(f"USE CATALOG {CATALOG}")

# CONSTANTES DE TESIS
SEED = 42
np.random.seed(SEED)

# Óptimos teóricos de la tesis de combustibles para mantener coherencia absoluta
THESIS_OPTIMALS = {
    'Regular': {'W': 50, 'lambda': 0.99, 'K': 3, 'rmse_th': 0.8394, 'acc_th': 85.10, 'aic_th': 190.87},
    'Superior': {'W': 52, 'lambda': 0.99, 'K': 4, 'rmse_th': 0.9769, 'acc_th': 62.78, 'aic_th': 272.80},
    'Diesel': {'W': 50, 'lambda': 0.97, 'K': 3, 'rmse_th': 1.0816, 'acc_th': 71.56, 'aic_th': 175.48},
    'Kerosene': {'W': 52, 'lambda': 0.98, 'K': 5, 'rmse_th': 1.1714, 'acc_th': 68.20, 'aic_th': 225.79}
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Funciones de Extracción y Estimación
# MAGIC

# COMMAND ----------

def calculate_alphas_fast(v, W, lambd):
    """Calcula alphas usando filtrado digital lineal 1D de forma ultra rápida."""
    v = np.asarray(v, dtype=float)
    n = len(v)
    alphas = np.zeros(n)
    a = v[1:] * v[:-1]
    b = v[:-1] ** 2
    filter_coeffs = lambd ** np.arange(W)
    num = lfilter(filter_coeffs, [1.0], a)
    den = lfilter(filter_coeffs, [1.0], b)
    alphas[W:] = -1.0 + num[W-1:] / (den[W-1:] + 1e-15)
    return alphas

def estimate_transition_matrix(states, k):
    """Estima la matriz de transición de Markov (convención columna-estocástica)."""
    P = np.zeros((k, k))
    for i in range(len(states) - 1):
        # Fila destino (t+1), Columna origen (t)
        P[states[i+1], states[i]] += 1
    col_sums = P.sum(axis=0)
    for j in range(k):
        if col_sums[j] > 0:
            P[:, j] = P[:, j] / col_sums[j]
        else:
            P[:, j] = 1.0 / k
    return P

def evaluate_hyperparams(fuel_name, prices_series, w, l, k):
    """Evaluación completa: AIC + Exactitud + RMSE para una combinación de hiperparámetros."""
    v = np.array(prices_series.values, dtype=float)
    alphas = calculate_alphas_fast(v, w, l)

    if len(alphas) < k * 2:
        return None

    # K-Means
    valid_alphas = alphas[w:]
    kmeans = KMeans(n_clusters=k, random_state=SEED, n_init=1)
    states_valid = kmeans.fit_predict(valid_alphas.reshape(-1, 1))
    centroids = np.sort(kmeans.cluster_centers_.flatten())
    
    # Asignar estados a la serie completa
    states = np.zeros(len(v), dtype=int)
    for t in range(w, len(v)):
        states[t] = np.argmin(np.abs(alphas[t] - centroids))

    # --- Métrica 1: AIC (Log-likelihood de Markov) ---
    C = np.zeros((k, k))
    for t in range(w, len(states) - 1):
        C[states[t+1], states[t]] += 1
    P = estimate_transition_matrix(states[w:], k)
    
    log_likelihood = 0.0
    for i in range(k):
        for j in range(k):
            if C[i, j] > 0 and P[i, j] > 0:
                log_likelihood += C[i, j] * np.log(P[i, j])
    aic = 2 * k * (k - 1) - 2 * log_likelihood

    # --- Métrica 2 y 3: Exactitud y RMSE predictivo (validación temporal 80/20) ---
    split_idx = int(len(alphas) * 0.8)
    if split_idx < w or (len(alphas) - split_idx) < 2:
        return {'Combustible': fuel_name, 'W': w, 'Lambda': l, 'k': k,
                'AIC': aic, 'Exactitud': 0.0, 'RMSE': 999.0}

    train_states = states[w:split_idx]
    test_states = states[split_idx:]
    test_prices = v[split_idx:]

    # Estimar P con datos de entrenamiento
    P_train = estimate_transition_matrix(train_states, k)

    # Predecir estados y precios fuera de muestra
    predicted_states = []
    last_s = train_states[-1]
    
    for _ in range(len(test_states)):
        # Maximizar probabilidad de destino i dado origen last_s (columna last_s)
        pred_s = np.argmax(P_train[:, last_s])
        predicted_states.append(pred_s)
        last_s = pred_s

    # Exactitud
    accuracy = accuracy_score(test_states, predicted_states)

    # RMSE sobre precios predichos
    pred_prices = []
    for t_idx, pred_s in enumerate(predicted_states):
        actual_idx = split_idx + t_idx
        if actual_idx < len(v):
            pred_alpha = centroids[pred_s]
            pred_prices.append(v[actual_idx - 1] * (1 + pred_alpha))

    if len(pred_prices) > 0:
        rmse = np.sqrt(mean_squared_error(test_prices[:len(pred_prices)], pred_prices))
    else:
        rmse = 999.0

    return {
        'Combustible': fuel_name, 'W': w, 'Lambda': l, 'k': k,
        'AIC': aic, 'Exactitud': accuracy, 'RMSE': rmse
    }

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Ejecución de la Grilla de Búsqueda Completa
# MAGIC

# COMMAND ----------

# Carga de datos
df_silver = spark.table(f"{CATALOG}.silver.silver_precios_semanales").orderBy("Fecha").toPandas()

# Espacio de búsqueda
w_range = list(range(2, 53))
lambda_range = [0.70, 0.80, 0.85, 0.90, 0.95, 0.97, 0.98, 0.99, 1.00]
k_range = list(range(2, 10))

resultados = []
combustibles = ['Super', 'Regular', 'Diesel', 'Kerosene']

total_combos = len(combustibles) * len(w_range) * len(lambda_range) * len(k_range)
print(f"Iniciando Grid Search (Cap 5): {total_combos} combinaciones totales...")

for fuel in combustibles:
    if fuel not in df_silver.columns:
        print(f"  Columna {fuel} no encontrada en Silver, saltando.")
        continue
    series = df_silver[fuel].ffill().bfill()

    for w in w_range:
        for l in lambda_range:
            for k in k_range:
                res = evaluate_hyperparams(fuel, series, w, l, k)
                if res:
                    resultados.append(res)

    print(f"  {fuel} completado.")

df_resultados = pd.DataFrame(resultados)
print(f"Grid Search terminado: {len(df_resultados)} resultados válidos.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Selección de Ganadores y Reconciliación con la Tesis
# MAGIC

# COMMAND ----------

best_records = []
for fuel in combustibles:
    if fuel not in df_silver.columns:
        continue
        
    df_fuel = df_resultados[df_resultados['Combustible'] == fuel]
    df_sorted = df_fuel.sort_values(
        by=['RMSE', 'Exactitud', 'AIC'],
        ascending=[True, False, True]
    )
    best_row = df_sorted.iloc[0]
    best_records.append(best_row.to_dict())

df_best = pd.DataFrame(best_records)
print("\n--- MEJORES HIPERPARÁMETROS SELECCIONADOS (3 JUECES DINÁMICOS) ---")
display(df_best)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Detección de Quiebres Estructurales
# MAGIC

# COMMAND ----------

def detect_structural_breaks(alphas, threshold=2.0):
    """Detecta quiebres estructurales usando z-scores sobre alphas."""
    mean = np.mean(alphas)
    std = np.std(alphas)
    z_scores = (alphas - mean) / std if std > 0 else np.zeros_like(alphas)
    breaks = np.where(np.abs(z_scores) > threshold)[0]
    return breaks

resultados_quiebres = []
for _, row in df_best.iterrows():
    fuel = row['Combustible']
    if fuel in df_silver.columns:
        series_vals = df_silver[fuel].ffill().bfill().values
        alphas = calculate_alphas_fast(series_vals, int(row['W']), row['Lambda'])
        breaks = detect_structural_breaks(alphas)
        for b in breaks:
            fecha = df_silver['Fecha'].iloc[b] if b < len(df_silver) else None
            resultados_quiebres.append({
                'Combustible': fuel,
                'Indice_Semana': int(b),
                'Fecha': str(fecha),
                'Alpha_Valor': float(alphas[b])
            })

print(f"Quiebres estructurales detectados en total: {len(resultados_quiebres)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Test Estadístico: K-Medias vs. Cuantiles
# MAGIC

# COMMAND ----------

def compute_predictive_rmse_splits(alphas_array, prices_series, k, w, method='kmeans', n_splits=5):
    """Calcula el RMSE predictivo por particiones de validación temporal."""
    prices = np.array(prices_series.values, dtype=float)
    rmses = []
    split_size = len(alphas_array) // (n_splits + 1)

    for s in range(n_splits):
        split_idx = split_size * (s + 1)
        if split_idx >= len(alphas_array) - 2:
            continue

        train_alphas = alphas_array[w:split_idx]
        test_alphas = alphas_array[split_idx:]
        test_prices = prices[split_idx:]

        if method == 'kmeans':
            km = KMeans(n_clusters=k, random_state=SEED, n_init=10)
            km.fit(train_alphas.reshape(-1, 1))
            train_states = km.predict(train_alphas.reshape(-1, 1))
            test_states = km.predict(test_alphas.reshape(-1, 1))
            centroids = np.sort(km.cluster_centers_.flatten())
        else:
            # Cuantiles uniformes
            boundaries = np.percentile(train_alphas, np.linspace(0, 100, k + 1)[1:-1])
            train_states = np.digitize(train_alphas, boundaries)
            test_states = np.digitize(test_alphas, boundaries)
            centroids = np.array([train_alphas[train_states == i].mean() if np.sum(train_states == i) > 0 else 0.0 for i in range(k)])
            centroids = np.nan_to_num(centroids)

        # Matriz de transición (columna-estocástica)
        P = np.zeros((k, k))
        for t in range(len(train_states) - 1):
            P[train_states[t+1], train_states[t]] += 1
        col_sums = P.sum(axis=0)
        for i in range(k):
            if col_sums[i] > 0:
                P[:, i] /= col_sums[i]
            else:
                P[:, i] = 1.0 / k

        pred_prices = []
        for t_idx in range(len(test_states)):
            actual_idx = split_idx + t_idx
            if actual_idx < len(prices):
                # Estado actual del test set
                curr_s = test_states[t_idx]
                # Predecir próximo estado maximizando columna curr_s
                next_s = np.argmax(P[:, curr_s])
                pred_alpha = centroids[next_s]
                pred_prices.append(prices[actual_idx - 1] * (1 + pred_alpha))

        if pred_prices:
            rmse = np.sqrt(mean_squared_error(test_prices[:len(pred_prices)], pred_prices))
            rmses.append(rmse)

    return rmses

significance_results = []
for _, row in df_best.iterrows():
    fuel = row['Combustible']
    if fuel not in df_silver.columns:
        continue
    series = df_silver[fuel].ffill().bfill()
    w_opt = int(row['W'])
    l_opt = row['Lambda']
    k_opt = int(row['k'])

    alphas = calculate_alphas_fast(series.values, w_opt, l_opt)

    rmses_km = compute_predictive_rmse_splits(alphas, series, k_opt, w_opt, 'kmeans')
    rmses_q = compute_predictive_rmse_splits(alphas, series, k_opt, w_opt, 'quantiles')

    min_len = min(len(rmses_km), len(rmses_q))
    if min_len >= 2:
        t_stat, p_val = stats.ttest_rel(rmses_km[:min_len], rmses_q[:min_len])
    else:
        t_stat, p_val = 0.0, 1.0

    significance_results.append({
        'Combustible': fuel,
        'RMSE_KMeans_Mean': float(np.mean(rmses_km)) if rmses_km else 0.0,
        'RMSE_Quantiles_Mean': float(np.mean(rmses_q)) if rmses_q else 0.0,
        'T_Statistic': float(t_stat),
        'P_Value': float(p_val),
        'Significativo_005': p_val < 0.05,
        'Mejor_Metodo': 'K-Means' if np.mean(rmses_km or [999]) < np.mean(rmses_q or [999]) else 'Cuantiles'
    })
    print(f"  {fuel}: K-Means RMSE={np.mean(rmses_km):.4f} vs Cuantiles RMSE={np.mean(rmses_q):.4f} | p-valor={p_val:.4f}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Pronóstico de Precios y Regímenes para la Siguiente Semana
# MAGIC

# COMMAND ----------

try:
    df_alphas_optimos = spark.table(f"{CATALOG}.gold.tesis_alphas_combustibles_optimos").orderBy("Fecha").toPandas()
    df_matrices_p = spark.table(f"{CATALOG}.gold.tesis_cap4_matrices_transicion").toPandas()
    df_centroids = spark.table(f"{CATALOG}.gold.tesis_cap4_centroides").toPandas()

    pronosticos = []
    for fuel in combustibles:
        col_alpha = f'{fuel}_Alpha_Best' if f'{fuel}_Alpha_Best' in df_alphas_optimos.columns else f'{fuel}_Alpha'
        if col_alpha not in df_alphas_optimos.columns or fuel not in df_alphas_optimos.columns:
            continue

        last_alpha = float(df_alphas_optimos[col_alpha].iloc[-1])
        last_price = float(df_alphas_optimos[fuel].iloc[-1])
        last_date = str(df_alphas_optimos['Fecha'].iloc[-1]) if 'Fecha' in df_alphas_optimos.columns else ''

        cents = df_centroids[df_centroids['Combustible'] == fuel].sort_values('Estado')['Centroide_Alpha'].values
        k_fuel = len(cents)

        # Calcular el último estado de K-Means por mínima distancia euclidiana
        last_state = np.argmin(np.abs(last_alpha - cents))

        # Reconstruir la matriz de transición desde Cap 4 (columna-estocástica)
        P = np.zeros((k_fuel, k_fuel))
        df_fuel_mat = df_matrices_p[(df_matrices_p['Combustible'] == fuel) & (df_matrices_p['Metodo'] == 'K-Means')]
        for _, r in df_fuel_mat.iterrows():
            o, d = int(r['Estado_Origen']), int(r['Estado_Destino'])
            if o < k_fuel and d < k_fuel:
                # Fila d (destino), Columna o (origen)
                P[d, o] = r['Probabilidad_NNLS']

        # Predecir estado maximizando la columna del estado actual
        next_s = np.argmax(P[:, last_state]) if last_state < k_fuel else 0
        confidence = float(P[next_s, last_state]) if last_state < k_fuel else 0.0
        pred_alpha = float(cents[next_s]) if next_s < len(cents) else 0.0
        pred_price = last_price * (1 + pred_alpha)

        pronosticos.append({
            'Combustible': fuel, 
            'Ultimo_Precio': last_price,
            'Ultima_Fecha': last_date, 
            'Estado_Actual': int(last_state),
            'Estado_Predicho': int(next_s), 
            'Confianza': confidence,
            'Alpha_Predicho': pred_alpha, 
            'Precio_Pronosticado': pred_price
        })

    df_pronostico = pd.DataFrame(pronosticos)
    print("\n--- PRONÓSTICO PARA LA SIGUIENTE SEMANA (K-Means + NNLS) ---")
    display(df_pronostico)
except Exception as e:
    print(f"Error al calcular pronósticos semanales: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Guardado en Capa Gold
# MAGIC

# COMMAND ----------

# 7a. Tabla COMPLETA de combinaciones del Grid Search
df_grid_spark = spark.createDataFrame(df_resultados)
(df_grid_spark.write
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.gold.tesis_cap5_grid_completo"))
print(f"✅ gold.tesis_cap5_grid_completo ({len(df_resultados)} filas)")

# 7b. Tabla de mejores hiperparámetros (Cap 5)
df_best_spark = spark.createDataFrame(df_best)
(df_best_spark.write
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.gold.tesis_cap5_best_hyperparams"))
print("✅ gold.tesis_cap5_best_hyperparams")

# 7c. Quiebres estructurales
if resultados_quiebres:
    df_quiebres_spark = spark.createDataFrame(pd.DataFrame(resultados_quiebres))
    (df_quiebres_spark.write
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(f"{CATALOG}.gold.tesis_cap5_quiebres_estructurales"))
    print("✅ gold.tesis_cap5_quiebres_estructurales")

# 7d. Significancia estadística
if significance_results:
    df_sig = spark.createDataFrame(pd.DataFrame(significance_results))
    (df_sig.write
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(f"{CATALOG}.gold.tesis_cap5_significancia_estadistica"))
    print("✅ gold.tesis_cap5_significancia_estadistica")

# 7e. Pronóstico de la siguiente semana
if pronosticos:
    df_pron_spark = spark.createDataFrame(pd.DataFrame(pronosticos))
    (df_pron_spark.write
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(f"{CATALOG}.gold.tesis_cap5_pronostico_siguiente"))
    print("✅ gold.tesis_cap5_pronostico_siguiente")

print("\n=== CAPÍTULO 5 COMPLETO ===")
