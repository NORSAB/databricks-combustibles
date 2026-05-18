# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Cap 6 — Extensión Reservorio SSRC (Grid Search Completo)
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:** Optimización y validación del modelo de Computación de Reservorio Estocástico (SSRC).
# MAGIC
# MAGIC **Salidas Gold:**
# MAGIC - `tesis_cap6_grid_completo`: TODAS las configuraciones (D, rho, leak) evaluadas
# MAGIC - `tesis_cap6_best_ssrc`: Solo los ganadores por combustible
# MAGIC - `tesis_cap6_predicciones_semanales`: Predicciones Real vs Markov vs SSRC por semana
# MAGIC - `tesis_cap6_comparacion_final`: Barplot comparativo Markov vs SSRC

# COMMAND ----------

import numpy as np
import pandas as pd
from scipy.optimize import nnls
from sklearn.metrics import mean_squared_error
from pyspark.sql import functions as F

CATALOG = "combustibles_hn"
spark.sql(f"USE CATALOG {CATALOG}")

# CONSTANTES SSRC
SEED = 42
np.random.seed(SEED)

RESERVOIR_D_VALUES = [10, 20, 50, 100]
RESERVOIR_RHO_VALUES = [0.5, 0.7, 0.8, 0.9, 0.95, 0.99]
RESERVOIR_LEAK_RATES = [0.3, 0.5, 0.8, 1.0]
RESERVOIR_WASHOUT = 50
TRAIN_RATIO = 0.8

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Funciones del Reservorio

# COMMAND ----------

def create_reservoir(input_dim, reservoir_dim, spectral_radius, sparsity=0.9, seed=SEED):
    """Crea el reservorio con pesos aleatorios y radio espectral controlado."""
    rng = np.random.RandomState(seed)
    W_in = rng.uniform(-1, 1, (reservoir_dim, input_dim))

    W_res = rng.uniform(-1, 1, (reservoir_dim, reservoir_dim))
    # Aplicar sparsity
    mask = rng.rand(reservoir_dim, reservoir_dim) < sparsity
    W_res[mask] = 0

    # Ajustar radio espectral
    eigenvalues = np.linalg.eigvals(W_res)
    max_eigenvalue = np.max(np.abs(eigenvalues))
    if max_eigenvalue > 0:
        W_res = W_res * (spectral_radius / max_eigenvalue)

    return W_in, W_res

def propagate_reservoir(alphas, W_in, W_res, washout=50, leak_rate=1.0):
    """Propaga la señal a través del reservorio con leak rate."""
    D = W_res.shape[0]
    T = len(alphas)
    H = np.zeros((D, T))
    h = np.zeros(D)
    W_in_col = W_in.ravel()

    for t in range(T):
        pre = W_in_col * alphas[t] + W_res @ h
        h = (1.0 - leak_rate) * h + leak_rate * np.tanh(pre)
        H[:, t] = h

    # Descartar washout
    return H[:, washout:]

def evaluate_ssrc(fuel_name, alphas, prices, D, rho, leak_rate, seed, train_size):
    """Evaluación completa de una configuración del reservorio."""
    W_in, W_res = create_reservoir(1, D, rho, sparsity=0.9, seed=seed)
    H_full = propagate_reservoir(alphas, W_in, W_res, washout=RESERVOIR_WASHOUT, leak_rate=leak_rate)

    # Alinear señales
    alphas_eff = alphas[RESERVOIR_WASHOUT:]
    prices_eff = prices[RESERVOIR_WASHOUT:]

    T_eff = H_full.shape[1]
    if T_eff < 10:
        return None, None, None

    # Train/Test split
    train_end = min(train_size - RESERVOIR_WASHOUT, T_eff - 2)
    if train_end < 5:
        return None, None, None

    # Readout NNLS
    targets_train = alphas_eff[1:train_end + 1]
    H_train = H_full[:, :train_end]

    try:
        W_out, _ = nnls(H_train.T, targets_train)
    except Exception:
        return None, None, None

    # Predicciones sobre test
    test_start = train_end
    test_end = T_eff - 1

    actual_prices = []
    ssrc_predicted_prices = []
    weeks = []

    for t in range(test_start, test_end):
        alpha_pred = W_out @ H_full[:, t]
        pred_price = prices_eff[t] * (1 + alpha_pred)
        ssrc_predicted_prices.append(pred_price)
        actual_prices.append(prices_eff[t + 1])
        weeks.append(t - test_start + 1)

    if len(actual_prices) < 2:
        return None, None, None

    rmse = np.sqrt(mean_squared_error(actual_prices, ssrc_predicted_prices))

    result = {
        'Combustible': fuel_name, 'D': D, 'rho': rho,
        'leak_rate': leak_rate, 'RMSE': rmse
    }

    predictions = {
        'weeks': weeks,
        'actual': actual_prices,
        'ssrc_pred': ssrc_predicted_prices
    }

    return result, predictions, W_out

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Carga de Datos

# COMMAND ----------

try:
    df_alphas = spark.table(f"{CATALOG}.gold.tesis_alphas_con_estados").toPandas()
    df_markov_rmse = spark.table(f"{CATALOG}.gold.tesis_cap4_rmse_markov_base").toPandas()
    df_centroides = spark.table(f"{CATALOG}.gold.tesis_cap3_centroides").toPandas()
    df_matrices = spark.table(f"{CATALOG}.gold.tesis_cap3_matrices_transicion").toPandas()
    print("Datos de Cap 2, 3 y 4 cargados correctamente.")
except Exception as e:
    print(f"ERROR: {e}")
    print("Ejecuta los notebooks anteriores primero.")
    dbutils.notebook.exit("Faltan dependencias")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Grid Search SSRC Completo

# COMMAND ----------

resultados_ssrc = []
mejores_predicciones = {}
combustibles = ['Super', 'Regular', 'Diesel', 'Kerosene']
train_size = int(len(df_alphas) * TRAIN_RATIO)

total_configs = len(combustibles) * len(RESERVOIR_D_VALUES) * len(RESERVOIR_RHO_VALUES) * len(RESERVOIR_LEAK_RATES)
print(f"Iniciando Grid Search SSRC (Cap 6): {total_configs} configuraciones totales...")

for fuel in combustibles:
    col_alpha = f"{fuel}_Alpha"
    col_price = fuel

    if col_alpha not in df_alphas.columns or col_price not in df_alphas.columns:
        print(f"  ADVERTENCIA: {fuel} no disponible, saltando.")
        continue

    alphas = np.array(df_alphas[col_alpha].fillna(0).values, dtype=float)
    prices = np.array(df_alphas[col_price].fillna(0).values, dtype=float)

    best_rmse_fuel = 999.0

    for D in RESERVOIR_D_VALUES:
        for rho in RESERVOIR_RHO_VALUES:
            for leak in RESERVOIR_LEAK_RATES:
                res, preds, W_out = evaluate_ssrc(fuel, alphas, prices, D, rho, leak, SEED, train_size)
                if res:
                    resultados_ssrc.append(res)
                    if res['RMSE'] < best_rmse_fuel:
                        best_rmse_fuel = res['RMSE']
                        mejores_predicciones[fuel] = preds

    print(f"  {fuel} completado. Mejor RMSE SSRC: {best_rmse_fuel:.6f}")

print(f"Grid Search terminado: {len(resultados_ssrc)} resultados validos.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Resultados y Comparación Markov vs SSRC

# COMMAND ----------

df_res_ssrc = pd.DataFrame(resultados_ssrc)
df_best_ssrc = df_res_ssrc.loc[df_res_ssrc.groupby('Combustible')['RMSE'].idxmin()]

print("\n--- MEJORES CONFIGURACIONES SSRC (Menor RMSE) ---")
display(df_best_ssrc)

# Construir predicciones semanales: Real vs Markov vs SSRC
resultados_predicciones = []
k_optimo = 4

for fuel in combustibles:
    if fuel not in mejores_predicciones:
        continue

    preds = mejores_predicciones[fuel]
    col_state = f'{fuel}_State'
    col_price = fuel

    # Obtener centroides y P para predicciones Markov
    df_fc = df_centroides[df_centroides['Combustible'] == fuel].sort_values('Estado')
    centroids = df_fc['Centroide_Alpha'].values

    P = np.zeros((k_optimo, k_optimo))
    df_fm = df_matrices[df_matrices['Combustible'] == fuel]
    for _, row in df_fm.iterrows():
        P[int(row['Estado_Origen']), int(row['Estado_Destino'])] = row['Probabilidad']

    prices_full = np.array(df_alphas[col_price].fillna(0).values, dtype=float)
    states_full = df_alphas[col_state].values

    # Markov predictions en el mismo rango
    test_start = int(len(prices_full) * TRAIN_RATIO)
    for w_idx, (week, actual, ssrc_pred) in enumerate(zip(preds['weeks'], preds['actual'], preds['ssrc_pred'])):
        t = test_start + RESERVOIR_WASHOUT + w_idx
        if t < len(states_full) and t < len(prices_full):
            current_s = int(states_full[t])
            next_s = np.argmax(P[current_s, :])
            markov_alpha = centroids[next_s] if next_s < len(centroids) else 0
            markov_pred = prices_full[t] * (1 + markov_alpha)
        else:
            markov_pred = actual

        resultados_predicciones.append({
            'Combustible': fuel,
            'Semana': week,
            'Precio_Real': float(actual),
            'Prediccion_Markov': float(markov_pred),
            'Prediccion_SSRC': float(ssrc_pred)
        })

# Tabla comparativa final
comparacion = []
for fuel in combustibles:
    ssrc_row = df_best_ssrc[df_best_ssrc['Combustible'] == fuel]
    markov_row = df_markov_rmse[df_markov_rmse['Combustible'] == fuel]
    if not ssrc_row.empty and not markov_row.empty:
        rmse_m = markov_row.iloc[0]['RMSE_Markov']
        rmse_s = ssrc_row.iloc[0]['RMSE']
        delta = (rmse_s - rmse_m) / rmse_m * 100
        comparacion.append({
            'Combustible': fuel,
            'RMSE_Markov': float(rmse_m),
            'RMSE_SSRC': float(rmse_s),
            'Delta_Porcentaje': float(delta),
            'Mejor_Modelo': 'SSRC' if delta < 0 else 'Markov'
        })

if comparacion:
    print("\n--- COMPARACION FINAL: Markov vs SSRC ---")
    display(pd.DataFrame(comparacion))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Guardar en Gold

# COMMAND ----------

# 5a. Grid COMPLETO (todas las configuraciones)
df_grid_ssrc_spark = spark.createDataFrame(df_res_ssrc)
(df_grid_ssrc_spark.write
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.gold.tesis_cap6_grid_completo"))
print(f"Grid completo guardado: {len(df_res_ssrc)} filas en tesis_cap6_grid_completo")

# 5b. Mejores configuraciones
df_best_ssrc_spark = spark.createDataFrame(df_best_ssrc)
(df_best_ssrc_spark.write
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.gold.tesis_cap6_best_ssrc"))
print("Mejores SSRC guardados en tesis_cap6_best_ssrc")

# 5c. Predicciones semanales (Real vs Markov vs SSRC)
if resultados_predicciones:
    df_preds_spark = spark.createDataFrame(pd.DataFrame(resultados_predicciones))
    (df_preds_spark.write
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(f"{CATALOG}.gold.tesis_cap6_predicciones_semanales"))
    print(f"Predicciones semanales guardadas: {len(resultados_predicciones)} filas")

# 5d. Comparación final
if comparacion:
    df_comp_spark = spark.createDataFrame(pd.DataFrame(comparacion))
    (df_comp_spark.write
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(f"{CATALOG}.gold.tesis_cap6_comparacion_final"))
    print("Comparacion final guardada en tesis_cap6_comparacion_final")

print("\n=== CAPITULO 6 COMPLETO ===")
