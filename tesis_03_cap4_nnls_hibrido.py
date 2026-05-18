# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Cap 4 — Modelo Híbrido TCROC-NNLS
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:** Validación predictiva NNLS + RMSE Markov base + Tabla 4.3 (umbrales fijos).
# MAGIC
# MAGIC **Salidas Gold:**
# MAGIC - `tesis_cap4_predicciones_nnls`: Vector de probabilidades NNLS por estado
# MAGIC - `tesis_cap4_rmse_markov_base`: RMSE Markov base (benchmark para Cap 6)
# MAGIC - `tesis_cap4_predicciones_precio_semanal`: Precio predicho vs real por semana
# MAGIC - `tesis_cap4_tabla_4_3`: Rendimiento predictivo P̂ vs Ap por partición (umbrales fijos)

# COMMAND ----------

import numpy as np
import pandas as pd
from scipy.optimize import nnls
from sklearn.metrics import mean_squared_error
from numpy import kron, identity, ones, zeros, diag
from pyspark.sql import functions as F

CATALOG = "combustibles_hn"
spark.sql(f"USE CATALOG {CATALOG}")

SEED = 42
np.random.seed(SEED)

# Umbrales fijos del Capítulo 4 original
THRESHOLDS = [-0.01, 0.02, 0.04]
STATE_NAMES = ["Caída", "Estable", "Subida", "Alza fuerte"]
K = 4
W_CAP4 = 2
LAMBDA_CAP4 = 1.0
TRAIN_RATIOS = [0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Carga de Dependencias

# COMMAND ----------

try:
    df_matrices = spark.table(f"{CATALOG}.gold.tesis_cap3_matrices_transicion").toPandas()
    df_centroides = spark.table(f"{CATALOG}.gold.tesis_cap3_centroides").toPandas()
    df_alphas = spark.table(f"{CATALOG}.gold.tesis_alphas_con_estados").toPandas()
    print("Datos cargados correctamente desde Cap 3.")
except Exception as e:
    print(f"ERROR: {e}")
    dbutils.notebook.exit("Faltan dependencias — ejecuta tesis_02 primero")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Funciones NNLS y Cap 4

# COMMAND ----------

def predict_next_state_nnls(P_matrix, current_state_vector):
    A = P_matrix.T
    b = current_state_vector
    x, residual = nnls(A, b)
    if np.sum(x) > 0:
        x = x / np.sum(x)
    return x, residual

def calcular_alphas_cap4(serie, W_val=W_CAP4, lambda_=LAMBDA_CAP4):
    """Calcula alpha_t con la fórmula EXACTA del Cap 4 (WLS ponderado)."""
    alphas = []
    for t in range(W_val, len(serie)):
        numerador = 0.0
        denominador = 0.0
        for tau in range(t - W_val + 1, t + 1):
            weight = lambda_ ** (t - tau)
            numerador += weight * serie[tau] * serie[tau - 1]
            denominador += weight * serie[tau - 1] ** 2
        alpha_t = -1 + numerador / denominador if denominador != 0 else 0
        alphas.append(alpha_t)
    return alphas

def asignar_estado_fijo(alpha):
    """Discretiza alpha con umbrales fijos (Cap 4)."""
    if alpha < THRESHOLDS[0]: return 0
    elif alpha <= THRESHOLDS[1]: return 1
    elif alpha <= THRESHOLDS[2]: return 2
    else: return 3

def estimate_P_from_C(C):
    """Estima P (MLE) normalizando columnas de C."""
    col_sums = C.sum(axis=0)
    col_sums[col_sums == 0] = 1
    return C / col_sums

def SRep(datos, ss):
    """Estimación NNLS/SRep (Ap). Replica notebook Cap 4."""
    n0 = datos.shape[0]
    S0 = datos[:, :ss]
    S0 = kron(S0, identity(n0)).T
    S1 = S0.T @ ((datos[:, 1:(1 + ss)]).T).reshape(ss * n0)
    C_mat = kron(identity(n0), ones((1, n0)))
    Mr = zeros((n0**2 + n0, n0**2))
    Mr[:n0**2, :] = S0.T @ S0
    Mr[n0**2:, :] = C_mat
    rhs = zeros((n0**2 + n0))
    rhs[:n0**2] = S1
    rhs[n0**2:] = 1
    c = zeros((n0**2, 1))
    c[:, 0] = nnls(Mr, rhs)[0]
    Pr = c.reshape(n0, n0).T
    Pr = Pr @ diag(1 / np.sum(Pr, axis=0))
    return Pr

def compute_Ap(P):
    """Calcula Ap via simulación + SRep."""
    p0 = zeros((K, 100))
    p0[0, 0] = 1
    for j in range(99):
        p0[:, j + 1] = P @ p0[:, j]
    return SRep(p0, ss=K)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Parte A: Predicciones NNLS + RMSE Markov Base (K-Medias)

# COMMAND ----------

resultados_nnls = []
resultados_markov_rmse = []
resultados_precios_semanales = []
combustibles = df_matrices['Combustible'].unique()
k_optimo = 4

for fuel in combustibles:
    P = np.zeros((k_optimo, k_optimo))
    df_fuel = df_matrices[df_matrices['Combustible'] == fuel]
    for _, row in df_fuel.iterrows():
        P[int(row['Estado_Origen']), int(row['Estado_Destino'])] = row['Probabilidad']

    df_fuel_centroides = df_centroides[df_centroides['Combustible'] == fuel].sort_values('Estado')
    centroids = df_fuel_centroides['Centroide_Alpha'].values

    # NNLS desde cada estado inicial
    for s in range(k_optimo):
        curr_state = np.zeros(k_optimo)
        curr_state[s] = 1.0
        next_state, residual = predict_next_state_nnls(P, curr_state)
        for i, prob in enumerate(next_state):
            resultados_nnls.append({
                'Combustible': fuel, 'Estado_Actual': s, 'Estado_Predicho': i,
                'Probabilidad_NNLS': float(prob), 'Residual_NNLS': float(residual)
            })

    # RMSE Markov base + predicciones semanales
    col_state = f'{fuel}_State'
    col_price = fuel
    if col_state in df_alphas.columns and col_price in df_alphas.columns:
        prices = np.array(df_alphas[col_price].fillna(0).values, dtype=float)
        states = df_alphas[col_state].values
        fechas = df_alphas['Fecha'].values if 'Fecha' in df_alphas.columns else range(len(states))

        actual_prices, predicted_prices = [], []
        for t in range(len(states) - 1):
            current_s = int(states[t])
            next_s = np.argmax(P[current_s, :])
            pred_alpha = centroids[next_s] if next_s < len(centroids) else 0
            pred_price = prices[t] * (1 + pred_alpha)
            actual_prices.append(prices[t + 1])
            predicted_prices.append(pred_price)

            resultados_precios_semanales.append({
                'Combustible': fuel, 'Semana': t + 1,
                'Fecha': str(fechas[t + 1]) if t + 1 < len(fechas) else '',
                'Precio_Real': float(prices[t + 1]),
                'Precio_Predicho_Markov': float(pred_price),
                'Estado_Actual': int(current_s), 'Estado_Predicho': int(next_s)
            })

        if actual_prices:
            rmse_markov = np.sqrt(mean_squared_error(actual_prices, predicted_prices))
            resultados_markov_rmse.append({
                'Combustible': fuel, 'RMSE_Markov': float(rmse_markov),
                'N_Predicciones': len(actual_prices)
            })
            print(f"{fuel}: RMSE Markov Base = {rmse_markov:.6f}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Parte B: Tabla 4.3 (Umbrales Fijos, P̂ vs Ap)

# COMMAND ----------

print("\n--- Tabla 4.3: Rendimiento predictivo con umbrales fijos ---")
print(f"Parámetros: W={W_CAP4}, lambda={LAMBDA_CAP4}, K={K}, umbrales={THRESHOLDS}")

tabla_4_3 = []
detalle_particiones = []

for fuel in combustibles:
    col_price = fuel
    if col_price not in df_alphas.columns:
        continue

    series = np.array(df_alphas[col_price].fillna(0).values, dtype=float)
    alphas = calcular_alphas_cap4(series, W_CAP4, LAMBDA_CAP4)
    states_full = [asignar_estado_fijo(a) for a in alphas]

    # Matrices completas (100%)
    X0 = np.zeros((K, len(states_full) - 1))
    X1 = np.zeros((K, len(states_full) - 1))
    for t in range(len(states_full) - 1):
        X0[states_full[t], t] = 1
        X1[states_full[t + 1], t] = 1
    C_full = X1 @ X0.T
    P_full = estimate_P_from_C(C_full)

    try:
        Ap_full = compute_Ap(P_full)
    except:
        Ap_full = P_full

    accs_P, accs_Ap = [], []
    for ratio in TRAIN_RATIOS:
        split_idx = int(len(states_full) * ratio)
        states_train = states_full[:split_idx]
        states_test = states_full[split_idx:]
        if len(states_test) < 2:
            continue

        # Matrices de entrenamiento
        X0t = np.zeros((K, len(states_train) - 1))
        X1t = np.zeros((K, len(states_train) - 1))
        for t in range(len(states_train) - 1):
            X0t[states_train[t], t] = 1
            X1t[states_train[t + 1], t] = 1
        C_train = X1t @ X0t.T
        P_train = estimate_P_from_C(C_train)
        try:
            Ap_train = compute_Ap(P_train)
        except:
            Ap_train = P_train

        # Accuracy con P̂ y con Ap
        correct_P, correct_Ap, n_pred = 0, 0, 0
        for i in range(len(states_test) - 1):
            current_idx = states_test[i]
            actual_next = states_test[i + 1]

            pred_P = np.argmax(P_train[:, current_idx])
            pred_Ap = np.argmax(Ap_train[:, current_idx])

            if pred_P == actual_next: correct_P += 1
            if pred_Ap == actual_next: correct_Ap += 1
            n_pred += 1

        if n_pred > 0:
            acc_P = round(correct_P / n_pred * 100, 1)
            acc_Ap = round(correct_Ap / n_pred * 100, 1)
            accs_P.append(acc_P)
            accs_Ap.append(acc_Ap)

            pct_train = int(ratio * 100)
            detalle_particiones.append({
                'Combustible': fuel, 'Particion': f"{pct_train}/{100-pct_train}",
                'N_Predicciones': n_pred,
                'Accuracy_P_hat': acc_P, 'Accuracy_Ap': acc_Ap
            })

    if accs_P:
        tabla_4_3.append({
            'Combustible': fuel,
            'Max_Acc_P_hat': max(accs_P), 'Avg_Acc_P_hat': round(np.mean(accs_P), 1),
            'Max_Acc_Ap': max(accs_Ap), 'Avg_Acc_Ap': round(np.mean(accs_Ap), 1)
        })
        print(f"  {fuel}: P̂ MAX={max(accs_P)}% AVG={np.mean(accs_P):.1f}% | Ap MAX={max(accs_Ap)}% AVG={np.mean(accs_Ap):.1f}%")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Guardar en Gold

# COMMAND ----------

# 5a. Predicciones NNLS
spark.createDataFrame(pd.DataFrame(resultados_nnls)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap4_predicciones_nnls")
print("✅ tesis_cap4_predicciones_nnls")

# 5b. RMSE Markov Base
spark.createDataFrame(pd.DataFrame(resultados_markov_rmse)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap4_rmse_markov_base")
print("✅ tesis_cap4_rmse_markov_base")

# 5c. Predicciones de precio semanal (para figura allseriecombustiblesNNLS)
spark.createDataFrame(pd.DataFrame(resultados_precios_semanales)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap4_predicciones_precio_semanal")
print(f"✅ tesis_cap4_predicciones_precio_semanal ({len(resultados_precios_semanales)} filas)")

# 5d. Tabla 4.3 (rendimiento por combustible)
if tabla_4_3:
    spark.createDataFrame(pd.DataFrame(tabla_4_3)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap4_tabla_4_3")
    print("✅ tesis_cap4_tabla_4_3")

# 5e. Detalle particiones (rendimiento por partición)
if detalle_particiones:
    spark.createDataFrame(pd.DataFrame(detalle_particiones)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap4_detalle_particiones")
    print(f"✅ tesis_cap4_detalle_particiones ({len(detalle_particiones)} filas)")
