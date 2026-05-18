# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Cap 6 — Extensión Reservorio SSRC (Completo)
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:** Grid Search SSRC completo + verificaciones teóricas + análisis de sensibilidad.
# MAGIC
# MAGIC **Salidas Gold:**
# MAGIC - `tesis_cap6_grid_completo`: Todas las configuraciones (D, rho, leak)
# MAGIC - `tesis_cap6_best_ssrc`: Ganadores por combustible
# MAGIC - `tesis_cap6_predicciones_semanales`: Real vs Markov vs SSRC
# MAGIC - `tesis_cap6_comparacion_final`: Barplot Markov vs SSRC
# MAGIC - `tesis_cap6_realizaciones`: Boxplot de N realizaciones
# MAGIC - `tesis_cap6_washout_convergencia`: Convergencia RMSE vs washout
# MAGIC - `tesis_cap6_verificaciones_teoricas`: ESP, rango, inclusión
# MAGIC - `tesis_cap6_perturbacion`: Cota de perturbación por ρ

# COMMAND ----------

import numpy as np
import pandas as pd
from scipy.optimize import nnls
from scipy import stats
from sklearn.metrics import mean_squared_error
from sklearn.decomposition import PCA
from sklearn.linear_model import Ridge
from pyspark.sql import functions as F

CATALOG = "combustibles_hn"
spark.sql(f"USE CATALOG {CATALOG}")

SEED = 42
np.random.seed(SEED)

# Espacio de búsqueda
RESERVOIR_D_VALUES = [10, 20, 50, 60, 100, 150]
RESERVOIR_RHO_VALUES = [0.5, 0.7, 0.8, 0.85, 0.9, 0.95, 0.99]
RESERVOIR_LEAK_RATES = [0.1, 0.3, 0.5, 0.8, 1.0]
RESERVOIR_WASHOUT = 50
TRAIN_RATIO = 0.8
N_REALIZATIONS = 30  # Para boxplot (Fig 07)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Funciones del Reservorio

# COMMAND ----------

def create_reservoir(input_dim, reservoir_dim, spectral_radius, sparsity=0.9, seed=SEED):
    rng = np.random.RandomState(seed)
    W_in = rng.uniform(-1, 1, (reservoir_dim, input_dim))
    W_res = rng.uniform(-1, 1, (reservoir_dim, reservoir_dim))
    mask = rng.rand(reservoir_dim, reservoir_dim) < sparsity
    W_res[mask] = 0
    eigenvalues = np.linalg.eigvals(W_res)
    max_eigenvalue = np.max(np.abs(eigenvalues))
    if max_eigenvalue > 0:
        W_res = W_res * (spectral_radius / max_eigenvalue)
    return W_in, W_res, eigenvalues * (spectral_radius / max_eigenvalue) if max_eigenvalue > 0 else eigenvalues

def propagate_reservoir(alphas, W_in, W_res, washout=50, leak_rate=1.0):
    D = W_res.shape[0]
    T = len(alphas)
    H = np.zeros((D, T))
    h = np.zeros(D)
    W_in_col = W_in.ravel()
    for t in range(T):
        pre = W_in_col * alphas[t] + W_res @ h
        h = (1.0 - leak_rate) * h + leak_rate * np.tanh(pre)
        H[:, t] = h
    return H[:, washout:]

def evaluate_ssrc(fuel_name, alphas, prices, D, rho, leak_rate, seed, train_size, washout=RESERVOIR_WASHOUT):
    W_in, W_res, eigs = create_reservoir(1, D, rho, sparsity=0.9, seed=seed)
    H_full = propagate_reservoir(alphas, W_in, W_res, washout=washout, leak_rate=leak_rate)

    alphas_eff = alphas[washout:]
    prices_eff = prices[washout:]
    T_eff = H_full.shape[1]
    if T_eff < 10:
        return None, None, None, None

    train_end = min(train_size - washout, T_eff - 2)
    if train_end < 5:
        return None, None, None, None

    targets_train = alphas_eff[1:train_end + 1]
    H_train = H_full[:, :train_end]

    try:
        W_out, _ = nnls(H_train.T, targets_train)
    except Exception:
        return None, None, None, None

    # Ridge Regression como benchmark alternativo (Tabla 6.2)
    try:
        ridge = Ridge(alpha=1.0, fit_intercept=False)
        ridge.fit(H_train.T, targets_train)
        W_out_ridge = ridge.coef_
    except Exception:
        W_out_ridge = None

    test_start = train_end
    test_end = T_eff - 1
    actual_prices, ssrc_pred_prices, ridge_pred_prices, weeks = [], [], [], []

    for t in range(test_start, test_end):
        alpha_pred = W_out @ H_full[:, t]
        pred_price = prices_eff[t] * (1 + alpha_pred)
        ssrc_pred_prices.append(pred_price)
        actual_prices.append(prices_eff[t + 1])
        weeks.append(t - test_start + 1)

        # Ridge prediction
        if W_out_ridge is not None:
            alpha_pred_r = W_out_ridge @ H_full[:, t]
            ridge_pred_prices.append(prices_eff[t] * (1 + alpha_pred_r))

    if len(actual_prices) < 2:
        return None, None, None, None

    rmse = np.sqrt(mean_squared_error(actual_prices, ssrc_pred_prices))
    rmse_ridge = np.sqrt(mean_squared_error(actual_prices, ridge_pred_prices)) if ridge_pred_prices else None
    result = {'Combustible': fuel_name, 'D': D, 'rho': rho, 'leak_rate': leak_rate, 'RMSE': rmse, 'RMSE_Ridge': rmse_ridge}
    predictions = {'weeks': weeks, 'actual': actual_prices, 'ssrc_pred': ssrc_pred_prices}
    return result, predictions, eigs, H_full

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
    dbutils.notebook.exit("Faltan dependencias")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Grid Search SSRC Completo

# COMMAND ----------

resultados_ssrc = []
mejores_predicciones = {}
mejores_eigs = {}
mejores_H = {}
combustibles = ['Super', 'Regular', 'Diesel', 'Kerosene']
train_size = int(len(df_alphas) * TRAIN_RATIO)

total_configs = len(combustibles) * len(RESERVOIR_D_VALUES) * len(RESERVOIR_RHO_VALUES) * len(RESERVOIR_LEAK_RATES)
print(f"Iniciando Grid Search SSRC: {total_configs} configuraciones...")

for fuel in combustibles:
    col_alpha = f"{fuel}_Alpha"
    col_price = fuel
    if col_alpha not in df_alphas.columns or col_price not in df_alphas.columns:
        continue

    alphas = np.array(df_alphas[col_alpha].fillna(0).values, dtype=float)
    prices = np.array(df_alphas[col_price].fillna(0).values, dtype=float)
    best_rmse_fuel = 999.0

    for D in RESERVOIR_D_VALUES:
        for rho in RESERVOIR_RHO_VALUES:
            for leak in RESERVOIR_LEAK_RATES:
                res, preds, eigs, H = evaluate_ssrc(fuel, alphas, prices, D, rho, leak, SEED, train_size)
                if res:
                    resultados_ssrc.append(res)
                    if res['RMSE'] < best_rmse_fuel:
                        best_rmse_fuel = res['RMSE']
                        mejores_predicciones[fuel] = preds
                        mejores_eigs[fuel] = eigs
                        mejores_H[fuel] = H

    print(f"  {fuel} completado. Mejor RMSE: {best_rmse_fuel:.6f}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. N=30 Realizaciones del Mejor Modelo (Para Boxplot)

# COMMAND ----------

df_res_ssrc = pd.DataFrame(resultados_ssrc)
df_best_ssrc = df_res_ssrc.loc[df_res_ssrc.groupby('Combustible')['RMSE'].idxmin()]

print("\n--- MEJORES CONFIGURACIONES SSRC ---")
display(df_best_ssrc)

realizaciones = []
for _, row in df_best_ssrc.iterrows():
    fuel = row['Combustible']
    D_opt = int(row['D'])
    rho_opt = row['rho']
    leak_opt = row['leak_rate']

    alphas = np.array(df_alphas[f'{fuel}_Alpha'].fillna(0).values, dtype=float)
    prices = np.array(df_alphas[fuel].fillna(0).values, dtype=float)

    for r in range(N_REALIZATIONS):
        seed_r = SEED + r * 1000 + D_opt * 7
        res, _, _, _ = evaluate_ssrc(fuel, alphas, prices, D_opt, rho_opt, leak_opt, seed_r, train_size)
        if res:
            realizaciones.append({
                'Combustible': fuel, 'Realizacion': r + 1,
                'D': D_opt, 'rho': rho_opt, 'leak_rate': leak_opt,
                'RMSE': res['RMSE'], 'Seed': seed_r
            })

    rmses_fuel = [x['RMSE'] for x in realizaciones if x['Combustible'] == fuel]
    if rmses_fuel:
        print(f"  {fuel}: mean={np.mean(rmses_fuel):.6f} std={np.std(rmses_fuel):.6f} [{len(rmses_fuel)} realizaciones]")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Convergencia del Washout (Fig 11)

# COMMAND ----------

washout_values = [0, 10, 20, 30, 50, 75, 100, 150]
washout_results = []

for _, row in df_best_ssrc.iterrows():
    fuel = row['Combustible']
    D_opt, rho_opt, leak_opt = int(row['D']), row['rho'], row['leak_rate']
    alphas = np.array(df_alphas[f'{fuel}_Alpha'].fillna(0).values, dtype=float)
    prices = np.array(df_alphas[fuel].fillna(0).values, dtype=float)

    for wo in washout_values:
        res, _, _, _ = evaluate_ssrc(fuel, alphas, prices, D_opt, rho_opt, leak_opt, SEED, train_size, washout=wo)
        if res:
            washout_results.append({
                'Combustible': fuel, 'Washout': wo, 'RMSE': res['RMSE']
            })

print(f"Convergencia washout: {len(washout_results)} evaluaciones")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Verificaciones Teóricas (ESP, Inclusión, Perturbación)

# COMMAND ----------

verificaciones = []
perturbacion_results = []

for _, row in df_best_ssrc.iterrows():
    fuel = row['Combustible']
    D_opt, rho_opt, leak_opt = int(row['D']), row['rho'], row['leak_rate']

    W_in, W_res, eigs = create_reservoir(1, D_opt, rho_opt, seed=SEED)
    actual_rho = np.max(np.abs(eigs))

    # ESP check: ρ(W_res) < 1
    esp_check = actual_rho < 1.0
    # Rank condition
    rank_Wres = np.linalg.matrix_rank(W_res)

    verificaciones.append({
        'Combustible': fuel, 'D': D_opt, 'rho_target': rho_opt,
        'rho_actual': float(actual_rho),
        'ESP_Cumple': esp_check,
        'Rank_Wres': int(rank_Wres), 'Rank_Full': int(D_opt),
        'Rank_Ratio': float(rank_Wres / D_opt)
    })

    # Cota de perturbación (Prop 6.2): evaluar para múltiples ρ cercanos
    alphas = np.array(df_alphas[f'{fuel}_Alpha'].fillna(0).values, dtype=float)
    prices = np.array(df_alphas[fuel].fillna(0).values, dtype=float)
    for delta_rho in [-0.1, -0.05, 0, 0.05, 0.1]:
        rho_test = rho_opt + delta_rho
        if rho_test <= 0 or rho_test >= 1.0:
            continue
        res, _, _, _ = evaluate_ssrc(fuel, alphas, prices, D_opt, rho_test, leak_opt, SEED, train_size)
        if res:
            perturbacion_results.append({
                'Combustible': fuel, 'rho_base': rho_opt,
                'rho_perturbado': rho_test, 'delta_rho': delta_rho,
                'RMSE': res['RMSE']
            })

    # Verificación de inclusión: SSRC con D=1, leak=1, ρ=0 ≡ Markov
    res_degenerate, _, _, _ = evaluate_ssrc(fuel, alphas, prices, 1, 0.01, 1.0, SEED, train_size)
    verificaciones[-1]['RMSE_Degenerado_D1'] = res_degenerate['RMSE'] if res_degenerate else None

    print(f"  {fuel}: ESP={'✅' if esp_check else '❌'} ρ={actual_rho:.4f} rank={rank_Wres}/{D_opt}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Predicciones Semanales y Comparación Final

# COMMAND ----------

resultados_predicciones = []
comparacion = []

for fuel in combustibles:
    if fuel not in mejores_predicciones:
        continue

    preds = mejores_predicciones[fuel]
    col_state = f'{fuel}_State'

    df_fc = df_centroides[df_centroides['Combustible'] == fuel].sort_values('Estado')
    centroids = df_fc['Centroide_Alpha'].values
    k_optimo = len(centroids)

    P = np.zeros((k_optimo, k_optimo))
    for _, r in df_matrices[df_matrices['Combustible'] == fuel].iterrows():
        o, d = int(r['Estado_Origen']), int(r['Estado_Destino'])
        if o < k_optimo and d < k_optimo:
            P[o, d] = r['Probabilidad']

    prices_full = np.array(df_alphas[fuel].fillna(0).values, dtype=float)
    states_full = df_alphas[col_state].values if col_state in df_alphas.columns else np.zeros(len(prices_full))

    test_start = int(len(prices_full) * TRAIN_RATIO)
    for w_idx, (week, actual, ssrc_pred) in enumerate(zip(preds['weeks'], preds['actual'], preds['ssrc_pred'])):
        t = test_start + RESERVOIR_WASHOUT + w_idx
        if t < len(states_full) and t < len(prices_full):
            current_s = int(states_full[t])
            if current_s < k_optimo:
                next_s = np.argmax(P[current_s, :])
                markov_alpha = centroids[next_s] if next_s < len(centroids) else 0
                markov_pred = prices_full[t] * (1 + markov_alpha)
            else:
                markov_pred = actual
        else:
            markov_pred = actual

        resultados_predicciones.append({
            'Combustible': fuel, 'Semana': week,
            'Precio_Real': float(actual), 'Prediccion_Markov': float(markov_pred),
            'Prediccion_SSRC': float(ssrc_pred)
        })

    # Comparación final con DM test y std (Tablas 6.2 y 6.3)
    ssrc_row = df_best_ssrc[df_best_ssrc['Combustible'] == fuel]
    markov_row = df_markov_rmse[df_markov_rmse['Combustible'] == fuel]
    if not ssrc_row.empty and not markov_row.empty:
        rmse_m = float(markov_row.iloc[0]['RMSE_Markov'])
        rmse_s = float(ssrc_row.iloc[0]['RMSE'])
        rmse_ridge = float(ssrc_row.iloc[0]['RMSE_Ridge']) if 'RMSE_Ridge' in ssrc_row.columns and pd.notna(ssrc_row.iloc[0].get('RMSE_Ridge')) else None
        delta = (rmse_s - rmse_m) / rmse_m * 100
        delta_solver = ((rmse_s - rmse_ridge) / rmse_ridge * 100) if rmse_ridge else None

        # Std de 30 realizaciones
        rmses_30 = [x['RMSE'] for x in realizaciones if x['Combustible'] == fuel]
        rmse_std = float(np.std(rmses_30)) if rmses_30 else 0.0

        # Test de Diebold-Mariano: comparar errores cuadráticos
        preds_f = [p for p in resultados_predicciones if p['Combustible'] == fuel]
        if preds_f:
            e_markov = [(p['Precio_Real'] - p['Prediccion_Markov'])**2 for p in preds_f]
            e_ssrc = [(p['Precio_Real'] - p['Prediccion_SSRC'])**2 for p in preds_f]
            d_t = np.array(e_markov) - np.array(e_ssrc)  # >0 means SSRC better
            if len(d_t) > 2 and np.std(d_t) > 0:
                dm_stat = np.mean(d_t) / (np.std(d_t) / np.sqrt(len(d_t)))
                dm_p = 2 * (1 - stats.t.cdf(abs(dm_stat), df=len(d_t) - 1))
            else:
                dm_stat, dm_p = 0.0, 1.0
        else:
            dm_stat, dm_p = 0.0, 1.0

        comparacion.append({
            'Combustible': fuel, 'RMSE_Markov': rmse_m,
            'RMSE_SSRC': rmse_s, 'RMSE_SSRC_Std': rmse_std,
            'RMSE_Ridge': rmse_ridge,
            'Delta_Porcentaje': delta,
            'Delta_Solver': delta_solver,
            'DM_Statistic': float(dm_stat), 'DM_P_Value': float(dm_p),
            'Significativo_005': dm_p < 0.05,
            'Mejor_Modelo': 'SSRC' if delta < 0 else 'Markov'
        })

if comparacion:
    print("\n--- COMPARACION FINAL (con DM test) ---")
    display(pd.DataFrame(comparacion))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Guardar TODO en Gold

# COMMAND ----------

# 8a. Grid completo
spark.createDataFrame(df_res_ssrc).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap6_grid_completo")
print(f"✅ tesis_cap6_grid_completo ({len(df_res_ssrc)} filas)")

# 8b. Mejores configuraciones
spark.createDataFrame(df_best_ssrc).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap6_best_ssrc")
print("✅ tesis_cap6_best_ssrc")

# 8c. Predicciones semanales
if resultados_predicciones:
    spark.createDataFrame(pd.DataFrame(resultados_predicciones)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap6_predicciones_semanales")
    print(f"✅ tesis_cap6_predicciones_semanales ({len(resultados_predicciones)} filas)")

# 8d. Comparación final
if comparacion:
    spark.createDataFrame(pd.DataFrame(comparacion)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap6_comparacion_final")
    print("✅ tesis_cap6_comparacion_final")

# 8e. Realizaciones (boxplot)
if realizaciones:
    spark.createDataFrame(pd.DataFrame(realizaciones)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap6_realizaciones")
    print(f"✅ tesis_cap6_realizaciones ({len(realizaciones)} filas)")

# 8f. Washout convergencia
if washout_results:
    spark.createDataFrame(pd.DataFrame(washout_results)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap6_washout_convergencia")
    print(f"✅ tesis_cap6_washout_convergencia ({len(washout_results)} filas)")

# 8g. Verificaciones teóricas
if verificaciones:
    spark.createDataFrame(pd.DataFrame(verificaciones)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap6_verificaciones_teoricas")
    print("✅ tesis_cap6_verificaciones_teoricas")

# 8h. Perturbación
if perturbacion_results:
    spark.createDataFrame(pd.DataFrame(perturbacion_results)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap6_perturbacion")
    print(f"✅ tesis_cap6_perturbacion ({len(perturbacion_results)} filas)")

# 8i. Autovalores del mejor modelo (para Fig 04)
eigenvalue_results = []
for fuel, eigs in mejores_eigs.items():
    for idx, ev in enumerate(eigs):
        eigenvalue_results.append({
            'Combustible': fuel, 'Index': idx,
            'Real': float(np.real(ev)), 'Imag': float(np.imag(ev)),
            'Modulus': float(np.abs(ev))
        })

if eigenvalue_results:
    spark.createDataFrame(pd.DataFrame(eigenvalue_results)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap6_autovalores")
    print(f"✅ tesis_cap6_autovalores ({len(eigenvalue_results)} filas)")

# 8j. Estados del reservorio PCA (para Fig 05, 13)
pca_results = []
for fuel, H in mejores_H.items():
    if H.shape[1] > 3:
        pca = PCA(n_components=2)
        H_pca = pca.fit_transform(H.T)
        alphas_eff = np.array(df_alphas[f'{fuel}_Alpha'].fillna(0).values, dtype=float)[RESERVOIR_WASHOUT:]
        states_eff = df_alphas[f'{fuel}_State'].values[RESERVOIR_WASHOUT:] if f'{fuel}_State' in df_alphas.columns else np.zeros(len(alphas_eff))

        for t in range(min(len(H_pca), len(alphas_eff), len(states_eff))):
            pca_results.append({
                'Combustible': fuel, 'Tiempo': t,
                'PC1': float(H_pca[t, 0]), 'PC2': float(H_pca[t, 1]),
                'Alpha': float(alphas_eff[t]),
                'Activacion_Media': float(np.mean(np.abs(H[:, t]))),
                'Estado_Markov': int(states_eff[t])
            })

if pca_results:
    spark.createDataFrame(pd.DataFrame(pca_results)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.tesis_cap6_reservorio_pca")
    print(f"✅ tesis_cap6_reservorio_pca ({len(pca_results)} filas)")

print("\n=== CAPITULO 6 COMPLETO ===")
