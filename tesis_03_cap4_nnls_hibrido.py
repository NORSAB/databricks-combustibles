# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Tesis Cap 4 — Modelo Híbrido TCRA-Markov con Estimación NNLS, Grid Search de 3 Jueces y K-Means
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:**
# MAGIC 1. Optimizar los hiperparámetros estocásticos $(W, \lambda, K)$ mediante una búsqueda en rejilla (Grid Search) utilizando el criterio jerárquico de los **3 Jueces Jerárquicos** (RMSE de precios, Exactitud de régimen y AIC de Markov).
# MAGIC 2. **Discretizar alphas por K-Means** (centroides ordenados de menor a mayor) para definir los estados de forma endógena a partir de la distribución de cada combustible.
# MAGIC 3. **Estimar las matrices de transición de Markov con NNLS (SRep)** bajo restricciones de no negatividad ($P_{ij} \ge 0$) y suma de columnas unitaria ($\sum_{i=1}^K P_{ij} = 1$).
# MAGIC 4. **Calcular propiedades espectrales** (brecha espectral $\gamma$, tiempo de mezcla $t_{\text{mix}}$ y distribución estacionaria $\boldsymbol{\pi}$).
# MAGIC 5. **Evaluar el rendimiento predictivo** del modelo híbrido, incluyendo predicciones semanales de precios con RMSE y exactitud (accuracy) predictiva en validación cruzada ($P$ vs $Ap$ para la Tabla 4.3).
# MAGIC 6. **Realizar un análisis comparativo** de K-Means frente a la discretización por **Cuantiles Fijos** y frente a los **Umbrales Fijos del Capítulo 3**.
# MAGIC 7. **Generar y exportar la gráfica de análisis de sensibilidad** (RMSE y Exactitud vs. Ventana $W$) consolidada en un gráfico multi-panel.
# MAGIC 8. **Exportar gráficos de evolución temporal de mezcla** para comprobar la convergencia de la cadena de Markov.
# MAGIC
# MAGIC **Salidas Gold:**
# MAGIC - `combustibles_hn.gold.tesis_cap4_best_hyperparams`: Hiperparámetros óptimos seleccionados por el protocolo de 3 jueces.
# MAGIC - `combustibles_hn.gold.tesis_cap4_matrices_transicion`: Matrices de transición estimadas por NNLS con conteos y totales.
# MAGIC - `combustibles_hn.gold.tesis_cap4_propiedades_espectrales`: Autovalores, brecha espectral, tiempo de mezcla y pi estacionaria.
# MAGIC - `combustibles_hn.gold.tesis_cap4_predicciones_nnls`: Predicciones de probabilidad NNLS por estado.
# MAGIC - `combustibles_hn.gold.tesis_cap4_rmse_markov_base`: RMSE Markov base para cada combustible (benchmark de la tesis).
# MAGIC - `combustibles_hn.gold.tesis_cap4_tabla_comparativa`: Comparación de K-Means contra Cuantiles y Umbrales Fijos.
# MAGIC - `combustibles_hn.gold.tesis_cap4_detalle_grid`: Resultados detallados de todas las corridas de la grilla de búsqueda.
# MAGIC

# COMMAND ----------

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.optimize import nnls
from scipy.signal import lfilter
from sklearn.cluster import KMeans
from sklearn.metrics import mean_squared_error
from numpy import kron, identity, ones, zeros, diag

CATALOG = "combustibles_hn"
spark.sql(f"USE CATALOG {CATALOG}")

# Configurar directorio local para gráficos
CHARTS_DIR = "d:/2026/Databricks/Combustibles/local/charts"
os.makedirs(CHARTS_DIR, exist_ok=True)
print(f"Directorio de gráficos configurado en: {CHARTS_DIR}")

# Paleta Nord
NORD_PALETTE = {
    'dark_bg': '#2E3440',
    'light_bg': '#ECEFF4',
    'frost_blue': '#5E81AC',
    'frost_teal': '#8FBCBB',
    'frost_sky': '#88C0D0',
    'aurora_red': '#BF616A',
    'aurora_orange': '#D08770',
    'aurora_yellow': '#EBCB8B',
    'aurora_green': '#A3BE8C',
    'aurora_purple': '#B48EAD',
    'gray_text': '#4C566A'
}

SEED = 42
np.random.seed(SEED)

W_VALUES = list(range(2, 53))
LAMBDAS = [0.70, 0.80, 0.85, 0.90, 0.95, 0.97, 0.98, 0.99, 1.00]
K_VALUES = list(range(2, 10))
TRAIN_RATIOS = [0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]

# Óptimos teóricos de la tesis de combustibles para mantener coherencia absoluta
THESIS_OPTIMALS = {
    'Regular': {'W': 50, 'lambda': 0.99, 'K': 3, 'rmse_th': 0.8394, 'acc_th': 85.10, 'aic_th': 190.87},
    'Superior': {'W': 52, 'lambda': 0.99, 'K': 4, 'rmse_th': 0.9769, 'acc_th': 62.78, 'aic_th': 272.80},
    'Diesel': {'W': 50, 'lambda': 0.97, 'K': 3, 'rmse_th': 1.0816, 'acc_th': 71.56, 'aic_th': 175.48},
    'Kerosene': {'W': 52, 'lambda': 0.98, 'K': 5, 'rmse_th': 1.1714, 'acc_th': 68.20, 'aic_th': 225.79}
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lectura de Precios Silver
# MAGIC

# COMMAND ----------

df_prices_spark = spark.table(f"{CATALOG}.silver.silver_precios_semanales").orderBy("Fecha")
df_prices = df_prices_spark.toPandas()

# Convertir Fecha a datetime
if 'Fecha' in df_prices.columns:
    df_prices['Fecha'] = pd.to_datetime(df_prices['Fecha'])
    
print(f"Total registros de precios cargados: {len(df_prices)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Funciones Helper de Estimación y Optimización
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

def estimate_P_from_C(C):
    """Estima P (MLE) normalizando columnas de C."""
    col_sums = C.sum(axis=0)
    col_sums[col_sums == 0] = 1
    return C / col_sums

def SRep(datos, ss):
    """Estimación NNLS/SRep (Ap) con restricciones de columnas unitarias y regularización progresiva."""
    n0 = datos.shape[0]
    S0 = datos[:, :ss]
    S0 = kron(S0, identity(n0)).T
    S1 = S0.T @ ((datos[:, 1:(1 + ss)]).T).reshape(ss * n0)
    C_mat = kron(identity(n0), ones((1, n0)))
    Mr = zeros((n0**2 + n0, n0**2))
    
    for epsilon in [0.0, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2]:
        try:
            Mr[:n0**2, :] = S0.T @ S0 + epsilon * identity(n0**2)
            Mr[n0**2:, :] = C_mat
            rhs = zeros((n0**2 + n0))
            rhs[:n0**2] = S1
            rhs[n0**2:] = 1
            c = zeros((n0**2, 1))
            c[:, 0] = nnls(Mr, rhs)[0]
            Pr = c.reshape(n0, n0).T
            Pr = Pr @ diag(1 / np.sum(Pr, axis=0))
            return Pr
        except RuntimeError:
            continue
    raise RuntimeError("NNLS no pudo converger incluso con regularización progresiva.")

def compute_Ap(P):
    """Calcula Ap vía simulación de probabilidades de estado + SRep."""
    k_local = len(P)
    p0 = zeros((k_local, 100))
    p0[0, 0] = 1
    for j in range(99):
        p0[:, j + 1] = P @ p0[:, j]
    return SRep(p0, ss=99)

def predict_next_state_nnls(P_matrix, current_state_vector):
    """Resuelve min ||P*x - b||_2^2 para la predicción NNLS a un paso."""
    A = P_matrix
    b = current_state_vector
    x, residual = nnls(A, b)
    if np.sum(x) > 0:
        x = x / np.sum(x)
    return x, residual

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ejecución del Grid Search de los 3 Jueces Jerárquicos
# MAGIC

# COMMAND ----------

grid_runs_all = []
best_params_results = []
results_nnls_opt = []
results_markov_rmse_opt = []
results_precios_semanales_opt = []
results_matrices_opt = []
results_propiedades_espectrales_opt = []
results_comparativos = []
results_centroids_opt = []

combustibles = ['Regular', 'Super', 'Diesel', 'Kerosene']
grid_results_by_fuel = {}

train_end = int(len(df_prices) * 0.8)  # Usar 80% de la serie para entrenamiento inicial (ajuste dinámico)

for fuel in combustibles:
    if fuel not in df_prices.columns:
        print(f"Advertencia: No se encontró columna {fuel}, saltando.")
        continue

    v = df_prices[fuel].ffill().bfill().astype(float).values
    n = len(v)
    
    print(f"\nProcesando grilla para {fuel}...")
    fuel_grid_runs = []
    
    for W in W_VALUES:
        for lambd in LAMBDAS:
            alphas = calculate_alphas_fast(v, W, lambd)
            for K in K_VALUES:
                # K-Means sobre alphas de entrenamiento
                alphas_train = alphas[W:train_end]
                if len(alphas_train) < K:
                    continue
                kmeans = KMeans(n_clusters=K, random_state=SEED, n_init=1)
                kmeans.fit(alphas_train.reshape(-1, 1))
                centroids = np.sort(kmeans.cluster_centers_.flatten())
                
                # Asignar estados por mínima distancia euclidiana
                states = np.zeros(n, dtype=int)
                for t in range(W, n):
                    states[t] = np.argmin(np.abs(alphas[t] - centroids))
                
                # Bucle de predicción un-paso-adelante fuera de muestra
                actual_prices = []
                pred_prices = []
                correct_states = 0
                n_preds = 0
                
                # Matriz de conteo inicial
                C_t = np.zeros((K, K))
                for t_idx in range(W, train_end - 1):
                    C_t[states[t_idx+1], states[t_idx]] += 1
                
                for t in range(train_end, n - 1):
                    C_t[states[t], states[t-1]] += 1
                    P_t = estimate_P_from_C(C_t)
                    
                    curr_s = states[t]
                    next_s_pred = np.argmax(P_t[:, curr_s])
                    
                    pred_alpha = centroids[next_s_pred]
                    pred_p = v[t] * (1.0 + pred_alpha)
                    
                    actual_prices.append(v[t+1])
                    pred_prices.append(pred_p)
                    
                    if next_s_pred == states[t+1]:
                        correct_states += 1
                    n_preds += 1
                
                if n_preds > 0:
                    rmse = np.sqrt(mean_squared_error(actual_prices, pred_prices))
                    accuracy = (correct_states / n_preds) * 100
                    
                    # Calcular AIC final de la cadena de Markov
                    P_final = estimate_P_from_C(C_t)
                    log_lik = 0.0
                    for i in range(K):
                        for j in range(K):
                            if C_t[i, j] > 0 and P_final[i, j] > 0:
                                log_lik += C_t[i, j] * np.log(P_final[i, j])
                    aic = 2 * K * (K - 1) - 2 * log_lik
                    
                    run_record = {
                        'Combustible': fuel, 'W': int(W), 'Lambda': float(lambd), 'K': int(K),
                        'RMSE': float(rmse), 'Accuracy': float(accuracy), 'AIC': float(aic)
                    }
                    fuel_grid_runs.append(run_record)
                    grid_runs_all.append(run_record)
                    
    df_fuel_grid = pd.DataFrame(fuel_grid_runs)
    grid_results_by_fuel[fuel] = df_fuel_grid
    
    # Aplicar protocolo de los 3 jueces para la selección de hiperparámetros
    # 1. Minimizar RMSE, 2. Maximizar Accuracy, 3. Minimizar AIC
    df_sorted = df_fuel_grid.sort_values(
        by=['RMSE', 'Accuracy', 'AIC'],
        ascending=[True, False, True]
    )
    best_row = df_sorted.iloc[0]
    W_opt = int(best_row['W'])
    lambda_opt = float(best_row['Lambda'])
    K_opt = int(best_row['K'])
    rmse_opt = float(best_row['RMSE'])
    acc_opt = float(best_row['Accuracy'])
    aic_opt = float(best_row['AIC'])
        
    best_params_results.append({
        'Combustible': fuel, 'W_opt': W_opt, 'Lambda_opt': lambda_opt, 'K_opt': K_opt,
        'RMSE_Opt': rmse_opt, 'Accuracy_Opt': acc_opt, 'AIC_Opt': aic_opt
    })
    
    print(f"Óptimos seleccionados dinámicamente para {fuel}: W={W_opt}, Lambda={lambda_opt:.2f}, K={K_opt} (RMSE={rmse_opt:.4f}, Accuracy={acc_opt:.2f}%, AIC={aic_opt:.2f})")

# COMMAND ----------

display(df_thesis_optimals)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Discretización de K-Means, Estimación NNLS y Propiedades Espectrales
# MAGIC

# COMMAND ----------

best_params_by_fuel = {row['Combustible']: row for row in best_params_results}

for fuel in combustibles:
    v = df_prices[fuel].ffill().bfill().astype(float).values
    n = len(v)
    
    bp = best_params_by_fuel[fuel]
    W_opt = bp['W_opt']
    lambda_opt = bp['Lambda_opt']
    K_opt = bp['K_opt']
    
    alphas_opt = calculate_alphas_fast(v, W_opt, lambda_opt)
    
    # Ajustar K-Means con el número de estados K_opt
    kmeans_opt = KMeans(n_clusters=K_opt, random_state=SEED, n_init=10)
    kmeans_opt.fit(alphas_opt[W_opt:train_end].reshape(-1, 1))
    centroids_opt = np.sort(kmeans_opt.cluster_centers_.flatten())
    
    # Guardar centroides
    for state_i, centroid_val in enumerate(centroids_opt):
        results_centroids_opt.append({
            'Combustible': fuel,
            'Estado': state_i,
            'Centroide_Alpha': float(centroid_val)
        })
        
    states_opt = np.zeros(n, dtype=int)
    for t in range(W_opt, n):
        states_opt[t] = np.argmin(np.abs(alphas_opt[t] - centroids_opt))
        
    # Guardar alphas óptimos con estados asignados en el DataFrame principal
    df_prices[f'{fuel}_Alpha_Opt'] = alphas_opt
    df_prices[f'{fuel}_State'] = states_opt
    
    # Matrices completas
    K_eff = K_opt
    X0 = np.zeros((K_eff, len(states_opt) - W_opt - 1))
    X1 = np.zeros((K_eff, len(states_opt) - W_opt - 1))
    for t_idx, t in enumerate(range(W_opt, n - 1)):
        X0[states_opt[t], t_idx] = 1.0
        X1[states_opt[t+1], t_idx] = 1.0
        
    C_full = X1 @ X0.T
    P_full = estimate_P_from_C(C_full)
    
    # Estimación de Ap mediante simulación + SRep (NNLS)
    Ap_full = compute_Ap(P_full)
    
    # Guardar matriz de transición (K-Means)
    col_sums = C_full.sum(axis=0)
    for i in range(K_eff):
        for j in range(K_eff):
            results_matrices_opt.append({
                'Combustible': fuel, 'Metodo': 'K-Means', 'Estado_Origen': j, 'Estado_Destino': i,
                'Probabilidad_MLE': float(P_full[i, j]), 'Probabilidad_NNLS': float(Ap_full[i, j]),
                'Conteo_Transiciones': int(C_full[i, j]), 'Total_Origen': int(col_sums[j])
            })
            
    # Propiedades Espectrales
    eigenvalues = np.linalg.eigvals(P_full)
    sorted_eigs = np.sort(np.abs(eigenvalues))[::-1]
    
    eig_vals, eig_vecs = np.linalg.eig(P_full)
    idx_one = np.argmin(np.abs(eig_vals - 1.0))
    pi_stationary = np.real(eig_vecs[:, idx_one])
    pi_stationary = pi_stationary / pi_stationary.sum()
    
    lambda_2 = sorted_eigs[1] if len(sorted_eigs) > 1 else 0.0
    spectral_gap = 1.0 - lambda_2
    mixing_time = -np.log(0.01) / spectral_gap if spectral_gap > 0 and lambda_2 < 1.0 else 0
    
    results_propiedades_espectrales_opt.append({
        'Combustible': fuel, 'K_Estados': K_opt,
        'Eigenvalue_Dominante': float(sorted_eigs[0]), 'Eigenvalue_2': float(lambda_2),
        'Spectral_Gap': float(spectral_gap), 'Mixing_Time_Approx': float(mixing_time),
        'Pi_Estacionaria': str([round(float(x), 6) for x in pi_stationary])
    })
    
    # Predicción y validación temporal a un paso
    actual_prices, predicted_prices = [], []
    for t in range(train_end, n - 1):
        curr_s = states_opt[t]
        next_s = np.argmax(P_full[:, curr_s])
        pred_alpha = centroids_opt[next_s]
        pred_p = v[t] * (1.0 + pred_alpha)
        actual_prices.append(v[t+1])
        predicted_prices.append(pred_p)
        
        results_precios_semanales_opt.append({
            'Combustible': fuel, 'Semana': t + 1,
            'Fecha': str(df_prices['Fecha'].iloc[t+1]) if 'Fecha' in df_prices.columns else '',
            'Precio_Real': float(v[t+1]), 'Precio_Predicho_Markov': float(pred_p),
            'Estado_Actual': int(curr_s), 'Estado_Predicho': int(next_s)
        })
        
    rmse_markov = np.sqrt(mean_squared_error(actual_prices, predicted_prices))
    results_markov_rmse_opt.append({
        'Combustible': fuel, 'RMSE_Markov': float(rmse_markov), 'N_Predicciones': len(actual_prices)
    })
    
    # NNLS desde cada estado inicial
    for s in range(K_eff):
        curr_state = np.zeros(K_eff)
        curr_state[s] = 1.0
        next_state, residual = predict_next_state_nnls(P_full, curr_state)
        for i, prob in enumerate(next_state):
            results_nnls_opt.append({
                'Combustible': fuel, 'Estado_Actual': s, 'Estado_Predicho': i,
                'Probabilidad_NNLS': float(prob), 'Residual_NNLS': float(residual)
            })

# COMMAND ----------

for fuel in combustibles:
    bp = best_params_by_fuel[fuel]
    W_opt = bp['W_opt']
    K_opt = bp['K_opt']
    alphas = df_prices[f'{fuel}_Alpha_Opt']
    states = df_prices[f'{fuel}_State']
    n = len(alphas)
    centroids = np.sort([c['Centroide_Alpha'] for c in results_centroids_opt if c['Combustible'] == fuel])
    colors_k = [NORD_PALETTE['aurora_red'], NORD_PALETTE['aurora_orange'], NORD_PALETTE['aurora_yellow'], NORD_PALETTE['frost_blue'], NORD_PALETTE['aurora_green'], NORD_PALETTE['aurora_purple'], NORD_PALETTE['frost_teal'], NORD_PALETTE['gray_text']]
    
    fig, ax = plt.subplots(figsize=(12, 6))  # Tamaño uniforme para todos
    for k in range(K_opt):
        idxs = np.where(states == k)[0]
        ax.scatter(idxs, alphas.iloc[idxs], color=colors_k[k % len(colors_k)], s=18, label=f'Regimen {k+1}', alpha=0.7)
    for c_idx, c_val in enumerate(centroids):
        ax.axhline(y=c_val, color=colors_k[c_idx % len(colors_k)], linestyle='--', linewidth=2, alpha=0.8, label=f'Centroide {c_idx+1}')
    ax.set_title(f"K-Means de Alphas — {fuel}", fontsize=13, fontweight='bold', pad=18)
    ax.set_xlabel("Tiempo (Semana)", fontsize=10)
    ax.set_ylabel("Alpha", fontsize=10)
    ax.grid(True, linestyle=':', alpha=0.4)
    # Leyenda a la derecha, centrada verticalmente
    ax.legend(loc='center left', bbox_to_anchor=(1.02, 0.5), fontsize=9, frameon=False)
    plt.tight_layout()
    display(fig)
    plt.close(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Simulación de Evolución de Mezcla (Gráfico)
# MAGIC

# COMMAND ----------

for fuel in combustibles:
    bp = best_params_by_fuel[fuel]
    W_opt = bp['W_opt']
    K_opt = bp['K_opt']
    
    # Recuperar matriz P_full para el combustible
    P = np.zeros((K_opt, K_opt))
    df_fuel = pd.DataFrame(results_matrices_opt)
    df_fuel = df_fuel[(df_fuel['Combustible'] == fuel) & (df_fuel['Metodo'] == 'K-Means')]
    for _, row in df_fuel.iterrows():
        o, d = int(row['Estado_Origen']), int(row['Estado_Destino'])
        P[d, o] = row['Probabilidad_NNLS']
        
    steps_sim = 30
    prob_evolution = np.zeros((K_opt, steps_sim))
    prob_evolution[0, 0] = 1.0  # partiendo de s1 (contracción)
    for step in range(1, steps_sim):
        prob_evolution[:, step] = P @ prob_evolution[:, step-1]
        
    # Recuperar pi estacionaria
    df_spec = pd.DataFrame(results_propiedades_espectrales_opt)
    pi_str = df_spec[df_spec['Combustible'] == fuel]['Pi_Estacionaria'].values[0]
    pi_stat = eval(pi_str)
    
    # Graficar
    plt.figure(figsize=(6, 4))
    colors_ev = [NORD_PALETTE['aurora_red'], NORD_PALETTE['aurora_orange'], NORD_PALETTE['aurora_yellow'], NORD_PALETTE['frost_blue'], NORD_PALETTE['aurora_green']]
    for k_ev in range(K_opt):
        plt.plot(range(steps_sim), prob_evolution[k_ev, :], label=f's{k_ev+1}', color=colors_ev[k_ev % len(colors_ev)], linewidth=2)
    plt.axhline(y=pi_stat[0], color=NORD_PALETTE['aurora_red'], linestyle=':', alpha=0.5)
    plt.title(f"Evolución de Mezcla Estocástica - {fuel}", fontsize=11, fontweight='bold', pad=12)
    plt.xlabel("Pasos (Semanas)", fontsize=9)
    plt.ylabel("Probabilidad de Ocupación", fontsize=9)
    plt.legend(loc='upper right')
    plt.grid(True, linestyle=':', alpha=0.5)
    plt.tight_layout()
    chart_ev_path = f"{CHARTS_DIR}/{fuel}_markov_evolution.png"
    plt.savefig(chart_ev_path, dpi=150)
    plt.close()
    print(f"Evolución de mezcla guardada: {chart_ev_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Análisis Comparativo: K-Means vs Cuantiles vs Umbrales Fijos
# MAGIC

# COMMAND ----------

for fuel in combustibles:
    v = df_prices[fuel].ffill().bfill().astype(float).values
    n = len(v)
    
    bp = best_params_by_fuel[fuel]
    W_opt = bp['W_opt']
    lambda_opt = bp['Lambda_opt']
    K_opt = bp['K_opt']
    
    alphas_opt = calculate_alphas_fast(v, W_opt, lambda_opt)
    
    # KMeans (óptimo)
    df_best = pd.DataFrame(best_params_results)
    rmse_kmeans = df_best[df_best['Combustible'] == fuel]['RMSE_Opt'].values[0]
    acc_kmeans = df_best[df_best['Combustible'] == fuel]['Accuracy_Opt'].values[0]
    
    # Cuantiles Fijos (percentiles uniformes de alphas)
    quantiles_th = np.percentile(alphas_opt[W_opt:], np.linspace(0, 100, K_opt+1)[1:-1])
    states_quant = np.zeros(n, dtype=int)
    states_quant[alphas_opt < quantiles_th[0]] = 0
    for idx_q in range(1, K_opt - 1):
        states_quant[(alphas_opt >= quantiles_th[idx_q-1]) & (alphas_opt < quantiles_th[idx_q])] = idx_q
    states_quant[alphas_opt >= quantiles_th[-1]] = K_opt - 1
    
    # Calcular matrices completas para Cuantiles
    X0_q = np.zeros((K_opt, len(states_quant) - W_opt - 1))
    X1_q = np.zeros((K_opt, len(states_quant) - W_opt - 1))
    for t_idx, t in enumerate(range(W_opt, n - 1)):
        X0_q[states_quant[t], t_idx] = 1.0
        X1_q[states_quant[t+1], t_idx] = 1.0
        
    C_full_q = X1_q @ X0_q.T
    P_full_q = estimate_P_from_C(C_full_q)
    Ap_full_q = compute_Ap(P_full_q)
    
    # Guardar matriz de transición para Cuantiles
    col_sums_q = C_full_q.sum(axis=0)
    for i in range(K_opt):
        for j in range(K_opt):
            results_matrices_opt.append({
                'Combustible': fuel, 'Metodo': 'Cuantiles', 'Estado_Origen': j, 'Estado_Destino': i,
                'Probabilidad_MLE': float(P_full_q[i, j]), 'Probabilidad_NNLS': float(Ap_full_q[i, j]),
                'Conteo_Transiciones': int(C_full_q[i, j]), 'Total_Origen': int(col_sums_q[j])
            })
            
    # Bucle de validación para Cuantiles
    C_quant = np.zeros((K_opt, K_opt))
    for t_idx in range(W_opt, train_end - 1):
        C_quant[states_quant[t_idx+1], states_quant[t_idx]] += 1
        
    actual_q, pred_q, correct_q = [], [], 0
    for t in range(train_end, n - 1):
        C_quant[states_quant[t], states_quant[t-1]] += 1
        P_q = estimate_P_from_C(C_quant)
        next_q_pred = np.argmax(P_q[:, states_quant[t]])
        actual_q.append(v[t+1])
        pred_q.append(v[t] * (1.0 + np.mean(alphas_opt[states_quant == next_q_pred])))
        if next_q_pred == states_quant[t+1]:
            correct_q += 1
            
    rmse_quant = np.sqrt(mean_squared_error(actual_q, pred_q))
    acc_quant = (correct_q / len(actual_q)) * 100 if len(actual_q) > 0 else 0
    
    # Umbrales Fijos (Capítulo 3)
    states_fijos = np.zeros(n, dtype=int)
    states_fijos[alphas_opt < -0.01] = 0
    states_fijos[(alphas_opt >= -0.01) & (alphas_opt <= 0.02)] = 1
    states_fijos[(alphas_opt > 0.02) & (alphas_opt <= 0.04)] = 2
    states_fijos[alphas_opt > 0.04] = 3
    
    C_fijos = np.zeros((4, 4))
    for t_idx in range(W_opt, train_end - 1):
        C_fijos[states_fijos[t_idx+1], states_fijos[t_idx]] += 1
        
    actual_f, pred_f, correct_f = [], [], 0
    for t in range(train_end, n - 1):
        C_fijos[states_fijos[t], states_fijos[t-1]] += 1
        P_f = estimate_P_from_C(C_fijos)
        next_f_pred = np.argmax(P_f[:, states_fijos[t]])
        actual_f.append(v[t+1])
        pred_f.append(v[t] * (1.0 + np.mean(alphas_opt[states_fijos == next_f_pred])))
        if next_f_pred == states_fijos[t+1]:
            correct_f += 1
    rmse_fijos = np.sqrt(mean_squared_error(actual_f, pred_f))
    acc_fijos = (correct_f / len(actual_f)) * 100 if len(actual_f) > 0 else 0
    
    results_comparativos.append({
        'Combustible': fuel,
        'RMSE_KMeans': float(rmse_kmeans), 'Accuracy_KMeans': float(acc_kmeans),
        'RMSE_Cuantiles': float(rmse_quant), 'Accuracy_Cuantiles': float(acc_quant),
        'RMSE_UmbralesFijos': float(rmse_fijos), 'Accuracy_UmbralesFijos': float(acc_fijos)
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Generación de Gráfico de Análisis de Sensibilidad (Multi-panel)
# MAGIC

# COMMAND ----------

fig, axes = plt.subplots(2, 2, figsize=(14, 9))
axes_flat = axes.flatten()

for idx, fuel in enumerate(['Regular', 'Super', 'Kerosene', 'Diesel']):
    ax1 = axes_flat[idx]
    
    # Filtrar resultados de grilla para este combustible
    df_grid = pd.DataFrame(grid_runs_all)
    df_grid = df_grid[df_grid['Combustible'] == fuel]
    
    # Agrupar por W para obtener el mejor desempeño por ventana (mínimo RMSE)
    df_sens = df_grid.groupby('W').agg({'RMSE': 'min', 'Accuracy': 'max'}).reset_index()
    
    # Graficar RMSE en eje izquierdo (Y1)
    color1 = NORD_PALETTE['frost_blue']
    ax1.plot(df_sens['W'], df_sens['RMSE'], color=color1, linestyle=':', marker='o', markersize=3, label='RMSE', linewidth=1.5)
    ax1.set_xlabel("Tamaño de Ventana (W)", fontsize=9)
    ax1.set_ylabel("Error Cuadrático Medio (RMSE)", color=color1, fontsize=9)
    ax1.tick_params(axis='y', labelcolor=color1)
    
    # Crear eje derecho para Accuracy (Y2)
    ax2 = ax1.twinx()
    color2 = NORD_PALETTE['aurora_red']
    ax2.plot(df_sens['W'], df_sens['Accuracy'], color=color2, linestyle=':', marker='x', markersize=3, label='Exactitud', linewidth=1.5)
    ax2.set_ylabel("Exactitud (%)", color=color2, fontsize=9)
    ax2.tick_params(axis='y', labelcolor=color2)
    
    # Parámetros óptimos dinámicos
    bp = best_params_by_fuel[fuel]
    W_opt = bp['W_opt']
    lambda_opt = bp['Lambda_opt']
    K_opt = bp['K_opt']
    rmse_val = bp['RMSE_Opt']
    acc_val = bp['Accuracy_Opt']
    
    # Líneas de referencia
    ax1.axvline(x=W_opt, color=NORD_PALETTE['aurora_yellow'], linestyle='--', linewidth=1.5)
    ax1.axhline(y=rmse_val, color=NORD_PALETTE['aurora_yellow'], linestyle='--', linewidth=1, alpha=0.7)
    
    # Punto de intersección
    ax1.plot(W_opt, rmse_val, color=NORD_PALETTE['aurora_yellow'], marker='o', markersize=6)
    
    ax1.set_title(f"Combustible: {'Superior' if fuel == 'Super' else fuel}", fontsize=11, fontweight='bold', pad=10)
    ax1.grid(True, linestyle=':', alpha=0.4)
    
    # Agregar leyenda y texto informativo
    info_text = f"Parámetros Óptimos\nW: {W_opt}\nLambda: {lambda_opt:.2f}\nK: {K_opt}\n\nMétricas\nRMSE: {rmse_val:.4f}\nExactitud: {acc_val:.2f}%"
    ax1.text(1.18, 0.45, info_text, transform=ax1.transAxes, fontsize=8,
             bbox=dict(boxstyle="round,pad=0.3", fc=NORD_PALETTE['light_bg'], ec="gray", alpha=0.8),
             verticalalignment='center')
    
    # Ajustar leyendas
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right', fontsize=8)

plt.suptitle("Análisis de Sensibilidad: Rendimiento vs. Tamaño de Ventana (W)", fontsize=13, fontweight='bold', y=0.98)
plt.tight_layout(rect=[0, 0, 0.90, 0.95])
sensitivity_chart_path = f"{CHARTS_DIR}/W_sensitivity_analysis_final_v6.png"
plt.savefig(sensitivity_chart_path, dpi=180, bbox_inches='tight')
plt.close()
print(f"Gráfico consolidado de sensibilidad guardado en: {sensitivity_chart_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Guardado de Resultados en Capa Gold
# MAGIC

# COMMAND ----------

# 8a. Hiperparámetros óptimos
spark.createDataFrame(pd.DataFrame(best_params_results)).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap4_best_hyperparams")
print("✅ gold.tesis_cap4_best_hyperparams")

# 8b. Matrices de transición NNLS (K-Means)
spark.createDataFrame(pd.DataFrame(results_matrices_opt)).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap4_matrices_transicion")
print("✅ gold.tesis_cap4_matrices_transicion")

# 8c. Propiedades Espectrales
spark.createDataFrame(pd.DataFrame(results_propiedades_espectrales_opt)).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap4_propiedades_espectrales")
print("✅ gold.tesis_cap4_propiedades_espectrales")

# 8d. Predicciones NNLS
spark.createDataFrame(pd.DataFrame(results_nnls_opt)).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap4_predicciones_nnls")
print("✅ gold.tesis_cap4_predicciones_nnls")

# 8e. RMSE Markov base
spark.createDataFrame(pd.DataFrame(results_markov_rmse_opt)).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap4_rmse_markov_base")
print("✅ gold.tesis_cap4_rmse_markov_base")

# 8f. Predicciones de precio semanal
spark.createDataFrame(pd.DataFrame(results_precios_semanales_opt)).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap4_predicciones_precio_semanal")
print("✅ gold.tesis_cap4_predicciones_precio_semanal")

# 8g. Tabla Comparativa de Rendimiento
spark.createDataFrame(pd.DataFrame(results_comparativos)).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap4_tabla_comparativa")
print("✅ gold.tesis_cap4_tabla_comparativa")

# 8h. Detalle Grid Runs
spark.createDataFrame(pd.DataFrame(grid_runs_all)).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap4_detalle_grid")
print("✅ gold.tesis_cap4_detalle_grid")

# 8i. Centroides K-medias del modelo óptimo
spark.createDataFrame(pd.DataFrame(results_centroids_opt)).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap4_centroides")
print("✅ gold.tesis_cap4_centroides")

# COMMAND ----------

df_thesis_optimals = pd.DataFrame(best_params_results)
display(df_thesis_optimals)
