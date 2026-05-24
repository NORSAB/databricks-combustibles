# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Cap 3 — Cadenas de Markov: Umbrales Fijos, Transiciones con Errores Estándar y Propiedades Espectrales
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:** 
# MAGIC 1. Leer los alphas óptimos del Capítulo 2 (`gold.tesis_alphas_combustibles_optimos`).
# MAGIC 2. Discretizar alphas en 4 estados mediante **Umbrales Fijos** ($\theta_1 = 0.05$, $\theta_2 = 0.0$, $\theta_3 = -0.05$) basados en la clasificación teórica del FMI y el Capítulo 3 de la tesis.
# MAGIC 3. Estimar las matrices de transición de Markov ($\hat{\mathbf{P}}$) y calcular analíticamente sus **errores estándar asintóticos** ($\hat{\sigma}_{ij}$) según el Corolario 3.1 de la tesis.
# MAGIC 4. Realizar el análisis espectral completo de cada matriz: autovalores, brecha espectral ($\gamma$), tiempo de mezcla ($t_{\text{mix}}$) y distribución estacionaria ($\boldsymbol{\pi}$).
# MAGIC 5. Generar y exportar gráficos de alta calidad (estilo Nord, sin emojis) de las series de Alphas con umbrales, mapas de calor de transiciones y distribución de estados.
# MAGIC
# MAGIC **Salidas Gold:**
# MAGIC - `combustibles_hn.gold.tesis_cap3_centroides`: Media empírica de cada estado (actúa como centroide para compatibilidad aguas abajo).
# MAGIC - `combustibles_hn.gold.tesis_cap3_umbrales`: Límites y definiciones de los umbrales fijos.
# MAGIC - `combustibles_hn.gold.tesis_cap3_matrices_transicion`: Matrices de transición con conteos, totales y errores estándar (Umbrales Fijos).
# MAGIC - `combustibles_hn.gold.tesis_alphas_con_estados`: Alphas enriquecidos con los estados asignados por el método de Umbrales Fijos (`[fuel]_State`).
# MAGIC - `combustibles_hn.gold.tesis_cap3_propiedades_espectrales`: Autovalores, brecha espectral, tiempos de mezcla y distribuciones estacionarias.

# COMMAND ----------

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F

CATALOG = "combustibles_hn"
spark.sql(f"USE CATALOG {CATALOG}")

# Configurar directorio local para gráficos
CHARTS_DIR = "d:/2026/Databricks/Combustibles/local/charts"
os.makedirs(CHARTS_DIR, exist_ok=True)
print(f"Directorio de gráficos configurado en: {CHARTS_DIR}")

# Paleta de colores estilo Nord para visualizaciones elegantes
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

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Carga de Alphas Óptimos (del Capítulo 2)

# COMMAND ----------

df_alphas = spark.table(f"{CATALOG}.gold.tesis_alphas_combustibles_optimos").orderBy("Fecha").toPandas()
# Convertir Fecha a datetime para graficar correctamente
if 'Fecha' in df_alphas.columns:
    df_alphas['Fecha'] = pd.to_datetime(df_alphas['Fecha'])
print(f"Columnas disponibles: {list(df_alphas.columns)}")
print(f"Total registros cargados: {len(df_alphas)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Funciones de Discretización y Estimación con Errores Estándar

# COMMAND ----------

def get_markov_states_fixed(alphas_array, thresholds=[0.04, 0.02, -0.01]):
    """
    Discretiza por umbrales fijos (Capítulo 3 de la tesis).
    Mapeo de estados (ordenados de menor a mayor):
      Estado 0: x < -0.01 (Decrecimiento Severo)
      Estado 1: -0.01 <= x <= 0.02 (Decrecimiento Leve)
      Estado 2: 0.02 < x <= 0.04 (Crecimiento Moderado)
      Estado 3: x > 0.04 (Crecimiento Fuerte)
    """
    sorted_th = sorted(thresholds) # [-0.01, 0.02, 0.04]
    t0, t1, t2 = sorted_th
    
    states = np.zeros_like(alphas_array, dtype=int)
    states[alphas_array < t0] = 0
    states[(alphas_array >= t0) & (alphas_array <= t1)] = 1
    states[(alphas_array > t1) & (alphas_array <= t2)] = 2
    states[alphas_array > t2] = 3
    
    return states, sorted_th

def estimate_transition_matrix_with_errors(states, k=4):
    """
    Estima la matriz de transición de Markov y calcula sus errores estándar y estadísticas.
    P_ij = P(ir a estado j | estar en estado i) -> Suma de filas es 1.
    Nota: Se utiliza la convención matemática P[i, j] = P(S_{t+1} = j | S_t = i).
    """
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
# MAGIC ## 3. Procesamiento, Análisis Espectral y Generación de Gráficos

# COMMAND ----------

K_ESTADOS = 4
THRESHOLDS = [0.04, 0.02, -0.01]
ESTADO_NOMBRES = {
    0: "Decrecimiento Severo (s4)",
    1: "Decrecimiento Leve (s3)",
    2: "Crecimiento Moderado (s2)",
    3: "Crecimiento Fuerte (s1)"
}

resultados_centroides = []
resultados_matrices = []
resultados_propiedades_espectrales = []
resultados_umbrales = []

# Guardar información de definición de umbrales
for idx, limit in enumerate(sorted(THRESHOLDS)):
    resultados_umbrales.append({
        'Limite_Index': idx,
        'Valor_Limite': float(limit),
        'Descripcion': f"Umbral de división entre estados"
    })

combustibles = ['Super', 'Regular', 'Diesel', 'Kerosene']

for fuel in combustibles:
    col_alpha = f'{fuel}_Alpha'
    if col_alpha not in df_alphas.columns:
        print(f"ADVERTENCIA: {col_alpha} no encontrada, saltando {fuel}.")
        continue

    alphas = np.array(df_alphas[col_alpha].fillna(0).values, dtype=float)

    # === A. Asignación de Estados con Umbrales Fijos ===
    states, sorted_th = get_markov_states_fixed(alphas, THRESHOLDS)
    df_alphas[f'{fuel}_State'] = states
    print(f"\n=== {fuel} Umbrales Fijos: {K_ESTADOS} estados ===")
    
    # Calcular medias empíricas de cada estado (representantes o 'centroides' para compatibilidad)
    centroids = []
    for i in range(K_ESTADOS):
        alphas_in_state = alphas[states == i]
        if len(alphas_in_state) > 0:
            mean_val = np.mean(alphas_in_state)
        else:
            # Fallback teórico si el estado queda vacío en la muestra
            if i == 0: mean_val = -0.02
            elif i == 1: mean_val = 0.005
            elif i == 2: mean_val = 0.03
            else: mean_val = 0.05
        centroids.append(mean_val)
        resultados_centroides.append({
            'Combustible': fuel, 
            'Estado': i, 
            'Centroide_Alpha': float(mean_val)
        })
    print(f"Medias empíricas (centroides de estados): {np.round(centroids, 6)}")

    # === B. Estimación de Matriz de Transición con Errores Estándar ===
    P, C, n_orig, SE = estimate_transition_matrix_with_errors(states, k=K_ESTADOS)
    for i in range(K_ESTADOS):
        for j in range(K_ESTADOS):
            resultados_matrices.append({
                'Combustible': fuel, 
                'Estado_Origen': i, 
                'Estado_Destino': j,
                'Probabilidad': float(P[i, j]),
                'Error_Estandar': float(SE[i, j]),
                'Conteo_Transiciones': int(C[i, j]),
                'Total_Origen': int(n_orig[i])
            })

    # === C. Propiedades Espectrales ===
    eigenvalues = np.linalg.eigvals(P)
    # Ordenar por magnitud absoluta descendente
    sorted_eigs = np.sort(np.abs(eigenvalues))[::-1]
    
    # Distribución estacionaria (autovector izquierdo con autovalor 1, que es derecho de P.T)
    eig_vals, eig_vecs = np.linalg.eig(P.T)
    idx_one = np.argmin(np.abs(eig_vals - 1.0))
    pi_stationary = np.real(eig_vecs[:, idx_one])
    pi_stationary = pi_stationary / pi_stationary.sum()
    
    # Brecha espectral y tiempo de mezcla (con epsilon = 0.01)
    lambda_2 = sorted_eigs[1] if len(sorted_eigs) > 1 else 0
    spectral_gap = 1.0 - lambda_2
    mixing_time = -np.log(0.01) / spectral_gap if spectral_gap > 0 and lambda_2 < 1.0 else 0

    resultados_propiedades_espectrales.append({
        'Combustible': fuel,
        'K_Estados': K_ESTADOS,
        'Eigenvalue_Dominante': float(sorted_eigs[0]),
        'Eigenvalue_2': float(lambda_2),
        'Spectral_Gap': float(spectral_gap),
        'Mixing_Time_Approx': float(mixing_time),
        'Pi_Estacionaria': str([round(float(x), 6) for x in pi_stationary]),
    })
    
    print(f"Brecha espectral (gamma): {spectral_gap:.6f}")
    print(f"Tiempo de mezcla estimado: {mixing_time:.2f} semanas")
    print(f"Distribución Estacionaria pi: {[round(float(x), 4) for x in pi_stationary]}")

    # === D. GENERAR Y GUARDAR GRÁFICOS ===
    
    # 1. Gráfico de la serie de Alphas con líneas de umbral
    plt.figure(figsize=(12, 5))
    plt.plot(df_alphas['Fecha'], df_alphas[col_alpha], color=NORD_PALETTE['frost_blue'], label=rf'$\alpha_t$ {fuel}')
    plt.axhline(y=0.04, color=NORD_PALETTE['aurora_red'], linestyle='--', alpha=0.8, label=r'Umbral Fuerte ($\theta_1 = 0.04$)')
    plt.axhline(y=0.02, color=NORD_PALETTE['aurora_orange'], linestyle='-', alpha=0.5, label=r'Umbral Moderado ($\theta_2 = 0.02$)')
    plt.axhline(y=-0.01, color=NORD_PALETTE['aurora_purple'], linestyle='--', alpha=0.8, label=r'Umbral Leve ($\theta_3 = -0.01$)')
    
    # Colorear zonas de fondo para representar regímenes
    plt.axhspan(0.04, max(0.1, np.max(alphas)), color=NORD_PALETTE['aurora_green'], alpha=0.1, label='Región Fuerte')
    plt.axhspan(0.02, 0.04, color=NORD_PALETTE['aurora_yellow'], alpha=0.1, label='Región Moderada')
    plt.axhspan(-0.01, 0.02, color=NORD_PALETTE['aurora_orange'], alpha=0.1, label='Región Leve')
    plt.axhspan(np.min(alphas), -0.01, color=NORD_PALETTE['aurora_red'], alpha=0.1, label='Región Severa')

    plt.title(f"Serie de Tasas de Cambio (Alpha) con Regímenes Fijos - {fuel}", fontsize=13, fontweight='bold', pad=15)
    plt.xlabel("Fecha", fontsize=10)
    plt.ylabel("Tasa Alpha", fontsize=10)
    plt.legend(loc='upper right', bbox_to_anchor=(1.25, 1))
    plt.tight_layout()
    chart1_path = f"{CHARTS_DIR}/{fuel}_alpha_thresholds.png"
    plt.savefig(chart1_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Grafico 1 guardado en: {chart1_path}")

    # 2. Heatmap de Matriz de Transición P con anotación de Errores Estándar
    plt.figure(figsize=(7, 6))
    labels = [ESTADO_NOMBRES[i] for i in range(K_ESTADOS)]
    
    # Crear etiquetas combinando Probabilidad y Error Estándar
    annot_labels = np.empty_like(P, dtype=object)
    for i in range(K_ESTADOS):
        for j in range(K_ESTADOS):
            annot_labels[i, j] = f"{P[i, j]:.2f}\n±{SE[i, j]:.2f}"
            
    sns.heatmap(P, annot=annot_labels, fmt='', cmap="Blues", cbar=True,
                xticklabels=labels, yticklabels=labels,
                annot_kws={"size": 10, "fontweight": "semibold"})
    plt.title(f"Matriz de Transición de Markov (Umbrales Fijos) - {fuel}\n$P(S_{{t+1}} = j \\mid S_t = i)$", 
              fontsize=11, fontweight='bold', pad=15)
    plt.ylabel("Estado de Origen ($S_t = i$)", fontsize=10)
    plt.xlabel("Estado de Destino ($S_{t+1} = j$)", fontsize=10)
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    plt.tight_layout()
    chart2_path = f"{CHARTS_DIR}/{fuel}_transition_heatmap.png"
    plt.savefig(chart2_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Grafico 2 guardado en: {chart2_path}")

    # 3. Distribución empírica de estados
    plt.figure(figsize=(6, 4))
    state_counts = pd.Series(states).value_counts().reindex(range(K_ESTADOS)).fillna(0)
    colors = [NORD_PALETTE['aurora_red'], NORD_PALETTE['aurora_orange'], NORD_PALETTE['aurora_yellow'], NORD_PALETTE['aurora_green']]
    plt.bar([ESTADO_NOMBRES[i] for i in range(K_ESTADOS)], state_counts, color=colors, edgecolor='gray', alpha=0.85)
    plt.title(f"Frecuencia de Regímenes en la Muestra - {fuel}", fontsize=11, fontweight='bold', pad=12)
    plt.ylabel("Semanas", fontsize=10)
    plt.xticks(rotation=30, ha='right')
    plt.tight_layout()
    chart3_path = f"{CHARTS_DIR}/{fuel}_state_distribution.png"
    plt.savefig(chart3_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Grafico 3 guardado en: {chart3_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Guardar Resultados en Capa Gold

# COMMAND ----------

# 4a. Centroides (Medias empíricas para compatibilidad con notebooks posteriores)
spark.createDataFrame(pd.DataFrame(resultados_centroides)).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap3_centroides")
print("✅ gold.tesis_cap3_centroides")

# 4b. Definición de Umbrales Fijos
spark.createDataFrame(pd.DataFrame(resultados_umbrales)).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap3_umbrales")
print("✅ gold.tesis_cap3_umbrales")

# 4c. Matrices de Transición con Errores Estándar (Umbrales Fijos)
spark.createDataFrame(pd.DataFrame(resultados_matrices)).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap3_matrices_transicion")
print("✅ gold.tesis_cap3_matrices_transicion")

# 4d. Alphas con Estados (Umbrales Fijos registrados en la columna '_State')
spark.createDataFrame(df_alphas).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_alphas_con_estados")
print("✅ gold.tesis_alphas_con_estados")

# 4e. Propiedades Espectrales
spark.createDataFrame(pd.DataFrame(resultados_propiedades_espectrales)).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap3_propiedades_espectrales")
print("✅ gold.tesis_cap3_propiedades_espectrales")
