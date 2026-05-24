# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Cap 2 — TCRA Variantes: Optimización por Validación Cruzada Temporal (Grid Search)
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:** 
# MAGIC 1. Realizar una búsqueda en rejilla (Grid Search) sobre los parámetros del operador TCRA ($W$ y $\lambda$) usando 8 particiones expansivas (CV).
# MAGIC 2. Evaluar el desempeño continuo (MAPE y RMSE) de la predicción un-paso-adelante para cada combustible.
# MAGIC 3. Seleccionar los parámetros óptimos por combustible (minimización de MAPE promedio de CV).
# MAGIC 4. Guardar los alphas óptimos calculados y el desempeño del Grid Search en el catálogo.
# MAGIC
# MAGIC **Salidas Gold:**
# MAGIC - `combustibles_hn.gold.tesis_cap2_grid_performance`: MAPE y RMSE de todas las combinaciones evaluadas por partición.
# MAGIC - `combustibles_hn.gold.tesis_cap2_best_hyperparams`: Parámetros óptimos seleccionados por CV y por la heurística del usuario (promedio de óptimos locales).
# MAGIC - `combustibles_hn.gold.tesis_alphas_combustibles_optimos`: Tabla con los precios y los alphas óptimos calculados.

# COMMAND ----------

import numpy as np
import pandas as pd
from scipy.signal import lfilter
from pyspark.sql import functions as F

CATALOG = "combustibles_hn"
spark.sql(f"USE CATALOG {CATALOG}")

# Semilla global para reproducibilidad
SEED = 42
np.random.seed(SEED)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Lectura de Datos Silver (Histórico + Scraping)

# COMMAND ----------

# Consumimos la capa Silver real del proyecto Combustibles
df_silver = spark.table(f"{CATALOG}.silver.silver_precios_semanales").orderBy("Fecha").toPandas()
print(f"Datos cargados: {len(df_silver)} semanas.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Funciones de Optimización y Cálculo Rápido

# COMMAND ----------

def calculate_alphas_fast(v, W, lambd):
    """Calcula los alphas de forma ultra-rápida usando filtrado digital lineal 1D."""
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

def run_grid_search(fuel_name, series):
    """Ejecuta la búsqueda en grilla de W y Lambda a lo largo de 8 particiones temporales."""
    v = np.asarray(series.ffill().bfill().values, dtype=float)
    n = len(v)
    
    ratios = [0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]
    splits = [int(n * r) for r in ratios]
    
    w_range = np.arange(10, 61, 1) # W de 10 a 60
    lambda_range = np.round(np.arange(0.70, 1.01, 0.01), 2) # lambda de 0.70 a 1.00
    
    results = []
    
    a = v[1:] * v[:-1]
    b = v[:-1] ** 2
    
    for W in w_range:
        for lambd in lambda_range:
            filter_coeffs = lambd ** np.arange(W)
            num = lfilter(filter_coeffs, [1.0], a)
            den = lfilter(filter_coeffs, [1.0], b)
            alphas = np.zeros(n)
            alphas[W:] = -1.0 + num[W-1:] / (den[W-1:] + 1e-15)
            
            for s_idx, split_idx in enumerate(splits):
                ratio = ratios[s_idx]
                if split_idx < W:
                    continue
                actual = v[split_idx + 1:]
                pred = v[split_idx:-1] * (1.0 + alphas[split_idx:-1])
                
                mape = np.mean(np.abs((actual - pred) / actual)) * 100
                rmse = np.sqrt(np.mean((actual - pred)**2))
                
                results.append({
                    'Combustible': fuel_name,
                    'W': int(W),
                    'Lambda': float(lambd),
                    'Particion': f"{int(ratio*100)}/{100-int(ratio*100)}",
                    'Ratio': ratio,
                    'MAPE': float(mape),
                    'RMSE': float(rmse)
                })
    return results

def select_optimal_params(df_results, fuel_name):
    """Selecciona los parámetros óptimos usando CV (Error Promedio) y la propuesta del Usuario."""
    df_fuel = df_results[df_results['Combustible'] == fuel_name]
    
    # Método A: Minimización de CV promedio MAPE
    df_grouped = df_fuel.groupby(['W', 'Lambda']).agg({'MAPE': 'mean', 'RMSE': 'mean'}).reset_index()
    best_idx_mape = df_grouped['MAPE'].idxmin()
    best_row_mape = df_grouped.loc[best_idx_mape]
    w_opt_mape = int(best_row_mape['W'])
    lambda_opt_mape = float(best_row_mape['Lambda'])
    min_avg_mape = float(best_row_mape['MAPE'])
    
    best_idx_rmse = df_grouped['RMSE'].idxmin()
    best_row_rmse = df_grouped.loc[best_idx_rmse]
    w_opt_rmse = int(best_row_rmse['W'])
    lambda_opt_rmse = float(best_row_rmse['Lambda'])
    min_avg_rmse = float(best_row_rmse['RMSE'])
    
    # Método B: Propuesta del Usuario (promediar mejores parámetros de cada partición)
    best_per_partition = []
    partitions = df_fuel['Particion'].unique()
    for part in partitions:
        df_part = df_fuel[df_fuel['Particion'] == part]
        best_row_part = df_part.loc[df_part['MAPE'].idxmin()]
        best_per_partition.append({
            'W': best_row_part['W'],
            'Lambda': best_row_part['Lambda']
        })
    
    df_best_parts = pd.DataFrame(best_per_partition)
    w_user = int(round(df_best_parts['W'].mean()))
    lambda_user = float(df_best_parts['Lambda'].mean())
    
    return {
        'Combustible': fuel_name,
        'W_opt_MAPE': w_opt_mape,
        'Lambda_opt_MAPE': lambda_opt_mape,
        'Avg_MAPE_min': min_avg_mape,
        'W_opt_RMSE': w_opt_rmse,
        'Lambda_opt_RMSE': lambda_opt_rmse,
        'Avg_RMSE_min': min_avg_rmse,
        'W_user': w_user,
        'Lambda_user': lambda_user
    }

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Procesamiento del Grid Search

# COMMAND ----------

combustibles = ['Super', 'Regular', 'Diesel', 'Kerosene']
all_grid_results = []
summary_best = []

for fuel in combustibles:
    if fuel in df_silver.columns:
        print(f"Iniciando Grid Search para {fuel}...")
        grid_res = run_grid_search(fuel, df_silver[fuel])
        all_grid_results.extend(grid_res)
        
        # Seleccionar mejores hiperparámetros
        best_params = select_optimal_params(pd.DataFrame(grid_res), fuel)
        summary_best.append(best_params)
        
        print(f"  CV óptimo (MAPE): W={best_params['W_opt_MAPE']}, Lambda={best_params['Lambda_opt_MAPE']:.2f} (MAPE={best_params['Avg_MAPE_min']:.4f}%)")
        print(f"  Usuario óptimo (Promedio): W={best_params['W_user']}, Lambda={best_params['Lambda_user']:.2f}")
    else:
        print(f"ADVERTENCIA: Columna {fuel} no encontrada en Silver.")

df_grid_all = pd.DataFrame(all_grid_results)
df_summary = pd.DataFrame(summary_best)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Generación de Alphas con Parámetros Óptimos

# COMMAND ----------

# Creamos una copia del dataframe de precios
df_alphas_optimos = df_silver[['Fecha', 'Super', 'Regular', 'Diesel', 'Kerosene']].copy()

for _, row in df_summary.iterrows():
    fuel = row['Combustible']
    w_opt = int(row['W_opt_MAPE'])
    lambda_opt = float(row['Lambda_opt_MAPE'])
    
    alphas = calculate_alphas_fast(df_alphas_optimos[fuel], w_opt, lambda_opt)
    df_alphas_optimos[f'{fuel}_Alpha'] = alphas
    print(f"Alphas óptimos calculados para {fuel} (W={w_opt}, Lambda={lambda_opt:.2f})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Almacenamiento en Capa Gold

# COMMAND ----------

# 5a. Guardar todas las combinaciones del Grid Search
spark.createDataFrame(df_grid_all).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap2_grid_performance")
print("✅ Tabla guardada: gold.tesis_cap2_grid_performance")

# 5b. Guardar el resumen de mejores hiperparámetros
spark.createDataFrame(df_summary).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_cap2_best_hyperparams")
print("✅ Tabla guardada: gold.tesis_cap2_best_hyperparams")

# 5c. Guardar la nueva tabla de alphas calculada con los hiperparámetros óptimos
spark.createDataFrame(df_alphas_optimos).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_alphas_combustibles_optimos")
print("✅ Tabla guardada: gold.tesis_alphas_combustibles_optimos")
