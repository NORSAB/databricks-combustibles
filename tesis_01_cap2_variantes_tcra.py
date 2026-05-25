# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Cap 2 — TCRA Variantes: Optimización por Validación Cruzada Temporal (Grid Search de las 4 Variantes)
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:** 
# MAGIC 1. Realizar una búsqueda en rejilla (Grid Search) sobre los parámetros de las 4 variantes de la familia TCRA:
# MAGIC    * **TCRA:** Ventana fija $W=2$, sin decaimiento ($\lambda = 1.0$)
# MAGIC    * **ETCRA:** Ventana fija $W=2$, con decaimiento ($\lambda < 1.0$)
# MAGIC    * **TCRAM:** Ventana móvil $W \ge 10$, sin decaimiento ($\lambda = 1.0$)
# MAGIC    * **ETCRAM:** Ventana móvil $W \ge 10$, con decaimiento ($\lambda < 1.0$)
# MAGIC 2. Evaluar el desempeño continuo (MAPE y RMSE) de la predicción un-paso-adelante para cada combustible en 8 particiones temporales (CV).
# MAGIC 3. Seleccionar los parámetros óptimos por combustible para cada una de las 4 variantes usando el protocolo de promedio de hiperparámetros óptimos por partición, y elegir la mejor global.
# MAGIC 4. Guardar los alphas óptimos calculados para cada variante (y el ganador absoluto) y el rendimiento en tablas Gold.
# MAGIC
# MAGIC **Salidas Gold:**
# MAGIC - `combustibles_hn.gold.tesis_cap2_grid_performance`: MAPE y RMSE de todas las combinaciones y variantes evaluadas (todas las ejecuciones de la grilla).
# MAGIC - `combustibles_hn.gold.tesis_cap2_best_hyperparams`: Parámetros óptimos para cada una de las 4 variantes.
# MAGIC - `combustibles_hn.gold.tesis_alphas_combustibles_optimos`: Precios y alphas óptimos calculados para cada una de las 4 variantes de la familia TCRA.
# MAGIC

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
# MAGIC ## 2. Funciones de Optimización y Cálculo de Variantes

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
    """Ejecuta la búsqueda en grilla evaluando y clasificando las 4 variantes de la TCRA."""
    v = np.asarray(series.ffill().bfill().values, dtype=float)
    n = len(v)
    
    ratios = [0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]
    splits = [int(n * r) for r in ratios]
    
    # Rango de W: 2 (para TCRA/ETCRA) y luego de 10 a 60 (para TCRAM/ETCRAM)
    w_range = [2] + list(range(10, 61))
    lambda_range = np.round(np.arange(0.70, 1.01, 0.01), 2)
    
    results = []
    
    a = v[1:] * v[:-1]
    b = v[:-1] ** 2
    
    for W in w_range:
        if 3 <= W <= 9:
            continue
        for lambd in lambda_range:
            # Clasificar variantes
            if W == 2:
                variant = "TCRA" if lambd == 1.0 else "ETCRA"
            else:
                variant = "TCRAM" if lambd == 1.0 else "ETCRAM"
                
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
                    'Variante': variant,
                    'Particion': f"{int(ratio*100)}/{100-int(ratio*100)}",
                    'Ratio': ratio,
                    'MAPE': float(mape),
                    'RMSE': float(rmse)
                })
    return results

def select_optimal_params(df_results, fuel_name):
    """Selecciona los parámetros óptimos para cada una de las 4 variantes
    aplicando el protocolo de promedio de hiperparámetros óptimos por partición,
    y luego elige la mejor variante global."""
    # Excluir explícitamente cualquier registro histórico inválido con W entre 3 y 9
    df_clean = df_results[~((df_results['W'] >= 3) & (df_results['W'] <= 9))].copy()
    df_fuel = df_clean[df_clean['Combustible'] == fuel_name]
    partitions = df_fuel['Particion'].unique()
    variants = ['TCRA', 'ETCRA', 'TCRAM', 'ETCRAM']
    
    opt_rows = []
    
    for var in variants:
        best_per_partition = []
        for part in partitions:
            df_part = df_fuel[(df_fuel['Particion'] == part) & (df_fuel['Variante'] == var)]
            if not df_part.empty:
                # Encontrar la combinación que minimiza el MAPE en este fold
                best_row_part = df_part.loc[df_part['MAPE'].idxmin()]
                best_per_partition.append({
                    'W': best_row_part['W'],
                    'Lambda': best_row_part['Lambda']
                })
                
        if best_per_partition:
            df_best_parts = pd.DataFrame(best_per_partition)
            # Promedio de hiperparámetros
            w_opt = int(round(df_best_parts['W'].mean()))
            lambda_opt = float(np.round(df_best_parts['Lambda'].mean(), 2))
        else:
            w_opt = 2 if var in ['TCRA', 'ETCRA'] else 52
            lambda_opt = 1.0 if var in ['TCRA', 'TCRAM'] else 0.99
            
        # Encontrar el rendimiento de esta combinación promedio (W_opt, Lambda_opt) en la grilla
        df_opt_comb = df_fuel[(df_fuel['W'] == w_opt) & 
                              (df_fuel['Lambda'] == lambda_opt) & 
                              (df_fuel['Variante'] == var)]
        
        if not df_opt_comb.empty:
            # Promedio de MAPE y RMSE a través de todas las particiones para esta combinación
            mape_min = df_opt_comb['MAPE'].mean()
            rmse_min = df_opt_comb['RMSE'].mean()
        else:
            # Fallback por si la combinación promedio no está en la grilla
            # Buscamos la combinación más cercana en la grilla para esta variante
            df_var_grouped = df_fuel[df_fuel['Variante'] == var].groupby(['W', 'Lambda']).agg({'MAPE': 'mean', 'RMSE': 'mean'}).reset_index()
            df_var_grouped['dist'] = (df_var_grouped['W'] - w_opt)**2 + (df_var_grouped['Lambda'] - lambda_opt)**2 * 1000
            best_group_row = df_var_grouped.loc[df_var_grouped['dist'].idxmin()]
            w_opt = int(best_group_row['W'])
            lambda_opt = float(best_group_row['Lambda'])
            mape_min = best_group_row['MAPE']
            rmse_min = best_group_row['RMSE']
            
        opt_rows.append({
            'Combustible': fuel_name,
            'Variante': var,
            'W_opt': w_opt,
            'Lambda_opt': lambda_opt,
            'MAPE_min': mape_min,
            'RMSE_min': rmse_min
        })
        
    df_opt = pd.DataFrame(opt_rows)
    
    # Elegir la mejor variante global basada en el menor MAPE promedio
    best_idx = df_opt['MAPE_min'].idxmin()
    df_opt['Es_Mejor_Global'] = False
    df_opt.loc[best_idx, 'Es_Mejor_Global'] = True
    
    return df_opt

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Procesamiento del Grid Search

# COMMAND ----------

# Limpieza explícita de tablas Gold de salida antes de iniciar el procesamiento
print("Limpiando tablas Gold preexistentes antes de iniciar la búsqueda en rejilla...")
for table_name in ['tesis_cap2_grid_performance', 'tesis_cap2_best_hyperparams', 'tesis_alphas_combustibles_optimos', 'tesis_alphas_combustibles']:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.gold.{table_name}")
        print(f"  * Tabla {table_name} eliminada exitosamente.")
    except Exception as e:
        print(f"  * Advertencia al intentar limpiar {table_name}: {e}")

combustibles = ['Super', 'Regular', 'Diesel', 'Kerosene']
all_grid_results = []
summary_best_dfs = []

for fuel in combustibles:
    if fuel in df_silver.columns:
        print(f"Iniciando Grid Search (4 variantes) para {fuel}...")
        grid_res = run_grid_search(fuel, df_silver[fuel])
        all_grid_results.extend(grid_res)
        
        # Seleccionar mejores hiperparámetros usando el método promedio de particiones
        df_opt_fuel = select_optimal_params(pd.DataFrame(grid_res), fuel)
        summary_best_dfs.append(df_opt_fuel)
        
        row_global = df_opt_fuel[df_opt_fuel['Es_Mejor_Global']].iloc[0]
        row_etcram = df_opt_fuel[df_opt_fuel['Variante'] == 'ETCRAM'].iloc[0]
        
        print(f"  Óptimo Global ({row_global['Variante']}): W={row_global['W_opt']}, Lambda={row_global['Lambda_opt']:.2f} (MAPE={row_global['MAPE_min']:.4f}%)")
        print(f"  Óptimo ETCRAM (Promedio Folds): W={row_etcram['W_opt']}, Lambda={row_etcram['Lambda_opt']:.2f} (MAPE={row_etcram['MAPE_min']:.4f}%)")
    else:
        print(f"ADVERTENCIA: Columna {fuel} no encontrada en Silver.")

df_grid_all = pd.DataFrame(all_grid_results)
df_summary = pd.concat(summary_best_dfs, ignore_index=True)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Generación de Alphas Óptimos de las Variantes

# COMMAND ----------

# Creamos el dataframe de precios y alphas optimizados para las 4 variantes
df_alphas_optimos = df_silver[['Fecha', 'Super', 'Regular', 'Diesel', 'Kerosene']].copy()

for fuel in combustibles:
    if fuel not in df_silver.columns:
        continue
    df_fuel_opt = df_summary[df_summary['Combustible'] == fuel]
    
    for _, row in df_fuel_opt.iterrows():
        var_name = row['Variante']
        w_opt = int(row['W_opt'])
        lambda_opt = float(row['Lambda_opt'])
        
        # Calcular alphas usando esta variante específica
        alphas = calculate_alphas_fast(df_alphas_optimos[fuel], w_opt, lambda_opt)
        df_alphas_optimos[f'{fuel}_Alpha_{var_name}'] = alphas
        
        # Si es el mejor global, guardar en [Fuel]_Alpha_Best
        if row['Es_Mejor_Global']:
            df_alphas_optimos[f'{fuel}_Alpha_Best'] = alphas
            
        # Para compatibilidad con notebooks siguientes, guardamos el óptimo de ETCRAM en [Fuel]_Alpha
        if var_name == 'ETCRAM':
            df_alphas_optimos[f'{fuel}_Alpha'] = alphas
            print(f"Alphas optimizados calculados para {fuel} (ETCRAM: W={w_opt}, Lambda={lambda_opt:.2f})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Almacenamiento en Capa Gold

# COMMAND ----------

# 5a. Guardar todas las combinaciones del Grid Search (todas las ejecuciones)
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

# 5c. Guardar la nueva tabla de alphas calculada con los hiperparámetros de las variantes
spark.createDataFrame(df_alphas_optimos).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_alphas_combustibles_optimos")
print("✅ Tabla guardada: gold.tesis_alphas_combustibles_optimos")

spark.createDataFrame(df_alphas_optimos).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.tesis_alphas_combustibles")
print("✅ Tabla guardada: gold.tesis_alphas_combustibles")
