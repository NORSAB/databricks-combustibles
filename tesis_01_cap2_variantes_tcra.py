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
# MAGIC 3. Seleccionar los parámetros óptimos por combustible para cada una de las 4 variantes, y elegir la mejor global.
# MAGIC 4. Guardar los alphas óptimos calculados para cada variante (y el ganador absoluto) y el rendimiento en tablas Gold.
# MAGIC
# MAGIC **Salidas Gold:**
# MAGIC - `combustibles_hn.gold.tesis_cap2_grid_performance`: MAPE y RMSE de todas las combinaciones y variantes evaluadas.
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
    """Selecciona los parámetros óptimos para cada una de las 4 variantes y destaca la mejor global."""
    df_fuel = df_results[df_results['Combustible'] == fuel_name]
    
    # Agrupar por W, Lambda y Variante para obtener promedios de CV
    df_grouped = df_fuel.groupby(['W', 'Lambda', 'Variante']).agg({'MAPE': 'mean', 'RMSE': 'mean'}).reset_index()
    
    # Encontrar el óptimo para cada una de las 4 variantes
    opt_rows = []
    
    # 1. TCRA (fijo W=2, Lambda=1.0)
    df_tcra = df_grouped[df_grouped['Variante'] == 'TCRA']
    if not df_tcra.empty:
        idx = df_tcra['MAPE'].idxmin()
        opt_rows.append(df_tcra.loc[idx])
    else:
        opt_rows.append(pd.Series({'W': 2, 'Lambda': 1.0, 'Variante': 'TCRA', 'MAPE': 99.0, 'RMSE': 9.9}))
        
    # 2. ETCRA (fijo W=2, Lambda < 1.0)
    df_etcra = df_grouped[df_grouped['Variante'] == 'ETCRA']
    if not df_etcra.empty:
        idx = df_etcra['MAPE'].idxmin()
        opt_rows.append(df_etcra.loc[idx])
    else:
        opt_rows.append(pd.Series({'W': 2, 'Lambda': 0.70, 'Variante': 'ETCRA', 'MAPE': 99.0, 'RMSE': 9.9}))
        
    # 3. TCRAM (ventana móvil W >= 10, Lambda = 1.0)
    df_tcram = df_grouped[df_grouped['Variante'] == 'TCRAM']
    if not df_tcram.empty:
        idx = df_tcram['MAPE'].idxmin()
        opt_rows.append(df_tcram.loc[idx])
    else:
        opt_rows.append(pd.Series({'W': 52, 'Lambda': 1.0, 'Variante': 'TCRAM', 'MAPE': 99.0, 'RMSE': 9.9}))
        
    # 4. ETCRAM (ventana móvil W >= 10, Lambda < 1.0)
    df_etcram = df_grouped[df_grouped['Variante'] == 'ETCRAM']
    if not df_etcram.empty:
        idx = df_etcram['MAPE'].idxmin()
        opt_rows.append(df_etcram.loc[idx])
    else:
        opt_rows.append(pd.Series({'W': 52, 'Lambda': 0.99, 'Variante': 'ETCRAM', 'MAPE': 99.0, 'RMSE': 9.9}))
        
    df_opt = pd.DataFrame(opt_rows)
    df_opt['Combustible'] = fuel_name
    
    # Encontrar la fila con el menor MAPE global entre las 4
    best_idx = df_opt['MAPE'].idxmin()
    df_opt['Es_Mejor_Global'] = False
    df_opt.loc[best_idx, 'Es_Mejor_Global'] = True
    
    # Renombrar columnas para claridad
    df_opt = df_opt.rename(columns={
        'W': 'W_opt',
        'Lambda': 'Lambda_opt',
        'MAPE': 'MAPE_min',
        'RMSE': 'RMSE_min'
    })
    
    return df_opt

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Procesamiento del Grid Search

# COMMAND ----------

combustibles = ['Super', 'Regular', 'Diesel', 'Kerosene']
all_grid_results = []
summary_best_dfs = []

for fuel in combustibles:
    if fuel in df_silver.columns:
        print(f"Iniciando Grid Search (4 variantes) para {fuel}...")
        grid_res = run_grid_search(fuel, df_silver[fuel])
        all_grid_results.extend(grid_res)
        
        # Seleccionar mejores hiperparámetros por variante
        df_opt_fuel = select_optimal_params(pd.DataFrame(grid_res), fuel)
        summary_best_dfs.append(df_opt_fuel)
        
        row_global = df_opt_fuel[df_opt_fuel['Es_Mejor_Global']].iloc[0]
        row_etcram = df_opt_fuel[df_opt_fuel['Variante'] == 'ETCRAM'].iloc[0]
        
        print(f"  Óptimo Global ({row_global['Variante']}): W={row_global['W_opt']}, Lambda={row_global['Lambda_opt']:.2f} (MAPE={row_global['MAPE_min']:.4f}%)")
        print(f"  Óptimo ETCRAM (Ventana): W={row_etcram['W_opt']}, Lambda={row_etcram['Lambda_opt']:.2f} (MAPE={row_etcram['MAPE_min']:.4f}%)")
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
