# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Cap 6 — Extensión Reservorio SSRC (Grid Search)
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:** Optimización y validación del modelo de Computación de Reservorio Estocástico (SSRC).
# MAGIC
# MAGIC Incluye simulación de `rho` (radio espectral), `leak_rate` y `D` (dimensión del reservorio).

# COMMAND ----------

import numpy as np
import pandas as pd
from scipy.optimize import nnls
from pyspark.sql import functions as F

CATALOG = "combustibles_hn"
spark.sql(f"USE CATALOG {CATALOG}")

# CONSTANTES SSRC
SEED = 42
np.random.seed(SEED)

RESERVOIR_D_VALUES = [10, 20, 50]
RESERVOIR_RHO_VALUES = [0.8, 0.9, 0.99]
RESERVOIR_LEAK_RATES = [0.5, 0.8, 1.0]

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Lógica del Reservorio y Readout (NNLS)

# COMMAND ----------

def create_reservoir(input_dim, reservoir_dim, spectral_radius, sparsity=0.9, seed=SEED):
    np.random.seed(seed)
    W_in = np.random.uniform(-1, 1, (reservoir_dim, input_dim))
    
    W_res = np.random.uniform(-1, 1, (reservoir_dim, reservoir_dim))
    # Aplicar sparsity
    mask = np.random.rand(reservoir_dim, reservoir_dim) < sparsity
    W_res[mask] = 0
    
    # Ajustar radio espectral
    eigenvalues = np.linalg.eigvals(W_res)
    max_eigenvalue = np.max(np.abs(eigenvalues))
    if max_eigenvalue > 0:
        W_res = W_res * (spectral_radius / max_eigenvalue)
        
    return W_in, W_res

def evaluate_ssrc(fuel_name, alphas, prices, D, rho, leak_rate, seed):
    """Evaluación de una configuración del reservorio (Def 6.3 NNLS)."""
    W_in, W_res = create_reservoir(1, D, rho, sparsity=0.9, seed=seed)
    T = len(alphas)
    
    # Propagacion NumPy 
    H = np.zeros((D, T))
    h = np.zeros(D)
    W_in_col = W_in.ravel()
    
    for t in range(T):
        pre = W_in_col * alphas[t] + W_res @ h
        h = (1.0 - leak_rate) * h + leak_rate * np.tanh(pre)
        H[:, t] = h
        
    # Readout NNLS simple sobre toda la serie (Simplificacion para Databricks)
    # Target: alphas a predecir
    targets = alphas[1:]
    H_train = H[:, :-1]
    
    try:
        W_out, _ = nnls(H_train.T, targets)
        alpha_pred = W_out @ H_train
        
        # Calcular RMSE sobre Alphas
        rmse = np.sqrt(np.mean((targets - alpha_pred)**2))
        return {'Combustible': fuel_name, 'D': D, 'rho': rho, 'leak_rate': leak_rate, 'RMSE': rmse}
    except Exception as e:
        return None

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Grid Search SSRC

# COMMAND ----------

# Carga de datos Alphas del Cap 2 (asumiendo que ya se guardaron en tesis_alphas_combustibles)
try:
    df_alphas = spark.table(f"{CATALOG}.gold.tesis_alphas_combustibles").toPandas()
except:
    print("Por favor, ejecuta primero Cap 2 para generar tesis_alphas_combustibles.")
    dbutils.notebook.exit("Faltan datos de Alphas")

resultados_ssrc = []
combustibles = ['Super', 'Regular', 'Diesel', 'Kerosene']

print("Iniciando Grid Search SSRC (Cap 6)...")
for fuel in combustibles:
    col_alpha = f"{fuel}_Alpha"
    col_price = fuel
    
    if col_alpha not in df_alphas.columns: continue
    
    alphas = np.array(df_alphas[col_alpha].fillna(0).values, dtype=float)
    prices = np.array(df_alphas[col_price].fillna(0).values, dtype=float)
    
    for D in RESERVOIR_D_VALUES:
        for rho in RESERVOIR_RHO_VALUES:
            for leak in RESERVOIR_LEAK_RATES:
                res = evaluate_ssrc(fuel, alphas, prices, D, rho, leak, SEED)
                if res:
                    resultados_ssrc.append(res)

df_res_ssrc = pd.DataFrame(resultados_ssrc)
df_best_ssrc = df_res_ssrc.loc[df_res_ssrc.groupby('Combustible')['RMSE'].idxmin()]

print("\n--- MEJORES CONFIGURACIONES SSRC (Menor RMSE) ---")
display(df_best_ssrc)

# Guardar resultados
df_best_ssrc_spark = spark.createDataFrame(df_best_ssrc)
(df_best_ssrc_spark.write
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.gold.tesis_cap6_best_ssrc"))
print("Guardado en combustibles_hn.gold.tesis_cap6_best_ssrc")
