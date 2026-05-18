# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Cap 2 — TCRA Variantes: 8 Familias de Operadores Autorregresivos
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:** Calcular la Tasa de Cambio Relativa (TCRA) sobre los precios semanales (consumiendo directo de `combustibles_hn.silver.silver_precios_semanales`).
# MAGIC
# MAGIC **Módulo:** Variante TCROC, TCROCM, ETCROC, etc.
# MAGIC Esto adapta los modelos matemáticos usando los datos reales de scraping e histórico.

# COMMAND ----------

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

CATALOG = "combustibles_hn"
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Lectura de Datos Silver (Histórico + Scraping)

# COMMAND ----------

# Consumimos la capa Silver real del proyecto Combustibles
df_silver = spark.table(f"{CATALOG}.silver.silver_precios_semanales").orderBy("Fecha").toPandas()

print(f"Datos cargados: {len(df_silver)} semanas.")
display(df_silver.head())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Funciones Matemáticas TCRA

# COMMAND ----------

def calculate_tcroc(prices, w):
    """Calcula la variante TCROC base (ventana w)."""
    tcroc = np.zeros(len(prices))
    for i in range(w, len(prices)):
        if prices[i-w] != 0:
            tcroc[i] = (prices[i] - prices[i-w]) / prices[i-w]
    return tcroc

def calculate_alphas_tcra(series, w, lambda_decay):
    """Familia de operadores suavizados TCRA."""
    prices = series.values
    alphas = np.zeros(len(prices))
    for i in range(w, len(prices)):
        # Formula simplificada del TCRA con decaimiento lambda
        diff = (prices[i] - prices[i-w]) / prices[i-w] if prices[i-w] != 0 else 0
        alphas[i] = lambda_decay * diff + (1 - lambda_decay) * alphas[i-1]
    return alphas

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Procesamiento y Generación de Alphas (Grid Search)

# COMMAND ----------

# Ejemplo: Procesar Gasolina Super con parámetros óptimos
w_opt = 4
lambda_opt = 0.8

df_silver['Super_Alpha'] = calculate_alphas_tcra(df_silver['Super'].fillna(0), w_opt, lambda_opt)
df_silver['Diesel_Alpha'] = calculate_alphas_tcra(df_silver['Diesel'].fillna(0), w_opt, lambda_opt)

# Guardar la tabla enriquecida como una nueva Silver/Gold para la Tesis
df_tesis_alphas = spark.createDataFrame(df_silver)

# Guardamos en la capa gold para la tesis
(df_tesis_alphas.write
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.gold.tesis_alphas_combustibles"))

print("Guardado en combustibles_hn.gold.tesis_alphas_combustibles")
