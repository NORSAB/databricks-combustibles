# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Cap 4 — Modelo Híbrido TCROC-NNLS
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras + Modelos de Tesis
# MAGIC **Objetivo:** Validación predictiva con Non-Negative Least Squares (NNLS) sobre la matriz de transición y los centros de estado.

# COMMAND ----------

import numpy as np
import pandas as pd
from scipy.optimize import nnls
from pyspark.sql import functions as F

CATALOG = "combustibles_hn"
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Aplicación NNLS

# COMMAND ----------

def predict_next_state_nnls(P_matrix, current_state_vector):
    """Aplica optimización NNLS para predecir el próximo vector de probabilidades de estado."""
    # A x = b (NNLS constraints x >= 0)
    # En Markov: P^T * state_t = state_{t+1}
    # Adaptación NNLS para suavizar transiciones
    A = P_matrix.T
    b = current_state_vector
    x, residual = nnls(A, b)
    
    # Normalizar para que sea una distribución de probabilidad válida
    if np.sum(x) > 0:
        x = x / np.sum(x)
    return x

# Ejemplo con matriz P identidad (mock para compilación)
P_mock = np.array([[0.8, 0.2], [0.1, 0.9]])
curr_state = np.array([1.0, 0.0])
next_state = predict_next_state_nnls(P_mock, curr_state)
print("Estado predicho (NNLS):", next_state)
