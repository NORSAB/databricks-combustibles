# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Tesis Dashboards — Vistas Gold Completas (Tableau & Power BI)
# MAGIC
# MAGIC **Objetivo:** Persistir las consultas analíticas de todos los capítulos de la tesis como Vistas (Views) en la capa Gold.
# MAGIC Esto permite que herramientas externas como Power BI o Tableau puedan consumirlas directamente sin necesidad de recalcularlas ni duplicar almacenamiento.
# MAGIC
# MAGIC **Convención de nombres:**
# MAGIC * `v_cap2_*` - Capítulo 2 (Análisis de Alphas)
# MAGIC * `v_cap3_*` - Capítulo 3 (Clustering K-Means)
# MAGIC * `v_cap4_*` - Capítulo 4 (NNLS/Markov Híbrido)
# MAGIC * `v_cap6_*` - Capítulo 6 (SSRC - State Space Reservoir Computing)

# COMMAND ----------

# Usamos spark.sql en Python para evitar el bug de truncamiento de archivos grandes con %sql en Databricks Free Edition
CATALOG = "combustibles_hn"
spark.sql(f"USE CATALOG {CATALOG}")

print(f"Catálogo {CATALOG} seleccionado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Vistas de Histogramas (Capítulo 2)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap2_histograma_kerosene_alpha AS
WITH ranges AS (
  SELECT
    MIN(Kerosene_Alpha) AS val_min,
    MAX(Kerosene_Alpha) AS val_max
  FROM
    combustibles_hn.gold.tesis_alphas_combustibles
),
binned AS (
  SELECT
    LEAST(FLOOR((Kerosene_Alpha - val_min) / ((val_max - val_min) / 10)), 9) AS bin_order,
    val_min
      + (
        LEAST(FLOOR((Kerosene_Alpha - val_min) / ((val_max - val_min) / 10)), 9)
          * ((val_max - val_min) / 10)
      ) AS bin_start,
    val_min
      + (
        (LEAST(FLOOR((Kerosene_Alpha - val_min) / ((val_max - val_min) / 10)), 9) + 1)
          * ((val_max - val_min) / 10)
      ) AS bin_end
  FROM
    combustibles_hn.gold.tesis_alphas_combustibles CROSS JOIN ranges
)
SELECT
  bin_order,
  CONCAT('[', ROUND(bin_start, 2), ', ', ROUND(bin_end, 2), ')') AS bin_label,
  COUNT(*) AS frecuencia
FROM
  binned
GROUP BY
  bin_order,
  bin_start,
  bin_end
ORDER BY
  bin_order
""")

print("Vista creada: v_cap2_histograma_kerosene_alpha")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap2_histograma_diesel_alpha AS
WITH ranges AS (
  SELECT
    MIN(Diesel_Alpha) AS val_min,
    MAX(Diesel_Alpha) AS val_max
  FROM
    combustibles_hn.gold.tesis_alphas_combustibles
),
binned AS (
  SELECT
    LEAST(FLOOR((Diesel_Alpha - val_min) / ((val_max - val_min) / 10)), 9) AS bin_order,
    val_min
      + (
        LEAST(FLOOR((Diesel_Alpha - val_min) / ((val_max - val_min) / 10)), 9)
          * ((val_max - val_min) / 10)
      ) AS bin_start,
    val_min
      + (
        (LEAST(FLOOR((Diesel_Alpha - val_min) / ((val_max - val_min) / 10)), 9) + 1)
          * ((val_max - val_min) / 10)
      ) AS bin_end
  FROM
    combustibles_hn.gold.tesis_alphas_combustibles CROSS JOIN ranges
)
SELECT
  bin_order,
  CONCAT('[', ROUND(bin_start, 2), ', ', ROUND(bin_end, 2), ')') AS bin_label,
  COUNT(*) AS frecuencia
FROM
  binned
GROUP BY
  bin_order,
  bin_start,
  bin_end
ORDER BY
  bin_order
""")

print("Vista creada: v_cap2_histograma_diesel_alpha")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap2_histograma_regular_alpha AS
WITH ranges AS (
  SELECT
    MIN(Regular_Alpha) AS val_min,
    MAX(Regular_Alpha) AS val_max
  FROM
    combustibles_hn.gold.tesis_alphas_combustibles
),
binned AS (
  SELECT
    LEAST(FLOOR((Regular_Alpha - val_min) / ((val_max - val_min) / 10)), 9) AS bin_order,
    val_min
      + (
        LEAST(FLOOR((Regular_Alpha - val_min) / ((val_max - val_min) / 10)), 9)
          * ((val_max - val_min) / 10)
      ) AS bin_start,
    val_min
      + (
        (LEAST(FLOOR((Regular_Alpha - val_min) / ((val_max - val_min) / 10)), 9) + 1)
          * ((val_max - val_min) / 10)
      ) AS bin_end
  FROM
    combustibles_hn.gold.tesis_alphas_combustibles CROSS JOIN ranges
)
SELECT
  bin_order,
  CONCAT('[', ROUND(bin_start, 2), ', ', ROUND(bin_end, 2), ')') AS bin_label,
  COUNT(*) AS frecuencia
FROM
  binned
GROUP BY
  bin_order,
  bin_start,
  bin_end
ORDER BY
  bin_order
""")

print("Vista creada: v_cap2_histograma_regular_alpha")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap2_histograma_super_alpha AS
WITH ranges AS (
  SELECT
    MIN(Super_Alpha) AS val_min,
    MAX(Super_Alpha) AS val_max
  FROM
    combustibles_hn.gold.tesis_alphas_combustibles
),
binned AS (
  SELECT
    LEAST(FLOOR((Super_Alpha - val_min) / ((val_max - val_min) / 10)), 9) AS bin_order,
    val_min
      + (
        LEAST(FLOOR((Super_Alpha - val_min) / ((val_max - val_min) / 10)), 9)
          * ((val_max - val_min) / 10)
      ) AS bin_start,
    val_min
      + (
        (LEAST(FLOOR((Super_Alpha - val_min) / ((val_max - val_min) / 10)), 9) + 1)
          * ((val_max - val_min) / 10)
      ) AS bin_end
  FROM
    combustibles_hn.gold.tesis_alphas_combustibles CROSS JOIN ranges
)
SELECT
  bin_order,
  CONCAT('[', ROUND(bin_start, 2), ', ', ROUND(bin_end, 2), ')') AS bin_label,
  COUNT(*) AS frecuencia
FROM
  binned
GROUP BY
  bin_order,
  bin_start,
  bin_end
ORDER BY
  bin_order
""")

print("Vista creada: v_cap2_histograma_super_alpha")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Matriz de Correlación (Capítulo 2)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap2_matriz_correlacion_alphas AS
SELECT
  'Super' AS combustible_x,
  'Super' AS combustible_y,
  1.0 AS correlacion
UNION ALL
SELECT
  'Super',
  'Regular',
  CORR(Super_Alpha, Regular_Alpha)
FROM
  combustibles_hn.gold.tesis_alphas_combustibles
UNION ALL
SELECT
  'Super',
  'Diesel',
  CORR(Super_Alpha, Diesel_Alpha)
FROM
  combustibles_hn.gold.tesis_alphas_combustibles
UNION ALL
SELECT
  'Super',
  'Kerosene',
  CORR(Super_Alpha, Kerosene_Alpha)
FROM
  combustibles_hn.gold.tesis_alphas_combustibles
UNION ALL
SELECT
  'Regular',
  'Super',
  CORR(Regular_Alpha, Super_Alpha)
FROM
  combustibles_hn.gold.tesis_alphas_combustibles
UNION ALL
SELECT
  'Regular',
  'Regular',
  1.0
UNION ALL
SELECT
  'Regular',
  'Diesel',
  CORR(Regular_Alpha, Diesel_Alpha)
FROM
  combustibles_hn.gold.tesis_alphas_combustibles
UNION ALL
SELECT
  'Regular',
  'Kerosene',
  CORR(Regular_Alpha, Kerosene_Alpha)
FROM
  combustibles_hn.gold.tesis_alphas_combustibles
UNION ALL
SELECT
  'Diesel',
  'Super',
  CORR(Diesel_Alpha, Super_Alpha)
FROM
  combustibles_hn.gold.tesis_alphas_combustibles
UNION ALL
SELECT
  'Diesel',
  'Regular',
  CORR(Diesel_Alpha, Regular_Alpha)
FROM
  combustibles_hn.gold.tesis_alphas_combustibles
UNION ALL
SELECT
  'Diesel',
  'Diesel',
  1.0
UNION ALL
SELECT
  'Diesel',
  'Kerosene',
  CORR(Diesel_Alpha, Kerosene_Alpha)
FROM
  combustibles_hn.gold.tesis_alphas_combustibles
UNION ALL
SELECT
  'Kerosene',
  'Super',
  CORR(Kerosene_Alpha, Super_Alpha)
FROM
  combustibles_hn.gold.tesis_alphas_combustibles
UNION ALL
SELECT
  'Kerosene',
  'Regular',
  CORR(Kerosene_Alpha, Regular_Alpha)
FROM
  combustibles_hn.gold.tesis_alphas_combustibles
UNION ALL
SELECT
  'Kerosene',
  'Diesel',
  CORR(Kerosene_Alpha, Diesel_Alpha)
FROM
  combustibles_hn.gold.tesis_alphas_combustibles
UNION ALL
SELECT
  'Kerosene',
  'Kerosene',
  1.0
""")

print("Vista creada: v_cap2_matriz_correlacion_alphas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Estadísticas Descriptivas (Capítulo 2)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap2_estadisticas_descriptivas_alphas AS
SELECT
  'Super Alpha' AS combustible,
  ROUND(AVG(Super_Alpha), 4) AS media,
  ROUND(STDDEV(Super_Alpha), 4) AS desv_estandar,
  ROUND(MIN(Super_Alpha), 4) AS minimo,
  ROUND(PERCENTILE(Super_Alpha, 0.25), 4) AS percentil_25,
  ROUND(PERCENTILE(Super_Alpha, 0.50), 4) AS mediana,
  ROUND(PERCENTILE(Super_Alpha, 0.75), 4) AS percentil_75,
  ROUND(MAX(Super_Alpha), 4) AS maximo,
  SUM(CASE WHEN Super_Alpha > 0 THEN 1 ELSE 0 END) AS observaciones_positivas,
  SUM(CASE WHEN Super_Alpha < 0 THEN 1 ELSE 0 END) AS observaciones_negativas,
  COUNT(*) AS total_observaciones
FROM combustibles_hn.gold.tesis_alphas_combustibles
UNION ALL
SELECT
  'Regular Alpha' AS combustible,
  ROUND(AVG(Regular_Alpha), 4) AS media,
  ROUND(STDDEV(Regular_Alpha), 4) AS desv_estandar,
  ROUND(MIN(Regular_Alpha), 4) AS minimo,
  ROUND(PERCENTILE(Regular_Alpha, 0.25), 4) AS percentil_25,
  ROUND(PERCENTILE(Regular_Alpha, 0.50), 4) AS mediana,
  ROUND(PERCENTILE(Regular_Alpha, 0.75), 4) AS percentil_75,
  ROUND(MAX(Regular_Alpha), 4) AS maximo,
  SUM(CASE WHEN Regular_Alpha > 0 THEN 1 ELSE 0 END) AS observaciones_positivas,
  SUM(CASE WHEN Regular_Alpha < 0 THEN 1 ELSE 0 END) AS observaciones_negativas,
  COUNT(*) AS total_observaciones
FROM combustibles_hn.gold.tesis_alphas_combustibles
UNION ALL
SELECT
  'Diesel Alpha' AS combustible,
  ROUND(AVG(Diesel_Alpha), 4) AS media,
  ROUND(STDDEV(Diesel_Alpha), 4) AS desv_estandar,
  ROUND(MIN(Diesel_Alpha), 4) AS minimo,
  ROUND(PERCENTILE(Diesel_Alpha, 0.25), 4) AS percentil_25,
  ROUND(PERCENTILE(Diesel_Alpha, 0.50), 4) AS mediana,
  ROUND(PERCENTILE(Diesel_Alpha, 0.75), 4) AS percentil_75,
  ROUND(MAX(Diesel_Alpha), 4) AS maximo,
  SUM(CASE WHEN Diesel_Alpha > 0 THEN 1 ELSE 0 END) AS observaciones_positivas,
  SUM(CASE WHEN Diesel_Alpha < 0 THEN 1 ELSE 0 END) AS observaciones_negativas,
  COUNT(*) AS total_observaciones
FROM combustibles_hn.gold.tesis_alphas_combustibles
UNION ALL
SELECT
  'Kerosene Alpha' AS combustible,
  ROUND(AVG(Kerosene_Alpha), 4) AS media,
  ROUND(STDDEV(Kerosene_Alpha), 4) AS desv_estandar,
  ROUND(MIN(Kerosene_Alpha), 4) AS minimo,
  ROUND(PERCENTILE(Kerosene_Alpha, 0.25), 4) AS percentil_25,
  ROUND(PERCENTILE(Kerosene_Alpha, 0.50), 4) AS mediana,
  ROUND(PERCENTILE(Kerosene_Alpha, 0.75), 4) AS percentil_75,
  ROUND(MAX(Kerosene_Alpha), 4) AS maximo,
  SUM(CASE WHEN Kerosene_Alpha > 0 THEN 1 ELSE 0 END) AS observaciones_positivas,
  SUM(CASE WHEN Kerosene_Alpha < 0 THEN 1 ELSE 0 END) AS observaciones_negativas,
  COUNT(*) AS total_observaciones
FROM combustibles_hn.gold.tesis_alphas_combustibles
""")

print("Vista creada: v_cap2_estadisticas_descriptivas_alphas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Análisis Temporal y Resumen (Capítulo 2)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap2_analisis_temporal_mensual_alphas AS
SELECT
  DATE_TRUNC('MONTH', Fecha) AS mes,
  ROUND(AVG(Super_Alpha), 4) AS super_alpha_promedio,
  ROUND(AVG(Regular_Alpha), 4) AS regular_alpha_promedio,
  ROUND(AVG(Diesel_Alpha), 4) AS diesel_alpha_promedio,
  ROUND(AVG(Kerosene_Alpha), 4) AS kerosene_alpha_promedio,
  COUNT(*) AS observaciones
FROM
  combustibles_hn.gold.tesis_alphas_combustibles
GROUP BY
  DATE_TRUNC('MONTH', Fecha)
ORDER BY
  mes
""")

print("Vista creada: v_cap2_analisis_temporal_mensual_alphas")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap2_metricas_resumen_alphas AS
SELECT
  ROUND(AVG(Super_Alpha), 4) AS media_super,
  ROUND(STDDEV(Super_Alpha), 4) AS desv_super,
  ROUND(AVG(Regular_Alpha), 4) AS media_regular,
  ROUND(STDDEV(Regular_Alpha), 4) AS desv_regular,
  ROUND(AVG(Diesel_Alpha), 4) AS media_diesel,
  ROUND(STDDEV(Diesel_Alpha), 4) AS desv_diesel,
  ROUND(AVG(Kerosene_Alpha), 4) AS media_kerosene,
  ROUND(STDDEV(Kerosene_Alpha), 4) AS desv_kerosene
FROM
  combustibles_hn.gold.tesis_alphas_combustibles
""")

print("Vista creada: v_cap2_metricas_resumen_alphas")

# COMMAND ----------

print("¡Vistas del Capítulo 2 completadas!")

# COMMAND ----------

# DBTITLE 1,Capítulo 3 - Clustering K-Means
# MAGIC %md
# MAGIC ## 5. Vistas Capítulo 3 - Clustering K-Means

# COMMAND ----------

# DBTITLE 1,Vista: Centroides K-Means
spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap3_centroides AS
SELECT
  Combustible,
  Estado,
  Centroide_Alpha
FROM
  combustibles_hn.gold.tesis_cap3_centroides
ORDER BY
  Combustible,
  Estado
""")

print("Vista creada: v_cap3_centroides")

# COMMAND ----------

# DBTITLE 1,Vista: Propiedades Clusters
spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap3_propiedades_espectrales AS
SELECT
  Combustible,
  K_Estados,
  Eigenvalue_Dominante,
  Eigenvalue_2,
  Spectral_Gap,
  Mixing_Time_Approx,
  Pi_Estacionaria
FROM
  combustibles_hn.gold.tesis_cap3_propiedades_espectrales
ORDER BY
  Combustible
""")

print("Vista creada: v_cap3_propiedades_espectrales")

# COMMAND ----------

# DBTITLE 1,Vista: Umbrales Óptimos
spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap3_umbrales AS
SELECT
  Limite_Index,
  Valor_Limite,
  Descripcion
FROM
  combustibles_hn.gold.tesis_cap3_umbrales
ORDER BY
  Limite_Index
""")

print("Vista creada: v_cap3_umbrales")

# COMMAND ----------

# DBTITLE 1,Vista: Matrices de Transición Cap 3
spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap3_matrices_transicion AS
SELECT
  Combustible,
  Estado_Origen,
  Estado_Destino,
  Probabilidad,
  Error_Estandar,
  Limite_Inferior_95,
  Limite_Superior_95,
  Conteo_Transiciones,
  Total_Origen
FROM
  combustibles_hn.gold.tesis_cap3_matrices_transicion
ORDER BY
  Combustible,
  Estado_Origen,
  Estado_Destino
""")

print("Vista creada: v_cap3_matrices_transicion")

# COMMAND ----------

# DBTITLE 1,Capítulo 4 - NNLS/Markov Híbrido
# MAGIC %md
# MAGIC ## 6. Vistas Capítulo 4 - NNLS/Markov Híbrido

# COMMAND ----------

# DBTITLE 1,Vista: Matrices de Transición
spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap4_matrices_transicion AS
SELECT
  Combustible,
  Metodo,
  Estado_Origen,
  Estado_Destino,
  Probabilidad_MLE,
  Probabilidad_NNLS,
  Conteo_Transiciones,
  Total_Origen
FROM
  combustibles_hn.gold.tesis_cap4_matrices_transicion
ORDER BY
  Combustible,
  Metodo,
  Estado_Origen,
  Estado_Destino
""")

print("Vista creada: v_cap4_matrices_transicion")

# COMMAND ----------

# DBTITLE 1,Vista: Hiperparámetros Óptimos
spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap4_hiperparametros_optimos AS
SELECT
  Combustible,
  K_opt,
  W_opt,
  Lambda_opt,
  RMSE_Opt,
  Accuracy_Opt,
  AIC_Opt
FROM
  combustibles_hn.gold.tesis_cap4_best_hyperparams
ORDER BY
  Combustible
""")

print("Vista creada: v_cap4_hiperparametros_optimos")

# COMMAND ----------

# DBTITLE 1,Vista: Predicciones Semanales
spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap4_predicciones AS
SELECT
  Combustible,
  Semana,
  Precio_Real,
  Precio_Predicho_Markov,
  Fecha,
  Estado_Actual,
  Estado_Predicho
FROM
  combustibles_hn.gold.tesis_cap4_predicciones_precio_semanal
ORDER BY
  Combustible,
  Semana
""")

print("Vista creada: v_cap4_predicciones")

# COMMAND ----------

# DBTITLE 1,Vista: RMSE Markov
spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap4_rmse_markov AS
SELECT
  Combustible,
  RMSE_Markov,
  N_Predicciones
FROM
  combustibles_hn.gold.tesis_cap4_rmse_markov_base
ORDER BY
  Combustible
""")

print("Vista creada: v_cap4_rmse_markov")

# COMMAND ----------

# DBTITLE 1,Vista: Comparativa Métodos MLE vs NNLS
spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap4_comparativa_metodos AS
SELECT
  Combustible,
  'KMeans' AS Metodo,
  RMSE_KMeans AS RMSE,
  Accuracy_KMeans AS Accuracy
FROM
  combustibles_hn.gold.tesis_cap4_tabla_comparativa
UNION ALL
SELECT
  Combustible,
  'Cuantiles' AS Metodo,
  RMSE_Cuantiles AS RMSE,
  Accuracy_Cuantiles AS Accuracy
FROM
  combustibles_hn.gold.tesis_cap4_tabla_comparativa
UNION ALL
SELECT
  Combustible,
  'UmbralesFijos' AS Metodo,
  RMSE_UmbralesFijos AS RMSE,
  Accuracy_UmbralesFijos AS Accuracy
FROM
  combustibles_hn.gold.tesis_cap4_tabla_comparativa
ORDER BY
  Combustible,
  Metodo
""")

print("Vista creada: v_cap4_comparativa_metodos")

# COMMAND ----------

# DBTITLE 1,Vista: Detalle Grid Search
spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap4_grid_search_detalle AS
SELECT
  Combustible,
  K,
  W,
  Lambda,
  RMSE,
  Accuracy,
  AIC
FROM
  combustibles_hn.gold.tesis_cap4_detalle_grid
ORDER BY
  Combustible,
  K
""")

print("Vista creada: v_cap4_grid_search_detalle")

# COMMAND ----------

# DBTITLE 1,Capítulo 6 - SSRC (Reservorio)
# MAGIC %md
# MAGIC ## 7. Vistas Capítulo 6 - SSRC (State Space Reservoir Computing)

# COMMAND ----------

# DBTITLE 1,Vista: Comparativa Final Markov vs SSRC vs Ridge
spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap6_comparativa_final AS
SELECT
  Combustible,
  RMSE_Markov,
  RMSE_SSRC,
  RMSE_SSRC_Std,
  RMSE_Ridge,
  Delta_Porcentaje,
  Delta_Solver,
  DM_Statistic,
  DM_P_Value,
  Significativo_005,
  Mejor_Modelo
FROM
  combustibles_hn.gold.tesis_cap6_comparacion_final
ORDER BY
  Combustible
""")

print("Vista creada: v_cap6_comparativa_final")

# COMMAND ----------

# DBTITLE 1,Vista: Predicciones SSRC
spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap6_predicciones_ssrc AS
SELECT
  Combustible,
  Semana,
  Precio_Real,
  Prediccion_Markov,
  Prediccion_SSRC
FROM
  combustibles_hn.gold.tesis_cap6_predicciones_semanales
ORDER BY
  Combustible,
  Semana
""")

print("Vista creada: v_cap6_predicciones_ssrc")

# COMMAND ----------

# DBTITLE 1,Vista: Hiperparámetros SSRC
spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap6_hiperparametros_ssrc AS
SELECT
  Combustible,
  D AS Dimensiones_Reservorio,
  rho AS Radio_Espectral,
  leak_rate AS Tasa_Fuga,
  RMSE,
  RMSE_Ridge
FROM
  combustibles_hn.gold.tesis_cap6_best_ssrc
ORDER BY
  Combustible
""")

print("Vista creada: v_cap6_hiperparametros_ssrc")

# COMMAND ----------

# DBTITLE 1,Vista: Realizaciones Múltiples (30 runs)
spark.sql("""
CREATE OR REPLACE VIEW combustibles_hn.gold.v_cap6_realizaciones_multiple AS
SELECT
  Combustible,
  Realizacion,
  RMSE,
  D AS Dimensiones_Reservorio,
  rho AS Radio_Espectral,
  leak_rate AS Tasa_Fuga
FROM
  combustibles_hn.gold.tesis_cap6_realizaciones
ORDER BY
  Combustible,
  Realizacion
""")

print("Vista creada: v_cap6_realizaciones_multiple")

# COMMAND ----------

# DBTITLE 1,Resumen Final
print("\n=== RESUMEN DE VISTAS CREADAS ===")
print("\nCapítulo 2 (Alphas): 8 vistas")
print("  - Histogramas (4): Super, Regular, Diesel, Kerosene")
print("  - Matriz de correlación (1)")
print("  - Estadísticas descriptivas (1)")
print("  - Análisis temporal mensual (1)")
print("  - Métricas resumen (1)")

print("\nCapítulo 3 (K-Means): 4 vistas")
print("  - Centroides")
print("  - Propiedades espectrales")
print("  - Umbrales")
print("  - Matrices de transición")

print("\nCapítulo 4 (Markov/NNLS): 6 vistas")
print("  - Matrices de transición")
print("  - Hiperparámetros óptimos")
print("  - Predicciones semanales")
print("  - RMSE Markov")
print("  - Comparativa métodos MLE vs NNLS")
print("  - Detalle grid search")

print("\nCapítulo 6 (SSRC): 4 vistas")
print("  - Comparativa final (con test Diebold-Mariano)")
print("  - Predicciones SSRC")
print("  - Hiperparámetros SSRC")
print("  - Realizaciones múltiples (30 runs)")

print("\n¡Total: 22 vistas creadas para consumo en Power BI/Tableau!")
