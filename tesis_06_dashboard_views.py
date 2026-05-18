# Databricks notebook source
# MAGIC %md
# MAGIC # Tesis Dashboards — Vistas Gold Capítulo 2 (Tableau & Power BI)
# MAGIC 
# MAGIC **Objetivo:** Persistir las consultas interactivas generadas por Genie para el **Capítulo 2** como Vistas (Views) en la capa Gold.
# MAGIC Esto permite que herramientas externas como Power BI o Tableau puedan consumirlas directamente sin necesidad de recalcularlas ni duplicar almacenamiento.
# MAGIC Todas las vistas usan el prefijo `v_cap2_`.

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
print("¡Todas las Vistas (Views) del Capítulo 2 han sido registradas exitosamente en la capa Gold!")
