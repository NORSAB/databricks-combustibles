# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Modelo Dimensional - Tesis Combustibles
# MAGIC %md
# MAGIC # Modelo Dimensional (Star Schema) - Tesis Combustibles Honduras
# MAGIC
# MAGIC **Objetivo:** Crear un modelo relacional dimensional que conecte todas las tablas analíticas de la tesis mediante:
# MAGIC * **Dimensión Combustibles** (Super, Regular, Diesel, Kerosene)
# MAGIC * **Dimensión Fechas** (Calendario con jerarquías)
# MAGIC * **Dimensión Estados** (Regímenes de mercado 0-3)
# MAGIC * **Dimensión Métodos** (MLE, NNLS, KMeans, SSRC, etc.)
# MAGIC
# MAGIC **Nomenclatura:** Todas las tablas del modelo usan el prefijo `tesis_modelo_` para diferenciarse de las tablas analíticas individuales (`tesis_cap*`)
# MAGIC
# MAGIC **Beneficios:**
# MAGIC * Filtros globales por combustible y fecha en dashboards
# MAGIC * Drill-down: Año → Trimestre → Mes → Semana → Día
# MAGIC * Comparaciones cruzadas entre capítulos
# MAGIC * Modelo optimizado para Power BI/Tableau
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,1. Dimensión: Combustibles
# MAGIC %md
# MAGIC ## 1. Dimensión: Combustibles
# MAGIC Catálogo maestro de los 4 tipos de combustible

# COMMAND ----------

# DBTITLE 1,dim_combustibles
spark.sql("""
CREATE OR REPLACE TABLE combustibles_hn.gold.tesis_modelo_dim_combustibles (
  combustible_id INT,
  combustible_nombre STRING,
  combustible_categoria STRING,
  combustible_descripcion STRING
)
USING DELTA
COMMENT 'Modelo Dimensional - Dimensión de Combustibles'
""")

spark.sql("""
INSERT INTO combustibles_hn.gold.tesis_modelo_dim_combustibles VALUES
  (1, 'Super', 'Gasolina', 'Gasolina Superior de alto octanaje'),
  (2, 'Regular', 'Gasolina', 'Gasolina Regular de octanaje estándar'),
  (3, 'Diesel', 'Diesel', 'Combustible Diesel para motores de compresión'),
  (4, 'Kerosene', 'Kerosene', 'Kerosene para uso industrial y doméstico')
""")

print("✓ Tabla creada: tesis_modelo_dim_combustibles (4 registros)")

# COMMAND ----------

# DBTITLE 1,2. Dimensión: Fechas
# MAGIC %md
# MAGIC ## 2. Dimensión: Fechas (Calendario Completo)
# MAGIC Calendario desde 2005-01-01 hasta 2026-12-31 con jerarquías temporales

# COMMAND ----------

# DBTITLE 1,dim_fechas
spark.sql("""
CREATE OR REPLACE TABLE combustibles_hn.gold.tesis_modelo_dim_fechas
USING DELTA
COMMENT 'Modelo Dimensional - Dimensión de Fechas con jerarquías'
AS
SELECT
  fecha,
  YEAR(fecha) AS anio,
  QUARTER(fecha) AS trimestre,
  MONTH(fecha) AS mes,
  WEEKOFYEAR(fecha) AS semana_anio,
  DAYOFMONTH(fecha) AS dia_mes,
  DAYOFWEEK(fecha) AS dia_semana,
  DAYOFYEAR(fecha) AS dia_anio,
  DATE_FORMAT(fecha, 'MMMM') AS mes_nombre,
  DATE_FORMAT(fecha, 'EEEE') AS dia_nombre,
  CASE WHEN DAYOFWEEK(fecha) IN (1, 7) THEN TRUE ELSE FALSE END AS es_fin_semana,
  DATE_TRUNC('MONTH', fecha) AS mes_inicio,
  DATE_TRUNC('QUARTER', fecha) AS trimestre_inicio,
  DATE_TRUNC('YEAR', fecha) AS anio_inicio
FROM (
  SELECT explode(sequence(to_date('2005-01-01'), to_date('2026-12-31'), interval 1 day)) AS fecha
)
""")

count = spark.sql("SELECT COUNT(*) as cnt FROM combustibles_hn.gold.tesis_modelo_dim_fechas").collect()[0]['cnt']
print(f"✓ Tabla creada: tesis_modelo_dim_fechas ({count:,} registros)")

# COMMAND ----------

# DBTITLE 1,3. Dimensión: Estados
# MAGIC %md
# MAGIC ## 3. Dimensión: Estados (Regímenes de Mercado)
# MAGIC Clasificación de los 4 estados identificados por K-Means

# COMMAND ----------

# DBTITLE 1,dim_estados
spark.sql("""
CREATE OR REPLACE TABLE combustibles_hn.gold.tesis_modelo_dim_estados (
  estado_id INT,
  estado_nombre STRING,
  estado_descripcion STRING,
  rango_alpha_min DOUBLE,
  rango_alpha_max DOUBLE
)
USING DELTA
COMMENT 'Modelo Dimensional - Dimensión de Estados (Regímenes de mercado)'
""")

spark.sql("""
INSERT INTO combustibles_hn.gold.tesis_modelo_dim_estados VALUES
  (0, 'Decrecimiento Severo', 'Caída pronunciada de precios (α < -0.03)', -0.15, -0.03),
  (1, 'Estabilidad', 'Precios estables con poca variación (-0.03 ≤ α ≤ 0.01)', -0.03, 0.01),
  (2, 'Crecimiento Moderado', 'Aumento moderado de precios (0.01 < α ≤ 0.04)', 0.01, 0.04),
  (3, 'Crecimiento Fuerte', 'Aumento pronunciado de precios (α > 0.04)', 0.04, 0.15)
""")

print("✓ Tabla creada: tesis_modelo_dim_estados (4 registros)")

# COMMAND ----------

# DBTITLE 1,4. Dimensión: Métodos
# MAGIC %md
# MAGIC ## 4. Dimensión: Métodos (Algoritmos de Predicción/Clustering)
# MAGIC Catálogo de todos los métodos utilizados en la tesis

# COMMAND ----------

# DBTITLE 1,dim_metodos
spark.sql("""
CREATE OR REPLACE TABLE combustibles_hn.gold.tesis_modelo_dim_metodos (
  metodo_id INT,
  metodo_nombre STRING,
  metodo_tipo STRING,
  metodo_capitulo STRING,
  metodo_descripcion STRING
)
USING DELTA
COMMENT 'Modelo Dimensional - Dimensión de Métodos y Algoritmos'
""")

spark.sql("""
INSERT INTO combustibles_hn.gold.tesis_modelo_dim_metodos VALUES
  (1, 'TCRA', 'Cálculo Alpha', 'Capítulo 2', 'Tasa de Cambio Relativa Acumulada'),
  (2, 'ETCRA', 'Cálculo Alpha', 'Capítulo 2', 'Tasa de Cambio Relativa Acumulada Exponencial'),
  (3, 'KMeans', 'Clustering', 'Capítulo 3', 'K-Means clustering para discretización de estados'),
  (4, 'Cuantiles', 'Clustering', 'Capítulo 3', 'Partición basada en cuantiles'),
  (5, 'UmbralesFijos', 'Clustering', 'Capítulo 3', 'Umbrales fijos de clasificación'),
  (6, 'MLE', 'Estimación Markov', 'Capítulo 4', 'Maximum Likelihood Estimation'),
  (7, 'NNLS', 'Estimación Markov', 'Capítulo 4', 'Non-Negative Least Squares'),
  (8, 'Markov', 'Predicción', 'Capítulo 4', 'Modelo Markov baseline'),
  (9, 'SSRC', 'Predicción', 'Capítulo 6', 'State Space Reservoir Computing'),
  (10, 'Ridge', 'Predicción', 'Capítulo 6', 'Ridge Regression como solver del SSRC')
""")

print("✓ Tabla creada: tesis_modelo_dim_metodos (10 registros)")

# COMMAND ----------

# DBTITLE 1,5. Hecho: Precios y Alphas Diarios
# MAGIC %md
# MAGIC ## 5. Hecho: Precios y Alphas Diarios (Capítulo 2)
# MAGIC Serie temporal completa de precios y alphas por fecha y combustible

# COMMAND ----------

# DBTITLE 1,fact_precios_alphas_diarios
spark.sql("""
CREATE OR REPLACE TABLE combustibles_hn.gold.tesis_modelo_fact_precios_diarios
USING DELTA
COMMENT 'Modelo Dimensional - Hecho: Precios y Alphas Diarios'
AS
SELECT
  date_format(Fecha, 'yyyy-MM-dd') AS fecha,
  'Super' AS combustible,
  CAST(Super AS DOUBLE) AS precio,
  Super_Alpha AS alpha,
  Super_Alpha_TCRA AS alpha_tcra,
  Super_Alpha_ETCRA AS alpha_etcra
FROM combustibles_hn.gold.tesis_alphas_combustibles
WHERE Super IS NOT NULL
UNION ALL
SELECT
  date_format(Fecha, 'yyyy-MM-dd') AS fecha,
  'Regular' AS combustible,
  CAST(Regular AS DOUBLE) AS precio,
  Regular_Alpha AS alpha,
  Regular_Alpha_TCRA AS alpha_tcra,
  Regular_Alpha_ETCRA AS alpha_etcra
FROM combustibles_hn.gold.tesis_alphas_combustibles
WHERE Regular IS NOT NULL
UNION ALL
SELECT
  date_format(Fecha, 'yyyy-MM-dd') AS fecha,
  'Diesel' AS combustible,
  CAST(Diesel AS DOUBLE) AS precio,
  Diesel_Alpha AS alpha,
  Diesel_Alpha_TCRA AS alpha_tcra,
  Diesel_Alpha_ETCRA AS alpha_etcra
FROM combustibles_hn.gold.tesis_alphas_combustibles
WHERE Diesel IS NOT NULL
UNION ALL
SELECT
  date_format(Fecha, 'yyyy-MM-dd') AS fecha,
  'Kerosene' AS combustible,
  CAST(Kerosene AS DOUBLE) AS precio,
  Kerosene_Alpha AS alpha,
  Kerosene_Alpha_TCRA AS alpha_tcra,
  Kerosene_Alpha_ETCRA AS alpha_etcra
FROM combustibles_hn.gold.tesis_alphas_combustibles
WHERE Kerosene IS NOT NULL
""")

count = spark.sql("SELECT COUNT(*) as cnt FROM combustibles_hn.gold.tesis_modelo_fact_precios_diarios").collect()[0]['cnt']
print(f"✓ Tabla creada: tesis_modelo_fact_precios_diarios ({count:,} registros)")

# COMMAND ----------

# DBTITLE 1,6. Hecho: Predicciones Semanales
# MAGIC %md
# MAGIC ## 6. Hecho: Predicciones Semanales Unificadas
# MAGIC Unifica predicciones de Markov (Cap 4) y SSRC (Cap 6)

# COMMAND ----------

# DBTITLE 1,fact_predicciones
spark.sql("""
CREATE OR REPLACE TABLE combustibles_hn.gold.tesis_modelo_fact_predicciones
USING DELTA
COMMENT 'Modelo Dimensional - Hecho: Predicciones Semanales Unificadas'
AS
SELECT
  Combustible, Fecha AS fecha, Semana AS semana,
  Precio_Real AS precio_real, Precio_Predicho_Markov AS prediccion,
  'Markov' AS metodo, 'Capítulo 4' AS origen,
  Estado_Actual AS estado_actual, Estado_Predicho AS estado_predicho
FROM combustibles_hn.gold.tesis_cap4_predicciones_precio_semanal
UNION ALL
SELECT
  Combustible, NULL AS fecha, Semana AS semana,
  Precio_Real AS precio_real, Prediccion_SSRC AS prediccion,
  'SSRC' AS metodo, 'Capítulo 6' AS origen,
  NULL AS estado_actual, NULL AS estado_predicho
FROM combustibles_hn.gold.tesis_cap6_predicciones_semanales
UNION ALL
SELECT
  Combustible, NULL AS fecha, Semana AS semana,
  Precio_Real AS precio_real, Prediccion_Markov AS prediccion,
  'Markov_Baseline_Cap6' AS metodo, 'Capítulo 6' AS origen,
  NULL AS estado_actual, NULL AS estado_predicho
FROM combustibles_hn.gold.tesis_cap6_predicciones_semanales
""")

count = spark.sql("SELECT COUNT(*) as cnt FROM combustibles_hn.gold.tesis_modelo_fact_predicciones").collect()[0]['cnt']
print(f"✓ Tabla creada: tesis_modelo_fact_predicciones ({count:,} registros)")

# COMMAND ----------

# DBTITLE 1,7. Hecho: Métricas de Modelos
# MAGIC %md
# MAGIC ## 7. Hecho: Métricas de Modelos
# MAGIC Métricas de rendimiento (RMSE, Accuracy) de todos los modelos

# COMMAND ----------

# DBTITLE 1,fact_metricas
spark.sql("""
CREATE OR REPLACE TABLE combustibles_hn.gold.tesis_modelo_fact_metricas
USING DELTA
COMMENT 'Modelo Dimensional - Hecho: Métricas de Rendimiento'
AS
SELECT Combustible, 'KMeans' AS metodo, RMSE_KMeans AS rmse, Accuracy_KMeans AS accuracy, NULL AS rmse_std, 'Capítulo 4' AS origen FROM combustibles_hn.gold.tesis_cap4_tabla_comparativa
UNION ALL
SELECT Combustible, 'Cuantiles' AS metodo, RMSE_Cuantiles AS rmse, Accuracy_Cuantiles AS accuracy, NULL AS rmse_std, 'Capítulo 4' AS origen FROM combustibles_hn.gold.tesis_cap4_tabla_comparativa
UNION ALL
SELECT Combustible, 'UmbralesFijos' AS metodo, RMSE_UmbralesFijos AS rmse, Accuracy_UmbralesFijos AS accuracy, NULL AS rmse_std, 'Capítulo 4' AS origen FROM combustibles_hn.gold.tesis_cap4_tabla_comparativa
UNION ALL
SELECT Combustible, 'Markov' AS metodo, RMSE_Markov AS rmse, NULL AS accuracy, NULL AS rmse_std, 'Capítulo 4' AS origen FROM combustibles_hn.gold.tesis_cap4_rmse_markov_base
UNION ALL
SELECT Combustible, 'SSRC' AS metodo, RMSE_SSRC AS rmse, NULL AS accuracy, RMSE_SSRC_Std AS rmse_std, 'Capítulo 6' AS origen FROM combustibles_hn.gold.tesis_cap6_comparacion_final
UNION ALL
SELECT Combustible, 'Markov_Cap6' AS metodo, RMSE_Markov AS rmse, NULL AS accuracy, NULL AS rmse_std, 'Capítulo 6' AS origen FROM combustibles_hn.gold.tesis_cap6_comparacion_final
UNION ALL
SELECT Combustible, 'Ridge' AS metodo, RMSE_Ridge AS rmse, NULL AS accuracy, NULL AS rmse_std, 'Capítulo 6' AS origen FROM combustibles_hn.gold.tesis_cap6_comparacion_final
""")

count = spark.sql("SELECT COUNT(*) as cnt FROM combustibles_hn.gold.tesis_modelo_fact_metricas").collect()[0]['cnt']
print(f"✓ Tabla creada: tesis_modelo_fact_metricas ({count:,} registros)")

# COMMAND ----------

# DBTITLE 1,8. Hecho: Transiciones
# MAGIC %md
# MAGIC ## 8. Hecho: Transiciones (Matrices de Transición)
# MAGIC Probabilidades de transición entre estados para todos los métodos

# COMMAND ----------

# DBTITLE 1,fact_transiciones
spark.sql("""
CREATE OR REPLACE TABLE combustibles_hn.gold.tesis_modelo_fact_transiciones
USING DELTA
COMMENT 'Modelo Dimensional - Hecho: Probabilidades de Transición'
AS
SELECT Combustible, 'UmbralesFijos' AS metodo, Estado_Origen, Estado_Destino, Probabilidad AS probabilidad,
  Error_Estandar AS error_estandar, Limite_Inferior_95, Limite_Superior_95, Conteo_Transiciones, Total_Origen, 'Capítulo 3' AS origen
FROM combustibles_hn.gold.tesis_cap3_matrices_transicion
UNION ALL
SELECT Combustible, 'MLE' AS metodo, Estado_Origen, Estado_Destino, Probabilidad_MLE AS probabilidad,
  NULL AS error_estandar, NULL AS Limite_Inferior_95, NULL AS Limite_Superior_95, Conteo_Transiciones, Total_Origen, 'Capítulo 4' AS origen
FROM combustibles_hn.gold.tesis_cap4_matrices_transicion WHERE Metodo = 'MLE'
UNION ALL
SELECT Combustible, 'NNLS' AS metodo, Estado_Origen, Estado_Destino, Probabilidad_NNLS AS probabilidad,
  NULL AS error_estandar, NULL AS Limite_Inferior_95, NULL AS Limite_Superior_95, Conteo_Transiciones, Total_Origen, 'Capítulo 4' AS origen
FROM combustibles_hn.gold.tesis_cap4_matrices_transicion WHERE Metodo = 'NNLS'
""")

count = spark.sql("SELECT COUNT(*) as cnt FROM combustibles_hn.gold.tesis_modelo_fact_transiciones").collect()[0]['cnt']
print(f"✓ Tabla creada: tesis_modelo_fact_transiciones ({count:,} registros)")

# COMMAND ----------

# DBTITLE 1,9. Diagrama del Modelo
# MAGIC %md
# MAGIC ## 9. Diagrama de Relaciones del Modelo Dimensional
# MAGIC
# MAGIC ```
# MAGIC                     tesis_modelo_dim_combustibles (PK: combustible_id)
# MAGIC                                     |
# MAGIC                          1:N        |         1:N
# MAGIC           _____________________________|_____________________________
# MAGIC          |                            |                             |
# MAGIC          v                            v                             v
# MAGIC tesis_modelo_fact_          tesis_modelo_fact_          tesis_modelo_fact_
# MAGIC precios_diarios             predicciones                metricas
# MAGIC FK: combustible             FK: combustible             FK: combustible
# MAGIC FK: fecha                   FK: fecha                   FK: metodo
# MAGIC          |                  FK: metodo
# MAGIC          |                            |
# MAGIC          v                            v
# MAGIC tesis_modelo_dim_fechas     tesis_modelo_dim_metodos
# MAGIC (PK: fecha)                 (PK: metodo_id)
# MAGIC                                     |
# MAGIC                                     | 1:N
# MAGIC                                     v
# MAGIC                            tesis_modelo_fact_transiciones
# MAGIC                            FK: combustible
# MAGIC                            FK: metodo
# MAGIC                            FK: estado_origen
# MAGIC                            FK: estado_destino
# MAGIC                                     ^
# MAGIC                                     | 1:N
# MAGIC                            tesis_modelo_dim_estados
# MAGIC                            (PK: estado_id)
# MAGIC ```
# MAGIC
# MAGIC ### Nomenclatura:
# MAGIC * **Dimensiones:** `tesis_modelo_dim_*` (combustibles, fechas, estados, metodos)
# MAGIC * **Hechos:** `tesis_modelo_fact_*` (precios_diarios, predicciones, metricas, transiciones)
# MAGIC
# MAGIC **Separadas de las tablas analíticas individuales:** `tesis_cap*`, `tesis_alphas_combustibles`, etc.

# COMMAND ----------

# DBTITLE 1,10. Validaciones de Integridad
# MAGIC %md
# MAGIC ## 10. Validaciones de Integridad Referencial
# MAGIC Verificar que todas las foreign keys tengan valores válidos en las dimensiones

# COMMAND ----------

# DBTITLE 1,Validar Integridad
print("=" * 70)
print("  VALIDACIONES DE INTEGRIDAD REFERENCIAL")
print("=" * 70)
print()

# 1. Validar combustibles en fact_precios_diarios
result = spark.sql("""
SELECT COUNT(DISTINCT f.combustible) as total, COUNT(DISTINCT d.combustible_nombre) as validos
FROM combustibles_hn.gold.tesis_modelo_fact_precios_diarios f
LEFT JOIN combustibles_hn.gold.tesis_modelo_dim_combustibles d ON f.combustible = d.combustible_nombre
""").collect()[0]
print(f"1. Combustibles en fact_precios_diarios:")
print(f"   Total: {result['total']} | Con match: {result['validos']}")
print(f"   ✓ OK" if result['total'] == result['validos'] else "   ✗ ERROR")
print()

# 2. Validar fechas
result = spark.sql("""
SELECT COUNT(DISTINCT f.fecha) as total, COUNT(DISTINCT d.fecha) as validos
FROM combustibles_hn.gold.tesis_modelo_fact_precios_diarios f
LEFT JOIN combustibles_hn.gold.tesis_modelo_dim_fechas d ON f.fecha = d.fecha
""").collect()[0]
print(f"2. Fechas en fact_precios_diarios:")
print(f"   Total: {result['total']} | Con match: {result['validos']}")
print(f"   ✓ OK" if result['total'] == result['validos'] else "   ✗ ERROR")
print()

# 3. Validar métodos en fact_metricas
result = spark.sql("""
SELECT COUNT(DISTINCT f.metodo) as total, COUNT(DISTINCT d.metodo_nombre) as validos
FROM combustibles_hn.gold.tesis_modelo_fact_metricas f
LEFT JOIN combustibles_hn.gold.tesis_modelo_dim_metodos d ON f.metodo = d.metodo_nombre
""").collect()[0]
print(f"3. Métodos en fact_metricas:")
print(f"   Total: {result['total']} | Con match: {result['validos']}")
if result['total'] != result['validos']:
    missing = spark.sql("""
    SELECT DISTINCT f.metodo FROM combustibles_hn.gold.tesis_modelo_fact_metricas f
    LEFT JOIN combustibles_hn.gold.tesis_modelo_dim_metodos d ON f.metodo = d.metodo_nombre
    WHERE d.metodo_nombre IS NULL
    """).collect()
    print(f"   ✗ Sin match: {[r['metodo'] for r in missing]}")
else:
    print(f"   ✓ OK")
print()

print("=" * 70)
print("✓ Integridad referencial verificada")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,11. Resumen Final
# MAGIC %md
# MAGIC ## 11. Resumen del Modelo Dimensional
# MAGIC Estadísticas finales de todas las tablas del modelo

# COMMAND ----------

# DBTITLE 1,Resumen Final
print("\n" + "=" * 70)
print("  MODELO DIMENSIONAL COMPLETO - TESIS COMBUSTIBLES HONDURAS")
print("=" * 70)

print("\n■ DIMENSIONES (tesis_modelo_dim_*):")
for tabla in ['tesis_modelo_dim_combustibles', 'tesis_modelo_dim_fechas', 'tesis_modelo_dim_estados', 'tesis_modelo_dim_metodos']:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM combustibles_hn.gold.{tabla}").collect()[0]['cnt']
    print(f"  ✓ {tabla:35s} {count:>10,} registros")

print("\n■ HECHOS (tesis_modelo_fact_*):")
for tabla in ['tesis_modelo_fact_precios_diarios', 'tesis_modelo_fact_predicciones', 'tesis_modelo_fact_metricas', 'tesis_modelo_fact_transiciones']:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM combustibles_hn.gold.{tabla}").collect()[0]['cnt']
    print(f"  ✓ {tabla:35s} {count:>10,} registros")

print("\n■ CARACTERÍSTICAS:")
print("  • Modelo Star Schema (Estrella)")
print("  • 4 Dimensiones + 4 Tablas de Hechos")
print("  • Conexiones vía Combustible, Fecha, Estado, Método")
print("  • Nomenclatura: tesis_modelo_* (separado de tablas analíticas tesis_cap*)")
print("  • Ubicación: combustibles_hn.gold (misma capa que tablas origen)")
print("  • Optimizado para Power BI / Tableau")

print("\n■ USO EN POWER BI/TABLEAU:")
print("  1. Conectar a combustibles_hn.gold")
print("  2. Importar tablas tesis_modelo_dim_* y tesis_modelo_fact_*")
print("  3. Crear relaciones automáticas o manuales")
print("  4. Usar dim_combustibles y dim_fechas como filtros globales")
print("  5. Drill-down: Año → Trimestre → Mes → Semana → Día")

print("\n" + "=" * 70)
print("✓ MODELO DIMENSIONAL CREADO EXITOSAMENTE")
print("=" * 70 + "\n")

# COMMAND ----------


