# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Capa GOLD: Modelo analitico
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras
# MAGIC **Capa:** Gold (modelo listo para BI: Power BI, Tableau, dashboard Databricks)
# MAGIC
# MAGIC **Entrada:** `combustibles_hn.silver.silver_precios_semanales`
# MAGIC **Salidas:**
# MAGIC - `gold_calendario` (dimension)
# MAGIC - `gold_fact_precios` (hecho con unpivot por combustible)
# MAGIC - `gold_super`, `gold_regular`, `gold_diesel`, `gold_kerosene` (agregados semanales)
# MAGIC - `gold_tendencias` (regresion lineal por combustible)
# MAGIC - `v_precios_completo` (vista unificada para BI)

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "combustibles_hn"
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Dimension Calendario
# MAGIC
# MAGIC Reemplaza tu `Calendario = CALENDARAUTO()` de Power BI.

# COMMAND ----------

# DBTITLE 1,Cell 4
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE combustibles_hn.gold.gold_calendario AS
# MAGIC WITH rango AS (
# MAGIC   SELECT MIN(Fecha) AS fecha_min, MAX(Fecha) AS fecha_max
# MAGIC   FROM combustibles_hn.silver.silver_precios_semanales
# MAGIC ),
# MAGIC fechas AS (
# MAGIC   SELECT explode(sequence(rango.fecha_min, rango.fecha_max, interval 1 day)) AS Fecha
# MAGIC   FROM rango
# MAGIC )
# MAGIC SELECT
# MAGIC   Fecha,
# MAGIC   YEAR(Fecha)                                              AS Anio,
# MAGIC   MONTH(Fecha)                                             AS Mes,
# MAGIC   DAY(Fecha)                                               AS Dia,
# MAGIC   WEEKOFYEAR(Fecha)                                        AS Semana,
# MAGIC   DATE_TRUNC('WEEK', Fecha)                                AS Inicio_Semana,
# MAGIC   DATE_FORMAT(Fecha, 'EEEE')                               AS Nombre_Dia,
# MAGIC   CONCAT(
# MAGIC     DATE_FORMAT(DATE_TRUNC('WEEK', Fecha), 'dd-MMM-yy'),
# MAGIC     ' - ',
# MAGIC     DATE_FORMAT(DATE_TRUNC('WEEK', Fecha) + INTERVAL 6 DAYS, 'dd-MMM-yy')
# MAGIC   )                                                        AS Rango_Semana
# MAGIC FROM fechas;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Hecho: gold_fact_precios (unpivot)
# MAGIC
# MAGIC Equivale al paso `Otras columnas con anulacion de dinamizacion` de Power Query.
# MAGIC Tambien calcula Valor_Anterior, Diferencia, Diferencia_Porcentual y Tipo_Aumento.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE combustibles_hn.gold.gold_fact_precios AS
# MAGIC WITH largo AS (
# MAGIC   SELECT Fecha, Anio, 'Super'    AS Atributo, Super    AS Valor_Actual
# MAGIC     FROM combustibles_hn.silver.silver_precios_semanales WHERE Super    IS NOT NULL
# MAGIC   UNION ALL
# MAGIC   SELECT Fecha, Anio, 'Regular'  AS Atributo, Regular  AS Valor_Actual
# MAGIC     FROM combustibles_hn.silver.silver_precios_semanales WHERE Regular  IS NOT NULL
# MAGIC   UNION ALL
# MAGIC   SELECT Fecha, Anio, 'Diesel'   AS Atributo, Diesel   AS Valor_Actual
# MAGIC     FROM combustibles_hn.silver.silver_precios_semanales WHERE Diesel   IS NOT NULL
# MAGIC   UNION ALL
# MAGIC   SELECT Fecha, Anio, 'Kerosene' AS Atributo, Kerosene AS Valor_Actual
# MAGIC     FROM combustibles_hn.silver.silver_precios_semanales WHERE Kerosene IS NOT NULL
# MAGIC ),
# MAGIC con_lag AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     LAG(Valor_Actual) OVER (PARTITION BY Atributo ORDER BY Fecha) AS Valor_Anterior
# MAGIC   FROM largo
# MAGIC )
# MAGIC SELECT
# MAGIC   Fecha,
# MAGIC   Anio,
# MAGIC   Atributo,
# MAGIC   CASE Atributo
# MAGIC     WHEN 'Super'    THEN 1
# MAGIC     WHEN 'Regular'  THEN 2
# MAGIC     WHEN 'Diesel'   THEN 3
# MAGIC     WHEN 'Kerosene' THEN 4
# MAGIC   END                                                    AS Orden_Atributo,
# MAGIC   Valor_Actual,
# MAGIC   Valor_Anterior,
# MAGIC   (Valor_Actual - Valor_Anterior)                        AS Diferencia,
# MAGIC   CASE
# MAGIC     WHEN Valor_Anterior IS NULL OR Valor_Anterior = 0 THEN NULL
# MAGIC     ELSE (Valor_Actual - Valor_Anterior) / Valor_Anterior
# MAGIC   END                                                    AS Diferencia_Porcentual,
# MAGIC   CASE
# MAGIC     WHEN Valor_Anterior IS NULL                THEN 'Inicio de Serie'
# MAGIC     WHEN Valor_Actual > Valor_Anterior         THEN 'Alza'
# MAGIC     WHEN Valor_Actual < Valor_Anterior         THEN 'Baja'
# MAGIC     ELSE 'Se Mantiene'
# MAGIC   END                                                    AS Tipo_Aumento,
# MAGIC   CASE Atributo
# MAGIC     WHEN 'Super'    THEN '#374649'
# MAGIC     WHEN 'Diesel'   THEN '#3F6C3E'
# MAGIC     WHEN 'Regular'  THEN '#982C33'
# MAGIC     WHEN 'Kerosene' THEN '#134966'
# MAGIC   END                                                    AS Color
# MAGIC FROM con_lag;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Agregados semanales por combustible
# MAGIC
# MAGIC Equivalen a tus tablas `Super`, `Regular`, `Diesel`, `Kerosene` del modelo Power BI
# MAGIC (Date.StartOfWeek con Monday + List.Average).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE combustibles_hn.gold.gold_super AS
# MAGIC SELECT DATE_TRUNC('WEEK', Fecha) AS fecha, AVG(Valor_Actual) AS valor
# MAGIC FROM combustibles_hn.gold.gold_fact_precios
# MAGIC WHERE Atributo = 'Super'
# MAGIC GROUP BY DATE_TRUNC('WEEK', Fecha)
# MAGIC ORDER BY fecha;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE combustibles_hn.gold.gold_regular AS
# MAGIC SELECT DATE_TRUNC('WEEK', Fecha) AS fecha, AVG(Valor_Actual) AS valor
# MAGIC FROM combustibles_hn.gold.gold_fact_precios
# MAGIC WHERE Atributo = 'Regular'
# MAGIC GROUP BY DATE_TRUNC('WEEK', Fecha)
# MAGIC ORDER BY fecha;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE combustibles_hn.gold.gold_diesel AS
# MAGIC SELECT DATE_TRUNC('WEEK', Fecha) AS fecha, AVG(Valor_Actual) AS valor
# MAGIC FROM combustibles_hn.gold.gold_fact_precios
# MAGIC WHERE Atributo = 'Diesel'
# MAGIC GROUP BY DATE_TRUNC('WEEK', Fecha)
# MAGIC ORDER BY fecha;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE combustibles_hn.gold.gold_kerosene AS
# MAGIC SELECT DATE_TRUNC('WEEK', Fecha) AS fecha, AVG(Valor_Actual) AS valor
# MAGIC FROM combustibles_hn.gold.gold_fact_precios
# MAGIC WHERE Atributo = 'Kerosene'
# MAGIC GROUP BY DATE_TRUNC('WEEK', Fecha)
# MAGIC ORDER BY fecha;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tendencias (regresion lineal por combustible)
# MAGIC
# MAGIC Reemplaza la tabla calculada `Tendencias Combistibles` (UNION de LINEST x 4).
# MAGIC Aplicamos minimos cuadrados directamente en SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE combustibles_hn.gold.gold_tendencias AS
# MAGIC WITH base AS (
# MAGIC   SELECT 'Super'    AS Atributo, CAST(fecha AS DOUBLE) AS x, valor AS y FROM combustibles_hn.gold.gold_super    WHERE valor IS NOT NULL
# MAGIC   UNION ALL SELECT 'Regular',    CAST(fecha AS DOUBLE), valor FROM combustibles_hn.gold.gold_regular  WHERE valor IS NOT NULL
# MAGIC   UNION ALL SELECT 'Diesel',     CAST(fecha AS DOUBLE), valor FROM combustibles_hn.gold.gold_diesel   WHERE valor IS NOT NULL
# MAGIC   UNION ALL SELECT 'Kerosene',   CAST(fecha AS DOUBLE), valor FROM combustibles_hn.gold.gold_kerosene WHERE valor IS NOT NULL
# MAGIC ),
# MAGIC stats AS (
# MAGIC   SELECT
# MAGIC     Atributo,
# MAGIC     COUNT(*) AS n,
# MAGIC     AVG(x)   AS xb,
# MAGIC     AVG(y)   AS yb,
# MAGIC     SUM(x*y) AS sxy,
# MAGIC     SUM(x*x) AS sxx,
# MAGIC     SUM(y*y) AS syy
# MAGIC   FROM base
# MAGIC   GROUP BY Atributo
# MAGIC )
# MAGIC SELECT
# MAGIC   Atributo,
# MAGIC   (sxy - n*xb*yb) / NULLIF((sxx - n*xb*xb), 0)                           AS Slope1,
# MAGIC   yb - ((sxy - n*xb*yb) / NULLIF((sxx - n*xb*xb), 0)) * xb               AS Intercept,
# MAGIC   POWER((sxy - n*xb*yb), 2) / NULLIF((sxx - n*xb*xb)*(syy - n*yb*yb), 0) AS R2,
# MAGIC   n                                                                     AS Observaciones
# MAGIC FROM stats;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Vista unificada para BI

# COMMAND ----------

# DBTITLE 1,Cell 15
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW combustibles_hn.gold.v_precios_completo AS
# MAGIC SELECT
# MAGIC   p.Fecha,
# MAGIC   p.Anio,
# MAGIC   p.Atributo,
# MAGIC   p.Orden_Atributo,
# MAGIC   p.Valor_Actual,
# MAGIC   p.Valor_Anterior,
# MAGIC   p.Diferencia,
# MAGIC   p.Diferencia_Porcentual,
# MAGIC   p.Tipo_Aumento,
# MAGIC   p.Color,
# MAGIC   c.Semana,
# MAGIC   c.Nombre_Dia,
# MAGIC   c.Inicio_Semana,
# MAGIC   c.Rango_Semana,
# MAGIC   t.Slope1,
# MAGIC   t.Intercept,
# MAGIC   t.R2,
# MAGIC   ROUND(t.Intercept + t.Slope1 * CAST(CAST(p.Fecha AS TIMESTAMP) AS DOUBLE), 2) AS Precio_Estimado
# MAGIC FROM combustibles_hn.gold.gold_fact_precios p
# MAGIC LEFT JOIN combustibles_hn.gold.gold_calendario c ON p.Fecha = c.Fecha
# MAGIC LEFT JOIN combustibles_hn.gold.gold_tendencias t ON p.Atributo = t.Atributo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Optimizaciones (OPCIONAL)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE combustibles_hn.gold.gold_fact_precios ZORDER BY (Fecha, Atributo);
# MAGIC OPTIMIZE combustibles_hn.gold.gold_calendario   ZORDER BY (Fecha);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validacion final del modelo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'gold_calendario'    AS tabla, COUNT(*) AS filas FROM combustibles_hn.gold.gold_calendario
# MAGIC UNION ALL SELECT 'gold_fact_precios',  COUNT(*) FROM combustibles_hn.gold.gold_fact_precios
# MAGIC UNION ALL SELECT 'gold_super',         COUNT(*) FROM combustibles_hn.gold.gold_super
# MAGIC UNION ALL SELECT 'gold_regular',       COUNT(*) FROM combustibles_hn.gold.gold_regular
# MAGIC UNION ALL SELECT 'gold_diesel',        COUNT(*) FROM combustibles_hn.gold.gold_diesel
# MAGIC UNION ALL SELECT 'gold_kerosene',      COUNT(*) FROM combustibles_hn.gold.gold_kerosene
# MAGIC UNION ALL SELECT 'gold_tendencias',    COUNT(*) FROM combustibles_hn.gold.gold_tendencias
# MAGIC UNION ALL SELECT 'v_precios_completo (vista)', COUNT(*) FROM combustibles_hn.gold.v_precios_completo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Atributo, Slope1, Intercept, R2, Observaciones
# MAGIC FROM combustibles_hn.gold.gold_tendencias
# MAGIC ORDER BY Atributo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Atributo, Fecha, Valor_Actual, Precio_Estimado, Tipo_Aumento
# MAGIC FROM combustibles_hn.gold.v_precios_completo
# MAGIC ORDER BY Fecha DESC, Orden_Atributo
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Siguiente paso
# MAGIC
# MAGIC 1. **Dashboard Databricks:** menu lateral -> Dashboards -> New Dashboard -> conectar a `v_precios_completo`.
# MAGIC 2. **Power BI:** Get Data -> Azure Databricks -> SQL Warehouse -> seleccionar `gold.v_precios_completo`.
# MAGIC 3. **Tableau:** Connect -> Databricks -> usar las mismas credenciales.
