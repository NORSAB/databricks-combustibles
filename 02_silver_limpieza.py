# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 02 - Capa SILVER: Limpieza y unificacion
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras
# MAGIC **Capa:** Silver (datos limpios, tipados, unificados)
# MAGIC
# MAGIC **Entrada:** las 2 tablas Bronze.
# MAGIC **Salida (1 tabla Delta):** `combustibles_hn.silver.silver_precios_semanales`
# MAGIC
# MAGIC **Que hace este notebook:**
# MAGIC 1. Toma el CSV historico (2017-2025) y la tabla scraping 2026.
# MAGIC 2. Aplica las correcciones de texto que tenias en Power Query (fechas mal formateadas en proceso.hn).
# MAGIC 3. Convierte el texto raro de fecha (ej. "31-6 de feb 22") en un `DATE` real.
# MAGIC 4. Tipa los precios a `DECIMAL(10,2)`.
# MAGIC 5. Une las 2 fuentes en una sola tabla unificada.

# COMMAND ----------

import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

CATALOG       = "combustibles_hn"
ANIO_EN_CURSO = datetime.date.today().year

spark.sql(f"USE CATALOG {CATALOG}")
print(f"Ano en curso: {ANIO_EN_CURSO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Catalogo de correcciones de texto en la columna Fecha
# MAGIC
# MAGIC Estas son **exactamente** las mismas `Table.ReplaceValue` que tenias en Power Query.
# MAGIC Se aplican en orden, una tras otra.

# COMMAND ----------

CORRECCIONES_FECHA = [
    ("27-30 de feb 22",      "27 ene -30 de feb 22"),
    ("25 ab-1 may 2022",     "25 abril -1 may 2022"),
    ("9-21 de agosto",       "15-21 de agosto"),
    ("26 dic  1 enero",      "26 diciembre 1"),
    ("30-05 febrero 2023",   "30 enero -05 febrero 2023"),
    ("28 oc-3 nov",          "28 oct - 3 nov"),
    ("25 al 1 diciembre",    "25 noviembre al 1 diciembre"),
    ("27 en-2 feb",          "27 enero - 2 feb"),
    ("24-2 mar",             "24 feb -2 mar"),
    ("31-6 abr",             "31 mar -6 abr"),
    ("26 -1 junio",          "26 mayo -1 junio"),
    ("30-6 julio",           "30 junio -6 julio"),
    ("28-4 mayo",            "28 abril  -4 mayo"),
    ("18-24",                "18-24 de agosto"),
    ("28 -3 agos",           "28 julio -3 agos"),
    ("29-5 oct",             "29 sep -5 oct"),
    ("27-2 nov",             "27 oct -2 nov"),
    ("29-4 ene",             "29 dic -4 ene"),
    ("29 dic -4 ene",        "29 dic"),
    ("26-1 feb",             "26 ene -1 feb"),
    ("23-1 mar",             "23 feb -1 mar"),
    ("30-5 abril",           "30 mar -5 abril"),
    ("27-3 mayo",            "27 abril -3 mayo"),
    ("31-6 de sept 2020",    "31 ago -6 de sept 2020"),
    ("31-6 de feb 22",       "31 ene -6 de feb 22"),
    ("14 al 27 mayo 2018",   "21 al 27 mayo 2018"),
    ("31-6 agosto 2023",     "31 jul - 6 agosto 2023"),
    ("28-3 sept 2023",       "28 agos -3 sept 2023"),
    ("30 dic-05 ene 2025",   "30 diciembre 2024"),
    # Correcciones adicionales con espacios al inicio/final
    (" 26-1 feb ",           "26 ene -1 feb"),
    (" 23-1 mar ",           "23 feb -1 mar"),
    (" 30-5 abril ",         "30 mar -5 abril"),
    (" 27-3 mayo ",          "27 abril -3 mayo"),
    ("26-1 feb",             "26 ene -1 feb"),
    ("23-1 mar",             "23 feb -1 mar"),
    ("30-5 abril",           "30 mar -5 abril"),
    ("27-3 mayo",            "27 abril -3 mayo"),
]

def aplicar_correcciones(col):
    """Aplica todas las correcciones de texto sobre una columna."""
    expr = F.trim(col)
    for buscar, reemplazar in CORRECCIONES_FECHA:
        expr = F.regexp_replace(expr, F.lit(buscar.strip()), F.lit(reemplazar.strip()))
    return expr

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Funciones para extraer dia y mes desde el texto

# COMMAND ----------

MESES_ES = [
    ("ene", 1),  ("feb", 2),  ("mar", 3),  ("abr", 4),
    ("may", 5),  ("jun", 6),  ("jul", 7),  ("ago", 8),
    ("sep", 9),  ("oct", 10), ("nov", 11), ("dic", 12),
]

def expr_mes(col_normalizada):
    """
    Devuelve un numero 1..12 segun la primera abreviatura de mes que aparezca
    en el texto normalizado. Equivale a la cadena de IF de Power Query.
    """
    cadena = None
    for txt, num in MESES_ES:
        cond = col_normalizada.contains(txt)
        cadena = F.when(cond, F.lit(num)) if cadena is None else cadena.when(cond, F.lit(num))
    return cadena.otherwise(F.lit(None).cast("int"))


def expr_dia_inicio(col_texto):
    """
    Replica:
        splitFecha = Splitter.SplitTextByDelimiter(' ')([Fecha])
        splitsplitFecha0 = Splitter.SplitTextByDelimiter('-')(splitFecha{0}?)
        Text.Start(splitsplitFecha0{0}?, 2)
    """
    primer_token    = F.split(col_texto, " ").getItem(0)
    primer_subtoken = F.split(primer_token, "-").getItem(0)
    return F.substring(primer_subtoken, 1, 2).cast(IntegerType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Funcion principal de limpieza

# COMMAND ----------

# DBTITLE 1,Cell 8
def limpiar_anio_web(tabla_bronze: str, anio: int):
    """
    Limpia una tabla Bronze proveniente de scraping (2024, 2025, 2026).
    Devuelve un DataFrame con columnas: Fecha (date), Super, Regular, Diesel,
    Kerosene (decimal), Anio (int).
    """
    df = spark.table(tabla_bronze)

    # Renombrar columnas que varian entre 2024/2025/2026
    renombres = {
        "Diesel":      "Diesel",
        "Super":       "Super",
        "LPG":         "LPG_25_Lbs",
        "LPG 25 Lbs":  "LPG_25_Lbs",
        "Fech":        "Fecha",
    }
    for actual, nuevo in renombres.items():
        if actual in df.columns and actual != nuevo:
            df = df.withColumnRenamed(actual, nuevo)

    # Manejar el caracter especial "Super" (con acento) que puede venir distinto
    for c in df.columns:
        if c.lower().startswith("s") and "per" in c.lower():
            df = df.withColumnRenamed(c, "Super")
        if c.lower().startswith("di") and "sel" in c.lower():
            df = df.withColumnRenamed(c, "Diesel")

    columnas_precio = [c for c in ["Super", "Regular", "Diesel", "Kerosene"] if c in df.columns]

    # 1. Limpieza de simbolos en valores numericos
    for c in columnas_precio:
        df = df.withColumn(c, F.regexp_replace(F.col(c), r"[\*–—]", ""))
        df = df.withColumn(c, F.regexp_replace(F.col(c), r"\s+\*+", ""))
        df = df.withColumn(c, F.trim(F.col(c)))

    # 2. Limpieza de la columna Fecha y normalizacion
    df = (df
          .withColumn("Fecha", F.regexp_replace(F.col("Fecha"), r"[\*–—]", ""))
          .withColumn("fecha_norm", F.lower(F.trim(F.col("Fecha"))))
          .withColumn("fecha_norm", aplicar_correcciones(F.col("fecha_norm")))
    )

    # 3. Filas no vacias
    df = df.filter(F.coalesce(F.col("Fecha"), F.lit("")) != "")

    # 4. Cast a decimal
    for c in columnas_precio:
        df = df.withColumn(c, F.col(c).cast("decimal(10,2)"))

    # 5. Extraer dia / mes
    df = (df
          .withColumn("Anio",       F.lit(anio).cast("int"))
          .withColumn("dia_inicio", expr_dia_inicio(F.col("fecha_norm")))
          .withColumn("mes",        expr_mes(F.col("fecha_norm")))
    )

    # 6. Construir Fecha real (DATE)
    df = df.withColumn(
        "Fecha_real",
        F.to_date(
            F.concat_ws("/",
                F.col("dia_inicio").cast("string"),
                F.col("mes").cast("string"),
                F.col("Anio").cast("string")
            ),
            "d/M/yyyy"
        )
    )

    # Seleccion final - usar nombres sin guion bajo
    ingesta_col = "_ingesta_ts" if "_ingesta_ts" in df.columns else "ingesta_ts"
    select_cols = [F.col("Fecha_real").alias("Fecha")]
    for c in ["Super", "Regular", "Diesel", "Kerosene"]:
        if c in df.columns:
            select_cols.append(F.col(c))
        else:
            select_cols.append(F.lit(None).cast("decimal(10,2)").alias(c))
    select_cols += [
        F.col("Anio"),
        F.col("Fecha").alias("Fecha_texto_original"),
        F.col(ingesta_col).alias("_ingesta_ts"),
    ]
    return df.select(*select_cols)


def limpiar_historico(tabla_bronze: str):
    """
    El CSV historico ya trae columnas tipadas y un campo Ano numerico.
    Solo necesitamos castear y construir la Fecha real.
    """
    df = spark.table(tabla_bronze)

    # Posibles nombres de columnas del CSV
    for c in df.columns:
        if c.lower().startswith("s") and "per" in c.lower():
            df = df.withColumnRenamed(c, "Super")
        if c.lower().startswith("a") and "o" in c.lower() and len(c) <= 4:
            df = df.withColumnRenamed(c, "Anio")

    columnas_precio = [c for c in ["Super", "Regular", "Diesel", "Kerosene"] if c in df.columns]

    # Normalizar fecha y aplicar correcciones (por si el CSV trae los mismos errores)
    df = (df
          .withColumn("fecha_norm", F.lower(F.trim(F.col("Fecha"))))
          .withColumn("fecha_norm", aplicar_correcciones(F.col("fecha_norm")))
          .withColumn("dia_inicio", expr_dia_inicio(F.col("fecha_norm")))
          .withColumn("mes",        expr_mes(F.col("fecha_norm")))
          .withColumn("Anio",       F.col("Anio").cast("int"))
    )

    df = df.withColumn(
        "Fecha_real",
        F.to_date(
            F.concat_ws("/",
                F.col("dia_inicio").cast("string"),
                F.col("mes").cast("string"),
                F.col("Anio").cast("string")
            ),
            "d/M/yyyy"
        )
    )

    for c in columnas_precio:
        df = df.withColumn(c, F.col(c).cast("decimal(10,2)"))

    # Seleccion final - usar nombres sin guion bajo
    ingesta_col = "_ingesta_ts" if "_ingesta_ts" in df.columns else "ingesta_ts"
    select_cols = [F.col("Fecha_real").alias("Fecha")]
    for c in ["Super", "Regular", "Diesel", "Kerosene"]:
        if c in df.columns:
            select_cols.append(F.col(c))
        else:
            select_cols.append(F.lit(None).cast("decimal(10,2)").alias(c))
    select_cols += [
        F.col("Anio"),
        F.col("Fecha").alias("Fecha_texto_original"),
        F.col(ingesta_col).alias("_ingesta_ts"),
    ]
    return df.select(*select_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Aplicar limpieza a las 2 fuentes

# COMMAND ----------

df_hist = limpiar_historico("combustibles_hn.bronze.bronze_csv_historico")
df_anio = limpiar_anio_web(f"combustibles_hn.bronze.bronze_web_{ANIO_EN_CURSO}", ANIO_EN_CURSO)

print(f"Historico (hasta {ANIO_EN_CURSO-1}): {df_hist.count()} filas")
print(f"Ano en curso ({ANIO_EN_CURSO}):       {df_anio.count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Union y persistencia en Silver

# COMMAND ----------

# DBTITLE 1,Union, dedup, and order with fix
# Fix: defensively cast "Anio" to INT using try_cast pattern; malformed years become NULL instead of failing
from pyspark.sql.functions import col, when

df_silver = (
    df_hist
      .unionByName(df_anio, allowMissingColumns=True)
      .withColumn("Anio", when(col("Anio").cast("int").isNotNull(), col("Anio").cast("int")).otherwise(None))
      .filter(F.col("Fecha").isNotNull())
      .dropDuplicates(["Fecha", "Anio"])
      .orderBy("Fecha")
)

print(f"Silver total: {df_silver.count()} filas")
df_silver.show(10, truncate=False)

# COMMAND ----------

(df_silver
   .write
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable("combustibles_hn.silver.silver_precios_semanales"))

print("Guardada: combustibles_hn.silver.silver_precios_semanales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validacion

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   MIN(Fecha) AS desde,
# MAGIC   MAX(Fecha) AS hasta,
# MAGIC   COUNT(*)   AS filas,
# MAGIC   COUNT(DISTINCT Anio) AS anios
# MAGIC FROM combustibles_hn.silver.silver_precios_semanales;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Anio, COUNT(*) AS filas
# MAGIC FROM combustibles_hn.silver.silver_precios_semanales
# MAGIC GROUP BY Anio
# MAGIC ORDER BY Anio;

# COMMAND ----------

# Ver el esquema y datos de la tabla silver
df_silver_check = spark.table("combustibles_hn.silver.silver_precios_semanales")

print("=" * 80)
print("SCHEMA DE LA TABLA SILVER")
print("=" * 80)
df_silver_check.printSchema()

print("\n" + "=" * 80)
print("MUESTRA DE DATOS (primeras 20 filas)")
print("=" * 80)
df_silver_check.orderBy("Fecha").show(20, truncate=False)

print("\n" + "=" * 80)
print("ESTADÍSTICAS")
print("=" * 80)
print(f"Total de filas: {df_silver_check.count()}")
print(f"Rango de fechas: {df_silver_check.agg({'Fecha': 'min'}).collect()[0][0]} a {df_silver_check.agg({'Fecha': 'max'}).collect()[0][0]}")
print(f"Años únicos: {df_silver_check.select('Anio').distinct().count()}")

print("\n" + "=" * 80)
print("VERIFICAR FILAS CON FECHA NULL")
print("=" * 80)
nulls = df_silver_check.filter(F.col("Fecha").isNull()).count()
print(f"Filas con Fecha NULL: {nulls}")

print("\n" + "=" * 80)
print("VERIFICAR FILAS CON ANIO NULL")
print("=" * 80)
anio_nulls = df_silver_check.filter(F.col("Anio").isNull()).count()
print(f"Filas con Anio NULL: {anio_nulls}")
if anio_nulls > 0:
    print("\nEjemplos de filas con Anio NULL:")
    df_silver_check.filter(F.col("Anio").isNull()).select("Fecha", "Fecha_texto_original", "Anio").show(10, truncate=False)

# COMMAND ----------

# Mostrar los datos del año en curso ordenados de forma ascendente, por cada combustible
for combustible in ["Super", "Regular", "Diesel", "Kerosene"]:
    if combustible in df_silver_check.columns:
        df_anio_actual = (
            df_silver_check
            .filter((F.col("Anio") == ANIO_EN_CURSO) & F.col(combustible).isNotNull())
            .orderBy(F.col("Fecha").asc())
        )
        print(f"\n{'='*40}\n{combustible}: datos año en curso ordenados ascendente\n{'='*40}")
        display(df_anio_actual.select("Fecha", combustible, "Anio", "Fecha_texto_original"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Siguiente paso
# MAGIC Corre el notebook **`03_gold_modelo.py`** para construir el modelo analitico.
