# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 01 - Capa BRONZE: Ingesta de datos crudos
# MAGIC
# MAGIC **Proyecto:** Combustibles en Honduras
# MAGIC **Capa:** Bronze (datos AS-IS, sin transformaciones)
# MAGIC
# MAGIC **Fuentes:**
# MAGIC 1. CSV `historico_combustibles.csv` con datos 2017-2025 (subido al volumen `bronze.raw`).
# MAGIC 2. Web scraping de `proceso.hn` para el **ano en curso** (detectado automaticamente).
# MAGIC
# MAGIC **Salidas (2 tablas Delta):**
# MAGIC - `combustibles_hn.bronze.bronze_csv_historico`
# MAGIC - `combustibles_hn.bronze.bronze_web_{ANIO_EN_CURSO}` (nombre dinamico segun el ano)
# MAGIC
# MAGIC **Pre-requisitos:**
# MAGIC - Haber ejecutado `00_setup_catalogo.sql`.
# MAGIC - Haber subido el CSV al volumen como `historico_combustibles.csv`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuracion y variables

# COMMAND ----------

import re
from pyspark.sql import functions as F

CATALOG    = "combustibles_hn"
SCHEMA     = "bronze"
VOLUME_RAW = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Catalogo activo: {CATALOG}")
print(f"Esquema activo:  {SCHEMA}")
print(f"Volumen raw:     {VOLUME_RAW}")


def sanear_columnas(df):
    """Reemplaza caracteres no permitidos por Delta en nombres de columna.
    Ej.: 'LPG 25 Lbs' -> 'LPG_25_Lbs'. Delta no acepta: ' ,;{}()\\n\\t='."""
    for c in df.columns:
        nuevo = re.sub(r"[ ,;{}()\n\t=]+", "_", c).strip("_")
        if c != nuevo:
            df = df.withColumnRenamed(c, nuevo)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificar contenido del volumen
# MAGIC
# MAGIC Antes de leer el CSV, confirmamos que esta subido.

# COMMAND ----------

archivos = dbutils.fs.ls(VOLUME_RAW)
for f in archivos:
    print(f"{f.name:<50} {f.size:>10} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze parte 1 - CSV historico 2017-2025
# MAGIC
# MAGIC Reglas Bronze:
# MAGIC - **NO se infiere schema:** todo se lee como STRING.
# MAGIC - **NO se transforma nada:** solo se anaden columnas de metadata (`_fuente`, `_ingesta_ts`).

# COMMAND ----------

ruta_csv = f"{VOLUME_RAW}/historico_combustibles.csv"

df_hist = (
    spark.read
         .option("header", True)
         .option("encoding", "UTF-8")
         .option("inferSchema", False)
         .csv(ruta_csv)
)

print(f"Filas leidas del CSV: {df_hist.count()}")
print(f"Columnas: {df_hist.columns}")
df_hist.show(5, truncate=False)

# COMMAND ----------

# Aniadir metadata y guardar como tabla Delta
df_hist_bronze = (
    df_hist
      .withColumn("_fuente",     F.lit("csv_historico_combustibles"))
      .withColumn("_ingesta_ts", F.current_timestamp())
)
df_hist_bronze = sanear_columnas(df_hist_bronze)   # 'LPG 25 Lbs' -> 'LPG_25_Lbs'

(df_hist_bronze
   .write
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_csv_historico"))

print(f"Guardada: {CATALOG}.{SCHEMA}.bronze_csv_historico")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Bronze parte 2 - Scraping de proceso.hn (ano en curso)

# COMMAND ----------

import datetime
import pandas as pd
import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}

# El ano en curso se detecta automaticamente.
# Cuando llegue 2027, 2028... el notebook sigue funcionando sin cambios.
ANIO_EN_CURSO = datetime.date.today().year
URL_BASE      = "https://proceso.hn/tabla-de-precios-combustibles-{anio}/"

URLS = {
    ANIO_EN_CURSO: URL_BASE.format(anio=ANIO_EN_CURSO),
}

print(f"Ano en curso detectado: {ANIO_EN_CURSO}")
print(f"URL a scrapear: {URLS[ANIO_EN_CURSO]}")


def scrape_anio(anio: int, url: str) -> "pd.DataFrame":
    """
    Descarga la pagina de proceso.hn y devuelve la tabla de precios como DataFrame.

    En lugar de hardcodear tablas[2], buscamos por columnas de combustibles
    para que resista cambios de estructura en proceso.hn.
    """
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    tablas = pd.read_html(resp.text)
    print(f"[{anio}] {len(tablas)} tablas detectadas")

    columnas_objetivo = {"super", "súper", "regular", "diesel", "diésel", "kerosene"}
    for i, t in enumerate(tablas):
        cols_lower = {str(c).strip().lower() for c in t.columns}
        if len(cols_lower & columnas_objetivo) >= 2:
            print(f"  -> Tabla [{i}] elegida (matches: {cols_lower & columnas_objetivo})")
            df = t.copy()
            df["AnioFuente"] = anio
            return df

    raise ValueError(f"[{anio}] Ninguna tabla tiene columnas de combustibles.")

# COMMAND ----------

 #%pip install --quiet lxml html5lib beautifulsoup4
 #dbutils.library.restartPython()

# COMMAND ----------

# Scraping del ano en curso (ANIO_EN_CURSO se calcula arriba automaticamente)
for anio, url in URLS.items():
    try:
        df_pd = scrape_anio(anio, url)
        print(f"[{anio}] Filas: {len(df_pd)}, columnas: {list(df_pd.columns)}")
    except Exception as e:
        print(f"[{anio}] ERROR scraping: {type(e).__name__}: {e}")
        print(f"[{anio}] Tip: si dice ImportError de lxml, descomenta la celda %pip de arriba y reinicia.")
        print(f"[{anio}] Tip: si dice 403/503, proceso.hn bloqueo la IP; avisa para usar cloudscraper.")
        continue

    # pandas -> Spark (forzando todo a string en Bronze)
    df_spark = spark.createDataFrame(df_pd.astype(str))

    df_bronze = (
        df_spark
          .withColumn("_fuente",     F.lit(f"web_proceso_hn_{anio}"))
          .withColumn("_ingesta_ts", F.current_timestamp())
    )
    df_bronze = sanear_columnas(df_bronze)   # 'LPG 25 Lbs' -> 'LPG_25_Lbs'

    tabla_destino = f"{CATALOG}.{SCHEMA}.bronze_web_{anio}"
    (df_bronze
        .write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabla_destino))

    print(f"[{anio}] Guardada: {tabla_destino} ({df_bronze.count()} filas)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Plan B (manual) si proceso.hn bloquea el scraping
# MAGIC
# MAGIC Si arriba dio error, descomenta la siguiente celda y subi tu archivo HTML
# MAGIC o CSV al volumen `bronze.raw`.

# COMMAND ----------

# Plan B: si proceso.hn bloquea el scraping para el ano en curso,
# descarga la pagina manualmente desde el navegador (Ctrl+S),
# subila al volumen como precios_{ANIO_EN_CURSO}.html y descomenta lo siguiente:

# ruta_html = f"{VOLUME_RAW}/precios_{ANIO_EN_CURSO}.html"
# with open(ruta_html, "r", encoding="utf-8") as fh:
#     html = fh.read()
# tablas = pd.read_html(html)
# df_anio = tablas[2].copy()
# df_anio["AnioFuente"] = ANIO_EN_CURSO
# spark.createDataFrame(df_anio.astype(str)) \
#   .withColumn("_fuente", F.lit(f"web_proceso_hn_{ANIO_EN_CURSO}_manual")) \
#   .withColumn("_ingesta_ts", F.current_timestamp()) \
#   .write.mode("overwrite").option("overwriteSchema", "true") \
#   .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_web_{ANIO_EN_CURSO}")

# Plan C (mas simple): copia la tabla del navegador, pegala en Excel,
# guarda como precios_{ANIO_EN_CURSO}.csv, subila al volumen y leela igual que el historico.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validacion final

# COMMAND ----------

# Validacion con Python (porque el nombre de tabla incluye el ano dinamico)
df_count_hist = spark.sql("SELECT COUNT(*) AS filas FROM combustibles_hn.bronze.bronze_csv_historico").collect()[0][0]
df_count_web  = spark.sql(f"SELECT COUNT(*) AS filas FROM combustibles_hn.bronze.bronze_web_{ANIO_EN_CURSO}").collect()[0][0]
print(f"bronze_csv_historico (hasta {ANIO_EN_CURSO-1}): {df_count_hist} filas")
print(f"bronze_web_{ANIO_EN_CURSO}:                     {df_count_web} filas")

# COMMAND ----------

# Mostrar todos los datos del webscraping agrupados por combustible
tabla_web = f"{CATALOG}.{SCHEMA}.bronze_web_{ANIO_EN_CURSO}"
df_web = spark.table(tabla_web)

# Detectar columnas de combustibles
columnas_combustible = [c for c in df_web.columns if re.search(r"(super|súper|regular|diesel|diésel|kerosene)", c, re.IGNORECASE)]

# Mostrar datos por cada combustible, incluyendo la fecha
for col in columnas_combustible:
    print(f"\nDatos para combustible: {col}")
    display(df_web.select("AnioFuente", "Fech", col, "fuente", "ingesta_ts"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Siguiente paso
# MAGIC
# MAGIC Corre el notebook **`02_silver_limpieza.py`** para limpiar y unificar todo.
