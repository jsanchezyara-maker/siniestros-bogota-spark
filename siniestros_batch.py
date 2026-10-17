"""
Tarea 3 - Big Data (UNAD) - Procesamiento en batch con Apache Spark
Problema: ¿Cuándo, dónde y de qué tipo ocurren los siniestros viales en Bogotá,
y cuáles zonas y horarios concentran los siniestros más graves?

Dataset: "Histórico Siniestros Bogotá D.C." - Secretaría Distrital de Movilidad
https://datosabiertos.bogota.gov.co/dataset/historico-siniestros-bogota-d-c

Uso:
    spark-submit siniestros_batch.py <ruta_csv> [carpeta_resultados]
    o
    python3 siniestros_batch.py <ruta_csv> [carpeta_resultados]
"""

import os
import sys
import unicodedata

import matplotlib
matplotlib.use("Agg")  # permite guardar gráficas sin interfaz gráfica
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# 0. Parámetros y sesión de Spark
# ---------------------------------------------------------------------------
RUTA_CSV = sys.argv[1] if len(sys.argv) > 1 else "historico_siniestros_bogota.csv"
SALIDA = sys.argv[2] if len(sys.argv) > 2 else "resultados"
os.makedirs(SALIDA, exist_ok=True)

spark = (SparkSession.builder
         .appName("SiniestrosVialesBogota-Batch")
         # Valores mal formados se vuelven null en vez de detener el proceso
         .config("spark.sql.ansi.enabled", "false")
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
         # Las fechas vienen en UTC (+00); se leen tal cual, sin cambiar de día
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def titulo(texto):
    print("\n" + "=" * 70 + f"\n{texto}\n" + "=" * 70)


def sin_tildes(texto):
    return "".join(c for c in unicodedata.normalize("NFD", texto)
                   if unicodedata.category(c) != "Mn")


# ---------------------------------------------------------------------------
# 1. Carga de datos
# ---------------------------------------------------------------------------
titulo("1. CARGA DE DATOS")
df_raw = (spark.read
          .option("header", True)
          .option("inferSchema", False)   # todo como texto; tipamos nosotros
          .option("encoding", "UTF-8")
          .csv(RUTA_CSV))

# Normalizar nombres de columnas: MAYÚSCULAS, sin tildes ni espacios
for c in df_raw.columns:
    nuevo = sin_tildes(c).strip().upper().replace(" ", "_")
    if nuevo != c:
        df_raw = df_raw.withColumnRenamed(c, nuevo)

total_raw = df_raw.count()
print(f"Registros cargados: {total_raw:,}")
print(f"Columnas ({len(df_raw.columns)}): {df_raw.columns}")
df_raw.printSchema()
df_raw.show(5, truncate=False)

# ---------------------------------------------------------------------------
# 2. Calidad de datos: nulos por columna
# ---------------------------------------------------------------------------
titulo("2. VALORES NULOS O VACÍOS POR COLUMNA")
nulos = df_raw.select([
    F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)).alias(c)
    for c in df_raw.columns
])
nulos.show(truncate=False)

# ---------------------------------------------------------------------------
# 3. Limpieza y transformación
# ---------------------------------------------------------------------------
titulo("3. LIMPIEZA Y TRANSFORMACIÓN")


def texto_limpio(col):
    """Mayúsculas, sin espacios extra y sin tildes (Ñ se conserva como N)."""
    c = F.upper(F.trim(F.col(col)))
    c = F.translate(c, "ÁÉÍÓÚÜÑ", "AEIOUUN")
    return F.regexp_replace(c, r"\s+", " ")


# 3.1 Fecha y hora del siniestro: se prueban varios formatos posibles
formatos = ["yyyy/MM/dd HH:mm:ssX", "yyyy/MM/dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss", "dd/MM/yyyy HH:mm:ss", "dd/MM/yyyy HH:mm"]


def a_timestamp(col):
    return F.coalesce(*[F.to_timestamp(F.col(col), f) for f in formatos])


candidatos = []
if "FECHA_HORA_ACC" in df_raw.columns:
    candidatos.append(a_timestamp("FECHA_HORA_ACC"))
if {"FECHA", "HORA"} <= set(df_raw.columns):
    df_raw = df_raw.withColumn("_FH_TXT", F.concat_ws(" ", F.col("FECHA"), F.col("HORA")))
    candidatos.append(a_timestamp("_FH_TXT"))
if "FECHA_HORA" in df_raw.columns:
    candidatos.append(a_timestamp("FECHA_HORA"))
if not candidatos:
    raise SystemExit("No se encontró una columna de fecha/hora reconocible.")

df = (df_raw
      .withColumn("FECHA_HORA_TS", F.coalesce(*candidatos))
      .withColumn("GRAVEDAD", texto_limpio("GRAVEDAD"))
      .withColumn("CLASE_ACC", texto_limpio("CLASE_ACC"))
      .withColumn("LOCALIDAD", texto_limpio("LOCALIDAD"))
      .withColumn("LATITUD", F.col("LATITUD").cast("double"))
      .withColumn("LONGITUD", F.col("LONGITUD").cast("double")))

# 3.2 Eliminar duplicados (un mismo siniestro reportado más de una vez)
clave = "CODIGO_ACCIDENTE" if "CODIGO_ACCIDENTE" in df.columns else "FORMULARIO"
df = df.dropDuplicates([clave])
print(f"Después de quitar duplicados por {clave}: {df.count():,}")

# 3.3 Quitar registros sin fecha, gravedad o localidad
df = df.filter(F.col("FECHA_HORA_TS").isNotNull()
               & F.col("GRAVEDAD").isNotNull() & (F.col("GRAVEDAD") != "")
               & F.col("LOCALIDAD").isNotNull() & (F.col("LOCALIDAD") != ""))

# 3.4 Coordenadas fuera de Bogotá se marcan como nulas (no se borra el registro)
dentro_bogota = (F.col("LATITUD").between(3.7, 4.9) & F.col("LONGITUD").between(-74.5, -73.9))
df = (df.withColumn("LATITUD", F.when(dentro_bogota, F.col("LATITUD")))
        .withColumn("LONGITUD", F.when(dentro_bogota, F.col("LONGITUD"))))

# 3.5 Columnas derivadas para el análisis
dias = F.create_map(*[F.lit(x) for par in
                      [(1, "7-Domingo"), (2, "1-Lunes"), (3, "2-Martes"), (4, "3-Miercoles"),
                       (5, "4-Jueves"), (6, "5-Viernes"), (7, "6-Sabado")] for x in par])
df = (df
      .withColumn("ANIO", F.year("FECHA_HORA_TS"))
      .withColumn("MES", F.month("FECHA_HORA_TS"))
      .withColumn("HORA_DIA", F.hour("FECHA_HORA_TS"))
      .withColumn("DIA_SEMANA", dias[F.dayofweek("FECHA_HORA_TS")])
      .withColumn("FRANJA",
                  F.when(F.col("HORA_DIA") < 6, "Madrugada (0-5)")
                   .when(F.col("HORA_DIA") < 12, "Mañana (6-11)")
                   .when(F.col("HORA_DIA") < 18, "Tarde (12-17)")
                   .otherwise("Noche (18-23)"))
      .withColumn("ES_FATAL", F.when(F.col("GRAVEDAD").contains("MUERT"), 1).otherwise(0))
      .withColumn("CON_VICTIMAS",
                  F.when(F.col("GRAVEDAD").contains("MUERT") | F.col("GRAVEDAD").contains("HERID"), 1)
                   .otherwise(0))
      .select(clave, "FECHA_HORA_TS", "ANIO", "MES", "DIA_SEMANA", "HORA_DIA", "FRANJA",
              "GRAVEDAD", "CLASE_ACC", "LOCALIDAD", "LATITUD", "LONGITUD",
              "ES_FATAL", "CON_VICTIMAS"))

df.cache()
total = df.count()
print(f"Registros limpios: {total:,} ({total / total_raw:.1%} del total original)")
df.show(5, truncate=False)

# ---------------------------------------------------------------------------
# 4. Análisis exploratorio (EDA) con DataFrames
# ---------------------------------------------------------------------------
titulo("4. ANÁLISIS EXPLORATORIO")
resultados = {}


def guardar(nombre, df_res, mostrar=20):
    df_res.show(mostrar, truncate=False)
    (df_res.coalesce(1).write.mode("overwrite")
        .option("header", True).csv(os.path.join(SALIDA, "csv", nombre)))
    resultados[nombre] = df_res.toPandas()


print("\n4.1 Siniestros por año")
guardar("por_anio", df.groupBy("ANIO").agg(
    F.count("*").alias("SINIESTROS"),
    F.sum("CON_VICTIMAS").alias("CON_VICTIMAS"),
    F.sum("ES_FATAL").alias("FATALES")).orderBy("ANIO"))

print("\n4.2 Distribución por gravedad")
guardar("por_gravedad", df.groupBy("GRAVEDAD").count()
        .withColumn("PORCENTAJE", F.round(F.col("count") * 100 / total, 2))
        .orderBy(F.desc("count")))

print("\n4.3 Distribución por clase de siniestro")
guardar("por_clase", df.groupBy("CLASE_ACC").count()
        .withColumn("PORCENTAJE", F.round(F.col("count") * 100 / total, 2))
        .orderBy(F.desc("count")))

print("\n4.4 Localidades: total, con víctimas y tasa de letalidad")
guardar("por_localidad", df.groupBy("LOCALIDAD").agg(
    F.count("*").alias("SINIESTROS"),
    F.sum("CON_VICTIMAS").alias("CON_VICTIMAS"),
    F.sum("ES_FATAL").alias("FATALES"))
    .withColumn("PCT_FATALES", F.round(F.col("FATALES") * 100 / F.col("SINIESTROS"), 2))
    .orderBy(F.desc("SINIESTROS")))

print("\n4.5 Siniestros por hora del día")
guardar("por_hora", df.groupBy("HORA_DIA").agg(
    F.count("*").alias("SINIESTROS"),
    F.sum("ES_FATAL").alias("FATALES")).orderBy("HORA_DIA"), mostrar=24)

print("\n4.6 Siniestros por día de la semana")
guardar("por_dia_semana", df.groupBy("DIA_SEMANA").agg(
    F.count("*").alias("SINIESTROS"),
    F.sum("ES_FATAL").alias("FATALES")).orderBy("DIA_SEMANA"))

print("\n4.7 ¿En qué franja horaria es más probable un siniestro fatal?")
guardar("letalidad_franja", df.groupBy("FRANJA").agg(
    F.count("*").alias("SINIESTROS"),
    F.sum("ES_FATAL").alias("FATALES"))
    .withColumn("PCT_FATALES", F.round(F.col("FATALES") * 100 / F.col("SINIESTROS"), 3))
    .orderBy("FRANJA"))

print("\n4.8 Matriz día de la semana x hora (para el mapa de calor)")
matriz = df.groupBy("DIA_SEMANA").pivot("HORA_DIA", list(range(24))).count().na.fill(0) \
           .orderBy("DIA_SEMANA")
guardar("matriz_dia_hora", matriz, mostrar=7)

# ---------------------------------------------------------------------------
# 5. El mismo análisis con RDDs (transformaciones y acciones)
# ---------------------------------------------------------------------------
titulo("5. EJEMPLO CON RDDs")
rdd = df.select("LOCALIDAD", "CLASE_ACC", "ES_FATAL").rdd

# Transformaciones: map, filter, reduceByKey (perezosas, no se ejecutan aún)
fatales_por_localidad = (rdd
                         .filter(lambda r: r.ES_FATAL == 1)        # filter
                         .map(lambda r: (r.LOCALIDAD, 1))          # map -> (clave, 1)
                         .reduceByKey(lambda a, b: a + b))         # reduceByKey

# Acciones: disparan la ejecución
print("Particiones del RDD:", rdd.getNumPartitions())
print("Total de siniestros fatales (count):", rdd.filter(lambda r: r.ES_FATAL == 1).count())
print("Top 5 localidades con más siniestros fatales (takeOrdered):")
for loc, n in fatales_por_localidad.takeOrdered(5, key=lambda x: -x[1]):
    print(f"   {loc:<25} {n:>6}")

# flatMap: separar palabras de la clase de siniestro y contarlas
palabras = (rdd.flatMap(lambda r: (r.CLASE_ACC or "").split(" "))
               .filter(lambda w: len(w) > 2)
               .map(lambda w: (w, 1))
               .reduceByKey(lambda a, b: a + b))
print("Palabras más frecuentes en CLASE_ACC (take):", palabras.sortBy(lambda x: -x[1]).take(5))
print("Suma total con reduce:", rdd.map(lambda r: 1).reduce(lambda a, b: a + b))

# ---------------------------------------------------------------------------
# 6. Visualización
# ---------------------------------------------------------------------------
titulo("6. VISUALIZACIÓN")
graf = os.path.join(SALIDA, "graficas")
os.makedirs(graf, exist_ok=True)
AZUL, ROJO = "#2a6fdb", "#d64545"


def guardar_fig(nombre):
    ruta = os.path.join(graf, nombre)
    plt.tight_layout()
    plt.savefig(ruta, dpi=130)
    plt.close()
    print("Gráfica guardada:", ruta)


# 6.1 Evolución anual
p = resultados["por_anio"]
fig, ax = plt.subplots(figsize=(9, 4.5))
ax.plot(p["ANIO"], p["SINIESTROS"], marker="o", color=AZUL, label="Total")
ax.plot(p["ANIO"], p["CON_VICTIMAS"], marker="o", color=ROJO, label="Con víctimas")
ax.set_title("Siniestros viales por año - Bogotá")
ax.set_xlabel("Año"); ax.set_ylabel("Siniestros"); ax.legend(); ax.grid(alpha=.3)
guardar_fig("01_siniestros_por_anio.png")

# 6.2 Gravedad
p = resultados["por_gravedad"]
fig, ax = plt.subplots(figsize=(7, 4))
ax.barh(p["GRAVEDAD"], p["count"], color=AZUL)
ax.invert_yaxis(); ax.set_title("Siniestros por gravedad"); ax.set_xlabel("Siniestros")
guardar_fig("02_gravedad.png")

# 6.3 Top 10 localidades
p = resultados["por_localidad"].head(10)
fig, ax = plt.subplots(figsize=(9, 5))
ax.barh(p["LOCALIDAD"], p["SINIESTROS"], color=AZUL)
ax.invert_yaxis(); ax.set_title("Top 10 localidades con más siniestros"); ax.set_xlabel("Siniestros")
guardar_fig("03_top_localidades.png")

# 6.4 Por hora del día
p = resultados["por_hora"]
fig, ax = plt.subplots(figsize=(10, 4))
ax.bar(p["HORA_DIA"], p["SINIESTROS"], color=AZUL)
ax.set_xticks(range(24)); ax.set_title("Siniestros por hora del día")
ax.set_xlabel("Hora"); ax.set_ylabel("Siniestros")
guardar_fig("04_por_hora.png")

# 6.5 Clase de siniestro
p = resultados["por_clase"]
fig, ax = plt.subplots(figsize=(8, 4.5))
ax.barh(p["CLASE_ACC"], p["count"], color=AZUL)
ax.invert_yaxis(); ax.set_title("Siniestros por clase"); ax.set_xlabel("Siniestros")
guardar_fig("05_por_clase.png")

# 6.6 Mapa de calor día x hora
p = resultados["matriz_dia_hora"].set_index("DIA_SEMANA")
p.index = [d.split("-", 1)[1] for d in p.index]
fig, ax = plt.subplots(figsize=(12, 4))
im = ax.imshow(p.values, aspect="auto", cmap="Blues")
ax.set_xticks(range(24)); ax.set_xticklabels(range(24))
ax.set_yticks(range(len(p.index))); ax.set_yticklabels(p.index)
ax.set_title("Siniestros por día de la semana y hora"); ax.set_xlabel("Hora")
fig.colorbar(im, ax=ax, label="Siniestros")
guardar_fig("06_mapa_calor_dia_hora.png")

# 6.7 Porcentaje de siniestros fatales por franja horaria
p = resultados["letalidad_franja"]
fig, ax = plt.subplots(figsize=(7, 4))
ax.bar(p["FRANJA"], p["PCT_FATALES"], color=ROJO)
ax.set_title("% de siniestros con muertos por franja horaria"); ax.set_ylabel("%")
guardar_fig("07_letalidad_franja.png")

titulo("PROCESO TERMINADO")
print(f"Resultados en: {os.path.abspath(SALIDA)}")
spark.stop()
