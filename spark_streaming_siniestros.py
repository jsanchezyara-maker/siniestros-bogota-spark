"""
Tarea 3 - Big Data (UNAD) - Consumidor con Spark Structured Streaming
Lee los siniestros simulados del topic de Kafka "siniestros_viales" y calcula,
en tiempo real:
  1. Siniestros por gravedad en ventanas de 1 minuto.
  2. Panel acumulado por localidad (total, con víctimas, fatales) con gráfica.
  3. Alertas: localidades con 8 o más siniestros con víctimas en los últimos 2 minutos.

Ejecución (primero el productor, luego este script):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
        spark_streaming_siniestros.py
"""

from datetime import datetime

import matplotlib
matplotlib.use("Agg")  # guardar la gráfica sin interfaz gráfica
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

TOPIC = "siniestros_viales"
GRAFICA = "panel_streaming_localidades.png"
UMBRAL_ALERTA = 8   # siniestros con víctimas en 2 minutos para disparar una alerta

spark = (SparkSession.builder
         .appName("SiniestrosVialesBogota-Streaming")
         .config("spark.sql.shuffle.partitions", "4")   # pocos datos: menos particiones
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 1. Esquema del mensaje JSON que envía el productor
# ---------------------------------------------------------------------------
esquema = StructType([
    StructField("codigo_accidente", StringType()),
    StructField("localidad", StringType()),
    StructField("clase_acc", StringType()),
    StructField("gravedad", StringType()),
    StructField("latitud", DoubleType()),
    StructField("longitud", DoubleType()),
    StructField("timestamp", LongType()),      # segundos desde 1970 (epoch)
])

# ---------------------------------------------------------------------------
# 2. Lectura del stream desde Kafka
# ---------------------------------------------------------------------------
df_kafka = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", TOPIC)
            .option("startingOffsets", "latest")
            .load())

eventos = (df_kafka
           .select(F.from_json(F.col("value").cast("string"), esquema).alias("d"))
           .select("d.*")
           .withColumn("fecha_hora", F.col("timestamp").cast("timestamp"))
           .withColumn("con_victimas",
                       F.when(F.col("gravedad").isin("CON HERIDOS", "CON MUERTOS"), 1).otherwise(0))
           .withColumn("fatal", F.when(F.col("gravedad") == "CON MUERTOS", 1).otherwise(0))
           .filter(F.col("localidad").isNotNull()))

# ---------------------------------------------------------------------------
# 3. Análisis 1: siniestros por gravedad en ventanas de 1 minuto
# ---------------------------------------------------------------------------
por_ventana = (eventos
               .groupBy(F.window("fecha_hora", "1 minute").alias("ventana"))
               .agg(F.count("*").alias("total"),
                    F.sum(F.when(F.col("gravedad") == "SOLO DANOS", 1).otherwise(0)).alias("solo_danos"),
                    F.sum(F.when(F.col("gravedad") == "CON HERIDOS", 1).otherwise(0)).alias("con_heridos"),
                    F.sum("fatal").alias("con_muertos"))
               .select(F.date_format("ventana.start", "HH:mm:ss").alias("desde"),
                       F.date_format("ventana.end", "HH:mm:ss").alias("hasta"),
                       "total", "solo_danos", "con_heridos", "con_muertos")
               .orderBy("desde"))

q_ventanas = (por_ventana.writeStream
              .queryName("siniestros_por_minuto")
              .outputMode("complete")
              .format("console")
              .option("truncate", False)
              .trigger(processingTime="10 seconds")
              .start())

# ---------------------------------------------------------------------------
# 4. Análisis 2: panel acumulado por localidad + visualización
# ---------------------------------------------------------------------------
por_localidad = (eventos.groupBy("localidad")
                 .agg(F.count("*").alias("total"),
                      F.sum("con_victimas").alias("con_victimas"),
                      F.sum("fatal").alias("fatales")))


def mostrar_panel(batch_df, batch_id):
    """Se ejecuta en cada micro-lote: imprime un ranking con barras y guarda una gráfica."""
    datos = batch_df.orderBy(F.desc("total")).toPandas()
    if datos.empty:
        return
    datos["pct_victimas"] = (datos["con_victimas"] * 100 / datos["total"]).round(1)
    total = int(datos["total"].sum())
    maximo = int(datos["total"].max())

    print("\n" + "=" * 78)
    print(f" PANEL EN TIEMPO REAL | lote {batch_id} | {datetime.now():%H:%M:%S} "
          f"| siniestros recibidos: {total}")
    print("=" * 78)
    print(f" {'LOCALIDAD':<20}{'TOTAL':>6}{'VICT.':>7}{'FATAL':>7}{'%VICT':>7}  GRÁFICA")
    for _, r in datos.head(10).iterrows():
        barra = "#" * max(1, round(30 * r["total"] / maximo))
        print(f" {r['localidad']:<20}{r['total']:>6}{r['con_victimas']:>7}"
              f"{r['fatales']:>7}{r['pct_victimas']:>7}  {barra}")

    # Gráfica: top 10 localidades (total vs. con víctimas)
    top = datos.head(10).iloc[::-1]
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.barh(top["localidad"], top["total"], color="#2a6fdb", label="Total")
    ax.barh(top["localidad"], top["con_victimas"], color="#d64545", label="Con víctimas")
    ax.set_title(f"Siniestros en tiempo real por localidad - {datetime.now():%H:%M:%S} "
                 f"({total} eventos)")
    ax.set_xlabel("Siniestros")
    ax.legend(loc="lower right")
    plt.tight_layout()
    plt.savefig(GRAFICA, dpi=110)
    plt.close()


q_panel = (por_localidad.writeStream
           .queryName("panel_localidades")
           .outputMode("complete")
           .foreachBatch(mostrar_panel)
           .trigger(processingTime="10 seconds")
           .start())

# ---------------------------------------------------------------------------
# 5. Análisis 3: alertas con ventana deslizante (2 min, se actualiza cada 30 s)
# ---------------------------------------------------------------------------
alertas = (eventos
           .withWatermark("fecha_hora", "1 minute")
           .groupBy(F.window("fecha_hora", "2 minutes", "30 seconds").alias("ventana"),
                    "localidad")
           .agg(F.sum("con_victimas").alias("siniestros_con_victimas"))
           .filter(F.col("siniestros_con_victimas") >= UMBRAL_ALERTA)
           .select(F.lit("ALERTA").alias("tipo"),
                   F.date_format("ventana.start", "HH:mm:ss").alias("desde"),
                   F.date_format("ventana.end", "HH:mm:ss").alias("hasta"),
                   "localidad", "siniestros_con_victimas"))

q_alertas = (alertas.writeStream
             .queryName("alertas_localidad")
             .outputMode("update")
             .format("console")
             .option("truncate", False)
             .trigger(processingTime="10 seconds")
             .start())

print(f"Escuchando el topic '{TOPIC}'... (Ctrl+C para detener)")
print(f"La gráfica se actualiza en: {GRAFICA}")
print("Interfaz web de Spark: http://<IP-de-la-maquina-virtual>:4040")
spark.streams.awaitAnyTermination()
