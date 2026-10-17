"""
Tarea 3 - Big Data (UNAD) - Productor de Kafka
Simula la llegada en tiempo real de reportes de siniestros viales en Bogotá
y los envía al topic "siniestros_viales".

Ejecución (en la máquina virtual, con ZooKeeper y Kafka iniciados):
    python3 kafka_producer_siniestros.py
"""

import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

TOPIC = "siniestros_viales"

# Pesos aproximados para que la simulación se parezca a la realidad de Bogotá:
# las localidades más grandes y con más tráfico reportan más siniestros.
LOCALIDADES = {
    "KENNEDY": 12, "ENGATIVA": 10, "SUBA": 10, "FONTIBON": 7, "PUENTE ARANDA": 8,
    "USAQUEN": 7, "CHAPINERO": 6, "TEUSAQUILLO": 6, "BARRIOS UNIDOS": 6,
    "BOSA": 5, "CIUDAD BOLIVAR": 4, "LOS MARTIRES": 4, "SANTA FE": 3,
    "SAN CRISTOBAL": 3, "RAFAEL URIBE URIBE": 3, "TUNJUELITO": 2, "USME": 2,
    "ANTONIO NARINO": 1, "LA CANDELARIA": 1,
}
CLASES = {"CHOQUE": 70, "ATROPELLO": 12, "CAIDA DE OCUPANTE": 8,
          "VOLCAMIENTO": 3, "AUTOLESION": 2, "OTRO": 5}
GRAVEDADES = {"SOLO DANOS": 60, "CON HERIDOS": 37, "CON MUERTOS": 3}

# Centro aproximado de cada localidad (para generar coordenadas cercanas)
CENTROS = {
    "KENNEDY": (4.628, -74.153), "ENGATIVA": (4.705, -74.111), "SUBA": (4.741, -74.084),
    "FONTIBON": (4.678, -74.143), "PUENTE ARANDA": (4.614, -74.112),
    "USAQUEN": (4.718, -74.032), "CHAPINERO": (4.646, -74.062),
    "TEUSAQUILLO": (4.638, -74.087), "BARRIOS UNIDOS": (4.667, -74.073),
    "BOSA": (4.617, -74.190), "CIUDAD BOLIVAR": (4.508, -74.156),
    "LOS MARTIRES": (4.605, -74.088), "SANTA FE": (4.596, -74.070),
    "SAN CRISTOBAL": (4.565, -74.087), "RAFAEL URIBE URIBE": (4.576, -74.118),
    "TUNJUELITO": (4.578, -74.135), "USME": (4.475, -74.120),
    "ANTONIO NARINO": (4.589, -74.099), "LA CANDELARIA": (4.597, -74.074),
}


def elegir(opciones):
    return random.choices(list(opciones), weights=list(opciones.values()), k=1)[0]


def generar_siniestro(contador):
    localidad = elegir(LOCALIDADES)
    clase = elegir(CLASES)
    gravedad = elegir(GRAVEDADES)
    # Los atropellos tienen más probabilidad de dejar víctimas
    if clase == "ATROPELLO" and gravedad == "SOLO DANOS":
        gravedad = "CON HERIDOS"
    lat, lon = CENTROS[localidad]
    return {
        "codigo_accidente": f"SIM-{contador:07d}",
        "localidad": localidad,
        "clase_acc": clase,
        "gravedad": gravedad,
        "latitud": round(lat + random.uniform(-0.01, 0.01), 6),
        "longitud": round(lon + random.uniform(-0.01, 0.01), 6),
        "timestamp": int(time.time()),
    }


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

print(f"Enviando siniestros simulados al topic '{TOPIC}'... (Ctrl+C para detener)")
contador = 1
try:
    while True:
        evento = generar_siniestro(contador)
        producer.send(TOPIC, value=evento)
        hora = datetime.now().strftime("%H:%M:%S")
        print(f"[{hora}] Enviado #{contador}: {evento['localidad']:<20} "
              f"{evento['clase_acc']:<18} {evento['gravedad']}")
        contador += 1
        time.sleep(random.uniform(0.3, 1.5))   # ritmo irregular, como en la realidad
except KeyboardInterrupt:
    print("\nProductor detenido.")
finally:
    producer.flush()
    producer.close()
