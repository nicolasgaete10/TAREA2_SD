import os
import time
import pandas as pd
import numpy as np
import random
import logging
import json
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger("TrafficGeneratorKafka")

# --- Variables de Entorno ---
KAFKA_BROKER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'preguntas_pendientes')
DATASET_PATH = os.environ.get('DATASET_PATH', '/app/Data/test.csv')
TRAFFIC_DISTRIBUTION = os.environ.get('TRAFFIC_DISTRIBUTION', 'uniform')
SIMULATION_TIME_MIN = int(os.environ.get('SIMULATION_TIME_MIN', 10))
STORAGE_CHECK_URL = os.environ.get('STORAGE_CHECK_URL', 'http://almacenamiento:5005/check')

# --- Almacén de datos ---
dataset = []
stats = {
    'cache_hits': 0,
    'cache_misses': 0,
    'total_requests': 0
}

def load_dataset(path, sample_size=10000):
    """Carga el dataset desde el CSV."""
    global dataset
    try:
        df = pd.read_csv(path, header=None)
        df_cleaned = df[[1, 3]].dropna()
        df_cleaned.columns = ['pregunta', 'respuesta']
        
        if len(df_cleaned) > sample_size:
            log.info(f"Tomando muestra de {sample_size} registros del dataset.")
            dataset = list(df_cleaned.sample(n=sample_size).itertuples(index=False, name=None))
        else:
            log.info(f"Usando dataset completo con {len(df_cleaned)} registros.")
            dataset = list(df_cleaned.itertuples(index=False, name=None))
            
        log.info(f"Dataset cargado con {len(dataset)} pares pregunta-respuesta.")
        
    except FileNotFoundError:
        log.error(f"Error: No se encontró el archivo dataset en {path}")
        exit(1)
    except Exception as e:
        log.error(f"Error al cargar el dataset: {e}")
        exit(1)

def get_random_question():
    """Obtiene una tupla (pregunta, respuesta) aleatoria del dataset."""
    return random.choice(dataset)

def connect_to_kafka():
    """Intenta conectarse a Kafka como Productor con reintentos."""
    log.info(f"Intentando conectar a Kafka en {KAFKA_BROKER}...")
    retries = 10
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            log.info("✅ Conexión con Kafka exitosa.")
            return producer
        except NoBrokersAvailable:
            log.warning(f"⚠️ No se pudo conectar a Kafka. Reintentando en 10 segundos... ({retries} intentos restantes)")
            retries -= 1
            time.sleep(10)
    
    log.error("❌ No se pudo establecer conexión con Kafka. Saliendo.")
    exit(1)

def check_storage(pregunta):
    """
    NUEVO: Consulta el almacenamiento para ver si la pregunta ya existe.
    
    Returns:
        dict o None: Datos si existe, None si no existe
    """
    try:
        # Hacer POST al endpoint de check (necesitarás crear este endpoint)
        response = requests.post(
            STORAGE_CHECK_URL,
            json={'question_text': pregunta},
            timeout=5
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('exists'):
                log.info(f"✅ CACHE HIT en BD: Pregunta ya procesada")
                stats['cache_hits'] += 1
                return data.get('data')
        
        log.info(f"❌ CACHE MISS en BD: Pregunta no encontrada")
        stats['cache_misses'] += 1
        return None
        
    except Exception as e:
        log.warning(f"⚠️ Error consultando almacenamiento: {e}. Asumiendo no existe.")
        stats['cache_misses'] += 1
        return None

def process_question(producer, question, best_answer):
    """
    MODIFICADO: Procesa una pregunta según el flujo del PDF.
    
    1. Consulta almacenamiento
    2. Si existe → Usar esa respuesta (opcionalmente popular caché Redis)
    3. Si NO existe → Enviar a Kafka
    """
    stats['total_requests'] += 1
    
    # Paso 1 y 2: Consultar almacenamiento
    existing_data = check_storage(question)
    
    if existing_data:
        # La pregunta ya fue procesada, usar resultado existente
        log.info(f"📊 Usando respuesta existente (Score: {existing_data.get('quality_score', 'N/A')})")
        # Aquí podrías popular Redis si quisieras
        return
    
    # Paso 3: No existe, enviar a Kafka para procesamiento
    message = {
        'id_pregunta': str(random.randint(10000, 99999)),
        'pregunta': question,
        'respuesta_original': best_answer
    }
    
    try:
        producer.send(KAFKA_TOPIC, message)
        log.info(f"📤 Mensaje enviado a '{KAFKA_TOPIC}': {question[:50]}...")
    except Exception as e:
        log.error(f"❌ Error al enviar mensaje a Kafka: {e}")

def generate_uniform_traffic(producer, duration_sec):
    """Genera tráfico con tasa de arribo constante."""
    log.info("🚀 Iniciando generación de tráfico UNIFORME (1 consulta cada 7 segundos).")
    start_time = time.time()
    
    while time.time() - start_time < duration_sec:
        question, best_answer = get_random_question()
        process_question(producer, question, best_answer)
        time.sleep(7)
    
    # Mostrar estadísticas finales
    log_stats()

def generate_exponential_traffic(producer, duration_sec):
    """Genera tráfico con tasa de arribo variable (exponencial)."""
    log.info("🚀 Iniciando generación de tráfico EXPONENCIAL (tasa media 7 seg).")
    start_time = time.time()
    
    while time.time() - start_time < duration_sec:
        question, best_answer = get_random_question()
        process_question(producer, question, best_answer)
        
        wait_time = np.random.exponential(scale=7.0)
        time.sleep(wait_time)
    
    # Mostrar estadísticas finales
    log_stats()

def log_stats():
    """Muestra estadísticas de caché."""
    log.info("=" * 60)
    log.info("📊 ESTADÍSTICAS FINALES")
    log.info(f"Total de solicitudes: {stats['total_requests']}")
    log.info(f"Cache Hits (BD): {stats['cache_hits']}")
    log.info(f"Cache Misses (BD): {stats['cache_misses']}")
    
    if stats['total_requests'] > 0:
        hit_rate = (stats['cache_hits'] / stats['total_requests']) * 100
        log.info(f"Hit Rate: {hit_rate:.2f}%")
    log.info("=" * 60)

def main():
    log.info("Iniciando Generador de Tráfico (Productor Kafka)...")
    
    # Esperar a que los servicios estén listos
    log.info("Esperando 30 segundos para que los servicios se inicien...")
    time.sleep(30)

    # Conectar a Kafka
    producer = connect_to_kafka()

    # Cargar Dataset
    load_dataset(DATASET_PATH)
    if not dataset:
        log.error("No se pudo cargar el dataset. Terminando.")
        return

    # Iniciar simulación
    duration_sec = SIMULATION_TIME_MIN * 60
    log.info(f"🎯 Iniciando simulación por {SIMULATION_TIME_MIN} minutos.")
    
    try:
        if TRAFFIC_DISTRIBUTION == "uniform":
            generate_uniform_traffic(producer, duration_sec)
        elif TRAFFIC_DISTRIBUTION == "exponential":
            generate_exponential_traffic(producer, duration_sec)
        else:
            log.warning(f"⚠️ Distribución '{TRAFFIC_DISTRIBUTION}' no reconocida. Usando 'uniforme'.")
            generate_uniform_traffic(producer, duration_sec)

    except KeyboardInterrupt:
        log.info("⚠️ Simulación detenida manualmente.")
        log_stats()
    finally:
        log.info("🏁 Simulación terminada. Cerrando productor de Kafka.")
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()