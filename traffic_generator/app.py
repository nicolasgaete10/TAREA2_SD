import os
import time
import pandas as pd
import numpy as np
import random
import logging
import json
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

# --- Almacén de datos (simple) ---
dataset = []

def load_dataset(path, sample_size=10000):
    """Carga el dataset desde el CSV."""
    global dataset
    try:
        df = pd.read_csv(path, header=None)
        # Usar las columnas 1 (pregunta) y 3 (respuesta)
        df_cleaned = df[[1, 3]].dropna()
        df_cleaned.columns = ['pregunta', 'respuesta']
        
        # Tomar una muestra si el dataset es muy grande, o usarlo completo si es menor
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
    producer = None
    retries = 10
    while retries > 0:
        try:
            # Serializamos el valor del mensaje como JSON y lo codificamos a utf-8
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            log.info("Conexión con Kafka exitosa.")
            return producer
        except NoBrokersAvailable:
            log.warning(f"No se pudo conectar a Kafka. Reintentando en 10 segundos... ({retries} intentos restantes)")
            retries -= 1
            time.sleep(10)
    
    log.error("No se pudo establecer conexión con Kafka después de varios intentos. Saliendo.")
    exit(1)

def send_to_kafka(producer, question, best_answer):
    """Envía el mensaje al topic de Kafka."""
    message = {
        'id_pregunta': str(random.randint(10000, 99999)), # Añadimos un ID para trazabilidad
        'pregunta': question,
        'respuesta_original': best_answer
    }
    try:
        producer.send(KAFKA_TOPIC, message)
        log.info(f"Mensaje enviado a '{KAFKA_TOPIC}': {question[:50]}...")
    except Exception as e:
        log.error(f"Error al enviar mensaje a Kafka: {e}")

def generate_uniform_traffic(producer, duration_sec):
    """Genera tráfico con una tasa de arribo constante (distribución uniforme)."""
    # MODIFICADO: Cambiado de 2 segundos a 7 segundos para respetar la cuota
    log.info("Iniciando generación de tráfico UNIFORME (1 consulta cada 7 segundos - adaptado a cuota).")
    start_time = time.time()
    while time.time() - start_time < duration_sec:
        question, best_answer = get_random_question()
        send_to_kafka(producer, question, best_answer)
        time.sleep(7)  # Tasa constante (1 req / 7 seg < 10 req / 60 seg)

def generate_exponential_traffic(producer, duration_sec):
    """Genera tráfico con tasa de arribo variable (distribución exponencial)."""
    # MODIFICADO: Cambiado de 2 segundos a 7 segundos para respetar la cuota
    log.info("Iniciando generación de tráfico EXPONENCIAL (tasa media 7 seg - adaptado a cuota).")
    start_time = time.time()
    while time.time() - start_time < duration_sec:
        question, best_answer = get_random_question()
        send_to_kafka(producer, question, best_answer)
        
        # Tiempo de espera basado en distribución exponencial (media de 7 seg)
        wait_time = np.random.exponential(scale=7.0)
        time.sleep(wait_time)

def main():
    log.info("Iniciando Generador de Tráfico (Productor Kafka)...")
    
    # 1. Esperar un poco para que Kafka esté listo (opcional pero recomendado)
    log.info("Esperando 20 segundos para que Kafka y Zookeeper se inicien...")
    time.sleep(20)

    # 2. Conectar a Kafka
    producer = connect_to_kafka()

    # 3. Cargar Dataset
    load_dataset(DATASET_PATH)
    if not dataset:
        log.error("No se pudo cargar el dataset. Terminando.")
        return

    # 4. Iniciar simulación de tráfico
    duration_sec = SIMULATION_TIME_MIN * 60
    log.info(f"Iniciando simulación por {SIMULATION_TIME_MIN} minutos.")
    
    try:
        if TRAFFIC_DISTRIBUTION == "uniform":
            generate_uniform_traffic(producer, duration_sec)
        elif TRAFFIC_DISTRIBUTION == "exponential":
            generate_exponential_traffic(producer, duration_sec)
        else:
            log.warning(f"Distribución '{TRAFFIC_DISTRIBUTION}' no reconocida. Usando 'uniforme'.")
            generate_uniform_traffic(producer, duration_sec)

    except KeyboardInterrupt:
        log.info("Simulación detenida manualmente.")
    finally:
        log.info("Simulación terminada. Cerrando productor de Kafka.")
        producer.flush() # Asegura que todos los mensajes pendientes se envíen
        producer.close()

if __name__ == "__main__":
    main()