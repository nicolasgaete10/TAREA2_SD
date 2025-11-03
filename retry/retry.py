import os
import time
import json
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("RetryWorker")

# --- Configuración ---
KAFKA_BROKER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
INPUT_TOPIC = os.environ.get('KAFKA_INPUT_TOPIC', 'preguntas_reintento_cuota')
OUTPUT_TOPIC = os.environ.get('KAFKA_OUTPUT_TOPIC', 'preguntas_pendientes')
BASE_RETRY_DELAY = int(os.environ.get('BASE_RETRY_DELAY', 30))  # Base: 30 segundos
MAX_RETRY_DELAY = int(os.environ.get('MAX_RETRY_DELAY', 300))   # Máximo: 5 minutos

def connect_kafka():
    """Conecta a Kafka con reintentos."""
    retries = 10
    while retries > 0:
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id='retry-worker-group',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest'
            )
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info(f"Conectado a Kafka. Escuchando: {INPUT_TOPIC}, Escribiendo: {OUTPUT_TOPIC}")
            return consumer, producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka no disponible. Reintentando en 10s... ({retries} intentos)")
            retries -= 1
            time.sleep(10)
    logger.error("No se pudo conectar a Kafka. Saliendo.")
    exit(1)

def calculate_exponential_backoff(retry_count):
    """
    Calcula el tiempo de espera usando Exponential Backoff.
    
    Formula: min(BASE * (2 ^ retry_count), MAX_DELAY)
    
    Ejemplo con BASE=30:
    - Intento 1: 30 * 2^0 = 30 segundos
    - Intento 2: 30 * 2^1 = 60 segundos
    - Intento 3: 30 * 2^2 = 120 segundos
    - Intento 4: 30 * 2^3 = 240 segundos
    - Intento 5+: 300 segundos (máximo)
    """
    delay = BASE_RETRY_DELAY * (2 ** retry_count)
    return min(delay, MAX_RETRY_DELAY)

def main():
    logger.info("=" * 60)
    logger.info("Iniciando Retry Worker (CON Exponential Backoff)")
    logger.info(f"Input Topic: {INPUT_TOPIC}")
    logger.info(f"Output Topic: {OUTPUT_TOPIC}")
    logger.info(f"Base Delay: {BASE_RETRY_DELAY}s, Max Delay: {MAX_RETRY_DELAY}s")
    logger.info("=" * 60)
    
    consumer, producer = connect_kafka()

    try:
        for message in consumer:
            data = message.value
            pregunta = data.get('pregunta', 'N/A')
            service_retry_count = data.get('service_retry_count', 0)
            
            logger.warning(f"Mensaje de reintento recibido: '{pregunta[:50]}...'")
            logger.info(f"Intento de servicio #{service_retry_count}")
            
            # Exponential backoff
            delay = calculate_exponential_backoff(service_retry_count)
            logger.info(f"Esperando {delay} segundos antes de re-encolar (exponential backoff)...")
            time.sleep(delay)
            
            # Re-enviar al tópico de preguntas pendientes
            logger.info(f"Re-encolando pregunta en '{OUTPUT_TOPIC}'.")
            producer.send(OUTPUT_TOPIC, data)
            producer.flush()
            
    except KeyboardInterrupt:
        logger.info("Deteniendo retry worker...")
    finally:
        consumer.close()
        producer.close()
        logger.info("Retry Worker detenido.")

if __name__ == "__main__":
    main()