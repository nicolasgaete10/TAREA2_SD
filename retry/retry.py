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
RETRY_DELAY_SECONDS = int(os.environ.get('RETRY_DELAY_SECONDS', 60))

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

def main():
    logger.info("=" * 60)
    logger.info("Iniciando Retry Worker (Cuota LLM)")
    logger.info(f"Input Topic: {INPUT_TOPIC}")
    logger.info(f"Output Topic: {OUTPUT_TOPIC}")
    logger.info(f"Retraso: {RETRY_DELAY_SECONDS} segundos")
    logger.info("=" * 60)
    
    consumer, producer = connect_kafka()

    try:
        for message in consumer:
            data = message.value
            pregunta = data.get('pregunta', 'N/A')
            
            logger.warning(f"Mensaje de reintento recibido para: '{pregunta[:50]}...'.")
            
            # --- LA LÓGICA DE RETRASO ---
            logger.info(f"Esperando {RETRY_DELAY_SECONDS} segundos antes de re-encolar...")
            time.sleep(RETRY_DELAY_SECONDS)
            
            # --- Re-enviar al tópico de preguntas pendientes ---
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