"""
Job simple de Quality Checker usando solo Kafka (sin PyFlink).

Este enfoque es más simple y evita problemas de compilación de PyFlink.
Funciona exactamente igual, pero usa directamente kafka-python.
"""

import os
import json
import logging
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("QualityChecker")

# Variables de entorno
KAFKA_BROKER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
INPUT_TOPIC = os.environ.get('FLINK_INPUT_TOPIC', 'resultados_procesados')
OUTPUT_VALIDATED_TOPIC = os.environ.get('FLINK_OUTPUT_VALIDATED_TOPIC', 'resultados_validados')
OUTPUT_RETRY_TOPIC = os.environ.get('FLINK_OUTPUT_RETRY_TOPIC', 'preguntas_pendientes')

# Umbral de calidad y límite de reintentos
QUALITY_THRESHOLD = float(os.environ.get('QUALITY_THRESHOLD', '0.3'))
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '2'))


def connect_to_kafka_consumer():
    """Conecta a Kafka como consumidor con reintentos."""
    logger.info(f"Conectando a Kafka (Consumer) en {KAFKA_BROKER}...")
    retries = 10
    
    while retries > 0:
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id='quality-checker-group',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            logger.info(f"Conexión exitosa. Escuchando: {INPUT_TOPIC}")
            return consumer
        except NoBrokersAvailable:
            logger.warning(f" Kafka no disponible. Reintentando... ({retries} intentos)")
            retries -= 1
            time.sleep(10)
    
    logger.error("No se pudo conectar a Kafka. Saliendo.")
    exit(1)


def connect_to_kafka_producer():
    """Conecta a Kafka como productor con reintentos."""
    logger.info(f"Conectando a Kafka (Producer) en {KAFKA_BROKER}...")
    retries = 10
    
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Productor conectado")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka no disponible. Reintentando... ({retries} intentos)")
            retries -= 1
            time.sleep(10)
    
    logger.error("No se pudo conectar a Kafka. Saliendo.")
    exit(1)


def evaluate_quality(data):
    """
    Evalúa la calidad de una respuesta y decide qué hacer.
    
    Returns:
        tuple: (topic_destino, mensaje)
    """
    try:
        score = data.get('score_rouge_l', 0.0)
        retry_count = data.get('retry_count', 0)
        pregunta = data.get('pregunta', 'N/A')
        
        logger.info(f"Evaluando: '{pregunta[:50]}...' | Score: {score:.4f} | Reintentos: {retry_count}")
        
        # ¿Score suficientemente bueno?
        if score >= QUALITY_THRESHOLD:
            logger.info(f" APROBADO - Score {score:.4f} >= {QUALITY_THRESHOLD}")
            data['status'] = 'validated'
            return (OUTPUT_VALIDATED_TOPIC, data)
        
        # Score bajo
        if retry_count < MAX_RETRIES:
            logger.warning(f"RECHAZADO - Score {score:.4f} < {QUALITY_THRESHOLD}. Reintento {retry_count + 1}/{MAX_RETRIES}")
            
            retry_message = {
                'id_pregunta': data.get('id_pregunta', 'N/A'),
                'pregunta': pregunta,
                'respuesta_original': data.get('respuesta_original', ''),
                'retry_count': retry_count + 1,
                'previous_score': score,
                'reason': 'low_quality_score'
            }
            return (OUTPUT_RETRY_TOPIC, retry_message)
        
        # Se agotaron los reintentos
        logger.warning(f" LÍMITE ALCANZADO - Score {score:.4f}. Guardando de todas formas.")
        data['status'] = 'validated_max_retries'
        data['quality_warning'] = True
        return (OUTPUT_VALIDATED_TOPIC, data)
        
    except Exception as e:
        logger.error(f"❌ Error evaluando calidad: {e}", exc_info=True)
        return (None, None)


def main():
    """Función principal del quality checker."""
    logger.info("=" * 60)
    logger.info("Iniciando Quality Checker Job")
    logger.info(f"Umbral de calidad: {QUALITY_THRESHOLD}")
    logger.info(f"Máximo de reintentos: {MAX_RETRIES}")
    logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    logger.info("=" * 60)
    
    # Esperar a que Kafka esté listo
    logger.info("Esperando 30 segundos para que Kafka esté listo...")
    time.sleep(30)
    
    # Conectar a Kafka
    consumer = connect_to_kafka_consumer()
    producer = connect_to_kafka_producer()
    
    logger.info("Quality Checker listo. Esperando mensajes...")
    
    # Bucle principal
    try:
        for message in consumer:
            data = message.value
            
            # Evaluar calidad
            topic, processed_data = evaluate_quality(data)
            
            if topic and processed_data:
                # Enviar al topic correspondiente
                producer.send(topic, processed_data)
                producer.flush()
                logger.info(f"Mensaje enviado a '{topic}'")
            
    except KeyboardInterrupt:
        logger.info(" Detención manual")
    except Exception as e:
        logger.error(f"Error crítico: {e}", exc_info=True)
    finally:
        consumer.close()
        producer.close()
        logger.info("Quality Checker detenido")


if __name__ == "__main__":
    main()