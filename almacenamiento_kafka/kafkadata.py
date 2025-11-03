"""
Consumidor de Kafka que persiste los resultados validados en la base de datos.

Este servicio:
1. Consume mensajes del topic 'resultados_validados'
2. Envía los datos al servicio de almacenamiento vía HTTP POST
3. Maneja errores de conexión y reintenta si es necesario
"""

import os
import time
import json
import logging
import requests
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("StorageConsumer")

# Variables de entorno
KAFKA_BROKER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
INPUT_TOPIC = os.environ.get('KAFKA_INPUT_TOPIC', 'resultados_validados')
STORAGE_URL = os.environ.get('STORAGE_SERVICE_URL', 'http://almacenamiento:5005/save')


def connect_to_kafka():
    """Intenta conectarse a Kafka como Consumidor con reintentos."""
    logger.info(f"Intentando conectar a Kafka en {KAFKA_BROKER}...")
    consumer = None
    retries = 10
    
    while retries > 0:
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id='storage-consumer-group',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            logger.info(f"Conexión con Kafka exitosa. Escuchando topic: {INPUT_TOPIC}")
            return consumer
        except NoBrokersAvailable:
            logger.warning(f"No se pudo conectar a Kafka. Reintentando en 10 segundos... ({retries} intentos restantes)")
            retries -= 1
            time.sleep(10)
    
    logger.error("No se pudo establecer conexión con Kafka. Saliendo.")
    exit(1)


def save_to_storage(data):
    """
    Envía los datos al servicio de almacenamiento.
    
    Args:
        data: Diccionario con los datos a guardar
        
    Returns:
        bool: True si se guardó exitosamente, False en caso contrario
    """
    try:
        # Preparar el payload según el formato esperado por el almacenamiento
        payload = {
            'question_text': data.get('pregunta', ''),
            'original_answer': data.get('respuesta_original', ''),
            'llm_answer': data.get('respuesta_llm', ''),
            'quality_score': data.get('score_rouge_l', 0.0)
        }
        
        # Agregar información adicional si existe
        if data.get('status') == 'validated_max_retries':
            logger.warning(f"Guardando respuesta que alcanzó el límite de reintentos")
        
        # Enviar POST request
        response = requests.post(
            STORAGE_URL,
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            logger.info(f"Datos guardados exitosamente en almacenamiento")
            return True
        else:
            logger.error(f"Error al guardar datos. Status code: {response.status_code}, Response: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Error de conexión con el servicio de almacenamiento: {e}")
        return False
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout al conectar con el servicio de almacenamiento: {e}")
        return False
    except Exception as e:
        logger.error(f"Error inesperado al guardar datos: {e}", exc_info=True)
        return False


def main():
    logger.info("=" * 60)
    logger.info("Iniciando Storage Consumer")
    logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"Input Topic: {INPUT_TOPIC}")
    logger.info(f"Storage URL: {STORAGE_URL}")
    logger.info("=" * 60)
    
    # Esperar un poco para que otros servicios estén listos
    logger.info("Esperando 25 segundos para que los servicios estén listos...")
    time.sleep(25)
    
    # Conectar a Kafka
    consumer = connect_to_kafka()
    
    logger.info("Storage Consumer listo. Esperando mensajes...")
    
    # Bucle principal de consumo
    try:
        for message in consumer:
            data = message.value
            pregunta = data.get('pregunta', 'N/A')
            score = data.get('score_rouge_l', 0.0)
            
            logger.info("-" * 60)
            logger.info(f"Mensaje recibido: '{pregunta[:50]}...' | Score: {score:.4f}")
            
            # Intentar guardar en la base de datos
            success = save_to_storage(data)
            
            if success:
                logger.info(f"Procesamiento completado exitosamente")
            else:
                logger.warning(f"No se pudo guardar el mensaje. Se continuará con el siguiente.")
            
    except KeyboardInterrupt:
        logger.info("Detención manual solicitada. Cerrando conexiones...")
    except Exception as e:
        logger.error(f"Error crítico en el bucle principal: {e}", exc_info=True)
    finally:
        consumer.close()
        logger.info("Storage Consumer detenido.")


if __name__ == "__main__":
    main()