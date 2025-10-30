import os
import time
import json
import random
import logging
import redis 
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

import google.generativeai as genai 
from google.api_core.exceptions import ResourceExhausted, InternalServerError 
from google.generativeai.types import GenerationConfig, HarmCategory, HarmBlockThreshold 
from rouge_score import rouge_scorer

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("ProcesadorLLM") 

# --- Variables de Entorno y Constantes ---
try:
    KAFKA_BROKER = os.environ['KAFKA_BOOTSTRAP_SERVERS']
    INPUT_TOPIC = os.environ['KAFKA_INPUT_TOPIC']
    OUTPUT_TOPIC_EXITO = os.environ['KAFKA_OUTPUT_TOPIC']
    RETRY_TOPIC_CUOTA = os.environ['KAFKA_RETRY_TOPIC_CUOTA']
    DEAD_LETTER_TOPIC = os.environ['KAFKA_DEAD_LETTER_TOPIC']
    GEMINI_API_KEY = os.environ['GEMINI_API_KEY']
    REDIS_URL = os.environ.get('REDIS_URL', 'redis://redis:6379')
except KeyError as e:
    logger.error(f"Error: Variable de entorno no definida: {e}. Saliendo.")
    exit(1)


MOCK_RESPONSE = "Respuesta MOCK: No se pudo procesar la solicitud."


class RateLimitError(Exception):
    """Excepción para errores 429 (Cuota)."""
    pass

class ServerError(Exception):
    """Excepción para errores 5xx (Fallo del servidor)."""
    pass



def connect_to_kafka_consumer(broker, topic, group_id="procesadores_llm"):
    """Intenta conectarse a Kafka como Consumidor con reintentos."""
    logger.info(f"Intentando conectar a Kafka (Consumidor) en {broker}...") 
    retries = 10
    while retries > 0:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=broker,
                group_id=group_id,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            logger.info(f"Conexión con Kafka (Consumidor) exitosa. Escuchando topic: {topic}") # <-- Corregido a 'logger'
            return consumer
        except NoBrokersAvailable:
            logger.warning(f"No se pudo conectar a Kafka. Reintentando en 10s... ({retries} restantes)") # <-- Corregido a 'logger'
            retries -= 1
            time.sleep(10)
    logger.error("No se pudo establecer conexión con Kafka (Consumidor). Saliendo.") # <-- Corregido a 'logger'
    exit(1)

def connect_to_kafka_producer(broker):
    """Intenta conectarse a Kafka como Productor con reintentos."""
    logger.info(f"Intentando conectar a Kafka (Productor) en {broker}...") # <-- Corregido a 'logger'
    # ... (el resto de tu función está bien, pero usa 'logger')
    retries = 10
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Conexión con Kafka (Productor) exitosa.") # <-- Corregido a 'logger'
            return producer
        except NoBrokersAvailable:
            logger.warning(f"No se pudo conectar a Kafka. Reintentando en 10s... ({retries} restantes)") # <-- Corregido a 'logger'
            retries -= 1
            time.sleep(10)
    logger.error("No se pudo establecer conexión con Kafka (Productor). Saliendo.") # <-- Corregido a 'logger'
    exit(1)

def connect_to_redis(url):
    """Intenta conectarse a Redis con reintentos."""
    logger.info(f"Intentando conectar a Redis en {url}...") # <-- Corregido a 'logger'
    # ... (el resto de tu función está bien, pero usa 'logger')
    retries = 10
    while retries > 0:
        try:
            redis_client = redis.from_url(url, decode_responses=True)
            redis_client.ping()
            logger.info("Conexión con Redis exitosa.") # <-- Corregido a 'logger'
            return redis_client
        except redis.exceptions.ConnectionError:
            logger.warning(f"No se pudo conectar a Redis. Reintentando en 10s... ({retries} restantes)") # <-- Corregido a 'logger'
            retries -= 1
            time.sleep(10)
    logger.error("No se pudo establecer conexión con Redis. Saliendo.") # <-- Corregido a 'logger'
    exit(1)

def configure_gemini(api_key):
    """Configura y devuelve el modelo Gemini."""
    if not api_key:
        logger.warning("GEMINI_API_KEY no encontrada. El servicio LLM no funcionará.") # <-- Corregido a 'logger'
        return None
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-1.5-flash') # Usando un modelo más nuevo
        logger.info("Modelo Gemini configurado exitosamente.") # <-- Corregido a 'logger'
        return model
    except Exception as e:
        logger.error(f"Error al configurar Gemini: {e}. El servicio no funcionará.") # <-- Corregido a 'logger'
        return None

# --- Lógica de Procesamiento (MODIFICADA) ---

def calculate_rouge_score(response_llm, response_original):
    """Calcula el score ROUGE-L entre dos respuestas."""
    try:
        scorer = rouge_scorer.RougeScorer(['rougeL'], use_stemmer=True)
        scores = scorer.score(response_original, response_llm)
        fmeasure = scores['rougeL'].fmeasure
        logger.info(f"Score ROUGE-L calculado: {fmeasure}") # <-- Corregido a 'logger'
        return fmeasure
    except Exception as e:
        logger.error(f"Error calculando ROUGE: {e}") # <-- Corregido a 'logger'
        return 0.0

def get_llm_response(model, pregunta):
    """
    Obtiene respuesta del LLM.
    LANZA RateLimitError o ServerError si la API falla.
    """
    if not model:
        logger.warning("No hay modelo LLM. Lanzando ServerError.")
        raise ServerError("Modelo Gemini no está configurado (probablemente falta API_KEY)")

    safety_settings = {
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    }
    
    config = GenerationConfig(temperature=0.7, max_output_tokens=250)

    try:
        response = model.generate_content(
            pregunta, 
            generation_config=config,
            safety_settings=safety_settings
        )
        
        if not response.parts:
            logger.warning(f"Respuesta de Gemini bloqueada o vacía (a pesar de los filtros) para: {pregunta[:50]}...")
            return MOCK_RESPONSE 
            
        return response.text
    
    except ResourceExhausted as e:
        # Ya no devolvemos MOCK. Lanzamos una excepción que el main() capturará.
        logger.error(f"Error 429 - Rate Limit Exceeded: {e}.")
        raise RateLimitError(str(e))
        
    except (InternalServerError, Exception) as e:
        # Error grave de la API (5xx) o error inesperado.
        logger.error(f"Error inesperado/Servidor (5xx) de API Gemini: {e}.")
        raise ServerError(str(e))

# --- Bucle Principal del Consumidor (MODIFICADO) ---

def main():
    logger.info("Iniciando Servicio Procesador LLM...") # <-- Corregido a 'logger'
    
    consumer = connect_to_kafka_consumer(KAFKA_BROKER, INPUT_TOPIC)
    producer = connect_to_kafka_producer(KAFKA_BROKER)
    redis_client = connect_to_redis(REDIS_URL)
    model_gemini = configure_gemini(GEMINI_API_KEY)

    logger.info("=== Servicio listo y esperando mensajes ===") # <-- Corregido a 'logger'

    try:
        for message in consumer:
            data = message.value
            pregunta = data['pregunta']
            
            # (Tu lógica de reintentos de Flink está bien)
            retry_count = data.get('retry_count', 0)
            if retry_count > 0:
                logger.info(f"REINTENTO Flink #{retry_count} - Mensaje recibido: {pregunta[:50]}...")
            else:
                logger.info(f"Mensaje nuevo recibido: {pregunta[:50]}...")

            try:
                # 1. Revisar Cache (Redis)
                cache_status = "MISS"
                cached_response = redis_client.get(pregunta)
                if cached_response:
                    response_llm = cached_response
                    cache_status = "HIT"
                    logger.info("Cache HIT.")
                else:
                    # 2. Si es Cache Miss, consultar LLM
                    logger.info("Cache MISS. Consultando LLM...")
                    response_llm = get_llm_response(model_gemini, pregunta)
                    
                    # 3. Guardar en Cache si la respuesta es válida
                    if response_llm != MOCK_RESPONSE:
                        redis_client.set(pregunta, response_llm, ex=3600) # Expira en 1 hora
                        logger.info("Respuesta guardada en caché.")

                # 4. Calcular Score (Solo si tuvimos éxito)
                score = calculate_rouge_score(response_llm, data['respuesta_original'])

                # 5. Preparar mensaje de ÉXITO
                output_data = {
                    **data, # Hereda todos los datos (incluyendo retry_count)
                    'respuesta_llm': response_llm,
                    'score_rouge_l': score,
                    'cache_status': cache_status,
                    'status': 'processed_success',
                    'timestamp_procesado': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                }
                
                # 6. ENVIAR A TÓPICO DE ÉXITO (para Flink)
                producer.send(OUTPUT_TOPIC_EXITO, value=output_data)
                logger.info(f"Resultado enviado a '{OUTPUT_TOPIC_EXITO}'. Score: {score:.4f}, Cache: {cache_status}")

            except RateLimitError as e:
                # 7. ENVIAR A TÓPICO DE REINTENTO (para retry_worker)
                logger.warning(f"Error 429 (Rate Limit). Enviando a '{RETRY_TOPIC_CUOTA}' para reintento de servicio.")
                data['service_retry_count'] = data.get('service_retry_count', 0) + 1
                producer.send(RETRY_TOPIC_CUOTA, value=data)
            
            except ServerError as e:
                # 8. ENVIAR A TÓPICO DE ERROR (Dead-Letter)
                logger.error(f"Error grave de servidor. Enviando a '{DEAD_LETTER_TOPIC}'. Error: {e}")
                data['error_detalle'] = str(e)
                producer.send(DEAD_LETTER_TOPIC, value=data)
            
            finally:
                producer.flush() 
            

    except KeyboardInterrupt:
        logger.info("Detención manual solicitada. Cerrando conexiones...") 
    except Exception as e:
        logger.error(f"Error crítico en el bucle principal: {e}", exc_info=True)
    finally:
        consumer.close()
        producer.close()
        redis_client.close()
        logger.info("Servicio Procesador LLM detenido.")

if __name__ == "__main__":
    main()