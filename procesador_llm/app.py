import os
import time
import json
import logging
import redis
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Importaciones de Lógica de Tarea 1 (Score Service) ---
import google.generativeai as genai
from google.generativeai.types.generation_types import GenerationConfig
from google.generativeai.types import HarmCategory, HarmBlockThreshold
from google.api_core.exceptions import ResourceExhausted
from rouge_score import rouge_scorer

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger("ProcesadorLLM")

KAFKA_BROKER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
INPUT_TOPIC = os.environ.get('KAFKA_INPUT_TOPIC', 'preguntas_pendientes')
OUTPUT_TOPIC = os.environ.get('KAFKA_OUTPUT_TOPIC', 'resultados_procesados')
REDIS_URL = os.environ.get('REDIS_URL', 'redis://redis_cache:6379')
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')

MOCK_RESPONSE = "This is a mock response due to API failure."


def connect_to_kafka_consumer(broker, topic, group_id="procesadores_llm"):
    """Intenta conectarse a Kafka como Consumidor con reintentos."""
    log.info(f"Intentando conectar a Kafka (Consumidor) en {broker}...")
    consumer = None
    retries = 10
    while retries > 0:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=broker,
                group_id=group_id,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest', # Empezar a leer desde el principio si el grupo es nuevo
                enable_auto_commit=True # Auto-commit de offsets
            )
            log.info(f"Conexión con Kafka (Consumidor) exitosa. Escuchando topic: {topic}")
            return consumer
        except NoBrokersAvailable:
            log.warning(f"No se pudo conectar a Kafka. Reintentando en 10 segundos... ({retries} intentos restantes)")
            retries -= 1
            time.sleep(10)
    
    log.error("No se pudo establecer conexión con Kafka (Consumidor). Saliendo.")
    exit(1)

def connect_to_kafka_producer(broker):
    """Intenta conectarse a Kafka como Productor con reintentos."""
    log.info(f"Intentando conectar a Kafka (Productor) en {broker}...")
    producer = None
    retries = 10
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            log.info("Conexión con Kafka (Productor) exitosa.")
            return producer
        except NoBrokersAvailable:
            log.warning(f"No se pudo conectar a Kafka. Reintentando en 10 segundos... ({retries} intentos restantes)")
            retries -= 1
            time.sleep(10)
    
    log.error("No se pudo establecer conexión con Kafka (Productor). Saliendo.")
    exit(1)

def connect_to_redis(url):
    """Intenta conectarse a Redis con reintentos."""
    log.info(f"Intentando conectar a Redis en {url}...")
    redis_client = None
    retries = 10
    while retries > 0:
        try:
            redis_client = redis.from_url(url, decode_responses=True)
            redis_client.ping()
            log.info("Conexión con Redis exitosa.")
            return redis_client
        except redis.exceptions.ConnectionError:
            log.warning(f"No se pudo conectar a Redis. Reintentando en 10 segundos... ({retries} intentos restantes)")
            retries -= 1
            time.sleep(10)

    log.error("No se pudo establecer conexión con Redis. Saliendo.")
    exit(1)

def configure_gemini(api_key):
    """Configura y devuelve el modelo Gemini."""
    if not api_key:
        log.warning("GEMINI_API_KEY no encontrada. El servicio LLM usará MOCK_RESPONSE.")
        return None
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.5-flash')
        log.info("Modelo Gemini configurado exitosamente.")
        return model
    except Exception as e:
        log.error(f"Error al configurar Gemini: {e}. El servicio usará MOCK_RESPONSE.")
        return None

# --- Lógica de Procesamiento (Reutilizada de Tarea 1) ---

def calculate_rouge_score(response_llm, response_original):
    """Calcula el score ROUGE-L entre dos respuestas."""
    try:
        scorer = rouge_scorer.RougeScorer(['rougeL'], use_stemmer=True)
        scores = scorer.score(response_original, response_llm)
        fmeasure = scores['rougeL'].fmeasure
        log.info(f"Score ROUGE-L calculado: {fmeasure}")
        return fmeasure
    except Exception as e:
        log.error(f"Error calculando ROUGE: {e}")
        return 0.0

def get_llm_response(model, pregunta):
    """Obtiene respuesta del LLM manejando errores de rate limit."""
    if not model:
        log.warning("No hay modelo LLM. Usando respuesta MOCK.")
        return MOCK_RESPONSE

    # --- INICIO DE MODIFICACIÓN ---
    # Definir configuraciones de seguridad para ser más permisivos
    # Esto evita que el filtro de Google bloquee la mayoría de las preguntas
    safety_settings = {
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    }
    # --- FIN DE MODIFICACIÓN ---

    try:
        config = GenerationConfig(temperature=0.7, max_output_tokens=250)
        
        # --- MODIFICADO: Añadido 'safety_settings' a la llamada ---
        response = model.generate_content(
            pregunta, 
            generation_config=config,
            safety_settings=safety_settings
        )
        
        # Manejar caso de respuesta vacía o bloqueada
        # (Aunque los filtros están bajos, aún puede ser bloqueada si la respuesta es realmente vacía)
        if not response.parts:
            log.warning(f"Respuesta de Gemini bloqueada o vacía (a pesar de los filtros) para: {pregunta[:50]}...")
            return MOCK_RESPONSE
            
        return response.text
    
    except ResourceExhausted as e:
        log.error(f"Error 429 - Rate Limit Exceeded: {e}. Usando respuesta MOCK.")
        return MOCK_RESPONSE
    except Exception as e:
        log.error(f"Error inesperado de API Gemini: {e}. Usando respuesta MOCK.")
        return MOCK_RESPONSE

# --- Bucle Principal del Consumidor ---

def main():
    log.info("Iniciando Servicio Procesador LLM...")
    
    # 1. Establecer conexiones
    consumer = connect_to_kafka_consumer(KAFKA_BROKER, INPUT_TOPIC)
    producer = connect_to_kafka_producer(KAFKA_BROKER)
    redis_client = connect_to_redis(REDIS_URL)
    model_gemini = configure_gemini(GEMINI_API_KEY)

    log.info("=== Servicio listo y esperando mensajes ===")

    # 2. Bucle infinito de consumo
    try:
        for message in consumer:
            data = message.value
            pregunta = data['pregunta']
            respuesta_original = data['respuesta_original']
            log.info(f"Mensaje recibido: {pregunta[:50]}...")

            response_llm = None
            cache_status = "MISS"

            # 3. Revisar Cache (Redis)
            try:
                cached_response = redis_client.get(pregunta)
                if cached_response:
                    response_llm = cached_response
                    cache_status = "HIT"
                    log.info("Cache HIT.")
            except redis.exceptions.RedisError as e:
                log.error(f"Error al consultar Redis: {e}. Tratando como Cache Miss.")
                
            # 4. Si es Cache Miss, consultar LLM
            if cache_status == "MISS":
                log.info("Cache MISS. Consultando LLM...")
                response_llm = get_llm_response(model_gemini, pregunta)
                
                # 5. Guardar en Cache si la respuesta es válida
                if response_llm != MOCK_RESPONSE:
                    try:
                        # La política de evicción (LRU) y tamaño (256mb)
                        # se configuran en el docker-compose.
                        redis_client.set(pregunta, response_llm)
                        log.info("Respuesta guardada en caché.")
                    except redis.exceptions.RedisError as e:
                        log.error(f"Error al guardar en Redis: {e}")

            # 6. Calcular Score
            score = calculate_rouge_score(response_llm, respuesta_original)

            # 7. Preparar mensaje de salida
            output_data = {
                'id_pregunta': data.get('id_pregunta', 'N/A'),
                'pregunta': pregunta,
                'respuesta_original': respuesta_original,
                'respuesta_llm': response_llm,
                'score_rouge_l': score,
                'cache_status': cache_status,
                'timestamp_procesado': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            }

            # 8. Producir mensaje de resultado
            producer.send(OUTPUT_TOPIC, value=output_data)
            log.info(f"Resultado enviado a '{OUTPUT_TOPIC}'. Score: {score:.4f}, Cache: {cache_status}")
            producer.flush() # Asegurar envío inmediato (opcional)

    except KeyboardInterrupt:
        log.info("Detención manual solicitada. Cerrando conexiones...")
    except Exception as e:
        log.error(f"Error crítico en el bucle principal: {e}", exc_info=True)
    finally:
        consumer.close()
        producer.close()
        redis_client.close()
        log.info("Servicio Procesador LLM detenido.")

if __name__ == "__main__":
    main()