from flask import Flask, request, jsonify
import redis
import requests
import os
import logging
from collections import deque

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
app = Flask(__name__)

MAX_CACHE_SIZE = 100
CACHED_KEYS = deque(maxlen=MAX_CACHE_SIZE)

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis_cache:6379")
SCORE_SERVICE_URL = os.environ.get("SCORE_SERVICE_URL", "http://score_service:5001/process")
STORAGE_SERVICE_URL = os.environ.get("STORAGE_SERVICE_URL", "http://almacenamiento:5005")

try:
    cache = redis.from_url(REDIS_URL, decode_responses=True)
    cache.ping()
    app.logger.info("Conexión a Redis exitosa.")
    existing_keys = cache.keys('*')
    for key in existing_keys:
        if len(CACHED_KEYS) < MAX_CACHE_SIZE:
            CACHED_KEYS.append(key)
    app.logger.info(f"Registro de caché inicializado con {len(CACHED_KEYS)} claves existentes.")
except redis.exceptions.ConnectionError as e:
    app.logger.error(f"FATAL: No se pudo conectar a Redis en {REDIS_URL}. Error: {e}")
    cache = None

def log_cache_access(question: str, cache_hit: bool, distribution: str):
    """Registra el acceso al cache en PostgreSQL"""
    try:
        data = {
            "question_text": question,
            "cache_hit": cache_hit,
            "cache_policy": "FIFO",
            "traffic_distribution": distribution
        }
        
        requests.post(
            f"{STORAGE_SERVICE_URL}/log_cache_access",
            json=data,
            timeout=5
        )
        app.logger.info(f"Log registrado: {distribution} - {'HIT' if cache_hit else 'MISS'}")
    except Exception as e:
        app.logger.error(f"Error registrando acceso cache: {e}")

@app.route('/check', methods=['POST'])
def check_cache():
    if not cache:
        return jsonify({"error": "Servicio de caché no disponible"}), 503

    data = request.json
    question = data.get('question')
    correct_answer = data.get('correct_answer')
    distribution = data.get('distribution', 'UNKNOWN')  # NUEVO: recibir distribución

    if not question:
        return jsonify({"error": "La solicitud JSON debe incluir una 'pregunta'"}), 400

    try:
        cached_response = cache.get(question)

        if cached_response:
            app.logger.info(f"Cache HIT para la pregunta: '{question[:50]}...'")
            # REGISTRAR HIT
            log_cache_access(question, True, distribution)
            return jsonify({
                "source": "cache",
                "question": question,
                "answer": cached_response
            })
        else:
            app.logger.info(f"Cache MISS para la pregunta: '{question[:50]}...'")
            # REGISTRAR MISS
            log_cache_access(question, False, distribution)
            
            response = requests.post(
                SCORE_SERVICE_URL, 
                json={
                    'question': question,
                    'correct_answer': correct_answer  
                }, 
                timeout=30
            )
            response.raise_for_status()
            
            score_result = response.json()
            llm_answer = score_result.get('answer')

            if llm_answer:
                if len(CACHED_KEYS) >= MAX_CACHE_SIZE:
                    oldest_key = CACHED_KEYS.popleft()
                    cache.delete(oldest_key)
                    app.logger.info(f"Caché lleno. Eliminando la clave más antigua: '{oldest_key[:50]}...'")

                cache.set(question, llm_answer)
                CACHED_KEYS.append(question)
                app.logger.info(f"Respuesta para '{question[:50]}...' guardada en caché. Tamaño actual: {len(CACHED_KEYS)}.")
                
            return jsonify(score_result)

    except Exception as e:
        app.logger.error(f"Ocurrió un error inesperado: {e}")
        return jsonify({"error": "Error interno inesperado en el servidor de caché"}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint para verificar estado del servicio"""
    try:
        if cache:
            cache.ping()
            redis_status = "conectado"
            cache_size = len(CACHED_KEYS)
        else:
            redis_status = "desconectado"
            cache_size = 0
            
        return jsonify({
            "status": "ok", 
            "redis_status": redis_status,
            "cache_size": cache_size,
            "max_cache_size": MAX_CACHE_SIZE,
            "policy": "FIFO"
        })
    except Exception as e:
        return jsonify({"status": "error", "redis_status": "error", "error": str(e)}), 500

if __name__ == '__main__':
    app.logger.info(f"Iniciando Cache Service con política FIFO (tamaño máximo: {MAX_CACHE_SIZE})")
    app.run(host='0.0.0.0', port=5000)