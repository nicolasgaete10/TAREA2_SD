from flask import Flask, request, jsonify
import redis
import requests
import os
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
app = Flask(__name__)

MAX_CACHE_SIZE = 100
LRU_SET_KEY = "cache_keys_lru_zset" 

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis_cache:6379")
SCORE_SERVICE_URL = os.environ.get("SCORE_SERVICE_URL", "http://score_service:5001/process")
STORAGE_SERVICE_URL = os.environ.get("STORAGE_SERVICE_URL", "http://almacenamiento:5005")

try:
    cache = redis.from_url(REDIS_URL, decode_responses=True)
    cache.ping()
    app.logger.info("Conexión a Redis exitosa.")
except redis.exceptions.ConnectionError as e:
    app.logger.error(f"FATAL: No se pudo conectar a Redis en {REDIS_URL}. Error: {e}")
    cache = None

def get_current_timestamp():
    """Retorna el timestamp actual en microsegundos para usar como score."""
    return time.time() * 1_000_000

def log_cache_access(question: str, cache_hit: bool, distribution: str):
    """Registra el acceso al cache en PostgreSQL"""
    try:
        data = {
            "question_text": question,
            "cache_hit": cache_hit,
            "cache_policy": "LRU",
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
    distribution = data.get('distribution', 'UNKNOWN')  # NUEVO: recibir distribución

    if not question:
        return jsonify({"error": "La solicitud JSON debe incluir una 'pregunta'"}), 400

    try:
        cached_response = cache.get(question)

        if cached_response:
            app.logger.info(f"Cache HIT para la pregunta: '{question[:50]}...'")
            # REGISTRAR HIT
            log_cache_access(question, True, distribution)
            
            cache.zadd(LRU_SET_KEY, {question: get_current_timestamp()})
            app.logger.info(f"Clave '{question[:50]}...' actualizada como la más reciente.")
            
            return jsonify({
                "source": "LRU_CACHE", 
                "question": question,
                "answer": cached_response
            })
        else:
            app.logger.info(f"Cache MISS para la pregunta: '{question[:50]}...'")
            # REGISTRAR MISS
            log_cache_access(question, False, distribution)
            
            response = requests.post(SCORE_SERVICE_URL, json={'question': question}, timeout=30)
            response.raise_for_status()
            
            score_result = response.json()
            llm_answer = score_result.get('answer')

            if llm_answer:
                current_size = cache.zcard(LRU_SET_KEY)
                if current_size >= MAX_CACHE_SIZE:
                    removed_items = cache.zpopmin(LRU_SET_KEY, 1)
                    if removed_items:
                        oldest_key, _ = removed_items[0]
                        cache.delete(oldest_key)
                        app.logger.info(f"Caché LRU lleno. Eliminando la clave menos reciente: '{oldest_key[:50]}...'")

                cache.set(question, llm_answer)
                cache.zadd(LRU_SET_KEY, {question: get_current_timestamp()})
                app.logger.info(f"Respuesta para '{question[:50]}...' guardada en caché LRU.")
                
            return jsonify(score_result)

    except Exception as e:
        app.logger.error(f"Ocurrió un error inesperado: {e}")
        return jsonify({"error": "Error interno inesperado en el servidor de caché"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002)