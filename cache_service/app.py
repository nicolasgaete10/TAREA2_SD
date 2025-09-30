from flask import Flask, request, jsonify
import redis
import requests
import os
import logging

# Configurar un logging más claro
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
app = Flask(__name__)


REDIS_URL = os.environ.get("REDIS_URL", "redis://redis_cache:6379")
SCORE_SERVICE_URL = os.environ.get("SCORE_SERVICE_URL", "http://score_service:5001/process")

try:
    # Usamos from_url para que sea más fácil de configurar con la variable de entorno
    cache = redis.from_url(REDIS_URL, decode_responses=True)
    # Comprobamos la conexión al iniciar
    cache.ping()
    app.logger.info("Conexión a Redis exitosa.")
except redis.exceptions.ConnectionError as e:
    app.logger.error(f"FATAL: No se pudo conectar a Redis en {REDIS_URL}. Error: {e}")
    cache = None

@app.route('/check', methods=['POST'])
def check_cache():
    """
    Este es el endpoint principal. Recibe una pregunta, la busca en caché
    y si no la encuentra, la delega al servicio de score.
    """
    if not cache:
        return jsonify({"error": "Servicio de caché no disponible (no se pudo conectar a Redis)"}), 503

    data = request.json
    question = data.get('question')

    if not question:
        return jsonify({"error": "La solicitud JSON debe incluir una 'pregunta'"}), 400

    try:
        # 1. Buscar la pregunta en la caché de Redis
        cached_response = cache.get(question)

        if cached_response:
            # CACHE HIT: La pregunta se encontró en Redis
            app.logger.info(f"Cache HIT para la pregunta: '{question[:50]}...'")
            return jsonify({
                "source": "cache",
                "question": question,
                "answer": cached_response
            })
        else:
            # CACHE MISS: La pregunta no estaba en Redis
            app.logger.info(f"Cache MISS para la pregunta: '{question[:50]}...'")
            
            # 2. Llamar al servicio de score para obtener la respuesta del LLM
            response = requests.post(SCORE_SERVICE_URL, json={'question': question}, timeout=30)
            response.raise_for_status()  # Lanza una excepción si hay un error (4xx o 5xx)
            
            score_result = response.json()
            llm_answer = score_result.get('answer')

            # 3. Guardar la nueva respuesta en la caché para futuras peticiones
            if llm_answer:
                cache.set(question, llm_answer)
                app.logger.info(f"Respuesta para '{question[:50]}...' guardada en caché.")
                
            # 4. Devolver el resultado obtenido del servicio de score
            return jsonify(score_result)

    except redis.exceptions.RedisError as e:
        app.logger.error(f"Error de Redis: {e}")
        return jsonify({"error": "Error interno del servicio de caché (Redis)"}), 500
    except requests.exceptions.RequestException as e:
        app.logger.error(f"No se pudo contactar al servicio de score: {e}")
        return jsonify({"error": f"Error contactando al servicio de score: {str(e)}", "source": "error"}), 502 # 502 Bad Gateway
    except Exception as e:
        app.logger.error(f"Ocurrió un error inesperado: {e}")
        return jsonify({"error": "Error interno inesperado en el servidor de caché"}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Un endpoint simple para verificar que el servicio está vivo y conectado a Redis."""
    try:
        cache.ping()
        redis_status = "conectado"
    except:
        redis_status = "desconectado"
    return jsonify({"status": "ok", "redis_status": redis_status})

if __name__ == '__main__':
    # Usar el puerto 5000, como se define en el docker-compose
    app.run(host='0.0.0.0', port=5000)