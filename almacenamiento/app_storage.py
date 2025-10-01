import os
import psycopg2
import time
import logging
from psycopg2 import OperationalError
from flask import Flask, request, jsonify

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def initialize_database():
    db_url = os.environ.get("DATABASE_URL")
    retries = 5
    conn = None
    
    while retries > 0:
        try:
            logger.info("Intentando conectar a la base de datos...")
            conn = psycopg2.connect(db_url)
            logger.info("Conexion a la base de datos exitosa.")
            break
        except OperationalError:
            retries -= 1
            logger.warning(f"Base de datos no lista. Reintentando en 5 segundos... ({retries} intentos restantes)")
            time.sleep(5)
    
    if retries == 0:
        logger.error("No se pudo conectar a la base de datos.")
        return None

    try:
        cur = conn.cursor()
        create_script = """
        CREATE TABLE IF NOT EXISTS qa_responses (
            id SERIAL PRIMARY KEY,
            question_text TEXT NOT NULL UNIQUE,
            original_answer TEXT,
            llm_answer TEXT,
            quality_score FLOAT,
            request_count INTEGER DEFAULT 1,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """
        cur.execute(create_script)
        conn.commit()
        cur.close()
        logger.info("Tabla 'qa_responses' inicializada correctamente.")
        return conn
    except Exception as e:
        logger.error(f"Error creando tabla: {e}")
        return None

db_connection = initialize_database()

@app.route('/health', methods=['GET'])
def health_check():
    status = "healthy" if db_connection else "unhealthy"
    return jsonify({
        "status": status,
        "service": "storage",
        "database_connected": db_connection is not None
    })

@app.route('/save', methods=['POST'])
def save_data():
    if not db_connection:
        return jsonify({"error": "Base de datos no disponible"}), 503

    data = request.get_json()
    if not data:
        return jsonify({"error": "No se proporcionaron datos JSON"}), 400

    required_fields = ['question_text', 'original_answer', 'llm_answer', 'quality_score']
    for field in required_fields:
        if field not in data:
            return jsonify({"error": f"Campo requerido faltante: {field}"}), 400

    question_text = data['question_text']
    original_answer = data['original_answer']
    llm_answer = data['llm_answer']
    quality_score = data['quality_score']

    try:
        cur = db_connection.cursor()
        cur.execute("""
            INSERT INTO qa_responses 
            (question_text, original_answer, llm_answer, quality_score) 
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (question_text) 
            DO UPDATE SET 
                llm_answer = EXCLUDED.llm_answer,
                quality_score = EXCLUDED.quality_score,
                request_count = qa_responses.request_count + 1,
                updated_at = NOW()
        """, (question_text, original_answer, llm_answer, quality_score))
        db_connection.commit()
        cur.close()
        
        logger.info(f"Datos guardados para pregunta: '{question_text[:50]}...'")
        return jsonify({"status": "success", "message": "Datos guardados correctamente"})
        
    except Exception as e:
        logger.error(f"Error guardando en base de datos: {e}")
        return jsonify({"error": f"Error interno al guardar datos: {str(e)}"}), 500

@app.route('/stats', methods=['GET'])
def get_stats():
    if not db_connection:
        return jsonify({"error": "Base de datos no disponible"}), 503

    try:
        cur = db_connection.cursor()
        
        cur.execute("SELECT COUNT(*) FROM qa_responses")
        total_records = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT question_text) FROM qa_responses")
        unique_questions = cur.fetchone()[0]
        
        cur.execute("SELECT AVG(quality_score) FROM qa_responses WHERE quality_score IS NOT NULL")
        avg_score = cur.fetchone()[0]
        
        cur.execute("SELECT question_text, request_count FROM qa_responses ORDER BY request_count DESC LIMIT 1")
        popular_result = cur.fetchone()
        most_popular = popular_result[0] if popular_result else "N/A"
        max_requests = popular_result[1] if popular_result else 0
        
        cur.close()
        
        return jsonify({
            "total_records": total_records,
            "unique_questions": unique_questions,
            "average_quality_score": round(avg_score, 4) if avg_score else 0,
            "most_popular_question": most_popular,
            "max_requests": max_requests
        })
        
    except Exception as e:
        logger.error(f"Error obteniendo estadisticas: {e}")
        return jsonify({"error": f"Error interno al obtener estadisticas: {str(e)}"}), 500

if __name__ == '__main__':
    logger.info("Iniciando Storage Service...")
    app.run(host='0.0.0.0', port=5002, debug=False)