import os
import psycopg2
import time
from psycopg2 import OperationalError
from flask import Flask


def initialize_database():
    """
    Se conecta a la base de datos con reintentos y asegura que la tabla exista.
    """
    db_url = os.environ.get("DATABASE_URL")
    retries = 5
    while retries > 0:
        try:
            print("Intentando conectar a la base de datos...")
            conn = psycopg2.connect(db_url)
            print(" Conexión a la base de datos exitosa.")
            break # Si la conexión es exitosa, sale del bucle
        except OperationalError:
            retries -= 1
            print(f"La base de datos no está lista. Reintentando en 5 segundos... ({retries} intentos restantes)")
            time.sleep(5)
    
    if retries == 0:
        print("No se pudo conectar a la base de datos.")
        return

    # Procede a crear la tabla si la conexión fue exitosa
    cur = conn.cursor()
    create_script = """
    CREATE TABL EIF NOT EXISTS qa_responses (
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
    conn.close()
    print(" Tabla 'qa_responses' inicializada correctamente.")

# --- Crea la aplicación Flask ---
app = Flask(__name__)

@app.route('/process', methods=['POST'])
def process_question():
    data = request.get_json()
    if not data or 'question' not in data:
        return jsonify({"error": "No se proporcionó una pregunta"}), 400

    question_text = data['question']
    print(f" Pregunta recibida: '{question_text}'")

    # --- LÓGICA PRINCIPAL (Cache Miss) ---
    # Por ahora, simularemos el flujo de un "cache miss" siempre.
    # 1. Llamar al LLM (Gemini) para obtener una respuesta. (Próximo paso)
    # 2. Implementar la métrica de score. (Próximo paso)
    # 3. Guardar todo en la base de datos PostgreSQL. (Próximo paso)

    # De momento, solo confirmamos la recepción
    return jsonify({"status": "Pregunta recibida y en procesamiento"}), 202


# --- Mantiene el servidor corriendo ---
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
