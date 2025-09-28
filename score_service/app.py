import os
import psycopg2
import time
import redis
import pandas as pd
from psycopg2 import OperationalError
from flask import Flask, request, jsonify
from google import genai
from google.genai import types
from rouge_score import rouge_scorer
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
DATASET_PATH = os.environ.get("DATASET_PATH", "/app/data/test.csv")

dataset_df = None

def initialize_database():
    """
    Se conecta a la base de datos con reintentos y asegura que la tabla exista.
    """
    db_url = os.environ.get("DATABASE_URL")
    retries = 5
    conn = None
    while retries > 0:
        try:
            print("Intentando conectar a la base de datos...")
            conn = psycopg2.connect(db_url)
            print("Conexión a la base de datos exitosa.")
            break
        except OperationalError:
            retries -= 1
            print(f"La base de datos no está lista. Reintentando en 5 segundos... ({retries} intentos restantes)")
            time.sleep(5)
    
    if retries == 0:
        print("No se pudo conectar a la base de datos.")
        return None

    # Procede a crear la tabla si la conexión fue exitosa
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
        print("Tabla 'qa_responses' inicializada correctamente.")
        return conn
    except Exception as e:
        print(f"Error creando tabla: {e}")
        return None

def get_redis_client():
    """Configura y retorna cliente Redis"""
    try:
        redis_url = os.environ.get("REDIS_URL", "redis://redis_cache:6379")
        redis_client = redis.from_url(redis_url)
        redis_client.ping()  
        print("Conexión a Redis exitosa.")
        return redis_client
    except Exception as e:
        print(f"Error conectando a Redis: {e}")
        return None

def get_gemini_client():
    """Configura y retorna cliente Gemini con la nueva API"""
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("GEMINI_API_KEY no configurada")
        return None
    
    try:
        from google import genai
        
        client = genai.Client(api_key=api_key)
        
        try:
            # Probar diferentes modelos
            test_response = client.models.generate_content(
                model="gemini-2.5-flash", 
                contents="Hola, responde con 'OK' si funciono"
            )
            print("Cliente Gemini configurado correctamente")
            return client
        except Exception as model_error:
            print(f"Error con el modelo: {model_error}")
            # Intentar con otro modelo
            try:
                test_response = client.models.generate_content(
                    model="gemini-1.5-flash",
                    contents="Test"
                )
                print("Cliente Gemini configurado con gemini-1.5-flash")
                return client
            except Exception as e2:
                print(f"Todos los modelos fallaron: {e2}")
                return None
                
    except ImportError:
        print("Librería 'google-genai' no instalada")
        return None
    except Exception as e:
        print(f"Error configurando Gemini: {e}")
        return None

class ScoreCalculator:
    def __init__(self):
        self.scorer = rouge_scorer.RougeScorer(['rouge1', 'rouge2', 'rougeL'], use_stemmer=True)
    
    def calculate_score(self, generated_answer: str, reference_answer: str) -> float:
        """
        Calcula score de calidad usando ROUGE metrics
        Retorna un valor entre 0 y 1
        """
        try:
            scores = self.scorer.score(reference_answer, generated_answer)
            # Usamos el promedio de ROUGE-1 y ROUGE-L como score principal
            rouge1 = scores['rouge1'].fmeasure
            rougeL = scores['rougeL'].fmeasure
            overall_score = (rouge1 + rougeL) / 2
            return round(overall_score, 4)
        except Exception as e:
            logger.error(f"Error calculando score: {e}")
            return 0.0
def load_dataset():
    """Carga el dataset sin encabezados"""
    global dataset_df
    
    try:
        dataset_path = pd.read_csv(DATASET_PATH)
        
        # Leer dataset SIN header
        dataset_df = pd.read_csv(dataset_path, header=None)
        print(f"Dataset cargado: {len(dataset_df)} filas")
        
        # Asignar nombres directamente para 3 columnas
        dataset_df.columns = ['class_label', 'question', 'best_answer']
        print("Columnas asignadas: ['class_label', 'question', 'best_answer']")
            
        # Mostrar muestra
        print("Muestra del dataset:")
        for i in range(min(3, len(dataset_df))):
            row = dataset_df.iloc[i]
            print(f"  {i+1}. Clase: {row['class_label']} | Pregunta: {str(row['question'])[:50]}...")
            
        return True
        
    except Exception as e:
        print(f"Error cargando dataset: {e}")
        dataset_df = None
        return False

def get_reference_answer(question: str) -> str:
    """
    Busca la respuesta REAL en el dataset CSV de 3 columnas
    """
    if dataset_df is None:
        return "Dataset no disponible"
    
    try:
        # Limpiar la pregunta para búsqueda
        question_clean = question.strip()
        
        print(f"Buscando: '{question_clean}'")
        
        # ESTRATEGIA 1: Búsqueda exacta en la columna 'question'
        if 'question' in dataset_df.columns:
            exact_match = dataset_df[dataset_df['question'] == question_clean]
            if not exact_match.empty:
                best_answer = exact_match.iloc[0]['best_answer']
                print("Encontrado (exacto)")
                return best_answer if pd.notna(best_answer) else "Respuesta no disponible"
        
        # ESTRATEGIA 2: Búsqueda por similitud (case-insensitive)
        question_lower = question_clean.lower()
        
        # Buscar en 'question' (case insensitive, parcial)
        if 'question' in dataset_df.columns:
            partial_matches = dataset_df[
                dataset_df['question'].str.lower().str.contains(question_lower, na=False)
            ]
            if not partial_matches.empty:
                best_answer = partial_matches.iloc[0]['best_answer']
                print("Encontrado (parcial)")
                return best_answer if pd.notna(best_answer) else "Respuesta no disponible"
        
        # ESTRATEGIA 3: Búsqueda por palabras clave
        words = [word for word in question_lower.split() if len(word) > 3]
        
        if words and 'question' in dataset_df.columns:
            for word in words[:2]:  # Solo primeras 2 palabras clave
                keyword_matches = dataset_df[
                    dataset_df['question'].str.lower().str.contains(word, na=False)
                ]
                if not keyword_matches.empty:
                    best_answer = keyword_matches.iloc[0]['best_answer']
                    print(f"Encontrado por palabra clave '{word}'")
                    return best_answer if pd.notna(best_answer) else "Respuesta no disponible"
        
        print("Pregunta no encontrada en dataset")
        return "Pregunta no encontrada en dataset"
        
    except Exception as e:
        print(f"Error buscando en dataset: {e}")
        return f"Error buscando respuesta: {str(e)}"

# --- Inicialización de componentes ---
db_connection = initialize_database()
redis_client = get_redis_client()
gemini_client = get_gemini_client()
score_calculator = ScoreCalculator()

# Carga el dataset al iniciar
load_dataset()

# --- Crea la aplicación Flask ---
app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint para verificar estado del servicio"""
    components = {
        "database": db_connection is not None,
        "redis": redis_client is not None,
        "gemini": gemini_client is not None
    }
    return jsonify({"status": "healthy", "components": components})

@app.route('/process', methods=['POST'])
def process_question():
    # Verificar JSON
    data = request.get_json()
    if not data or 'question' not in data:
        return jsonify({"error": "No se proporcionó una pregunta"}), 400

    question_text = data['question'].strip()
    if not question_text:
        return jsonify({"error": "Pregunta vacía"}), 400

    logger.info(f"Pregunta recibida: '{question_text[:60]}...'")

    # 1. VERIFICAR CACHÉ
    cache_key = f"answer:{question_text}"
    if redis_client:
        try:
            cached_answer = redis_client.get(cache_key)
            if cached_answer:
                logger.info("Respuesta encontrada en caché")
                # Incrementar contador en base de datos
                increment_request_count(question_text)
                return jsonify({
                    "status": "success",
                    "source": "cache",
                    "answer": cached_answer.decode('utf-8'),
                    "question": question_text
                })
        except Exception as e:
            logger.error(f"Error accediendo a Redis: {e}")

    # 2. CACHE MISS - PROCESAR CON LLM
    try:
        # Obtener respuesta de referencia
        reference_answer = get_reference_answer(question_text)
        
        # Generar respuesta con Gemini
        if gemini_client:
            try:
        
                response = gemini_client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=f"Responde de manera concisa y directa en ingles: {question_text}"
                )
                llm_answer = response.text.strip()
                source = "gemini"
                logger.info(f"Respuesta generada con Gemini: {llm_answer[:150]}...")
            except Exception as e:
                logger.error(f"Error con Gemini API: {e}")
            # Fallback a mock
                llm_answer = generate_mock_response(question_text)
                source = "mock_fallback"
        else:
            # Fallback mock
            llm_answer = generate_mock_response(question_text)
            source = "mock"
                
        # 3. CALCULAR SCORE DE CALIDAD
        quality_score = score_calculator.calculate_score(llm_answer, reference_answer)
        logger.info(f"Score de calidad calculado: {quality_score}")
        
        # 4. GUARDAR EN CACHÉ Y BASE DE DATOS
        if redis_client:
            redis_client.setex(cache_key, 3600, llm_answer)  # Cache por 1 hora
        
        save_to_database(question_text, reference_answer, llm_answer, quality_score)
        
        # 5. RETORNAR RESPUESTA
        return jsonify({
            "status": "success",
            "source": source,
            "answer": llm_answer,
            "quality_score": quality_score,
            "question": question_text,
            "cache_key": cache_key
        })
        
    except Exception as e:
        logger.error(f"Error procesando pregunta: {e}")
        return jsonify({"error": "Error interno del servidor"}), 500

def generate_mock_response(question_text: str) -> str:
    return f"Respuesta simulada para: {question_text}"

def increment_request_count(question_text: str):
    """Incrementa el contador de requests para una pregunta"""
    if not db_connection:
        return
    
    try:
        cur = db_connection.cursor()
        cur.execute("""
            UPDATE qa_responses 
            SET request_count = request_count + 1, updated_at = NOW()
            WHERE question_text = %s
        """, (question_text,))
        db_connection.commit()
        cur.close()
    except Exception as e:
        logger.error(f"Error incrementando contador: {e}")

def save_to_database(question_text: str, original_answer: str, llm_answer: str, quality_score: float):
    """Guarda la pregunta y respuestas en la base de datos"""
    if not db_connection:
        logger.warning(" No hay conexión a la base de datos")
        return
    
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
        logger.info("Datos guardados en base de datos")
    except Exception as e:
        logger.error(f"Error guardando en base de datos: {e}")

# --- Mantiene el servidor corriendo ---
if __name__ == '__main__':
    print("Iniciando Score Service...")
    app.run(host='0.0.0.0', port=5001, debug=False)  # debug=False para producción