import os
import requests
from flask import Flask, request, jsonify
from google import genai
from rouge_score import rouge_scorer
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STORAGE_SERVICE_URL = os.environ.get("STORAGE_SERVICE_URL", "http://storage_service:5002/save")

def get_gemini_client():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        logger.error("GEMINI_API_KEY no configurada")
        return None
    
    try:
        client = genai.Client(api_key=api_key)
        
        try:
            test_response = client.models.generate_content(
                model="gemini-2.5-flash", 
                contents="Hola, responde con 'OK' si funciono"
            )
            logger.info("Cliente Gemini configurado correctamente")
            return client
        except Exception as model_error:
            logger.error(f"Error con el modelo: {model_error}")
            try:
                test_response = client.models.generate_content(
                    model="gemini-1.5-flash",
                    contents="Test"
                )
                logger.info("Cliente Gemini configurado con gemini-1.5-flash")
                return client
            except Exception as e2:
                logger.error(f"Todos los modelos fallaron: {e2}")
                return None
                
    except Exception as e:
        logger.error(f"Error configurando Gemini: {e}")
        return None

class ScoreCalculator:
    def __init__(self):
        self.scorer = rouge_scorer.RougeScorer(['rouge1', 'rouge2', 'rougeL'], use_stemmer=True)
    
    def preprocess_text(self, text: str) -> str:
        if not text:
            return ""
            
        text = text.lower()
        text = re.sub(r'[^\w\s\.\?\!]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
    def calculate_score(self, generated_answer: str, reference_answer: str) -> float:
        try:
            gen_processed = self.preprocess_text(generated_answer)
            ref_processed = self.preprocess_text(reference_answer)
            
            if not gen_processed or not ref_processed:
                return 0.1
                
            error_phrases = ["no encontrada", "no disponible", "error buscando", "respuesta simulada"]
            if any(phrase in gen_processed for phrase in error_phrases):
                return 0.1
            
            scores = self.scorer.score(ref_processed, gen_processed)
            
            rouge1 = scores['rouge1'].fmeasure
            rouge2 = scores['rouge2'].fmeasure
            rougeL = scores['rougeL'].fmeasure
            
            if len(gen_processed.split()) > 3 and len(ref_processed.split()) > 3:
                overall_score = (rouge1 * 0.3 + rouge2 * 0.4 + rougeL * 0.3)
            else:
                overall_score = (rouge1 * 0.4 + rouge2 * 0.2 + rougeL * 0.4)
            
            gen_length = len(gen_processed.split())
            ref_length = len(ref_processed.split())
            
            if ref_length > 0:
                length_ratio = min(gen_length / ref_length, 2.0)
                if length_ratio < 0.3 or length_ratio > 1.7:
                    overall_score *= 0.8
            
            overall_score = max(0.0, min(1.0, overall_score))
            
            logger.info(f"ROUGE Scores - R1: {rouge1:.4f}, R2: {rouge2:.4f}, RL: {rougeL:.4f} -> Final: {overall_score:.4f}")
            
            return round(overall_score, 4)
            
        except Exception as e:
            logger.error(f"Error calculando score: {e}")
            return 0.0

def save_to_storage(question_text: str, original_answer: str, llm_answer: str, quality_score: float):
    try:
        data = {
            "question_text": question_text,
            "original_answer": original_answer,
            "llm_answer": llm_answer,
            "quality_score": quality_score
        }
        
        response = requests.post(STORAGE_SERVICE_URL, json=data, timeout=10)
        if response.status_code == 200:
            logger.info("Datos guardados exitosamente en storage service")
        else:
            logger.error(f"Error guardando en storage service: {response.status_code} - {response.text}")
            
    except requests.exceptions.RequestException as e:
        logger.error(f"No se pudo conectar al storage service: {e}")
    except Exception as e:
        logger.error(f"Error inesperado al guardar en storage: {e}")

def generate_semantic_mock_response(question_text: str) -> str:
    question_lower = question_text.lower()
    
    if 'distributed' in question_lower and 'system' in question_lower:
        return "A distributed system consists of multiple components located on different networked computers that communicate and coordinate their actions."
    elif 'cache' in question_lower:
        return "Cache is a hardware or software component that stores data so that future requests for that data can be served faster."
    elif 'database' in question_lower:
        return "A database is an organized collection of structured information, or data, typically stored electronically in a computer system."
    else:
        return f"This question about '{question_text}' addresses important aspects that require comprehensive understanding of the underlying principles and practical applications in the field."

gemini_client = get_gemini_client()
score_calculator = ScoreCalculator()

app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    components = {
        "gemini": gemini_client is not None
    }
    return jsonify({"status": "healthy", "components": components})

@app.route('/process', methods=['POST'])
def process_question():
    data = request.get_json()
    if not data or 'question' not in data:
        return jsonify({"error": "No se proporcionó una pregunta"}), 400

    question_text = data['question'].strip()
    correct_answer = data.get('correct_answer', '').strip()  # RECIBIR respuesta correcta
    
    if not question_text:
        return jsonify({"error": "Pregunta vacía"}), 400

    logger.info(f"Pregunta recibida: '{question_text[:60]}...'")

    try:
        # USAR LA RESPUESTA CORRECTA QUE YA RECIBIMOS
        reference_answer = correct_answer if correct_answer else "No se proporcionó respuesta de referencia"
        logger.info(f"Respuesta de referencia recibida: '{reference_answer[:60]}...'")
        
        if gemini_client:
            try:
                prompt = f"""
                Please provide a clear and concise answer to the following question in English.
                Focus on the key information and keep the response direct and factual.
                
                Question: {question_text}
                
                Answer:
                """
                
                response = gemini_client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt
                )
                llm_answer = response.text.strip()
                source = "gemini"
                logger.info(f"Respuesta generada con Gemini: {llm_answer[:100]}...")
                
            except Exception as e:
                logger.error(f"Error con Gemini API: {e}")
                llm_answer = generate_semantic_mock_response(question_text)
                source = "mock_fallback"
        else:
            llm_answer = generate_semantic_mock_response(question_text)
            source = "mock"
                
        quality_score = score_calculator.calculate_score(llm_answer, reference_answer)
        logger.info(f"Score de calidad calculado: {quality_score}")
        
        save_to_storage(question_text, reference_answer, llm_answer, quality_score)
        
        return jsonify({
            "status": "success",
            "source": source,
            "answer": llm_answer,
            "quality_score": quality_score,
            "question": question_text,
        })
        
    except Exception as e:
        logger.error(f"Error procesando pregunta: {e}")
        return jsonify({"error": "Error interno del servidor"}), 500

if __name__ == '__main__':
    logger.info("Iniciando Score Service (sin dataset local)...")
    app.run(host='0.0.0.0', port=5001, debug=False)