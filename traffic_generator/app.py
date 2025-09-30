import requests
import time
import pandas as pd
import random
import os
import logging
import sys

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("TrafficGenerator")

CACHE_SERVICE_URL = os.environ.get("CACHE_SERVICE_URL", "http://cache_service:5000/check")
DATASET_PATH = os.environ.get("DATASET_PATH", "/app/Data/test.csv")

class TrafficGenerator:
    def __init__(self):
        self.questions = self.load_questions()
        if self.questions:
            logger.info(f"Dataset cargado con {len(self.questions)} preguntas.")
        
    def load_questions(self):
        try:
            df = pd.read_csv(DATASET_PATH, header=None)
            questions = df.iloc[:, 1].dropna().tolist()
            return questions[:15] # Limitar a 15 para pruebas
        except Exception as e:
            logger.error(f"Error cargando dataset: {e}")
            return ["What is a distributed system?"]
    
    def send_question(self, question_text: str):
        try:
            log_question = (question_text[:60] + '...') if len(question_text) > 60 else question_text
            logger.info(f"Enviando pregunta: '{log_question}'")
            
            response = requests.post(
                CACHE_SERVICE_URL, 
                json={"question": question_text}, 
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                source = result.get('source', 'unknown').upper()
                
                # --- INICIO DE LA CORRECCIÓN ---
                # Ahora aceptamos 'CACHE' y 'LRU_CACHE' como un hit
                if 'CACHE' in source:
                    logger.info(f">>> ¡CACHE HIT! Respuesta obtenida desde {source}.")
                # --- FIN DE LA CORRECCIÓN ---
                else:
                    score = result.get('quality_score', 'N/A')
                    logger.info(f"<<< CACHE MISS. Respuesta obtenida desde {source} con Score: {score}")
            else:
                logger.error(f"HTTP Error {response.status_code}: {response.text}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"No se pudo conectar al servicio de caché: {e}")

def main():
    logger.info("Iniciando Generador de Tráfico en MODO NORMAL...")
    logger.info("Esperando 15 segundos para que los servicios se inicien...")
    time.sleep(15)

    generator = TrafficGenerator()

    if not generator.questions:
        logger.error("No se encontraron preguntas para enviar. Saliendo.")
        return

    logger.info("--- Iniciando envío de tráfico continuo ---")
    while True:
        try:
            question = random.choice(generator.questions)
            generator.send_question(question)
            sleep_time = random.uniform(2, 5) # Intervalo ligeramente más largo para observar mejor
            logger.info(f"Esperando {sleep_time:.2f} segundos...")
            time.sleep(sleep_time)
        except KeyboardInterrupt:
            logger.info("Generación de tráfico detenida por el usuario.")
            break
        except Exception as e:
            logger.error(f"Error inesperado en el bucle principal: {e}")
            time.sleep(10)

if __name__ == '__main__':
    main()
