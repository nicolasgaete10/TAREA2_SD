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
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# La URL del otro servicio, tomada de las variables de entorno de docker-compose
SCORE_SERVICE_URL = os.environ.get("SCORE_SERVICE_URL", "http://score_service:5001/process")
DATASET_PATH = os.environ.get("DATASET_PATH", "/app/data/test.csv")

class TrafficGenerator:
    def __init__(self):
        self.questions = self.load_questions()
        logger.info(f"Dataset cargado con {len(self.questions)} preguntas.")
        
    def load_questions(self):
        """Carga preguntas desde el dataset CSV"""
        try:
            df = pd.read_csv(DATASET_PATH)
            
            # Intentar diferentes columnas posibles
            if 'question_title' in df.columns:
                questions = df['question_title'].dropna().tolist()
            elif 'question_content' in df.columns:
                questions = df['question_content'].dropna().tolist()
            elif 'Question' in df.columns:
                questions = df['Question'].dropna().tolist()
            else:
                # Usar primera columna de texto disponible
                text_columns = df.select_dtypes(include=['object']).columns
                if len(text_columns) > 0:
                    questions = df[text_columns[0]].dropna().tolist()
                else:
                    raise ValueError("No se encontraron columnas de texto en el dataset")
            
            return questions[:2000]  # Limitar para pruebas
            
        except Exception as e:
            logger.error(f"Error cargando dataset: {e}")
            # Preguntas de respaldo
            return [
                "¿Qué es un sistema distribuido?",
                "¿Cómo funciona Docker?",
                "¿Cuáles son los beneficios del ejercicio regular?",
                "¿Qué es la inteligencia artificial?",
                "¿Cómo se produce la energía solar?",
                "¿Qué es el machine learning?",
                "¿Cuáles son los planetas del sistema solar?",
                "¿Cómo funciona internet?",
                "¿Qué es la programación orientada a objetos?",
                "¿Cuáles son las principales causas del cambio climático?"
            ]
    
    def send_question(self, question_text: str):
        """Envía una pregunta al servicio de scoring"""
        try:
            logger.info(f"Enviando pregunta: '{question_text[:50]}...'")
            
            response = requests.post(
                SCORE_SERVICE_URL, 
                json={"question": question_text}, 
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                source = result.get('source', 'unknown')
                if source == 'cache':
                    logger.info(f"Respuesta desde CACHÉ")
                else:
                    score = result.get('quality_score', 'N/A')
                    logger.info(f"Respuesta desde {source.upper()} - Score: {score}")
            else:
                logger.error(f"Error HTTP {response.status_code}: {response.text}")
                
        except requests.exceptions.Timeout:
            logger.error("Timeout - El servicio no respondió a tiempo")
        except requests.exceptions.ConnectionError:
            logger.error("Error de conexión - No se pudo conectar al servicio")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error de solicitud: {e}")
    
    def generate_uniform_traffic(self, requests_per_minute: int, duration_minutes: int):
        """Genera tráfico con distribución uniforme"""
        interval = 60.0 / requests_per_minute  # segundos entre requests
        total_requests = requests_per_minute * duration_minutes
        
        logger.info(f"Iniciando tráfico UNIFORME: {requests_per_minute} req/min por {duration_minutes} min")
        logger.info(f"Intervalo: {interval:.2f}s entre requests")
        
        for i in range(total_requests):
            question = random.choice(self.questions)
            self.send_question(question)
            
            if i < total_requests - 1:
                time.sleep(interval)
        
        logger.info(f"Tráfico UNIFORME completado: {total_requests} requests")
    
    def generate_burst_traffic(self, burst_size: int, burst_interval: int, duration_minutes: int):
        """Genera tráfico en ráfagas (útil para testing de caché)"""
        total_bursts = (duration_minutes * 60) // burst_interval
        
        logger.info(f"Iniciando tráfico BURST: {burst_size} req cada {burst_interval}s por {duration_minutes} min")
        
        for burst in range(total_bursts):
            logger.info(f"Ráfaga {burst + 1}/{total_bursts}")
            
            # Enviar múltiples requests rápidamente
            for i in range(burst_size):
                question = random.choice(self.questions)
                self.send_question(question)
                time.sleep(0.1)  # Pequeño delay entre requests de la misma ráfaga
            
            # Esperar hasta la siguiente ráfaga
            if burst < total_bursts - 1:
                time.sleep(burst_interval)
        
        logger.info(f"Tráfico BURST completado: {total_bursts * burst_size} requests")

def main():
    """Función principal"""
    logger.info("Iniciando Generador de Tráfico...")
    
    # Esperar a que el score_service esté listo
    logger.info("Esperando a que el score_service esté disponible...")
    time.sleep(10)
    
    generator = TrafficGenerator()
    
    # Ejecutar diferentes patrones de tráfico
    try:
        # Patrón 1: Tráfico uniforme bajo
        generator.generate_uniform_traffic(
            requests_per_minute=12,  # 1 request cada 5 segundos
            duration_minutes=2
        )
        
        time.sleep(5)  # Pausa entre patrones
        
        # Patrón 2: Tráfico en ráfagas (para testear caché)
        generator.generate_burst_traffic(
            burst_size=5,
            burst_interval=10,
            duration_minutes=1
        )
        
        logger.info("Todos los patrones de tráfico completados")
        
    except KeyboardInterrupt:
        logger.info(" Generador de tráfico detenido manualmente")
    except Exception as e:
        logger.error(f" Error en generador de tráfico: {e}")

if __name__ == '__main__':
    main()