import requests
import time
import pandas as pd
import random
import os
import logging
import sys
import numpy as np
from enum import Enum

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("TrafficGenerator")

CACHE_SERVICE_URL = os.environ.get("CACHE_SERVICE_URL", "http://cache_service:5000/check")
DATASET_PATH = os.environ.get("DATASET_PATH", "/app/Data/test.csv")
# NUEVO: Leer la distribución seleccionada desde el script de inicio
TRAFFIC_DISTRIBUTION_CHOICE = os.environ.get("TRAFFIC_DISTRIBUTION", "uniform").lower() 

class TrafficDistribution(Enum):
    UNIFORM = "uniform"
    EXPONENTIAL = "exponential"

class TrafficGenerator:
    def __init__(self):
        self.qa_pairs = self.load_qa_pairs()
        if self.qa_pairs:
            logger.info(f"Dataset cargado con {len(self.qa_pairs)} pares pregunta-respuesta.")
        
    def load_qa_pairs(self):
        """Carga pares de pregunta-respuesta del dataset"""
        try:
            df = pd.read_csv(DATASET_PATH, header=None)
            logger.info(f"Dataset cargado: {len(df)} filas, {len(df.columns)} columnas")
            
            if len(df.columns) == 4:
                questions = df.iloc[:, 1].dropna().tolist()  
                answers = df.iloc[:, 3].dropna().tolist()    
                logger.info("Usando columnas 1 (pregunta) y 3 (respuesta)")
            elif len(df.columns) >= 3:
                questions = df.iloc[:, 1].dropna().tolist()
                answers = df.iloc[:, 2].dropna().tolist()
                logger.info("Usando columnas 1 y 2")
            else:
                questions = df.iloc[:, 0].dropna().tolist()
                answers = df.iloc[:, 1].dropna().tolist()
            
            qa_pairs = list(zip(questions, answers))
            logger.info(f"Se cargaron {len(qa_pairs)} pares pregunta-respuesta")
            return qa_pairs[:10000]
                
        except Exception as e:
            logger.error(f"Error cargando dataset: {e}")
            return [
                ("What is a distributed system?", "A system with multiple components on different computers"),
                ("What is cache memory?", "Fast memory that stores frequently accessed data")
            ]
    
    def send_question(self, question: str, correct_answer: str, distribution: str):
        """Envía una pregunta junto con su respuesta correcta y distribución al cache service"""
        try:
            log_question = (question[:60] + '...') if len(question) > 60 else question
            
            response = requests.post(
                CACHE_SERVICE_URL, 
                json={
                    "question": question,
                    "correct_answer": correct_answer,
                    "distribution": distribution  
                }, 
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                source = result.get('source', 'unknown').upper()
                
                if 'CACHE' in source:
                    logger.info(f"CACHE HIT! Respuesta obtenida desde {source}.")
                else:
                    score = result.get('quality_score', 'N/A')
                    logger.info(f"CACHE MISS. Respuesta obtenida desde {source} con Score: {score}")
            else:
                logger.error(f"HTTP Error {response.status_code}: {response.text}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"No se pudo conectar al servicio de cache: {e}")
        
    def get_interarrival_time(self, distribution: TrafficDistribution, phase: str = "normal"):
        if distribution == TrafficDistribution.UNIFORM:
            return random.uniform(1.0, 5.0)
        elif distribution == TrafficDistribution.EXPONENTIAL:
            if phase == "high":
                return np.random.exponential(scale=1.5)
            else:
                return np.random.exponential(scale=3.0)
        return 0  # Fallback

def simulate_uniform_distribution(generator: TrafficGenerator, duration_minutes: int = 10):
    logger.info("INICIANDO DISTRIBUCION UNIFORME - Trafico constante")
    logger.info("Justificacion: Simula usuarios regulares con comportamiento predecible")
    
    end_time = time.time() + (duration_minutes * 60)
    request_count = 0
    
    while time.time() < end_time:
        question, correct_answer = random.choice(generator.qa_pairs)
        generator.send_question(question, correct_answer, "UNIFORM")  # Añadir distribución
        request_count += 1
        
        sleep_time = generator.get_interarrival_time(TrafficDistribution.UNIFORM)
        time.sleep(sleep_time)
    
    logger.info(f"Distribucion Uniforme completada: {request_count} solicitudes en {duration_minutes} minutos")

def simulate_exponential_distribution(generator: TrafficGenerator, duration_minutes: int = 10):
    logger.info("INICIANDO DISTRIBUCION EXPONENCIAL - Trafico realista")
    logger.info("Justificacion: Modela trafico web real con periodos de alta/baja demanda")
    
    end_time = time.time() + (duration_minutes * 60)
    request_count = 0
    phase_duration = 60
    last_phase_change = time.time()
    current_phase = "normal"
    
    while time.time() < end_time:
        # Verificar cambio de fase cada minuto
        if time.time() - last_phase_change > phase_duration:
            current_phase = "high" if current_phase == "normal" else "normal"
            last_phase_change = time.time()
            logger.info(f"Cambiando a fase: {current_phase.upper()}")
        
        question, correct_answer = random.choice(generator.qa_pairs)
        generator.send_question(question, correct_answer, "EXPONENTIAL")  # Añadir distribución
        request_count += 1
        
        sleep_time = generator.get_interarrival_time(TrafficDistribution.EXPONENTIAL, current_phase)
        time.sleep(sleep_time)
    
    logger.info(f"Distribucion Exponencial completada: {request_count} solicitudes en {duration_minutes} minutos")

def main():
    logger.info("Iniciando Generador de Trafico con Multiples Distribuciones")
    logger.info("Esperando 20 segundos para que todos los servicios se inicien...")
    time.sleep(20)

    generator = TrafficGenerator()

    if not generator.qa_pairs:
        logger.error("No se encontraron pares pregunta-respuesta para enviar. Saliendo.")
        return

    try:
        logger.info("\n" + "="*60)
        
        # LÓGICA DE SELECCIÓN DE DISTRIBUCIÓN
        if TRAFFIC_DISTRIBUTION_CHOICE == TrafficDistribution.UNIFORM.value:
            simulate_uniform_distribution(generator, duration_minutes=10)  # Añadir parámetro
        elif TRAFFIC_DISTRIBUTION_CHOICE == TrafficDistribution.EXPONENTIAL.value:
            simulate_exponential_distribution(generator, duration_minutes=10)  # Añadir parámetro
        else:
            logger.error(f"Distribución '{TRAFFIC_DISTRIBUTION_CHOICE}' no reconocida. Ejecutando Uniforme por defecto.")
            simulate_uniform_distribution(generator, duration_minutes=10)
            
        logger.info("\nGeneración de tráfico completada exitosamente!")
        
    except KeyboardInterrupt:
        logger.info("Generacion de trafico detenida por el usuario.")
    except Exception as e:
        logger.error(f"Error inesperado: {e}")

if __name__ == '__main__':
    main()