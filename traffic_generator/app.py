# traffic_generator/app.py
import requests
import time
import pandas as pd
import random
import os

# La URL del otro servicio, tomada de las variables de entorno de docker-compose
SCORE_SERVICE_URL = "http://score_service:5001/process"
DATASET_PATH = "/app/Data/test.csv"

print("✅ Iniciando Generador de Tráfico...")

# Carga el dataset en memoria
try:
    df = pd.read_csv(DATASET_PATH)
    # Usaremos el título de la pregunta. Asegúrate de que 'question_title' es el nombre correcto de la columna
    questions = df['question_title'].dropna().tolist()
    print(f"Dataset cargado con {len(questions)} preguntas.")
except FileNotFoundError:
    print(f"Error: No se encontró el archivo del dataset en {DATASET_PATH}")
    questions = ["¿Qué es un sistema distribuido?", "¿Cómo funciona Docker?"] # Preguntas de respaldo

while True:
    try:
        # Elige una pregunta al azar
        question_text = random.choice(questions)

        # Envía la pregunta al score_service en formato JSON
        print(f"Enviando pregunta: '{question_text[:60]}...'")
        response = requests.post(SCORE_SERVICE_URL, json={"question": question_text}, timeout=10)

        # Imprime el resultado de la petición
        print(f"Respuesta del servicio: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"Error de conexión con el score_service: {e}")
    
    # Espera un tiempo aleatorio antes de la siguiente pregunta
    time.sleep(random.uniform(2, 6))