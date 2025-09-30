import requests
import time
import pandas as pd
import random
import os
import logging
import sys

# Configure logging to be clearer in the console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("TrafficGenerator")

# The URL of the cache service, taken from docker-compose environment variables
CACHE_SERVICE_URL = os.environ.get("CACHE_SERVICE_URL", "http://cache_service:5000/check")
DATASET_PATH = os.environ.get("DATASET_PATH", "/app/Data/test.csv")

class TrafficGenerator:
    def __init__(self):
        """Initializes the TrafficGenerator by loading questions from the dataset."""
        self.questions = self.load_questions()
        if self.questions:
            logger.info(f"Dataset loaded with {len(self.questions)} questions.")
        
    def load_questions(self):
        """Loads questions from the CSV dataset."""
        try:
            
            df = pd.read_csv(DATASET_PATH, header=None)
            questions = df.iloc[:, 1].dropna().tolist()
            return questions[:100]  # LIMITE DE PREGUNTAS QUE SE VAN A MANDAR EN EL CSV
        except Exception as e:
            logger.error(f"Error loading dataset: {e}")
            # Fallback question in case file loading fails
            return ["What is a distributed system?"]
    
    def send_question(self, question_text: str):
        """Sends a single question to the CACHE service."""
        try:
            log_question = (question_text[:60] + '...') if len(question_text) > 60 else question_text
            logger.info(f"Sending question: '{log_question}'")
            
            response = requests.post(
                CACHE_SERVICE_URL, 
                json={"question": question_text}, 
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                source = result.get('source', 'unknown').upper()
                
                if source == 'CACHE':
                    logger.info(">>>  CACHE HIT!!!!!! Response obtained directly from cache.")
                else:
                    score = result.get('quality_score', 'N/A')
                    logger.info(f"<<< CACHE MISS. Response obtained from {source} with Score: {score}")
            else:
                logger.error(f"HTTP Error {response.status_code}: {response.text}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Could not connect to cache service: {e}")

def main():
    """Main function to generate continuous random traffic."""
    logger.info("Starting Traffic Generator in NORMAL MODE...")

    # Wait for other services to be ready
    logger.info("Waiting 15 seconds for all services to start correctly...")
    time.sleep(15)

    generator = TrafficGenerator()

    if not generator.questions:
        logger.error("No questions found to send. Exiting.")
        return

    logger.info("--- Starting to send traffic continuously ---")
    while True:
        try:
            # Select a random question from the list
            question = random.choice(generator.questions)
            
            # Send the question
            generator.send_question(question)
            
            # Wait for a random interval before sending the next one
            sleep_time = random.uniform(1, 5) # Wait between 1 and 5 seconds
            logger.info(f"Waiting for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Traffic generation stopped by user.")
            break
        except Exception as e:
            logger.error(f"An unexpected error occurred in the main loop: {e}")
            time.sleep(10) # Wait a bit longer after an error


if __name__ == '__main__':
    main()
