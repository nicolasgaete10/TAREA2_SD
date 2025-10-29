import os
import json
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, 
    KafkaOffsetResetStrategy,
    KafkaSink,
    KafkaRecordSerializationSchema
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("FlinkQualityChecker")

# Variables de entorno
KAFKA_BROKER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
INPUT_TOPIC = os.environ.get('FLINK_INPUT_TOPIC', 'resultados_procesados')
OUTPUT_VALIDATED_TOPIC = os.environ.get('FLINK_OUTPUT_VALIDATED_TOPIC', 'resultados_validados')
OUTPUT_RETRY_TOPIC = os.environ.get('FLINK_OUTPUT_RETRY_TOPIC', 'preguntas_pendientes')

# Umbral de calidad y límite de reintentos (configurables)
QUALITY_THRESHOLD = float(os.environ.get('QUALITY_THRESHOLD', '0.3'))
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '2'))


class QualityEvaluator(MapFunction):
    """
    Función que evalúa la calidad de cada respuesta y decide qué hacer con ella.
    """
    
    def map(self, value):
        """
        Procesa cada mensaje recibido.
        
        Args:
            value: String JSON con los datos del mensaje
            
        Returns:
            Tupla (topic_destino, mensaje_json)
        """
        try:
            # Parsear el JSON
            data = json.loads(value)
            
            score = data.get('score_rouge_l', 0.0)
            retry_count = data.get('retry_count', 0)
            pregunta = data.get('pregunta', 'N/A')
            
            logger.info(f"Evaluando pregunta: '{pregunta[:50]}...' | Score: {score:.4f} | Reintentos: {retry_count}")
            
            # Decisión: ¿El score supera el umbral?
            if score >= QUALITY_THRESHOLD:
                #VALIDADO: Score suficientemente bueno
                logger.info(f"APROBADO - Score {score:.4f} >= {QUALITY_THRESHOLD}")
                data['status'] = 'validated'
                return (OUTPUT_VALIDATED_TOPIC, json.dumps(data))
            
            else:
                #RECHAZADO: Score bajo
                if retry_count < MAX_RETRIES:
                    # Aún quedan reintentos disponibles
                    logger.warning(f"RECHAZADO - Score {score:.4f} < {QUALITY_THRESHOLD}. Reintento {retry_count + 1}/{MAX_RETRIES}")
                    
                    # Preparar mensaje para reintento
                    retry_message = {
                        'id_pregunta': data.get('id_pregunta', 'N/A'),
                        'pregunta': pregunta,
                        'respuesta_original': data.get('respuesta_original', ''),
                        'retry_count': retry_count + 1,
                        'previous_score': score,
                        'reason': 'low_quality_score'
                    }
                    return (OUTPUT_RETRY_TOPIC, json.dumps(retry_message))
                
                else:
                    # Se agotaron los reintentos - aceptar de todas formas
                    logger.warning(f"⚠️ LÍMITE ALCANZADO - Score {score:.4f} pero sin más reintentos. Guardando de todas formas.")
                    data['status'] = 'validated_max_retries'
                    data['quality_warning'] = True
                    return (OUTPUT_VALIDATED_TOPIC, json.dumps(data))
        
        except json.JSONDecodeError as e:
            logger.error(f"Error al parsear JSON: {e}")
            return (None, None)
        except Exception as e:
            logger.error(f"Error inesperado en QualityEvaluator: {e}", exc_info=True)
            return (None, None)


def create_kafka_source(env, bootstrap_servers, topic, group_id):
    """
    Crea un KafkaSource para consumir mensajes.
    """
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_topics(topic) \
        .set_group_id(group_id) \
        .set_starting_offsets(KafkaOffsetResetStrategy.EARLIEST) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    return env.from_source(
        kafka_source,
        watermark_strategy=None,
        source_name=f"Kafka Source - {topic}"
    )


def create_kafka_sink(bootstrap_servers, topic):
    """
    Crea un KafkaSink para producir mensajes.
    """
    serialization_schema = KafkaRecordSerializationSchema.builder() \
        .set_topic(topic) \
        .set_value_serialization_schema(SimpleStringSchema()) \
        .build()
    
    return KafkaSink.builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_record_serializer(serialization_schema) \
        .build()


def main():
    """
    Función principal que configura y ejecuta el job de Flink.
    """
    logger.info("=" * 60)
    logger.info("Iniciando Flink Job: Quality Checker")
    logger.info(f"Umbral de calidad: {QUALITY_THRESHOLD}")
    logger.info(f"Máximo de reintentos: {MAX_RETRIES}")
    logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    logger.info("=" * 60)
    
    # 1. Crear el entorno de ejecución
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Configurar paralelismo (ajustable según recursos)
    env.set_parallelism(1)
    
    # 2. Crear el source de Kafka (consumir resultados procesados)
    input_stream = create_kafka_source(
        env,
        KAFKA_BROKER,
        INPUT_TOPIC,
        "flink-quality-checker-group"
    )
    
    # 3. Aplicar la lógica de evaluación
    evaluated_stream = input_stream.map(
        QualityEvaluator(),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    )
    
    # 4. Filtrar mensajes válidos (topic != None)
    valid_stream = evaluated_stream.filter(lambda x: x[0] is not None)
    
    # 5. Split del stream según el topic destino
    # Stream para resultados validados
    validated_stream = valid_stream.filter(
        lambda x: x[0] == OUTPUT_VALIDATED_TOPIC
    ).map(lambda x: x[1], output_type=Types.STRING())
    
    # Stream para reintentos
    retry_stream = valid_stream.filter(
        lambda x: x[0] == OUTPUT_RETRY_TOPIC
    ).map(lambda x: x[1], output_type=Types.STRING())
    
    # 6. Crear sinks de Kafka
    validated_sink = create_kafka_sink(KAFKA_BROKER, OUTPUT_VALIDATED_TOPIC)
    retry_sink = create_kafka_sink(KAFKA_BROKER, OUTPUT_RETRY_TOPIC)
    
    # 7. Conectar streams a sus respectivos sinks
    validated_stream.sink_to(validated_sink).name("Sink - Resultados Validados")
    retry_stream.sink_to(retry_sink).name("Sink - Reintentos")
    
    # 8. Ejecutar el job
    logger.info("Ejecutando Flink Job...")
    env.execute("Quality Checker Job")


if __name__ == "__main__":
    main()