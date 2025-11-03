import os
import json
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessFunction
from pyflink.datastream.connectors.kafka import (
    KafkaSource, 
    KafkaSink, 
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.output_tag import OutputTag

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PyFlinkQualityChecker")

# --- Variables de Entorno ---
KAFKA_BROKER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
INPUT_TOPIC = os.environ.get('FLINK_INPUT_TOPIC', 'resultados_procesados')
OUTPUT_VALIDATED_TOPIC = os.environ.get('FLINK_OUTPUT_VALIDATED_TOPIC', 'resultados_validados')
OUTPUT_RETRY_TOPIC = os.environ.get('FLINK_OUTPUT_RETRY_TOPIC', 'preguntas_pendientes')

QUALITY_THRESHOLD = float(os.environ.get('QUALITY_THRESHOLD', '0.3'))
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '2'))

retry_output_tag = OutputTag("retry-output", Types.STRING())


class QualityCheckFunction(ProcessFunction):
    """
    Función de Flink que implementa la lógica de negocio.
    """
    def __init__(self, threshold, max_retries):
        self.threshold = threshold
        self.max_retries = max_retries

    def process_element(self, value, ctx):
        """Procesa cada mensaje que llega del tópico de resultados."""
        try:
            data = json.loads(value)
            score = data.get('score_rouge_l', 0.0)
            retry_count = data.get('retry_count', 0)
            pregunta = data.get('pregunta', 'N/A')

            logger.info(f"Evaluando: '{pregunta[:50]}...' | Score: {score:.4f} | Reintentos: {retry_count}")

            # 1. ¿Score suficientemente bueno?
            if score >= self.threshold:
                logger.info(f"APROBADO - Score {score:.4f} >= {self.threshold}")
                data['status'] = 'validated'
                # Enviar al stream PRINCIPAL (validados)
                yield json.dumps(data)
            
            # 2. Score bajo, pero aún quedan reintentos
            elif retry_count < self.max_retries:
                logger.warning(f"RECHAZADO - Score {score:.4f} < {self.threshold}. Reintento {retry_count + 1}/{self.max_retries}")
                
                # Preparar el mensaje para el reintento
                retry_message = {
                    'id_pregunta': data.get('id_pregunta', 'N/A'),
                    'pregunta': pregunta,
                    'respuesta_original': data.get('respuesta_original', ''),
                    'retry_count': retry_count + 1,
                    'previous_score': score,
                    'reason': 'low_quality_score'
                }
                
                # Usar yield con tupla (tag, valor)
                yield retry_output_tag, json.dumps(retry_message)

            # 3. Score bajo Y se agotaron los reintentos
            else:
                logger.warning(f"LÍMITE ALCANZADO - Score {score:.4f}. Guardando de todas formas.")
                data['status'] = 'validated_max_retries'
                data['quality_warning'] = True
                
                # Enviar al stream PRINCIPAL (validados)
                yield json.dumps(data)
                
        except Exception as e:
            logger.error(f"Error evaluando calidad: {e}", exc_info=True)


def run_flink_job():
    """Define y ejecuta el Job de PyFlink."""
    
    logger.info("Iniciando Job de PyFlink Quality Checker...")
    logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"Input Topic: {INPUT_TOPIC}")
    logger.info(f"Output Validated: {OUTPUT_VALIDATED_TOPIC}")
    logger.info(f"Output Retry: {OUTPUT_RETRY_TOPIC}")
    logger.info(f"Threshold: {QUALITY_THRESHOLD}, Max Retries: {MAX_RETRIES}")
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Para debugging, usar 1

    # --- 1. Definir el Source (de dónde leemos) ---
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics(INPUT_TOPIC) \
        .set_group_id("pyflink-quality-checker-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # --- 2. Definir los Sinks (dónde escribimos) ---
    
    # Sink para mensajes validados
    validated_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(OUTPUT_VALIDATED_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()

    # Sink para mensajes de reintento
    retry_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(OUTPUT_RETRY_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()

    # --- 3. Construir el Grafo de Flujo ---
    
    # Leer de Kafka
    # from_source requiere WatermarkStrategy
    data_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source (Resultados)"
    )

    # Aplicar la lógica de procesamiento
    processed_stream = data_stream.process(
        QualityCheckFunction(QUALITY_THRESHOLD, MAX_RETRIES),
        output_type=Types.STRING()
    ).name("Quality Check & Split")

    # Obtener el stream lateral usando el OutputTag creado fuera
    retry_stream = processed_stream.get_side_output(retry_output_tag)

    # --- 4. Enviar los streams a sus Sinks ---
    processed_stream.sink_to(validated_sink).name("Sink (Validados)")
    retry_stream.sink_to(retry_sink).name("Sink (Reintentos)")

    # --- 5. Ejecutar el Job ---
    logger.info("Job construido. Ejecutando...")
    env.execute("Kafka Quality Checker Job")


if __name__ == "__main__":
    try:
        run_flink_job()
    except Exception as e:
        logger.error(f"Error crítico en el job de Flink: {e}", exc_info=True)
        raise