import os
import json
import logging
from pyflink.datastream import StreamExecutionEnvironment, ProcessFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor

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

retry_tag = "retry-output"


class QualityCheckFunction(ProcessFunction):
    """
    Función de Flink que implementa la lógica de negocio.
    - El stream principal (ctx.output) será para los mensajes validados.
    - El stream lateral (ctx.output(retry_tag)) será para los reintentos.
    """
    def __init__(self, threshold, max_retries):
        self.threshold = threshold
        self.max_retries = max_retries
        self._retry_tag = retry_tag 
        self.flink_retry_tag = None 

    def open(self, runtime_context):
        self.flink_retry_tag = runtime_context.get_side_output(self._retry_tag)

    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        """Procesa cada mensaje que llega del tópico de resultados."""
        try:
            data = json.loads(value)
            score = data.get('score_rouge_l', 0.0)
            retry_count = data.get('retry_count', 0)
            pregunta = data.get('pregunta', 'N/A')

            logger.info(f"Evaluando: '{pregunta[:50]}...' | Score: {score:.4f} | Reintentos: {retry_count}")

            
            # 1. ¿Score suficientemente bueno?
            if score >= self.threshold:
                logger.info(f" APROBADO - Score {score:.4f} >= {self.threshold}")
                data['status'] = 'validated'
                # Enviar al stream PRINCIPAL (validados)
                yield json.dumps(data)
            
            # 2. Score bajo, pero aún quedan reintentos
            elif retry_count < self.max_retries:
                logger.warning(f"RECHAZADO - Score {score:.4f} < {self.threshold}. Reintento {retry_count + 1}/{self.max_retries}")
                
                # Prepara el mensaje para el reintento
                retry_message = {
                    'id_pregunta': data.get('id_pregunta', 'N/A'),
                    'pregunta': pregunta,
                    'respuesta_original': data.get('respuesta_original', ''),
                    'retry_count': retry_count + 1,
                    'previous_score': score,
                    'reason': 'low_quality_score'
                }
                
                # Enviar al stream LATERAL (reintentos)
                ctx.output(self.flink_retry_tag, json.dumps(retry_message))

            # 3. Score bajo Y se agotaron los reintentos
            else:
                logger.warning(f" LÍMITE ALCANZADO - Score {score:.4f}. Guardando de todas formas.")
                data['status'] = 'validated_max_retries'
                data['quality_warning'] = True
                
                # Enviar al stream PRINCIPAL (validados)
                yield json.dumps(data)
                
        except Exception as e:
            logger.error(f"❌ Error evaluando calidad: {e}", exc_info=True)
            # Opcional: enviar a un "dead-letter-queue" de Flink


def run_flink_job():
    """Define y ejecuta el Job de PyFlink."""
    
    logger.info("Iniciando Job de PyFlink Quality Checker...")
    env = StreamExecutionEnvironment.get_execution_environment()

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
    data_stream = env.from_source(kafka_source, "Kafka Source (Resultados)")

    # Aplicar la lógica de procesamiento
    # Necesitamos el OutputTag aquí
    _retry_tag_typed = Types.STRING()
    
    # Procesar y dividir el stream
    main_validated_stream = data_stream.process(
        QualityCheckFunction(QUALITY_THRESHOLD, MAX_RETRIES),
        output_type=Types.STRING() # Tipo del stream principal
    ).name("Quality Check & Split")

    # Obtener el stream lateral (de reintentos) usando el tag
    retry_stream = main_validated_stream.get_side_output(retry_tag, _retry_tag_typed)

    # --- 4. Enviar los streams a sus Sinks ---
    main_validated_stream.sink_to(validated_sink).name("Sink (Validados)")
    retry_stream.sink_to(retry_sink).name("Sink (Reintentos)")

    # --- 5. Ejecutar el Job ---
    logger.info("Job construido. Ejecutando...")
    env.execute("Kafka Quality Checker Job")


if __name__ == "__main__":
    # Necesitamos importar los conectores de Kafka
    # Esta es una forma de asegurar que Flink cargue los JARs correctos
    from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema
    
    run_flink_job()