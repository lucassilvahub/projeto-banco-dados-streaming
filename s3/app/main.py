import asyncio
import json
from aiokafka import AIOKafkaConsumer
import logging
from elasticsearch import AsyncElasticsearch
from datetime import datetime
import os

# ========================
# 🛠️ Configurações
# ========================
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPICOS = ["user_events", "response_events"]
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "elasticsearch:9200")
LOG_FILE = "system_logs.json"

# ========================
# 📥 Consumer Kafka
# ========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def log_evento(es, evento, origem):
    """Registra o evento no Elasticsearch ou em arquivo local"""
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "event": evento,
        "topic": origem,
        "correlation_id": evento.get("correlation_id", "N/A"),
        "event_type": evento.get("event_type", "N/A"),
        "source": evento.get("source", "N/A"),
        "target": evento.get("target", "N/A")
    }
    
    # Tenta enviar para o Elasticsearch
    if es:
        try:
            await es.index(index="system_logs", document=log_entry)
            logger.info(f"📝 Evento registrado no Elasticsearch: {evento.get('event_type')}")
            return
        except Exception as e:
            logger.error(f"❌ Erro ao registrar no Elasticsearch: {e}")
            # Continua para o fallback
    
    # Fallback: registra em arquivo local
    try:
        with open(LOG_FILE, "a") as log_file:
            log_file.write(json.dumps(log_entry) + "\n")
        logger.info(f"📝 Evento registrado em arquivo local: {evento.get('event_type')}")
    except Exception as e:
        logger.error(f"❌ Erro ao registrar em arquivo: {e}")

async def verificar_correlacao(evento_resposta):
    """Verifica e registra a correlação entre request e response"""
    if evento_resposta.get("event_type") == "response" and "correlation_id" in evento_resposta:
        correlation_id = evento_resposta["correlation_id"]
        original_type = evento_resposta.get("original_event_type", "desconhecido")
        status = evento_resposta.get("status", "desconhecido")
        
        logger.info(f"🔄 Ciclo completo para correlation_id: {correlation_id}")
        logger.info(f"   Evento original: {original_type}")
        logger.info(f"   Status: {status}")
        
        # Poderia registrar estatísticas de tempo de processamento, taxas de sucesso, etc.

async def monitorar_eventos():
    logger.info("🔄 Iniciando serviço de monitoramento S3...")
    
    # Tentar conectar ao Elasticsearch
    es = None
    try:
        logger.info(f"🔌 Tentando conectar ao Elasticsearch em {ES_HOST}...")
        es = AsyncElasticsearch([f"http://{ES_HOST}"])
        
        # Verificar a conexão
        if await es.ping():
            logger.info("✅ Conexão com Elasticsearch bem-sucedida!")
            
            # Verificar se o índice existe, se não, criá-lo
            if not await es.indices.exists(index="system_logs"):
                logger.info("🛠️ Criando índice system_logs...")
                await es.indices.create(
                    index="system_logs",
                    mappings={
                        "properties": {
                            "timestamp": {"type": "date"},
                            "correlation_id": {"type": "keyword"},
                            "event_type": {"type": "keyword"},
                            "source": {"type": "keyword"},
                            "target": {"type": "keyword"},
                            "topic": {"type": "keyword"},
                            "event": {"type": "object"}
                        }
                    }
                )
        else:
            logger.warning("⚠️ Elasticsearch respondeu, mas a conexão falhou.")
            es = None
    except Exception as e:
        logger.error(f"❌ Erro ao conectar ao Elasticsearch: {e}")
        es = None
    
    if not es:
        logger.warning("⚠️ Usando fallback para logs em arquivo local")
        # Garantir que o arquivo de log existe
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, "w") as f:
                f.write("")

    # Iniciar consumer Kafka
    try:
        logger.info(f"🔌 Conectando ao Kafka em {KAFKA_BROKER}...")
        consumer = AIOKafkaConsumer(
            *TOPICOS,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="s3_logger_group",
            auto_offset_reset="latest"
        )

        await consumer.start()
        logger.info("✅ Consumer Kafka iniciado com sucesso!")

        logger.info(f"📡 Monitorando tópicos: {', '.join(TOPICOS)}")
        async for msg in consumer:
            evento = msg.value
            topico = msg.topic
            
            # Registrar o evento no Elasticsearch ou no fallback
            await log_evento(es, evento, topico)
            
            # Verificar correlação entre request e response
            if topico == "response_events":
                await verificar_correlacao(evento)

    except Exception as e:
        logger.error(f"❌ Erro ao iniciar consumer Kafka: {e}")
    finally:
        logger.info("🛑 Fechando conexões...")
        if 'consumer' in locals():
            await consumer.stop()
        if es:
            await es.close()

if __name__ == "__main__":
    try:
        asyncio.run(monitorar_eventos())
    except KeyboardInterrupt:
        logger.info("🛑 Serviço interrompido pelo usuário")
    except Exception as e:
        logger.error(f"❌ Erro fatal: {e}")