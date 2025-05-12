import asyncio
import json
from aiokafka import AIOKafkaConsumer
import logging
from elasticsearch import AsyncElasticsearch
from datetime import datetime
import os
import sys
import time

# ========================
# 🛠️ Configurações
# ========================
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPICOS = ["user_events", "response_events"]
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "elasticsearch:9200")
LOG_FILE = "system_logs.json"
KAFKA_CONSUMER_TIMEOUT_MS = int(os.getenv("KAFKA_CONSUMER_TIMEOUT_MS", "30000"))

# ========================
# 📥 Configuração de Logs
# ========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("s3")

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

async def connect_to_elasticsearch(max_retries=15, retry_delay=5):
    """Conecta ao Elasticsearch com várias tentativas."""
    es = None
    retries = 0
    
    while es is None and retries < max_retries:
        try:
            logger.info(f"🔌 Tentando conectar ao Elasticsearch em {ES_HOST}... ({retries+1}/{max_retries})")
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
                return es
            else:
                logger.warning("⚠️ Elasticsearch respondeu, mas a conexão falhou.")
                retries += 1
                await asyncio.sleep(retry_delay)
        except Exception as e:
            logger.error(f"❌ Erro ao conectar ao Elasticsearch: {e}")
            retries += 1
            await asyncio.sleep(retry_delay)
    
    logger.warning("⚠️ Não foi possível conectar ao Elasticsearch. Usando fallback para logs em arquivo local.")
    return None

async def create_kafka_consumer(max_retries=15, retry_delay=5):
    """Inicia o consumidor Kafka com várias tentativas."""
    consumer = None
    retries = 0
    
    while consumer is None and retries < max_retries:
        try:
            logger.info(f"🔌 Conectando ao Kafka em {KAFKA_BROKER}... ({retries+1}/{max_retries})")
            consumer = AIOKafkaConsumer(
                *TOPICOS,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                group_id="s3_logger_group",
                auto_offset_reset="latest",
                consumer_timeout_ms=KAFKA_CONSUMER_TIMEOUT_MS
            )

            await consumer.start()
            logger.info("✅ Consumer Kafka iniciado com sucesso!")
            
            # Verificar os tópicos disponíveis
            topics = await consumer.topics()
            logger.info(f"📋 Tópicos disponíveis no Kafka: {', '.join(topics)}")
            
            # Verificar se os tópicos necessários existem
            missing_topics = [topic for topic in TOPICOS if topic not in topics]
            if missing_topics:
                logger.warning(f"⚠️ Tópicos não encontrados: {', '.join(missing_topics)}")
                # Não é necessário criar os tópicos aqui, pois o S2 já fará isso
            
            return consumer
        except Exception as e:
            logger.error(f"❌ Erro ao iniciar consumer Kafka: {e}")
            retries += 1
            await asyncio.sleep(retry_delay)
    
    if consumer is None:
        logger.error("❌ Não foi possível iniciar o Kafka Consumer após várias tentativas.")
        return None

async def monitorar_eventos():
    """Função principal para monitorar eventos."""
    logger.info("🔄 Iniciando serviço de monitoramento S3...")
    
    # Esperando para os serviços estarem prontos
    logger.info("⏳ Aguardando serviços estarem disponíveis...")
    await asyncio.sleep(10)  # Aguarda um pouco antes de tentar conectar
    
    # Tentar conectar ao Elasticsearch
    es = await connect_to_elasticsearch()
    
    if not es:
        logger.warning("⚠️ Usando fallback para logs em arquivo local")
        # Garantir que o arquivo de log existe
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, "w") as f:
                f.write("")

    # Iniciar consumer Kafka
    consumer = await create_kafka_consumer()
    if not consumer:
        if es:
            await es.close()
        logger.error("❌ Não foi possível iniciar o monitoramento sem conexão com o Kafka.")
        return False
    
    try:
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
        logger.error(f"❌ Erro ao processar mensagens: {e}")
        return False
    finally:
        logger.info("🛑 Fechando conexões...")
        if 'consumer' in locals():
            await consumer.stop()
        if es:
            await es.close()
    
    return True

async def main():
    """Função principal com retry para garantir a execução."""
    retry_count = 0
    max_retries = 5
    
    while retry_count < max_retries:
        success = await monitorar_eventos()
        if success:
            break
        
        retry_count += 1
        logger.warning(f"⚠️ Tentativa {retry_count}/{max_retries} falhou. Tentando novamente em 10 segundos...")
        await asyncio.sleep(10)
    
    if retry_count == max_retries:
        logger.error("❌ Número máximo de tentativas excedido. Encerrando serviço.")
        sys.exit(1)

# ========================
# 🚀 Ponto de Entrada
# ========================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Serviço interrompido pelo usuário")
    except Exception as e:
        logger.error(f"❌ Erro fatal: {e}")
        sys.exit(1)