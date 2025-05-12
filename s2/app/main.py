import asyncio
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from datetime import datetime
import logging
import os
import time
import sys
from aiohttp import web

# Importar handlers para os diferentes bancos de dados
# Handlers foram modificados para evitar importação circular
import postgres_handler
import mongodb_handler
import redis_handler

# ========================
# 🛠️ Configurações
# ========================
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = "user_events"
OUTPUT_TOPIC = "response_events"
KAFKA_CONSUMER_TIMEOUT_MS = int(os.getenv("KAFKA_CONSUMER_TIMEOUT_MS", "30000"))

# ========================
# 📥 Configuração de Logs
# ========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("s2_main")

# ========================
# 🔄 Funções Utilitárias do Kafka
# ========================
async def create_kafka_producer(max_retries=15, retry_delay=5):
    """Inicia o produtor Kafka com várias tentativas."""
    producer = None
    retries = 0
    
    while producer is None and retries < max_retries:
        try:
            logger.info(f"🔌 Iniciando Kafka Producer... ({retries+1}/{max_retries})")
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            await producer.start()
            logger.info("✅ Kafka Producer iniciado com sucesso!")
            return producer
        except Exception as e:
            logger.error(f"❌ Erro ao iniciar Kafka Producer: {e}")
            retries += 1
            await asyncio.sleep(retry_delay)
    
    if producer is None:
        logger.error("❌ Não foi possível iniciar o Kafka Producer após várias tentativas.")
        return None

async def create_kafka_consumer(max_retries=15, retry_delay=5):
    """Inicia o consumidor Kafka com várias tentativas."""
    consumer = None
    retries = 0
    
    while consumer is None and retries < max_retries:
        try:
            logger.info(f"🔌 Iniciando Kafka Consumer... ({retries+1}/{max_retries})")
            consumer = AIOKafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id="s2_consumer_group",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
                consumer_timeout_ms=KAFKA_CONSUMER_TIMEOUT_MS
            )
            await consumer.start()
            logger.info("✅ Kafka Consumer iniciado com sucesso!")
            
            # Verificar se o tópico existe
            topics = await consumer.topics()
            if INPUT_TOPIC not in topics:
                logger.warning(f"⚠️ Tópico {INPUT_TOPIC} não encontrado. Tentando criar...")
                # Tenta enviar uma mensagem para criar o tópico
                temp_producer = AIOKafkaProducer(
                    bootstrap_servers=KAFKA_BROKER,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8")
                )
                await temp_producer.start()
                await temp_producer.send_and_wait(
                    INPUT_TOPIC, 
                    {"event_type": "init", "message": "Inicializando tópico"}
                )
                await temp_producer.stop()
                logger.info(f"✅ Tópico {INPUT_TOPIC} criado com sucesso!")
            else:
                logger.info(f"✅ Tópico {INPUT_TOPIC} encontrado!")
                
            # Verificar se o tópico de resposta existe
            if OUTPUT_TOPIC not in topics:
                logger.warning(f"⚠️ Tópico {OUTPUT_TOPIC} não encontrado. Tentando criar...")
                # Tenta enviar uma mensagem para criar o tópico
                temp_producer = AIOKafkaProducer(
                    bootstrap_servers=KAFKA_BROKER,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8")
                )
                await temp_producer.start()
                await temp_producer.send_and_wait(
                    OUTPUT_TOPIC, 
                    {"event_type": "init", "message": "Inicializando tópico de resposta"}
                )
                await temp_producer.stop()
                logger.info(f"✅ Tópico {OUTPUT_TOPIC} criado com sucesso!")
            else:
                logger.info(f"✅ Tópico {OUTPUT_TOPIC} encontrado!")
                
            return consumer
        except Exception as e:
            logger.error(f"❌ Erro ao iniciar Kafka Consumer: {e}")
            retries += 1
            await asyncio.sleep(retry_delay)
    
    if consumer is None:
        logger.error("❌ Não foi possível iniciar o Kafka Consumer após várias tentativas.")
        return None

# ========================
# 📡 Processador Principal
# ========================
async def processar_evento(evento, pg_conn, mongo_db, redis_conn, producer):
    """Roteia eventos para o handler apropriado com base no tipo de evento."""
    tipo = evento.get("event_type")
    correlation_id = evento.get("correlation_id", "unknown")
    
    logger.info(f"🔄 Processando evento: {tipo} (correlation_id: {correlation_id})")
    
    try:
        # Tentar processar com o handler PostgreSQL primeiro
        if pg_conn is not None:
            result = await postgres_handler.processar_evento_postgres(pg_conn, evento, producer)
            if result is not None:
                return result
        
        # Se não foi processado pelo PostgreSQL, tentar com MongoDB
        if mongo_db is not None:
            result = await mongodb_handler.processar_evento_mongodb(mongo_db, evento, producer)
            if result is not None:
                return result
        
        # Se ainda não foi processado, tentar com Redis
        if redis_conn is not None:
            result = await redis_handler.processar_evento_redis(redis_conn, evento, producer)
            if result is not None:
                return result
        
        # Se nenhum handler processou o evento
        logger.warning(f"⚠️ Evento não reconhecido ou não suportado: {tipo}")
        
        # Enviar resposta de erro para eventos não reconhecidos
        error_response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": tipo,
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Tipo de evento não suportado: {tipo}"
        }
        
        await producer.send_and_wait(OUTPUT_TOPIC, error_response)
        return False
        
    except Exception as e:
        logger.error(f"❌ Erro ao processar evento {tipo}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Enviar resposta de erro genérica
        error_response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": tipo,
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao processar evento: {str(e)}"
        }
        
        await producer.send_and_wait(OUTPUT_TOPIC, error_response)
        return False

async def iniciar_processador():
    """Inicializa o serviço de processamento de eventos com todos os handlers"""
    # Esperando para os serviços estarem prontos
    logger.info("⏳ Aguardando serviços estarem disponíveis...")
    await asyncio.sleep(10)
    
    # Inicializar conexões com PostgreSQL
    logger.info("🔄 Inicializando conexão com PostgreSQL...")
    pg_conn = await postgres_handler.connect_to_postgres()
    
    # Inicializar conexões com MongoDB
    logger.info("🔄 Inicializando conexão com MongoDB...")
    mongo_client, mongo_db = await mongodb_handler.connect_to_mongodb()
    
    # Inicializar conexões com Redis
    logger.info("🔄 Inicializando conexão com Redis...")
    redis_conn = await redis_handler.connect_to_redis()
    
    # Verificar se pelo menos um banco de dados está disponível
    if pg_conn is None and redis_conn is None and mongo_db is None:
        logger.error("❌ Nenhum banco de dados disponível. Encerrando serviço.")
        return
    
    # Configurar databases disponíveis
    if pg_conn is not None:
        # Inicializar tabelas PostgreSQL
        await postgres_handler.criar_tabelas_postgres(pg_conn)
    
    if redis_conn is not None:
        # Inicializar Redis
        await redis_handler.inicializar_redis(redis_conn)
        
    if mongo_db is not None:
        # Inicializar MongoDB
        await mongodb_handler.inicializar_mongodb(mongo_db)
    
    # Inicialização do produtor Kafka
    producer = await create_kafka_producer()
    if producer is None:
        logger.error("❌ Não foi possível iniciar o produtor Kafka. Encerrando serviço.")
        await cleanup_connections(pg_conn, mongo_client, redis_conn)
        return
    
    # Inicialização do consumidor Kafka
    consumer = await create_kafka_consumer()
    if consumer is None:
        logger.error("❌ Não foi possível iniciar o consumidor Kafka. Encerrando serviço.")
        await producer.stop()
        await cleanup_connections(pg_conn, mongo_client, redis_conn)
        return
    
    # Loop principal de processamento de eventos
    try:
        logger.info("📡 Aguardando mensagens...")
        async for msg in consumer:
            evento = msg.value
            logger.info(f"📩 Evento recebido: {evento.get('event_type')} (correlation_id: {evento.get('correlation_id', 'N/A')})")
            
            try:
                await processar_evento(evento, pg_conn, mongo_db, redis_conn, producer)
            except Exception as e:
                logger.error(f"❌ Erro ao processar evento: {e}")
                import traceback
                logger.error(traceback.format_exc())
    except Exception as e:
        logger.error(f"❌ Erro no loop principal: {e}")
    finally:
        logger.info("🛑 Encerrando conexões...")
        await consumer.stop()
        await producer.stop()
        await cleanup_connections(pg_conn, mongo_client, redis_conn)

async def cleanup_connections(pg_conn, mongo_client, redis_conn):
    """Fecha todas as conexões com bancos de dados"""
    if pg_conn is not None:
        try:
            await pg_conn.close()
            logger.info("✅ Conexão PostgreSQL encerrada")
        except Exception as e:
            logger.error(f"❌ Erro ao fechar conexão PostgreSQL: {e}")
    
    if mongo_client is not None:
        try:
            mongo_client.close()
            logger.info("✅ Conexão MongoDB encerrada")
        except Exception as e:
            logger.error(f"❌ Erro ao fechar conexão MongoDB: {e}")
    
    if redis_conn is not None:
        try:
            await redis_conn.close()
            logger.info("✅ Conexão Redis encerrada")
        except Exception as e:
            logger.error(f"❌ Erro ao fechar conexão Redis: {e}")

# ========================
# 🛡️ API de Health Check
# ========================
async def health_check_api():
    """Endpoint de saúde simples para verificar se o serviço está funcionando."""
    async def health_handler(request):
        # Verificar conexões aqui, se necessário
        return web.json_response({
            "service": "s2",
            "status": "healthy",
            "components": {
                "postgres": os.getenv("POSTGRES_HOST", "postgres"),
                "mongodb": os.getenv("MONGO_HOST", "mongo"),
                "redis": os.getenv("REDIS_HOST", "redis"),
                "kafka": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
            },
            "timestamp": datetime.utcnow().isoformat()
        })
    
    app = web.Application()
    app.router.add_get('/health', health_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8000)
    await site.start()
    logger.info("✅ API de health check iniciada na porta 8000")
    
    return runner

# ========================
# 🚀 Ponto de Entrada
# ========================
async def main():
    try:
        logger.info("🚀 Iniciando serviço S2 de processamento com persistência poliglota...")
        
        # Iniciar API de health check em uma task separada
        health_api_runner = await health_check_api()
        
        # Iniciar o processador
        await iniciar_processador()
        
        # Cleanup
        await health_api_runner.cleanup()
        
    except KeyboardInterrupt:
        logger.info("👋 Serviço interrompido pelo usuário")
    except Exception as e:
        logger.error(f"❌ Erro fatal: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Serviço interrompido pelo usuário")
    except Exception as e:
        logger.error(f"❌ Erro fatal: {e}")
        sys.exit(1)