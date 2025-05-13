import asyncio
import json
from aiokafka import AIOKafkaConsumer
import logging
from elasticsearch import AsyncElasticsearch
from datetime import datetime
import os
import sys
import time
import re

# ========================
# 🛠️ Configurações
# ========================
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPICOS = ["user_events", "response_events"]
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "elasticsearch:9200")
LOG_FILE = "api_logs.json"  # Arquivo de fallback renomeado
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
    """Registra o evento no Elasticsearch com índices dinâmicos baseados no evento"""
    
    # Determinar qual índice usar com base no tipo de evento e origem
    event_type = evento.get("event_type", "unknown")
    transaction_id = evento.get("correlation_id", "N/A")
    
    # Função para obter o nome do índice dinamicamente
    def get_index_name():
        # Para eventos de resposta, extrair o tipo original
        if origem == "response_events" and evento.get("original_event_type"):
            # Usamos o tipo original para categorizar as respostas
            base_type = evento.get("original_event_type", "unknown")
            return f"api_{base_type}_responses"
        
        # Para eventos de requisição, usar o event_type diretamente
        elif origem == "user_events":
            # Extrair o nome da "entidade" do event_type - normalmente o primeiro substantivo
            # Por exemplo: "criar_usuario" -> "usuario", "atualizar_assinatura" -> "assinatura"
            if '_' in event_type:
                # Tenta extrair o nome da entidade após o verbo
                entity = event_type.split('_', 1)[1]  # Pega a segunda parte após o primeiro _
                # Se a entidade tiver outro verbo, pega apenas o substantivo
                if '_' in entity:
                    entity = entity.split('_')[0]  # Pega o substantivo principal
                return f"api_{entity}"
            else:
                # Fallback para o tipo de evento completo
                return f"api_{event_type}"
        
        # Fallback para qualquer outro caso
        return "api_events"
    
    # Obter o nome do índice
    index_name = get_index_name()
    
    # Adequar o formato do nome do índice para Elasticsearch (lowercase, sem caracteres especiais)
    index_name = re.sub(r'[^a-z0-9_]', '', index_name.lower())
    
    # Garantir que o índice tenha o prefixo "api_"
    if not index_name.startswith("api_"):
        index_name = f"api_{index_name}"
    
    # Criar o objeto de log
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "transaction_id": transaction_id,
        "event_type": event_type,
        "original_event_type": evento.get("original_event_type", event_type),
        "source": evento.get("source", "N/A"),
        "target": evento.get("target", "N/A"),
        "status": evento.get("status", "N/A"),
        "message": evento.get("message", "N/A"),
        "topic": origem,
        "payload": evento  # Mantém o evento completo para referência
    }
    
    # Tenta enviar para o Elasticsearch
    if es:
        try:
            # Verificar se o índice existe, se não, criá-lo
            if not await es.indices.exists(index=index_name):
                await create_dynamic_index(es, index_name)
            
            await es.index(index=index_name, document=log_entry)
            logger.info(f"📝 API Log registrado no Elasticsearch ({index_name}): {event_type}")
            
            # Atualizar ou criar alias automaticamente
            await update_aliases(es, index_name)
            
            return
        except Exception as e:
            logger.error(f"❌ Erro ao registrar no Elasticsearch: {e}")
            # Continua para o fallback
    
    # Fallback: registra em arquivo local
    try:
        with open(LOG_FILE, "a") as log_file:
            log_file.write(json.dumps(log_entry) + "\n")
        logger.info(f"📝 API Log registrado em arquivo local: {event_type}")
    except Exception as e:
        logger.error(f"❌ Erro ao registrar em arquivo: {e}")

async def create_dynamic_index(es, index_name):
    """Cria um novo índice com mapeamento padronizado de forma dinâmica"""
    logger.info(f"🛠️ Criando dinamicamente o índice {index_name}...")
    
    # Mapeamento padrão para todos os índices de API
    mapping = {
        "properties": {
            "timestamp": {"type": "date"},
            "transaction_id": {"type": "keyword"},
            "event_type": {"type": "keyword"},
            "original_event_type": {"type": "keyword"},
            "source": {"type": "keyword"},
            "target": {"type": "keyword"},
            "status": {"type": "keyword"},
            "message": {"type": "text"},
            "topic": {"type": "keyword"},
            "payload": {"type": "object", "enabled": False}
        }
    }
    
    # Criar o índice
    await es.indices.create(index=index_name, mappings=mapping)
    logger.info(f"✅ Índice {index_name} criado com sucesso")

async def update_aliases(es, index_name):
    """Atualiza aliases baseado no nome do índice para facilitar pesquisas"""
    try:
        # Determinar em quais aliases esse índice deve ser incluído
        alias_actions = []
        
        # Todos os índices vão para o alias geral
        alias_actions.append({"add": {"index": index_name, "alias": "api_all_logs"}})
        
        # Índices de resposta vão para o alias de respostas
        if "responses" in index_name:
            alias_actions.append({"add": {"index": index_name, "alias": "api_responses"}})
        # Outros índices vão para o alias de requisições
        else:
            alias_actions.append({"add": {"index": index_name, "alias": "api_requests"}})
        
        # Extrair o tipo da entidade (ex: usuarios, assinaturas, etc)
        entity_match = re.match(r'api_([^_]+)', index_name)
        if entity_match:
            entity_type = entity_match.group(1)
            # Criar um alias para todos os índices dessa entidade
            entity_alias = f"api_{entity_type}_all"
            alias_actions.append({"add": {"index": index_name, "alias": entity_alias}})
        
        # Executar as ações de alias
        if alias_actions:
            await es.indices.update_aliases({"actions": alias_actions})
            
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar aliases: {e}")

async def verificar_correlacao(evento_resposta):
    """Verifica e registra a correlação entre request e response"""
    if evento_resposta.get("event_type") == "response" and "correlation_id" in evento_resposta:
        transaction_id = evento_resposta["correlation_id"] 
        original_type = evento_resposta.get("original_event_type", "desconhecido")
        status = evento_resposta.get("status", "desconhecido")
        
        logger.info(f"🔄 Ciclo completo para transaction_id: {transaction_id}")
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
                
                # Configurar os aliases principais se não existirem
                base_aliases = ["api_all_logs", "api_requests", "api_responses"]
                
                for alias in base_aliases:
                    # Verificar se o alias já existe
                    try:
                        alias_exists = await es.indices.exists_alias(name=alias)
                        if not alias_exists:
                            # Criar um índice template para permitir criação dinâmica de índices
                            await es.indices.put_index_template(
                                name=f"{alias}_template",
                                body={
                                    "index_patterns": ["api_*"],
                                    "template": {
                                        "mappings": {
                                            "properties": {
                                                "timestamp": {"type": "date"},
                                                "transaction_id": {"type": "keyword"},
                                                "event_type": {"type": "keyword"},
                                                "original_event_type": {"type": "keyword"},
                                                "source": {"type": "keyword"},
                                                "target": {"type": "keyword"},
                                                "status": {"type": "keyword"},
                                                "message": {"type": "text"},
                                                "topic": {"type": "keyword"},
                                                "payload": {"type": "object", "enabled": False}
                                            }
                                        }
                                    }
                                }
                            )
                            logger.info(f"✅ Template para índices api_* criado")
                    except Exception as e:
                        logger.error(f"❌ Erro ao verificar/criar alias {alias}: {e}")
                
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
    logger.info("🔄 Iniciando serviço de monitoramento de API S3...")
    
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
        logger.info(f"📡 Monitorando tópicos para logs de API: {', '.join(TOPICOS)}")
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