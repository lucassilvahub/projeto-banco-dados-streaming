import asyncio
import json
from aiokafka import AIOKafkaConsumer
import logging
from elasticsearch import AsyncElasticsearch
from datetime import datetime
import os
import sys
from collections import defaultdict

# ========================
# 🛠️ Configurações
# ========================
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPICOS = ["user_events", "response_events"]
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "elasticsearch:9200")
KAFKA_CONSUMER_TIMEOUT_MS = int(os.getenv("KAFKA_CONSUMER_TIMEOUT_MS", "30000"))

# ========================
# 📥 Configuração de Logs
# ========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("s3")

# ========================
# 📊 Métricas
# ========================
class MetricsCollector:
    def __init__(self):
        self.event_counts = defaultdict(int)
        self.response_times = defaultdict(list)
        self.correlation_map = {}
        self.success_count = 0
        self.error_count = 0
        self.start_time = datetime.utcnow()
        self.last_report_time = self.start_time
        
    def record_event(self, evento, topic):
        """Registra um evento nas métricas"""
        event_type = evento.get("event_type", "unknown")
        self.event_counts[f"{topic}/{event_type}"] += 1
        
        # Se for um evento de requisição, registre o timestamp
        if topic == "user_events":
            correlation_id = evento.get("correlation_id")
            if correlation_id:
                self.correlation_map[correlation_id] = {
                    "request_time": datetime.utcnow(),
                    "event_type": event_type,
                    "source": evento.get("source", "unknown"),
                    "target": evento.get("target", "unknown"),
                    "user_id": evento.get("user_id", "N/A"),
                    "status": "pending"
                }
        
        # Se for um evento de resposta, calcule o tempo de resposta
        if topic == "response_events":
            correlation_id = evento.get("correlation_id")
            if correlation_id and correlation_id in self.correlation_map:
                request_time = self.correlation_map[correlation_id]["request_time"]
                response_time = datetime.utcnow()
                processing_time = (response_time - request_time).total_seconds()
                original_event_type = evento.get("original_event_type", "unknown")
                
                self.response_times[original_event_type].append(processing_time)
                
                # Atualizar status na correlação
                self.correlation_map[correlation_id]["status"] = evento.get("status", "unknown")
                self.correlation_map[correlation_id]["processing_time"] = processing_time
                
                # Contabilizar sucesso ou erro
                if evento.get("status") == "success":
                    self.success_count += 1
                else:
                    self.error_count += 1
    
    def get_metrics_summary(self):
        """Retorna um resumo das métricas coletadas"""
        current_time = datetime.utcnow()
        uptime = (current_time - self.start_time).total_seconds()
        time_since_last_report = (current_time - self.last_report_time).total_seconds()
        self.last_report_time = current_time
        
        # Calcular tempos médios de resposta
        avg_response_times = {}
        for event_type, times in self.response_times.items():
            if times:
                avg_response_times[event_type] = sum(times) / len(times)
        
        total_events = sum(self.event_counts.values())
        events_per_second = total_events / uptime if uptime > 0 else 0
        events_since_last_report = total_events / time_since_last_report if time_since_last_report > 0 else 0
        
        # Limpar eventos antigos para evitar vazamento de memória
        current_time = datetime.utcnow()
        keys_to_remove = []
        for correlation_id, data in self.correlation_map.items():
            if "request_time" in data:
                age = (current_time - data["request_time"]).total_seconds()
                if age > 3600:  # Remover entradas com mais de 1 hora
                    keys_to_remove.append(correlation_id)
        
        for key in keys_to_remove:
            del self.correlation_map[key]
        
        return {
            "uptime_seconds": uptime,
            "total_events": total_events,
            "events_per_second": events_per_second,
            "events_since_last_report": events_since_last_report,
            "event_counts": dict(self.event_counts),
            "avg_response_times": avg_response_times,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "success_rate": (self.success_count / (self.success_count + self.error_count)) * 100 if (self.success_count + self.error_count) > 0 else 0,
            "pending_correlations": len([corr for corr, data in self.correlation_map.items() if data.get("status") == "pending"]),
            "timestamp": current_time.isoformat()
        }

# Instanciar o coletor de métricas
metrics = MetricsCollector()

async def log_evento(es, evento, origem):
    """Registra o evento no Elasticsearch"""
    # Enriquecer o evento com informações adicionais
    correlation_id = evento.get("correlation_id", "N/A")
    event_type = evento.get("event_type", "N/A")
    
    # Formatar as datas para compatibilidade com Elasticsearch
    current_time = datetime.utcnow().isoformat()
    
    # Criar uma entrada de log mais rica
    log_entry = {
        "timestamp": current_time,
        "topic": origem,
        "correlation_id": correlation_id,
        "event_type": event_type,
        "source": evento.get("source", "N/A"),
        "target": evento.get("target", "N/A"),
        "user_id": evento.get("user_id", "N/A"),
        "status": evento.get("status", "N/A") if origem == "response_events" else "pending",
        "details": {
            "original_event_type": evento.get("original_event_type", "N/A") if origem == "response_events" else "N/A",
            "message": evento.get("message", "N/A") if origem == "response_events" else "N/A",
            "processing_environment": "production",
            "service_name": "streaming_platform",
            "kafka_topic": origem
        }
    }
    
    # Registrar o evento nas métricas
    metrics.record_event(evento, origem)
    
    # Enviar para o Elasticsearch
    if es:
        try:
            # Registra o log diretamente, sem incluir o evento completo para evitar estruturas aninhadas complexas
            result = await es.index(index="system_logs", document=log_entry)
            logger.info(f"📝 Evento registrado no Elasticsearch: {event_type} (correlation_id: {correlation_id})")
            return True
        except Exception as e:
            logger.error(f"❌ Erro ao registrar no Elasticsearch: {e}")
            return False
    else:
        logger.warning(f"⚠️ Elasticsearch não disponível para registrar evento: {event_type}")
        return False

async def verificar_correlacao(evento_resposta, es):
    """Verifica e registra a correlação entre request e response"""
    if evento_resposta.get("event_type") == "response" and "correlation_id" in evento_resposta:
        correlation_id = evento_resposta["correlation_id"]
        original_type = evento_resposta.get("original_event_type", "desconhecido")
        status = evento_resposta.get("status", "desconhecido")
        
        current_time = datetime.utcnow().isoformat()
        
        # Calcular métricas de tempo se possível
        processing_time = None
        request_data = {}
        
        if correlation_id in metrics.correlation_map:
            request_data = metrics.correlation_map[correlation_id]
            processing_time = request_data.get("processing_time", None)
            
            logger.info(f"🔄 Ciclo completo para correlation_id: {correlation_id}")
            logger.info(f"   Evento original: {original_type}")
            logger.info(f"   Status: {status}")
            
            if processing_time is not None:
                logger.info(f"   Tempo de processamento: {processing_time}s")
        
        # Registrar informações de correlação no Elasticsearch mesmo se não tivermos o tempo de processamento
        if es:
            correlation_entry = {
                "timestamp": current_time,
                "correlation_id": correlation_id,
                "event_type": "correlation",
                "original_event_type": original_type,
                "status": status,
                "processing_time": processing_time if processing_time is not None else 0.0,
                "request_source": request_data.get("source", "unknown") if request_data else "unknown",
                "request_target": request_data.get("target", "unknown") if request_data else "unknown",
                "user_id": request_data.get("user_id", "N/A") if request_data else "N/A",
                "message": evento_resposta.get("message", "N/A")
            }
            
            try:
                result = await es.index(index="correlations", document=correlation_entry)
                logger.info(f"📊 Correlação registrada no Elasticsearch: {correlation_id}")
            except Exception as e:
                logger.error(f"❌ Erro ao registrar correlação no Elasticsearch: {e}")

async def gerar_relatorio_metricas(es):
    """Gera relatórios periódicos de métricas"""
    while True:
        await asyncio.sleep(120)  # Gerar relatório a cada 2 minutos
        
        metrics_summary = metrics.get_metrics_summary()
        logger.info(f"📊 Relatório de Métricas:")
        logger.info(f"   Total de eventos: {metrics_summary['total_events']}")
        logger.info(f"   Taxa de eventos: {metrics_summary['events_per_second']:.2f}/s")
        logger.info(f"   Taxa de sucesso: {metrics_summary['success_rate']:.2f}%")
        logger.info(f"   Correlações pendentes: {metrics_summary['pending_correlations']}")
        
        # Registrar métricas no Elasticsearch
        if es:
            try:
                # O problema é que já temos um índice com event_counts e avg_response_times definidos como objetos,
                # mas estamos tentando enviar strings. Vamos remover esses campos completamente.
                clean_summary = {
                    "timestamp": metrics_summary["timestamp"],
                    "uptime_seconds": metrics_summary["uptime_seconds"],
                    "total_events": metrics_summary["total_events"],
                    "events_per_second": metrics_summary["events_per_second"],
                    "events_since_last_report": metrics_summary["events_since_last_report"],
                    "success_count": metrics_summary["success_count"],
                    "error_count": metrics_summary["error_count"],
                    "success_rate": metrics_summary["success_rate"],
                    "pending_correlations": metrics_summary["pending_correlations"]
                }
                
                # Também vamos adicionar alguns contadores individuais em campos específicos
                # para poder visualizar melhor os tipos de eventos mais comuns
                for event_type, count in metrics_summary.get("event_counts", {}).items():
                    # Sanitizar o nome do evento para ser um nome de campo válido
                    safe_name = event_type.replace("/", "_").replace(":", "_").replace("-", "_")
                    clean_summary[f"event_{safe_name}"] = count
                
                # Para médias de tempo de resposta, também adicione como campos específicos
                for event_type, avg_time in metrics_summary.get("avg_response_times", {}).items():
                    # Sanitizar o nome do evento para ser um nome de campo válido
                    safe_name = event_type.replace("/", "_").replace(":", "_").replace("-", "_")
                    clean_summary[f"avg_time_{safe_name}"] = avg_time
                
                result = await es.index(index="system_metrics", document=clean_summary)
                logger.info(f"📊 Métricas registradas no Elasticsearch")
            except Exception as e:
                logger.error(f"❌ Erro ao registrar métricas no Elasticsearch: {e}")

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
                
                # Verificar se os índices existem, se não, criá-los
                indices_to_create = {
                    "system_logs": {
                        "mappings": {
                            "properties": {
                                "timestamp": {"type": "date"},
                                "correlation_id": {"type": "keyword"},
                                "event_type": {"type": "keyword"},
                                "source": {"type": "keyword"},
                                "target": {"type": "keyword"},
                                "user_id": {"type": "keyword"},
                                "topic": {"type": "keyword"},
                                "status": {"type": "keyword"},
                                "details": {"type": "object", "enabled": True}
                            }
                        }
                    },
                    "correlations": {
                        "mappings": {
                            "properties": {
                                "timestamp": {"type": "date"},
                                "correlation_id": {"type": "keyword"},
                                "event_type": {"type": "keyword"},
                                "original_event_type": {"type": "keyword"},
                                "status": {"type": "keyword"},
                                "processing_time": {"type": "float"},
                                "request_source": {"type": "keyword"},
                                "request_target": {"type": "keyword"},
                                "user_id": {"type": "keyword"},
                                "message": {"type": "text"}
                            }
                        }
                    },
                    "system_metrics": {
                        "mappings": {
                            "properties": {
                                "timestamp": {"type": "date"},
                                "uptime_seconds": {"type": "float"},
                                "total_events": {"type": "long"},
                                "events_per_second": {"type": "float"},
                                "events_since_last_report": {"type": "float"},
                                "success_count": {"type": "long"},
                                "error_count": {"type": "long"},
                                "success_rate": {"type": "float"},
                                "pending_correlations": {"type": "long"}
                                # Campos dinâmicos serão criados automaticamente para contadores específicos
                            }
                        }
                    }
                }
                
                for index_name, index_config in indices_to_create.items():
                    if not await es.indices.exists(index=index_name):
                        logger.info(f"🛠️ Criando índice {index_name}...")
                        try:
                            await es.indices.create(
                                index=index_name,
                                mappings=index_config["mappings"]
                            )
                            logger.info(f"✅ Índice {index_name} criado com sucesso")
                        except Exception as e:
                            logger.error(f"❌ Erro ao criar índice {index_name}: {e}")
                            # Se o índice já existir, continue
                            if "resource_already_exists_exception" not in str(e).lower():
                                raise
                return es
            else:
                logger.warning("⚠️ Elasticsearch respondeu, mas a conexão falhou.")
                retries += 1
                await asyncio.sleep(retry_delay)
        except Exception as e:
            logger.error(f"❌ Erro ao conectar ao Elasticsearch: {e}")
            retries += 1
            await asyncio.sleep(retry_delay)
    
    logger.warning("⚠️ Não foi possível conectar ao Elasticsearch.")
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
    await asyncio.sleep(20)  # Aguarda um pouco antes de tentar conectar
    
    # Tentar conectar ao Elasticsearch
    es = await connect_to_elasticsearch()
    
    if not es:
        logger.warning("⚠️ Elasticsearch não disponível, os logs não serão armazenados")
    
    # Iniciar relatórios de métricas em uma task separada
    metrics_task = asyncio.create_task(gerar_relatorio_metricas(es))

    # Iniciar consumer Kafka
    consumer = await create_kafka_consumer()
    if not consumer:
        logger.error("❌ Não foi possível iniciar o monitoramento sem conexão com o Kafka.")
        if es:
            await es.close()
        metrics_task.cancel()
        return False
    
    try:
        logger.info(f"📡 Monitorando tópicos: {', '.join(TOPICOS)}")
        async for msg in consumer:
            evento = msg.value
            topico = msg.topic
            
            # Registrar o evento no Elasticsearch
            await log_evento(es, evento, topico)
            
            # Verificar correlação entre request e response
            if topico == "response_events":
                await verificar_correlacao(evento, es)
    except Exception as e:
        logger.error(f"❌ Erro ao processar mensagens: {e}")
        return False
    finally:
        logger.info("🛑 Fechando conexões...")
        if 'consumer' in locals():
            await consumer.stop()
        if es:
            await es.close()
        metrics_task.cancel()
    
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
        logger.warning(f"⚠️ Tentativa {retry_count}/{max_retries} falhou. Tentando novamente em 5 segundos...")
        await asyncio.sleep(5)
    
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