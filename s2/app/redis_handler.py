import asyncio
import aioredis
from datetime import datetime, timedelta
import logging
import os
import json
import traceback

# ========================
# 🛠️ Configurações
# ========================
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}"

# ========================
# 📥 Configuração de Logs
# ========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("s2_redis")

# ========================
# 📡 Funções de Redis
# ========================
async def connect_to_redis(max_retries=15, retry_delay=5):
    """Conecta ao Redis com várias tentativas."""
    redis = None
    retries = 0
    
    while redis is None and retries < max_retries:
        try:
            logger.info(f"🔌 Tentando conectar ao Redis em {REDIS_URL} ({retries+1}/{max_retries})...")
            redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
            # Verificar conexão
            await redis.ping()
            logger.info("✅ Conexão com o Redis bem-sucedida!")
            return redis
        except Exception as e:
            logger.error(f"❌ Erro ao conectar ao Redis: {e}")
            retries += 1
            await asyncio.sleep(retry_delay)
    
    if redis is None:
        logger.error("❌ Não foi possível conectar ao Redis após várias tentativas.")
        return None

async def inicializar_redis(redis):
    """Configura chaves e estruturas iniciais necessárias."""
    try:
        logger.info("🏗️ Verificando configurações iniciais do Redis...")
        
        # Verificar se o Redis está vazio e configurar algumas chaves globais
        counter = await redis.exists("app:config:initialized")
        
        if not counter:
            # Primeira inicialização, criar chaves de configuração
            logger.info("⚙️ Inicializando configurações do Redis...")
            
            # Adicionar timestamp de inicialização
            await redis.set("app:config:initialized", datetime.utcnow().isoformat())
            
            # Counters para estatísticas
            await redis.set("app:stats:sessoes_ativas", 0)
            await redis.set("app:stats:usuarios_online", 0)
            await redis.set("app:stats:cache_hits", 0)
            
            logger.info("✅ Configurações iniciais do Redis criadas")
        else:
            # Incrementar contador de inicializações
            await redis.incr("app:stats:reinicializacoes")
            logger.info("✅ Redis já inicializado anteriormente")
            
    except Exception as e:
        logger.error(f"❌ Erro ao inicializar configurações do Redis: {e}")
        logger.error(traceback.format_exc())

async def processar_evento_redis(redis, evento, producer):
    """Processa eventos específicos do Redis"""
    tipo = evento.get("event_type")
    correlation_id = evento.get("correlation_id", "unknown")
    
    # Redis não está sendo utilizado para eventos via Kafka nesta implementação
    # Esta função está aqui para futura expansão
    return None