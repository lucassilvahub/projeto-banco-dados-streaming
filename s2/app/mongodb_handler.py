import asyncio
import motor.motor_asyncio
from datetime import datetime
import logging
import os
import json
from bson import ObjectId
import traceback

# ========================
# 🛠️ Configurações
# ========================
MONGO_HOST = os.getenv("MONGO_HOST", "mongo")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DB = os.getenv("MONGO_DB", "streaming_db")
MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}"
OUTPUT_TOPIC = "response_events"  # Definição local para evitar importação circular

# ========================
# 📥 Configuração de Logs
# ========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("s2_mongodb")

# Classe auxiliar para permitir JSON serialização de ObjectId
class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)

# ========================
# 📡 Funções de MongoDB
# ========================
async def connect_to_mongodb(max_retries=15, retry_delay=5):
    """Conecta ao MongoDB com várias tentativas."""
    client = None
    db = None
    retries = 0
    
    while client is None and retries < max_retries:
        try:
            logger.info(f"🔌 Tentando conectar ao MongoDB em {MONGO_URI} ({retries+1}/{max_retries})...")
            client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
            # Realizar uma operação para verificar se a conexão está funcionando
            await client.admin.command('ping')
            
            db = client[MONGO_DB]
            logger.info("✅ Conexão com o MongoDB bem-sucedida!")
            return client, db
        except Exception as e:
            logger.error(f"❌ Erro ao conectar ao MongoDB: {e}")
            retries += 1
            await asyncio.sleep(retry_delay)
    
    if client is None:
        logger.error("❌ Não foi possível conectar ao MongoDB após várias tentativas.")
        return None, None

async def inicializar_mongodb(db):
    """Configura as coleções iniciais e índices necessários."""
    try:
        logger.info("🏗️ Verificando/criando coleções e índices no MongoDB...")
        
        # Criar coleções se não existirem
        if "historico_visualizacao" not in await db.list_collection_names():
            await db.create_collection("historico_visualizacao")
            logger.info("✅ Coleção 'historico_visualizacao' criada")
        
        if "recomendacoes" not in await db.list_collection_names():
            await db.create_collection("recomendacoes")
            logger.info("✅ Coleção 'recomendacoes' criada")
        
        # Criar índices para melhorar performance de consultas
        # Índice para histórico de visualização
        await db.historico_visualizacao.create_index([("user_id", 1)])
        await db.historico_visualizacao.create_index([("user_id", 1), ("tipo", 1)])
        await db.historico_visualizacao.create_index([("data", -1)])
        
        # Índice para recomendações
        await db.recomendacoes.create_index([("user_id", 1)], unique=True)
        
        logger.info("✅ Índices MongoDB criados com sucesso")
    except Exception as e:
        logger.error(f"❌ Erro ao inicializar coleções e índices no MongoDB: {e}")
        logger.error(traceback.format_exc())

async def processar_evento_mongodb(db, evento, producer):
    """Processa eventos específicos do MongoDB"""
    tipo = evento.get("event_type")
    correlation_id = evento.get("correlation_id", "unknown")
    
    # MongoDB não está sendo utilizado para eventos via Kafka nesta implementação
    # Esta função está aqui para futura expansão
    return None