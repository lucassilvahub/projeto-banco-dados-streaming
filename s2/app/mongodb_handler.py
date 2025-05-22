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
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
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
            logger.info(
                f"🔌 Tentando conectar ao MongoDB em {MONGO_URI} ({retries+1}/{max_retries})..."
            )
            client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
            # Realizar uma operação para verificar se a conexão está funcionando
            await client.admin.command("ping")

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


async def validar_usuario_existe(pg_conn, user_id):
    """Valida se o usuário existe no PostgreSQL antes de executar operação no MongoDB"""
    try:
        if pg_conn is None:
            logger.warning("⚠️ Conexão PostgreSQL não disponível para validação")
            return False

        user = await pg_conn.fetchrow(
            "SELECT user_id FROM usuarios WHERE user_id = $1", user_id
        )
        if user:
            logger.info(f"✅ Usuário {user_id} encontrado no PostgreSQL")
            return True
        else:
            logger.warning(f"⚠️ Usuário {user_id} não encontrado no PostgreSQL")
            return False
    except Exception as e:
        logger.error(f"❌ Erro ao validar usuário {user_id}: {e}")
        return False


async def processar_evento_mongodb(db, evento, producer, pg_conn=None):
    """Processa eventos específicos do MongoDB com validação de usuário no PostgreSQL"""
    if db is None:
        logger.warning("⚠️ Conexão MongoDB não disponível. Ignorando evento.")
        return None

    tipo = evento.get("event_type")
    correlation_id = evento.get("correlation_id", "unknown")

    try:
        if tipo == "registrar_visualizacao":
            return await processar_registrar_visualizacao(db, evento, producer, pg_conn)
        elif tipo == "gerar_recomendacoes":
            return await processar_gerar_recomendacoes(db, evento, producer, pg_conn)
        elif tipo == "obter_historico_visualizacao":
            return await processar_obter_historico(db, evento, producer, pg_conn)
        elif tipo == "obter_recomendacoes":
            return await processar_obter_recomendacoes(db, evento, producer, pg_conn)

        # Retorna None se o evento não for para o MongoDB
        return None
    except Exception as e:
        logger.error(f"❌ Erro ao processar evento {tipo} no MongoDB: {e}")
        logger.error(traceback.format_exc())

        # Enviar resposta de erro
        error_response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": tipo,
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao processar evento no MongoDB: {str(e)}",
        }

        await producer.send_and_wait(OUTPUT_TOPIC, error_response)
        return False


async def processar_registrar_visualizacao(db, evento, producer, pg_conn):
    """Processa o evento de registrar visualização no MongoDB"""
    user_id = evento.get("user_id")
    conteudo_id = evento.get("conteudo_id")
    titulo = evento.get("titulo")
    tipo = evento.get("tipo", "filme")
    tempo_assistido = evento.get("tempo_assistido", 0)
    posicao = evento.get("posicao", 0)
    correlation_id = evento.get("correlation_id")

    try:
        # Validar se o usuário existe no PostgreSQL
        if not await validar_usuario_existe(pg_conn, user_id):
            raise ValueError(f"Usuário {user_id} não encontrado no sistema")

        # Dados da visualização
        visualizacao = {
            "user_id": user_id,
            "conteudo_id": conteudo_id,
            "titulo": titulo,
            "tipo": tipo,
            "tempo_assistido": tempo_assistido,
            "posicao": posicao,
            "data": datetime.utcnow(),
            "concluido": posicao >= 90,  # Considera concluído se assistiu mais de 90%
        }

        # Inserir no MongoDB
        resultado = await db.historico_visualizacao.insert_one(visualizacao)

        logger.info(f"✅ Visualização registrada para usuário {user_id}")

        # Enviar resposta de sucesso
        response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "registrar_visualizacao",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "success",
            "message": f"Visualização registrada com sucesso",
            "user_id": user_id,
            "conteudo_id": conteudo_id,
            "id_documento": str(resultado.inserted_id),
        }

        await producer.send_and_wait(OUTPUT_TOPIC, response)
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao registrar visualização: {e}")

        # Enviar resposta de erro
        error_response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "registrar_visualizacao",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao registrar visualização: {str(e)}",
        }

        await producer.send_and_wait(OUTPUT_TOPIC, error_response)
        return False


async def processar_gerar_recomendacoes(db, evento, producer, pg_conn):
    """Processa o evento de gerar recomendações no MongoDB"""
    user_id = evento.get("user_id")
    correlation_id = evento.get("correlation_id")

    try:
        # Validar se o usuário existe no PostgreSQL
        if not await validar_usuario_existe(pg_conn, user_id):
            raise ValueError(f"Usuário {user_id} não encontrado no sistema")

        # Buscar os tipos de conteúdo que o usuário mais assiste
        pipeline = [
            {"$match": {"user_id": user_id}},
            {"$group": {"_id": "$tipo", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]

        tipos_preferidos = []
        async for tipo in db.historico_visualizacao.aggregate(pipeline):
            tipos_preferidos.append(tipo)

        # Gerar recomendações baseadas nas preferências
        recomendacoes = []
        categorias = [
            "Ação",
            "Comédia",
            "Drama",
            "Suspense",
            "Ficção Científica",
            "Romance",
            "Documentário",
        ]

        import random
        from faker import Faker

        fake = Faker("pt_BR")

        for tipo in tipos_preferidos:
            tipo_conteudo = tipo["_id"]
            quantidade = min(
                tipo["count"] * 2, 10
            )  # 2x mais recomendações do que o histórico, até 10

            for _ in range(quantidade):
                recomendacao = {
                    "conteudo_id": random.randint(1000, 9999),
                    "titulo": fake.sentence(nb_words=4),
                    "tipo": tipo_conteudo,
                    "categoria": random.choice(categorias),
                    "pontuacao": round(
                        random.uniform(0.5, 1.0), 2
                    ),  # Score de 0.5 a 1.0
                    "motivo": f"Baseado nos seus interesses em {tipo_conteudo}s",
                }
                recomendacoes.append(recomendacao)

        # Se não houver histórico, gerar algumas recomendações aleatórias
        if not recomendacoes:
            for _ in range(5):
                tipo_conteudo = random.choice(["filme", "série", "documentário"])
                recomendacao = {
                    "conteudo_id": random.randint(1000, 9999),
                    "titulo": fake.sentence(nb_words=4),
                    "tipo": tipo_conteudo,
                    "categoria": random.choice(categorias),
                    "pontuacao": round(
                        random.uniform(0.5, 0.8), 2
                    ),  # Score menor para recomendações aleatórias
                    "motivo": "Recomendação baseada em tendências populares",
                }
                recomendacoes.append(recomendacao)

        # Salvar recomendações no MongoDB
        recomendacao_doc = {
            "user_id": user_id,
            "data_geracao": datetime.utcnow(),
            "itens": recomendacoes,
        }

        # Remover recomendações anteriores e inserir novas
        await db.recomendacoes.delete_many({"user_id": user_id})
        resultado = await db.recomendacoes.insert_one(recomendacao_doc)

        logger.info(f"✅ Recomendações geradas para usuário {user_id}")

        # Enviar resposta de sucesso
        response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "gerar_recomendacoes",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "success",
            "message": f"Recomendações geradas com sucesso",
            "user_id": user_id,
            "total_recomendacoes": len(recomendacoes),
            "id_documento": str(resultado.inserted_id),
        }

        await producer.send_and_wait(OUTPUT_TOPIC, response)
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao gerar recomendações: {e}")

        # Enviar resposta de erro
        error_response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "gerar_recomendacoes",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao gerar recomendações: {str(e)}",
        }

        await producer.send_and_wait(OUTPUT_TOPIC, error_response)
        return False


async def processar_obter_historico(db, evento, producer, pg_conn):
    """Processa o evento de obter histórico de visualização no MongoDB"""
    user_id = evento.get("user_id")
    tipo = evento.get("tipo")
    concluido = evento.get("concluido")
    limit = evento.get("limit", 10)
    correlation_id = evento.get("correlation_id")

    try:
        # Validar se o usuário existe no PostgreSQL
        if not await validar_usuario_existe(pg_conn, user_id):
            raise ValueError(f"Usuário {user_id} não encontrado no sistema")

        # Construir query com filtros
        query = {"user_id": user_id}

        if tipo:
            query["tipo"] = tipo

        if concluido is not None:
            query["concluido"] = concluido

        # Buscar no MongoDB com ordenação por data decrescente
        cursor = db.historico_visualizacao.find(query).sort("data", -1).limit(limit)
        resultados = []

        async for item in cursor:
            item["_id"] = str(item["_id"])
            item["data"] = item["data"].isoformat()
            resultados.append(item)

        logger.info(
            f"✅ Histórico recuperado para usuário {user_id} - {len(resultados)} itens"
        )

        # Enviar resposta de sucesso
        response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "obter_historico_visualizacao",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "success",
            "message": f"Histórico recuperado com sucesso",
            "user_id": user_id,
            "total": len(resultados),
            "historico": resultados,
        }

        await producer.send_and_wait(OUTPUT_TOPIC, response)
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao obter histórico: {e}")

        # Enviar resposta de erro
        error_response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "obter_historico_visualizacao",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao obter histórico: {str(e)}",
        }

        await producer.send_and_wait(OUTPUT_TOPIC, error_response)
        return False


async def processar_obter_recomendacoes(db, evento, producer, pg_conn):
    """Processa o evento de obter recomendações no MongoDB"""
    user_id = evento.get("user_id")
    correlation_id = evento.get("correlation_id")

    try:
        # Validar se o usuário existe no PostgreSQL
        if not await validar_usuario_existe(pg_conn, user_id):
            raise ValueError(f"Usuário {user_id} não encontrado no sistema")

        # Buscar recomendações
        recomendacao = await db.recomendacoes.find_one({"user_id": user_id})

        if not recomendacao:
            # Enviar resposta indicando que não há recomendações
            response = {
                "correlation_id": correlation_id,
                "event_type": "response",
                "original_event_type": "obter_recomendacoes",
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success",
                "message": "Não há recomendações para este usuário",
                "user_id": user_id,
                "recomendacoes": None,
            }
        else:
            # Converter ObjectId e data para string para serialização JSON
            recomendacao["_id"] = str(recomendacao["_id"])
            recomendacao["data_geracao"] = recomendacao["data_geracao"].isoformat()

            response = {
                "correlation_id": correlation_id,
                "event_type": "response",
                "original_event_type": "obter_recomendacoes",
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success",
                "message": "Recomendações recuperadas com sucesso",
                "user_id": user_id,
                "recomendacoes": recomendacao,
            }

        logger.info(f"✅ Recomendações recuperadas para usuário {user_id}")
        await producer.send_and_wait(OUTPUT_TOPIC, response)
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao obter recomendações: {e}")

        # Enviar resposta de erro
        error_response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "obter_recomendacoes",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao obter recomendações: {str(e)}",
        }

        await producer.send_and_wait(OUTPUT_TOPIC, error_response)
        return False