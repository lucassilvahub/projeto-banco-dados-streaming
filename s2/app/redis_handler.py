import asyncio
import redis.asyncio as aioredis  # Changed import to use redis.asyncio
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
OUTPUT_TOPIC = "response_events"  # Definição local para evitar importação circular

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
            redis = aioredis.from_url(REDIS_URL, decode_responses=True)
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

async def validar_usuario_existe(pg_conn, user_id):
    """Valida se o usuário existe no PostgreSQL antes de executar operação no Redis"""
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

async def processar_evento_redis(redis, evento, producer, pg_conn=None):
    """Processa eventos específicos do Redis com validação de usuário no PostgreSQL"""
    if redis is None:
        logger.warning("⚠️ Conexão Redis não disponível. Ignorando evento.")
        return None

    tipo = evento.get("event_type")
    correlation_id = evento.get("correlation_id", "unknown")

    try:
        if tipo == "criar_sessao":
            return await processar_criar_sessao(redis, evento, producer, pg_conn)
        elif tipo == "atualizar_atividade_sessao":
            return await processar_atualizar_atividade_sessao(redis, evento, producer)
        elif tipo == "encerrar_sessao":
            return await processar_encerrar_sessao(redis, evento, producer)
        elif tipo == "listar_sessoes_usuario":
            return await processar_listar_sessoes_usuario(redis, evento, producer, pg_conn)
        elif tipo == "cache_conteudo":
            return await processar_cache_conteudo(redis, evento, producer)
        elif tipo == "obter_cache_conteudo":
            return await processar_obter_cache_conteudo(redis, evento, producer)

        # Retorna None se o evento não for para o Redis
        return None
    except Exception as e:
        logger.error(f"❌ Erro ao processar evento {tipo} no Redis: {e}")
        logger.error(traceback.format_exc())

        # Enviar resposta de erro
        error_response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": tipo,
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao processar evento no Redis: {str(e)}",
        }

        await producer.send_and_wait(OUTPUT_TOPIC, error_response)
        return False

async def processar_criar_sessao(redis, evento, producer, pg_conn):
    """Processa o evento de criar sessão no Redis"""
    user_id = evento.get("user_id")
    dispositivo = evento.get("dispositivo", "desconhecido")
    localizacao = evento.get("localizacao", "desconhecida")
    correlation_id = evento.get("correlation_id")

    try:
        # Validar se o usuário existe no PostgreSQL
        if pg_conn and not await validar_usuario_existe(pg_conn, user_id):
            raise ValueError(f"Usuário {user_id} não encontrado no sistema")

        # Gerar session_id único
        import uuid
        session_id = str(uuid.uuid4())

        # Dados da sessão
        sessao_data = {
            "session_id": session_id,
            "user_id": user_id,
            "dispositivo": dispositivo,
            "localizacao": localizacao,
            "data_criacao": datetime.utcnow().isoformat(),
            "ultima_atividade": datetime.utcnow().isoformat(),
            "status": "ativa"
        }

        # Chaves Redis para armazenar a sessão
        session_key = f"sessao:{session_id}"
        user_sessions_key = f"usuario:{user_id}:sessoes"

        # Definir TTL de 24 horas para a sessão
        ttl_seconds = 24 * 60 * 60  # 24 horas

        # Armazenar dados da sessão como hash
        await redis.hset(session_key, mapping=sessao_data)
        await redis.expire(session_key, ttl_seconds)

        # Adicionar session_id à lista de sessões do usuário
        await redis.sadd(user_sessions_key, session_id)
        await redis.expire(user_sessions_key, ttl_seconds)

        # Incrementar contador de sessões ativas
        await redis.incr("app:stats:sessoes_ativas")
        
        # Adicionar usuário ao conjunto de usuários online
        await redis.sadd("app:usuarios_online", str(user_id))

        logger.info(f"✅ Sessão {session_id} criada para usuário {user_id}")

        # Enviar resposta de sucesso
        response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "criar_sessao",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "success",
            "message": "Sessão criada com sucesso",
            "user_id": user_id,
            "session_id": session_id,
            "dispositivo": dispositivo,
            "localizacao": localizacao
        }

        await producer.send_and_wait(OUTPUT_TOPIC, response)
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao criar sessão: {e}")

        # Enviar resposta de erro
        error_response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "criar_sessao",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao criar sessão: {str(e)}",
        }

        await producer.send_and_wait(OUTPUT_TOPIC, error_response)
        return False

async def processar_atualizar_atividade_sessao(redis, evento, producer):
    """Processa o evento de atualizar atividade da sessão no Redis"""
    session_id = evento.get("session_id")
    correlation_id = evento.get("correlation_id")

    try:
        session_key = f"sessao:{session_id}"

        # Verificar se a sessão existe
        session_exists = await redis.exists(session_key)
        if not session_exists:
            raise ValueError(f"Sessão {session_id} não encontrada")

        # Atualizar timestamp de última atividade
        await redis.hset(session_key, "ultima_atividade", datetime.utcnow().isoformat())

        # Renovar TTL da sessão
        ttl_seconds = 24 * 60 * 60  # 24 horas
        await redis.expire(session_key, ttl_seconds)

        logger.info(f"✅ Atividade da sessão {session_id} atualizada")

        # Enviar resposta de sucesso
        response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "atualizar_atividade_sessao",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "success",
            "message": "Atividade da sessão atualizada com sucesso",
            "session_id": session_id
        }

        await producer.send_and_wait(OUTPUT_TOPIC, response)
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar atividade da sessão: {e}")

        # Enviar resposta de erro
        error_response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "atualizar_atividade_sessao",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao atualizar atividade da sessão: {str(e)}",
        }

        await producer.send_and_wait(OUTPUT_TOPIC, error_response)
        return False

async def processar_encerrar_sessao(redis, evento, producer):
    """Processa o evento de encerrar sessão no Redis"""
    session_id = evento.get("session_id")
    correlation_id = evento.get("correlation_id")

    try:
        session_key = f"sessao:{session_id}"

        # Obter dados da sessão antes de deletar
        session_data = await redis.hgetall(session_key)
        if not session_data:
            raise ValueError(f"Sessão {session_id} não encontrada")

        user_id = session_data.get("user_id")
        user_sessions_key = f"usuario:{user_id}:sessoes"

        # Marcar sessão como encerrada antes de deletar
        await redis.hset(session_key, "status", "encerrada")
        await redis.hset(session_key, "data_encerramento", datetime.utcnow().isoformat())

        # Remover da lista de sessões ativas do usuário
        await redis.srem(user_sessions_key, session_id)

        # Verificar se o usuário ainda tem outras sessões ativas
        sessions_restantes = await redis.scard(user_sessions_key)
        if sessions_restantes == 0:
            # Remover usuário do conjunto de usuários online
            await redis.srem("app:usuarios_online", str(user_id))

        # Decrementar contador de sessões ativas
        await redis.decr("app:stats:sessoes_ativas")

        # Definir TTL curto para a sessão encerrada (para auditoria)
        await redis.expire(session_key, 3600)  # 1 hora

        logger.info(f"✅ Sessão {session_id} encerrada para usuário {user_id}")

        # Enviar resposta de sucesso
        response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "encerrar_sessao",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "success",
            "message": "Sessão encerrada com sucesso",
            "session_id": session_id,
            "user_id": user_id
        }

        await producer.send_and_wait(OUTPUT_TOPIC, response)
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao encerrar sessão: {e}")

        # Enviar resposta de erro
        error_response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "encerrar_sessao",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao encerrar sessão: {str(e)}",
        }

        await producer.send_and_wait(OUTPUT_TOPIC, error_response)
        return False

async def processar_listar_sessoes_usuario(redis, evento, producer, pg_conn):
    """Processa o evento de listar sessões de um usuário no Redis"""
    user_id = evento.get("user_id")
    ativas_apenas = evento.get("ativas_apenas", True)
    correlation_id = evento.get("correlation_id")

    try:
        # Validar se o usuário existe no PostgreSQL
        if pg_conn and not await validar_usuario_existe(pg_conn, user_id):
            raise ValueError(f"Usuário {user_id} não encontrado no sistema")

        user_sessions_key = f"usuario:{user_id}:sessoes"
        
        # Obter IDs das sessões do usuário
        session_ids = await redis.smembers(user_sessions_key)
        
        sessoes = []
        for session_id in session_ids:
            session_key = f"sessao:{session_id}"
            session_data = await redis.hgetall(session_key)
            
            if session_data:
                # Filtrar por status se ativas_apenas for True
                if ativas_apenas and session_data.get("status") != "ativa":
                    continue
                
                sessoes.append(session_data)

        logger.info(f"✅ {len(sessoes)} sessões encontradas para usuário {user_id}")

        # Enviar resposta de sucesso
        response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "listar_sessoes_usuario",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "success",
            "message": f"Sessões recuperadas com sucesso",
            "user_id": user_id,
            "total": len(sessoes),
            "sessoes": sessoes
        }

        await producer.send_and_wait(OUTPUT_TOPIC, response)
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao listar sessões: {e}")

        # Enviar resposta de erro
        error_response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "listar_sessoes_usuario",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao listar sessões: {str(e)}",
        }

        await producer.send_and_wait(OUTPUT_TOPIC, error_response)
        return False

async def processar_cache_conteudo(redis, evento, producer):
    """Processa o evento de cache de conteúdo no Redis"""
    conteudo_id = evento.get("conteudo_id")
    correlation_id = evento.get("correlation_id")

    try:
        # Gerar dados fake para o conteúdo
        from faker import Faker
        import random
        
        fake = Faker("pt_BR")
        
        categorias = ["Ação", "Comédia", "Drama", "Suspense", "Ficção Científica", "Romance", "Documentário"]
        tipos = ["filme", "série", "documentário"]
        
        conteudo_data = {
            "conteudo_id": conteudo_id,
            "titulo": fake.sentence(nb_words=4),
            "tipo": random.choice(tipos),
            "categoria": random.choice(categorias),
            "descricao": fake.text(max_nb_chars=200),
            "duracao_minutos": random.randint(45, 180),
            "ano_lancamento": random.randint(2015, 2024),
            "classificacao": random.choice(["Livre", "10", "12", "14", "16", "18"]),
            "avaliacao": round(random.uniform(3.0, 5.0), 1),
            "data_cache": datetime.utcnow().isoformat(),
            "visualizacoes": random.randint(100, 10000),
            "disponivel": 1
        }

        # Chave para armazenar o conteúdo
        content_key = f"conteudo:{conteudo_id}"
        
        # TTL de 1 hora para cache de conteúdo
        ttl_seconds = 60 * 60  # 1 hora

        # Armazenar dados do conteúdo como hash
        await redis.hset(content_key, mapping=conteudo_data)
        await redis.expire(content_key, ttl_seconds)

        # Adicionar à lista de conteúdos em cache
        await redis.sadd("conteudos_cached", str(conteudo_id))

        # Incrementar contador de cache hits
        await redis.incr("app:stats:cache_hits")

        logger.info(f"✅ Conteúdo {conteudo_id} armazenado em cache")

        # Enviar resposta de sucesso
        response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "cache_conteudo",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "success",
            "message": "Conteúdo armazenado em cache com sucesso",
            "conteudo_id": conteudo_id,
            "conteudo": conteudo_data
        }

        await producer.send_and_wait(OUTPUT_TOPIC, response)
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao armazenar conteúdo em cache: {e}")

        # Enviar resposta de erro
        error_response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "cache_conteudo",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao armazenar conteúdo em cache: {str(e)}",
        }

        await producer.send_and_wait(OUTPUT_TOPIC, error_response)
        return False

async def processar_obter_cache_conteudo(redis, evento, producer):
    """Processa o evento de obter conteúdo do cache no Redis"""
    conteudo_id = evento.get("conteudo_id")
    correlation_id = evento.get("correlation_id")

    try:
        content_key = f"conteudo:{conteudo_id}"
        
        # Verificar se o conteúdo existe no cache
        conteudo_data = await redis.hgetall(content_key)
        
        if not conteudo_data:
            # Cache miss - conteúdo não encontrado
            response = {
                "correlation_id": correlation_id,
                "event_type": "response",
                "original_event_type": "obter_cache_conteudo",
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success",
                "message": "Conteúdo não encontrado no cache",
                "conteudo_id": conteudo_id,
                "cache_hit": False,
                "conteudo": None
            }
        else:
            # Cache hit - conteúdo encontrado
            await redis.incr("app:stats:cache_hits")
            
            response = {
                "correlation_id": correlation_id,
                "event_type": "response",
                "original_event_type": "obter_cache_conteudo",
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success",
                "message": "Conteúdo recuperado do cache com sucesso",
                "conteudo_id": conteudo_id,
                "cache_hit": True,
                "conteudo": conteudo_data
            }

        logger.info(f"✅ Cache {'hit' if conteudo_data else 'miss'} para conteúdo {conteudo_id}")

        await producer.send_and_wait(OUTPUT_TOPIC, response)
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao obter conteúdo do cache: {e}")

        # Enviar resposta de erro
        error_response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "obter_cache_conteudo",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao obter conteúdo do cache: {str(e)}",
        }

        await producer.send_and_wait(OUTPUT_TOPIC, error_response)
        return False