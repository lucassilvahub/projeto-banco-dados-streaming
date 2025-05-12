import asyncio
import asyncpg
from datetime import datetime
import logging
import os
import traceback
import json

# ========================
# 🛠️ Configurações
# ========================
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_DB = os.getenv("POSTGRES_DB", "streaming_db")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
OUTPUT_TOPIC = "response_events"  # Definição local para evitar importação circular

# ========================
# 📥 Configuração de Logs
# ========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("s2_postgres")

# ========================
# 📡 Funções de Postgres
# ========================
async def connect_to_postgres(max_retries=15, retry_delay=5):
    """Conecta ao PostgreSQL com várias tentativas."""
    conn = None
    retries = 0
    
    postgres_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    
    while conn is None and retries < max_retries:
        try:
            logger.info(f"🔌 Tentando conectar ao PostgreSQL em {POSTGRES_HOST}:{POSTGRES_PORT} ({retries+1}/{max_retries})...")
            conn = await asyncpg.connect(postgres_url)
            logger.info("✅ Conexão com o PostgreSQL bem-sucedida!")
            return conn
        except Exception as e:
            logger.error(f"❌ Erro ao conectar ao PostgreSQL: {e}")
            retries += 1
            await asyncio.sleep(retry_delay)
    
    if conn is None:
        logger.error("❌ Não foi possível conectar ao PostgreSQL após várias tentativas.")
        return None

async def criar_tabelas_postgres(conn):
    """Cria tabelas necessárias no PostgreSQL sem foreign keys."""
    try:
        logger.info("🏗️ Verificando/criando tabelas no PostgreSQL...")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id SERIAL PRIMARY KEY,
                user_id INTEGER UNIQUE NOT NULL,
                nome VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                cpf VARCHAR(20) UNIQUE NOT NULL,
                data_criacao TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS assinaturas (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                plano VARCHAR(50) NOT NULL,
                inicio DATE NOT NULL,
                fim DATE NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'ativa',
                UNIQUE(user_id)
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS pagamentos (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                valor DECIMAL(10,2) NOT NULL,
                forma_pagamento VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL,
                data_pagamento TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS preferencias (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                idioma VARCHAR(10) NOT NULL DEFAULT 'pt-BR',
                notificacoes BOOLEAN NOT NULL DEFAULT TRUE,
                tema VARCHAR(20) NOT NULL DEFAULT 'escuro',
                data_atualizacao TIMESTAMP NOT NULL DEFAULT NOW(),
                UNIQUE(user_id)
            )
        """)

        logger.info("✅ Tabelas PostgreSQL criadas/verificadas sem constraints")
    except Exception as e:
        logger.error(f"❌ Erro ao criar tabelas no PostgreSQL: {e}")
        logger.error(traceback.format_exc())

async def processar_evento_postgres(conn, evento, producer):
    """Processa eventos específicos do PostgreSQL"""
    if conn is None:
        logger.warning("⚠️ Conexão PostgreSQL não disponível. Ignorando evento.")
        return None
        
    tipo = evento.get("event_type")
    correlation_id = evento.get("correlation_id", "unknown")
    
    try:
        if tipo == "criar_usuario":
            return await processar_criar_usuario(conn, evento, producer)
        elif tipo == "atualizar_assinatura":
            return await processar_atualizar_assinatura(conn, evento, producer)
        elif tipo == "pagamento_realizado":
            return await processar_pagamento(conn, evento, producer)
        elif tipo == "atualizar_config":
            return await processar_atualizar_config(conn, evento, producer)
        
        # Retorna None se o evento não for para o PostgreSQL
        return None
    except Exception as e:
        logger.error(f"❌ Erro ao processar evento {tipo} no PostgreSQL: {e}")
        logger.error(traceback.format_exc())
        
        # Enviar resposta de erro
        error_response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": tipo,
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao processar evento no PostgreSQL: {str(e)}"
        }
        
        await producer.send_and_wait(OUTPUT_TOPIC, error_response)
        return False

async def processar_criar_usuario(conn, evento, producer):
    """Processa o evento de criação de usuário no PostgreSQL"""
    user_id = evento.get("user_id")
    nome = evento.get("nome")
    email = evento.get("email")
    cpf = evento.get("cpf")
    correlation_id = evento.get("correlation_id")
    
    try:
        # Inserir na tabela de usuários
        await conn.execute('''
            INSERT INTO usuarios(user_id, nome, email, cpf, data_criacao)
            VALUES($1, $2, $3, $4, $5)
            ON CONFLICT (user_id) DO UPDATE
            SET nome = $2, email = $3, cpf = $4
        ''', user_id, nome, email, cpf, datetime.utcnow())
        
        logger.info(f"✅ Usuário {user_id} criado/atualizado com sucesso")
        
        # Enviar resposta de sucesso
        response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "criar_usuario",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "success",
            "message": f"Usuário {user_id} criado/atualizado com sucesso",
            "user_id": user_id
        }
        
        await producer.send_and_wait(OUTPUT_TOPIC, response)
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao criar usuário {user_id}: {e}")
        
        # Enviar resposta de erro
        error_response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "criar_usuario",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao criar usuário: {str(e)}"
        }
        
        await producer.send_and_wait(OUTPUT_TOPIC, error_response)
        return False

async def processar_atualizar_assinatura(conn, evento, producer):
    """Processa o evento de atualização de assinatura no PostgreSQL"""
    from datetime import datetime, date

    user_id = evento.get("user_id")
    plano = evento.get("plano")
    correlation_id = evento.get("correlation_id")

    # Converter datas de string para objeto date
    try:
        inicio = datetime.fromisoformat(evento.get("inicio")).date()
        fim = datetime.fromisoformat(evento.get("fim")).date()
    except Exception as e:
        logger.error(f"❌ Datas inválidas fornecidas: {e}")
        await producer.send_and_wait(OUTPUT_TOPIC, {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "atualizar_assinatura",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao processar datas da assinatura: {str(e)}"
        })
        return False

    try:
        # Verifica se usuário existe
        user = await conn.fetchrow("SELECT 1 FROM usuarios WHERE user_id = $1", user_id)
        if not user:
            logger.error(f"❌ Usuário {user_id} não encontrado.")
            raise ValueError("Usuário não encontrado.")

        # Atualiza ou insere a assinatura
        await conn.execute('''
            INSERT INTO assinaturas(user_id, plano, inicio, fim, status)
            VALUES($1, $2, $3, $4, 'ativa')
            ON CONFLICT (user_id) DO UPDATE
            SET plano = $2, inicio = $3, fim = $4, status = 'ativa'
        ''', user_id, plano, inicio, fim)

        logger.info(f"✅ Assinatura para usuário {user_id} atualizada com sucesso")
        await producer.send_and_wait(OUTPUT_TOPIC, {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "atualizar_assinatura",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "success",
            "message": f"Assinatura atualizada com sucesso",
            "user_id": user_id,
            "plano": plano
        })
        return True

    except Exception as e:
        logger.error(f"❌ Erro ao atualizar assinatura: {e}")
        await producer.send_and_wait(OUTPUT_TOPIC, {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "atualizar_assinatura",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao atualizar assinatura: {str(e)}"
        })
        return False

async def processar_pagamento(conn, evento, producer):
    """Processa o evento de pagamento no PostgreSQL"""
    from datetime import datetime

    user_id = evento.get("user_id")
    valor = evento.get("valor")
    forma_pagamento = evento.get("forma_pagamento")
    status = evento.get("status")
    correlation_id = evento.get("correlation_id")

    try:
        # Verifica se usuário existe
        user = await conn.fetchrow("SELECT 1 FROM usuarios WHERE user_id = $1", user_id)
        if not user:
            logger.error(f"❌ Usuário {user_id} não encontrado.")
            raise ValueError("Usuário não encontrado.")

        # Insere o pagamento
        await conn.execute('''
            INSERT INTO pagamentos(user_id, valor, forma_pagamento, status, data_pagamento)
            VALUES($1, $2, $3, $4, $5)
        ''', user_id, valor, forma_pagamento, status, datetime.utcnow())

        logger.info(f"✅ Pagamento registrado com sucesso para usuário {user_id}")
        await producer.send_and_wait(OUTPUT_TOPIC, {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "pagamento_realizado",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "success",
            "message": f"Pagamento registrado com sucesso",
            "user_id": user_id,
            "valor": valor
        })
        return True

    except Exception as e:
        logger.error(f"❌ Erro ao registrar pagamento: {e}")
        await producer.send_and_wait(OUTPUT_TOPIC, {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "pagamento_realizado",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao registrar pagamento: {str(e)}"
        })
        return False

async def processar_atualizar_config(conn, evento, producer):
    """Processa o evento de atualização de preferências do usuário no PostgreSQL"""
    from datetime import datetime

    user_id = evento.get("user_id")
    idioma = evento.get("idioma")
    notificacoes = evento.get("notificacoes")
    tema = evento.get("tema")
    correlation_id = evento.get("correlation_id")

    try:
        # Verificar se o usuário existe
        user = await conn.fetchrow("SELECT user_id FROM usuarios WHERE user_id = $1", user_id)
        if not user:
            logger.error(f"❌ Usuário {user_id} não encontrado.")
            raise ValueError("Usuário não encontrado.")

        # Atualizar ou inserir as preferências
        await conn.execute('''
            INSERT INTO preferencias(user_id, idioma, notificacoes, tema)
            VALUES($1, $2, $3, $4)
            ON CONFLICT (user_id) DO UPDATE
            SET idioma = $2, notificacoes = $3, tema = $4, data_atualizacao = NOW()
        ''', user_id, idioma, notificacoes, tema)

        logger.info(f"✅ Configurações atualizadas para o usuário {user_id}")

        response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "atualizar_config",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "success",
            "message": f"Configurações atualizadas com sucesso para o usuário {user_id}",
            "user_id": user_id,
        }
        await producer.send_and_wait(OUTPUT_TOPIC, response)
        return True

    except Exception as e:
        logger.error(f"❌ Erro ao atualizar configurações para o usuário {user_id}: {e}")
        error_response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": "atualizar_config",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao atualizar configurações: {str(e)}"
        }
        await producer.send_and_wait(OUTPUT_TOPIC, error_response)
        return False
