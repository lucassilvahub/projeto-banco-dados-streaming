import asyncio
import json
import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from datetime import datetime
import logging
import os

# ========================
# 🛠️ Configurações
# ========================
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = "user_events"
OUTPUT_TOPIC = "response_events"
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_DB = os.getenv("POSTGRES_DB", "streaming_db")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DSN = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# ========================
# 📥 Configuração de Logs
# ========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("s2")

# ========================
# 🔄 Funções Utilitárias
# ========================
def parse_datetime(dt_str):
    """Converte string ISO para objeto datetime."""
    if isinstance(dt_str, str):
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    return dt_str

# ========================
# 📡 Serviço de Processamento
# ========================
async def processar_evento(conn, evento, producer):
    """Processa os eventos recebidos e envia uma resposta"""
    tipo = evento.get("event_type")
    correlation_id = evento.get("correlation_id", "unknown")
    
    try:
        if tipo == "criar_usuario":
            # Converter a string ISO para datetime
            data_criacao = parse_datetime(evento["data_criacao"])
            
            await conn.execute(
                """
                INSERT INTO usuarios (user_id, nome, email, cpf, data_criacao)
                VALUES ($1, $2, $3, $4, $5)
                """,
                evento["user_id"],
                evento["nome"],
                evento["email"],
                evento["cpf"],
                data_criacao,
            )
            logger.info(f"👤 Novo usuário salvo! User ID: {evento['user_id']}")
            
            # Preparar resposta de sucesso
            response = {
                "correlation_id": correlation_id,
                "event_type": "response",
                "original_event_type": tipo,
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success",
                "message": f"Usuário {evento['nome']} criado com sucesso",
                "user_id": evento["user_id"]
            }

        elif tipo == "atualizar_assinatura":
            # Converter as strings de data para objetos datetime
            inicio = parse_datetime(evento["inicio"])
            fim = parse_datetime(evento["fim"])

            # Verificar se já existe assinatura para este usuário
            assinatura_existente = await conn.fetchrow(
                "SELECT * FROM assinaturas WHERE user_id = $1", 
                evento["user_id"]
            )
            
            if assinatura_existente:
                # Se existir, atualiza em vez de inserir
                await conn.execute(
                    """
                    UPDATE assinaturas 
                    SET plano = $2, inicio = $3, fim = $4
                    WHERE user_id = $1
                    """,
                    evento["user_id"],
                    evento["plano"],
                    inicio,
                    fim,
                )
                logger.info(f"📄 Assinatura atualizada para User ID: {evento['user_id']}")
                action = "atualizada"
            else:
                # Se não existir, insere novo registro
                await conn.execute(
                    """
                    INSERT INTO assinaturas (user_id, plano, inicio, fim)
                    VALUES ($1, $2, $3, $4)
                    """,
                    evento["user_id"],
                    evento["plano"],
                    inicio,
                    fim,
                )
                logger.info(f"📄 Nova assinatura criada para User ID: {evento['user_id']}")
                action = "criada"
            
            # Preparar resposta de sucesso
            response = {
                "correlation_id": correlation_id,
                "event_type": "response",
                "original_event_type": tipo,
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success",
                "message": f"Assinatura {action} com sucesso",
                "user_id": evento["user_id"],
                "plano": evento["plano"]
            }

        elif tipo == "atualizar_config":
            preferencias = evento["preferencias"]
            
            # Verificar se já existem preferências para este usuário
            config_existente = await conn.fetchrow(
                "SELECT * FROM preferencias WHERE user_id = $1", 
                evento["user_id"]
            )
            
            if config_existente:
                # Se existir, atualiza em vez de inserir
                await conn.execute(
                    """
                    UPDATE preferencias 
                    SET idioma = $2, notificacoes = $3
                    WHERE user_id = $1
                    """,
                    evento["user_id"],
                    preferencias["idioma"],
                    preferencias["notificacoes"],
                )
                logger.info(f"⚙️ Preferências atualizadas para User ID: {evento['user_id']}")
            else:
                # Se não existir, insere novo registro
                await conn.execute(
                    """
                    INSERT INTO preferencias (user_id, idioma, notificacoes)
                    VALUES ($1, $2, $3)
                    """,
                    evento["user_id"],
                    preferencias["idioma"],
                    preferencias["notificacoes"],
                )
                logger.info(f"⚙️ Novas preferências salvas para User ID: {evento['user_id']}")
            
            # Preparar resposta de sucesso
            response = {
                "correlation_id": correlation_id,
                "event_type": "response",
                "original_event_type": tipo,
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success",
                "message": f"Configurações atualizadas com sucesso",
                "user_id": evento["user_id"],
                "idioma": preferencias["idioma"],
                "notificacoes": preferencias["notificacoes"]
            }

        elif tipo == "pagamento_realizado":
            # Converter a string de data para objeto datetime
            data_pagamento = parse_datetime(evento["data_pagamento"])

            await conn.execute(
                """
                INSERT INTO pagamentos (user_id, valor, forma_pagamento, status, data_pagamento)
                VALUES ($1, $2, $3, $4, $5)
                """,
                evento["user_id"],
                evento["valor"],
                evento["forma_pagamento"],
                evento["status"],
                data_pagamento,
            )
            logger.info(f"💸 Pagamento registrado para User ID: {evento['user_id']}")
            
            # Preparar resposta de sucesso
            response = {
                "correlation_id": correlation_id,
                "event_type": "response",
                "original_event_type": tipo,
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success",
                "message": f"Pagamento de R$ {evento['valor']} processado com sucesso",
                "user_id": evento["user_id"],
                "forma_pagamento": evento["forma_pagamento"]
            }

        else:
            logger.warning(f"⚠️ Tipo de evento não reconhecido: {tipo}")
            
            # Preparar resposta de erro para tipo desconhecido
            response = {
                "correlation_id": correlation_id,
                "event_type": "response",
                "original_event_type": tipo,
                "timestamp": datetime.utcnow().isoformat(),
                "status": "error",
                "message": f"Tipo de evento desconhecido: {tipo}"
            }
        
        # Enviar resposta via Kafka
        await producer.send_and_wait(OUTPUT_TOPIC, response)
        logger.info(f"✅ Resposta enviada para correlation_id: {correlation_id}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao processar evento {tipo}: {e}")
        
        # Enviar resposta de erro
        error_response = {
            "correlation_id": correlation_id,
            "event_type": "response",
            "original_event_type": tipo,
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "message": f"Erro ao processar evento: {str(e)}"
        }
        
        await producer.send_and_wait(OUTPUT_TOPIC, error_response)
        logger.info(f"⚠️ Resposta de erro enviada para correlation_id: {correlation_id}")
        
        return False

async def criar_tabelas(conn):
    """Cria as tabelas necessárias se não existirem."""
    logger.info("🏗️ Verificando/criando tabelas...")

    # Tabela de usuários
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS usuarios (
            id SERIAL PRIMARY KEY,
            user_id INTEGER UNIQUE NOT NULL,
            nome VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            cpf VARCHAR(14) UNIQUE NOT NULL,
            data_criacao TIMESTAMP NOT NULL
        )
        """
    )

    # Tabela de assinaturas
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS assinaturas (
            id SERIAL PRIMARY KEY,
            user_id INTEGER UNIQUE NOT NULL,
            plano VARCHAR(20) NOT NULL,
            inicio DATE NOT NULL,
            fim DATE NOT NULL
        )
        """
    )

    # Tabela de pagamentos
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pagamentos (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            valor NUMERIC(10, 2) NOT NULL,
            forma_pagamento VARCHAR(20) NOT NULL,
            status VARCHAR(20) NOT NULL,
            data_pagamento TIMESTAMP NOT NULL
        )
        """
    )

    # Tabela de preferências
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS preferencias (
            id SERIAL PRIMARY KEY,
            user_id INTEGER UNIQUE NOT NULL,
            idioma VARCHAR(10) NOT NULL,
            notificacoes BOOLEAN NOT NULL
        )
        """
    )

    logger.info("✅ Tabelas verificadas/criadas com sucesso!")

async def iniciar_processador():
    """Inicializa o serviço de processamento de eventos"""
    # Tentativas de conexão ao PostgreSQL
    conn = None
    retries = 0
    max_retries = 10
    
    while conn is None and retries < max_retries:
        try:
            logger.info(f"🔌 Tentando conectar ao PostgreSQL ({retries+1}/{max_retries})...")
            conn = await asyncpg.connect(POSTGRES_DSN)
            logger.info("✅ Conexão com o PostgreSQL bem-sucedida!")
            
            # Criação das tabelas caso não existam
            await criar_tabelas(conn)
            break
            
        except Exception as e:
            logger.error(f"❌ Erro ao conectar ao PostgreSQL: {e}")
            retries += 1
            await asyncio.sleep(5)  # Espera antes de tentar novamente
    
    if conn is None:
        logger.error("❌ Não foi possível conectar ao PostgreSQL após várias tentativas.")
        return  # Encerra a função se não conseguir conectar
    
    # Inicialização do produtor Kafka
    try:
        logger.info("🔌 Iniciando Kafka Producer...")
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        await producer.start()
        logger.info("✅ Kafka Producer iniciado com sucesso!")
    except Exception as e:
        logger.error(f"❌ Erro ao iniciar Kafka Producer: {e}")
        await conn.close()
        return
    
    # Inicialização do consumidor Kafka
    try:
        logger.info("🔌 Iniciando Kafka Consumer...")
        consumer = AIOKafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            group_id="s2_consumer_group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest"
        )
        await consumer.start()
        logger.info("✅ Kafka Consumer iniciado com sucesso!")
    except Exception as e:
        logger.error(f"❌ Erro ao iniciar Kafka Consumer: {e}")
        await producer.stop()
        await conn.close()
        return
    
    # Loop principal de processamento de eventos
    try:
        logger.info("📡 Aguardando mensagens...")
        async for msg in consumer:
            evento = msg.value
            logger.info(f"📩 Evento recebido: {evento.get('event_type')} (correlation_id: {evento.get('correlation_id', 'N/A')})")
            
            try:
                await processar_evento(conn, evento, producer)
            except Exception as e:
                logger.error(f"❌ Erro ao processar evento: {e}")
    except Exception as e:
        logger.error(f"❌ Erro no loop principal: {e}")
    finally:
        logger.info("🛑 Encerrando conexões...")
        await consumer.stop()
        await producer.stop()
        await conn.close()

# ========================
# 🚀 Ponto de Entrada
# ========================
if __name__ == "__main__":
    try:
        logger.info("🚀 Iniciando serviço S2 de processamento...")
        asyncio.run(iniciar_processador())
    except KeyboardInterrupt:
        logger.info("👋 Serviço interrompido pelo usuário")
    except Exception as e:
        logger.error(f"❌ Erro fatal: {e}")