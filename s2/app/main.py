import asyncio
import json
import asyncpg
from aiokafka import AIOKafkaConsumer
from datetime import datetime
import logging

# ========================
# 🛠️ Configurações
# ========================
KAFKA_BROKER = "kafka:9092"
TOPICO = "user_events"
POSTGRES_DSN = "postgresql://user:password@postgres:5432/streaming_db"  # Ajuste conforme seu docker-compose

# ========================
# 📥 Consumer Kafka
# ========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_datetime(dt_str):
    """Converte string ISO para objeto datetime."""
    if isinstance(dt_str, str):
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    return dt_str


async def processar_evento(conn, evento):
    tipo = evento.get("event_type")

    if tipo == "criar_usuario":
        # Converter a string ISO para datetime

        await conn.execute(
            """
            INSERT INTO usuarios (user_id, nome, email, cpf, data_criacao)
            VALUES ($1, $2, $3, $4, $5)
        """,
            evento["user_id"],
            evento["nome"],
            evento["email"],
            evento["cpf"],
            evento["data_criacao"],
        )
        logger.info("👤 Novo usuário salvo!")

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
            logger.info("📄 Assinatura atualizada!")
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
            logger.info("📄 Nova assinatura criada!")

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
            logger.info("⚙️ Preferências atualizadas!")
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
            logger.info("⚙️ Novas preferências salvas!")

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
        logger.info("💸 Pagamento salvo!")

    else:
        logger.warning(f"⚠️ Tipo de evento não reconhecido: {tipo}")


async def consumir():
    try:
        # Tenta fazer a conexão com o PostgreSQL
        conn = await asyncpg.connect(POSTGRES_DSN)
        logger.info("✅ Conexão com o PostgreSQL bem-sucedida!")

        # Criação das tabelas caso não existam
        await criar_tabelas(conn)

    except Exception as e:
        logger.error(f"❌ Erro ao conectar ao PostgreSQL: {e}")
        return  # Se falhar, não continua o consumo do Kafka

    logger.info("🔌 Iniciando consumer Kafka...")
    consumer = AIOKafkaConsumer(
        TOPICO,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="s2_consumer_group",
    )

    # Iniciando o consumo de mensagens
    await consumer.start()

    try:
        logger.info("📡 Aguardando mensagens...")
        async for msg in consumer:
            evento = msg.value
            try:
                await processar_evento(conn, evento)
            except Exception as e:
                logger.error(f"❌ Erro ao processar evento: {e}")

    finally:
        await consumer.stop()
        await conn.close()


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


if __name__ == "__main__":
    asyncio.run(consumir())