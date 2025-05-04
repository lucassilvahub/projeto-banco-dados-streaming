import asyncio
import json
import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from datetime import datetime
import logging

# ========================
# 🛠️ Configurações
# ========================
KAFKA_BROKER = "kafka:9092"
TOPICO_ENTRADA = "user_events"
TOPICO_SAIDA = "response_events"
POSTGRES_DSN = "postgresql://user:password@postgres:5432/streaming_db"

# ========================
# 📥 Consumer e Producer Kafka
# ========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def enviar_resposta(producer, evento_original, status, mensagem=None):
    """Envia uma resposta para o evento original"""
    resposta = {
        "event_type": "response",
        "correlation_id": evento_original.get("correlation_id"),
        "status": status,
        "timestamp": datetime.utcnow().isoformat(),
        "source": "s2",
        "target": "s1",
        "original_event_type": evento_original.get("event_type"),
        "message": mensagem
    }
    
    await producer.send_and_wait(TOPICO_SAIDA, value=resposta)
    logger.info(f"✅ Resposta enviada para correlation_id: {resposta['correlation_id']}")

async def processar_evento(conn, producer, evento):
    tipo = evento.get("event_type")
    status = "success"
    mensagem = None

    try:
        if tipo == "usuario_criado":
            await conn.execute("""
                INSERT INTO usuarios (user_id, nome, email, cpf, data_criacao)
                VALUES ($1, $2, $3, $4, $5)
            """, evento["user_id"], evento["nome"], evento["email"], evento["cpf"], evento["data_criacao"])
            logger.info("👤 Novo usuário salvo!")
            mensagem = "Usuário criado com sucesso"

        elif tipo == "assinatura_criada":
            await conn.execute(
                """
                INSERT INTO assinaturas (user_id, plano, inicio, fim)
                VALUES ($1, $2, $3, $4)
            """,
                evento["user_id"],
                evento["plano"],
                evento["inicio"],
                evento["fim"],
            )
            logger.info("📄 Assinatura salva!")
            mensagem = "Assinatura criada com sucesso"

        elif tipo == "pagamento_realizado":
            await conn.execute(
                """
                INSERT INTO pagamentos (user_id, valor, forma_pagamento, status, data_pagamento)
                VALUES ($1, $2, $3, $4, $5)
            """,
                evento["user_id"],
                evento["valor"],
                evento["forma_pagamento"],
                evento["status"],
                evento["data_pagamento"],
            )
            logger.info("💸 Pagamento salvo!")
            mensagem = "Pagamento registrado com sucesso"

        elif tipo == "config_atualizada":
            preferencias = evento["preferencias"]
            await conn.execute(
                """
                INSERT INTO preferencias (user_id, idioma, notificacoes)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id) DO UPDATE
                SET idioma = $2, notificacoes = $3
            """,
                evento["user_id"],
                preferencias["idioma"],
                preferencias["notificacoes"],
            )
            logger.info("⚙️ Preferências salvas!")
            mensagem = "Configurações atualizadas com sucesso"

        else:
            logger.warning(f"⚠️ Tipo de evento não reconhecido: {tipo}")
            status = "error"
            mensagem = f"Tipo de evento não reconhecido: {tipo}"

    except Exception as e:
        logger.error(f"❌ Erro ao processar evento: {e}")
        status = "error"
        mensagem = f"Erro ao processar evento: {str(e)}"
    
    # Enviar resposta
    if "correlation_id" in evento:
        await enviar_resposta(producer, evento, status, mensagem)
    else:
        logger.warning("⚠️ Evento sem correlation_id, resposta não enviada")

async def consumir():
    logger.info("🔄 Conectando ao PostgreSQL...")

    try:
        # Conectar ao PostgreSQL
        conn = await asyncpg.connect(POSTGRES_DSN)
        logger.info("✅ Conexão com o PostgreSQL bem-sucedida!")

        # Criar tabelas se não existirem
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS usuarios (
                user_id INTEGER PRIMARY KEY,
                nome TEXT NOT NULL,
                email TEXT NOT NULL,
                cpf TEXT NOT NULL,
                data_criacao TIMESTAMP NOT NULL
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS assinaturas (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                plano TEXT NOT NULL,
                inicio DATE NOT NULL,
                fim DATE NOT NULL,
                FOREIGN KEY (user_id) REFERENCES usuarios(user_id)
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS pagamentos (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                valor NUMERIC(10,2) NOT NULL,
                forma_pagamento TEXT NOT NULL,
                status TEXT NOT NULL,
                data_pagamento TIMESTAMP NOT NULL,
                FOREIGN KEY (user_id) REFERENCES usuarios(user_id)
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS preferencias (
                user_id INTEGER PRIMARY KEY,
                idioma TEXT NOT NULL,
                notificacoes BOOLEAN NOT NULL,
                FOREIGN KEY (user_id) REFERENCES usuarios(user_id)
            )
        ''')

    except Exception as e:
        logger.error(f"❌ Erro ao conectar ao PostgreSQL: {e}")
        return

    logger.info("🔌 Iniciando consumer e producer Kafka...")
    
    consumer = AIOKafkaConsumer(
        TOPICO_ENTRADA,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="s2_consumer_group",
    )
    
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Iniciar consumer e producer
    await consumer.start()
    await producer.start()

    try:
        logger.info("📡 Aguardando mensagens...")
        async for msg in consumer:
            evento = msg.value
            try:
                await processar_evento(conn, producer, evento)
            except Exception as e:
                logger.error(f"❌ Erro ao processar evento: {e}")
                if "correlation_id" in evento:
                    await enviar_resposta(producer, evento, "error", str(e))

    finally:
        await consumer.stop()
        await producer.stop()
        await conn.close()

if __name__ == "__main__":
    asyncio.run(consumir())