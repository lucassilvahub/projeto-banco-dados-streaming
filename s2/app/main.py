import asyncio
import json
import asyncpg
from aiokafka import AIOKafkaConsumer
from datetime import datetime

# ========================
# 🛠️ Configurações
# ========================
KAFKA_BROKER = "kafka:9092"
TOPICO = "user_events"
POSTGRES_DSN = "postgresql://postgres:postgres@postgres:5432/postgres"

# ========================
# 📥 Consumer Kafka
# ========================
async def processar_evento(conn, evento):
    tipo = evento.get("event_type")
    
    if tipo == "usuario_criado":
        await conn.execute("""
            INSERT INTO usuarios (user_id, nome, email, cpf, data_criacao)
            VALUES ($1, $2, $3, $4, $5)
        """, evento["user_id"], evento["nome"], evento["email"], evento["cpf"], evento["data_criacao"])
        print("👤 Novo usuário salvo!")

    elif tipo == "assinatura_criada":
        await conn.execute("""
            INSERT INTO assinaturas (user_id, plano, inicio, fim)
            VALUES ($1, $2, $3, $4)
        """, evento["user_id"], evento["plano"], evento["inicio"], evento["fim"])
        print("📄 Assinatura salva!")

    elif tipo == "pagamento_realizado":
        await conn.execute("""
            INSERT INTO pagamentos (user_id, valor, forma_pagamento, status, data_pagamento)
            VALUES ($1, $2, $3, $4, $5)
        """, evento["user_id"], evento["valor"], evento["forma_pagamento"], evento["status"], evento["data_pagamento"])
        print("💸 Pagamento salvo!")

    elif tipo == "config_atualizada":
        preferencias = evento["preferencias"]
        await conn.execute("""
            INSERT INTO preferencias (user_id, idioma, notificacoes)
            VALUES ($1, $2, $3)
        """, evento["user_id"], preferencias["idioma"], preferencias["notificacoes"])
        print("⚙️ Preferências salvas!")

    else:
        print(f"⚠️ Tipo de evento não reconhecido: {tipo}")

async def consumir():
    print("🔄 Conectando ao PostgreSQL...")
    
    try:
        # Tenta fazer a conexão com o PostgreSQL
        conn = await asyncpg.connect(POSTGRES_DSN)
        print("✅ Conexão com o PostgreSQL bem-sucedida!")
        
    except Exception as e:
        print(f"❌ Erro ao conectar ao PostgreSQL: {e}")
        return  # Se falhar, não continua o consumo do Kafka
    
    print("🔌 Iniciando consumer Kafka...")
    consumer = AIOKafkaConsumer(
        TOPICO,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="s2_consumer_group"
    )
    await consumer.start()

    try:
        print("📡 Aguardando mensagens...")
        async for msg in consumer:
            evento = msg.value
            await processar_evento(conn, evento)
    finally:
        await consumer.stop()
        await conn.close()

if __name__ == "__main__":
    asyncio.run(consumir())
