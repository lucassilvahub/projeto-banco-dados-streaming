from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from faker import Faker
from datetime import datetime, timedelta
import time, json, random

app = FastAPI()
fake = Faker("pt_BR")

# ========================
# 🔌 Kafka Producer Setup
# ========================
producer = None
max_retries = 10
retry_count = 0

while producer is None and retry_count < max_retries:
    try:
        producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("[S1] ✅ KafkaProducer conectado!")
    except NoBrokersAvailable:
        print(f"[S1] ⚠️ Kafka não disponível... tentativa {retry_count+1}/{max_retries}")
        time.sleep(3)
        retry_count += 1

if retry_count == max_retries:
    print("[S1] ❌ Kafka não respondeu após várias tentativas.")

# ========================
# 📦 Banco fake em memória
# ========================
fake_users_db = {}

# ========================
# 🚀 Endpoints
# ========================

@app.get("/")
def root():
    return {"status": "ok", "msg": "API do S1 no ar"}

@app.post("/usuarios")
def criar_usuario():
    user_id = random.randint(1000, 9999)
    user = {
        "user_id": user_id,
        "nome": fake.name(),
        "email": fake.email(),
        "cpf": fake.cpf(),
        "data_criacao": datetime.utcnow().isoformat()
    }
    fake_users_db[user_id] = user

    evento = {"event_type": "usuario_criado", **user}
    producer.send("user_events", value=evento)
    producer.flush()

    return {"mensagem": "Usuário criado com sucesso", "usuario": user}

@app.post("/usuarios/{user_id}/assinatura")
def criar_assinatura(user_id: int):
    if user_id not in fake_users_db:
        raise HTTPException(status_code=404, detail="Usuário não encontrado")

    assinatura = {
        "user_id": user_id,
        "plano": random.choice(["Básico", "Padrão", "Premium"]),
        "inicio": datetime.utcnow().date().isoformat(),
        "fim": (datetime.utcnow() + timedelta(days=30)).date().isoformat()
    }

    evento = {"event_type": "assinatura_criada", **assinatura}
    producer.send("user_events", value=evento)
    producer.flush()

    return {"mensagem": "Assinatura registrada", "assinatura": assinatura}

@app.post("/usuarios/{user_id}/pagamento")
def registrar_pagamento(user_id: int):
    if user_id not in fake_users_db:
        raise HTTPException(status_code=404, detail="Usuário não encontrado")

    pagamento = {
        "user_id": user_id,
        "valor": round(random.uniform(19.90, 49.90), 2),
        "forma_pagamento": random.choice(["Cartão", "Pix", "Boleto"]),
        "status": "aprovado",
        "data_pagamento": datetime.utcnow().isoformat()
    }

    evento = {"event_type": "pagamento_realizado", **pagamento}
    producer.send("user_events", value=evento)
    producer.flush()

    return {"mensagem": "Pagamento simulado", "pagamento": pagamento}

@app.put("/usuarios/{user_id}/config")
def atualizar_config(user_id: int, idioma: str = "pt-BR", notificacoes: bool = True):
    if user_id not in fake_users_db:
        raise HTTPException(status_code=404, detail="Usuário não encontrado")

    preferencias = {
        "idioma": idioma,
        "notificacoes": notificacoes
    }

    fake_users_db[user_id]["preferencias"] = preferencias

    evento = {
        "event_type": "config_atualizada",
        "user_id": user_id,
        "preferencias": preferencias
    }

    producer.send("user_events", value=evento)
    producer.flush()

    return {"mensagem": "Configurações atualizadas", "config": preferencias}
