from fastapi import FastAPI, HTTPException, BackgroundTasks
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
from faker import Faker
from datetime import datetime, timedelta
import time, json, random, uuid, threading

app = FastAPI()
fake = Faker("pt_BR")

# Dicionário para armazenar as respostas
response_storage = {}

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

# Função para consumir respostas
def listen_for_responses():
    consumer = KafkaConsumer(
        'response_events',
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="s1_response_consumer"
    )
    
    for message in consumer:
        response = message.value
        if 'correlation_id' in response:
            correlation_id = response['correlation_id']
            response_storage[correlation_id] = response
            print(f"[S1] ✅ Resposta recebida para correlation_id: {correlation_id}")

# Iniciar o consumidor em uma thread separada
response_thread = threading.Thread(target=listen_for_responses, daemon=True)
response_thread.start()

# Função para enviar evento para o Kafka com correlation_id
def send_event(topic, event_type, payload):
    correlation_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().isoformat()
    
    evento = {
        "event_type": event_type,
        "correlation_id": correlation_id,
        "timestamp": timestamp,
        "source": "s1",
        "target": "s2",
        **payload
    }
    
    producer.send(topic, value=evento)
    producer.flush()
    
    return correlation_id

# Função para verificar status da resposta
@app.get("/status/{correlation_id}")
def check_status(correlation_id: str):
    if correlation_id in response_storage:
        return response_storage[correlation_id]
    else:
        return {"status": "pendente", "message": "Processamento em andamento"}

# ========================
# 🚀 Endpoints
# ========================

@app.get("/")
def root():
    return {"status": "success", "msg": "API do S1 no ar"}

@app.post("/usuarios")
def criar_usuario():
    """
    Gera um novo usuário fake e publica o evento no Kafka.
    """
    user = {
        "user_id": random.randint(1000, 9999),
        "nome": fake.name(),
        "email": fake.email(),
        "cpf": fake.cpf(),
        "data_criacao": datetime.utcnow().isoformat()
    }

    correlation_id = send_event("user_events", "usuario_criado", user)
    
    return {
        "mensagem": "Solicitação de criação de usuário enviada",
        "usuario": user,
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}"
    }

@app.post("/usuarios/{user_id}/assinatura")
def criar_assinatura(user_id: int):
    """
    Gera uma assinatura fake e publica o evento no Kafka.
    """
    assinatura = {
        "user_id": user_id,
        "plano": random.choice(["Básico", "Padrão", "Premium"]),
        "inicio": datetime.utcnow().date().isoformat(),
        "fim": (datetime.utcnow() + timedelta(days=30)).date().isoformat()
    }

    correlation_id = send_event("user_events", "assinatura_criada", assinatura)
    
    return {
        "mensagem": "Solicitação de assinatura enviada",
        "assinatura": assinatura,
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}"
    }

@app.post("/usuarios/{user_id}/pagamento")
def registrar_pagamento(user_id: int):
    """
    Simula um pagamento e envia evento para o Kafka.
    """
    pagamento = {
        "user_id": user_id,
        "valor": round(random.uniform(19.90, 49.90), 2),
        "forma_pagamento": random.choice(["Cartão", "Pix", "Boleto"]),
        "status": "aprovado",
        "data_pagamento": datetime.utcnow().isoformat()
    }

    correlation_id = send_event("user_events", "pagamento_realizado", pagamento)
    
    return {
        "mensagem": "Solicitação de pagamento enviada",
        "pagamento": pagamento,
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}"
    }

@app.post("/usuarios/{user_id}/config")
def atualizar_config(user_id: int, idioma: str = "pt-BR", notificacoes: bool = True):
    """
    Atualiza preferências do usuário e envia o evento.
    """
    preferencias = {
        "idioma": idioma,
        "notificacoes": notificacoes
    }

    evento = {
        "user_id": user_id,
        "preferencias": preferencias
    }

    correlation_id = send_event("user_events", "config_atualizada", evento)
    
    return {
        "mensagem": "Solicitação de atualização de configurações enviada",
        "config": preferencias,
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}"
    }

# Iniciar a API com uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)