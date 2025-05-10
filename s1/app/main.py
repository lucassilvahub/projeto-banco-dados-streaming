from fastapi import FastAPI, HTTPException, BackgroundTasks
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
from faker import Faker
from datetime import datetime, timedelta
import time
import json
import random
import uuid
import threading
import os

app = FastAPI(
    title="Streaming Platform API",
    description="API para gerenciamento de usuários e assinaturas da plataforma de streaming",
    version="1.0.0"
)
fake = Faker("pt_BR")

# Dicionário para armazenar as respostas
response_storage = {}

# ========================
# 🔌 Kafka Producer Setup
# ========================
producer = None
max_retries = 10
retry_count = 0
kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

while producer is None and retry_count < max_retries:
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_server,
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
    """Thread para ouvir as respostas do serviço S2 via Kafka"""
    try:
        consumer = KafkaConsumer(
            'response_events',
            bootstrap_servers=kafka_server,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="s1_response_consumer",
            auto_offset_reset="latest"
        )
        
        print("[S1] ✅ KafkaConsumer iniciado com sucesso!")
        
        for message in consumer:
            response = message.value
            if 'correlation_id' in response:
                correlation_id = response['correlation_id']
                response_storage[correlation_id] = response
                print(f"[S1] ✅ Resposta recebida para correlation_id: {correlation_id}")
    except Exception as e:
        print(f"[S1] ❌ Erro no consumer: {e}")

# Iniciar o consumidor em uma thread separada
response_thread = threading.Thread(target=listen_for_responses, daemon=True)
response_thread.start()

# Função para enviar evento para o Kafka com correlation_id
def send_event(topic, event_type, payload):
    """Envia um evento para o Kafka com um correlation_id para rastreamento"""
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

# ========================
# 🚀 Endpoints
# ========================

@app.get("/")
def root():
    """Endpoint raiz para verificar se a API está no ar"""
    return {"status": "success", "msg": "API do Streaming Platform no ar", "version": "1.0.0"}

@app.get("/status/{correlation_id}")
def check_status(correlation_id: str):
    """Verifica o status de uma operação pelo correlation_id"""
    if correlation_id in response_storage:
        return response_storage[correlation_id]
    else:
        return {"status": "pendente", "message": "Processamento em andamento"}

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

    correlation_id = send_event("user_events", "criar_usuario", user)
    
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

    correlation_id = send_event("user_events", "atualizar_assinatura", assinatura)
    
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

    correlation_id = send_event("user_events", "atualizar_config", evento)
    
    return {
        "mensagem": "Solicitação de atualização de configurações enviada",
        "config": preferencias,
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}"
    }

# Endpoint para fins de teste/debug
@app.get("/health")
def health_check():
    """Endpoint para verificação de saúde do serviço"""
    return {
        "service": "s1",
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "kafka_connected": producer is not None
    }

# Iniciar a API com uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)