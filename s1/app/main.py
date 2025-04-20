from fastapi import FastAPI
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json
import random

app = FastAPI()

# Tentativa de conexão com Kafka com retries
producer = None
max_retries = 10  # Definir número máximo de tentativas
retry_count = 0

while producer is None and retry_count < max_retries:
    try:
        producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("[S1] ✅ KafkaProducer criado com sucesso!")
    except NoBrokersAvailable:
        print(f"[S1] ⚠️ Kafka não disponível ainda... Tentando novamente em 3s. Tentativa {retry_count + 1}/{max_retries}")
        time.sleep(3)
        retry_count += 1

    if retry_count == max_retries:
        print("[S1] ❌ Não foi possível conectar ao Kafka após várias tentativas.")
        break

@app.get("/")
def root():
    return {"message": "API do S1 está no ar!"}

@app.get("/send_event")
def send_event():
    event = {
        "user_id": random.randint(1, 1000),
        "event_type": "user_registered",
        "message": "Novo usuário registrado!"
    }

    print(f"[S1] Enviando evento para o Kafka: {event}")
    try:
        producer.send('user_events', value=event)
        print("[S1] ✅ Evento enviado com sucesso!")
        return {"status": "sucesso", "evento": event}
    except Exception as e:
        print(f"[S1] ❌ Erro ao enviar evento: {e}")
        return {"status": "erro", "detalhes": str(e)}
