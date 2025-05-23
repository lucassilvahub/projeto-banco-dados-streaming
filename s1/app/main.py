from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
from faker import Faker
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
import redis
import time
import json
import random
import uuid
import threading
import os

app = FastAPI(
    title="Streaming Platform API",
    description="API para gerenciamento de usuários e assinaturas da plataforma de streaming com persistência poliglota",
    version="1.0.0",
)

# Configuração de CORS para permitir acesso do dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
fake = Faker("pt_BR")

# Dicionário para armazenar as respostas
response_storage = {}

# ========================
# 🔌 Conexões com Bancos de Dados
# ========================

# MongoDB
mongo_client = None
mongo_db = None
max_mongo_retries = 10
mongo_retry_count = 0
mongo_server = os.getenv("MONGO_HOST", "mongo")
mongo_port = int(os.getenv("MONGO_PORT", "27017"))
mongo_db_name = os.getenv("MONGO_DB", "streaming_db")

while mongo_client is None and mongo_retry_count < max_mongo_retries:
    try:
        mongo_client = MongoClient(f"mongodb://{mongo_server}:{mongo_port}/")
        mongo_db = mongo_client[mongo_db_name]
        print("[S1] ✅ MongoDB conectado!")
    except Exception as e:
        print(
            f"[S1] ⚠️ MongoDB não disponível... tentativa {mongo_retry_count+1}/{max_mongo_retries}: {e}"
        )
        time.sleep(3)
        mongo_retry_count += 1

if mongo_retry_count == max_mongo_retries:
    print("[S1] ❌ MongoDB não respondeu após várias tentativas.")

# Redis
redis_client = None
max_redis_retries = 10
redis_retry_count = 0
redis_server = os.getenv("REDIS_HOST", "redis")
redis_port = int(os.getenv("REDIS_PORT", "6379"))

while redis_client is None and redis_retry_count < max_redis_retries:
    try:
        redis_client = redis.Redis(
            host=redis_server, port=redis_port, decode_responses=True
        )
        redis_client.ping()  # Verificar se está conectado
        print("[S1] ✅ Redis conectado!")
    except Exception as e:
        print(
            f"[S1] ⚠️ Redis não disponível... tentativa {redis_retry_count+1}/{max_redis_retries}: {e}"
        )
        time.sleep(3)
        redis_retry_count += 1

if redis_retry_count == max_redis_retries:
    print("[S1] ❌ Redis não respondeu após várias tentativas.")

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
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
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
            "response_events",
            bootstrap_servers=kafka_server,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="s1_response_consumer",
            auto_offset_reset="latest",
        )

        print("[S1] ✅ KafkaConsumer iniciado com sucesso!")

        for message in consumer:
            response = message.value
            if "correlation_id" in response:
                correlation_id = response["correlation_id"]
                response_storage[correlation_id] = response
                print(
                    f"[S1] ✅ Resposta recebida para correlation_id: {correlation_id}"
                )
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
        **payload,
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
    return {
        "status": "success",
        "msg": "API do Streaming Platform no ar",
        "version": "1.0.0",
    }


@app.get("/status/{correlation_id}")
def check_status(correlation_id: str):
    """Verifica o status de uma operação pelo correlation_id"""
    if correlation_id in response_storage:
        return response_storage[correlation_id]
    else:
        return {"status": "pendente", "message": "Processamento em andamento"}


# ========================
# 🚀 Endpoints - PostgreSQL
# ========================


@app.post("/usuarios")
def criar_usuario():
    """
    Gera um novo usuário fake e publica o evento no Kafka para persistir no PostgreSQL.

    (PostgreSQL)
    """
    user = {
        "user_id": random.randint(1000, 9999),
        "nome": fake.name(),
        "email": fake.email(),
        "cpf": fake.cpf(),
        "data_criacao": datetime.utcnow().isoformat(),
    }

    correlation_id = send_event("user_events", "criar_usuario", user)

    return {
        "mensagem": "Solicitação de criação de usuário enviada (PostgreSQL)",
        "usuario": user,
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}",
    }


@app.post("/usuarios/{user_id}/assinatura")
def criar_assinatura(user_id: int):
    """
    Gera uma assinatura fake e publica o evento no Kafka para persistir no PostgreSQL.

    (PostgreSQL)
    """
    assinatura = {
        "user_id": user_id,
        "plano": random.choice(["Básico", "Padrão", "Premium"]),
        "inicio": datetime.utcnow().date().isoformat(),
        "fim": (datetime.utcnow() + timedelta(days=30)).date().isoformat(),
    }

    correlation_id = send_event("user_events", "atualizar_assinatura", assinatura)

    return {
        "mensagem": "Solicitação de assinatura enviada (PostgreSQL)",
        "assinatura": assinatura,
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}",
    }


@app.post("/usuarios/{user_id}/pagamento")
def registrar_pagamento(user_id: int):
    """
    Simula um pagamento e envia evento para o Kafka para persistir no PostgreSQL.

    (PostgreSQL)
    """
    pagamento = {
        "user_id": user_id,
        "valor": round(random.uniform(19.90, 49.90), 2),
        "forma_pagamento": random.choice(["Cartão", "Pix", "Boleto"]),
        "status": "success",
        "data_pagamento": datetime.utcnow().isoformat(),
    }

    correlation_id = send_event("user_events", "pagamento_realizado", pagamento)

    return {
        "mensagem": "Solicitação de pagamento enviada (PostgreSQL)",
        "pagamento": pagamento,
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}",
    }


@app.post("/usuarios/{user_id}/config")
def atualizar_config(
    user_id: int, idioma: str = "pt-BR", notificacoes: bool = True, tema: str = "escuro"
):
    """
    Atualiza preferências do usuário com dados aleatórios e envia o evento para persistir no PostgreSQL.
    (PostgreSQL)
    """
    # Gerar dados aleatórios para as preferências
    idiomas_disponiveis = ["pt-BR", "en-US", "es-ES", "fr-FR"]
    temas_disponiveis = ["escuro", "claro", "sistema"]
    notificacoes_opcoes = [True, False]

    # Usando random.choice para selecionar valores aleatórios
    idioma = random.choice(idiomas_disponiveis)
    tema = random.choice(temas_disponiveis)
    notificacoes = random.choice(notificacoes_opcoes)

    # VAR FOR DEBBUG

    preferencias = {
        "idioma": idioma,
        "notificacoes": notificacoes,
        "tema": tema,
    }

    evento = {
        "user_id": user_id,
        "idioma": idioma,
        "notificacoes": notificacoes,
        "tema": tema,
        "data_atualizacao": datetime.utcnow().isoformat(),  # Data de atualização da configuração
    }

    correlation_id = send_event("user_events", "atualizar_config", evento)

    return {
        "mensagem": "Solicitação de atualização de configurações enviada (PostgreSQL)",
        "config": preferencias,
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}",
    }


# ========================
# 🚀 Endpoints - MongoDB
# ========================


@app.post("/historico-visualizacao")
def registrar_visualizacao(
    user_id: int,
    conteudo_id: int = None,
    titulo: str = None,
    tipo: str = None,
    tempo_assistido: int = None,
    posicao: int = None,
):
    """
    Registra um histórico de visualização de conteúdo via Kafka para MongoDB.
    CORRIGIDO: Agora usa o fluxo S1 → Kafka → S2 → MongoDB

    (MongoDB)
    """
    # Gerar dados fake se não fornecidos
    if conteudo_id is None:
        conteudo_id = random.randint(1000, 9999)
    if titulo is None:
        titulo = fake.sentence(nb_words=4)
    if tipo is None:
        tipo = random.choice(["filme", "série", "documentário"])
    if tempo_assistido is None:
        tempo_assistido = random.randint(30, 180)
    if posicao is None:
        posicao = random.randint(10, 100)

    visualizacao = {
        "user_id": user_id,
        "conteudo_id": conteudo_id,
        "titulo": titulo,
        "tipo": tipo,
        "tempo_assistido": tempo_assistido,
        "posicao": posicao,
    }

    correlation_id = send_event("user_events", "registrar_visualizacao", visualizacao)

    return {
        "mensagem": "Solicitação de registro de visualização enviada (MongoDB)",
        "visualizacao": visualizacao,
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}",
    }


@app.get("/historico-visualizacao/{user_id}")
def obter_historico_visualizacao(
    user_id: int,
    tipo: Optional[str] = None,
    concluido: Optional[bool] = None,
    limit: int = Query(10, ge=1, le=100),
):
    """
    Recupera o histórico de visualização de um usuário via Kafka para MongoDB.
    CORRIGIDO: Agora usa o fluxo S1 → Kafka → S2 → MongoDB

    (MongoDB)
    """
    evento = {
        "user_id": user_id,
        "tipo": tipo,
        "concluido": concluido,
        "limit": limit,
    }

    correlation_id = send_event("user_events", "obter_historico_visualizacao", evento)

    return {
        "mensagem": "Solicitação de histórico de visualização enviada (MongoDB)",
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}",
    }


@app.post("/recomendacoes/gerar")
def gerar_recomendacoes(user_id: int):
    """
    Gera recomendações para um usuário via Kafka para MongoDB.
    CORRIGIDO: Agora usa o fluxo S1 → Kafka → S2 → MongoDB

    (MongoDB)
    """
    evento = {
        "user_id": user_id,
    }

    correlation_id = send_event("user_events", "gerar_recomendacoes", evento)

    return {
        "mensagem": "Solicitação de geração de recomendações enviada (MongoDB)",
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}",
    }


@app.get("/recomendacoes/{user_id}")
def obter_recomendacoes(user_id: int):
    """
    Recupera as recomendações geradas para um usuário via Kafka para MongoDB.
    CORRIGIDO: Agora usa o fluxo S1 → Kafka → S2 → MongoDB

    (MongoDB)
    """
    evento = {
        "user_id": user_id,
    }

    correlation_id = send_event("user_events", "obter_recomendacoes", evento)

    return {
        "mensagem": "Solicitação de recomendações enviada (MongoDB)",
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}",
    }


# ========================
# 🚀 Endpoints - Redis
# ========================


@app.post("/sessoes")
def criar_sessao(
    user_id: int, 
    dispositivo: str = None, 
    localizacao: str = None
):
    """
    Cria uma nova sessão de usuário via Kafka para Redis.
    CORRIGIDO: Agora usa o fluxo S1 → Kafka → S2 → Redis

    (Redis)
    """
    # Gerar dados fake se não fornecidos
    if dispositivo is None:
        dispositivo = random.choice(["Android", "iOS", "Web", "Smart TV", "Console"])
    if localizacao is None:
        localizacao = fake.city()

    sessao = {
        "user_id": user_id,
        "dispositivo": dispositivo,
        "localizacao": localizacao,
    }

    correlation_id = send_event("user_events", "criar_sessao", sessao)

    return {
        "mensagem": "Solicitação de criação de sessão enviada (Redis)",
        "sessao": sessao,
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}",
    }


@app.put("/sessoes/{session_id}/atividade")
def atualizar_atividade_sessao(session_id: str):
    """
    Atualiza o timestamp de última atividade para uma sessão via Kafka para Redis.
    CORRIGIDO: Agora usa o fluxo S1 → Kafka → S2 → Redis

    (Redis)
    """
    evento = {
        "session_id": session_id,
    }

    correlation_id = send_event("user_events", "atualizar_atividade_sessao", evento)

    return {
        "mensagem": "Solicitação de atualização de atividade enviada (Redis)",
        "session_id": session_id,
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}",
    }


@app.delete("/sessoes/{session_id}")
def encerrar_sessao(session_id: str):
    """
    Encerra uma sessão de usuário via Kafka para Redis.
    CORRIGIDO: Agora usa o fluxo S1 → Kafka → S2 → Redis

    (Redis)
    """
    evento = {
        "session_id": session_id,
    }

    correlation_id = send_event("user_events", "encerrar_sessao", evento)

    return {
        "mensagem": "Solicitação de encerramento de sessão enviada (Redis)",
        "session_id": session_id,
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}",
    }


@app.get("/sessoes/usuario/{user_id}")
def listar_sessoes_usuario(user_id: int, ativas_apenas: bool = True):
    """
    Lista todas as sessões de um usuário via Kafka para Redis.
    CORRIGIDO: Agora usa o fluxo S1 → Kafka → S2 → Redis

    (Redis)
    """
    evento = {
        "user_id": user_id,
        "ativas_apenas": ativas_apenas,
    }

    correlation_id = send_event("user_events", "listar_sessoes_usuario", evento)

    return {
        "mensagem": "Solicitação de listagem de sessões enviada (Redis)",
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}",
    }


@app.post("/cache/conteudo/{conteudo_id}")
def cache_conteudo(conteudo_id: int):
    """
    Cria ou atualiza informações de conteúdo em cache via Kafka para Redis.
    CORRIGIDO: Agora usa o fluxo S1 → Kafka → S2 → Redis

    (Redis)
    """
    evento = {
        "conteudo_id": conteudo_id,
    }

    correlation_id = send_event("user_events", "cache_conteudo", evento)

    return {
        "mensagem": "Solicitação de cache de conteúdo enviada (Redis)",
        "conteudo_id": conteudo_id,
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}",
    }


@app.get("/cache/conteudo/{conteudo_id}")
def obter_cache_conteudo(conteudo_id: int):
    """
    Recupera informações de conteúdo do cache via Kafka para Redis.
    CORRIGIDO: Agora usa o fluxo S1 → Kafka → S2 → Redis

    (Redis)
    """
    evento = {
        "conteudo_id": conteudo_id,
    }

    correlation_id = send_event("user_events", "obter_cache_conteudo", evento)

    return {
        "mensagem": "Solicitação de cache de conteúdo enviada (Redis)",
        "conteudo_id": conteudo_id,
        "correlation_id": correlation_id,
        "status_url": f"/status/{correlation_id}",
    }


# Endpoint para fins de teste/debug
@app.get("/health")
def health_check():
    """Endpoint para verificação de saúde do serviço"""
    return {
        "service": "s1",
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "kafka_connected": producer is not None,
    }


# Iniciar a API com uvicorn
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)