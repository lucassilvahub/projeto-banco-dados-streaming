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
    allow_origins=["*"],  # Em produção, especifique os domínios permitidos
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
    conteudo_id: int,
    titulo: str,
    tipo: str = "filme",
    tempo_assistido: int = 0,
    posicao: int = 0,
):
    """
    Registra um histórico de visualização de conteúdo no MongoDB.

    (MongoDB)
    """
    # FIXED: Changed from 'if not mongo_db:' to 'if mongo_db is None:'
    if mongo_db is None:
        raise HTTPException(status_code=503, detail="MongoDB não está disponível")

    historico_collection = mongo_db.historico_visualizacao

    # Dados para salvar
    visualizacao = {
        "user_id": user_id,
        "conteudo_id": conteudo_id,
        "titulo": titulo,
        "tipo": tipo,
        "tempo_assistido": tempo_assistido,
        "posicao": posicao,
        "data": datetime.utcnow(),
        "concluido": posicao >= 90,  # Considera concluído se assistiu mais de 90%
    }

    # Inserir no MongoDB
    resultado = historico_collection.insert_one(visualizacao)

    # Converter ObjectId para string para serialização JSON
    visualizacao["_id"] = str(resultado.inserted_id)
    visualizacao["data"] = visualizacao["data"].isoformat()

    return {
        "mensagem": "Histórico de visualização registrado com sucesso (MongoDB)",
        "historico": visualizacao,
    }


@app.get("/historico-visualizacao/{user_id}")
def obter_historico_visualizacao(
    user_id: int,
    tipo: Optional[str] = None,
    concluido: Optional[bool] = None,
    limit: int = Query(10, ge=1, le=100),
):
    """
    Recupera o histórico de visualização de um usuário do MongoDB.
    Permite filtrar por tipo de conteúdo e status de conclusão.

    (MongoDB)
    """
    # FIXED: Changed from 'if not mongo_db:' to 'if mongo_db is None:'
    if mongo_db is None:
        raise HTTPException(status_code=503, detail="MongoDB não está disponível")

    historico_collection = mongo_db.historico_visualizacao

    # Construir query com filtros
    query = {"user_id": user_id}

    if tipo:
        query["tipo"] = tipo

    if concluido is not None:
        query["concluido"] = concluido

    # Buscar no MongoDB com ordenação por data decrescente
    resultados = list(historico_collection.find(query).sort("data", -1).limit(limit))

    # Converter ObjectId e data para string para serialização JSON
    for item in resultados:
        item["_id"] = str(item["_id"])
        item["data"] = item["data"].isoformat()

    return {"user_id": user_id, "total": len(resultados), "historico": resultados}


@app.post("/recomendacoes/gerar")
def gerar_recomendacoes(user_id: int):
    """
    Gera recomendações para um usuário com base no histórico de visualização
    e armazena no MongoDB.

    (MongoDB)
    """
    # FIXED: Changed from 'if not mongo_db:' to 'if mongo_db is None:'
    if mongo_db is None:
        raise HTTPException(status_code=503, detail="MongoDB não está disponível")

    # Obter o histórico de visualização do usuário
    historico_collection = mongo_db.historico_visualizacao
    recomendacoes_collection = mongo_db.recomendacoes

    # Buscar os tipos de conteúdo que o usuário mais assiste
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$group": {"_id": "$tipo", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]

    tipos_preferidos = list(historico_collection.aggregate(pipeline))

    # Gerar recomendações fake baseadas nas preferências
    recomendacoes = []
    categorias = [
        "Ação",
        "Comédia",
        "Drama",
        "Suspense",
        "Ficção Científica",
        "Romance",
        "Documentário",
    ]

    for tipo in tipos_preferidos:
        tipo_conteudo = tipo["_id"]
        quantidade = min(
            tipo["count"] * 2, 10
        )  # 2x mais recomendações do que o histórico, até 10

        for _ in range(quantidade):
            recomendacao = {
                "conteudo_id": random.randint(1000, 9999),
                "titulo": fake.sentence(nb_words=4),
                "tipo": tipo_conteudo,
                "categoria": random.choice(categorias),
                "pontuacao": round(random.uniform(0.5, 1.0), 2),  # Score de 0.5 a 1.0
                "motivo": f"Baseado nos seus interesses em {tipo_conteudo}s",
            }
            recomendacoes.append(recomendacao)

    # Se não houver histórico, gerar algumas recomendações aleatórias
    if not recomendacoes:
        for _ in range(5):
            tipo_conteudo = random.choice(["filme", "série", "documentário"])
            recomendacao = {
                "conteudo_id": random.randint(1000, 9999),
                "titulo": fake.sentence(nb_words=4),
                "tipo": tipo_conteudo,
                "categoria": random.choice(categorias),
                "pontuacao": round(
                    random.uniform(0.5, 0.8), 2
                ),  # Score menor para recomendações aleatórias
                "motivo": "Recomendação baseada em tendências populares",
            }
            recomendacoes.append(recomendacao)

    # Salvar recomendações no MongoDB
    recomendacao_doc = {
        "user_id": user_id,
        "data_geracao": datetime.utcnow(),
        "itens": recomendacoes,
    }

    # Remover recomendações anteriores e inserir novas
    recomendacoes_collection.delete_many({"user_id": user_id})
    resultado = recomendacoes_collection.insert_one(recomendacao_doc)

    # Preparar resposta
    recomendacao_doc["_id"] = str(resultado.inserted_id)
    recomendacao_doc["data_geracao"] = recomendacao_doc["data_geracao"].isoformat()

    return {
        "mensagem": "Recomendações geradas com sucesso (MongoDB)",
        "recomendacoes": recomendacao_doc,
    }


@app.get("/recomendacoes/{user_id}")
def obter_recomendacoes(user_id: int):
    """
    Recupera as recomendações geradas para um usuário do MongoDB.

    (MongoDB)
    """
    # FIXED: Changed from 'if not mongo_db:' to 'if mongo_db is None:'
    if mongo_db is None:
        raise HTTPException(status_code=503, detail="MongoDB não está disponível")

    recomendacoes_collection = mongo_db.recomendacoes

    # Buscar recomendações
    recomendacao = recomendacoes_collection.find_one({"user_id": user_id})

    if not recomendacao:
        return {
            "mensagem": "Não há recomendações para este usuário. Use o endpoint /recomendacoes/gerar primeiro."
        }

    # Converter ObjectId e data para string para serialização JSON
    recomendacao["_id"] = str(recomendacao["_id"])
    recomendacao["data_geracao"] = recomendacao["data_geracao"].isoformat()

    return recomendacao


# ========================
# 🚀 Endpoints - Redis
# ========================


@app.post("/sessoes")
def criar_sessao(user_id: int, dispositivo: str, localizacao: str = "Brasil"):
    """
    Cria uma nova sessão de usuário no Redis.

    (Redis)
    """
    # FIXED: Changed from 'if not redis_client:' to 'if redis_client is None:'
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis não está disponível")

    # Gerar ID de sessão
    session_id = str(uuid.uuid4())

    # Dados da sessão
    session_data = {
        "user_id": user_id,
        "dispositivo": dispositivo,
        "localizacao": localizacao,
        "inicio": datetime.utcnow().isoformat(),
        "ultima_atividade": datetime.utcnow().isoformat(),
        "ativo": True,
    }

    # Salvar no Redis
    redis_client.hset(f"session:{session_id}", mapping=session_data)

    # Definir expiração (24 horas)
    redis_client.expire(f"session:{session_id}", 60 * 60 * 24)

    # Adicionar à lista de sessões do usuário
    redis_client.sadd(f"user_sessions:{user_id}", session_id)

    return {
        "mensagem": "Sessão criada com sucesso (Redis)",
        "session_id": session_id,
        "session_data": session_data,
    }


@app.put("/sessoes/{session_id}/atividade")
def atualizar_atividade_sessao(session_id: str):
    """
    Atualiza o timestamp de última atividade para uma sessão no Redis.

    (Redis)
    """
    # FIXED: Changed from 'if not redis_client:' to 'if redis_client is None:'
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis não está disponível")

    # Verificar se a sessão existe
    if not redis_client.exists(f"session:{session_id}"):
        raise HTTPException(status_code=404, detail="Sessão não encontrada")

    # Atualizar timestamp de última atividade
    timestamp = datetime.utcnow().isoformat()
    redis_client.hset(f"session:{session_id}", "ultima_atividade", timestamp)

    # Renovar expiração (24 horas desde a última atividade)
    redis_client.expire(f"session:{session_id}", 60 * 60 * 24)

    return {
        "mensagem": "Atividade de sessão atualizada com sucesso (Redis)",
        "session_id": session_id,
        "ultima_atividade": timestamp,
    }


@app.delete("/sessoes/{session_id}")
def encerrar_sessao(session_id: str):
    """
    Encerra uma sessão de usuário no Redis.

    (Redis)
    """
    # FIXED: Changed from 'if not redis_client:' to 'if redis_client is None:'
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis não está disponível")

    # Verificar se a sessão existe
    if not redis_client.exists(f"session:{session_id}"):
        raise HTTPException(status_code=404, detail="Sessão não encontrada")

    # Obter user_id para remover da lista de sessões do usuário
    user_id = redis_client.hget(f"session:{session_id}", "user_id")

    # Marcar sessão como inativa
    redis_client.hset(f"session:{session_id}", "ativo", "False")
    redis_client.hset(
        f"session:{session_id}", "encerramento", datetime.utcnow().isoformat()
    )

    # Definir expiração curta (1 hora)
    redis_client.expire(f"session:{session_id}", 60 * 60)

    # Remover da lista de sessões ativas do usuário
    if user_id:
        redis_client.srem(f"user_sessions:{user_id}", session_id)

    return {
        "mensagem": "Sessão encerrada com sucesso (Redis)",
        "session_id": session_id,
    }


@app.get("/sessoes/usuario/{user_id}")
def listar_sessoes_usuario(user_id: int, ativas_apenas: bool = True):
    """
    Lista todas as sessões de um usuário armazenadas no Redis.

    (Redis)
    """
    # FIXED: Changed from 'if not redis_client:' to 'if redis_client is None:'
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis não está disponível")

    # Obter IDs de sessão do usuário
    session_ids = redis_client.smembers(f"user_sessions:{user_id}")

    if not session_ids:
        return {
            "mensagem": "Usuário não possui sessões ativas",
            "user_id": user_id,
            "sessoes": [],
        }

    # Recuperar dados de cada sessão
    sessoes = []
    for session_id in session_ids:
        session_data = redis_client.hgetall(f"session:{session_id}")

        if session_data:
            session_data["session_id"] = session_id
            sessoes.append(session_data)

    return {"user_id": user_id, "total": len(sessoes), "sessoes": sessoes}


@app.post("/cache/conteudo/{conteudo_id}")
def cache_conteudo(conteudo_id: int):
    """
    Cria ou atualiza informações de conteúdo em cache no Redis.

    (Redis)
    """
    # FIXED: Changed from 'if not redis_client:' to 'if redis_client is None:'
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis não está disponível")

    # Gerar dados fake para o conteúdo
    categorias = [
        "Ação",
        "Comédia",
        "Drama",
        "Suspense",
        "Ficção Científica",
        "Romance",
        "Documentário",
    ]

    conteudo = {
        "conteudo_id": conteudo_id,
        "titulo": fake.sentence(nb_words=4),
        "tipo": random.choice(["filme", "série", "documentário"]),
        "categoria": random.choice(categorias),
        "duracao": random.randint(30, 180),
        "classificacao": random.choice(["Livre", "10+", "12+", "14+", "16+", "18+"]),
        "ano": random.randint(1990, 2025),
        "descricao": fake.paragraph(),
        "popularidade": round(random.uniform(0, 10), 1),
        "ultimo_acesso": datetime.utcnow().isoformat(),
    }

    # Salvar no Redis
    redis_client.hset(f"conteudo:{conteudo_id}", mapping=conteudo)

    # Definir expiração (12 horas)
    redis_client.expire(f"conteudo:{conteudo_id}", 60 * 60 * 12)

    # Incrementar contador de acessos
    redis_client.hincrby(f"conteudo:{conteudo_id}", "acessos", 1)

    return {
        "mensagem": "Conteúdo em cache atualizado com sucesso (Redis)",
        "conteudo": conteudo,
    }


@app.get("/cache/conteudo/{conteudo_id}")
def obter_cache_conteudo(conteudo_id: int):
    """
    Recupera informações de conteúdo do cache no Redis.

    (Redis)
    """
    # FIXED: Changed from 'if not redis_client:' to 'if redis_client is None:'
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis não está disponível")

    # Verificar se o conteúdo está em cache
    if not redis_client.exists(f"conteudo:{conteudo_id}"):
        raise HTTPException(status_code=404, detail="Conteúdo não encontrado em cache")

    # Obter dados do cache
    conteudo = redis_client.hgetall(f"conteudo:{conteudo_id}")

    # Renovar expiração (12 horas)
    redis_client.expire(f"conteudo:{conteudo_id}", 60 * 60 * 12)

    # Atualizar timestamp de último acesso
    redis_client.hset(
        f"conteudo:{conteudo_id}", "ultimo_acesso", datetime.utcnow().isoformat()
    )

    # Incrementar contador de acessos
    redis_client.hincrby(f"conteudo:{conteudo_id}", "acessos", 1)

    return conteudo


# Endpoint para fins de teste/debug
@app.get("/health")
def health_check():
    """Endpoint para verificação de saúde do serviço"""
    return {
        "service": "s1",
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "kafka_connected": producer is not None,
        "mongo_connected": mongo_db is not None,
        "redis_connected": redis_client is not None,
    }


# Iniciar a API com uvicorn
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
