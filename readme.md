# Plataforma de Streaming com Polyglot Persistence

## 📋 Visão Geral

Este projeto implementa uma plataforma de streaming moderna com arquitetura orientada a eventos e persistência poliglota (utilizando diferentes bancos de dados para diferentes tipos de dados). A aplicação é construída como um sistema distribuído baseado em microserviços, com comunicação assíncrona via Kafka.

## 🏗️ Arquitetura

O sistema é composto por 3 serviços principais e um dashboard, todos containerizados via Docker:

![Arquitetura do Sistema](https://github.com/user-attachments/assets/7eda8a42-e30a-4ff3-be1a-72a2d119a746)

### Serviços

**S1 (API Gateway)** - Porta: `8000`
- Serviço de API RESTful desenvolvido com FastAPI
- Recebe requisições HTTP e gera eventos para o Kafka
- Rastreia operações via correlation_id
- Expõe documentação interativa via Swagger UI

**S2 (Processador de Eventos)** - Consumidor Kafka
- Consome eventos do Kafka e processa regras de negócio
- Persiste dados nos bancos PostgreSQL, MongoDB e Redis
- Envia respostas assíncronas via Kafka
- Implementa validação de integridade entre bancos

**S3 (Monitor de Sistema)** - Observabilidade
- Monitora todos os eventos do sistema
- Armazena logs estruturados no Elasticsearch
- Fornece fallback para logs em arquivo
- Rastreia fluxos de requisição-resposta

**Dashboard** - Porta: `8089`
- Interface web centralizada para acesso a todos os componentes
- Links diretos para administração dos bancos de dados
- Documentação dos endpoints disponíveis

### Bancos de Dados (Polyglot Persistence)

| Banco | Porta | Uso | Dados Armazenados |
|-------|-------|-----|-------------------|
| **PostgreSQL** | 5432 | OLTP | Usuários, assinaturas, pagamentos, preferências |
| **MongoDB** | 27017 | NoSQL | Histórico de visualização, recomendações |
| **Redis** | 6379 | Cache | Sessões, cache de conteúdo |
| **Elasticsearch** | 9200 | Logs | Logs estruturados, métricas de sistema |

### Mensageria

**Apache Kafka** - Porta: `9092`
- **Tópicos:**
  - `user_events`: Eventos de entrada (requisições)
  - `response_events`: Eventos de saída (respostas)
- **Padrão:** Request-Response assíncrono com correlation_id

## 🚀 Como Executar

### Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) 20.x ou superior
- [Docker Compose](https://docs.docker.com/compose/install/) 2.x ou superior
- [Python](https://www.python.org/downloads/) 3.8+ (para o script de inicialização)

### Execução Rápida

1. **Clone o repositório:**
```bash
git clone [URL_DO_REPOSITORIO]
cd plataforma-streaming
```

2. **Execute o script de inicialização:**
```bash
python start.py
```

**O script automaticamente:**
- ✅ Verifica se Docker e Docker Compose estão instalados
- 📁 Cria a estrutura de diretórios necessária
- 🛑 Para containers existentes (limpeza)
- 🚀 Inicia todos os serviços via Docker Compose
- 🌐 Abre o dashboard no navegador (http://localhost:8089)

3. **Aguarde a inicialização completa** (aproximadamente 30-45 segundos)

### Execução Manual (Alternativa)

```bash
# Parar containers existentes
docker-compose down

# Iniciar todos os serviços
docker-compose up -d

# Visualizar logs em tempo real
docker-compose logs -f
```

## 🌐 Acesso aos Serviços

### Interface Principal
- **🏠 Dashboard:** http://localhost:8089 (Ponto de entrada principal)

### APIs e Documentação
- **📡 API Principal:** http://localhost:8000
- **📖 Swagger UI:** http://localhost:8000/docs (Documentação interativa)
- **🔍 Health Check:** http://localhost:8000/health

### Administração de Bancos de Dados

| Serviço | URL | Usuário | Senha | Descrição |
|---------|-----|---------|-------|-----------|
| **PostgreSQL (Adminer)** | http://localhost:8081 | `user` | `password` | Interface web para PostgreSQL |
| **MongoDB (Mongo Express)** | http://localhost:8082 | `admin` | `admin` | Interface web para MongoDB |
| **Redis (Commander)** | http://localhost:8083 | - | - | Interface web para Redis |
| **Elasticsearch (Kibana)** | http://localhost:5601 | - | - | Análise de logs e métricas |

### Ferramentas de Monitoramento
- **📊 Kafka UI:** http://localhost:8080 (Tópicos e mensagens)
- **📈 Kibana:** http://localhost:5601 (Logs e dashboards)

## 🔧 Testando a API

### Opção 1: Postman Collection (Recomendado)

O projeto inclui uma **collection completa do Postman** com todos os endpoints configurados:

1. **Importe a collection:** `Plataforma de Streaming API.postman_collection.json`
2. **Configure o environment** (opcional):
   - Base URL: `http://localhost:8000`
   - User ID: `1234` (exemplo)
3. **Execute os requests** organizados por categoria

**📁 Estrutura da Collection:**
- 🔍 **Status Endpoints:** Verificação de saúde e status
- 👥 **PostgreSQL - Usuários:** CRUD de usuários, assinaturas, pagamentos
- 📺 **MongoDB - Histórico:** Visualizações e recomendações
- ⚡ **Redis - Cache:** Sessões e cache de conteúdo

### Opção 2: Swagger UI

Acesse http://localhost:8000/docs para documentação interativa com exemplos em tempo real.

### Opção 3: cURL (Exemplos)

```bash
# Criar usuário
curl -X POST "http://localhost:8000/usuarios"

# Criar assinatura
curl -X POST "http://localhost:8000/usuarios/1234/assinatura"

# Verificar status da operação
curl "http://localhost:8000/status/{correlation_id}"

# Registrar visualização
curl -X POST "http://localhost:8000/historico-visualizacao?user_id=1234&conteudo_id=5678"

# Cache de conteúdo
curl -X POST "http://localhost:8000/cache/conteudo/1001"
```

## 📊 Principais Endpoints

### PostgreSQL (Dados Relacionais)
- `POST /usuarios` - Criar usuário
- `POST /usuarios/{id}/assinatura` - Criar assinatura
- `POST /usuarios/{id}/pagamento` - Registrar pagamento
- `POST /usuarios/{id}/config` - Atualizar preferências

### MongoDB (Dados Não-Estruturados)
- `POST /historico-visualizacao` - Registrar visualização
- `GET /historico-visualizacao/{user_id}` - Obter histórico
- `POST /recomendacoes/gerar` - Gerar recomendações
- `GET /recomendacoes/{user_id}` - Obter recomendações

### Redis (Cache e Sessões)
- `POST /cache/conteudo/{id}` - Cachear conteúdo
- `GET /cache/conteudo/{id}` - Obter cache

### Sistema
- `GET /status/{correlation_id}` - Status da operação
- `GET /health` - Saúde do serviço

## 🗃️ Esquema dos Bancos de Dados

### PostgreSQL - Tabelas

```sql
-- Usuários principais
usuarios (id, user_id, nome, email, cpf, data_criacao)

-- Planos de assinatura
assinaturas (id, user_id, plano, inicio, fim, status)

-- Histórico de pagamentos
pagamentos (id, user_id, valor, forma_pagamento, status, data_pagamento)

-- Preferências do usuário
preferencias (id, user_id, idioma, notificacoes, tema, data_atualizacao)
```

### MongoDB - Coleções

```javascript
// Histórico de visualização
historico_visualizacao {
  user_id: Number,
  conteudo_id: Number,
  titulo: String,
  tipo: String, // "filme", "série", "documentário"
  tempo_assistido: Number,
  posicao: Number, // percentual assistido
  data: Date,
  concluido: Boolean
}

// Sistema de recomendações
recomendacoes {
  user_id: Number,
  data_geracao: Date,
  itens: Array // lista de conteúdos recomendados
}
```

### Redis - Estruturas de Dados

```redis
# Cache de conteúdo
conteudo:{id} -> Hash com dados do conteúdo

# Contadores globais
app:stats:cache_hits -> Counter
```

### Características do Sistema

- **🔄 Comunicação Assíncrona:** Todas as operações retornam imediatamente um `correlation_id`
- **📍 Rastreabilidade:** Cada operação pode ser rastreada end-to-end
- **🛡️ Resiliência:** Sistema continua funcionando mesmo com falhas parciais
- **📊 Observabilidade:** Todos os eventos são logados e monitorados
- **⚡ Performance:** Operações não bloqueantes

## 🛠️ Desenvolvimento e Debug

### Logs dos Serviços

```bash
# Ver logs de todos os serviços
docker-compose logs -f

# Logs específicos por serviço
docker-compose logs -f s1  # API
docker-compose logs -f s2  # Processor
docker-compose logs -f s3  # Monitor
```

### Restart de Serviços

```bash
# Reiniciar serviço específico
docker-compose restart s1

# Rebuild e restart
docker-compose up -d --build s1
```

### Variáveis de Ambiente

As configurações estão no arquivo `.env`:

```bash
# PostgreSQL
POSTGRES_USER=user
POSTGRES_PASSWORD=password
POSTGRES_DB=streaming_db
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# Elasticsearch
ELASTICSEARCH_HOST=elasticsearch:9200

# Logs
LOG_LEVEL=INFO
```

## 🔍 Monitoramento e Troubleshooting

### Health Checks

Todos os serviços possuem health checks configurados:

```bash
# Status geral do sistema
curl http://localhost:8000/health

# Status específico do S2
curl http://localhost:8000/health  # (via S2)
```

### Verificação de Conectividade

```bash
# Verificar se Kafka está funcionando
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Verificar PostgreSQL
docker exec -it postgres psql -U user -d streaming_db -c "SELECT version();"

# Verificar MongoDB
docker exec -it mongo mongosh --eval "db.adminCommand('ping')"

# Verificar Redis
docker exec -it redis redis-cli ping

# Verificar Elasticsearch
curl http://localhost:9200/_cluster/health
```

### Análise de Logs no Kibana

1. Acesse http://localhost:5601
2. Configure o index pattern: `api_*`
3. Explore os logs por:
   - **transaction_id**: Rastrear operações específicas
   - **event_type**: Filtrar por tipo de evento
   - **status**: Ver sucessos/erros
   - **timestamp**: Análise temporal

## 🧩 Tecnologias Utilizadas

### Backend
- **🐍 Python 3.11** - Linguagem principal
- **⚡ FastAPI** - Framework web moderno e rápido
- **🔄 asyncio** - Programação assíncrona nativa
- **📨 aiokafka** - Cliente Kafka assíncrono

### Bancos de Dados
- **🐘 PostgreSQL 15** - Banco relacional ACID
- **🍃 MongoDB 7** - Banco de documentos NoSQL
- **🚀 Redis 7** - Cache em memória
- **🔍 Elasticsearch 7.17** - Motor de busca e análise

### Infraestrutura
- **🐳 Docker & Docker Compose** - Containerização
- **📡 Apache Kafka** - Message Streaming Platform
- **🌐 Nginx** - Web server para dashboard
- **📊 Kibana** - Visualização de dados

### Ferramentas de Desenvolvimento
- **📖 Swagger/OpenAPI** - Documentação de API
- **🔧 Postman** - Testes de API
- **🎨 Faker** - Geração de dados fake
- **📝 Adminer** - Administração de PostgreSQL
