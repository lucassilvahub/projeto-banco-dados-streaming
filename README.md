# 🎬 Plataforma de Streaming com Polyglot Persistence  

## 🚀 Tecnologias Utilizadas  

- **PostgreSQL (RDB)** – Usuários, assinaturas, pagamentos, preferências persistentes.  
- **MongoDB (DB1)** – Catálogo de filmes, relacionamentos, blobs de exibição.  
- **Redis (DB2)** – Sessões, cache, leaderboards, rankings e variáveis dinâmicas.  
- **Kafka (Mensageria)** – Comunicação assíncrona entre serviços.  
- **Elasticsearch (Opcional – Logs)** – Monitoramento e auditoria.  

## 🏗 Arquitetura do Sistema  

```
                                          -------
                                --------> |     |
                                |         | DB1 |  (MongoDB)
                                | ------- |     |
                                | |       -------
                                | v
------      --------------     ------     -------
|    |      |            | --> |    | --> |     |
| S1 | ---> | mensageria |     | S2 |     | DB2 |  (Redis)
|    |      |            | <-- |    | <-- |     |
------      --------------     ------     -------
  |            |                 ^ |
  |   ------   |                 | |      -------
  |   |    |   |                 | -----> |     |
  --->| S3 |<--|                 |        | RDB |  (PostgreSQL)
      |    |                     ---------|     |
      ------                              -------
```
💡 **Componentes:**  
✅ **S1** → Gera eventos (cadastro, avaliações, sessões).  
✅ **Kafka** → Encaminha eventos para os serviços (S2).  
✅ **S2** → Processa dados e os armazena nos bancos correspondentes.  
✅ **S3** → Registra logs para auditoria e monitoramento.  

## 🔄 Fluxo de Operações  
1️⃣ **S1 gera eventos** – Cadastro, novas avaliações, atualizações de catálogo.  
2️⃣ **Kafka distribui mensagens** – Comunicação entre serviços de forma assíncrona.  
3️⃣ **S2 processa e armazena** – Cada tipo de dado vai para seu banco ideal.  
4️⃣ **S3 registra logs** – Histórico para análise e monitoramento.  

## ▶ Como Executar  
📌 **Pré-requisitos:**  
✔ Docker + Docker Compose instalados.  
✔ Containers configurados para PostgreSQL, MongoDB, Redis e Kafka.  

📌 **Passos:**  
1️⃣ Clone este repositório.  
2️⃣ Ajuste as variáveis de ambiente (conexões com os bancos).  
3️⃣ Inicie os containers/serviços (`docker-compose up`).  
4️⃣ Rode **S1** para gerar eventos e acompanhar o fluxo de dados.  

📌 **Monitoramento:**  
- Logs disponíveis via **Elasticsearch/Kibana** (se ativado).  
- Banco de dados acessíveis via ferramentas como **pgAdmin, Mongo Compass e RedisInsight**.  

## 🎯 Por que essa abordagem?  
🔹 **PostgreSQL** → Segurança e consistência dos dados críticos.  
🔹 **MongoDB** → Flexibilidade para um catálogo dinâmico.  
🔹 **Redis** → Respostas ultra rápidas para cache e rankings.  
🔹 **Kafka** → Comunicação escalável e eficiente.  
