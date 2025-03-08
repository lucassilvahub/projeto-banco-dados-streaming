# Plataforma de Streaming com Polyglot Persistence

## Tecnologias Utilizadas
- **PostgreSQL (RDB):**  
  - Armazena dados estruturados (usuários, assinaturas, pagamentos, preferências persistentes).

- **MongoDB (DB1):**  
  - Gerencia o catálogo de filmes/séries, relacionamentos entre conteúdos (filmes relacionados) e blobs de exibição (thumbnails, banners).

- **Redis (DB2):**  
  - Gerencia sessões, cache de dados, leaderboards, rankings e variáveis dinâmicas para alta performance.

- **Kafka (Mensageria):**  
  - Distribui mensagens e eventos entre os serviços de forma assíncrona.

- **Elasticsearch (Opcional – S3 Logger):**  
  - Armazena logs e auditoria para monitoramento do fluxo de mensagens.

---

## Arquitetura do Sistema
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

- **S1:** Gera eventos (cadastros, atualizações de filmes, avaliações, início de sessões).  
- **Kafka:** Distribui os eventos para os serviços S2.  
- **S2:**  
  - **S2.1:** Processa dados de usuários e assinaturas → **PostgreSQL**.  
  - **S2.2:** Processa catálogo, relacionamentos e blobs → **MongoDB**.  
  - **S2.3:** Gerencia sessões, cache, leaderboards e rankings → **Redis**.  
- **S3:** Log de todas as mensagens para auditoria (pode utilizar Elasticsearch).

---

## Fluxo de Operações
1. **Geração de Eventos (S1):**  
   - S1 envia eventos (novo usuário, atualização de catálogo, avaliação, sessão iniciada) para o Kafka.

2. **Distribuição (Kafka):**  
   - O Kafka encaminha as mensagens para os serviços processadores (S2).

3. **Processamento (S2):**  
   - **Usuários/Assinaturas:** Dados persistidos no PostgreSQL.  
   - **Catálogo/Relacionamentos:** Dados armazenados no MongoDB.  
   - **Sessões/Cache/Leaderboards:** Dados gerenciados no Redis.

4. **Log/Auditoria (S3):**  
   - Todos os eventos são registrados para monitoramento e auditoria.

---

## Instruções para Execução
1. **Pré-requisitos:**  
   - Docker e Docker Compose.  
   - Containers ou serviços para PostgreSQL, MongoDB, Redis e Kafka.

2. **Configuração:**  
   - Ajuste as variáveis de ambiente para as conexões com os bancos e serviços.

3. **Execução:**  
   - Inicie os containers/serviços.  
   - Execute S1 para gerar eventos.  
   - Monitore o fluxo de mensagens e verifique os dados armazenados nos respectivos bancos.

4. **Monitoramento:**  
   - Utilize dashboards (ex.: Kibana com Elasticsearch) para auditoria e análise dos logs.

---

## Considerações Finais
- **PostgreSQL:** Assegura a integridade dos dados críticos dos usuários.
- **MongoDB:** Oferece flexibilidade para um catálogo dinâmico e relacionamentos complexos.
- **Redis:** Proporciona alta performance para gerenciamento de sessões, cache, leaderboards e rankings.
- **Kafka:** Garante comunicação assíncrona e escalável entre os serviços.

Esta arquitetura integra persistência, cache e mensageria para oferecer uma plataforma de streaming escalável, rápida e confiável.
