# Descrição do Projeto

Este projeto é uma plataforma de streaming de filmes e séries que utiliza a abordagem de **Polyglot Persistence**. O uso de diferentes tecnologias de banco de dados permite otimizar o armazenamento e processamento de dados, de acordo com suas características e necessidades.

# Arquitetura do Sistema

<br>

<p align="center">
  <img src="https://github.com/user-attachments/assets/20049402-7c0a-41d9-a1db-efb088342faa" width="750"/>
</p>

<br>

## Tecnologias Utilizadas

- **PostgreSQL (RDB)** – Para armazenar dados de usuários, assinaturas, pagamentos e preferências persistentes.  
- **MongoDB (DB1)** – Responsável pelo armazenamento do catálogo de filmes, relacionamentos entre filmes e blobs de exibição.  
- **Redis (DB2)** – Utilizado para sessões, cache, rankings e variáveis dinâmicas.  
- **Kafka (Mensageria)** – Comunicação assíncrona entre os serviços para garantir desacoplamento e escalabilidade.  
- **Elasticsearch (Logs)** – Utilizado para monitoramento, logs e auditoria.

## Componentes:

- **S1** → Gera eventos como cadastro de usuários, avaliações de filmes e atualizações de sessões.  
- **Kafka** → Encaminha esses eventos para os serviços de processamento, garantindo a comunicação assíncrona.  
- **S2** → Processa os eventos e armazena os dados nos bancos de dados apropriados.  
- **S3** → Registra logs de atividades para auditoria e monitoramento.

# Aprofundamento/detalhes de cada etapa

## **PostgreSQL**  

Utilizado para armazenar dados relacionais e críticos. Suas principais aplicações incluem:  
  - **Usuários**: Armazenamento de informações dos usuários (nome, email, senha, etc.).
  - **Assinaturas**: Gerenciamento das assinaturas dos usuários (plano, data de renovação, etc.).
  - **Pagamentos**: Processamento e registro de transações financeiras, como pagamentos de assinaturas.
  - **Preferências Persistentes**: Armazenamento de preferências e configurações do usuário, como preferências de conteúdo.

## **MongoDB**  

Empregado para o gerenciamento do catálogo de filmes e séries. Suas aplicações incluem:
  - **Catálogo de Filmes e Séries**: Armazenamento das informações dos filmes e séries (títulos, descrições, diretores, etc.).
  - **Relacionamentos entre Filmes**: Armazenamento de dados relacionados entre filmes, como recomendações ou categorias.
  - **Blobs de Exibição**: Armazenamento de dados não estruturados, como imagens, trailers e arquivos de vídeo.

## **Redis**  

Integrado para melhorar a performance do sistema. Suas principais aplicações incluem:
  - **Cache**: Armazenamento em cache de dados frequentemente acessados, como informações de filmes populares ou recentemente assistidos.
  - **Rankings e Leaderboards**: Armazenamento e atualização rápida de rankings de filmes mais assistidos ou avaliados.
  - **Sessões Ativas**: Gerenciamento de sessões de usuários, como armazenamento de tokens de autenticação e dados temporários.

## **Kafka**  

Facilita a comunicação entre os microserviços de forma assíncrona. Suas principais aplicações incluem:
  - **Distribuição de Eventos**: Envio de eventos para outros serviços, como criação de novos usuários, avaliações de filmes, ou alterações no catálogo.
  - **Comunicação Assíncrona**: Desacoplamento dos microserviços, garantindo que os sistemas se comuniquem sem bloquear uns aos outros.

## **Elasticsearch**  

Utilizado para monitoramento, logs e auditoria. Suas aplicações incluem:
  - **Monitoramento**: Coleta de métricas de desempenho e estado do sistema para análise em tempo real.
  - **Logs de Atividades**: Armazenamento e análise de logs de eventos do sistema para detectar problemas ou falhas.

O objetivo é criar uma plataforma escalável e altamente disponível, onde diferentes tipos de dados são tratados com as tecnologias mais adequadas para cada caso de uso, permitindo uma performance otimizada e uma manutenção eficiente.
