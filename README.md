# Plataforma de Streaming com Polyglot Persistence

## Tecnologias Utilizadas

- **PostgreSQL (RDB)** – Para armazenar dados de usuários, assinaturas, pagamentos e preferências persistentes.  
- **MongoDB (DB1)** – Responsável pelo armazenamento do catálogo de filmes, relacionamentos entre filmes e blobs de exibição.  
- **Redis (DB2)** – Utilizado para sessões, cache, rankings e variáveis dinâmicas.  
- **Kafka (Mensageria)** – Comunicação assíncrona entre os serviços para garantir desacoplamento e escalabilidade.  
- **Elasticsearch (Logs)** – Utilizado para monitoramento, logs e auditoria.

## Arquitetura do Sistema

<p align="center">
  <img src="https://github.com/user-attachments/assets/20049402-7c0a-41d9-a1db-efb088342faa" width="720"/>
</p>

### Componentes:

- **S1** → Gera eventos como cadastro de usuários, avaliações de filmes e atualizações de sessões.  
- **Kafka** → Encaminha esses eventos para os serviços de processamento, garantindo a comunicação assíncrona.  
- **S2** → Processa os eventos e armazena os dados nos bancos de dados apropriados.  
- **S3** → Registra logs de atividades para auditoria e monitoramento.

## 📜 Descrição do Projeto

Este projeto é uma plataforma de streaming de filmes e séries que utiliza a abordagem de **Polyglot Persistence**. O uso de diferentes tecnologias de banco de dados permite otimizar o armazenamento e processamento de dados, de acordo com suas características e necessidades.

- **PostgreSQL** é utilizado para armazenar dados relacionais e críticos, como informações de usuários, assinaturas e pagamentos, garantindo consistência e segurança.  
- **MongoDB** é empregado no gerenciamento do catálogo de filmes e séries, com flexibilidade para armazenar dados não estruturados, como informações dinâmicas e blobs de exibição.  
- **Redis** é integrado para melhorar a performance do sistema, oferecendo armazenamento em cache de dados frequentemente acessados, como rankings e sessões ativas.  
- **Kafka** facilita a comunicação entre os microserviços, permitindo que diferentes partes do sistema se comuniquem de forma escalável e assíncrona, sem sobrecarregar os componentes principais.  
- **Elasticsearch** é opcional, mas pode ser utilizado para coletar e analisar logs de atividades do sistema, oferecendo insights para monitoramento e auditoria.

O objetivo é criar uma plataforma escalável e altamente disponível, onde diferentes tipos de dados são tratados com as tecnologias mais adequadas para cada caso de uso, permitindo uma performance otimizada e uma manutenção eficiente.
