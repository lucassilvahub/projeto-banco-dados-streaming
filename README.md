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
- **FastAPI (API)** – Framework leve e moderno utilizado para a criação das APIs do projeto. Possui documentação interativa disponível em `/docs`.

## Componentes:

- **S1** → Gera eventos como cadastro de usuários, avaliações de filmes e atualizações de sessões.  
- **Kafka** → Encaminha esses eventos para os serviços de processamento, garantindo a comunicação assíncrona.  
- **S2** → Processa os eventos e armazena os dados nos bancos de dados apropriados.  
- **S3** → Registra logs de atividades para auditoria e monitoramento.

---

## **Índice de Navegação**
- [Arquitetura do Sistema](#arquitetura-do-sistema)
- [Tecnologias Utilizadas](#tecnologias-utilizadas)
- [Componentes](#componentes)
- [Aprofundamento de Cada Etapa](#aprofundamento-detalhes-de-cada-etapa)
- [Como Rodar o Projeto](#como-rodar-o-projeto)
- [Requisitos Mínimos](#requisitos-mínimos)

---

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

## **FastAPI**

O FastAPI é utilizado para a criação da API RESTful do sistema. A API fornece endpoints para:
  - Cadastro de usuários, assinaturas e pagamentos.
  - Configurações de preferências do usuário.
  - Integração com Kafka para envio de eventos para outros sistemas.
  
A documentação interativa da API está disponível em **`/docs`**. Você pode acessar essa documentação diretamente em seu navegador após iniciar a aplicação FastAPI. A interface é gerada automaticamente e fornece uma maneira prática de testar a API e visualizar os endpoints disponíveis.

# Como Rodar o Projeto

Este projeto utiliza **Docker** e **Docker Compose** para facilitar a execução de todos os serviços. Siga os passos abaixo para rodar o projeto localmente:

1. **Clone o repositório:**
   ```bash
   git clone https://github.com/seu-usuario/streaming-app.git
   cd streaming-app
   ```

2. **Certifique-se de que o Docker e o Docker Compose estão instalados:**
   - Você pode verificar se o Docker está instalado com:
     ```bash
     docker --version
     ```
   - Verifique se o Docker Compose está instalado com:
     ```bash
     docker-compose --version
     ```

   Caso você ainda não tenha o Docker e o Docker Compose instalados, siga as instruções nas respectivas documentações:
   - [Instalar Docker](https://docs.docker.com/get-docker/)
   - [Instalar Docker Compose](https://docs.docker.com/compose/install/)

3. **Configure o ambiente:**
   O arquivo `docker-compose.yml` já contém as configurações necessárias para a execução do projeto. Caso precise ajustar variáveis de ambiente (como credenciais ou configurações específicas), você pode fazer isso editando diretamente o arquivo `docker-compose.yml`.

4. **Suba os containers com o Docker Compose:**
   Dentro da pasta do projeto, execute o comando abaixo para rodar os containers do Docker:
   ```bash
   docker-compose up --build
   ```

   Esse comando irá construir as imagens e iniciar todos os containers definidos no arquivo `docker-compose.yml`.

5. **Acesse a API:**
   Após os containers estarem em execução, você pode acessar a API no seguinte endereço:
   ```
   http://localhost:8000
   ```

   Além disso, a documentação interativa da API estará disponível em:
   ```
   http://localhost:8000/docs
   ```

6. **Verifique os logs (opcional):**
   Para verificar os logs do projeto enquanto ele está rodando, use o comando:
   ```bash
   docker-compose logs -f
   ```

7. **Parar os containers:**
   Quando terminar de utilizar o projeto, você pode parar todos os containers com:
   ```bash
   docker-compose down
   ```

# Requisitos Mínimos

- **Docker**: Versão 20.10 ou superior
- **Docker Compose**: Versão 1.27 ou superior
- **Sistema Operacional**: Linux, macOS ou Windows (com Docker Desktop)
