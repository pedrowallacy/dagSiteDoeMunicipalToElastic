# DAG Airflow - Diário Municipal para Elasticsearch

Este projeto implementa uma DAG no **Apache Airflow** para automatizar o download de PDFs do Diário Municipal, extrair seu conteúdo textual e indexar no **Elasticsearch**.

## ⚙️ Funcionalidades
- Baixa PDFs do [Diário Municipal](https://diariomunicipalaam.org.br/).
- Extrai o texto usando **PyMuPDF (fitz)**.
- Cria ou atualiza automaticamente o índice no **Elasticsearch** com análise em português.
- Evita duplicações, usando o ID da publicação como chave única.
- Armazena metadados como `idPost`, `postDate`, `postTitle` e `urlPdfDoe`.

## 🚀 Estrutura
- **Airflow DAG**: agenda a execução diária às 21h.
- **Elasticsearch**: recebe e indexa os documentos.
- **Cloudscraper**: contorna bloqueios para obter o PDF.
- **PyMuPDF**: faz a extração estruturada do texto.

## 📅 Agendamento
A DAG roda diariamente às **21:00** (`0 21 * * *`) e permite *catchup* para processar datas anteriores.

## 📦 Requisitos
- Apache Airflow
- requests
- cloudscraper
- PyMuPDF
- Elasticsearch ativo e configurado

## 🔑 Variáveis no Airflow
Defina no Airflow as seguintes variáveis:
- `elastic_chat_user`
- `elastic_chat_password`
- `elastic_doe_municipal_index_endpoint`

## ▶️ Execução
1. Copie o arquivo da DAG para o diretório do Airflow (`dags/`).
2. Configure as variáveis no Airflow.
3. Ative a DAG na interface web.
4. Acompanhe o processamento e o envio ao Elasticsearch.

---
