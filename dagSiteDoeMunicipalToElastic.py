import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import fitz  # PyMuPDF
import cloudscraper

logging.basicConfig(level=logging.INFO)
logging.info(f"Processando o arquivo: {os.path.abspath(__file__)}")

# Obtém as variáveis do Airflow
ELASTIC_USER = Variable.get('elastic_chat_user')
ELASTIC_PASSWORD = Variable.get('elastic_chat_password')
ELASTIC_INDEX_ENDPOINT = Variable.get('elastic_doe_municipal_index_endpoint')
AUTH = (ELASTIC_USER, ELASTIC_PASSWORD)

INDEX_SETTINGS = {
    "settings": {
        "analysis": {
            "analyzer": {
                "custom_analyzer": {
                    "type": "standard",
                    "stopwords": "portuguese"
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "metadados": {
                "properties": {
                    "idPost": {"type": "integer"},
                    "postDate": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                    "postTitle": {"type": "text"},
                    "numDoe": {"type": "integer"},
                    "urlPdfDoe": {"type": "text"}
                }
            },
            "texto_doe": {"type": "text", "analyzer": "custom_analyzer"}
        }
    }
}

def index_exists():
    try:
        response = requests.head(ELASTIC_INDEX_ENDPOINT, auth=AUTH)
        return response.status_code == 200
    except requests.RequestException as e:
        logging.error(f"Erro ao verificar o índice: {e}")
        return False

def create_or_update_index():
    try:
        if not index_exists():
            logging.info(f"Índice não encontrado em {ELASTIC_INDEX_ENDPOINT}. Criando...")
            response = requests.put(ELASTIC_INDEX_ENDPOINT, json=INDEX_SETTINGS, auth=AUTH)
            response.raise_for_status()
            logging.info("Índice criado com sucesso.")
        else:
            logging.info("Índice já existe.")
    except requests.RequestException as e:
        logging.error(f"Erro ao criar ou atualizar o índice: {e}")

def exists_in_elastic(id_doc):
    try:
        url = f"{ELASTIC_INDEX_ENDPOINT}/_doc/{id_doc}"
        response = requests.head(url, auth=AUTH)
        return 200 <= response.status_code < 300
    except requests.RequestException as e:
        logging.error(f"Erro ao verificar documento no Elasticsearch: {e}")
        return False

def send_to_elastic(id_doc, data):
    try:
        url = f"{ELASTIC_INDEX_ENDPOINT}/_doc/{id_doc}"
        # Utilizando PUT para evitar duplicação (atualiza caso o documento já exista)
        response = requests.put(url, json=data, auth=AUTH)
        response.raise_for_status()
        logging.info(f"Documento salvo no Elasticsearch: {id_doc}")
    except requests.RequestException as e:
        logging.error(f"Erro ao enviar documento ao Elasticsearch: {e}")

def extract_text_from_pdf(pdf_bytes):
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        texto_extraido = ""
        for page_num in range(len(doc)):
            page = doc[page_num]
            blocks = page.get_text("blocks")
            if not blocks:
                continue
            x0_values = [b[0] for b in blocks]
            threshold = (min(x0_values) + max(x0_values)) / 2
            left_blocks = sorted([b for b in blocks if b[0] < threshold], key=lambda b: (b[1], b[0]))
            right_blocks = sorted([b for b in blocks if b[0] >= threshold], key=lambda b: (b[1], b[0]))
            linhas_vistas = set()
            def processa_blocos(blocos):
                texto_final = []
                for b in blocos:
                    for linha in b[4].split("\n"):
                        linha_limpa = linha.strip()
                        if linha_limpa and linha_limpa not in linhas_vistas:
                            linhas_vistas.add(linha_limpa)
                            texto_final.append(linha_limpa)
                return "\n".join(texto_final)
            page_text = f"--- Página {page_num + 1} ---\n"
            page_text += processa_blocos(left_blocks) + "\n"
            page_text += processa_blocos(right_blocks) + "\n\n"
            texto_extraido += page_text
        doc.close()
        return texto_extraido
    except Exception as e:
        logging.error(f"Erro ao extrair texto do PDF: {e}")
        return None

def download_pdf(date):
    base_url = "https://diariomunicipalaam.org.br/visualizar-publicacao/"
    date_str = date.strftime("%Y%m%d")
    url = f"{base_url}{date_str}"
    
    logging.info(f"Acessando URL: {url}")
    
    scraper = cloudscraper.create_scraper()
    response = scraper.get(url)
    logging.info(f"Status code: {response.status_code}")
    
    if response.status_code == 200:
        return response.content
    else:
        logging.warning(f"Erro: URL não encontrada para a data {date_str}")
        logging.warning(f"Conteúdo da resposta: {response.text[:500]}")
        return None

def processarPdf(**kwargs):
    """
    Processa o PDF para a data de execução fornecida (variável ds).
    """
    ds = kwargs.get("ds")
    if not ds:
        logging.error("Data de execução (ds) não fornecida.")
        return

    current_date = datetime.strptime(ds, "%Y-%m-%d")
    
    create_or_update_index()
    
    pdf_bytes = download_pdf(current_date)
    if not pdf_bytes:
        logging.warning(f"PDF não encontrado para a data {current_date.strftime('%Y-%m-%d')}")
        return
    
    id_post = current_date.strftime("%Y%m%d")
    if exists_in_elastic(id_post):
        logging.info(f"Documento já existe no Elasticsearch: {id_post}")
        return
    
    texto_doe = extract_text_from_pdf(pdf_bytes)
    if not texto_doe:
        logging.warning(f"Texto vazio ou não extraível para a data: {id_post}")
        return
    
    documento = {
        "metadados": {
            "idPost": id_post,
            "postDate": current_date.strftime('%Y-%m-%d %H:%M:%S'),
            "postTitle": f"Publicação de {current_date.strftime('%d/%m/%Y')}",
            "urlPdfDoe": f"https://diariomunicipalaam.org.br/visualizar-publicacao/{id_post}"
        },
        "texto_doe": texto_doe
    }
    send_to_elastic(id_doc=id_post, data=documento)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023,1,1)
    #'end_date': datetime.now(),
    #'retries': 3,
    #'retry_delay': timedelta(minutes=5)
}

with DAG(
    'dagSiteDoeMunicipalToElastic',
    default_args=default_args,
    description='Processa os pdfs e envia ao elastic',
    schedule_interval='0 21 * * *',  # Executa todos os dias às 21:00
    catchup=True,
    max_active_runs=1
) as dag:

    process_records = PythonOperator(
        task_id='process_records',
        python_callable=processarPdf,
        provide_context=True  # Agora a função receberá o contexto de execução
    )

    process_records
