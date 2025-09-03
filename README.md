# dagSiteDoeMunicipalToElastic
Pipeline em Airflow que baixa PDFs do Diário Municipal, extrai texto com PyMuPDF, e indexa os documentos no Elasticsearch com metadados, garantindo que não haja duplicações e que o índice seja criado/atualizado automaticamente.
