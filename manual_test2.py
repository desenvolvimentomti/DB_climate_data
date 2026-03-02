import os
import ee
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

# Weather data imports
import openmeteo_requests
# GIS/Geospatial imports
import geopandas as gpd

load_dotenv()

# CARREGANDO DADOS DE .ENV
service_account_path = os.getenv('GEE_SERVICE_ACCOUNT_JSON_PATH')

if not service_account_path or not os.path.exists(service_account_path):
    raise FileNotFoundError(f"Arquivo de credenciais não encontrado: {service_account_path}")

bucket_name = os.getenv('GCP_BUCKET_NAME')

if not bucket_name:
    raise ValueError("A variável GCP_BUCKET_NAME não foi configurada no .env")

# Use barras para frente para evitar SyntaxWarning no Windows       
local_file = "sample_centroids.csv" 
destination_blob = "resultado_teste_2.csv"



# 1. Inicialização do Earth Engine
try:
    # Definindo credenciais explicitamente para reuso
    credentials = service_account.Credentials.from_service_account_file(service_account_path)
    scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/earthengine', 'https://www.googleapis.com/auth/cloud-platform'])
    
    ee.Initialize(scoped_credentials)
    print(f"✅ Earth Engine inicializado!")
except Exception as e:
    print(f"❌ Erro EE: {e}")

# 2. Função de Upload com tratamento de erro
def upload_to_bucket(blob_name, file_path, bucket):
    try:
        # Reutiliza as mesmas credenciais para o Storage
        storage_client = storage.Client(credentials=credentials, project=credentials.project_id)
        bucket_obj = storage_client.bucket(bucket)
        blob = bucket_obj.blob(blob_name)
        
        blob.upload_from_filename(file_path)
        print(f"✅ Sucesso! Arquivo enviado para {bucket}/{blob_name}")
    except Exception as e:
        print(f"❌ Falha no upload: {e}")
        if "403" in str(e):
            print("👉 DICA: Verifique se a Service Account tem a permissão 'Storage Object Creator' no console do GCP.")

# Execução

if __name__ == "__main__":
    upload_to_bucket(destination_blob, local_file, bucket_name) #ja esta funcionando 

    print("Teste concluído. Verifique os logs para detalhes.")

    # now to use open-meteo . the weather Forecast API
    # teste de envio do projeto para um repositório proprietátio do MTI 
