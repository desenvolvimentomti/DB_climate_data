import os
import tempfile
import boto3
from dotenv import load_dotenv

# SICAR API imports
from SICAR import Sicar, State, Polygon
#from SICAR.drivers import Paddle

import pytesseract
from SICAR.drivers import Tesseract



load_dotenv()

# CARREGANDO DADOS DE .ENV
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')
bucket_name = os.getenv('AWS_S3_BUCKET')

tesseract_path = os.getenv('TESSERACT_PATH')
disable_paddle = os.getenv('PADDLE_DISABLE_CHECK', 'True')


if not all([aws_access_key_id, aws_secret_access_key, aws_region, bucket_name]):
    raise ValueError("Variáveis de ambiente AWS não configuradas corretamente no .env")

# 3. Apply the settings
os.environ['PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK'] = disable_paddle
pytesseract.pytesseract.tesseract_cmd = tesseract_path

# Lista de estados brasileiros (usar enum do SICAR)
states = list(State)

# Initialize the S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

# Função de Upload para o bucket
def upload_to_bucket(object_name, file_path, bucket):
    try:
        s3.upload_file(file_path, bucket, object_name)
        print(f"✅ {file_path} uploaded to {bucket}/{object_name}")
    except Exception as e:
        print(f"❌ Error: {e}")

# Função para baixar usando SICAR e fazer upload para um estado
def download_and_upload_state(state: State):
    state_code = state.value
    try:
        print(f"Fetching state {state_code} via SICAR API...")
        folder = tempfile.mkdtemp()

        car = Sicar(driver=Tesseract)   # Use Tesseract OCR driver

        result_path = car.download_state(state, Polygon.CONSOLIDATED_AREA, folder=folder)

        if not result_path:
            print(f"❌ SICAR download failed for {state_code}")
            return

        object_name = f"SICAR_data/{state_code}/CONSOLIDATED_AREA.zip"
        upload_to_bucket(object_name, str(result_path), bucket_name)

        try:
            os.remove(result_path)
        except Exception:
            pass
        try:
            os.rmdir(folder)
        except Exception:
            pass

    except Exception as e:
        print(f"❌ Error processing {state_code}: {e}")

if __name__ == "__main__":
    print("Iniciando download dos dados CAR por estado via SICAR API...")
    #for state in states:
    for state in [State.AC, State.AL]:  # Testar apenas com 2 estados para evitar muitos downloads
        download_and_upload_state(state)
    print("Processo concluído. Verifique os logs para detalhes.")
