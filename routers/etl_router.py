
import os
import tempfile
import boto3
import pytesseract
from SICAR import Sicar, State, Polygon
from SICAR.drivers import Tesseract

from fastapi import APIRouter, HTTPException, BackgroundTasks
from models import (
    SICARExtractParams,
    SentinelETLParams,
    LULCExtractParams,
    IBGEPAMProcessParams,
    ERA5ExtractParams,
    OpenMeteoDownloadParams,
    ClimateProcessParams,
)
from climate_etl import extract_era5_data, download_openmeteo_data, process_climate_data

router = APIRouter(prefix="/etl", tags=["ETL Module"])


# --- FUNÇÃO AUXILIAR DE PROCESSAMENTO (BACKGROUND) ---
def sicar_download_upload_task(state_code: str, info_name: str):
    try:
        # Configurações do ambiente extraídas do .env
        pytesseract.pytesseract.tesseract_cmd = os.getenv('TESSERACT_PATH')
        bucket_name = os.getenv('AWS_S3_BUCKET')

        # Inicializa cliente S3
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )

        # Mapeia a string para o Enum do SICAR
        state_enum = State[state_code.upper()]
        polygon_enum = Polygon[info_name.upper()]

        with tempfile.TemporaryDirectory() as temp_dir:
            # Inicializa Driver e API SICAR
            car = Sicar(driver=Tesseract)
            print(f"Iniciando download SICAR: {state_code} - {info_name}")
            
            result_path = car.download_state(state=state_enum, polygon=polygon_enum, folder=temp_dir)

            if result_path:
                object_name = f"SICAR_data/{state_code}/{info_name}.zip"
                # Upload para S3
                s3.upload_file(str(result_path), bucket_name, object_name)
                print(f"✅ Upload concluído: {object_name}")

    except Exception as e:
        print(f"❌ Erro na tarefa SICAR: {str(e)}")

# --- NOVO ENDPOINT: SICAR/info/extract ---
@router.post("/sicar/info/extract")
async def extract_sicar_info(params: SICARExtractParams, background_tasks: BackgroundTasks):
    """
    Inicia o download de dados do SICAR e upload para o S3 em segundo plano.
    """
    background_tasks.add_task(sicar_download_upload_task, params.state, params.info)
    return {
        "message": "Processo de extração SICAR iniciado",
        "state": params.state,
        "info": params.info
    }



@router.post("/sentinel/process")
async def process_sentinel_etl(params: SentinelETLParams):
    # Placeholder: ETL for Sentinel-2
    return {"message": "Sentinel data processed"}

@router.post("/lulc/extract-percentage")
async def extract_lulc_percentage(params: LULCExtractParams):
    # Placeholder: Extract LULC percentages
    return {"message": "LULC percentages extracted"}

@router.post("/ibge/process-pam")
async def process_ibge_pam(params: IBGEPAMProcessParams):
    # Placeholder: Process IBGE PAM data
    return {"message": "PAM data processed"}

@router.post("/climate/era5/extract")
async def extract_era5_etl(params: ERA5ExtractParams):
    try:
        result = await extract_era5_data(params)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/climate/openmeteo/download")
async def download_openmeteo_etl(params: OpenMeteoDownloadParams):
    try:
        result = await download_openmeteo_data(params)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/climate/process")
async def process_climate_etl(params: ClimateProcessParams):
    try:
        result = await process_climate_data(params)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))