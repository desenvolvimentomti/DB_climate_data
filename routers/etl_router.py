
import os
import tempfile
import boto3
import pytesseract
from SICAR import Sicar, State, Polygon
from SICAR.drivers import Tesseract
from typing import Dict

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

from datetime import datetime

# Reaproveitamos a estrutura de status  para ETL
etl_status: Dict[str, Dict] = {}

router = APIRouter(prefix="/etl", tags=["ETL Module"])


# -------------EXTRACT ------------ 

# --- FUNÇÃO AUXILIAR DE PROCESSAMENTO (BACKGROUND) ---
def sicar_download_upload_task(state_code: str, info_name: str):
    task_id = f"{state_code}_{info_name}"
    try:
        etl_status[task_id] = {"status": "solving_captcha", "progress": 15}
        
        # Configurações do ambiente extraídas do .env
        pytesseract.pytesseract.tesseract_cmd = os.getenv('TESSERACT_PATH')
        bucket_name = os.getenv('AWS_S3_BUCKET_ATUAL')

        # Inicializa cliente S3
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        etl_status[task_id] = {"status": "downloading", "progress": 40}

        # Mapeia a string para o Enum do SICAR
        state_enum = State[state_code.upper()]
        polygon_enum = Polygon[info_name.upper()]

        with tempfile.TemporaryDirectory() as temp_dir:
            # Inicializa Driver e API SICAR
            car = Sicar(driver=Tesseract)
            print(f"Iniciando download SICAR: {state_code} - {info_name}")
            
            result_path = car.download_state(state=state_enum, polygon=polygon_enum, folder=temp_dir)
            date_str = datetime.now().strftime("%Y-%m-%d")


            if result_path:
                object_name = f"{info_name}/{state_code}/{date_str}.zip"
                
                # Upload para S3
                etl_status[task_id] = {"status": "uploading_to_s3", "progress": 80}

                s3.upload_file(str(result_path), bucket_name, object_name)
                print(f"✅ Upload concluído: {object_name}")
                etl_status[task_id] = {"status": "completed", "progress": 100}

    except Exception as e:
        print(f"❌ Erro na tarefa SICAR: {str(e)}")
        etl_status[task_id] = {"status": "failed", "error": str(e), "progress": 0}

@router.get("/status/{state}/{info}")
async def get_etl_status(state: str, info: str):
    task_id = f"{state}_{info}"
    return etl_status.get(task_id, {"status": "idle", "progress": 0})

# --- NOVO ENDPOINT: SICAR/info/extract ---
@router.post("/sicar/info/extract")
async def extract_sicar_info(params: SICARExtractParams, background_tasks: BackgroundTasks):
    """
    Inicia o download de dados do SICAR e upload para o S3 em segundo plano.
    """
    task_id = f"{params.state}_{params.info}"
    etl_status[task_id] = {"status": "queued", "progress": 0}

    background_tasks.add_task(sicar_download_upload_task, params.state, params.info)
    return {
        "message": "Processo de extração SICAR iniciado",
        "state": params.state,
        "info": params.info,
        "task_id": task_id
    }



# ------------- TRANSFORM & LOAD -------------









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