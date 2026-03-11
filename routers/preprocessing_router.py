from fastapi import APIRouter, HTTPException, UploadFile, File, BackgroundTasks

from models import GridGenerationParams, SICARPreprocessParams, BoundaryCalculationParams, RasterClippingParams, SICARS3ProcessParams
import boto3
import zipfile
import io
import os
import geopandas as gpd
import rasterio
from shapely.geometry import shape
import json

router = APIRouter(prefix="/geo", tags=["Preprocessing Module"])



# Função que fará o trabalho pesado
def process_s3_zip_task(s3_path: str, state: str):
    try:
        s3 = boto3.client('s3')
        bucket = os.getenv('AWS_S3_BUCKET')
        temp_zip = f"temp_{state}.zip"
        extract_path = f"data/sicar/{state}"
        
        os.makedirs(extract_path, exist_ok=True)

        # 1. Download do S3 para memória/arquivo temporário
        print(f"Baixando {s3_path}...")
        s3.download_file(bucket, s3_path, temp_zip)

        # 2. Extração
        with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        
        # 3. Conversão (Exemplo: Shapefile para GeoJSON para facilitar o Folium)
        # Procura por arquivos .shp extraídos
        for file in os.listdir(extract_path):
            if file.endswith(".shp"):
                gdf = gpd.read_file(os.path.join(extract_path, file))
                # Aqui você pode salvar no seu Banco de Dados (PostgreSQL/DuckDB)
                # gdf.to_postgis(...) 
                
                print(f"Processado: {file} com {len(gdf)} registros.")

        # Limpeza
        os.remove(temp_zip)
        print(f"Processamento de {state} concluído com sucesso.")

    except Exception as e:
        print(f"Erro no processamento em segundo plano: {e}")


@router.post("/process-sicar-s3")
async def process_sicar_s3(params: SICARS3ProcessParams, background_tasks: BackgroundTasks):
    # Adiciona a tarefa para rodar após a resposta ser enviada
    background_tasks.add_task(process_s3_zip_task, params.s3_path, params.state)
    
    return {
        "message": "Processamento iniciado em segundo plano",
        "s3_path": params.s3_path,
        "status": "running"
    }

@router.post("/generate-grid")
async def generate_grid(params: GridGenerationParams, shapefile: UploadFile = File(...)):
    # Placeholder: Generate grid from shapefile
    # Use geopandas to create grid
    return {"message": "Grid generated"}

@router.post("/preprocess-sicar")
async def preprocess_sicar(params: SICARPreprocessParams, zip_file: UploadFile = File(...)):
    # Placeholder: Preprocess SICAR data
    return {"message": "SICAR data preprocessed"}

@router.post("/calculate-boundaries")
async def calculate_boundaries(params: BoundaryCalculationParams):
    # Placeholder: Calculate spatial boundaries
    return {"message": "Boundaries calculated"}

@router.post("/clip-raster")
async def clip_raster(params: RasterClippingParams):
    # Placeholder: Clip raster
    # Use rasterio
    return {"message": "Raster clipped"}