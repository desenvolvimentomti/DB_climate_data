from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from models import AOIUploadResponse, AuthCredentials, AuthResponse, INPEDownloadParams, RemoteSensingDownloadParams
import aiofiles
import os
import time
import geopandas as gpd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

router = APIRouter(prefix="/input", tags=["Input Module"])


# --- FUNÇÃO AUXILIAR DE CARGA (LOGICA DO TESTE_LOAD_GIS.PY) ---
def load_shp_to_postgis_task(file_path: str, table_name: str):
    """
    Executa a carga do PostGIS em segundo plano para não travar a resposta da API.
    """
    try:
        # Recupera configurações do .env
        DB_NAME = os.getenv("DB_NAME")
        DB_USER = os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
        DB_PORT = os.getenv("DB_PORT", "5532")

        db_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?options=-csearch_path=public"
        engine = create_engine(db_string)

        # Lógica de Retry para conexão
        max_retries = 5
        for i in range(max_retries):
            try:
                with engine.connect() as conn:
                    break
            except Exception:
                time.sleep(2)
        else:
            raise Exception("Não foi possível conectar ao banco após retentativas.")

        # Leitura e Limpeza
        gdf = gpd.read_file(file_path)
        gdf.columns = gdf.columns.str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.lower()
        
        if gdf.crs is None:
            gdf.set_crs(epsg=4674, inplace=True) # Padrão SIRGAS 2000

        # Carga no PostGIS
        gdf.to_postgis(table_name, engine, if_exists='replace', index=False)
        print(f"✅ Tabela {table_name} populada com sucesso a partir de {file_path}")

    except Exception as e:
        print(f"❌ Erro na carga PostGIS: {str(e)}")
    finally:
        engine.dispose()




# --- ENDPOINT ATUALIZADO ---
#@router.post("/aoi/upload", response_model=AOIUploadResponse)
#async def upload_aoi_gis(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
#    temp_dir = "temp"
#    os.makedirs(temp_dir, exist_ok=True)
#    file_path = os.path.join(temp_dir, file.filename)
#    
#    # Salva o arquivo localmente
#    async with aiofiles.open(file_path, 'wb') as f:
#        content = await file.read()
#        await f.write(content)
#    
#    # Se for um arquivo .shp, adiciona a tarefa de carga ao banco de dados em background
#    if file.filename.endswith('.shp'):
#        # Define o nome da tabela baseado no nome do arquivo (sem extensão)
#        table_name = f"aoi_{os.path.splitext(file.filename)[0].lower()}"
#        background_tasks.add_task(load_shp_to_postgis_task, file_path, table_name)
        
#        return AOIUploadResponse(
#            message="AOI recebida e carga no PostGIS iniciada", 
#            file_path=file_path
#        )
    
#    return AOIUploadResponse(message="Arquivo recebido com sucesso", file_path=file_path)


@router.post("/aoi/upload", response_model=AOIUploadResponse)
async def upload_aoi(file: UploadFile = File(...)):
    # Placeholder: Save file to temp storage
    temp_dir = "temp"
    os.makedirs(temp_dir, exist_ok=True)
    file_path = os.path.join(temp_dir, file.filename)
    async with aiofiles.open(file_path, 'wb') as f:
        content = await file.read()
        await f.write(content)
    return AOIUploadResponse(message="AOI uploaded successfully", file_path=file_path)

@router.post("/auth/drive-gee", response_model=AuthResponse)
async def authenticate_drive_gee(credentials: AuthCredentials):
    # Placeholder: Authenticate with Google Drive and GEE
    # Use pydrive and ee
    try:
        # ee.Initialize(credentials.service_account_key)
        return AuthResponse(message="Authenticated successfully", authenticated=True)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/data/inpe/download")
async def download_inpe_fire_data(params: INPEDownloadParams):
    # Placeholder: Download CSV files
    # Save to S3 or local
    return {"message": "INPE fire data downloaded", "output_folder": params.output_folder}

@router.post("/data/remote-sensing/download")
async def download_remote_sensing_data(params: RemoteSensingDownloadParams):
    # Placeholder: Download GEDI/Sentinel data
    return {"message": "Remote sensing data downloaded"}