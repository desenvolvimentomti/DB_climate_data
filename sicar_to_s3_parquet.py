import os
import tempfile
import zipfile
import boto3
import geopandas as gpd
import httpx
import pandas as pd
from dotenv import load_dotenv
from SICAR import Sicar, State, Polygon
from SICAR.drivers import Tesseract
import pytesseract
import fastparquet

from datetime import datetime
import time


from boto3.s3.transfer import TransferConfig
from botocore.config import Config


# 1. Configurações Iniciais
load_dotenv()
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')
bucket_name = os.getenv('AWS_S3_BUCKET_ATUAL')

# --- ADICIONE ESTAS LINHAS ---
tesseract_path = os.getenv('TESSERACT_PATH')
pytesseract.pytesseract.tesseract_cmd = tesseract_path
# -----------------------------

# 1. Configuração de Tolerância a Falhas
# Aumentamos o timeout e o número de tentativas automáticas
config_boto = Config(
    retries = {
        'max_attempts': 10,
        'mode': 'standard'
    },
    connect_timeout=600, # 10 minutos
    read_timeout=600
)



s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region,
    config=config_boto
)

# 2. Configuração de Transferência para arquivos grandes
# chunk
transfer_config = TransferConfig(
    multipart_threshold=1024 * 25, # 25MB (começa multipart acima disso)
    multipart_chunksize=1024 * 25, # Pedaços de 25MB
    max_concurrency=2,             # Reduzir concorrência ajuda em conexões instáveis
    use_threads=True
)


def process_and_upload(shp_path, state_code, info_code, bucket):
    """Lê o SHP, limpa, converte para Parquet e sobe para o S3"""
    try:
        print(f"--- Processando em Chunks: {os.path.basename(shp_path)} ---")

        # 1. Definimos o tamanho do bloco (ex: 50.000 linhas por vez)
        chunk_size = 50000
        temp_parquet = tempfile.NamedTemporaryFile(delete=False, suffix='.parquet')

        # Usamos o engine 'pyogrio' que permite leitura parcial (skip e max_rows)
        # Primeiro, descobrimos o total de linhas
        total_rows = len(gpd.read_file(shp_path, rows=0)) # Lê apenas o cabeçalho


        first_chunk = True
        for skip in range(0, total_rows, chunk_size):
            print(f"   -> Processando linhas {skip} até {min(skip + chunk_size, total_rows)}...")
            
            # Lê apenas um pedaço do arquivo
            gdf_chunk = gpd.read_file(shp_path, skip_features=skip, max_features=chunk_size)

            # Limpeza de colunas (mesma lógica anterior)
            gdf_chunk.columns = gdf_chunk.columns.str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.lower()
            
            if gdf_chunk.crs is None:
                gdf_chunk.set_crs(epsg=4674, inplace=True)

            # Conversão para WKT apenas deste pedaço
            gdf_chunk['geometry_wkt'] = gdf_chunk['geometry'].apply(lambda x: x.wkt if x else None)
            df_chunk = pd.DataFrame(gdf_chunk.drop(columns='geometry'))

            # Salva no arquivo Parquet (append mode não existe direto no to_parquet, 
            # então usamos o fastparquet ou simplesmente acumulamos se couber, 
            # mas para 2GB o ideal é escrever cada chunk e depois unir)
            # Dica: Se a RAM ainda chorar, salve chunks individuais e use s3.upload_part
            
            if first_chunk:
                df_chunk.to_parquet(temp_parquet.name, index=False, engine='fastparquet')
                first_chunk = False
            else:
                df_chunk.to_parquet(temp_parquet.name, index=False, engine='fastparquet', append=True)

        temp_parquet.close()





        #gdf = gpd.read_file(shp_path)

        # Limpeza de colunas 
        # alterando nomes


        #gdf.columns = gdf.columns.str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.lower()
        
        #alterando nomes das primeiras colunas, para evitar repetição de nomes e facilitar a leitura no Athena
        #if len(gdf.columns) > 0:        
        #    gdf.rename(columns={gdf.columns[0]: 'id'}, inplace=True)
        #if len(gdf.columns) > 1:        
        #    gdf.rename(columns={gdf.columns[1]: 'geometry'}, inplace=True)              




        # Garante o CRS SIRGAS 2000
        #if gdf.crs is None:
        #    gdf.set_crs(epsg=4674, inplace=True)

        # TRUQUE PARA O ATHENA: 
        # O Athena lê melhor geometrias se elas forem strings no formato WKT
        #print(f"--- Convertendo para WKT: {os.path.basename(shp_path)} ---")
        
       # gdf['geometry_wkt'] = gdf['geometry'].apply(lambda x: x.wkt if x else None)
        
        # Removemos a coluna 'geometry' original (binária) para salvar como Parquet comum
        #df_final = pd.DataFrame(gdf.drop(columns='geometry'))

        # Salva temporariamente como Parquet


        #temp_parquet = tempfile.NamedTemporaryFile(delete=False, suffix='.parquet')
        #df_final.to_parquet(temp_parquet.name, index=False)
        #temp_parquet.close()





        # Define o caminho no S3 (Particionado por estado para economizar no Athena)

        date_str = datetime.now().strftime("%Y-%m-%d")
        file_name = os.path.basename(shp_path).replace('.shp', '.parquet')


        # Extraímos o nome do tipo de dado do próprio nome do arquivo (ex: AREA_IMOVEL)
        tipo_dado_clean = os.path.basename(shp_path).replace('_1.shp', '').replace('.shp', '')

        object_name = (
            f"athena_data/"
            f"estado={state_code}/"
            f"tipo_dado={info_code}/"
            f"dt_proc={date_str}/"
            f"{file_name}"
)




        #object_name = f"athena_data/{state_code}/{info_code}/{date_str}/{file_name}"
        # No seu script Python, mude o caminho para:
        #object_name = f"athena_data/estado={state_code}/tipo_dado={info_code}/dt_proc={date_str}/{file_name}"

        # Upload
        print(f"Fazendo Upload : s3://{bucket}/{object_name}")

        #s3.upload_file(temp_parquet.name, bucket, object_name)
        s3.upload_file(
            temp_parquet.name, 
            bucket, 
            object_name,
            Config=transfer_config # Usando a nossa configuração de transferência
        )

        print(f"✅ Upload concluído: s3://{bucket}/{object_name}")

        # Limpeza
        os.remove(temp_parquet.name)

    except Exception as e:
        print(f"❌ Erro no processamento de {shp_path}: {e}")

def check_s3_exists(bucket, state_code, tipo_dado, date_str):
    """Verifica se já existe algum arquivo na pasta da partição específica"""
    prefix = f"athena_data/estado={state_code}/tipo_dado={tipo_dado}/dt_proc={date_str}/"
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return 'Contents' in response

def check_file_exists_s3(bucket, state_code, tipo_dado, date_str, file_name):
    """Verifica se o arquivo específico já existe no S3"""
    target_key = f"athena_data/estado={state_code}/tipo_dado={tipo_dado}/dt_proc={date_str}/{file_name.replace('.shp', '.parquet')}"
    try:
        s3.head_object(Bucket=bucket, Key=target_key)
        return True
    except:
        return False

def download_and_convert(state: State, info: Polygon):
    state_code = state.value
    info_code = info.value

    folder = tempfile.mkdtemp()
    
    try:
        print(f"\nIniciando download: {state_code} - {info_code}")
        car = Sicar(driver=Tesseract)
        # Baixa o ZIP
        zip_path = car.download_state(state, info, folder=folder)

        if not zip_path:
            print(f"❌ SICAR download failed for {state_code}")
            print(f"❌ SICAR download failed for {info_code}")
            return

        # Extrai o ZIP para achar o .shp
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            extract_path = os.path.join(folder, "extracted")
            zip_ref.extractall(extract_path)

            # Procura por arquivos .shp dentro da pasta extraída
            for root, dirs, files in os.walk(extract_path):
                for file in files:
                    if file.endswith(".shp"):
                        full_shp_path = os.path.join(root, file)
                        process_and_upload(full_shp_path, state_code, info_code, bucket_name)

    finally:
        # Limpa tudo no final
        import shutil
        shutil.rmtree(folder)

#if __name__ == "__main__":
#    # Testando com estados selecionados
#    for state in [State.AC, State.AL]:
#        for selected_info in [Polygon.CONSOLIDATED_AREA, Polygon.APPS]:
#
#            download_and_convert(state, selected_info)

if __name__ == "__main__":

# 'State' contém todos os estados brasileiros (AC, AL, AM, AP, BA, etc.)
    all_states = list(State) 
    
    # Polígonos que você deseja baixar para cada estado
    polygons = list(Polygon)  # Isso inclui AREA_PROPERTY, APPS, CONSOLIDATED_AREA, etc.

    date_str = datetime.now().strftime("%Y-%m-%d")


    print("poligons:", [p.name for p in polygons])


    for state in all_states:
        for selected_info in polygons:
            print(f"\n=== Iniciando processo para {state.value} - selec info value {selected_info.value} e selec info name {selected_info.name} selec info {selected_info} ===")

            state_code = state.value
            # Pegamos o nome limpo do polígono para a pasta
            tipo_dado = selected_info.value  # nomes em português 

            # VERIFICAÇÃO DE DUPLICIDADE
            if check_s3_exists(bucket_name, state_code, tipo_dado, date_str):
                print(f"⏭️ Pulando {state_code} - {tipo_dado}: Já existe no S3 para a data {date_str}.")
                continue
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    download_and_convert(state, selected_info)
                    time.sleep(5)
                    break  # Se deu certo, sai do loop de tentativas
                except (httpx.ReadTimeout, Exception) as e:
                    if attempt < max_retries - 1:
                        wait_time = 30 * (attempt + 1)
                        print(f"⚠️ Timeout/Erro no {state.value} - {selected_info.value}. Tentando novamente em {wait_time}s... ({attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        print(f"❌ Falha definitiva para {state.value} - {selected_info.value} após {max_retries} tentativas.")