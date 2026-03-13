import geopandas as gpd
import time
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()


def import_shp_to_postgis(shp_file_path, dbname, user, password, host, port, table_name):
    """
    Importa um arquivo Shapefile para uma tabela PostGIS usando SQLAlchemy e GeoPandas,
    seguindo a lógica de retry e limpeza de colunas do import_data.py.
    """
    try:
        # 1. Mesma lógica de string de conexão (adicionado ?options=-csearch_path=public)
        # Nota: Se der erro de "NoSuchModule", use 'postgresql+psycopg://'
        db_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?options=-csearch_path=public"
        engine = create_engine(db_string)

        # 2. Lógica de Retry (Igual ao seu import_data.py)
        max_retries = 10
        for i in range(max_retries):
            try:
                conn = engine.connect()
                print("Conexão com o PostgreSQL estabelecida.")
                conn.close()
                break
            except Exception as e:
                print(f"Aguardando o banco de dados ficar disponível... Tentativa {i+1}/{max_retries}")
                time.sleep(2)
        else:
            raise Exception("Não foi possível conectar ao banco de dados após várias tentativas.")

        # 3. Lê o arquivo Shapefile (GeoPandas)
        print(f"Lendo arquivo SHP: {shp_file_path}")
        gdf = gpd.read_file(shp_file_path)
        print("Arquivo SHP lido com sucesso.")

        # 4. Limpa os nomes das colunas (Igual ao seu import_data.py)
        gdf.columns = gdf.columns.str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.lower()
        
        # 5. Garante o CRS (Sistema de Coordenadas) para o PostGIS
        if gdf.crs is None:
            gdf.set_crs(epsg=4674, inplace=True) # Padrão SICAR (SIRGAS 2000)

        # 6. Insere os dados diretamente na tabela usando to_postgis
        # if_exists='replace' garante que a tabela seja criada/sobrescrita

        # 6. Insere os dados em lotes (Chunks) para evitar MemoryError
        chunk_size = 5000  # Ajuste este número se ainda der erro (ex: 2000)
        total_rows = len(gdf)
        print(f"Iniciando carga de {total_rows} linhas em lotes de {chunk_size}...")

        for i in range(0, total_rows, chunk_size):
            chunk = gdf.iloc[i:i + chunk_size]
            
            # O primeiro lote cria a tabela ('replace'), os próximos adicionam ('append')
            mode = 'replace' if i == 0 else 'append'
            
            chunk.to_postgis(
                name=table_name, 
                con=engine, 
                if_exists=mode, 
                index=False
            )
            print(f"Progress: {min(i + chunk_size, total_rows)}/{total_rows} processados...")


        #gdf.to_postgis(table_name, engine, if_exists='replace', index=False)
        
        print(f"Dados espaciais importados com sucesso para a tabela '{table_name}'.")

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
    finally:
        if 'engine' in locals():
            engine.dispose()
            print("Conexão com o PostgreSQL fechada.")

if __name__ == "__main__":
    # Configurações do banco de dados (extraídas do .env)
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")

    # Caminho do SHP (usando o 'r' para evitar erro de barras no Windows)
    SHP_FILE = r"data\sicar\MG\AREA_IMOVEL_1.shp" # AREA_CONSOLIDADA_1.shp , AREA_IMOVEL_1.shp
    TABLE_NAME = "sicara_mg_AREA_IMOVEL_1"

    import_shp_to_postgis(SHP_FILE, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, TABLE_NAME)