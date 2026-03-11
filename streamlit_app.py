import os

import streamlit as st
import json
import httpx
import pandas as pd
from pathlib import Path
# ---changes to use S3 bucket---

import boto3
from dotenv import load_dotenv

load_dotenv() # Carrega as chaves do .env
# ----
import time
import folium
from streamlit_folium import st_folium

BASE_URL = "http://127.0.0.1:8000"
DATA_DIR = Path(__file__).parent / "data"

ERA5_VARIABLES = [
    'dewpoint_temperature_2m', 'temperature_2m', 'temperature_2m_min', 'temperature_2m_max',
    'soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3', 'soil_temperature_level_4',
    'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2', 'volumetric_soil_water_layer_3', 'volumetric_soil_water_layer_4',
    'surface_net_solar_radiation_sum', 'surface_net_thermal_radiation_sum',
    'surface_solar_radiation_downwards_sum', 'surface_thermal_radiation_downwards_sum',
    'evaporation_from_bare_soil_sum', 'evaporation_from_the_top_of_canopy_sum',
    'evaporation_from_vegetation_transpiration_sum', 'potential_evaporation_sum', 'total_evaporation_sum',
    'runoff_sum', 'sub_surface_runoff_sum', 'surface_runoff_sum',
    'u_component_of_wind_10m', 'v_component_of_wind_10m',
    'total_precipitation_sum', 'leaf_area_index_high_vegetation', 'leaf_area_index_low_vegetation'
]

st.set_page_config(page_title="Geo Analysis Frontend", layout="wide")
st.title("Geospatial Data Analysis — Frontend")

st.sidebar.title("Controls")
module = st.sidebar.selectbox("Module", ["Input", "Preprocessing", "ETL", "Analysis", "Report"]) 

# Filtro de Estados Brasileiros
BR_STATES = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", 
             "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]

st.sidebar.markdown("---")
selected_state = st.sidebar.selectbox("Filter by Brazilian State", BR_STATES)

# Helper: load default files
@st.cache_data
def load_default_aoi():
    path = DATA_DIR / "sample_aoi.geojson"
    return json.loads(path.read_text())

@st.cache_data
def load_default_fire():
    path = DATA_DIR / "sample_fire.csv"
    return pd.read_csv(path)

# Helper: call API with fallback
def post_or_fallback(path, json_data=None, files=None):
    url = BASE_URL + path
    try:
        with httpx.Client(timeout=30) as client:
            if files:
                resp = client.post(url, files=files)
            else:
                resp = client.post(url, json=json_data)
            resp.raise_for_status()
            return resp.json(), True
    except Exception as e:
        return {"error": str(e), "used_fallback": True}, False

def get_or_fallback(path, params=None):
    url = BASE_URL + path
    try:
        with httpx.Client(timeout=30) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            return resp.json(), True
    except Exception as e:
        return {"error": str(e), "used_fallback": True}, False

# UI for Input module
if module == "Input":
    st.header("Input — AOI / Authentication / Downloads")
    

    # --- Nova Seção de Filtro S3 ---
    st.subheader(f"S3 Data Explorer - State: {selected_state}")
    
    def list_s3_contents(state):
        try:
            s3 = boto3.client('s3')
            bucket = os.getenv('AWS_S3_BUCKET')

            # Ajuste do Prefixo para o seu caminho específico
            # Resultado esperado: "SICAR_data/AC/"
            custom_prefix = f"SICAR_data/{state}/"
            # Busca arquivos que começam com a sigla do estado (ex: "SP/")
            response = s3.list_objects_v2(Bucket=bucket, Prefix=custom_prefix  )
            
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []
        except Exception as e:
            return [f"Error connecting to S3: {str(e)}"]

    s3_files = list_s3_contents(selected_state)
    
    if s3_files:
        selected_file = st.selectbox(
            "Select file from S3 bucket", 
            s3_files,
            format_func=lambda x: x.replace(f"SICAR_data/{selected_state}/", "")
            )
        st.info(f"Ready to process: {selected_file}")
    else:
        st.warning(f"No data found in S3 for state {selected_state}")
    
    st.markdown("---")

    
    # 1. Initialize 'aoi' as None so it always exists in this scope
    aoi = None


    # preprocessing the s3 data to show 

    # UI for Preprocessing, acording to the state
    if module == "Input" and s3_files:
        if st.button("🚀 Processar ZIP do S3 via API"):
            # selected_file contém o caminho como 'SICAR_data/AC/CONSOLIDATED_AREA.zip'
            params = {
                "s3_path": selected_file, 
                "state": selected_state
            }
            
            # Chama o novo endpoint da API
            result, ok = post_or_fallback("/geo/process-sicar-s3", json_data=params)
            
            if ok:
                st.success("A API recebeu o pedido e está processando os dados no S3!")
                st.json(result)
                
                # 2. Cria placeholders para atualização dinâmica
                status_container = st.empty()
                progress_bar = st.progress(0)
                # 3. Loop de monitoramento (Polling)
                finished = False
                while not finished:
                    # Consulta o novo endpoint /status/{state}
                    status_data, status_ok = get_or_fallback(f"/geo/status/{selected_state}")
                    
                    if status_ok:
                        prog = status_data.get("progress", 0)
                        stat = status_data.get("status", "idle")
                        
                        # Atualiza a interface
                        progress_bar.progress(prog)
                        status_container.info(f"Status: **{stat.upper()}** ({prog}%)")
                        
                        if stat in ["completed", "failed"]:
                            finished = True
                            if stat == "completed":
                                st.success("Processamento finalizado com sucesso!")
                            else:
                                st.error(f"Erro no processamento: {status_data.get('error')}")
                    
                    time.sleep(2) # Aguarda 2 segundos antes da próxima consulta



            else:
                st.error("Falha ao comunicar com a API de processamento.")

    st.subheader("AOI Upload")
    uploaded = st.file_uploader("Upload AOI (GeoJSON / Shapefile zip)", type=["geojson", "zip"], key="aoi")

    if uploaded is None:
        st.info("No file uploaded — using default sample AOI")
        try: 

            aoi = load_default_aoi()
        except Exception as e:
            st.error(f"Could not load default AOI: {e}")
    else:

        #st.json(aoi)
        #else:
        st.write("Uploaded file:", uploaded.name)
        if uploaded.type == "application/json" or uploaded.name.endswith("geojson"):
            aoi = json.load(uploaded)
            st.json(aoi)
        else:
            st.write("File saved to /tmp for processing by API")

    # 2. Only attempt to use 'aoi' if it was successfully defined
    if aoi:
        st.subheader("Map Visualization (Leaflet)")
        m = folium.Map(location=[-15.78, -47.93], zoom_start=4)
        folium.GeoJson(aoi, name="Area of Interest").add_to(m)
        st_folium(m, width=1100, height=500)
    else:
        st.warning("No AOI data available to display on the map.")



    if st.button("Send AOI to API (POST /input/aoi/upload)"):
        if uploaded:
            files = {"file": (uploaded.name, uploaded.getvalue())}
            result, ok = post_or_fallback(f"/input/aoi/upload", files=files)
            st.json(result)
        else:
            # send default as file content
            files = {"file": ("sample_aoi.geojson", json.dumps(load_default_aoi()))}
            result, ok = post_or_fallback(f"/input/aoi/upload", files=files)
            if not ok:
                st.warning("API unreachable — simulated upload result shown")
                st.write({"message": "AOI uploaded successfully (local simulation)", "file_path": str(DATA_DIR / "sample_aoi.geojson")})
            else:
                st.json(result)



    st.markdown("---")
    st.subheader("INPE fire download (simulate)")
    fire_df = load_default_fire()
    st.write("Sample fire points:")
    st.dataframe(fire_df)
    if st.button("Call /input/data/inpe/download"):
        params = {"csv_path": "sample_fire.csv", "id_column": "id", "output_folder": "outputs"}
        result, ok = post_or_fallback("/input/data/inpe/download", json_data=params)
        if not ok:
            st.warning("API unreachable — showing local sample data instead")
            st.success("INPE fire data downloaded (local sample)")
            st.dataframe(fire_df)
        else:
            st.json(result)



# UI for Preprocessing
if module == "Preprocessing":
    st.header("Preprocessing — Grid / SICAR / Clip Raster")
    st.subheader("Generate Grid")
    resolution = st.number_input("Grid resolution (deg)", value=0.01)
    st.write("Using sample AOI: ")
    st.json(load_default_aoi())
    if st.button("Call /geo/generate-grid"):
        params = {"resolution_grid": resolution}
        result, ok = post_or_fallback("/geo/generate-grid", json_data=params)
        if not ok:
            st.warning("API unreachable — showing simulated grid result")
            st.json({"message": "Grid generated (simulated)", "cells": 100})
        else:
            st.json(result)

    st.markdown("---")
    st.subheader("Clip Raster")
    raster_path = st.text_input("Raster path (or leave default)", value="/path/to/sample_raster.tif")
    if st.button("Call /geo/clip-raster"):
        params = {"raster_path": raster_path, "clipping_geometry": load_default_aoi()}
        result, ok = post_or_fallback("/geo/clip-raster", json_data=params)
        if not ok:
            st.warning("API unreachable — simulated raster clipping completed")
            st.success("Raster clipped locally (simulated)")
        else:
            st.json(result)

# UI for ETL
if module == "ETL":
    st.header("ETL — SICAR Extraction")
    
    # Sub-header conforme solicitado
    st.subheader("SICAR/info/extract")


    # Seguindo exatamente as suas instruções de nomes de variáveis
    sicar_info_map = {
        "Área do Imóvel": "AREA_PROPERTY",
        "Áreas de Preservação Permanente (APP)": "APPS",
        "Vegetação Nativa": "NATIVE_VEGETATION",
        "Área Consolidada": "CONSOLIDATED_AREA",
        "Área de Pousio": "AREA_FALL",
        "Hidrografia": "HYDROGRAPHY",
        "Uso Restrito": "RESTRICTED_USE",
        "Servidão Administrativa": "ADMINISTRATIVE_SERVICE",
        "Reserva Legal": "LEGAL_RESERVE"
    }
    
    # Opções baseadas no Polygon Enum do SICAR
    # 2. Exibição das opções em Português no selectbox
    selected_label = st.selectbox("Selecione o tipo de dado", list(sicar_info_map.keys()))
    # 3. Atribuição do valor em Inglês para a variável selected_info
    selected_info = sicar_info_map[selected_label]
    # Exibição para o desenvolvedor confirmar (opcional)
    st.caption(f"Valor interno a ser enviado: `{selected_info}`")

    #info_options = ["AREA_CONSOLIDADA", "RESERVA_LEGAL", "AREA_IMOVEL", "VEGETACAO_NATIVA"]
    #selected_info = st.selectbox("Selecione o tipo de dado", info_options)
    
    if st.button("Executar Extração SICAR"):
        # selected_state vem do seletor global que criamos anteriormente
        payload = {"state": selected_state, "info": selected_info}
        
        result, ok = post_or_fallback("/etl/sicar/info/extract", json_data=payload)
        if ok:
            st.success(f"Extração de {selected_info} para {selected_state} enviada para a fila.")
            st.json(result)

            status_container = st.empty()
            progress_bar = st.progress(0)
            
            # 2. Loop de monitoramento em tempo real
            finished = False
            while not finished:
                # Consulta o status específico da tarefa ETL
                status_url = f"/etl/status/{selected_state}/{selected_info}"
                status_data, s_ok = get_or_fallback(status_url)
                
                if s_ok:
                    prog = status_data.get("progress", 0)
                    stat = status_data.get("status", "idle")
                    
                    progress_bar.progress(prog)
                    status_container.warning(f"Tarefa: {selected_info} | Status: {stat.upper()}...")
                    
                    if stat in ["completed", "failed"]:
                        finished = True
                        if stat == "completed":
                            status_container.success(f"✅ Extração de {selected_state} concluída e salva no S3!")
                        else:
                            st.error(f"Erro: {status_data.get('error')}")
                
                time.sleep(3) # O download é lento, então verificamos a cada 3 segundos
        else:
            st.error("Não foi possível iniciar a extração.")



if module == "ETL":
    st.header("ETL — Sentinel / LULC / IBGE PAM / Climate")
    st.subheader("Process Sentinel ETL")
    if st.button("Call /etl/sentinel/process"):
        params = {"sentinel_files": [], "farm_grid_shapefile": "sample_grid.shp"}
        result, ok = post_or_fallback("/etl/sentinel/process", json_data=params)
        if not ok:
            st.warning("API unreachable — simulated ETL result")
            st.json({"message": "Sentinel data processed (simulated)"})
        else:
            st.json(result)

    st.markdown("---")
    st.subheader("Extract LULC percentage")
    if st.button("Call /etl/lulc/extract-percentage"):
        params = {"mapbiomas_raster": "mapbiomas.tif", "aoi_geometry": load_default_aoi()}
        result, ok = post_or_fallback("/etl/lulc/extract-percentage", json_data=params)
        if not ok:
            st.warning("API unreachable — simulated LULC percentages")
            st.json({"forest": 60.5, "agriculture": 30.0, "water": 9.5})
        else:
            st.json(result)

    st.markdown("---")
    st.subheader("Climate Data ETL")
    st.write("ERA5 Extraction")
    era5_start = st.date_input("Start Date", value=pd.to_datetime("2025-01-01"))
    era5_end = st.date_input("End Date", value=pd.to_datetime("2025-06-30"))
    if st.button("Extract ERA5 Data"):
        params = {
            "centroids_shapefile": str(DATA_DIR / "sample_centroids.csv"),  # Use CSV
            "start_date": era5_start.strftime("%Y-%m-%d"),
            "end_date": era5_end.strftime("%Y-%m-%d"),
            "output_folder": str(DATA_DIR / "climate_output"),
            "variables": ERA5_VARIABLES[:5]  # Subset for demo
        }
        result, ok = post_or_fallback("/etl/climate/era5/extract", json_data=params)
        if not ok:
            st.warning("API unreachable — simulated ERA5 extraction")
            st.success("ERA5 data extracted (simulated)")
        else:
            st.json(result)

    st.write("Open-Meteo Download")
    if st.button("Download Open-Meteo Data"):
        params = {
            "centroids_shapefile": str(DATA_DIR / "sample_centroids.csv"),
            "output_file": str(DATA_DIR / "openmeteo_data.parquet"),
            "past_days": 5,
            "forecast_days": 3
        }
        result, ok = post_or_fallback("/etl/climate/openmeteo/download", json_data=params)
        if not ok:
            st.warning("API unreachable — simulated Open-Meteo download")
            st.success("Open-Meteo data downloaded (simulated)")
        else:
            st.json(result)

    st.write("Process Climate Data")
    if st.button("Process Climate Data"):
        params = {
            "era5_raw_files": [str(DATA_DIR / "climate_output" / "raw_era5_data_20250101_20250630.parquet")],
            "openmeteo_file": str(DATA_DIR / "openmeteo_data.parquet"),
            "output_folder": str(DATA_DIR / "processed_climate")
        }
        result, ok = post_or_fallback("/etl/climate/process", json_data=params)
        if not ok:
            st.warning("API unreachable — simulated climate processing")
            st.success("Climate data processed (simulated)")
        else:
            st.json(result)

# UI for Analysis
if module == "Analysis":
    st.header("Analysis — Crop Age / AGB / CAR status")
    st.subheader("Breakeven calculation (example)")
    fixed_cost = st.number_input("Fixed cost", value=10000.0)
    herd_sizes = st.text_input("Herd sizes (comma)", value="10,20,30")
    carbon_yield = st.number_input("Carbon yield", value=1.0)
    if st.button("Call /analysis/breakeven-point"):
        try:
            herd_list = [int(x.strip()) for x in herd_sizes.split(",") if x.strip()]
        except Exception:
            herd_list = [10,20]
        params = {"fixed_cost": fixed_cost, "herd_sizes": herd_list, "carbon_yield": carbon_yield, "verra_levy": 0.0, "product_cost_annual": 0.0}
        result, ok = get_or_fallback("/analysis/breakeven-point", params=params)
        if not ok:
            st.warning("API unreachable — simulated breakeven")
            st.json({"breakeven_per_herd": [1000,2000]})
        else:
            st.json(result)

# UI for Report
if module == "Report":
    st.header("Report — Dashboard / PDF / Figures")
    st.subheader("Dashboard PAM (sample)")
    if st.button("Call /report/dashboard-pam"):
        params = {"pam_geoparquet": "sample_pam.gpkg", "selected_culture": "soy", "selected_uf": "MG"}
        result, ok = get_or_fallback("/report/dashboard-pam", params=params)
        if not ok:
            st.warning("API unreachable — showing simulated dashboard summary")
            st.json({"message": "Dashboard generated (simulated)", "charts": 3})
        else:
            st.json(result)

    st.markdown("---")
    st.subheader("Generate monitoring report (PDF)")
    if st.button("Call /report/generate-report"):
        params = {"truecolor_images": [], "area_location_data": "sample_area"}
        result, ok = post_or_fallback("/report/generate-report", json_data=params)
        if not ok:
            st.warning("API unreachable — simulated report generated")
            st.success("Report generated locally: outputs/report_sample.pdf (simulated)")
        else:
            st.json(result)

st.sidebar.markdown("---")
st.sidebar.write("API base:", BASE_URL)
st.sidebar.write("Data folder:", str(DATA_DIR))

st.sidebar.info("If the FastAPI server is not running, the app will use local simulated results and sample data.")
