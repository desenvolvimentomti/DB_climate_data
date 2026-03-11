# Geospatial Data Analysis API

This FastAPI application implements a workflow for geospatial data analysis focused on environmental monitoring, agriculture, and carbon credits in Brazil.

## Features

The API is divided into 5 main modules:

1. **Input Module**: Data acquisition and authentication
2. **Preprocessing Module**: Data transformation and georeferencing
3. **ETL Module**: Data extraction, transformation, and loading (including climate data ETL from ERA5-Land and Open-Meteo)
4. **Analysis Module**: Complex analyses and modeling
5. **Output Module**: Visualization and report generation

## Installation

1. Clone or download the project.
2. Create a virtual environment: `python -m venv .venv`
3. Activate the virtual environment: `.venv\Scripts\activate` (Windows)
4. Install dependencies: `pip install -r requirements.txt`
5. **Set up Earth Engine authentication** (required for climate ETL):
   - Follow the [Earth Engine Authentication Guide](./EARTH_ENGINE_AUTH.md)
   - Create a Google Cloud service account
   - Save the JSON key
   - Create `.env` file and add: `GEE_SERVICE_ACCOUNT_JSON_PATH=/path/to/key.json`

Note: Geospatial libraries like GDAL may require additional system dependencies on Windows. Consider using conda for easier installation.

## Running the App

Run the application with uvicorn:

```bash
uvicorn main:app --reload
```

The API will be available at `http://127.0.0.1:8000`

To run the Streamlit frontend:

```bash
streamlit run streamlit_app.py
```

The frontend will be available at the URL shown in the terminal (usually `http://localhost:8501`)

## API Documentation

Visit `http://127.0.0.1:8000/docs` for interactive Swagger UI documentation.

## Endpoints Overview

### Input Module (/input)
- `POST /input/aoi/upload`: Upload Area of Interest (AOI) file
- `POST /input/auth/drive-gee`: Authenticate with Google Drive and Earth Engine
- `POST /input/data/inpe/download`: Download INPE fire data
- `POST /input/data/remote-sensing/download`: Download remote sensing data

### Preprocessing Module (/geo)
- `POST /geo/generate-grid`: Generate spatial grid
- `POST /geo/preprocess-sicar`: Preprocess SICAR data
- `POST /geo/calculate-boundaries`: Calculate spatial boundaries
- `POST /geo/clip-raster`: Clip raster data

### ETL Module (/etl)
- `POST /etl/sentinel/process`: Process Sentinel-2 data
- `POST /etl/lulc/extract-percentage`: Extract land use/land cover percentages
- `POST /etl/ibge/process-pam`: Process IBGE PAM data
- `POST /etl/climate/era5/extract`: Extract ERA5-Land climate data from Google Earth Engine
- `POST /etl/climate/openmeteo/download`: Download Open-Meteo weather forecast data
- `POST /etl/climate/process`: Process and merge climate data

### Analysis Module (/analysis)
- `POST /analysis/age-perennial-crops`: Estimate perennial crop age
- `POST /model/agb/predict`: Predict Above Ground Biomass (AGB)
- `GET /analysis/breakeven-point`: Calculate breakeven point
- `POST /analysis/car-status`: Analyze CAR (Cadastro Ambiental Rural) status

### Output Module (/report)
- `GET /report/dashboard-pam`: Generate PAM dashboard
- `POST /report/generate-report`: Generate monitoring report
- `POST /report/generate-figure`: Generate analysis figures

## Implementation Notes

- All endpoints currently return placeholder responses. Actual implementations need to be added based on the specific geospatial processing logic.
- For file uploads, use multipart/form-data.
- Asynchronous operations are recommended for I/O-bound tasks.
- Consider using background tasks for long-running processes.
- Data storage should be configured for PostGIS or cloud storage (S3, GCS).

## Dependencies

- FastAPI: Web framework
- Uvicorn: ASGI server
- Geospatial libraries: GeoPandas, Rasterio, Shapely, Fiona
- Authentication: PyDrive, Earth Engine API
- Cloud storage: Boto3
- Visualization: Folium, Streamlit
- Data processing: Pandas, NumPy
- HTTP requests: Requests, httpx
- PDF generation: ReportLab
- Async file handling: aiofiles, python-multipart
- Climate data: openmeteo-requests, requests-cache, retry-requests, tqdm


## Inclusão modular de novas funcionalidades 

-> climate_etl.py

- Usa console cloude google 
- usado para acessar dados de clima de  Earth Engine
- extract de climate data como no código legado 
- possivelmente poderemos usar OPENMETEO_API_URL futuramente


-> manual_test3.py

- baixar dados dos estados diretamente do Sicar para o bucket - data- lake 
- Confirmado que funciona

