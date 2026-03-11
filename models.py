from pydantic import BaseModel
from typing import Optional, List

# Models for Input Module
class AOIUploadResponse(BaseModel):
    message: str
    file_path: str

class AuthCredentials(BaseModel):
    service_account_key: str  # or dict for JSON
    token: Optional[str] = None

class AuthResponse(BaseModel):
    message: str
    authenticated: bool

class INPEDownloadParams(BaseModel):
    csv_path: str
    id_column: str
    output_folder: str

class RemoteSensingDownloadParams(BaseModel):
    gedi_short_name: str
    global_temporal_range: str
    brazil_bbox: List[float]  # [min_lon, min_lat, max_lon, max_lat]

# Models for Preprocessing Module
class GridGenerationParams(BaseModel):
    resolution_grid: float

class SICARPreprocessParams(BaseModel):
    aoi_geometry: str  # GeoJSON or WKT

class BoundaryCalculationParams(BaseModel):
    ibge_shapefile_path: str
    mapbiomas_polygons_path: str

class RasterClippingParams(BaseModel):
    raster_path: str
    clipping_geometry: str  # GeoJSON

# Models for ETL Module
class SentinelETLParams(BaseModel):
    sentinel_files: List[str]
    farm_grid_shapefile: str

class LULCExtractParams(BaseModel):
    mapbiomas_raster: str
    aoi_geometry: str

class IBGEPAMProcessParams(BaseModel):
    pam_jsons: List[str]
    municipalities_shapefile: str

# Models for Analysis Module
class CropAgeEstimationParams(BaseModel):
    satellite_indices_series: List[float]  # or more complex
    plantation_location: str

class AGBModelingParams(BaseModel):
    gedi_data_path: str
    features_rasters: List[str]

class BreakevenParams(BaseModel):
    fixed_cost: float
    herd_sizes: List[int]
    carbon_yield: float
    verra_levy: float
    product_cost_annual: float

class CARStatusParams(BaseModel):
    farm_geometry: str
    deforestation_data_path: str

# Models for Climate ETL
class ERA5ExtractParams(BaseModel):
    centroids_shapefile: str
    start_date: str
    end_date: str
    output_folder: str
    variables: List[str]

class OpenMeteoDownloadParams(BaseModel):
    centroids_shapefile: str
    output_file: str
    past_days: int = 5
    forecast_days: int = 3

class ClimateProcessParams(BaseModel):
    era5_raw_files: List[str]
    openmeteo_file: str
    output_folder: str

# Models for Output Module
class DashboardPAMParams(BaseModel):
    pam_geoparquet: str
    selected_culture: str
    selected_uf: str

class ReportGenerationParams(BaseModel):
    truecolor_images: List[str]
    area_location_data: str

class FigureGenerationParams(BaseModel):
    analysis_results: dict  # flexible
class DashboardPAMParams(BaseModel):
    pam_geoparquet: str
    selected_culture: str
    selected_uf: str

class ReportGenerationParams(BaseModel):
    truecolor_images: List[str]
    area_location_data: str

class ClimateProcessParams(BaseModel):
    era5_raw_files: List[str]
    openmeteo_file: str
    output_folder: str


class SICARS3ProcessParams(BaseModel):
    s3_path: str  # Ex: SICAR_data/AC/CONSOLIDATED_AREA.zip
    state: str    # Ex: AC

class SICARExtractParams(BaseModel):
    state: str  # Ex: "AC", "SP"
    info: str   # Ex: "AREA_CONSOLIDADA", "RESERVA_LEGAL"