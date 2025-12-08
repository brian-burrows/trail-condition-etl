from etl.models.tasks import BaseTask
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Literal

class WeatherCategorizationTask(BaseTask):
    city_id: int
    last_historical_timestamp: str
    forecast_generated_at_timestamp: str

# TODO: Update API and return more fields with the combined weather forecast
class WeatherRecord(BaseModel):
    timestamp_utc: datetime = Field(
        description="Timestamp of the measurement (historical) or forecast (future) in UTC."
    ) 
    temperature_deg_c: float
    rain_fall_total_mm: float
    data_source: Literal["HISTORICAL", "FORECAST"]