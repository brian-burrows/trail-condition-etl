"""Provides interfaces and concrete implementations for classifying trail conditions based on weather data"""
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone

from src.client import WeatherRecord

class TrailConditionClassifier(ABC):
    """Abstract base class for taking historical weather and forecast weather
    and producing some class labels.
    """
    @abstractmethod
    def classify_trail_conditions(
        self, 
        historical_weather: list[WeatherRecord], 
        forecast_weather: list[WeatherRecord]
    ):
        pass


class RuleBasedTrailConditionClassifier(TrailConditionClassifier):
    RAIN_THRESHOLD_MM = 5.0
    HEAVY_RAIN_THRESHOLD_MM = 10.0
    MUD_DAYS = 2
    FREEZING_POINT_C = 0.0
    HEAT_ADVISORY_C = 35.0
    WIND_ADVISORY_MPS = 15.0

    def check_all_weather_for_mud(self, historical_data: list[WeatherRecord], forecast_data: list[WeatherRecord]) -> str | None:
        latest_ts = datetime.now(timezone.utc)
        rain_sum_24h_forecast = sum(d.rain_fall_total_mm for d in forecast_data[:24])
        mud_window_start = latest_ts - timedelta(days=self.MUD_DAYS)
        recent_rain_mm = 0.0
        recent_warm_days = False
        for record in historical_data:
            if record.timestamp_utc >= mud_window_start:
                recent_rain_mm += record.rain_fall_total_mm
                if record.temperature_deg_c > 10.0:
                    recent_warm_days = True
        if recent_rain_mm >= self.RAIN_THRESHOLD_MM and not recent_warm_days:
            return "TRAIL_MUD_WARNING"
        elif recent_rain_mm < self.RAIN_THRESHOLD_MM and rain_sum_24h_forecast < self.RAIN_THRESHOLD_MM:
            return "TRAIL_DRY_EXCELLENT"
        return None
    
    def check_forecast_for_heavy_precipitation(self, forecast_data: list[WeatherRecord]) -> str | None:
        rain_sum_24h_forecast = sum(d.rain_fall_total_mm for d in forecast_data[:24])
        if rain_sum_24h_forecast >= self.HEAVY_RAIN_THRESHOLD_MM:
            rain_data_24h = forecast_data[:24]
            forecast_temp_avg = (
                sum(d.temperature_deg_c for d in rain_data_24h) / len(rain_data_24h)
                if rain_data_24h
                else 0
            )
            if forecast_temp_avg < self.FREEZING_POINT_C:
                return "HEAVY_SNOW_WARNING"
            return "TRAIL_CLOSED_HEAVY_RAIN"
        return None
    
    def check_for_heavy_snowpack(self, historical_data: list[WeatherRecord], forecast_data: list[WeatherRecord]) -> str | None:
        if historical_data and forecast_data:
            historical_max_temp = max(d.temperature_deg_c for d in historical_data)
            forecast_min_temp = min(d.temperature_deg_c  for d in forecast_data[:24])
            if historical_max_temp > self.FREEZING_POINT_C and forecast_min_temp < self.FREEZING_POINT_C:
                return "SNOWPACK_ICY_CONDITIONS"
            elif historical_max_temp > 5.0 and forecast_min_temp > self.FREEZING_POINT_C:
                return "SNOWPACK_HEAVY_WET"
        return None
            
    def check_for_wind_advisory(self, forecast_data: list[WeatherRecord]) -> str | None:
        # TODO: Update API to return wind speed data in the historical/forecast period
        return None
        
    def check_for_heat_advisory(self, forecast_data: list[WeatherRecord]) -> str | None:
        max_temp_forecast = max([d.temperature_deg_c for d in forecast_data])
        if max_temp_forecast > self.HEAT_ADVISORY_C:
            return "HEAT_ADVISORY"
        return None

    def classify_trail_conditions(
        self, 
        historical_data: list[WeatherRecord], 
        forecast_data: list[WeatherRecord]
    ) -> list[str]:
        conditions = []
        for condition in [
            self.check_all_weather_for_mud(historical_data=historical_data, forecast_data=forecast_data),
            self.check_for_heat_advisory(forecast_data=forecast_data),
            self.check_for_wind_advisory(forecast_data=forecast_data),
            self.check_for_heavy_snowpack(historical_data=historical_data, forecast_data=forecast_data),
            self.check_forecast_for_heavy_precipitation(forecast_data=forecast_data),
        ]:
            if condition is not None:
                conditions.append(condition)
        return conditions