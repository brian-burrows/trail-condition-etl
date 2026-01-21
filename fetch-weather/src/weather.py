import logging
from abc import ABC, abstractmethod

import requests

from src.config import ConsumerConfig

LOGGER = logging.getLogger(__name__)

class WeatherServiceInterface(ABC):
    @abstractmethod
    def post_historical_data(self, data: dict) -> bool:
        pass

    @abstractmethod
    def post_forecast_data(self, data: list[dict]) -> bool:
        pass


class WeatherService(WeatherServiceInterface):
    def __init__(self, config: ConsumerConfig):
        self.base_url = config.WEATHER_SERVICE_URL
        self.timeout = config.HTTP_TIMEOUT_SEC

    def post_historical_data(self, data: dict) -> bool:
        url = f"{self.base_url}/weather/historical"
        # Strip task_id for the internal service API contract
        payload = {k: v for k, v in data.items() if k != "task_id"}
        response = requests.post(url=url, json=payload, timeout=self.timeout)
        response.raise_for_status()
        LOGGER.info(f"Successfully posted historical data for city {data.get('city_id')}.")
        return True
    
    def post_forecast_data(self, data: list[dict]) -> bool: 
        url = f"{self.base_url}/weather/forecast"
        response = requests.post(url=url, json=data, timeout=self.timeout)
        response.raise_for_status()
        LOGGER.info(f"Successfully posted {len(data)} forecast records.")
        return True