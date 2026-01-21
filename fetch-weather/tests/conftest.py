import time
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

import docker
import pytest
import requests
from etl.models.tasks import QueuedTask
from redis import StrictRedis

from src.client import OpenWeatherMapAccessObject
from src.tasks import OwmIngestionTask


@pytest.fixture
def mock_open_weather_map_daily_historical_weather() -> dict[str, Any]:
    """
    Mocks the response for the OWM 3.0 day_summary endpoint.
    
    The temperature is determined deterministically by adding the day-of-month
    from the URL to the base temperature (e.g., if date=2025-11-19, temp = BASE_TEMP + 19).
    This ensures that mocking the same date yields the same result, and different dates
    yield different results, without relying on external state.
    """
    def f(url: str, timeout):
        if "day_summary" not in url:
            raise ValueError("Mock called with incorrect URL for daily summary.")
        parsed_url = urlparse(url)
        params = parse_qs(parsed_url.query)
        try:
            target_date_str = params['date'][0]
            lat = float(params['lat'][0])
            lon = float(params['lon'][0])
            target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
        except (KeyError, IndexError, ValueError) as e:
            raise ValueError(f"URL missing required parameters (date, lat, lon): {e}")
        response_data = {
            "lat": lat,
            "lon": lon,
            "tz": "+00:00", # OWM should provide times in UTC
            "date": target_date,
            "units":"metric",
            "wind": {"max": {"speed": 6}, "direction": 120},
            "precipitation": {"total": 2}, # millimeters
            "cloud_cover":{"afternoon":0},
            "humidity":{"afternoon":33},
            "pressure":{"afternoon":1015},
            "temperature":{ # Updated this because we're returning metric (Celcius now), - 273.15
                "min":286.48 - 273.15,
                "max":299.24 - 273.15,
                "afternoon":296.15 - 273.15,
                "night":289.56 - 273.15,
                "evening":295.93 - 273.15,
                "morning":287.59 - 273.15
            },
        }
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = response_data
        return mock_response
    return f

@pytest.fixture
def mock_open_weather_map_hourly_forecast() -> dict[str, Any]:
    """
    Mocks the response for the OWM 3.0 onecall endpoint, specifically for hourly forecast.
    It generates a sequence of 4 hourly records using local sequence generation.
    """
    def f(url, timeout):
        BASE_TEMP_K: float = 288.15 
        if "onecall" not in url:
            raise ValueError("Mock called with incorrect URL for hourly forecast.")
        parsed_url = urlparse(url)
        params = parse_qs(parsed_url.query)
        try:
            lat = float(params['lat'][0])
            lon = float(params['lon'][0])
        except (KeyError, IndexError, ValueError) as e:
            raise ValueError(f"URL missing required parameters (lat, lon): {e}")
        current_time = int(datetime.now().replace(minute=0, second=0, microsecond=0).timestamp())
        hourly_records: list[dict[str, Any]] = []
        for i in range(48):
            hourly_records.append(
                {
                    "dt": current_time + (i * 3600),
                    "temp": BASE_TEMP_K + (i * 2.0)  - 273.15,
                    "wind_speed": 4.1 - (i * 0.2), 
                    "rain": {"1h": 2.5} if i % 2 == 0 else None, 
                    "feels_like":292.33 - 273.15,
                    "pressure":1014,
                    "humidity":91,
                    "dew_point":290.51  - 273.15,
                    "uvi":0,
                    "clouds":54,
                    "visibility":10000,
                    "wind_deg":86,
                    "wind_gust":5.88,
                    "weather":[
                        {
                        "id":803,
                        "main":"Clouds",
                        "description":"broken clouds",
                        "icon":"04n"
                        }
                    ],
                    "pop":0.15
                }
            )
        response_data = {
            "lat": lat,
            "lon": lon,
            "timezone": "America/Chicago",
            "timezone_offset":-18000,
            "hourly": hourly_records,
        }
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = response_data
        return mock_response
    return f


@pytest.fixture
def owm_client():
    """Fixture to instantiate the client with mocked resilience objects."""
    return OpenWeatherMapAccessObject()

@pytest.fixture
def current_utc_hour():
    return (
        datetime
        .now()
        .astimezone(timezone.utc)
        .replace(minute=0, second=0, microsecond=0)
    )

@pytest.fixture
def current_utc_hour_no_tz():
    return (
        datetime
        .now()
        .replace(minute=0, second=0, microsecond=0)
    )

@pytest.fixture 
def current_date_utc(current_utc_hour):
    return current_utc_hour.date()


@pytest.fixture(scope="session")
def redis_service():
    """Returns the host and port for the running Redis container."""
    return {
        "host": "localhost",
        "port": 6381
    }


@pytest.fixture
def test_env_config(monkeypatch, redis_service):
    """
    Overwrites environment variables so that any Config instantiated 
    within the test uses the Docker Redis port.
    """
    # Override host and port to match the docker container
    monkeypatch.setenv("REDIS_MASTER_HOST", redis_service["host"])
    monkeypatch.setenv("REDIS_MASTER_PORT", str(redis_service["port"]))
    # Use a safe temp path for SQLite during tests
    monkeypatch.setenv("CATEGORIZATION_SQLITE_DB", ":memory:")
    
    # Lower timeouts for faster tests
    monkeypatch.setenv("HTTP_TIMEOUT_SEC", "1")

@pytest.fixture(scope="session")
def redis_container(redis_service):
    client = docker.from_env()
    
    # Mapping your YAML config to Python SDK
    container = client.containers.run(
        image="redis:7.0-alpine",
        command="redis-server --appendonly yes",
        name="test-redis-instance",
        detach=True,
        ports={'6379/tcp': redis_service["port"]},
        restart_policy={"Name": "always"},
        volumes={'redis_data': {'bind': '/data', 'mode': 'rw'}},
        healthcheck={
            "Test": ["CMD", "redis-cli", "ping"],
            "Interval": 10_000_000_000, # 10s in nanoseconds
            "Timeout": 5_000_000_000,   # 5s
            "Retries": 3
        }
    )
    timeout = 15
    stop_time = time.time() + timeout
    while time.time() < stop_time:
        container.reload()
        if container.attrs['State']['Health']['Status'] == 'healthy':
            break
        time.sleep(0.01)
    else:
        container.stop()
        container.remove()
        raise RuntimeError("Redis container failed to become healthy")

    yield container
    container.stop()
    container.remove()

@pytest.fixture
def strict_redis_client(redis_service, redis_container):
    client = StrictRedis(
        host=redis_service["host"], 
        port=redis_service["port"], 
        decode_responses=True
    )
    client.flushdb()
    return client
@pytest.fixture
def mock_rate_limiter():
    class RL():
        def allow_request(self):
            return True 
        
    return RL()

@pytest.fixture
def mock_queued_task():
    payload = OwmIngestionTask(
        task_id=uuid4(),
        longitude_deg=0.0,
        latitude_deg=0.0,
        city_id=0
    )
    return QueuedTask(
        queued_message_id="abc",
        time_since_queued_seconds=1,
        time_since_last_delivered_seconds=0,
        number_of_times_delivered=0,
        payload=payload
    )