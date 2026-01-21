import pytest
from unittest.mock import patch, MagicMock
from src.weather import WeatherService
class DummyConfig:
    WEATHER_SERVICE_URL = "http://test-api"
    HTTP_TIMEOUT_SEC = 2

@pytest.fixture
def weather_service():
    return WeatherService(config=DummyConfig())

def test_post_historical_data_success(weather_service):
    input_data = {"city_id": 1, "temp": 25, "task_id": "abc-123"}
    expected_payload = {"city_id": 1, "temp": 25}
    with patch("requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        result = weather_service.post_historical_data(input_data)
        assert result is True
        mock_post.assert_called_once_with(
            url="http://test-api/weather/historical",
            json=expected_payload,
            timeout=2
        )

def test_post_historical_data_failure(weather_service):
    input_data = {"city_id": 1}
    with patch("requests.post") as mock_post:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("API Error")
        mock_post.return_value = mock_response
        with pytest.raises(Exception, match="API Error"):
            weather_service.post_historical_data(input_data)

def test_post_forecast_data_success(weather_service):
    forecast_batch = [
        {"timestamp": "2026-01-21T10:00:00", "temp": 12},
        {"timestamp": "2026-01-21T11:00:00", "temp": 13}
    ]
    with patch("src.weather.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        result = weather_service.post_forecast_data(forecast_batch)
        assert result is True
        mock_post.assert_called_once_with(
            url="http://test-api/weather/forecast",
            json=forecast_batch,
            timeout=2
        )

def test_post_forecast_data_network_timeout(weather_service):
    from requests.exceptions import Timeout
    forecast_batch = [{"temp": 10}]
    with patch("src.weather.requests.post") as mock_post:
        mock_post.side_effect = Timeout("Request timed out")
        with pytest.raises(Timeout, match="Request timed out"):
            weather_service.post_forecast_data(forecast_batch)