import pytest
from unittest.mock import patch, MagicMock
from src.weather import WeatherService

# 1. Setup a dummy config for testing
class DummyConfig:
    WEATHER_SERVICE_URL = "http://test-api"
    HTTP_TIMEOUT_SEC = 2

@pytest.fixture
def weather_service():
    return WeatherService(config=DummyConfig())

def test_post_historical_data_success(weather_service):
    # Data including 'task_id' which should be stripped
    input_data = {"city_id": 1, "temp": 25, "task_id": "abc-123"}
    expected_payload = {"city_id": 1, "temp": 25}

    with patch("requests.post") as mock_post:
        # Simulate a successful 200 OK response
        mock_post.return_value.status_code = 200
        
        # Act
        result = weather_service.post_historical_data(input_data)

        # Assert
        assert result is True
        # Verify the URL and the payload stripping
        mock_post.assert_called_once_with(
            url="http://test-api/weather/historical",
            json=expected_payload,
            timeout=2
        )

def test_post_historical_data_failure(weather_service):
    input_data = {"city_id": 1}

    with patch("requests.post") as mock_post:
        # Simulate a 500 Internal Server Error
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("API Error")
        mock_post.return_value = mock_response

        # Act & Assert
        with pytest.raises(Exception, match="API Error"):
            weather_service.post_historical_data(input_data)