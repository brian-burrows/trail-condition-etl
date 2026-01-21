import datetime
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest
import requests

BASE_TEMP_K: float = 288.15 
BASE_TEMP_C: float = 15.0
TEST_LAT = 39.0
TEST_LON = -105.0


def test_fetch_daily_historical_success(
    current_date_utc,
    owm_client, 
    monkeypatch,
    mock_open_weather_map_daily_historical_weather
):
    """Tests successful fetching and mapping of daily historical data."""
    monkeypatch.setattr(
        "src.client.requests.get", 
        mock_open_weather_map_daily_historical_weather
    )
    target_date = current_date_utc
    historical_data = owm_client.fetch_daily_historical_weather_data(
        target_date = target_date,
        lat = TEST_LAT,
        lon = TEST_LON,
    )
    assert historical_data.aggregation_level == "daily"
    assert historical_data.temperature_deg_c == pytest.approx(299.24 - 273.15)
    assert historical_data.rain_fall_total_mm == 2.0
    assert historical_data.wind_speed_mps == 6.0
    assert historical_data.timestamp.date() == target_date
    
def test_fetch_hourly_forecast_success_and_duration_filter(
    current_utc_hour,
    owm_client, 
    monkeypatch,
    mock_open_weather_map_hourly_forecast
):
    """Tests successful fetching, correct mapping, and filtering by duration (e.g., 2 hours)."""
    monkeypatch.setattr("src.client.requests.get", mock_open_weather_map_hourly_forecast)
    start_date = current_utc_hour
    duration = timedelta(hours=2)
    result = owm_client.fetch_hourly_weather_forecast(start_date, duration, TEST_LAT, TEST_LON)
    assert len(result) == 2
    assert result[0].aggregation_level == "hourly"
    assert result[0].temperature_deg_c == pytest.approx(BASE_TEMP_C)
    assert result[0].rain_fall_total_mm == 2.5
    assert result[1].temperature_deg_c == pytest.approx(BASE_TEMP_C + 2.0)
    assert result[1].rain_fall_total_mm == 0.0

def test_fetch_hourly_forecast_with_naive_datetime(
    owm_client, 
    monkeypatch,
    mock_open_weather_map_hourly_forecast,
    current_utc_hour_no_tz
):
    """Hits the branch where start_datetime.tzinfo is None."""
    monkeypatch.setattr("src.client.requests.get", mock_open_weather_map_hourly_forecast)
    duration = timedelta(hours=2)
    with pytest.raises(ValueError, match="start_datetime must be timezone-aware"):
        owm_client.fetch_hourly_weather_forecast(
            current_utc_hour_no_tz, 
            duration, 
            TEST_LAT, 
            TEST_LON
        )

def test_daily_data_mapping_raises_on_missing_key(owm_client):
    """Tests that a ValueError is raised if the API response is missing a required key (e.g., 'wind')."""
    malformed_data = {
        "lat": 39.0, "lon": -105.0, "date": "2025-01-01",
        "temperature": {"max": 300.0},
        # Missing 'wind' key entirely
    }  
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = malformed_data
    with patch('src.client.requests.get', return_value=mock_response):
        result = owm_client.fetch_daily_historical_weather_data(date.today(), TEST_LAT, TEST_LON)
        assert result.wind_speed_mps == 0.0

def test_hourly_data_mapping_raises_on_malformed_item(current_utc_hour, owm_client):
    """Tests that a ValueError is raised if an hourly item within the forecast array is malformed."""
    malformed_forecast = {
        "hourly": [
            {"dt": current_utc_hour.timestamp(), "wind_speed": 5.0},
        ]
    }
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = malformed_forecast
    with patch('src.client.requests.get', return_value=mock_response):
        with pytest.raises(
            ValueError, 
            match="Failed to parse OWM hourly forecast data structure:"
        ):
            owm_client.fetch_hourly_weather_forecast(
                current_utc_hour, 
                timedelta(hours=3), 
                TEST_LAT, TEST_LON
            )
            

def test_map_daily_data_type_error(owm_client):
    # 'temperature' is a string instead of a dict
    malformed_json = {
        "temperature": "very hot", 
        "wind": {"max": {"speed": 10}}
    }
    with pytest.raises(ValueError, match="Failed to parse OWM data structure"):
        owm_client._map_daily_data(malformed_json, date.today())

def test_execute_request_connection_error(owm_client):
    with patch("src.client.requests.get") as mock_get:
        # Simulate a network timeout/drop
        mock_get.side_effect = requests.exceptions.ConnectionError("Network is down")
        
        with pytest.raises(ConnectionError, match="OWM API request"):
            owm_client.fetch_daily_historical_weather_data(date.today(), 0, 0)

def test_map_daily_data_missing_mandatory_temp(owm_client):
    incomplete_json = {"temperature": {"min": 10}}
    with pytest.raises(ValueError, match="Daily data missing required max temperature field"):
        owm_client._map_daily_data(incomplete_json, date.today())