from unittest.mock import MagicMock

from src.tasks import new_worker, new_producer, WeatherProcessor
from src.config import ConsumerConfig, ProducerConfig
from src.api import new_weather_api_client
from src.weather import WeatherService



def test_new_consumer(strict_redis_client, mock_rate_limiter, test_env_config):
    new_worker(
        redis_client=strict_redis_client, 
        rate_limiter=mock_rate_limiter,
        weather_api_client=new_weather_api_client,
        weather_service=WeatherService(config=ConsumerConfig()),
        config=ConsumerConfig()
    )
    assert True 

def test_new_producer(strict_redis_client, test_env_config):
    new_producer(redis_client=strict_redis_client, config=ProducerConfig())
    assert True 


def test_process_ingestion_task(
    mock_queued_task,
    mock_rate_limiter
):  
    mock_api = MagicMock()
    mock_service = MagicMock()
    result = WeatherProcessor(
        api=mock_api,
        svc=mock_service,
        limit=mock_rate_limiter,
    )(queued_task=mock_queued_task)
    
    # Assert
    assert result.is_success is True
    assert result.task_result.city_id == mock_queued_task.payload.city_id
    mock_service.post_historical_data.assert_called_once()
    mock_service.post_forecast_data.assert_called_once()