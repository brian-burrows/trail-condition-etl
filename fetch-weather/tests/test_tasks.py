from unittest.mock import MagicMock

from src.tasks import new_consumer, new_producer, process_ingestion_task
from src.config import ConsumerConfig, ProducerConfig



def test_new_consumer(strict_redis_client, mock_rate_limiter, test_env_config):
    new_consumer(
        redis_client=strict_redis_client, 
        rate_limiter=mock_rate_limiter,
        config=None
    )

    new_consumer(
        redis_client=None, 
        rate_limiter=None, 
        config=ConsumerConfig()
    )
    assert True 

def test_new_producer(strict_redis_client, test_env_config):
    new_producer(redis_client=strict_redis_client)
    new_producer(redis_client=None, config=ProducerConfig())
    assert True 


def test_process_ingestion_task(
    mock_queued_task,
    mock_rate_limiter
):  
    mock_api = MagicMock()
    mock_service = MagicMock()
    
    # Act
    result = process_ingestion_task(
        queued_task=mock_queued_task,
        rate_limiter=mock_rate_limiter,
        weather_api_client=mock_api,
        weather_service=mock_service
    )
    
    # Assert
    assert result.is_success is True
    assert result.task_result.city_id == mock_queued_task.payload.city_id
    mock_service.post_historical_data.assert_called_once()
    mock_service.post_forecast_data.assert_called_once()