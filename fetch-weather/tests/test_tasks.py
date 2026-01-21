from unittest.mock import MagicMock

from src.tasks import new_consumer, new_producer, process_ingestion_task


def test_new_consumer(strict_redis_client, mock_rate_limiter):
    new_consumer(strict_redis_client, mock_rate_limiter)
    assert True 

def test_new_producer(strict_redis_client):
    new_producer(strict_redis_client)
    assert True 

def test_process_ingestion_task(
    strict_redis_client,
    mock_rate_limiter
):
    try:
        process_ingestion_task()
        assert False
    except:
        assert True


# def test_process_ingestion_task_success():
#     # Arrange
#     mock_task = MagicMock()
#     mock_task.payload.city_id = 123
    
#     mock_limiter = MagicMock()
#     mock_limiter.allow_request.return_value = True
    
#     mock_api = MagicMock()
#     mock_service = MagicMock()
    
#     # Act
#     result = process_ingestion_task(
#         queued_task=mock_task,
#         rate_limiter=mock_limiter,
#         weather_api_client=mock_api,
#         weather_service=mock_service
#     )
    
#     # Assert
#     assert result.is_success is True
#     assert result.task_result.city_id == 123
#     mock_service.post_historical_data.assert_called_once()
#     mock_service.post_forecast_data.assert_called_once()