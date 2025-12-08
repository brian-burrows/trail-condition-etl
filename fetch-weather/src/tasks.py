import logging
import os
from datetime import date, datetime, timedelta, timezone

import requests
from etl.brokers.redis import RedisTaskQueueClient
from etl.dequeue import ResilientTaskWorker, TaskWorker
from etl.enqueue import OutboxProducer
from etl.exceptions import RateLimitExceededError
from etl.models.tasks import (
    BaseTask,
    DeadLetterTaskPayload,
    QueuedTask,
    TaskTransformationResult,
)
from etl.storage.sqlite import SqliteTaskOutboxDAO
from etl.transformers import SimpleTaskTransformer
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

from src.breakers import (
    DLQ_CIRCUIT_BREAKER,
    OPEN_WEATHER_MAPS_API_CIRCUIT_BREAKER,
    UPSTREAM_BROKER_CIRCUIT_BREAKER,
    WEATHER_SERVICE_CIRCUIT_BREAKER,
)
from src.db import CATEGORIZATION_SQLITE_DB, OWM_DAILY_RATE_LIMITER, REDIS_MASTER_CLIENT

INGESTION_STREAM_KEY = "owm-ingestion-stream"
CONSUMER_GROUP = "ingestion-workers"
CONSUMER_NAME = os.environ.get("WORKER_NAME", f"worker-{os.getpid()}") 
INGESTION_MAX_RETRIES = 5
INGESTION_MAX_IDLE_MS = 600000
INGESTION_EXPIRY_TIME_MS = 2.16e7
PENDING_ENTRY_LIST_BATCH_SIZE = 10
CATEGORIZATION_STREAM_KEY = "owm-categorization-stream"
CATEGORIZATION_MAX_STREAM_SIZE = 200

WEATHER_API_BASE_URL = "http://outpost-api-weather:8000"
LOGGER = logging.getLogger(__name__)

class OwmIngestionTask(BaseTask):
    latitude_deg: float 
    longitude_deg: float
    city_id: int
    model_config = {"frozen": True}

class WeatherCategorizationTask(BaseTask):
    city_id: int
    last_historical_timestamp: str 
    forecast_generated_at_timestamp: str
    

@OPEN_WEATHER_MAPS_API_CIRCUIT_BREAKER 
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max = 30, jitter = 1),
    reraise=True
)
def _fetch_historical_data(
        task: OwmIngestionTask, 
        previous_date: date,
    ):
    historical_weather = {
        "lat": task.latitude_deg,
        "lon": task.longitude_deg,
        "tz": "+00:00", 
        "date": previous_date,
        "units":"metric",
        "wind": {"max": {"speed": 6}, "direction": 120},
        "precipitation": {"total": 2}, 
        "cloud_cover":{"afternoon":0},
        "humidity":{"afternoon":33},
        "pressure":{"afternoon":1015},
        "temperature":{ 
            "min":286.48 - 273.15,
            "max":299.24 - 273.15,
            "afternoon":296.15 - 273.15,
            "night":289.56 - 273.15,
            "evening":295.93 - 273.15,
            "morning":287.59 - 273.15
        },
    }
    measured_at_ts = datetime(
        previous_date.year, 
        previous_date.month, 
        previous_date.day, 
        tzinfo=timezone.utc
    )
    LOGGER.info(f"Successfully fetched historical data for city {task.city_id}.")
    return {
        "task_id": task.task_id,
        "city_id": task.city_id,
        "temperature_deg_c": historical_weather["temperature"]["max"],
        "wind_speed_mps": historical_weather["wind"]['max']["speed"],
        "rain_fall_total_mm": historical_weather["precipitation"]["total"],
        "aggregation_level": "daily",
        "measured_at_ts_utc": measured_at_ts.isoformat(),
    }

@OPEN_WEATHER_MAPS_API_CIRCUIT_BREAKER 
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max = 30, jitter = 1),
    reraise=True
)
def _fetch_weather_forecast(task: OwmIngestionTask, current_hour: datetime) -> list[dict]:
    hourly_records = []
    BASE_TEMP_K: float = 288.15 
    for i in range(48):
        hourly_offset = timedelta(seconds=(i * 3600))
        hourly_records.append(
            {
                "dt": current_hour + hourly_offset,
                "temp": BASE_TEMP_K + (i * 2.0)  - 273.15,
                "wind_speed": 4.1 - (i * 0.2), 
                "rain": {"1h": 2.5}, 
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
        "lat": task.latitude_deg,
        "lon": task.longitude_deg,
        "timezone": "America/Chicago",
        "timezone_offset":-18000,
        "hourly": hourly_records,
    }
    city_id = task.city_id
    return [
        {
            "city_id": city_id, 
            "temperature_deg_c": hd["temp"],
            "wind_speed_mps": hd["wind_speed"],
            "rain_fall_total_mm": (hd.get("rain") or {}).get("1h", 0.0),
            "aggregation_level": "hourly",
            "forecast_generated_at_ts_utc" : current_hour.isoformat(),
            "forecast_timestamp_utc": hd["dt"].isoformat()
        }
        for hd in response_data["hourly"]
    ]

@WEATHER_SERVICE_CIRCUIT_BREAKER
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max = 30, jitter = 1),
    reraise=True
)
def _post_historical_data(data: dict) -> bool: 
    historical_data_url = f"{WEATHER_API_BASE_URL}/weather/historical"
    historical_payload = {
        k: v for k, v in data.items() 
        if k not in ["task_id"]
    }
    response = requests.post(
        url=historical_data_url, 
        json=historical_payload,
        timeout = 5
    )
    response.raise_for_status()
    LOGGER.info(f"Successfully posted historical data for city {data['city_id']}.")
    return True

@WEATHER_SERVICE_CIRCUIT_BREAKER
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max = 30, jitter = 1),
    reraise=True
)
def _post_weather_forecast(data: list[dict]) -> bool: 
    forecast_data_url = f"{WEATHER_API_BASE_URL}/weather/forecast"
    response = requests.post(url=forecast_data_url, json=data, timeout=5)
    response.raise_for_status()
    LOGGER.info(f"Successfully posted {len(data)} forecast records for city {data[0]['city_id']}.")
    return True

def process_ingestion_task(queued_task: QueuedTask):
    # This task needs to be split into more pipeline components for
    # decoupling in a future PR. It has too many responsibilities
    # and it complicates retry/circuit breaking mechanisms
    task: OwmIngestionTask = queued_task.task
    key = OWM_DAILY_RATE_LIMITER._get_key_for_today()
    count = OWM_DAILY_RATE_LIMITER.client.get(key)
    LOGGER.info(f"The current rate limit count is {count}")
    if not (OWM_DAILY_RATE_LIMITER.allow_request() and OWM_DAILY_RATE_LIMITER.allow_request()):
        raise RateLimitExceededError("Daily rate limit exceeded for Open Weather Maps")
    current_hour = datetime.now().astimezone(timezone.utc).replace(minute=0,second=0, microsecond=0)
    previous_date = current_hour.date() - timedelta(days=1)
    historical_data = _fetch_historical_data(task, previous_date)
    _post_historical_data(historical_data)
    forecast = _fetch_weather_forecast(task, current_hour)
    _post_weather_forecast(forecast)
    LOGGER.info("Posted weather to Weather Service, forming WeatherCategorizationTask")
    downstream_task = WeatherCategorizationTask(
        task_id = task.task_id,
        city_id = task.city_id,
        last_historical_timestamp=previous_date.isoformat(),
        forecast_generated_at_timestamp=current_hour.isoformat(),
    )
    return TaskTransformationResult(
        queued_task = queued_task, 
        is_success=True,
        task_result = downstream_task,
        exception_details = None
    )


def make_upstream_consumer_and_dlq() -> TaskWorker:
    # Need to find the upstream Redis Stream to consume
    task_queue = RedisTaskQueueClient(
        client=REDIS_MASTER_CLIENT,
        stream_key = INGESTION_STREAM_KEY,
        task_model=OwmIngestionTask,
        group_name=CONSUMER_GROUP,
        consumer_name=CONSUMER_NAME,
        max_stream_length=10000,
        dequeue_blocking_time_seconds=1,
        dequeue_batch_size=100,
        min_idle_time_seconds=10 * 60,
    )
    task_consumer = SimpleTaskTransformer(executable = process_ingestion_task)
    # Need to create a DLQ to send failed messages to
    dlq = RedisTaskQueueClient(
        client=REDIS_MASTER_CLIENT,
        stream_key = f"dlq:{INGESTION_STREAM_KEY}",
        task_model=DeadLetterTaskPayload
    )
    # Need to instatiate the Manager instance
    return ResilientTaskWorker(
        task_queue = task_queue,
        task_consumer = task_consumer,
        stuck_task_definition_seconds=60 * 10,
        expired_task_definition_seconds=60 * 60 * 24,
        max_delivery_count=6,
        retrying=None,
        dead_letter_queue = dlq,
        queue_breaker = UPSTREAM_BROKER_CIRCUIT_BREAKER,
        dlq_breaker = DLQ_CIRCUIT_BREAKER,
    )

def make_downstream_producer_and_persistent_storage() -> OutboxProducer:
    # Need to create the task queue to send weather data processing tasks to
    task_queue = RedisTaskQueueClient(
        client = REDIS_MASTER_CLIENT,
        stream_key = CATEGORIZATION_STREAM_KEY,
        task_model=WeatherCategorizationTask,
        max_stream_length=CATEGORIZATION_MAX_STREAM_SIZE,
        dequeue_blocking_time_seconds=1,
        dequeue_batch_size=100,
        min_idle_time_seconds=10 * 60,
    )
    # Need to create the persistent disk storage for the transactional outbox pattern
    storage = SqliteTaskOutboxDAO(
        db_path = CATEGORIZATION_SQLITE_DB,
        task_type = WeatherCategorizationTask
    )
    # Need to create the manager to handle sending tasks to Redis Streams
    return OutboxProducer(
        task_queue = task_queue, 
        storage = storage
    )