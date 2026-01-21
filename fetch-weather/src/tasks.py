import logging
from datetime import datetime, timedelta, timezone
from functools import partial

import redis
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
from etl.throttling import RateLimiter
from etl.throttling.redis import RedisDailyRateLimiter
from etl.transformers import SimpleTaskTransformer
from redis import StrictRedis
from redis.backoff import ExponentialWithJitterBackoff
from redis.exceptions import BusyLoadingError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.retry import Retry
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

from src.breakers import (
    DLQ_CIRCUIT_BREAKER,
    UPSTREAM_BROKER_CIRCUIT_BREAKER,
)
from src.client import WeatherApiInterface, new_weather_api_client
from src.config import ConsumerConfig, ProducerConfig
from src.weather import WeatherService, WeatherServiceInterface

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


class WeatherProcessor():
    def __init__(
        self, 
        api: WeatherApiInterface,
        svc: WeatherServiceInterface,
        limit: RateLimiter
    ):
        self.api = api 
        self.svc = svc 
        self.limit = limit

    def __call__(self, queued_task: QueuedTask):
        # This task needs to be split into more pipeline components for
        # decoupling in a future PR. It has too many responsibilities
        # and it complicates retry/circuit breaking mechanisms
        task: OwmIngestionTask = queued_task.payload
        if not self.limit.allow_request():
            raise RateLimitExceededError("Daily rate limit exceeded for Open Weather Maps")
        current_hour = datetime.now().astimezone(timezone.utc).replace(minute=0,second=0, microsecond=0)
        previous_date = current_hour.date() - timedelta(days=1)
        historical_data = self.api.fetch_daily_historical_weather_data(
            target_date=previous_date,
            lat = task.latitude_deg,
            lon = task.longitude_deg,
        )
        forecast = self.api.fetch_hourly_weather_forecast(
            start_datetime=current_hour,
            duration=timedelta(hours=48),
            lat=task.latitude_deg,
            lon=task.longitude_deg,
        )
        self.svc.post_historical_data(historical_data)
        self.svc.post_forecast_data(forecast)
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

def process_ingestion_task(
    queued_task: QueuedTask,
    rate_limiter: RateLimiter,
    weather_api_client: WeatherApiInterface,
    weather_service: WeatherServiceInterface,
):
    # This task needs to be split into more pipeline components for
    # decoupling in a future PR. It has too many responsibilities
    # and it complicates retry/circuit breaking mechanisms
    task: OwmIngestionTask = queued_task.payload
    if not rate_limiter.allow_request():
        raise RateLimitExceededError("Daily rate limit exceeded for Open Weather Maps")
    current_hour = datetime.now().astimezone(timezone.utc).replace(minute=0,second=0, microsecond=0)
    previous_date = current_hour.date() - timedelta(days=1)
    historical_data = weather_api_client.fetch_daily_historical_weather_data(
        target_date=previous_date,
        lat = task.latitude_deg,
        lon = task.longitude_deg,
    )
    weather_service.post_historical_data(historical_data)
    forecast = weather_api_client.fetch_hourly_weather_forecast(
        start_datetime=current_hour,
        duration=timedelta(hours=48),
        lat=task.latitude_deg,
        lon=task.longitude_deg,
    )
    weather_service.post_forecast_data(forecast)
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

def new_consumer(
    redis_client: StrictRedis,
    rate_limiter: RedisDailyRateLimiter,
    weather_api_client: WeatherApiInterface,
    weather_service: WeatherServiceInterface,
    config: ConsumerConfig,
) -> TaskWorker:
    task_queue = RedisTaskQueueClient(
        client=redis_client,
        task_model=OwmIngestionTask,
        stream_key=config.INGESTION_STREAM_KEY,
        group_name=config.CONSUMER_GROUP,
        consumer_name=config.CONSUMER_NAME,
        max_stream_length=config.STREAM_MAX_LEN,
        dequeue_blocking_time_seconds=config.BLOCKING_TIME_SEC,
        dequeue_batch_size=config.BATCH_SIZE,
        min_idle_time_seconds=config.IDLE_TIME_SEC,
    )
    # Configure the DLQ we'll send poison pills to
    dlq = RedisTaskQueueClient(
        client=redis_client,
        stream_key=f"dlq:{config.INGESTION_STREAM_KEY}",
        task_model=DeadLetterTaskPayload
    )
    # Configure the task processing function
    worker_logic = WeatherProcessor(
        api=weather_api_client, 
        svc=weather_service,
        limit=rate_limiter
    )
    # 6. Build the Resilient Worker
    return ResilientTaskWorker(
        task_queue=task_queue,
        task_transformer=SimpleTaskTransformer(executable=worker_logic),
        dead_letter_queue=dlq,
        stuck_task_definition_seconds=config.STUCK_TASK_SEC,
        expired_task_definition_seconds=config.EXPIRY_TASK_SEC,
        max_delivery_count=config.MAX_RETRIES,
        queue_breaker=UPSTREAM_BROKER_CIRCUIT_BREAKER,
        dlq_breaker=DLQ_CIRCUIT_BREAKER,
    )


def new_producer(
    redis_client: redis.StrictRedis | None = None,
    config: ProducerConfig | None = None
) -> OutboxProducer:
    config = config or ProducerConfig()
    if redis_client is None:
        pool = redis.BlockingConnectionPool(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            retry=Retry(ExponentialWithJitterBackoff(), 8),
            retry_on_error=[BusyLoadingError, RedisConnectionError],
            max_connections=2
        )
        redis_client = redis.StrictRedis(connection_pool=pool)
    task_queue = RedisTaskQueueClient(
        client=redis_client,
        stream_key=config.STREAM_KEY,
        task_model=WeatherCategorizationTask,
        max_stream_length=config.MAX_STREAM_SIZE,
        dequeue_blocking_time_seconds=config.DEQUEUE_BLOCKING_SECONDS,
        dequeue_batch_size=config.DEQUEUE_BATCH_SIZE,
        min_idle_time_seconds=config.MIN_IDLE_SECONDS,
    )
    storage = SqliteTaskOutboxDAO(
        db_path=config.SQLITE_DB_PATH,
        task_type=WeatherCategorizationTask
    )
    return OutboxProducer(task_queue=task_queue, storage=storage)