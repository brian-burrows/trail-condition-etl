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
    key = rate_limiter._get_key_for_today()
    count = rate_limiter.client.get(key)
    LOGGER.info(f"The current rate limit count is {count}")
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
    redis_client: StrictRedis | None = None,
    rate_limiter: RedisDailyRateLimiter | None = None,
    weather_api_client: WeatherApiInterface | None = None,
    weather_service: WeatherServiceInterface | None = None,
    config: ConsumerConfig | None = None,
) -> TaskWorker:
    # 1. Initialize Config first
    cfg = config or ConsumerConfig()

    # 2. Build Infrastructure
    if redis_client is None:
        pool = redis.BlockingConnectionPool(
            host=cfg.REDIS_HOST,
            port=cfg.REDIS_PORT,
            retry=Retry(ExponentialWithJitterBackoff(), 8),
            retry_on_error=[BusyLoadingError, RedisConnectionError],
            health_check_interval=3,
            socket_connect_timeout=15,
            socket_timeout=cfg.HTTP_TIMEOUT_SEC,
            max_connections=cfg.REDIS_MAX_CONNECTIONS
        )
        redis_client = redis.StrictRedis(connection_pool=pool)

    if rate_limiter is None:
        rate_limiter = RedisDailyRateLimiter(
            client=redis_client,
            base_key=cfg.RATE_LIMITER_KEY,
            max_requests=cfg.OWM_MAX_DAILY_REQUESTS,
        )

    # 3. Setup Repositories/Clients
    weather_api = weather_api_client or new_weather_api_client()
    internal_service = weather_service or WeatherService(cfg)

    # 4. Configure Queue and DLQ
    task_queue = RedisTaskQueueClient(
        client=redis_client,
        task_model=OwmIngestionTask,
        stream_key=cfg.INGESTION_STREAM_KEY,
        group_name=cfg.CONSUMER_GROUP,
        consumer_name=cfg.CONSUMER_NAME,
        max_stream_length=cfg.STREAM_MAX_LEN,
        dequeue_blocking_time_seconds=cfg.BLOCKING_TIME_SEC,
        dequeue_batch_size=cfg.BATCH_SIZE,
        min_idle_time_seconds=cfg.IDLE_TIME_SEC,
    )

    dlq = RedisTaskQueueClient(
        client=redis_client,
        stream_key=f"dlq:{cfg.INGESTION_STREAM_KEY}",
        task_model=DeadLetterTaskPayload
    )

    # 5. Dependency Injection into the Task Function
    worker_logic = partial(
        process_ingestion_task,
        rate_limiter=rate_limiter,
        weather_api_client=weather_api,
        weather_service=internal_service
    )

    # 6. Build the Resilient Worker
    return ResilientTaskWorker(
        task_queue=task_queue,
        task_transformer=SimpleTaskTransformer(executable=worker_logic),
        dead_letter_queue=dlq,
        stuck_task_definition_seconds=cfg.STUCK_TASK_SEC,
        expired_task_definition_seconds=cfg.EXPIRY_TASK_SEC,
        max_delivery_count=cfg.MAX_RETRIES,
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