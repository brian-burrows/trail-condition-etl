"""tasks.py

Defines the worker instance for this ETL pipeline.

"""
import logging
import os
import traceback

import requests
from etl.brokers.redis import RedisTaskQueueClient
from etl.dequeue import ResilientTaskWorker
from etl.exceptions import DuplicateTaskError
from etl.models.tasks import (
    DeadLetterTaskPayload,
    ExceptionDetails,
    QueuedTask,
    TaskTransformationResult,
)
from etl.transformers import SimpleTaskTransformer
from pybreaker import CircuitBreakerError

from src.breakers import (
    DLQ_CIRCUIT_BREAKER,
    UPSTREAM_QUEUE_CIRCUIT_BREAKER,
)
from src.classifier import RuleBasedTrailConditionClassifier
from src.client import WeatherServiceClient
from src.db import DEDUPLICATION_CACHE, REDIS_CLIENT
from src.models import WeatherCategorizationTask

CONSUMER_GROUP = "categorization-workers"
CONSUMER_NAME = os.environ.get("WORKER_NAME", f"categorization-worker-{os.getpid()}")
CATEGORIZATION_STREAM_KEY = "owm-categorization-stream"
LOGGER = logging.getLogger(__name__)


def _handle_task_failure(queued_task: QueuedTask, e: Exception, safe_to_retry: bool):
    """
    Centralized function to create a failed TaskTransformationResult.
    """
    task = queued_task.payload
    LOGGER.error(f"Task processing failed for task {task.task_id}. Error: {e}")
    return TaskTransformationResult(
        queued_task=queued_task, 
        is_success=False,
        task_result=False, 
        exception_details=ExceptionDetails(
            exception_type=e.__class__.__name__,
            exception_message=f"{e}",
            traceback=traceback.format_exc(),
            safe_to_retry=safe_to_retry
        )
    )

def process_task(queued_task: QueuedTask) -> TaskTransformationResult:
    """Executes the weather categorization in three sequential steps.

    If any step fails then the entire task fails. We raise errors to signal that failed tasks
    should be sent to the DLQ.
    """
    task = queued_task.payload
    if DEDUPLICATION_CACHE.is_task_id_in_cache(task.task_id):
        raise DuplicateTaskError(f"Task ID {task.task_id} has already been processed")
    LOGGER.info(f"Starting classification for Task ID: {task.task_id}, City ID: {task.city_id}")
    city_id = task.city_id
    try:
        client = WeatherServiceClient()
        historical_weather_data, forecast_weather_data = client.fetch_weather_data(city_id)
        classification_label = RuleBasedTrailConditionClassifier().classify_trail_conditions(
            historical_data=historical_weather_data,
            forecast_data=forecast_weather_data
        )
        LOGGER.info(f"City {city_id} classified as: {classification_label}")
        # TODO: Update API to take a list of classification labels, database to have one-to-many relationship
        is_succesful = client.post_weather_classification(city_id, ",".join(classification_label))
        if is_succesful:
            DEDUPLICATION_CACHE.insert_task_id(task.task_id)
            return TaskTransformationResult(
                queued_task=queued_task, 
                is_success=True,
                task_result = True,
                exception_details=None,
            )
    except requests.exceptions.RequestException as e:
        return _handle_task_failure(queued_task, e, safe_to_retry=False)
    except CircuitBreakerError as e:
        return _handle_task_failure(queued_task, e, safe_to_retry=True)
    except Exception as e:
        return _handle_task_failure(queued_task, e, safe_to_retry=False)


def make_upstream_worker_and_dlq():
    redis_client = REDIS_CLIENT
    task_queue = RedisTaskQueueClient(
        client=redis_client,
        stream_key=CATEGORIZATION_STREAM_KEY,
        group_name=CONSUMER_GROUP,
        consumer_name=CONSUMER_NAME,
        task_model=WeatherCategorizationTask,
        dequeue_blocking_time_seconds= 1,
        dequeue_batch_size= 100,
        min_idle_time_seconds = 120,
    )
    dlq = RedisTaskQueueClient(
        client=redis_client,
        stream_key=f"dlq:{CATEGORIZATION_STREAM_KEY}",
        task_model=DeadLetterTaskPayload,
        max_stream_length=10000,
        dequeue_blocking_time_seconds= 1,
        dequeue_batch_size= 100,
        min_idle_time_seconds = 120,
    )
    task_transformer = SimpleTaskTransformer(executable=process_task)
    return ResilientTaskWorker(
        task_queue=task_queue,
        task_transformer=task_transformer,
        stuck_task_definition_seconds=10 * 60,
        expired_task_definition_seconds=60 * 60 * 24,
        max_delivery_count=6,
        retrying=None, # TODO: We need to configure this for task acquisition, acknowledgement, and dead letter queuing
        dead_letter_queue=dlq,
        queue_breaker=UPSTREAM_QUEUE_CIRCUIT_BREAKER,
        dlq_breaker=DLQ_CIRCUIT_BREAKER,
    )