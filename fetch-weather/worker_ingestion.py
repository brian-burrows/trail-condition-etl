import logging
import sys
import threading
import time

from etl.dequeue import TaskWorker
from etl.enqueue import OutboxProducer
from etl.handlers import SignalCatcher
from etl.models.tasks import TaskTransformationResult

from src.breakers import DOWNSTREAM_BROKER_CIRCUIT_BREAKER
from src.tasks import (
    new_producer,
    new_consumer,
)

logging.basicConfig(
    encoding='utf-8', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)
LOGGER = logging.getLogger(__name__)
REDIS_SUBMIT_FREQUENCY_SECONDS = 30
CATEGORIZATION_BATCH_SIZE = 10
PEL_CHECK_FREQUENCY_SECONDS = 60

# For the producer, we need a background process to send tasks from the persistent storage to Redis
# Because we don't want to block the main thread that reads upstream tasks
@DOWNSTREAM_BROKER_CIRCUIT_BREAKER
def flush_producer_from_disk_to_queue(
    producer: OutboxProducer, 
    catcher: SignalCatcher, 
):
    name = threading.current_thread().name
    LOGGER.info(f"Thread {name} starting flush loop (Frequency: {REDIS_SUBMIT_FREQUENCY_SECONDS}s)")
    try:
        expire_time_seconds = 86400 * 1
        while not catcher.stop_event.is_set():
            producer.clear_tasks(expire_time_seconds=expire_time_seconds)
            start_time = time.time()
            total_flushed = 0
            while True:
                num_flushed = producer.flush_tasks_to_queue(batch_size=CATEGORIZATION_BATCH_SIZE)
                total_flushed += num_flushed
                if num_flushed == 0:
                    break 
            LOGGER.info(f"Flushed {total_flushed} tasks to the downstream queue")
            elapsed = time.time() - start_time
            sleep_duration = REDIS_SUBMIT_FREQUENCY_SECONDS - elapsed
            if sleep_duration > 0:
                catcher.stop_event.wait(sleep_duration)
            else:
                LOGGER.warning(
                    f"Flusher cycle took {elapsed:.2f}s, exceeding target frequency of {REDIS_SUBMIT_FREQUENCY_SECONDS}s. Skipping sleep."
                )      
            producer.clear_tasks(expire_time_seconds=expire_time_seconds)
    except Exception as e:
        LOGGER.fatal(f"Flusher thread failed with unhandled exception: {e}", exc_info=True)
    finally:
        producer.clear_tasks(expire_time_seconds=expire_time_seconds)
        catcher.stop_event.set()
        LOGGER.info(f"Thread {name} exiting.")

# Every now and then, we need the consumer to see if it needs to reprocess tasks
def _handle_PEL_batch(producer, consumer) -> None:
    LOGGER.debug("Fetching tasks from the upstream queue's PEL")
    while True:
        pel_tasks: list[TaskTransformationResult] = consumer.get_and_process_stuck_tasks()
        if len(pel_tasks) == 0:
            break 
        downstream_tasks = []
        for pel_task in pel_tasks:
            if pel_task.is_success:
                downstream_tasks.append(pel_task.task_result)
        LOGGER.debug(f"Found {len(downstream_tasks)} in the upstream queue's PEL")
        producer.produce_batch_to_disk(downstream_tasks)

def check_pending_entries_list(
    consumer: TaskWorker, 
    producer: OutboxProducer,
    catcher: SignalCatcher, 
):
    LOGGER.info(f"Starting PEL check (Frequency: {PEL_CHECK_FREQUENCY_SECONDS}s)")
    try:
        while not catcher.stop_event.is_set():
            start_time = time.time()
            _handle_PEL_batch(producer, consumer)
            elapsed = time.time() - start_time
            sleep_duration = PEL_CHECK_FREQUENCY_SECONDS - elapsed
            if sleep_duration > 0:
                catcher.stop_event.wait(sleep_duration)
            else:
                LOGGER.warning(
                    f"PEL check took {elapsed:.2f}s, exceeding target frequency of {PEL_CHECK_FREQUENCY_SECONDS}s. Skipping sleep."
                )      
    except Exception as e:
        LOGGER.fatal(f"PEL check failed with unhandled exception: {e}", exc_info=True)
    finally:
        catcher.stop_event.set()
        LOGGER.info("Thread exiting.")

def main(catcher):
    consumer = new_consumer()
    producer = new_producer()
    threads = [
        threading.Thread(
            group = None, 
            target=flush_producer_from_disk_to_queue,
            name="FlusherThread",
            args=(producer, catcher)
        ),
        threading.Thread(
            group=None,
            target=check_pending_entries_list,
            name="PelCheckerThread",
            args=(consumer, producer, catcher)
        )
    ]
    for thread in threads:
        thread.start()
    # Main thread pulls tasks from the upstream queue and processes them, 
    # then passes new tasks downstream. The PEL is relative to the consumer, which 
    # checks periodically for stuck tasks in other workers and claims them
    try:
        while not catcher.stop_event.wait(timeout=1):
            # We'll ignore `failed_tasks` from `get_and_process_tasks` and `produce_batch_to_disk`
            # by taking only the first entry, which are the successful tasks to send downstream
            LOGGER.info("Attempting to fetch upstream tasks and process them")
            results: list[TaskTransformationResult] = consumer.get_and_process_tasks()
            downstream_tasks = []
            for result in results:
                if result.is_success:
                    downstream_tasks.append(result.task_result)
            LOGGER.debug(f"Downstream tasks {downstream_tasks}")
            # TODO: we've already acknowledged the upstream task
            # so if this worker dies now, then we won't persist the message downstream
            if downstream_tasks:
                LOGGER.info(f"Processed batch: obtained {len(downstream_tasks)} tasks for downstream production.")
                producer.produce_batch_to_disk(downstream_tasks)
            else:
                LOGGER.info(f"No downstream tasks to be processed: {len(downstream_tasks)}")
    finally:
        catcher.stop_event.set()
        for t in threads:
            t.join(timeout=60)
            if t.is_alive():
                LOGGER.warning(f"{t.name} did not exit in time!")
    LOGGER.info("Worker loop finished. Exiting gracefully.")
    return 0


if __name__ == '__main__':
    with SignalCatcher() as catcher:
        exit_code = 0
        try:
            exit_code = main(catcher)
        except Exception as e:
            LOGGER.fatal(f"Startup check failed or unhandled error: {e}", exc_info=True)
            exit_code = 1
        finally:
            catcher.stop_event.set()
            LOGGER.info("Shutdown complete.")
            sys.exit(exit_code)