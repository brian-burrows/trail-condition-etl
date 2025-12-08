"""worker_categorization

This is a driver script that runs the following ETL pipeline:

-> Fetch tasks from an upstream message broker
-> Fetch weather data from the weather service
-> Apply some custom logic to categorize trail conditions
-> Write the trail conditions backt to the weather service

It is a single threaded worker that gets fresh tasks and processes them,
then checks for stuck tasks, claims them, and processes them.

"""
import logging
import sys

from etl.handlers import SignalCatcher

from src.tasks import make_upstream_worker_and_dlq

logging.basicConfig(
    encoding="utf-8",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
LOGGER = logging.getLogger(__name__)


def main(catcher):
    consumer = make_upstream_worker_and_dlq()
    try:
        while not catcher.stop_event.wait(timeout=1):
            # TOOD: These should be instance arguments so that the main function doesn't have to know how the tasks are consumed.
            # The main function should just be responsible for orchestrating the components, not configuration.
            consumer.get_and_process_tasks()
            consumer.get_and_process_stuck_tasks()
    finally:
        catcher.stop_event.set()
    LOGGER.info("Worker loop finished. Exiting gracefully.")
    return 0


if __name__ == "__main__":
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
