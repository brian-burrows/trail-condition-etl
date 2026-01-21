import os 
import dotenv

class ProducerConfig:
    # Infrastructure
    REDIS_HOST: str = os.getenv("REDIS_MASTER_HOST", "redis-master")
    REDIS_PORT: int = int(os.getenv("REDIS_MASTER_PORT", 6379))
    
    # Storage
    SQLITE_DB_PATH: str = os.getenv(
        "CATEGORIZATION_SQLITE_DB", 
        "/tmp/categorization_producer_tasks.db"
    )
    
    # Stream Tuning
    STREAM_KEY: str = "owm-categorization-stream"
    MAX_STREAM_SIZE: int = 200
    DEQUEUE_BLOCKING_SECONDS: int = 1
    DEQUEUE_BATCH_SIZE: int = 100
    MIN_IDLE_SECONDS: int = 600  # 10 * 60

class ConsumerConfig:
    # Redis Infrastructure
    REDIS_HOST = os.environ.get("REDIS_MASTER_HOST", "redis-master")
    REDIS_PORT = int(os.environ.get("REDIS_MASTER_PORT", 6379))
    REDIS_MAX_CONNECTIONS = 2
    
    # Ingestion Stream Settings
    INGESTION_STREAM_KEY = "owm-ingestion-stream"
    CONSUMER_GROUP = "ingestion-workers"
    CONSUMER_NAME = os.environ.get("WORKER_NAME", f"worker-{os.getpid()}")
    
    # Resiliency & Tuning
    MAX_RETRIES = 6
    STREAM_MAX_LEN = 10000
    BATCH_SIZE = 100
    BLOCKING_TIME_SEC = 1
    IDLE_TIME_SEC = 600  # 10 minutes
    STUCK_TASK_SEC = 600
    EXPIRY_TASK_SEC = 86400  # 24 hours
    
    # API & Throttling
    WEATHER_SERVICE_URL = os.environ.get("WEATHER_SERVICE_URL", "http://outpost-api-weather:8000")
    OWM_MAX_DAILY_REQUESTS = 500
    RATE_LIMITER_KEY = "owm:daily:request:count:attempted"
    HTTP_TIMEOUT_SEC = 5