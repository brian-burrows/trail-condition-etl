import os 
from dotenv import load_dotenv

load_dotenv()
class ProducerConfig:
    # Infrastructure
    def __init__(self):
        self.REDIS_HOST: str = os.getenv("REDIS_MASTER_HOST", "redis-master")
        self.REDIS_PORT: int = int(os.getenv("REDIS_MASTER_PORT", 6379))
        
        # Storage
        self.SQLITE_DB_PATH: str = os.getenv(
            "CATEGORIZATION_SQLITE_DB", 
            "/tmp/categorization_producer_tasks.db"
        )
        
        # Stream Tuning
        self.STREAM_KEY: str = "owm-categorization-stream"
        self.MAX_STREAM_SIZE: int = 200
        self.DEQUEUE_BLOCKING_SECONDS: int = 1
        self.DEQUEUE_BATCH_SIZE: int = 100
        self.MIN_IDLE_SECONDS: int = 600  # 10 * 60

class ConsumerConfig:
    def __init__(self):
        # Redis Infrastructure
        self.REDIS_HOST = os.environ.get("REDIS_MASTER_HOST", "redis-master")
        self.REDIS_PORT = int(os.environ.get("REDIS_MASTER_PORT", 6379))
        self.REDIS_MAX_CONNECTIONS = 2
        
        # Ingestion Stream Settings
        self.INGESTION_STREAM_KEY = "owm-ingestion-stream"
        self.CONSUMER_GROUP = "ingestion-workers"
        self.CONSUMER_NAME = os.environ.get("WORKER_NAME", f"worker-{os.getpid()}")
        
        # Resiliency & Tuning
        self.MAX_RETRIES = 6
        self.STREAM_MAX_LEN = 10000
        self.BATCH_SIZE = 100
        self.BLOCKING_TIME_SEC = 1
        self.IDLE_TIME_SEC = 600  # 10 minutes
        self.STUCK_TASK_SEC = 600
        self.EXPIRY_TASK_SEC = 86400  # 24 hours
        
        # API & Throttling
        self.WEATHER_SERVICE_URL = os.environ.get("WEATHER_SERVICE_URL", "http://outpost-api-weather:8000")
        self.OWM_MAX_DAILY_REQUESTS = 500
        self.RATE_LIMITER_KEY = "owm:daily:request:count:attempted"
        self.HTTP_TIMEOUT_SEC = 5