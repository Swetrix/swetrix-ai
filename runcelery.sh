REDIS_BROKER=redis://10.0.0.2:6379/0 REDIS_BACKEND=redis://10.0.0.2:6379/0 celery -A celery_tasks.celery_config worker --loglevel=info
