CELERY_IMPORTS = ('src.tasks')
CELERY_IGNORE_RESULTS = False
BROKER_HOST = "127.0.0.1"
BROKER_PORT = 15672
BROKER_URL = 'amqp://'
# BROKER_USER = "kri"
# BROKER_PASSWORD
# BROKER_POOL_LIMIT = None
from datetime import timedelta

CELERYBEAT_SCHEDULE = {
    'test_task':{
        'task':'tasks.test_task',
        'schedule':timedelta(seconds=10),
    },
}
