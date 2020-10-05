CELERY_IMPORTS = ('src.tasks')
CELERY_IGNORE_RESULTS = False
BROKER_HOST = "rabbit"
BROKER_PORT = 5672
CELERY_BROKER_URL = 'amqp://'
# BROKER_USER = "kri"
# BROKER_PASSWORD
# BROKER_POOL_LIMIT = None
from datetime import timedelta

CELERYBEAT_SCHEDULE = {
    'test_task':{
        'task':'src.tasks.test_task',
        'schedule':timedelta(seconds=60),
    },
}
