from celery.task import task
# from src.main import cel

@task()
def test_task():
    print("test ok")
