from celery import Celery

from app.core.config import settings

celery_app = Celery(
    "worker",
    backend=f"db+{settings.SQLALCHEMY_DATABASE_URI}",
    broker="amqp://guest@queue//",
)

celery_app.conf.task_routes = {"app.worker.*": "main-queue"}
