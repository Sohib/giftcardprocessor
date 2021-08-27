from raven import Client

from app.core.celery_app import celery_app
from app.core.config import settings
from app.giftcardsprocessor import GiftCardProcessor

client_sentry = Client(settings.SENTRY_DSN)


@celery_app.task(acks_late=True)
def test_celery(word: str) -> str:
    return f"test task return {word}"


@celery_app.task()
def get_best_gift_card(csvfile):
    gift_cards_processor = GiftCardProcessor(open(csvfile, "rb"))
    max_row = gift_cards_processor.get_max(3)
    return {"top_product": max_row[1], "product_rating": float(max_row[2])}
