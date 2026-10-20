from app.common.config import settings


def retry_delay(delivery_count: int) -> int:
    """Capped exponential backoff in seconds."""
    return min(2 ** max(0, delivery_count - 1), 30)


def should_dead_letter(delivery_count: int) -> bool:
    return delivery_count >= settings.max_deliveries
