import asyncio
import logging
import httpx
from types import SimpleNamespace
from app.queue import dequeue_pending, mark_done, mark_failed
from app.services.classify import Classify
from app import env
logger = logging.getLogger(__name__)


async def fetch_access_token(client: httpx.AsyncClient) -> str:
    response = await client.post(
        env.AUTH_URL + "/portal/user/login",
        json={"email": env.AUTH_EMAIL_ID, "password": env.AUTH_PASSWORD},
    )
    response.raise_for_status()
    return response.json()['tokens']["access"]


async def notify_update(ticket_id: int, category: str):
    async with httpx.AsyncClient() as client:
        token = await fetch_access_token(client)
        logger.info(f"Logins {ticket_id}: {category}")
        url = f"{env.TICKET_UPDATE_URL}/portal/tickets/{ticket_id}/update?tenant_id=1"
        headers = {"Authorization": f"Bearer {token}"}
        response = await client.patch(url, json={"category": category.upper()}, headers=headers)
        response.raise_for_status()
        logger.info(f"Successfully notified update service for ticket {ticket_id}: {category}")


async def scheduled_task():
    while True:
        await asyncio.sleep(20)
        pending = dequeue_pending()
        if not pending:
            continue

        for row in pending:
            ticket = SimpleNamespace(
                id=row["ticket_id"],
                title=row["title"],
                description=row["description"],
            )
            logger.info(f"Retrying LLM for ticket {ticket.id}")
            try:
                result = Classify.run(ticket)
                logger.info(f"{result}")
                if result is not None:
                    mark_done(row["queue_id"])
                    logger.info(f"Retry succeeded for ticket {ticket.id}: {result}")
                    await notify_update(result["id"], result["category"])
                else:
                    mark_failed(row["queue_id"], row["retry_count"])
            except Exception as e:
                logger.error(f"Retry failed for ticket {ticket.id}: {e}")
                mark_failed(row["queue_id"], row["retry_count"])
