import asyncio
import logging
import httpx
from types import SimpleNamespace
from app.queue import dequeue_pending, mark_done, mark_failed
from app.services.classify import Classify
from app.services.priority import Priority
from app import env
logger = logging.getLogger(__name__)


async def fetch_access_token(client: httpx.AsyncClient) -> str:
    response = await client.post(
        env.AUTH_URL + "/portal/user/login",
        json={"email": env.AUTH_EMAIL_ID, "password": env.AUTH_PASSWORD},
    )
    response.raise_for_status()
    return response.json()['tokens']["access"]


async def notify_update(ticket_id: int, data:dict):
    async with httpx.AsyncClient() as client:
        token = await fetch_access_token(client)
        url = f"{env.TICKET_UPDATE_URL}/portal/tickets/{ticket_id}/update?tenant_id=1"
        headers = {"Authorization": f"Bearer {token}"}
        response = await client.patch(url, json=data, headers=headers)
        response.raise_for_status()
        logger.info(f"Successfully notified update service for ticket {ticket_id}")


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
                if row["job_type"] == 'classify':
                    result = Classify.run(ticket)
                    logger.info(f"classify={result}")
                    if result is not None:
                        mark_done(row["queue_id"])
                        logger.info(f"Retry succeeded for ticket {ticket.id}: {result}")
                        await notify_update(result["id"],{"category":  result["category"].upper()})
                    else:
                        mark_failed(row["queue_id"], row["retry_count"])

                if row["job_type"] == 'priority':    
                    priority_result = Priority.run(ticket)
                    logger.info(f"priority={priority_result}")
                    if priority_result is not None:
                        mark_done(row["queue_id"])
                        logger.info(f"Retry succeeded for ticket {ticket.id}: {result}")
                        await notify_update(result["id"],{"priority":  priority_result["priority"].upper()})
                    else:
                        mark_failed(row["queue_id"], row["retry_count"])
            except Exception as e:
                logger.error(f"Retry failed for ticket {ticket.id}: {e}")
                mark_failed(row["queue_id"], row["retry_count"])
