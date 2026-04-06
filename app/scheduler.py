import asyncio
import logging
from types import SimpleNamespace
from app.queue import dequeue_pending, mark_done, mark_failed

logger = logging.getLogger(__name__)


async def scheduled_task():
    while True:
        await asyncio.sleep(20)

        pending = dequeue_pending()
        if not pending:
            continue

        from app.services.cassify import Classify

        for row in pending:
            ticket = SimpleNamespace(
                id=row["ticket_id"],
                title=row["title"],
                description=row["description"],
            )
            logger.info(f"Retrying LLM for ticket {ticket.id}")
            try:
                result = Classify.run(ticket)
                if result is not None:
                    mark_done(row["queue_id"])
                    logger.info(f"Retry succeeded for ticket {ticket.id}: {result}")
                else:
                    mark_failed(row["queue_id"], row["retry_count"])
            except Exception as e:
                logger.error(f"Retry failed for ticket {ticket.id}: {e}")
                mark_failed(row["queue_id"], row["retry_count"])
