import logging
from app.connection import get_connection

logger = logging.getLogger(__name__)


def enqueue(ticket) -> None:
    """Push a failed ticket into the DB retry queue."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_retry_queue (ticket_id, title, description, status, retry_count)
                VALUES (%s, %s, %s, 'pending', 0)
                """,
                (ticket.id, ticket.title, ticket.description),
            )
        conn.commit()
        logger.info(f"Ticket {ticket.id} added to retry queue.")
    finally:
        conn.close()


def dequeue_pending() -> list[dict]:
    """Fetch and lock all pending rows, mark them as processing."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE llm_retry_queue
                SET status = 'processing'
                WHERE id IN (
                    SELECT id FROM llm_retry_queue
                    WHERE status = 'pending'
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, ticket_id, title, description, retry_count
                """
            )
            rows = cur.fetchall()
        conn.commit()
        return [
            {"queue_id": r[0], "ticket_id": r[1], "title": r[2], "description": r[3], "retry_count": r[4]}
            for r in rows
        ]
    finally:
        conn.close()


def mark_done(queue_id: int) -> None:
    """Remove a successfully processed row."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM llm_retry_queue WHERE id = %s", (queue_id,))
        conn.commit()
    finally:
        conn.close()


def mark_failed(queue_id: int, retry_count: int) -> None:
    """Put a row back to pending for the next cycle."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE llm_retry_queue
                SET status = 'pending', retry_count = %s
                WHERE id = %s
                """,
                (retry_count + 1, queue_id),
            )
        conn.commit()
    finally:
        conn.close()
