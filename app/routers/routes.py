from fastapi import APIRouter
from pydantic import BaseModel
from app.connection import get_connection
from app.services.suggest import Suggest
from app.services.summarize import CommentSummarize
import json
import logging

logger = logging.getLogger(__name__)


class TicketClassifyRequest(BaseModel):
    id: int
    title: str
    description: str


router = APIRouter(prefix="/portal/ai-engine", tags=["routes"])


@router.post("/ticket-classify", status_code=200)
async def classify_ticket(ticket: TicketClassifyRequest):
    """
    Classify a ticket into a category using AI MODEL.

    Request body:
      {
        "ticket_id":    "number",
        "tenant_id":       "number",
        "title":        "Cannot login after password reset",
        "description":  "Users are getting a 401 in the EU region.",
        "categories":   ["billing", "bug", "infra", "access"]  // optional
      }

    Response (201):
      {
        "category": "bug"
      }
    """
    # ticket    = request.get_json(silent=True) or {}
    # missing = [f for f in ("ticket_id", "tenant_id", "title", "description") if not ticket.get(f)]
    # if missing:
    #     return {"error": f"Missing required fields: {missing}"}
    # title       = ticket["title"]
    # description = ticket["description"]
    # org_id      = ticket["tenant_id"]
    # categories  = ticket.get("categories") or []

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_retry_queue (ticket_id, title, description, status, retry_count, job_type)
                VALUES (%s, %s, %s, 'pending', 0, 'classify'),(%s, %s, %s, 'pending', 0, 'priority')
                """,
                (
                    ticket.id,
                    ticket.title,
                    ticket.description,
                    ticket.id,
                    ticket.title,
                    ticket.description,
                ),
            )
        conn.commit()
        logger.info(f"Ticket {ticket.id} added to retry queue.")
    finally:
        conn.close()

    return {"message": "successfull"}

    # query = the ticket content — examples are selected by cosine similarity
    # query   = f"{title}\n{description}"
    # builder = few_shot.get_builder(org_id, "classify")
    # fewshot = builder.build(query=query, k=4)
    # # Count how many examples were actually selected (for the response)
    # examples_used = 0
    # if fewshot:
    #     try:
    #         selected = fewshot.example_selector.select_examples({"input": query})
    #         examples_used = len(selected)
    #     except Exception:
    #         pass
    # # ── Run the classify chain ────────────────────────────────────────────────
    # result  = llm.run_classify(
    #     title=title,
    #     description=description,
    #     categories=categories,
    #     fewshot=fewshot,
    # )
    # content = result.content or {}
    # return {"message": "File uploaded", "filename": "file_location"}


@router.get("/ticket-comment-suggest/{ticket_id}", status_code=200)
async def suggest_ticket(ticket_id: int):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT title, description, category
                FROM portal_ticket
                WHERE id = %s
                """,
                (ticket_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    result = Suggest.run(row[0], row[1])
    if not result:
        return {"error": "Failed to generate suggestion"}

    return {"ticket_id":ticket_id ,
            "suggested_comment":result['suggested_comment']
        }


@router.get("/ticket-comment-summary/{ticket_id}", status_code=200)
async def summarize_ticket_comments(ticket_id: int):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT message
                FROM portal_comment
                WHERE ticket_id = %s
                ORDER BY created_at ASC
                limit 10
                """,
                (ticket_id,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return {"error": "No comments found for this ticket"}

    comments = [row[0] for row in rows]

    result = CommentSummarize.run(comments)
    if not result:
        return {"error": "Failed to generate summary"}

    return {"ticket_id":ticket_id ,
            "summary":result['summary']
        }


@router.get("/table_create", status_code=200)
async def table_create():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS llm_retry_queue (
                    id          SERIAL PRIMARY KEY,
                    ticket_id   INTEGER NOT NULL,
                    title       TEXT NOT NULL,
                    description TEXT NOT NULL,
                    status      TEXT NOT NULL DEFAULT 'pending',
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    job_type    TEXT DEFAULT NULL,
                    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        conn.commit()
        return {"message": "Table created successfully"}
    finally:
        conn.close()
