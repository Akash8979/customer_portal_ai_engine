from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from app.connection import get_connection
from app.services.suggest import Suggest
from app.services.summarize import CommentSummarize
from app.services.sentiment import SentimentAnalyse
from app.services.duplicate import DuplicateDetect
from app.services.account_health import AccountHealth
from app.services.release_notes import ReleaseNotesDraft
from app.services.churn_risk import ChurnRisk
from app.services.outreach import OutreachDraft
from app.services.agent_run import AgentRun, OnboardingRecovery
from app.services.personal_agent import agent as personal_agent
from app.services.personal_agent import memory as agent_memory
from app.services.personal_agent.setup import create_agent_tables
from app.services.personal_agent.memory import new_session_id
import json
import logging

logger = logging.getLogger(__name__)


class TicketClassifyRequest(BaseModel):
    id: int
    title: str
    description: str


class SentimentRequest(BaseModel):
    comment: str


class DuplicateRequest(BaseModel):
    new_title: str
    new_description: str
    existing_tickets: list[dict]


class AccountHealthRequest(BaseModel):
    account_data: dict


class ReleaseNotesRequest(BaseModel):
    version: str
    features: list[dict]
    bug_fixes: list[dict]


class ChurnRiskRequest(BaseModel):
    account_data: dict


class OutreachRequest(BaseModel):
    account_data: dict
    purpose: str


class AgentRunRequest(BaseModel):
    user_prompt: str
    context_data: dict = {}


class OnboardingRecoveryRequest(BaseModel):
    onboarding_data: dict
    days_behind: int


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


@router.post("/ticket-sentiment", status_code=200)
async def analyse_sentiment(req: SentimentRequest):
    """Analyse sentiment of a client comment."""
    result = SentimentAnalyse.run(req.comment)
    if not result:
        return {"error": "Sentiment analysis failed"}
    return result


@router.post("/ticket-duplicate-check", status_code=200)
async def check_duplicate(req: DuplicateRequest):
    """Detect if a new ticket is a duplicate of existing ones."""
    result = DuplicateDetect.run(req.new_title, req.new_description, req.existing_tickets)
    if not result:
        return {"error": "Duplicate detection failed"}
    return result


@router.post("/account-health", status_code=200)
async def account_health(req: AccountHealthRequest):
    """Generate AI health score for a client account."""
    result = AccountHealth.run(req.account_data)
    if not result:
        return {"error": "Account health analysis failed"}
    return result


@router.post("/draft-release-notes", status_code=200)
async def draft_release_notes(req: ReleaseNotesRequest):
    """Draft release notes from a list of features and bug fixes."""
    result = ReleaseNotesDraft.run(req.version, req.features, req.bug_fixes)
    if not result:
        return {"error": "Release notes draft failed"}
    return result


@router.post("/churn-risk", status_code=200)
async def churn_risk(req: ChurnRiskRequest):
    """Assess churn risk for a client account."""
    result = ChurnRisk.run(req.account_data)
    if not result:
        return {"error": "Churn risk assessment failed"}
    return result


@router.post("/draft-outreach", status_code=200)
async def draft_outreach(req: OutreachRequest):
    """Draft a client outreach email."""
    result = OutreachDraft.run(req.account_data, req.purpose)
    if not result:
        return {"error": "Outreach draft failed"}
    return result


@router.post("/agent-run", status_code=200)
async def agent_run(req: AgentRunRequest):
    """Run a free-prompt AI agent action."""
    result = AgentRun.run(req.user_prompt, req.context_data)
    if not result:
        return {"error": "Agent run failed"}
    return result


@router.post("/onboarding-recovery-plan", status_code=200)
async def onboarding_recovery(req: OnboardingRecoveryRequest):
    """Generate an onboarding recovery plan for an at-risk client."""
    result = OnboardingRecovery.run(req.onboarding_data, req.days_behind)
    if not result:
        return {"error": "Recovery plan generation failed"}
    return result


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


# ── Personal Agent ────────────────────────────────────────────────────────────

class PersonalAgentChatRequest(BaseModel):
    user_id: str = Field(..., description="Unique user identifier (email or DB id)")
    role: str = Field(..., description="User role: CLIENT_ADMIN, CLIENT_USER, AGENT, LEAD, ADMIN")
    tenant_id: Optional[str] = Field(None, description="Tenant ID for access scoping")
    message: str = Field(..., description="The user's message to the agent")
    session_id: Optional[str] = Field(None, description="Resume an existing session; omit to start new")


class MemoryUpdateRequest(BaseModel):
    key: str
    value: str
    importance: int = Field(5, ge=1, le=10)


@router.post("/personal-agent/setup", status_code=200)
async def personal_agent_setup():
    """Create DB tables required by the personal agent (run once)."""
    try:
        create_agent_tables()
        return {"message": "Agent tables created successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/personal-agent/chat", status_code=200)
async def personal_agent_chat(req: PersonalAgentChatRequest):
    """
    Send a message to the user's persistent personal AI agent.

    The agent:
    - Loads long-term memory (user preferences, past context) into its system prompt
    - Loads conversation history for the given session_id
    - Uses tool calls to save/update/forget memories during the turn
    - Returns the response and the session_id (for follow-up turns)
    """
    session = req.session_id or new_session_id()
    try:
        result = personal_agent.run_agent(
            user_id=req.user_id,
            role=req.role,
            tenant_id=req.tenant_id,
            message=req.message,
            session_id=session,
        )
        return {"data": result}
    except Exception as e:
        logger.error(f"Personal agent chat failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/personal-agent/memory/{user_id}", status_code=200)
async def get_agent_memory(user_id: str):
    """List all long-term memories stored for a user."""
    memories = agent_memory.get_all_memories(user_id)
    return {"data": memories, "total": len(memories)}


@router.delete("/personal-agent/memory/{user_id}/{key}", status_code=200)
async def delete_agent_memory(user_id: str, key: str):
    """Remove a specific long-term memory entry for a user."""
    removed = agent_memory.delete_memory(user_id, key)
    if not removed:
        raise HTTPException(status_code=404, detail=f"Memory key '{key}' not found for user.")
    return {"message": f"Memory '{key}' removed."}


@router.put("/personal-agent/memory/{user_id}", status_code=200)
async def upsert_agent_memory(user_id: str, req: MemoryUpdateRequest):
    """Manually add or update a long-term memory entry for a user."""
    agent_memory.upsert_memory(user_id, req.key, req.value, req.importance)
    return {"message": f"Memory '{req.key}' saved."}
