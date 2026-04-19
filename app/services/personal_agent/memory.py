"""
3-layer memory management for the persistent personal AI agent.

- Short-term  : active conversation messages (passed per request)
- Long-term   : stored in agent_long_term_memory table (key/value + importance)
- System      : user role, permissions, tenant — injected into system prompt
"""
import logging
import uuid
from app.connection import get_connection
logger = logging.getLogger(__name__)

# ── Long-term memory ─────────────────────────────────────────────────────────

def upsert_memory(user_id: str, key: str, value: str, importance: int = 5) -> None:
    """Store or update a long-term memory entry for a user."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO agent_long_term_memory (user_id, key, value, importance, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (user_id, key)
                DO UPDATE SET value = EXCLUDED.value,
                              importance = EXCLUDED.importance,
                              updated_at = NOW()
            """, (user_id, key, value, importance))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"upsert_memory failed: {e}")
        raise
    finally:
        conn.close()


def delete_memory(user_id: str, key: str) -> bool:
    """Remove a long-term memory entry. Returns True if row was deleted."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM agent_long_term_memory WHERE user_id = %s AND key = %s
            """, (user_id, key))
            deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    except Exception as e:
        conn.rollback()
        logger.error(f"delete_memory failed: {e}")
        raise
    finally:
        conn.close()


def get_all_memories(user_id: str) -> list[dict]:
    """Return all long-term memories for a user, ordered by importance desc."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT key, value, importance, updated_at
                FROM agent_long_term_memory
                WHERE user_id = %s
                ORDER BY importance DESC, updated_at DESC
            """, (user_id,))
            rows = cur.fetchall()
        return [
            {"key": r[0], "value": r[1], "importance": r[2], "updated_at": r[3].isoformat()}
            for r in rows
        ]
    finally:
        conn.close()


def search_memories(user_id: str, query: str) -> list[dict]:
    """Simple substring search across keys and values (case-insensitive)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT key, value, importance, updated_at
                FROM agent_long_term_memory
                WHERE user_id = %s AND (
                    key   ILIKE %s OR
                    value ILIKE %s
                )
                ORDER BY importance DESC, updated_at DESC
                LIMIT 20
            """, (user_id, f"%{query}%", f"%{query}%"))
            rows = cur.fetchall()
        return [
            {"key": r[0], "value": r[1], "importance": r[2], "updated_at": r[3].isoformat()}
            for r in rows
        ]
    finally:
        conn.close()


# ── Conversation history (short-term across sessions) ────────────────────────

def save_turn(user_id: str, session_id: str, role: str, content: str) -> None:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO agent_conversation_history (user_id, session_id, role, content)
                VALUES (%s, %s, %s, %s)
            """, (user_id, session_id, role, content))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"save_turn failed: {e}")
        raise
    finally:
        conn.close()


def get_session_history(user_id: str, session_id: str, limit: int = 20) -> list[dict]:
    """Return recent conversation turns for a session as {role, content} dicts."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT role, content
                FROM agent_conversation_history
                WHERE user_id = %s AND session_id = %s
                ORDER BY created_at ASC
                LIMIT %s
            """, (user_id, session_id, limit))
            rows = cur.fetchall()
        return [{"role": r[0], "content": r[1]} for r in rows]
    finally:
        conn.close()


def new_session_id() -> str:
    return str(uuid.uuid4())
