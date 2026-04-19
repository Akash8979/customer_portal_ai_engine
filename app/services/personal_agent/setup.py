"""
Run once to create the DB tables used by the personal agent.
Called automatically on first use or via POST /portal/ai-engine/personal-agent/setup
"""
import logging
from app.connection import get_connection

logger = logging.getLogger(__name__)


def create_agent_tables():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS agent_long_term_memory (
                    id          SERIAL PRIMARY KEY,
                    user_id     TEXT NOT NULL,
                    key         TEXT NOT NULL,
                    value       TEXT NOT NULL,
                    importance  INTEGER NOT NULL DEFAULT 5,
                    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (user_id, key)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS agent_conversation_history (
                    id          SERIAL PRIMARY KEY,
                    user_id     TEXT NOT NULL,
                    session_id  TEXT NOT NULL,
                    role        TEXT NOT NULL,
                    content     TEXT NOT NULL,
                    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_agent_memory_user
                ON agent_long_term_memory (user_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_agent_history_session
                ON agent_conversation_history (user_id, session_id)
            """)
        conn.commit()
        logger.info("Agent tables created successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to create agent tables: {e}")
        raise
    finally:
        conn.close()
