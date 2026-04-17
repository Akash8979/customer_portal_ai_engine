import json
import logging
from langchain_core.prompts import ChatPromptTemplate
from app.services import llm

logger = logging.getLogger(__name__)

prompt = ChatPromptTemplate.from_template("""
You are a support ticket deduplication engine.

Given a new ticket and a list of existing tickets, identify if any existing ticket is likely a duplicate.

New Ticket:
Title: {new_title}
Description: {new_description}

Existing Tickets (JSON list with id, title, description):
{existing_tickets}

Return ONLY valid JSON:
{{
  "is_duplicate": true | false,
  "duplicate_ticket_id": <id or null>,
  "confidence": 0.0-1.0,
  "reason": "<brief explanation>"
}}
""")


class DuplicateDetect:
    def run(new_title: str, new_description: str, existing_tickets: list[dict]):
        try:
            existing_json = json.dumps(existing_tickets[:20], indent=2)
            chain = prompt | llm._get_llm()
            response = chain.invoke({
                "new_title": new_title,
                "new_description": new_description,
                "existing_tickets": existing_json,
            })
            return json.loads(response.content.strip())
        except Exception as e:
            logger.error(f"Duplicate detection failed: {e}")
            return None
