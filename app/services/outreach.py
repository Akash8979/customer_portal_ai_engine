import json
import logging
from langchain_core.prompts import ChatPromptTemplate
from app.services import llm

logger = logging.getLogger(__name__)

prompt = ChatPromptTemplate.from_template("""
You are a customer success manager drafting a professional outreach email for a B2B SaaS client.

Context about the account:
{account_data}

Purpose of outreach: {purpose}

Write a concise, professional email from the customer success team.
Tone: warm but business-focused.

Return ONLY valid JSON:
{{
  "subject": "<email subject>",
  "body": "<full email body in plain text>",
  "suggested_cta": "<call to action>"
}}
""")


class OutreachDraft:
    def run(account_data: dict, purpose: str):
        try:
            chain = prompt | llm._get_llm()
            response = chain.invoke({
                "account_data": json.dumps(account_data, indent=2),
                "purpose": purpose,
            })
            return json.loads(response.content.strip())
        except Exception as e:
            logger.error(f"Outreach draft failed: {e}")
            return None
