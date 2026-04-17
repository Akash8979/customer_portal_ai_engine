import json
import logging
from langchain_core.prompts import ChatPromptTemplate
from app.services import llm

logger = logging.getLogger(__name__)

prompt = ChatPromptTemplate.from_template("""
You are a customer success AI analysing the health of a B2B SaaS client account.

Account data:
{account_data}

Generate a health score (0-100) and a breakdown. A higher score = healthier.

Return ONLY valid JSON:
{{
  "health_score": <0-100>,
  "rating": "HEALTHY" | "AT_RISK" | "CRITICAL",
  "breakdown": {{
    "ticket_volume": <0-100>,
    "sla_compliance": <0-100>,
    "csat": <0-100>,
    "onboarding_progress": <0-100>,
    "response_sentiment": <0-100>
  }},
  "summary": "<2-3 sentence account health summary>",
  "top_risks": ["<risk 1>", "<risk 2>"],
  "recommendations": ["<action 1>", "<action 2>"]
}}
""")


class AccountHealth:
    def run(account_data: dict):
        try:
            chain = prompt | llm._get_llm()
            response = chain.invoke({"account_data": json.dumps(account_data, indent=2)})
            return json.loads(response.content.strip())
        except Exception as e:
            logger.error(f"Account health analysis failed: {e}")
            return None
