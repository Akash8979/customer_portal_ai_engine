import json
import logging
from langchain_core.prompts import ChatPromptTemplate
from app.services import llm

logger = logging.getLogger(__name__)

prompt = ChatPromptTemplate.from_template("""
You are a churn risk assessment AI for a B2B SaaS platform.

Analyse the following account data and assess the probability of churn.

Account data:
{account_data}

Return ONLY valid JSON:
{{
  "churn_probability": 0.0-1.0,
  "risk_level": "LOW" | "MEDIUM" | "HIGH" | "CRITICAL",
  "top_signals": ["<signal 1>", "<signal 2>", "<signal 3>"],
  "recommended_actions": ["<action 1>", "<action 2>"],
  "summary": "<2-3 sentence churn risk summary>"
}}
""")


class ChurnRisk:
    def run(account_data: dict):
        try:
            chain = prompt | llm._get_llm()
            response = chain.invoke({"account_data": json.dumps(account_data, indent=2)})
            return json.loads(response.content.strip())
        except Exception as e:
            logger.error(f"Churn risk assessment failed: {e}")
            return None
