import json
import logging
from langchain_core.prompts import ChatPromptTemplate
from app.services import llm

logger = logging.getLogger(__name__)

prompt = ChatPromptTemplate.from_template("""
You are a sentiment analysis engine for a B2B SaaS support platform.

Analyse the following client comment and return the sentiment.

Return ONLY valid JSON:
{{
  "sentiment": "POSITIVE" | "NEUTRAL" | "NEGATIVE" | "FRUSTRATED",
  "confidence": 0.0-1.0,
  "signals": ["<brief reason>"]
}}

Comment:
{comment}
""")


class SentimentAnalyse:
    def run(comment: str):
        try:
            chain = prompt | llm._get_llm()
            response = chain.invoke({"comment": comment})
            return json.loads(response.content.strip())
        except Exception as e:
            logger.error(f"Sentiment analysis failed: {e}")
            return None
