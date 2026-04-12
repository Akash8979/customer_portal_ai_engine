import logging
from app.services import llm
from langchain_core.prompts import ChatPromptTemplate

logger = logging.getLogger(__name__)


prompt = ChatPromptTemplate.from_template("""
You are a helpful support assistant.

Given the following support ticket, suggest a helpful comment that a support agent could post.
The comment should acknowledge the issue, provide guidance or next steps, and be professional and concise.

Return ONLY valid JSON:
{{
  "suggested_comment": "<comment text>"
}}

Ticket Title: {title}
Ticket Description: {description}
""")


class Suggest:
    def run(title: str, description: str):
        try:
            llm__ = llm._get_llm()
            chain = prompt | llm__
            response = chain.invoke({
                "title": title,
                "description": description,
            })
            return response.content.strip()
        except Exception as e:
            logger.error(f"LLM suggestion failed: {e}")
            return None
