import logging
from app.services import llm
from langchain_core.prompts import ChatPromptTemplate
import json
logger = logging.getLogger(__name__)


prompt = ChatPromptTemplate.from_template("""
You are a helpful support assistant.

Summarize the following comment thread from a support ticket.
The summary should capture the key points, any resolutions reached, and the current status of the issue.

Return ONLY valid JSON:
{{
  "summary": "<concise summary of the comment thread>",
}}

Comment thread:
{comments}
""")


class CommentSummarize:
    def run(comments: list[str]):
        try:
            comment_text = "\n".join(f"- {c}" for c in comments)

            llm__ = llm._get_llm()
            chain = prompt | llm__
            response = chain.invoke({"comments": comment_text})
            if not response:
                return {}
            return json.loads(response.content.strip())
        except Exception as e:
            logger.error(f"LLM summarization failed: {e}")
            return None
