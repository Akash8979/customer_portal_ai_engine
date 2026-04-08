import logging
from app.services import llm
from langchain_core.prompts import ChatPromptTemplate

logger = logging.getLogger(__name__)

# Prompt template
prompt = ChatPromptTemplate.from_template("""
You are a support ticket priority classifier.

Assign a priority to the ticket based on its urgency and impact. Choose one of:
- low
- medium
- high
- critical

Only return the priority.

Title: {title}
Description: {description}
""")


class Priority:
    def run(ticket):
        try:
            llm__    = llm._get_llm()
            chain = prompt | llm__
            response = chain.invoke({
                "title": ticket.title,
                "description": ticket.description
            })
            return {
                "id": ticket.id,
                "priority": response.content.strip()
            }
        except Exception as e:
            logger.error(f"LLM failed for priority on ticket {ticket.id}: {e}.")
            return None
        
