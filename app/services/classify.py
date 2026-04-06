import logging
from app.services import llm
from app.queue import enqueue
from langchain_core.prompts import ChatPromptTemplate

logger = logging.getLogger(__name__)

# Prompt template
prompt = ChatPromptTemplate.from_template("""
You are a support ticket classifier.
x   
Classify the ticket into one of:
- bug
- feature
- billing

Only return the category.

Title: {title}
Description: {description}
""")


class Classify:
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
                "category": response.content.strip()
            }
        except Exception as e:
            logger.error(f"LLM failed for ticket {ticket.id}: {e}. Scheduling retry.")
            enqueue(ticket)
            return None
        
