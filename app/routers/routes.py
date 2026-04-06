from fastapi import APIRouter
from pydantic import BaseModel
from app.services import llm
from langchain_core.prompts import ChatPromptTemplate


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

class TicketClassifyRequest(BaseModel):
    id:int
    title:str
    description:str
    

router = APIRouter(
    prefix="/portal/ai-engine",
    tags=["routes"]
)

@router.post("/ticket-classify", status_code=200)
async def classify_ticket(ticket: TicketClassifyRequest): 
    """
    Classify a ticket into a category using AI MODEL.

    Request body:
      {
        "ticket_id":    "number",
        "tenant_id":       "number",
        "title":        "Cannot login after password reset",
        "description":  "Users are getting a 401 in the EU region.",
        "categories":   ["billing", "bug", "infra", "access"]  // optional
      }

    Response (201):
      {
        "category":         "bug"
      }
    """
    # ticket    = request.get_json(silent=True) or {}
    # missing = [f for f in ("ticket_id", "tenant_id", "title", "description") if not ticket.get(f)]
    # if missing:
    #     return {"error": f"Missing required fields: {missing}"} 
    # title       = ticket["title"]
    # description = ticket["description"]
    # org_id      = ticket["tenant_id"]
    # categories  = ticket.get("categories") or []



    return {
        "message": "successfull"
    }


    # query = the ticket content — examples are selected by cosine similarity
    # query   = f"{title}\n{description}"
    # builder = few_shot.get_builder(org_id, "classify")
    # fewshot = builder.build(query=query, k=4)
    # # Count how many examples were actually selected (for the response)
    # examples_used = 0
    # if fewshot:
    #     try:
    #         selected = fewshot.example_selector.select_examples({"input": query})
    #         examples_used = len(selected)
    #     except Exception:
    #         pass
    # # ── Run the classify chain ────────────────────────────────────────────────
    # result  = llm.run_classify(
    #     title=title,
    #     description=description,
    #     categories=categories,
    #     fewshot=fewshot,
    # )
    # content = result.content or {}    
    # return {"message": "File uploaded", "filename": "file_location"}