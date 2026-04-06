"""
app/services/llm.py — LangChain + Claude chains

All chains are built here. classify and prioritise chains accept an optional
FewShotChatMessagePromptTemplate from few_shot.py, which is inserted between
the system message and the user message — the standard LangChain few-shot
pattern for chat models.

Chain anatomy (with few-shot examples):
    SystemMessage        ← task definition + output schema
    HumanMessage(ex 1)  ⎤
    AIMessage(ex 1)     ⎥ ← FewShotChatMessagePromptTemplate
    HumanMessage(ex 2)  ⎥   (selected by semantic similarity to query)
    AIMessage(ex 2)     ⎦
    HumanMessage        ← the actual ticket to classify/prioritise
    Claude → JSON response

Chain anatomy (no examples yet):
    SystemMessage
    HumanMessage        ← the actual ticket
    Claude → JSON response

Both variants use the same prompt template — the few-shot block simply
contributes zero messages when there are no examples in the store.
"""

import logging
import time
from dataclasses import dataclass
from typing import Any, Optional
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts.chat import HumanMessagePromptTemplate
from langchain_core.prompts import FewShotChatMessagePromptTemplate
from pydantic import BaseModel, Field
from app.env import MODEL,API_KEY,TEMPERATURE,MAX_TOKENS,MODEL_URL
logger = logging.getLogger(__name__)



# ─────────────────────────────────────────────────────────────────────────────
# Pydantic output schemas — LangChain validates Claude's response against these
# ─────────────────────────────────────────────────────────────────────────────

class ClassifyOutput(BaseModel):
    category:   str   = Field(description="The ticket category")
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning:  str   = Field(description="One sentence explanation")


class PrioritiseOutput(BaseModel):
    priority:   str   = Field(description="P1, P2, P3, or P4")
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning:  str   = Field(description="One sentence explanation")


class SuggestOutput(BaseModel):
    replies: list[str] = Field(description="2-3 draft reply options")

@dataclass
class LLMResult:
    content:    Any
    latency_ms: int = 0
    tokens:     int = 0



_CLASSIFY_SYSTEM = """\
You are a support ticket classifier for a software delivery team.
Classify the ticket into exactly one of the available categories.

Available categories:
{categories}

Return ONLY valid JSON — no markdown, no preamble:
{{
  "category":   "<one of the categories above>",
  "confidence": <float 0.0–1.0>,
  "reasoning":  "<one sentence>"
}}"""


# ─────────────────────────────────────────────────────────────────────────────
# LLM factory
# ─────────────────────────────────────────────────────────────────────────────

def _get_llm(temperature: float = None, max_tokens: int = None) -> ChatOpenAI:
    return ChatOpenAI(
    model=MODEL,
    base_url=MODEL_URL,
    api_key=API_KEY,
    temperature=temperature if temperature is not None else TEMPERATURE,
    max_tokens=max_tokens or MAX_TOKENS,
)

def run_classify(
    title: str,
    description: str,
    categories: list[str],
    fewshot: Optional[FewShotChatMessagePromptTemplate] = None,
) -> LLMResult:
    """
    Classify a ticket.

    Parameters
    ----------
    fewshot : FewShotChatMessagePromptTemplate | None
        Produced by few_shot.FewShotPromptBuilder.build().
        If provided, it is inserted between the system message and the
        user message — selected examples are semantically closest to the
        current ticket.
        If None (no promoted examples yet), the chain works without examples.
    """
    categories_str = (
        "\n".join(f"  - {c}" for c in categories)
        if categories
        else "  - General\n  - Technical\n  - Billing\n  - Infrastructure"
    )
    user_input = f'Ticket title: "{title}"\nTicket description: "{description}"'

    # Build message list dynamically
    messages = [
        SystemMessage(content=_CLASSIFY_SYSTEM.format(categories=categories_str))
    ]

    if fewshot:
        # FewShotChatMessagePromptTemplate.format_messages(input=query)
        # returns a list of Human/AI message pairs
        example_messages = fewshot.format_messages(input=user_input)
        messages.extend(example_messages)

    messages.append(HumanMessagePromptTemplate.from_template("{input}").format(input=user_input))

    llm    = _get_llm()
    parser = JsonOutputParser(pydantic_object=ClassifyOutput)
    start  = time.time()
    result = (llm | parser).invoke(messages)

    return LLMResult(result, latency_ms=int((time.time() - start) * 1000))
