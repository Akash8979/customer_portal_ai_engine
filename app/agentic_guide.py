"""
=============================================================================
AGENTIC AI — COMPLETE IMPLEMENTATION GUIDE
=============================================================================
This file teaches you every stage of building an AI agent from scratch.
Every single line of code has a comment explaining WHAT it does and WHY.

READ THIS FILE TOP TO BOTTOM. Each stage builds on the previous one.
Do not jump to Stage 4 before understanding Stage 2.

STAGES IN THIS FILE:
  Stage 0  — Setup: imports, clients, config
  Stage 1  — Single LLM call (not an agent yet, just understanding Claude)
  Stage 2  — Tool definitions (describing functions to Claude)
  Stage 3  — Tool execution (running the functions Claude requests)
  Stage 4  — The ReAct loop (the actual agent engine)
  Stage 5  — System prompts (how to instruct an agent)
  Stage 6  — Multi-agent (supervisor + worker pattern)
  Stage 7  — Memory (stateful agents that remember between runs)
  Stage 8  — Human-in-the-loop (approval before write actions)
  Stage 9  — Flask integration (wiring agents into your portal routes)
  Stage 10 — Production hardening (logging, error handling, cost control)

HOW TO USE THIS FILE:
  1. Read a stage completely before trying to run it
  2. Run each stage in isolation first (call the function at the bottom)
  3. Then integrate into your existing Flask portal
  4. Never skip stages — complexity builds up

PREREQUISITES:
  pip install anthropic langchain langchain-anthropic flask celery redis

=============================================================================
"""


# =============================================================================
# STAGE 0 — SETUP
# =============================================================================
# Before anything else, we need:
#   1. The Anthropic client (talks to Claude)
#   2. Environment variables (API keys)
#   3. A few standard library imports
# =============================================================================

import os           # os.environ reads environment variables
import json         # json.dumps/loads converts between Python dicts and strings
import time         # time.time() measures how long things take
import logging      # logging.info() writes messages to your log file

from datetime import datetime, timezone  # datetime.now() for timestamps

# anthropic is the official Python SDK for Claude
# Install it: pip install anthropic
import anthropic

# We'll use this for type hints — makes code easier to read
from typing import Optional

# Set up logging so we can see what the agent is doing
# Format: [2026-03-30 10:00:00] INFO agent_guide: message
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("agent_guide")


# -----------------------------------------------------------------------------
# The Anthropic client
# -----------------------------------------------------------------------------
# This is the object that makes API calls to Claude.
# We create it once and reuse it everywhere.
# It reads ANTHROPIC_API_KEY from environment automatically.
#
# In your .env file add:
#   ANTHROPIC_API_KEY=sk-ant-your-key-here
#
# Never hardcode your API key in code — anyone who reads your code gets it.
# -----------------------------------------------------------------------------
client = anthropic.Anthropic(
    api_key=os.environ.get("ANTHROPIC_API_KEY")  # reads from environment
)

# The model we'll use throughout this file.
# claude-3-5-sonnet-20241022 = good balance of speed, cost, intelligence
# claude-3-haiku-20240307    = faster + cheaper, less capable (use for simple tasks)
# claude-3-opus-20240229     = most capable, most expensive (use for complex reasoning)
MODEL = "claude-3-5-sonnet-20241022"


# =============================================================================
# STAGE 1 — SINGLE LLM CALL
# =============================================================================
# Before building agents, understand exactly what happens in one Claude call.
# This is the foundation. If you don't understand this, agents won't make sense.
#
# A single call is:
#   YOU send:  a list of messages (the conversation)
#   CLAUDE sends back: a response object with content + stop_reason
#
# stop_reason tells you WHY Claude stopped talking:
#   "end_turn"   = Claude is done, here is the final answer
#   "tool_use"   = Claude wants to call a tool (we'll see this in Stage 3)
#   "max_tokens" = Claude ran out of token budget (increase max_tokens)
# =============================================================================

def stage1_single_call_explained():
    """
    Run a single Claude call and understand every part of the response.
    This is NOT an agent — just one request + one response.
    """
    logger.info("=== STAGE 1: Single LLM call ===")

    # ─────────────────────────────────────────────────────────────────────────
    # THE REQUEST
    # ─────────────────────────────────────────────────────────────────────────

    response = client.messages.create(
        # Which Claude model to use
        model = MODEL,

        # Maximum tokens Claude is ALLOWED to generate in its response.
        # 1 token ≈ 0.75 words. 1024 tokens ≈ 750 words.
        # If Claude hits this limit, stop_reason becomes "max_tokens"
        # and the response is cut off mid-sentence.
        max_tokens = 1024,

        # The system prompt defines who Claude is and what rules to follow.
        # Think of it as the job description given to a new employee.
        # It is NOT part of the conversation — it's background context.
        system = "You are a support ticket classifier for a software delivery company.",

        # The messages list is the actual conversation.
        # Each message has:
        #   role    = "user" (your code speaking) or "assistant" (Claude speaking)
        #   content = the text of the message
        #
        # IMPORTANT: Claude has NO memory between API calls.
        # The messages list IS the memory.
        # To give Claude context of past conversation, include it here.
        messages = [
            {
                "role":    "user",    # this is your code speaking to Claude
                "content": "Classify this ticket: 'Login broken for all users after the 2pm deploy'"
            }
        ]
    )

    # ─────────────────────────────────────────────────────────────────────────
    # THE RESPONSE
    # ─────────────────────────────────────────────────────────────────────────

    # stop_reason: WHY did Claude stop generating text?
    print(f"stop_reason: {response.stop_reason}")
    # → "end_turn" means Claude is done and the response is complete

    # content: A LIST of content blocks.
    # Usually just one TextBlock, but can have ToolUseBlock too (Stage 3)
    print(f"content type: {type(response.content[0])}")
    # → <class 'anthropic.types.text_block.TextBlock'>

    # The actual text Claude wrote
    print(f"text: {response.content[0].text}")
    # → "Category: bug, Confidence: high, Reasoning: ..."

    # usage: How many tokens were used (affects your bill)
    print(f"input_tokens: {response.usage.input_tokens}")   # tokens YOU sent
    print(f"output_tokens: {response.usage.output_tokens}") # tokens Claude sent back

    return response.content[0].text


def stage1_getting_json_back():
    """
    The problem: Claude sometimes adds explanation text before/after JSON.
    "Sure! Here is the JSON you asked for: {...}"
    We need JUST the JSON. Two solutions: prompt engineering or LangChain parser.
    """
    logger.info("=== STAGE 1: Getting reliable JSON ===")

    # ─────────────────────────────────────────────────────────────────────────
    # SOLUTION A: Tell Claude very explicitly to ONLY return JSON.
    # Works most of the time. Simple to implement.
    # ─────────────────────────────────────────────────────────────────────────
    response = client.messages.create(
        model      = MODEL,
        max_tokens = 512,
        system = """
        You are a ticket classifier. You MUST return ONLY valid JSON.
        No text before the JSON. No text after. No markdown code fences.
        Just the raw JSON object.

        Required format:
        {
          "category":   "<one of: bug, billing, infra, access, performance, feature>",
          "confidence": <float between 0.0 and 1.0>,
          "reasoning":  "<one sentence explaining your choice>"
        }
        """,
        messages = [
            {
                "role": "user",
                "content": "Classify: 'Login broken after 2pm deploy'"
            }
        ]
    )

    raw_text = response.content[0].text
    # raw_text should be: '{"category":"bug","confidence":0.91,"reasoning":"..."}'

    # json.loads() converts JSON string → Python dict
    # If Claude returns invalid JSON, this will raise json.JSONDecodeError
    result = json.loads(raw_text)

    print(f"category:   {result['category']}")    # → "bug"
    print(f"confidence: {result['confidence']}")  # → 0.91
    print(f"reasoning:  {result['reasoning']}")   # → "Login issue after deploy indicates a bug"

    return result


def stage1_multi_turn_conversation():
    """
    Showing how the messages list works as memory.
    Claude only knows what you include in messages[].
    This is critical for understanding agents — agents work by
    appending to this list on every loop iteration.
    """
    logger.info("=== STAGE 1: Multi-turn conversation ===")

    # Start with the first message
    messages = [
        {
            "role":    "user",
            "content": "Classify this ticket: 'Login broken after 2pm deploy'"
        }
    ]

    # First call — Claude classifies the ticket
    response1 = client.messages.create(
        model=MODEL, max_tokens=256,
        system="You are a ticket classifier. Return only JSON.",
        messages=messages
    )

    classification = response1.content[0].text  # e.g. '{"category":"bug",...}'
    print(f"Turn 1 — Claude said: {classification}")

    # APPEND Claude's response to messages.
    # Now messages has 2 items: user question + Claude's answer.
    messages.append({
        "role":    "assistant",  # Claude's role is "assistant"
        "content": classification
    })

    # APPEND a follow-up question
    messages.append({
        "role":    "user",
        "content": "Now suggest which team should handle this bug ticket."
    })

    # Second call — Claude now sees the ENTIRE conversation (both turns)
    # It knows what it classified in turn 1 and can reference it.
    response2 = client.messages.create(
        model=MODEL, max_tokens=256,
        system="You are a ticket classifier. Return only JSON.",
        messages=messages  # includes all 3 messages
    )

    print(f"Turn 2 — Claude said: {response2.content[0].text}")
    # Claude will say something like "backend team" because it remembers
    # it classified this as a bug in turn 1

    # KEY INSIGHT: messages is the ONLY state Claude has.
    # In agents, every loop iteration appends to this list.
    # Claude reads the WHOLE list at every call to decide what to do next.


# =============================================================================
# STAGE 2 — TOOL DEFINITIONS
# =============================================================================
# Tools are functions you describe to Claude so it can request them.
# Claude reads the descriptions and decides WHEN to call each tool.
#
# A tool definition has 3 parts:
#   name         → how Claude refers to the tool (like a function name)
#   description  → WHY/WHEN to use this tool (Claude reads this to decide)
#   input_schema → WHAT parameters Claude must provide (like function args)
#
# CRITICAL: Claude reads only the description, not your Python code.
# Bad description = Claude calls the wrong tool or misuses it.
# Write descriptions like you're explaining to a smart junior developer.
# =============================================================================

# Define all tools your ticket investigation agent can use.
# We define them as a Python list of dicts.
# This list gets passed to client.messages.create(tools=TOOLS)

TICKET_INVESTIGATION_TOOLS = [

    # ─────────────────────────────────────────────────────────────────────────
    # TOOL 1: Search knowledge base
    # ─────────────────────────────────────────────────────────────────────────
    {
        # name: how Claude refers to this tool
        # Rule: use snake_case, make it clear what it does
        "name": "search_knowledge_base",

        # description: Claude reads this to decide WHEN to call this tool.
        # Be specific. Tell Claude: what it does, what it returns, WHEN to use it.
        # Bad: "Search KB" → too vague, Claude doesn't know when to use it
        # Good: detailed description below
        "description": """Search the 3SC knowledge base for documented solutions.
        Use this tool FIRST for any technical issue — there may already be a
        documented fix. Returns a list of relevant articles with titles and summaries.
        Input: a search query with keywords from the ticket title/description.""",

        # input_schema: defines what parameters Claude must provide.
        # This is JSON Schema format (same as OpenAPI).
        "input_schema": {
            "type": "object",  # the input is always an object (dict)
            "properties": {
                # Each key is a parameter Claude can pass
                "query": {
                    "type": "string",  # must be a string
                    # description: Claude reads this to understand what to put here
                    "description": "Search query. Use keywords from the ticket. E.g. 'login 401 error EU region'"
                },
                "max_results": {
                    "type": "integer",  # must be a whole number
                    "description": "Maximum number of articles to return. Default 5.",
                    "default": 5        # Claude uses this if it doesn't specify
                }
            },
            # required: which parameters Claude MUST provide (no default)
            "required": ["query"]
            # max_results is NOT required because it has a default value
        }
    },

    # ─────────────────────────────────────────────────────────────────────────
    # TOOL 2: Get ticket history for a customer org
    # ─────────────────────────────────────────────────────────────────────────
    {
        "name": "get_ticket_history",
        "description": """Retrieve recent support tickets from a customer organisation.
        Use this to check if the customer has reported this issue before, and if so,
        how it was resolved previously. Very useful for recurring issues.
        Returns ticket titles, priorities, statuses, and resolution notes.""",
        "input_schema": {
            "type": "object",
            "properties": {
                "org_id": {
                    "type": "string",
                    "description": "The customer organisation ID from the current ticket"
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of recent tickets to retrieve. Default 10.",
                    "default": 10
                },
                "status": {
                    "type": "string",
                    # enum: Claude can ONLY choose from these exact values
                    "enum": ["open", "closed", "all"],
                    "description": "Filter by ticket status. Use 'all' to see everything.",
                    "default": "all"
                }
            },
            "required": ["org_id"]
        }
    },

    # ─────────────────────────────────────────────────────────────────────────
    # TOOL 3: Find similar resolved tickets using AI
    # ─────────────────────────────────────────────────────────────────────────
    {
        "name": "find_similar_resolved_tickets",
        "description": """Use AI (vector similarity) to find past tickets similar to
        the current one that have already been resolved. Returns how those tickets
        were fixed, which helps the agent recommend proven solutions.
        Use after searching the KB if no documented solution was found.""",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticket_id": {
                    "type": "string",
                    "description": "ID of the current ticket being investigated"
                },
                "limit": {
                    "type": "integer",
                    "description": "Max similar tickets to return. Default 5.",
                    "default": 5
                }
            },
            "required": ["ticket_id"]
        }
    },

    # ─────────────────────────────────────────────────────────────────────────
    # TOOL 4: Check SLA status
    # ─────────────────────────────────────────────────────────────────────────
    {
        "name": "get_sla_status",
        "description": """Check the SLA deadlines and urgency of a ticket.
        Returns response deadline, resolution deadline, percentage of time elapsed,
        and whether SLA has already been breached. Use this to understand urgency
        and whether the investigation needs to be expedited.""",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticket_id": {
                    "type": "string",
                    "description": "ID of the ticket to check SLA for"
                }
            },
            "required": ["ticket_id"]
        }
    },

    # ─────────────────────────────────────────────────────────────────────────
    # TOOL 5: Draft the final response (last step only)
    # ─────────────────────────────────────────────────────────────────────────
    {
        "name": "draft_resolution_report",
        "description": """Create the final resolution report and suggested customer reply.
        Use this ONLY as the LAST step, after you have gathered all information
        from the other tools. Do not call this tool first.
        The report will be shown to the 3SC agent for review before sending.""",
        "input_schema": {
            "type": "object",
            "properties": {
                "root_cause": {
                    "type": "string",
                    "description": "What is causing the issue, based on investigation"
                },
                "recommended_fix": {
                    "type": "string",
                    "description": "Step-by-step fix the agent should apply"
                },
                "customer_reply": {
                    "type": "string",
                    "description": "Draft message to send to the customer"
                },
                "estimated_resolution_hours": {
                    "type": "number",
                    "description": "Estimated hours to fully resolve this issue"
                },
                "escalate": {
                    "type": "boolean",
                    "description": "True if this ticket needs to be escalated to a lead"
                }
            },
            "required": ["root_cause", "recommended_fix", "customer_reply"]
        }
    }
]


# =============================================================================
# STAGE 3 — TOOL EXECUTION
# =============================================================================
# Claude decides WHICH tool to call and with WHAT parameters.
# YOUR code actually RUNS the tool and returns the result.
#
# These are real Python functions that query your database,
# search your KB, call your APIs, etc.
#
# Claude never sees this Python code — it only sees the JSON result.
# =============================================================================

def tool_search_knowledge_base(query: str, max_results: int = 5) -> dict:
    """
    Real implementation of the search_knowledge_base tool.

    In your portal this would query the KBArticle table.
    We return a dict — it gets JSON-serialized and sent back to Claude.

    The structure of the return value matters:
    - Use clear key names so Claude understands the result
    - Include a "found" count so Claude knows if results exist
    - Keep individual results short — you're feeding this to Claude's context
    """
    # In your actual portal, this would be:
    # from app.models import KBArticle
    # articles = KBArticle.query.filter(
    #     KBArticle.content.ilike(f"%{query}%")
    # ).limit(max_results).all()
    # return {
    #     "found": len(articles),
    #     "articles": [{"id": a.id, "title": a.title, "summary": a.summary[:300]} for a in articles]
    # }

    # Simulated result for this learning file:
    logger.info(f"[TOOL] search_knowledge_base(query='{query}', max_results={max_results})")
    return {
        "found": 2,
        "articles": [
            {
                "id":      "KB-0142",
                "title":   "Login failures after deployment — auth token bug",
                "summary": "After deploy v2.4+, auth tokens may expire prematurely in EU region due to timezone offset in token validation. Fix: apply hotfix AUTH-2024-03 or roll back to v2.3.",
                "url":     "/kb/0142"
            },
            {
                "id":      "KB-0098",
                "title":   "Handling 401 errors for EU users",
                "summary": "EU-specific 401 errors are often caused by GDPR-compliant session handling. Check that session_timeout_eu is set correctly in deployment config.",
                "url":     "/kb/0098"
            }
        ]
    }


def tool_get_ticket_history(org_id: str, limit: int = 10, status: str = "all") -> dict:
    """
    Real implementation: queries your Ticket table filtered by org_id.

    In your portal:
    from app.models import Ticket
    q = Ticket.query.filter_by(org_id=org_id)
    if status != "all":
        q = q.filter_by(status=status)
    tickets = q.order_by(Ticket.created_at.desc()).limit(limit).all()
    return {
        "org_id": org_id,
        "total":  len(tickets),
        "tickets": [{"title":t.title, "status":t.status, "priority":t.priority,
                     "created_at":t.created_at.isoformat()} for t in tickets]
    }
    """
    logger.info(f"[TOOL] get_ticket_history(org_id='{org_id}', limit={limit}, status='{status}')")
    return {
        "org_id":       org_id,
        "total":        3,
        "tickets": [
            {"title": "Login broken for EU users",          "status": "closed", "priority": "P1", "created_at": "2024-12-15", "resolved_by": "Deploy rollback to v2.3"},
            {"title": "Auth tokens expiring too quickly",   "status": "closed", "priority": "P2", "created_at": "2024-11-02", "resolved_by": "Applied AUTH-2024-02 hotfix"},
            {"title": "New users cannot log in",            "status": "closed", "priority": "P2", "created_at": "2024-09-18", "resolved_by": "Fixed session config for EU region"},
        ]
    }


def tool_find_similar_resolved_tickets(ticket_id: str, limit: int = 5) -> dict:
    """
    Real implementation: uses your pgvector embeddings to find similar tickets.

    In your portal:
    from app.services.embeddings import search_similar_tickets
    results = search_similar_tickets(ticket_id=ticket_id, status_filter="closed", limit=limit)
    return {"ticket_id": ticket_id, "similar_count": len(results), "tickets": results}
    """
    logger.info(f"[TOOL] find_similar_resolved_tickets(ticket_id='{ticket_id}', limit={limit})")
    return {
        "ticket_id":     ticket_id,
        "similar_count": 2,
        "tickets": [
            {
                "title":           "EU login failure post-deploy 2024-12",
                "similarity":      0.94,  # 0 to 1, higher = more similar
                "resolution_note": "Rolled back to v2.3. Root cause was auth token timezone bug in v2.4.",
                "resolved_in_hours": 1.5
            },
            {
                "title":           "Production login broken after maintenance",
                "similarity":      0.81,
                "resolution_note": "Applied session config fix for GDPR timeout settings.",
                "resolved_in_hours": 3.0
            }
        ]
    }


def tool_get_sla_status(ticket_id: str) -> dict:
    """
    Real implementation: reads from your Ticket model's SLA fields.

    In your portal:
    from app.models import Ticket
    ticket = Ticket.query.get(ticket_id)
    return {
        "ticket_id":              ticket_id,
        "priority":               ticket.priority,
        "response_due_at":        ticket.sla_response_due_at.isoformat() if ticket.sla_response_due_at else None,
        "resolution_due_at":      ticket.sla_resolution_due_at.isoformat() if ticket.sla_resolution_due_at else None,
        "response_elapsed_pct":   ticket.sla_response_elapsed_pct,
        "resolution_elapsed_pct": ticket.sla_resolution_elapsed_pct,
        "response_breached":      ticket.sla_response_breached,
        "resolution_breached":    ticket.sla_resolution_breached,
    }
    """
    logger.info(f"[TOOL] get_sla_status(ticket_id='{ticket_id}')")
    return {
        "ticket_id":              ticket_id,
        "priority":               "P1",
        "response_due_at":        "2026-03-30T15:00:00Z",  # 1h from create
        "resolution_due_at":      "2026-03-30T18:00:00Z",  # 4h from create
        "response_elapsed_pct":   22.5,   # 22.5% of response window used
        "resolution_elapsed_pct": 22.5,   # same since P1 clocks run together
        "response_breached":      False,  # still within deadline
        "resolution_breached":    False,
        "minutes_until_breach":   185     # 3 hours 5 minutes until resolution due
    }


def tool_draft_resolution_report(
    root_cause:    str,
    recommended_fix: str,
    customer_reply:  str,
    estimated_resolution_hours: float = 2.0,
    escalate: bool = False
) -> dict:
    """
    This tool doesn't query anything — it just receives Claude's structured output
    and packages it for your portal to display to the agent.

    We return it as a dict so it gets saved to AgentDecision.ai_value in your portal.
    """
    logger.info(f"[TOOL] draft_resolution_report — escalate={escalate}")
    return {
        "root_cause":              root_cause,
        "recommended_fix":         recommended_fix,
        "customer_reply":          customer_reply,
        "estimated_resolution_hours": estimated_resolution_hours,
        "escalate":                escalate,
        "status":                  "draft",    # agent drafted, human must approve
        "created_at":              datetime.now(timezone.utc).isoformat()
    }


# ─────────────────────────────────────────────────────────────────────────────
# TOOL ROUTER
# Maps tool names (strings) → Python functions
# This is the switch statement that execute_tool() uses.
# ─────────────────────────────────────────────────────────────────────────────
TOOL_FUNCTION_MAP = {
    "search_knowledge_base":         tool_search_knowledge_base,
    "get_ticket_history":            tool_get_ticket_history,
    "find_similar_resolved_tickets": tool_find_similar_resolved_tickets,
    "get_sla_status":                tool_get_sla_status,
    "draft_resolution_report":       tool_draft_resolution_report,
}


def execute_tool(tool_name: str, tool_inputs: dict) -> dict:
    """
    Execute a tool by name with the inputs Claude provided.

    This is the bridge between Claude's decision and your Python code.
    Claude says "call search_knowledge_base with {'query': 'login 401'}".
    This function looks up 'search_knowledge_base' in TOOL_FUNCTION_MAP
    and calls tool_search_knowledge_base(query='login 401').

    Args:
        tool_name:   The name Claude used (matches the "name" in tool definition)
        tool_inputs: The dict of parameters Claude chose

    Returns:
        A dict that gets JSON-serialized and sent back to Claude as the tool result.
        Always return a dict — never raise an exception out of this function.
        If something goes wrong, return {"error": "description"} so Claude
        can adapt its plan instead of the whole agent crashing.
    """
    logger.info(f"Executing tool: {tool_name} with inputs: {tool_inputs}")
    start_time = time.time()

    # Look up the function for this tool name
    func = TOOL_FUNCTION_MAP.get(tool_name)

    if func is None:
        # Tool name doesn't exist in our map
        # This shouldn't happen if tool definitions match function names
        logger.error(f"Unknown tool requested: {tool_name}")
        return {"error": f"Tool '{tool_name}' is not registered"}

    try:
        # Call the Python function with the parameters Claude chose
        # ** unpacks the dict as keyword arguments
        # e.g. func(**{"query": "login"}) → func(query="login")
        result = func(**tool_inputs)

        elapsed = round((time.time() - start_time) * 1000)  # milliseconds
        logger.info(f"Tool {tool_name} completed in {elapsed}ms")

        return result

    except TypeError as e:
        # Claude passed wrong parameters (wrong type or missing required arg)
        logger.error(f"Tool {tool_name} received wrong parameters: {e}")
        return {"error": f"Invalid parameters for {tool_name}: {str(e)}"}

    except Exception as e:
        # Anything else went wrong (DB error, network error, etc.)
        # Return error dict so Claude can work around it, don't crash the agent
        logger.error(f"Tool {tool_name} failed: {e}", exc_info=True)
        return {"error": f"Tool execution failed: {str(e)}"}


# =============================================================================
# STAGE 4 — THE REACT LOOP (THE AGENT ENGINE)
# =============================================================================
# This is the most important function in this entire file.
# Read every line. Understand it completely.
#
# ReAct = Reasoning + Acting
# The agent alternates between:
#   THINKING (Claude reads the conversation and decides what to do)
#   ACTING   (Claude calls a tool, your code runs it, result goes back)
# ...until Claude has enough information to give a final answer.
#
# The messages list is the agent's notebook:
#   - You add the initial task
#   - Claude's tool requests get appended
#   - Tool results get appended
#   - Claude reads EVERYTHING on each turn to decide the next step
# =============================================================================

def run_agent(
    system_prompt: str,         # Who Claude is + rules + strategy
    user_message:  str,         # The task to complete
    tools:         list,        # List of tool definitions Claude can use
    max_turns:     int = 10,    # Safety limit — prevents infinite loops
    verbose:       bool = True  # Print each turn for debugging
) -> str:
    """
    Run a ReAct agent loop until Claude gives a final answer.

    This function:
    1. Sends the task to Claude
    2. If Claude calls a tool → executes it, appends result, loops
    3. If Claude is done → returns the final text

    Returns: Claude's final text answer as a string.
    """

    # ─────────────────────────────────────────────────────────────────────────
    # INITIALISE
    # ─────────────────────────────────────────────────────────────────────────

    # The messages list is everything Claude has seen so far.
    # We start with just the user's task.
    # This list grows on every turn as Claude thinks and acts.
    messages = [
        {
            "role":    "user",
            "content": user_message
        }
    ]

    # Track which tools were called and results, for logging/debugging
    agent_run_log = []

    # ─────────────────────────────────────────────────────────────────────────
    # THE LOOP
    # ─────────────────────────────────────────────────────────────────────────

    for turn_number in range(max_turns):

        if verbose:
            logger.info(f"--- Agent Turn {turn_number + 1}/{max_turns} ---")
            logger.info(f"Messages in context: {len(messages)}")

        # ── CALL CLAUDE ───────────────────────────────────────────────────────
        # We send the ENTIRE messages list every time.
        # Claude reads everything from the beginning each turn.
        # This is expensive (more tokens) but necessary — Claude has no memory.
        response = client.messages.create(
            model      = MODEL,
            system     = system_prompt,
            messages   = messages,    # entire conversation history so far
            tools      = tools,       # all tools Claude can call
            max_tokens = 4096,        # generous limit for complex reasoning
        )

        if verbose:
            logger.info(f"Turn {turn_number + 1} — stop_reason: {response.stop_reason}")
            logger.info(f"Turn {turn_number + 1} — tokens used: {response.usage.input_tokens} in / {response.usage.output_tokens} out")

        # ── DECISION POINT: What did Claude decide to do? ─────────────────────

        # ────────────────────────────────────────────────────────────────────
        # CASE 1: CLAUDE IS DONE
        # stop_reason == "end_turn" means Claude has enough information
        # and is giving us the final answer. Extract and return the text.
        # ────────────────────────────────────────────────────────────────────
        if response.stop_reason == "end_turn":
            # response.content is a list — extract the text block
            final_text = ""
            for block in response.content:
                if hasattr(block, "text"):  # TextBlock has .text attribute
                    final_text += block.text

            if verbose:
                logger.info(f"Agent finished after {turn_number + 1} turns")
                logger.info(f"Agent used tools: {[log['tool'] for log in agent_run_log]}")

            return final_text  # ← THIS IS THE AGENT'S FINAL ANSWER

        # ────────────────────────────────────────────────────────────────────
        # CASE 2: CLAUDE WANTS TO CALL A TOOL
        # stop_reason == "tool_use" means Claude is NOT done.
        # It needs more information before it can answer.
        # We need to:
        #   A) Append Claude's response to messages (so Claude remembers it asked)
        #   B) Execute the tool(s) Claude requested
        #   C) Append the tool results to messages (so Claude sees the results)
        #   D) Loop back to the top (Claude reads results and decides next step)
        # ────────────────────────────────────────────────────────────────────
        if response.stop_reason == "tool_use":

            # ── STEP A: Add Claude's response to message history ──────────────
            # Claude's response contains the tool_use blocks (its requests).
            # We MUST add this to messages before adding tool results.
            # If we skip this, Claude won't know it made the request.
            messages.append({
                "role":    "assistant",       # this is Claude speaking
                "content": response.content   # the list of blocks (may include text + tool_use)
            })

            # ── STEP B: Find all tool_use blocks and execute each one ─────────
            # Claude can request MULTIPLE tools in a single response.
            # We collect all results and send them back in one batch.
            tool_results = []

            for block in response.content:
                # block.type == "tool_use" means this is a tool request
                if block.type == "tool_use":
                    if verbose:
                        logger.info(f"Claude calls: {block.name}({block.input})")

                    # block.name  = tool name (e.g. "search_knowledge_base")
                    # block.input = dict of parameters Claude chose (e.g. {"query": "login 401"})
                    # block.id    = unique ID for THIS specific tool call

                    # YOUR CODE runs the actual function
                    tool_result = execute_tool(block.name, block.input)

                    if verbose:
                        logger.info(f"Tool result preview: {str(tool_result)[:200]}...")

                    # Log for debugging
                    agent_run_log.append({
                        "turn":   turn_number + 1,
                        "tool":   block.name,
                        "inputs": block.input,
                        "result": tool_result
                    })

                    # Package the result in the exact format Claude expects
                    tool_results.append({
                        "type":        "tool_result",    # tells Claude this is a result
                        "tool_use_id": block.id,         # MUST match block.id above
                        "content":     json.dumps(tool_result)  # JSON string of your function's output
                    })

            # ── STEP C: Add tool results to message history ───────────────────
            # This is the crucial step — Claude reads these results in the next turn.
            # Without this, Claude would ask for the same tool again (it doesn't know the result).
            messages.append({
                "role":    "user",          # tool results come from "user" role
                "content": tool_results     # list of tool result dicts
            })

            # ── STEP D: Loop continues ─────────────────────────────────────────
            # The for loop goes back to the top.
            # Claude is called again with the FULL messages list.
            # It reads its previous tool requests + results and decides:
            #   - Call more tools (if it needs more information)
            #   - Give the final answer (if it has enough)
            continue

        # ────────────────────────────────────────────────────────────────────
        # CASE 3: UNEXPECTED STOP REASON
        # max_tokens = Claude ran out of token budget (increase max_tokens)
        # stop_sequence = Claude hit a stop sequence (unusual for agents)
        # ────────────────────────────────────────────────────────────────────
        logger.warning(f"Unexpected stop_reason: {response.stop_reason}")
        return f"Agent stopped unexpectedly: {response.stop_reason}"

    # ─────────────────────────────────────────────────────────────────────────
    # MAX TURNS REACHED
    # The loop ran max_turns times without Claude finishing.
    # This is a safety net. If you hit this:
    #   1. Increase max_turns (default 10 is usually enough)
    #   2. Check if the system prompt is causing Claude to loop
    #   3. Check if tool results are incomplete (Claude keeps asking same tool)
    # ─────────────────────────────────────────────────────────────────────────
    logger.error(f"Agent reached max_turns ({max_turns}) without finishing")
    return f"ERROR: Agent could not complete in {max_turns} turns. Review agent logs."


# =============================================================================
# STAGE 5 — SYSTEM PROMPTS
# =============================================================================
# The system prompt is your agent's job description, rules, and strategy.
# It runs BEFORE every tool call. Claude reads it on every turn.
#
# A good system prompt answers:
#   WHO:    What is the agent's role/persona?
#   WHAT:   What is the agent trying to accomplish?
#   HOW:    What strategy should it follow? (Which tools first? In what order?)
#   FORMAT: What should the final output look like?
# =============================================================================

TICKET_INVESTIGATION_SYSTEM_PROMPT = """
You are a senior support lead at 3SC, a software delivery company.
Your role is to investigate customer support tickets and produce clear,
actionable resolution recommendations for the frontline support agents.

INVESTIGATION STRATEGY (follow this order):
1. ALWAYS search the knowledge base first with keywords from the ticket.
   Many issues have documented solutions. Find them before doing anything else.
2. Check the customer's ticket history to see if they reported this before
   and how it was resolved.
3. Find similar resolved tickets using AI similarity search to discover
   resolution patterns across all customers.
4. Check SLA status to understand urgency — a P1 with 20 minutes left is
   different from a P4 with 6 days remaining.
5. ONLY AFTER steps 1-4: use draft_resolution_report to produce the final output.
   Never call this first.

IMPORTANT RULES:
- Be efficient: gather all information before drafting a response
- Be specific: "apply hotfix AUTH-2024-03" is better than "fix the bug"
- Be honest: if you cannot find a solution, say so clearly
- Set escalate=True if the issue cannot be resolved at agent level

FINAL REPORT FORMAT:
Your resolution report must include:
- Root cause (what is actually broken)
- Recommended fix (step-by-step, specific actions)
- Customer reply draft (professional, empathetic, specific)
- Estimated resolution time
- Whether to escalate to a lead

Be concise. Agents are busy. Give them what they need to act immediately.
"""

CUSTOMER_HEALTH_SYSTEM_PROMPT = """
You are a Customer Success analyst at 3SC.
Your role is to proactively identify patterns in customer ticket data
and produce health reports that help the team prevent recurring issues.

You have access to tools to analyse ticket patterns.
Look for:
- Recurring issue types (same category multiple times)
- Escalating frequency (more tickets this month than last)
- SLA compliance trends
- Resolution time trends

Your output should be a health report with:
- Executive summary (2-3 sentences)
- Identified patterns (each with evidence — ticket counts, dates)
- Risk flags (issues that might escalate if not addressed)
- Recommended actions for the 3SC team
"""


# =============================================================================
# STAGE 6 — MULTI-AGENT (SUPERVISOR + WORKERS)
# =============================================================================
# Multi-agent = multiple Claude instances working together.
# WHY: Parallelism (workers run simultaneously) and specialisation
# (each worker has a focused, smaller context).
#
# PATTERN:
#   Supervisor → delegates subtasks to Workers
#   Workers    → each does one focused job
#   Supervisor → synthesises all worker results into final answer
#
# HOW: Each worker is just run_agent() with a narrow system prompt
# and limited tools. The supervisor gets the collected worker outputs
# and synthesises them.
# =============================================================================

def run_kb_worker(query: str) -> str:
    """
    Specialised worker: only searches the knowledge base.
    Has ONE tool. Does ONE job. Returns a focused result.

    Why a worker instead of just calling the tool directly?
    Because Claude can INTERPRET the results and decide which
    articles are most relevant, rather than returning raw search output.
    """
    return run_agent(
        system_prompt = """You are a knowledge base specialist.
        Search the KB and return the 3 most relevant articles for this query.
        Summarise each article in 2 sentences. Be concise.""",

        user_message  = f"Find KB articles relevant to: {query}",

        # Only ONE tool available — this worker can only search KB
        tools         = [TICKET_INVESTIGATION_TOOLS[0]],  # search_knowledge_base only

        max_turns     = 3,   # KB worker shouldn't need many turns
        verbose       = False  # suppress logging for sub-agents
    )


def run_history_worker(org_id: str, ticket_title: str) -> str:
    """
    Specialised worker: only checks ticket history for an org.
    """
    return run_agent(
        system_prompt = """You are a ticket history analyst.
        Look up this customer's recent tickets. Identify any patterns,
        recurring issues, or relevant past resolutions. Be concise.""",

        user_message  = f"Check history for org '{org_id}' for ticket: '{ticket_title}'",

        tools         = [TICKET_INVESTIGATION_TOOLS[1]],  # get_ticket_history only

        max_turns     = 3,
        verbose       = False
    )


def run_sla_worker(ticket_id: str) -> str:
    """
    Specialised worker: only checks SLA status.
    """
    return run_agent(
        system_prompt = """You are an SLA analyst.
        Check the SLA status for this ticket. Report deadline, elapsed percentage,
        and urgency level. Recommend if escalation is needed based on SLA risk.""",

        user_message  = f"Check SLA status for ticket: {ticket_id}",

        tools         = [TICKET_INVESTIGATION_TOOLS[3]],  # get_sla_status only

        max_turns     = 2,
        verbose       = False
    )


def run_supervisor(
    ticket_id:    str,
    ticket_title: str,
    kb_findings:  str,
    history_findings: str,
    sla_findings: str
) -> str:
    """
    Supervisor agent: receives all worker results and synthesises the final answer.

    The supervisor doesn't have tools — it doesn't need to call anything.
    Its job is to READ the worker outputs and PRODUCE the final report.
    We inject the worker results directly into the user_message.

    This is the synthesis step. Claude reads the pre-gathered research
    and writes the resolution recommendation.
    """
    # Build the context message with all worker results
    # We inject worker outputs directly into the message (not as tool results)
    synthesis_message = f"""
    Ticket ID:    {ticket_id}
    Ticket title: {ticket_title}

    RESEARCH GATHERED BY SPECIALIST AGENTS:

    === KNOWLEDGE BASE FINDINGS ===
    {kb_findings}

    === CUSTOMER HISTORY FINDINGS ===
    {history_findings}

    === SLA STATUS ===
    {sla_findings}

    Based on the above research, produce a complete resolution report.
    Use the draft_resolution_report tool to structure your output.
    """

    return run_agent(
        system_prompt = """You are a senior support lead synthesising research
        to produce a resolution report. All research has already been gathered.
        Your job is to interpret the findings and write clear recommendations.
        Use the draft_resolution_report tool to structure your output.""",

        user_message  = synthesis_message,

        # Supervisor only needs the draft tool — it's not gathering more info
        tools         = [TICKET_INVESTIGATION_TOOLS[4]],  # draft_resolution_report only

        max_turns     = 3,
        verbose       = True
    )


def run_multi_agent_investigation(
    ticket_id:    str,
    ticket_title: str,
    org_id:       str
) -> str:
    """
    Full multi-agent investigation flow.

    In a real portal, workers would run in parallel using Celery:
        from celery import group, chord
        workers = group([
            kb_worker_task.s(ticket_title),
            history_worker_task.s(org_id, ticket_title),
            sla_worker_task.s(ticket_id),
        ])
        pipeline = chord(workers)(supervisor_task.s(ticket_id, ticket_title))
        pipeline.delay()

    In this learning file we run them sequentially for clarity.
    """
    logger.info("=== STAGE 6: Multi-agent investigation ===")

    # Step 1: Run all workers (in real code these would run in parallel)
    logger.info("Running KB worker...")
    kb_result       = run_kb_worker(query=ticket_title)

    logger.info("Running history worker...")
    history_result  = run_history_worker(org_id=org_id, ticket_title=ticket_title)

    logger.info("Running SLA worker...")
    sla_result      = run_sla_worker(ticket_id=ticket_id)

    # Step 2: Supervisor synthesises all results
    logger.info("Running supervisor...")
    final_report    = run_supervisor(
        ticket_id         = ticket_id,
        ticket_title      = ticket_title,
        kb_findings       = kb_result,
        history_findings  = history_result,
        sla_findings      = sla_result
    )

    return final_report


# =============================================================================
# STAGE 7 — MEMORY (STATEFUL AGENTS)
# =============================================================================
# Claude has NO memory between API calls.
# For an agent to remember past interactions, you must:
#   1. WRITE memories to your database after each run
#   2. READ memories from your database at the start of each run
#   3. INJECT memories into the system prompt so Claude sees them
#
# Three types of memory (store all three for best results):
#   episodic  = specific past events ("Acme had login bug in Dec 2024")
#   fact      = stable truths ("Acme is on Premium SLA")
#   pattern   = recurring observations ("Billing issues appear at month end")
# =============================================================================

class SimpleMemoryStore:
    """
    A simple in-memory store for learning purposes.

    In your real portal, replace this with your AgentMemory SQLAlchemy model:
        class AgentMemory(db.Model):
            __tablename__ = "agent_memory"
            id           = db.Column(db.String(36), primary_key=True)
            agent_type   = db.Column(db.String(50))
            scope_id     = db.Column(db.String(36))  # org_id
            memory_type  = db.Column(db.String(20))  # episodic | fact | pattern
            content      = db.Column(db.Text)
            importance   = db.Column(db.Float)
            created_at   = db.Column(db.DateTime)
    """
    def __init__(self):
        # Simple dict: {scope_id: [memory_dict, ...]}
        self._store = {}

    def save(self, scope_id: str, memory_type: str, content: str, importance: float = 0.7):
        """Save a memory for a scope (org_id)."""
        if scope_id not in self._store:
            self._store[scope_id] = []
        self._store[scope_id].append({
            "type":       memory_type,
            "content":    content,
            "importance": importance,
            "created_at": datetime.now(timezone.utc).isoformat()
        })
        logger.info(f"[MEMORY] Saved {memory_type} for scope {scope_id}: {content[:80]}...")

    def load(self, scope_id: str, limit: int = 10) -> list:
        """Load memories for a scope, sorted by importance."""
        memories = self._store.get(scope_id, [])
        # Sort by importance descending (most important first)
        return sorted(memories, key=lambda m: -m["importance"])[:limit]


# Global memory store (in real code, this is your database)
memory_store = SimpleMemoryStore()


def extract_memories_from_result(result_text: str, scope_id: str) -> list:
    """
    After each agent run, extract key facts worth remembering.

    We use a second, cheap Claude call to do this.
    This call is focused: "what from this report is worth remembering long-term?"

    Returns a list of memory dicts to store.
    """
    # Use a smaller model for this extraction call — it's a simple task
    response = client.messages.create(
        model      = MODEL,
        max_tokens = 500,
        system     = """Extract key facts from this agent report that should be
        remembered for future interactions with this customer.

        Return ONLY valid JSON — no other text:
        [
          {"type": "fact|pattern|episodic", "content": "...", "importance": 0.0-1.0},
          ...
        ]

        Guidelines:
        - fact:     Stable truths ("Customer is on Premium SLA")
        - pattern:  Recurring observations ("Billing issues appear at month end")
        - episodic: Specific past events ("Resolved login bug March 2026 by rolling back to v2.3")
        - importance: 1.0 = very important, 0.5 = useful, 0.1 = minor detail
        - Only extract facts that will be USEFUL in future conversations
        - Skip details that won't matter next week
        - Maximum 5 memories per report""",

        messages = [
            {
                "role":    "user",
                "content": f"Extract memorable facts from this report:\n\n{result_text}"
            }
        ]
    )

    raw = response.content[0].text
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Memory extraction returned invalid JSON")
        return []  # Return empty list — memory extraction failure is non-fatal


def run_stateful_agent(org_id: str, task: str) -> str:
    """
    Run an agent that remembers past interactions with this org.

    Flow:
    1. Load memories for this org from DB
    2. Inject memories into system prompt
    3. Run the agent (same run_agent function from Stage 4)
    4. Extract key memories from the result
    5. Save new memories to DB

    The agent "gets smarter" about this specific customer over time.
    """
    logger.info(f"=== STAGE 7: Stateful agent for org={org_id} ===")

    # ── Step 1: Load memories ─────────────────────────────────────────────────
    memories = memory_store.load(scope_id=org_id, limit=10)

    # ── Step 2: Build memory context block ────────────────────────────────────
    # Format memories as readable text to inject into the system prompt
    memory_block = ""
    if memories:
        memory_block = "\n\nWHAT YOU REMEMBER ABOUT THIS CUSTOMER:\n"
        for mem in memories:
            # e.g. "- [pattern] Billing issues appear at month end (importance: 0.9)"
            memory_block += f"- [{mem['type']}] {mem['content']}\n"
        memory_block += "\nUse this memory to personalise your response and spot patterns."

    # ── Step 3: Build system prompt with memories injected ────────────────────
    system_with_memory = f"""
    You are a Customer Success agent for 3SC.
    You help with support tickets and proactively identify customer health issues.
    {memory_block}
    Use your memory to spot recurring patterns, reference past resolutions,
    and provide personalised, contextual responses.
    """

    # ── Step 4: Run the agent (same engine, now with memory context) ──────────
    result = run_agent(
        system_prompt = system_with_memory,
        user_message  = task,
        tools         = TICKET_INVESTIGATION_TOOLS,
        max_turns     = 8
    )

    # ── Step 5: Extract and save new memories ──────────────────────────────────
    logger.info("Extracting memories from this run...")
    new_memories = extract_memories_from_result(result, org_id)

    for mem in new_memories:
        memory_store.save(
            scope_id   = org_id,
            memory_type = mem.get("type", "episodic"),
            content    = mem.get("content", ""),
            importance = mem.get("importance", 0.5)
        )

    logger.info(f"Saved {len(new_memories)} new memories for org {org_id}")

    return result


# =============================================================================
# STAGE 8 — HUMAN IN THE LOOP
# =============================================================================
# Every tool that changes state (write tools) needs human approval.
# The agent produces a PLAN first, a human reviews and approves it,
# THEN the agent executes the approved plan.
#
# Read tools   → run automatically (search KB, get history, get SLA)
# Write tools  → require approval  (assign ticket, post comment, change status)
#
# This is not optional. Without it, agents can make mistakes
# that you can't undo (sending wrong email, closing wrong ticket).
# =============================================================================

class ActionPlan:
    """
    Stores a proposed action plan before human approval.

    In your real portal this is a SQLAlchemy model:
        class AgentPlan(db.Model):
            __tablename__ = "agent_plans"
            id          = db.Column(db.String(36), primary_key=True)
            ticket_id   = db.Column(db.String(36))
            actions     = db.Column(db.JSON)  # list of proposed actions
            status      = db.Column(db.String(20))  # pending|approved|rejected|executed
            approved_by = db.Column(db.String(36))  # user_id of approver
            created_at  = db.Column(db.DateTime)
    """
    def __init__(self, ticket_id: str, actions: list):
        import uuid
        self.id         = str(uuid.uuid4())
        self.ticket_id  = ticket_id
        self.actions    = actions           # list of {"action", "params", "reason"}
        self.status     = "pending"         # waiting for human approval
        self.approved_by = None
        self.created_at = datetime.now(timezone.utc)

    def to_dict(self):
        return {
            "id":          self.id,
            "ticket_id":   self.ticket_id,
            "actions":     self.actions,
            "status":      self.status,
            "approved_by": self.approved_by,
            "created_at":  self.created_at.isoformat()
        }


# Simple in-memory plan store (in real code, use DB + Redis)
pending_plans = {}


def create_action_plan(ticket_id: str, ticket_title: str, org_id: str) -> ActionPlan:
    """
    Phase 1 of human-in-the-loop:
    Run the INVESTIGATION phase (read-only tools only).
    Ask Claude to produce a plan of PROPOSED actions.
    Save the plan to DB. Return plan ID to the caller.

    Claude does NOT execute write actions in this phase.
    It only proposes what it WOULD do.
    """
    logger.info(f"=== STAGE 8: Creating action plan for ticket {ticket_id} ===")

    # READ-ONLY tools — safe to run automatically during planning
    # We do NOT include assign_ticket, post_comment, change_status here
    read_only_tools = [
        TICKET_INVESTIGATION_TOOLS[0],  # search_knowledge_base
        TICKET_INVESTIGATION_TOOLS[1],  # get_ticket_history
        TICKET_INVESTIGATION_TOOLS[2],  # find_similar_resolved_tickets
        TICKET_INVESTIGATION_TOOLS[3],  # get_sla_status
    ]

    planning_system = """
    You are a support lead creating an action plan for a ticket.
    IMPORTANT: DO NOT execute any write actions. This is a PLANNING phase only.

    Investigate the ticket using the read tools, then propose a plan.
    Your final response must be a JSON object in this exact format:
    {
        "summary": "What you found during investigation",
        "proposed_actions": [
            {
                "action":  "assign_ticket",
                "params":  {"agent_id": "agent-uuid-here", "reason": "expertise in auth issues"},
                "reason":  "Why this action is needed"
            },
            {
                "action":  "post_comment",
                "params":  {"body": "Draft reply text here", "is_internal": false},
                "reason":  "Customer needs immediate acknowledgement"
            }
        ]
    }

    Possible actions: assign_ticket, post_comment, change_status, escalate
    """

    result = run_agent(
        system_prompt = planning_system,
        user_message  = f"Investigate and create an action plan for ticket {ticket_id}: '{ticket_title}' (org: {org_id})",
        tools         = read_only_tools,
        max_turns     = 6
    )

    # Parse Claude's proposed plan from the JSON response
    try:
        plan_data = json.loads(result)
        proposed_actions = plan_data.get("proposed_actions", [])
    except json.JSONDecodeError:
        # Claude didn't return clean JSON — extract what we can
        logger.warning("Planning response was not clean JSON, using empty actions")
        proposed_actions = []

    # Create and store the plan (pending approval)
    plan = ActionPlan(ticket_id=ticket_id, actions=proposed_actions)
    pending_plans[plan.id] = plan

    logger.info(f"Created plan {plan.id} with {len(proposed_actions)} proposed actions")
    logger.info(f"Plan actions: {[a['action'] for a in proposed_actions]}")

    return plan


def approve_and_execute_plan(plan_id: str, approved_by_user_id: str) -> dict:
    """
    Phase 2 of human-in-the-loop:
    Human reviewed the plan and clicked Approve.
    NOW we execute the write actions.

    In your real portal, this is called from the Flask route:
        POST /api/agent/plans/<plan_id>/approve
    """
    logger.info(f"=== STAGE 8: Executing approved plan {plan_id} ===")

    plan = pending_plans.get(plan_id)
    if not plan:
        return {"error": f"Plan {plan_id} not found"}

    if plan.status != "pending":
        return {"error": f"Plan already has status '{plan.status}'"}

    # Mark as approved
    plan.status     = "executing"
    plan.approved_by = approved_by_user_id

    # WRITE ACTIONS — these change real state
    # In your portal these would call real DB functions
    WRITE_ACTION_MAP = {
        "assign_ticket": lambda params: logger.info(f"WRITE: Assigning ticket to {params.get('agent_id')}"),
        "post_comment":  lambda params: logger.info(f"WRITE: Posting comment: {params.get('body', '')[:100]}..."),
        "change_status": lambda params: logger.info(f"WRITE: Changing status to {params.get('status')}"),
        "escalate":      lambda params: logger.info(f"WRITE: Escalating to lead {params.get('lead_id')}"),
    }

    results = []
    for action in plan.actions:
        action_name = action["action"]
        action_params = action.get("params", {})

        logger.info(f"Executing: {action_name}({action_params})")

        execute_fn = WRITE_ACTION_MAP.get(action_name)
        if execute_fn:
            try:
                execute_fn(action_params)  # In real code: calls DB function
                results.append({"action": action_name, "status": "ok"})
            except Exception as e:
                results.append({"action": action_name, "status": "failed", "error": str(e)})
                plan.status = "failed"
                return {"plan_id": plan_id, "status": "failed", "results": results}
        else:
            results.append({"action": action_name, "status": "unknown_action"})

    plan.status = "completed"
    logger.info(f"Plan {plan_id} executed successfully")
    return {"plan_id": plan_id, "status": "completed", "results": results}


def reject_plan(plan_id: str, reason: str) -> dict:
    """
    Human reviewed the plan and clicked Reject.
    Plan is cancelled, no write actions are executed.
    """
    plan = pending_plans.get(plan_id)
    if not plan:
        return {"error": f"Plan {plan_id} not found"}

    plan.status = "rejected"
    logger.info(f"Plan {plan_id} rejected: {reason}")
    return {"plan_id": plan_id, "status": "rejected", "reason": reason}


# =============================================================================
# STAGE 9 — FLASK INTEGRATION
# =============================================================================
# How to wire all of the above into your existing Flask portal.
# These are the route functions to add to your tickets blueprint.
#
# Note: In a real portal, the agent runs inside a Celery task because:
#   - HTTP requests time out after ~30 seconds
#   - Agent runs can take 30-60 seconds with multiple tool calls
#   - Celery runs the agent in the background, client polls for result
# =============================================================================

def get_flask_routes_code() -> str:
    """
    Returns the Flask route code as a string for reference.
    Copy these into your app/routes/tickets.py
    """
    return '''
## ─────────────────────────────────────────────────────────────────────────────
## ADD THESE ROUTES TO app/routes/tickets.py
## ─────────────────────────────────────────────────────────────────────────────

from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity

tickets_bp = Blueprint("tickets", __name__)


@tickets_bp.post("/<ticket_id>/investigate")
@jwt_required()
def trigger_investigation(ticket_id: str):
    """
    Start an AI investigation of a ticket.
    Returns immediately with a task_id — client polls for result.

    POST /api/tickets/<ticket_id>/investigate
    """
    identity = get_jwt_identity()

    # Check permissions (only agents, leads, admins can trigger)
    if identity["role"] not in ("AGENT", "LEAD", "ADMIN", "SUPERADMIN"):
        return jsonify({"error": "Insufficient permissions"}), 403

    # Get the ticket
    from app.models import Ticket
    ticket = Ticket.query.filter_by(
        id=ticket_id,
        org_id=identity.get("org_id")  # agents can see all orgs, clients only theirs
    ).first_or_404()

    # Queue the investigation as a Celery background task
    # This returns immediately — the investigation runs in the background
    from app.tasks import run_ticket_investigation_task
    task = run_ticket_investigation_task.delay(
        ticket_id    = ticket_id,
        ticket_title = ticket.title,
        org_id       = ticket.org_id,
        requested_by = identity["user_id"]
    )

    # Return the task ID — client uses this to poll for results
    return jsonify({
        "status":  "started",
        "task_id": str(task.id),
        "message": "Investigation running. Poll /investigate/status/<task_id> for results."
    }), 202  # 202 Accepted — result is not ready yet


@tickets_bp.get("/investigate/status/<task_id>")
@jwt_required()
def investigation_status(task_id: str):
    """
    Poll the status of an investigation task.

    GET /api/tickets/investigate/status/<task_id>
    """
    from app import celery
    task = celery.AsyncResult(task_id)

    if task.status == "SUCCESS":
        return jsonify({
            "status": "done",
            "result": task.result   # the investigation report text
        }), 200
    elif task.status == "FAILURE":
        return jsonify({
            "status": "failed",
            "error":  str(task.result)
        }), 500
    else:
        return jsonify({
            "status":  task.status,  # PENDING | STARTED | RETRY
            "message": "Investigation still running..."
        }), 200


@tickets_bp.post("/<ticket_id>/plan")
@jwt_required()
def create_plan(ticket_id: str):
    """
    Create an action plan for a ticket (human-in-the-loop Phase 1).
    Agent investigates and proposes actions. Human reviews before execution.

    POST /api/tickets/<ticket_id>/plan
    """
    identity = get_jwt_identity()

    if identity["role"] not in ("AGENT", "LEAD", "ADMIN", "SUPERADMIN"):
        return jsonify({"error": "Insufficient permissions"}), 403

    from app.models import Ticket
    ticket = Ticket.query.get_or_404(ticket_id)

    # Create the plan (reads data, proposes actions, does NOT execute)
    from your_agentic_file import create_action_plan
    plan = create_action_plan(
        ticket_id    = ticket_id,
        ticket_title = ticket.title,
        org_id       = ticket.org_id
    )

    return jsonify(plan.to_dict()), 201


@tickets_bp.post("/plans/<plan_id>/approve")
@jwt_required()
def approve_plan(plan_id: str):
    """
    Approve and execute a plan (human-in-the-loop Phase 2).
    Only leads and admins can approve write actions.

    POST /api/tickets/plans/<plan_id>/approve
    """
    identity = get_jwt_identity()

    # Require lead or admin to approve write actions
    if identity["role"] not in ("LEAD", "ADMIN", "SUPERADMIN"):
        return jsonify({"error": "Only leads and admins can approve plans"}), 403

    from your_agentic_file import approve_and_execute_plan
    result = approve_and_execute_plan(
        plan_id             = plan_id,
        approved_by_user_id = identity["user_id"]
    )
    return jsonify(result), 200


@tickets_bp.post("/plans/<plan_id>/reject")
@jwt_required()
def reject_plan_route(plan_id: str):
    """
    Reject a proposed plan. No actions are executed.

    POST /api/tickets/plans/<plan_id>/reject
    Body: {"reason": "wrong agent suggested"}
    """
    identity = get_jwt_identity()
    if identity["role"] not in ("LEAD", "ADMIN", "SUPERADMIN"):
        return jsonify({"error": "Only leads and admins can reject plans"}), 403

    data = request.get_json() or {}
    from your_agentic_file import reject_plan
    result = reject_plan(plan_id, reason=data.get("reason", "No reason given"))
    return jsonify(result), 200


## ─────────────────────────────────────────────────────────────────────────────
## ADD THIS TO app/tasks.py
## ─────────────────────────────────────────────────────────────────────────────

@celery.task(
    bind         = True,       # self = the task instance (for retry)
    name         = "app.tasks.run_ticket_investigation_task",
    max_retries  = 2,
    time_limit   = 120,        # kill task after 120 seconds
    soft_time_limit = 100,     # raises SoftTimeLimitExceeded at 100s (graceful)
)
def run_ticket_investigation_task(self, ticket_id, ticket_title, org_id, requested_by):
    """
    Celery task that runs the ticket investigation agent.
    Called by POST /api/tickets/<ticket_id>/investigate
    """
    try:
        from your_agentic_file import (
            run_agent,
            TICKET_INVESTIGATION_TOOLS,
            TICKET_INVESTIGATION_SYSTEM_PROMPT
        )

        result = run_agent(
            system_prompt = TICKET_INVESTIGATION_SYSTEM_PROMPT,
            user_message  = f"""
                Investigate this ticket and produce a resolution recommendation.
                Ticket ID: {ticket_id}
                Title: {ticket_title}
                Organisation: {org_id}
            """,
            tools     = TICKET_INVESTIGATION_TOOLS,
            max_turns = 8,
            verbose   = True
        )

        # Save result to DB (so it persists after task completes)
        from app.models import AgentInvestigation
        from app import db
        investigation = AgentInvestigation(
            ticket_id      = ticket_id,
            result_text    = result,
            requested_by   = requested_by,
            celery_task_id = self.request.id,
        )
        db.session.add(investigation)
        db.session.commit()

        return result

    except Exception as exc:
        # Retry up to max_retries times with 15 second delay
        raise self.retry(exc=exc, countdown=15)
'''


# =============================================================================
# STAGE 10 — PRODUCTION HARDENING
# =============================================================================
# Rules to follow before shipping any agent to production.
# These are the lessons every team learns the hard way.
# =============================================================================

PRODUCTION_CHECKLIST = """
=============================================================================
PRODUCTION CHECKLIST — COMPLETE BEFORE SHIPPING ANY AGENT
=============================================================================

OBSERVABILITY (you must be able to see what happened):
  □ Log every tool call: tool_name, inputs, outputs, latency_ms
  □ Log every agent run: ticket_id, turns_used, total_tokens, finish_reason
  □ Save prompt_snapshot to DB for every decision (for debugging)
  □ Set up alerts for: agent_error_rate > 5%, avg_turns > 8, cost_per_run > $0.50
  □ Add /api/agent/logs endpoint so admins can review agent behavior

SAFETY:
  □ max_turns=10 on every run_agent() call (prevents infinite loops)
  □ 120s timeout on every Celery task (prevents hanging tasks)
  □ try/except in execute_tool() — tool failure returns {"error":...} not crash
  □ All write tools require human approval (ActionPlan pattern)
  □ Never give agents database delete permissions
  □ Rate limit: max 3 agent runs per ticket, 50 per org per day

COST CONTROL:
  □ Log input_tokens + output_tokens per run to your DB
  □ Use claude-3-haiku for simple classification (10x cheaper than Sonnet)
  □ Use claude-3-5-sonnet for investigation and synthesis
  □ Cache tool results in Redis (TTL=10min) — same KB search twice = one API call
  □ Set spending limits in Anthropic console dashboard
  □ Alert if daily spend > $50

RELIABILITY:
  □ Celery task retries (max_retries=2, countdown=15)
  □ Graceful degradation: if agent fails, fall back to non-agent workflow
  □ Circuit breaker: if 3+ consecutive failures, disable agent and alert
  □ Test with: mock tools (no real API), real tools (real DB), full end-to-end

TESTING:
  □ Unit test each tool function with real DB data
  □ Integration test run_agent() with mock tools (fast, no API calls)
  □ Eval test: run on 20 real tickets, human grades recommendations 1-5
  □ Track override_rate: if agents are wrong >30% of the time, retune system prompt
  □ Load test: 10 concurrent investigations — check DB connection pool

SECURITY:
  □ Agent tools enforce same permission checks as REST endpoints
  □ org_id filter on every DB query in every tool function
  □ Never return raw stack traces to agent (return {"error": "description"})
  □ Sanitise tool inputs before passing to DB queries (prevent injection)
  □ API key in environment variable only, never in code or logs
"""


# =============================================================================
# DEMO RUNNER — run this to see everything working
# =============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("AGENTIC AI DEMO — running all stages")
    print("=" * 70 + "\n")

    # ── Check if API key is set ───────────────────────────────────────────────
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ERROR: ANTHROPIC_API_KEY environment variable not set.")
        print("Set it with: export ANTHROPIC_API_KEY=sk-ant-your-key-here")
        exit(1)

    # ── DEMO: Run Stage 1 — single call ──────────────────────────────────────
    print("\n--- DEMO: Stage 1 — Single LLM call ---")
    result = stage1_getting_json_back()
    print(f"Stage 1 result: {result}\n")

    # ── DEMO: Run Stage 4 — full agent loop ──────────────────────────────────
    print("\n--- DEMO: Stage 4 — Full agent investigation ---")
    investigation_result = run_agent(
        system_prompt = TICKET_INVESTIGATION_SYSTEM_PROMPT,
        user_message  = """
        Investigate this P1 ticket and produce a resolution recommendation:

        Ticket ID:   T-1001
        Title:       Login broken for all EU users after 2pm deploy
        Description: Since the 14:00 deployment today, all users in the EU region
                     are receiving 401 Unauthorized errors when logging in.
                     US users are unaffected. Affects approximately 2,400 users.
                     Customer has already rebooted servers with no effect.
        Organisation: Acme Corp (org_id=acme-001)
        Priority:     P1
        """,
        tools     = TICKET_INVESTIGATION_TOOLS,
        max_turns = 8,
        verbose   = True
    )
    print("\n=== INVESTIGATION RESULT ===")
    print(investigation_result)

    # ── DEMO: Run Stage 7 — memory ───────────────────────────────────────────
    print("\n--- DEMO: Stage 7 — Stateful agent with memory ---")

    # First run — no memories yet
    print("First run (no memories):")
    run_stateful_agent(
        org_id = "acme-001",
        task   = "Give me a brief status update on Acme Corp's support health."
    )

    # Memories were saved during first run
    # Second run — agent remembers what it found
    print("\nSecond run (with memories from first run):")
    run_stateful_agent(
        org_id = "acme-001",
        task   = "Acme reported another login issue. Is this a pattern?"
    )

    # ── DEMO: Run Stage 8 — human-in-the-loop ────────────────────────────────
    print("\n--- DEMO: Stage 8 — Human in the loop ---")

    plan = create_action_plan(
        ticket_id    = "T-1001",
        ticket_title = "Login broken for all EU users",
        org_id       = "acme-001"
    )

    print(f"\nPlan created: {plan.id}")
    print(f"Proposed actions: {json.dumps(plan.actions, indent=2)}")
    print("\nIn real portal: human reviews this in the UI and clicks Approve or Reject")
    print("Simulating approval...")

    result = approve_and_execute_plan(
        plan_id             = plan.id,
        approved_by_user_id = "lead-vera-001"
    )
    print(f"Execution result: {result}")

    print("\n" + "=" * 70)
    print("DEMO COMPLETE")
    print("Review the logs above to understand what happened at each stage.")
    print(PRODUCTION_CHECKLIST)
