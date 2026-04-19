"""
Persistent Personal AI Agent
=============================
3-layer memory:
  - System memory   : user role, permissions, tenant (injected into system prompt)
  - Long-term memory: stored in agent_long_term_memory table (retrieved before each turn)
  - Short-term memory: conversation history for the current session (loaded from DB)

Memory update policy:
  - Store only useful, reusable facts (preferences, recurring issues, key decisions)
  - Never store passwords, tokens, or PII beyond what the user explicitly shares
  - Deduplicate by key; assign importance 1-10 (10 = most important)
"""
import logging
from typing import Optional
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import tool
from app.services.llm import _get_llm
from app.services.personal_agent import memory as mem
logger = logging.getLogger(__name__)

# ── Role → permissions map (mirrors customer_portal/accounts/constant.py) ────
ROLE_PERMISSIONS: dict[str, list[str]] = {
    "CLIENT_ADMIN": ["TICKET_CREATE", "TICKET_VIEW", "TICKET_UPDATE", "COMMENT_CREATE",
                     "COMMENT_VIEW", "MANAGE_USERS"],
    "CLIENT_USER":  ["TICKET_CREATE", "TICKET_VIEW", "COMMENT_CREATE", "COMMENT_VIEW"],
    "AGENT":        ["TICKET_VIEW", "TICKET_UPDATE", "TICKET_STATUS_UPDATE", "COMMENT_CREATE",
                     "COMMENT_VIEW", "COMMENT_UPDATE"],
    "LEAD":         ["TICKET_VIEW", "TICKET_UPDATE", "TICKET_STATUS_UPDATE", "TICKET_ASSIGN",
                     "COMMENT_CREATE", "COMMENT_VIEW", "COMMENT_UPDATE", "MANAGE_USERS"],
    "ADMIN":        ["TICKET_CREATE", "TICKET_VIEW", "TICKET_UPDATE", "TICKET_STATUS_UPDATE",
                     "TICKET_ASSIGN", "TICKET_DELETE", "COMMENT_CREATE", "COMMENT_VIEW",
                     "COMMENT_UPDATE", "COMMENT_DELETE", "MANAGE_USERS", "VIEW_REPORTS"],
}

# ── Memory tools (the agent can call these during its turn) ──────────────────

def _build_memory_tools(user_id: str):
    """Return LangChain tool functions bound to a specific user_id."""

    @tool
    def save_memory(key: str, value: str, importance: int = 5) -> str:
        """
        Save or update a fact in long-term memory.
        Use for preferences, recurring issues, key decisions, user style, context clues.
        importance: 1 (trivial) 10 (critical). Default 5.
        key must be short and descriptive, e.g. 'preferred_language', 'ticket_escalation_style'.
        """
        if not key or not value:
            return "key and value are required."
        importance = max(1, min(10, importance))
        mem.upsert_memory(user_id, key, value, importance)
        return f"Memory saved: {key} = {value!r} (importance={importance})"

    @tool
    def forget_memory(key: str) -> str:
        """
        Remove a fact from long-term memory when it's no longer relevant.
        """
        removed = mem.delete_memory(user_id, key)
        return f"Memory removed: {key}" if removed else f"No memory found for key: {key}"

    @tool
    def search_memory(query: str) -> str:
        """
        Search long-term memory for facts related to a query.
        Returns matching key-value pairs with their importance scores.
        """
        results = mem.search_memories(user_id, query)
        if not results:
            return "No matching memories found."
        lines = [f"- [{r['importance']}] {r['key']}: {r['value']}" for r in results]
        return "\n".join(lines)

    return [save_memory, forget_memory, search_memory]


# ── System prompt builder ────────────────────────────────────────────────────

def _build_system_prompt(user_id: str, role: str, tenant_id: Optional[str]) -> str:
    perms = ROLE_PERMISSIONS.get(role, [])
    long_term = mem.get_all_memories(user_id)

    memory_block = ""
    if long_term:
        lines = [f"  [{m['importance']}] {m['key']}: {m['value']}" for m in long_term[:30]]
        memory_block = "\n\nLONG-TERM MEMORY (what you know about this user):\n" + "\n".join(lines)

    return f"""\
            You are a persistent personal AI assistant for this user. You remember them across every session \
            and adapt to their preferences over time.

            USER PROFILE:
            - user_id   : {user_id}
            - role      : {role}
            - tenant_id : {tenant_id or 'N/A'}
            - permissions: {', '.join(perms) or 'none'}

            ROLE & ACCESS RULES:
            - Only help the user with actions their role permits (see permissions above).
            - If a request requires a permission they do not have, politely explain the restriction.
            - Never reveal other users' data, internal notes restricted to higher roles, or system secrets.
            {memory_block}

            MEMORY UPDATE POLICY:
            - After every turn, use save_memory for any reusable facts you learned (preferences, patterns, \
                decisions). Importance 7+ for critical facts, 4-6 for useful context, 1-3 for minor details.
            - Avoid storing sensitive data (passwords, tokens).
            - Use forget_memory to remove outdated facts.
            - Use search_memory to recall relevant context before answering complex questions.

            RELIABILITY RULES:
            - If you are not sure about something, say so clearly and ask a clarifying question.
            - Never fabricate ticket IDs, user names, or system data.
            - Treat every session as a continuation of an ongoing relationship.

            OUTPUT: Be concise, structured, and directly useful. Use bullet points for lists."""


# ── Main agent entry point ────────────────────────────────────────────────────

def run_agent(
    user_id: str,
    role: str,
    tenant_id: Optional[str],
    message: str,
    session_id: str,
) -> dict:
    """
    Run one conversational turn for the personal agent.

    Returns:
        {
            "response": str,
            "session_id": str,
            "memories_updated": bool,
        }
    """
    # ── 1. Load short-term (session) history ──────────────────────────────
    history = mem.get_session_history(user_id, session_id, limit=20)

    # ── 2. Build system prompt (injects system + long-term memory) ─────────
    system_prompt = _build_system_prompt(user_id, role, tenant_id)

    # ── 3. Persist user turn ───────────────────────────────────────────────
    mem.save_turn(user_id, session_id, "user", message)

    # ── 4. Build message list for LLM ─────────────────────────────────────
    messages = [SystemMessage(content=system_prompt)]
    for turn in history:
        if turn["role"] == "user":
            messages.append(HumanMessage(content=turn["content"]))
        else:
            messages.append(AIMessage(content=turn["content"]))
    messages.append(HumanMessage(content=message))

    # ── 5. Bind memory tools and run agentic loop ─────────────────────────
    tools = _build_memory_tools(user_id)
    tool_map = {t.name: t for t in tools}
    llm_with_tools = _get_llm(max_tokens=2048).bind_tools(tools)

    memories_updated = False
    final_response = ""

    for _ in range(6):  # max 6 LLM calls per turn (tool loop)
        response = llm_with_tools.invoke(messages)
        messages.append(response)

        tool_calls = getattr(response, "tool_calls", []) or []
        if not tool_calls:
            # Extract text response
            if hasattr(response, "content") and isinstance(response.content, str):
                final_response = response.content
            elif hasattr(response, "content") and isinstance(response.content, list):
                final_response = " ".join(
                    b.get("text", "") for b in response.content if isinstance(b, dict)
                )
            break

        # Execute each tool call
        for tc in tool_calls:
            tool_name = tc["name"]
            tool_args = tc["args"]
            tool_id   = tc["id"]

            if tool_name in ("save_memory", "forget_memory"):
                memories_updated = True

            fn = tool_map.get(tool_name)
            if fn:
                try:
                    result = fn.invoke(tool_args)
                except Exception as e:
                    result = f"Tool error: {e}"
            else:
                result = f"Unknown tool: {tool_name}"

            messages.append(ToolMessage(content=str(result), tool_call_id=tool_id))

    # ── 6. Persist assistant turn ──────────────────────────────────────────
    if final_response:
        mem.save_turn(user_id, session_id, "assistant", final_response)

    return {
        "response": final_response or "I was unable to generate a response. Please try again.",
        "session_id": session_id,
        "memories_updated": memories_updated,
    }
