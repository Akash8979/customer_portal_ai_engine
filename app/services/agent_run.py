import json
import logging
from langchain_core.prompts import ChatPromptTemplate
from app.services import llm

logger = logging.getLogger(__name__)

prompt = ChatPromptTemplate.from_template("""
You are an AI agent for a B2B SaaS customer success platform.

User prompt: {user_prompt}

Context data:
{context_data}

Execute the user's request using the provided context.
Return a structured, actionable response.

Return ONLY valid JSON:
{{
  "output": "<main response text or analysis>",
  "action_items": ["<action 1>", "<action 2>"],
  "data": {{}}
}}
""")

onboarding_recovery_prompt = ChatPromptTemplate.from_template("""
You are a project manager AI for a B2B SaaS onboarding process.

This client's onboarding is behind schedule by {days_behind} days.

Onboarding data:
{onboarding_data}

Generate a practical recovery plan to get back on track.

Return ONLY valid JSON:
{{
  "summary": "<situation summary>",
  "recovery_plan": [
    {{"week": 1, "actions": ["<action>"], "owner": "CLIENT|DELIVERY"}},
    {{"week": 2, "actions": ["<action>"], "owner": "CLIENT|DELIVERY"}}
  ],
  "risks": ["<risk>"],
  "revised_go_live": "<suggested new date or null>"
}}
""")


class AgentRun:
    def run(user_prompt: str, context_data: dict):
        try:
            chain = prompt | llm._get_llm()
            response = chain.invoke({
                "user_prompt": user_prompt,
                "context_data": json.dumps(context_data, indent=2),
            })
            return json.loads(response.content.strip())
        except Exception as e:
            logger.error(f"Agent run failed: {e}")
            return None


class OnboardingRecovery:
    def run(onboarding_data: dict, days_behind: int):
        try:
            chain = onboarding_recovery_prompt | llm._get_llm()
            response = chain.invoke({
                "onboarding_data": json.dumps(onboarding_data, indent=2),
                "days_behind": days_behind,
            })
            return json.loads(response.content.strip())
        except Exception as e:
            logger.error(f"Onboarding recovery plan failed: {e}")
            return None
