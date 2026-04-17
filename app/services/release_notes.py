import json
import logging
from langchain_core.prompts import ChatPromptTemplate
from app.services import llm

logger = logging.getLogger(__name__)

prompt = ChatPromptTemplate.from_template("""
You are a technical writer producing release notes for a B2B Sales Forecasting SaaS platform.

Generate human-readable release notes from the following list of features and bug fixes.

Version: {version}
Features: {features}
Bug Fixes: {bug_fixes}

Write professional, client-facing release notes. Group into sections: What's New, Bug Fixes, Improvements.
Use plain language — avoid jargon.

Return ONLY valid JSON:
{{
  "release_notes": "<full markdown release notes>",
  "summary": "<1-2 sentence executive summary>"
}}
""")


class ReleaseNotesDraft:
    def run(version: str, features: list[dict], bug_fixes: list[dict]):
        try:
            chain = prompt | llm._get_llm()
            response = chain.invoke({
                "version": version,
                "features": json.dumps(features, indent=2),
                "bug_fixes": json.dumps(bug_fixes, indent=2),
            })
            return json.loads(response.content.strip())
        except Exception as e:
            logger.error(f"Release notes draft failed: {e}")
            return None
