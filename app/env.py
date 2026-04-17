import os
from dotenv import load_dotenv

load_dotenv()

def _pg_url_to_psycopg2(url: str) -> str:
    if url and url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url

_DATABASE_URL = os.getenv("DATABASE_URL", "")

if _DATABASE_URL:
    CONNECTION_STRING = _pg_url_to_psycopg2(_DATABASE_URL)
    DNS_TYPE_CONNECTION_STRING = _DATABASE_URL
else:
    _db_user = os.getenv("DB_USER", "akash.kumar")
    _db_password = os.getenv("DB_PASSWORD", "India%40123")
    _db_host = os.getenv("DB_HOST", "localhost")
    _db_port = os.getenv("DB_PORT", "5432")
    _db_name = os.getenv("DB_NAME", "customer_portal")
    CONNECTION_STRING = f"postgresql+psycopg2://{_db_user}:{_db_password}@{_db_host}:{_db_port}/{_db_name}"
    DNS_TYPE_CONNECTION_STRING = f"postgresql://{_db_user}:{_db_password}@{_db_host}:{_db_port}/{_db_name}"

MODEL = os.getenv("AI_MODEL", "openai/gpt-oss-120b:free")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "bge-m3")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "langchain_pg_embedding")
API_KEY = os.getenv("OPENROUTER_API_KEY", "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")
TEMPERATURE = int(os.getenv("AI_TEMPERATURE", "0"))
MAX_TOKENS = int(os.getenv("AI_MAX_TOKENS", "1024"))
MODEL_URL = os.getenv("AI_MODEL_URL", "https://openrouter.ai/api/v1")

TICKET_UPDATE_URL = os.getenv("TICKET_UPDATE_URL", "http://localhost:9090")
AUTH_URL = os.getenv("AUTH_URL", "http://localhost:9090")
AUTH_EMAIL_ID = os.getenv("AUTH_EMAIL_ID", "internal_agent_test_1@gmail.com")
AUTH_PASSWORD = os.getenv("AUTH_PASSWORD", "test")
