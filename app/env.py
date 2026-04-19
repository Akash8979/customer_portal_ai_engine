import os

DNS_TYPE_CONNECTION_STRING = os.getenv("DNS_TYPE_CONNECTION_STRING", "")
CONNECTION_STRING = os.getenv("CONNECTION_STRING", "")
    

MODEL = os.getenv("AI_MODEL", "openai/gpt-oss-120b:free")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "bge-m3")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "langchain_pg_embedding")
API_KEY = os.getenv("API_KEY","")
TEMPERATURE = int(os.getenv("AI_TEMPERATURE", "0"))
MAX_TOKENS = int(os.getenv("AI_MAX_TOKENS", "1024"))
MODEL_URL = os.getenv("AI_MODEL_URL", "https://openrouter.ai/api/v1")

TICKET_UPDATE_URL = os.getenv("TICKET_UPDATE_URL", "http://localhost:9090")
AUTH_URL = os.getenv("AUTH_URL", "http://localhost:9090")
AUTH_EMAIL_ID = os.getenv("AUTH_EMAIL_ID", "internal_agent_test_1@gmail.com")
AUTH_PASSWORD = os.getenv("AUTH_PASSWORD", "test")
