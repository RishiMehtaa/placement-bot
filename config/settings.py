# # config/settings.py
# from pydantic_settings import BaseSettings
# from typing import Optional


# class Settings(BaseSettings):
#     # Database
#     DATABASE_URL: str

#     # Baileys
#     TARGET_GROUP_JID: str = "120363XXXXXXXXXX@g.us"

#     # FastAPI
#     INGEST_URL: str = "http://fastapi:8000/ingest"
#     API_PORT: int = 8000

#     # CORS
#     CORS_ORIGINS: str = "http://localhost:8080"

#     # LLM
#     LLM_PROVIDER: str = "openai"
#     LLM_API_KEY: str = "sk-placeholder"
#     LLM_MODEL: str = "gpt-4o-mini"
#     LLM_MAX_TOKENS: int = 300
#     LLM_DAILY_CALL_LIMIT: int = 200
#     LLM_CACHE_TTL_HOURS: int = 24

#     # Google APIs
#     GOOGLE_SERVICE_ACCOUNT_JSON: str = "config/google_service_account.json"
#     GOOGLE_SHEET_ID: str = "your-sheet-id-here"
#     GOOGLE_CALENDAR_ID: str = "primary"

#     # Queue
#     QUEUE_BACKEND: str = "postgres"

#     # Scheduler
#     SCHEDULER_INTERVAL_SECONDS: int = 7200
#     SCHEDULER_ENABLED: bool = True

#     # Environment
#     ENV: str = "development"
#     LOG_LEVEL: str = "INFO"

#     class Config:
#         env_file = ".env"
#         env_file_encoding = "utf-8"
#         extra = "ignore"


# settings = Settings()


"""
Application settings — loaded from .env via pydantic-settings.
"""

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional


class Settings(BaseSettings):
    # PostgreSQL
    DATABASE_URL: str
    POSTGRES_USER: str = "placement_user"
    POSTGRES_PASSWORD: str = "placement_pass"
    POSTGRES_DB: str = "placement_bot"

    # Baileys
    TARGET_GROUP_JID: str

    # FastAPI
    INGEST_URL: str = "http://fastapi:8000/ingest"
    API_PORT: int = 8000

    # CORS
    CORS_ORIGINS: str = "http://localhost:8080"

    # LLM
    LLM_PROVIDER: str = "groq"
    LLM_API_KEY: str
    LLM_MODEL: str = "llama-3.1-8b-instant"
    LLM_MAX_TOKENS: int = 300
    LLM_DAILY_CALL_LIMIT: int = 200
    LLM_CACHE_TTL_HOURS: int = 24

    # Google APIs
    GOOGLE_SERVICE_ACCOUNT_JSON: str = "config/google_service_account.json"
    GOOGLE_SHEET_ID: str
    GOOGLE_CALENDAR_ID: str = "primary"

    # Queue
    QUEUE_BACKEND: str = "postgres"
    AWS_SQS_QUEUE_URL: Optional[str] = None
    AWS_REGION: str = "ap-south-1"

    # Scheduler
    SCHEDULER_INTERVAL_SECONDS: int = 7200
    SCHEDULER_ENABLED: bool = True

    # Environment
    ENV: str = "development"
    LOG_LEVEL: str = "INFO"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()