"""
config/settings.py
------------------
Centralised settings loader. All modules import from here.
Never read os.environ directly — always use `settings`.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- PostgreSQL ---
    database_url: str = Field(..., description="PostgreSQL async connection string")

    # --- Baileys ---
    target_group_jid: str = Field(..., description="WhatsApp group JID to listen to")

    # --- FastAPI ---
    ingest_url: str = Field(default="http://localhost:8000/ingest")
    api_port: int = Field(default=8000)

    # --- LLM (Phase 9 only) ---
    llm_provider: str = Field(default="openai")
    llm_api_key: str = Field(default="")
    llm_model: str = Field(default="gpt-4o-mini")
    llm_max_tokens: int = Field(default=300)
    llm_daily_call_limit: int = Field(default=200)
    llm_cache_ttl_hours: int = Field(default=24)

    # --- Google APIs (Phases 15-16) ---
    google_service_account_json: str = Field(default="config/google_service_account.json")
    google_sheet_id: str = Field(default="")
    google_calendar_id: str = Field(default="primary")

    # --- Queue ---
    queue_backend: str = Field(default="postgres")

    # --- Cloud (Phase 17 only) ---
    aws_sqs_queue_url: str = Field(default="")
    aws_region: str = Field(default="")
    azure_queue_connection_string: str = Field(default="")
    azure_queue_name: str = Field(default="")

    # --- Environment ---
    env: str = Field(default="development")
    log_level: str = Field(default="INFO")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Returns a cached singleton Settings instance.
    Import and call this wherever settings are needed:

        from config.settings import get_settings
        settings = get_settings()
    """
    return Settings()