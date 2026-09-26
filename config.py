"""Application configuration backed by pydantic-settings.

All secrets, hostnames, ports, and file paths are read from environment
variables or a local ``.env`` file. No defaults are provided for credentials
to ensure deployments fail loudly when configuration is missing.
"""
from __future__ import annotations

from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Strongly typed runtime configuration loaded from the environment.

    Attributes are grouped by concern: RCON transport, log ingestion,
    persistence, messenger integrations, rate limiting, visualization
    thresholds, and the WebSocket broadcast endpoint.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    rcon_host: str = "127.0.0.1"
    rcon_port: int = 27015
    rcon_password: str
    rcon_command_timeout: float = 5.0
    rcon_connect_timeout: float = 5.0
    rcon_max_backoff_seconds: float = 60.0

    log_file_path: Path

    db_path: Path = Path("factorio_metrics.sqlite3")
    db_flush_interval_seconds: float = 5.0

    telegram_enabled: bool = False
    telegram_bot_token: str = ""
    telegram_allowed_user_ids: list[int] = Field(default_factory=list)

    slack_enabled: bool = False
    slack_bot_token: str = ""
    slack_app_token: str = ""

    rate_limit_max_commands: int = 5
    rate_limit_window_seconds: float = 30.0

    ups_warning_threshold: float = 50.0

    ws_host: str = "0.0.0.0"
    ws_port: int = 8765
    ws_secret_token: str

    @field_validator("telegram_allowed_user_ids", mode="before")
    @classmethod
    def _split_user_ids(cls, value: object) -> object:
        """Accept comma-separated strings for env var convenience."""
        if isinstance(value, str):
            return [int(part) for part in value.split(",") if part.strip()]
        return value


def load_settings() -> Settings:
    """Return a fully validated :class:`Settings` instance."""
    return Settings()  # type: ignore[call-arg]
