"""Runtime configuration parsed from YAML with env-var overrides for secrets."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(slots=True)
class RconSettings:
    host: str
    port: int
    password: str


@dataclass(slots=True)
class TelegramSettings:
    enabled: bool
    token: str
    allowed_user_ids: set[int] = field(default_factory=set)


@dataclass(slots=True)
class SlackSettings:
    enabled: bool
    bot_token: str
    app_token: str
    allowed_user_ids: set[str] = field(default_factory=set)


@dataclass(slots=True)
class Settings:
    log_path: Path
    websocket_host: str
    websocket_port: int
    rcon: RconSettings
    telegram: TelegramSettings
    slack: SlackSettings

    @classmethod
    def from_yaml(cls, path: Path) -> "Settings":
        with path.open("r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle)
        return cls(
            log_path=Path(raw["log_path"]),
            websocket_host=raw.get("websocket_host", "0.0.0.0"),
            websocket_port=int(raw.get("websocket_port", 8765)),
            rcon=RconSettings(
                host=raw["rcon"]["host"],
                port=int(raw["rcon"]["port"]),
                password=os.environ.get("FACTORIO_RCON_PASSWORD", raw["rcon"].get("password", "")),
            ),
            telegram=TelegramSettings(
                enabled=bool(raw["telegram"].get("enabled", False)),
                token=os.environ.get("TELEGRAM_BOT_TOKEN", raw["telegram"].get("token", "")),
                allowed_user_ids=set(raw["telegram"].get("allowed_user_ids", [])),
            ),
            slack=SlackSettings(
                enabled=bool(raw["slack"].get("enabled", False)),
                bot_token=os.environ.get("SLACK_BOT_TOKEN", raw["slack"].get("bot_token", "")),
                app_token=os.environ.get("SLACK_APP_TOKEN", raw["slack"].get("app_token", "")),
                allowed_user_ids=set(raw["slack"].get("allowed_user_ids", [])),
            ),
        )
