from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=str(ROOT / ".env"), extra="ignore")

    # Week 1 now accepts either a single CSV or a directory containing many
    # location-specific CSVs. Directory input is the default for the Kaggle dump.
    uber_movement_input: str = "data/raw/uber_movement"
    profile_report: str = "artifacts/data_profile.json"
    file_profile_report: str = "artifacts/file_profile.csv"
    normalized_preview: str = "artifacts/normalized_preview.parquet"

    def resolve_repo_path(self, value: str) -> Path:
        path = Path(value)
        return path if path.is_absolute() else ROOT / path


@lru_cache
def get_settings() -> Settings:
    return Settings()


@lru_cache
def get_yaml_config() -> dict:
    with (ROOT / "config" / "settings.yaml").open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)
