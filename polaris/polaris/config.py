"""
Polaris 配置
============
通过环境变量或 .env 文件配置。本地开发用 SQLite，云上用 PostgreSQL。
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # ── 数据库 ────────────────────────────────────────────────────
    # 支持 sqlite:///path 和 postgresql://user:pass@host/db
    anchor_db_url: str = "sqlite:///./data/facet.db"
    polaris_db_url: str = "sqlite:///./data/facet.db"

    # ── 运行环境 ─────────────────────────────────────────────────
    env: str = "local"  # local / aliyun


settings = Settings()
