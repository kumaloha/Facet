"""
Axion 自有 DB（存储特征值和评分结果）
=====================================
本地开发：SQLite
云上部署：PostgreSQL via SQLAlchemy
"""

from pathlib import Path

from sqlalchemy import create_engine, text

from axion.config import settings

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        url = settings.axion_db_url
        connect_args = {}
        if url.startswith("sqlite"):
            connect_args = {"check_same_thread": False}
            # 确保 SQLite 数据目录存在
            db_path = url.replace("sqlite:///", "")
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        _engine = create_engine(url, connect_args=connect_args, pool_pre_ping=True)
    return _engine


def get_connection():
    """获取一个数据库连接（调用方负责 close）。"""
    return get_engine().connect()


def create_tables() -> None:
    """创建 Axion 表。兼容 SQLite 和 PostgreSQL 语法。"""
    engine = get_engine()
    is_pg = str(engine.url).startswith("postgresql")

    if is_pg:
        ddl = """
            CREATE TABLE IF NOT EXISTS feature_values (
                id SERIAL PRIMARY KEY,
                company_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                feature_name TEXT NOT NULL,
                value DOUBLE PRECISION NOT NULL,
                detail TEXT,
                computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_fv_company_period
                ON feature_values(company_id, period);

            CREATE TABLE IF NOT EXISTS company_scores (
                id SERIAL PRIMARY KEY,
                company_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                model_version TEXT NOT NULL DEFAULT 'v1',
                dimension_scores_json TEXT NOT NULL,
                composite_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                top_drivers_json TEXT NOT NULL,
                scored_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_cs_company_period
                ON company_scores(company_id, period);
        """
    else:
        ddl = """
            CREATE TABLE IF NOT EXISTS feature_values (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                feature_name TEXT NOT NULL,
                value REAL NOT NULL,
                detail TEXT,
                computed_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_fv_company_period
                ON feature_values(company_id, period);

            CREATE TABLE IF NOT EXISTS company_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                model_version TEXT NOT NULL DEFAULT 'v1',
                dimension_scores_json TEXT NOT NULL,
                composite_score REAL NOT NULL DEFAULT 0,
                top_drivers_json TEXT NOT NULL,
                scored_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_cs_company_period
                ON company_scores(company_id, period);
        """

    with engine.begin() as conn:
        for stmt in ddl.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))
