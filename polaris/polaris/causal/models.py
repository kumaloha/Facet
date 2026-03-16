"""
因果预测表 — Polaris 自有 DB
============================
CausalPrediction: 从因果链生成的可检验预测。
"""

from datetime import datetime, timezone

from sqlalchemy import text

from polaris.db.session import get_engine


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


_DDL_SQLITE = """
CREATE TABLE IF NOT EXISTS causal_predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chain_link_ids TEXT NOT NULL,
    claim TEXT NOT NULL,
    target_variable TEXT NOT NULL,
    predicted_direction TEXT NOT NULL,
    predicted_magnitude TEXT,
    deadline TEXT,
    outcome TEXT NOT NULL DEFAULT 'pending',
    resolved_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_cp_outcome
    ON causal_predictions(outcome);
CREATE INDEX IF NOT EXISTS idx_cp_deadline
    ON causal_predictions(deadline);
"""

_DDL_PG = """
CREATE TABLE IF NOT EXISTS causal_predictions (
    id SERIAL PRIMARY KEY,
    chain_link_ids TEXT NOT NULL,
    claim TEXT NOT NULL,
    target_variable TEXT NOT NULL,
    predicted_direction TEXT NOT NULL,
    predicted_magnitude TEXT,
    deadline TEXT,
    outcome TEXT NOT NULL DEFAULT 'pending',
    resolved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cp_outcome
    ON causal_predictions(outcome);
CREATE INDEX IF NOT EXISTS idx_cp_deadline
    ON causal_predictions(deadline);
"""


def create_prediction_tables() -> None:
    """创建 causal_predictions 表。"""
    engine = get_engine()
    is_pg = str(engine.url).startswith("postgresql")
    ddl = _DDL_PG if is_pg else _DDL_SQLITE
    with engine.begin() as conn:
        for stmt in ddl.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))
