"""
预测追踪与置信度更新
====================
定期检查预测结果，更新因果链 confidence。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text as sa_text

from polaris.db.anchor import get_engine as get_anchor_engine
from polaris.db.session import get_connection


@dataclass
class PredictionResult:
    prediction_id: int
    chain_link_ids: list[int]
    outcome: str  # correct | incorrect


def get_pending_predictions() -> pd.DataFrame:
    """获取所有待检验的预测。"""
    with get_connection() as conn:
        return pd.read_sql_query(
            sa_text(
                "SELECT id, chain_link_ids, claim, target_variable, "
                "predicted_direction, predicted_magnitude, deadline "
                "FROM causal_predictions WHERE outcome = 'pending'"
            ),
            conn,
        )


def resolve_prediction(prediction_id: int, outcome: str) -> None:
    """
    标记预测结果，并更新因果链置信度。

    Args:
        prediction_id: 预测 ID
        outcome: "correct" | "incorrect"
    """
    now = datetime.now(timezone.utc).isoformat()

    with get_connection() as conn:
        # 1. 读取预测的 chain_link_ids
        row = conn.execute(
            sa_text("SELECT chain_link_ids FROM causal_predictions WHERE id = :id"),
            {"id": prediction_id},
        ).fetchone()
        if row is None:
            return

        link_ids = json.loads(row[0])

        # 2. 更新预测状态
        conn.execute(
            sa_text(
                "UPDATE causal_predictions SET outcome = :outcome, resolved_at = :now "
                "WHERE id = :id"
            ),
            {"outcome": outcome, "now": now, "id": prediction_id},
        )

        # 3. 更新因果链置信度（Anchor DB）
        _update_link_confidence(link_ids, outcome)

        conn.commit()


def _update_link_confidence(link_ids: list[int], outcome: str) -> None:
    """
    根据预测结果更新因果链 confidence。

    简单策略：
    - correct: confidence += 0.05（上限 0.95）
    - incorrect: confidence -= 0.10（下限 0.05）

    不对称惩罚：错误预测的惩罚是正确预测奖励的 2 倍。
    """
    delta = 0.05 if outcome == "correct" else -0.10

    engine = get_anchor_engine()
    with engine.connect() as conn:
        for lid in link_ids:
            conn.execute(
                sa_text(
                    "UPDATE causal_links "
                    "SET confidence = MIN(0.95, MAX(0.05, confidence + :delta)), "
                    "    updated_at = :now "
                    "WHERE id = :id"
                ),
                {
                    "delta": delta,
                    "now": datetime.now(timezone.utc).isoformat(),
                    "id": lid,
                },
            )
        conn.commit()
