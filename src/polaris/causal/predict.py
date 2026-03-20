"""
因果预测生成
============
当某个变量发生变化时，沿因果图传播，生成末端可检验预测。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

from polaris.causal.graph import CausalChain, CausalGraph, Variable
from polaris.db.session import get_connection


@dataclass
class Prediction:
    chain: CausalChain
    claim: str                      # 可检验的预测文本
    target_variable: str            # 末端变量名
    predicted_direction: str        # up | down | stable
    predicted_magnitude: str | None # 量级
    deadline: date | None           # 什么时候检验


def generate_predictions(
    trigger_variable: str,
    direction: str,
    graph: CausalGraph,
    min_confidence: float = 0.3,
    max_depth: int = 4,
) -> list[Prediction]:
    """
    从触发变量出发，沿因果链传播，生成末端预测。

    Args:
        trigger_variable: 变化的变量名
        direction: 变化方向 "up" | "down"
        graph: 因果图
        min_confidence: 最低链置信度阈值
        max_depth: 最大传播深度
    """
    var = graph.var_by_name.get(trigger_variable)
    if var is None:
        return []

    chains = graph.downstream(var.id, max_depth=max_depth)
    predictions = []

    for chain in chains:
        if chain.min_confidence < min_confidence:
            continue

        # 末端变量
        end_var = graph.variables.get(chain.effect_id)
        if end_var is None:
            continue

        # 推导方向（简化：正向传导保持方向，每条 link 假设正相关）
        # 实际使用中应从 mechanism 判断正/负相关，这里先取最后一条 link 的 magnitude
        last_link = chain.links[-1]

        pred = Prediction(
            chain=chain,
            claim=_build_claim(graph, chain, direction),
            target_variable=end_var.name,
            predicted_direction=direction,  # 简化版，后续可分析正/负传导
            predicted_magnitude=last_link.magnitude,
            deadline=None,
        )
        predictions.append(pred)

    return predictions


def save_predictions(predictions: list[Prediction]) -> int:
    """将预测写入 Polaris DB。返回写入数量。"""
    if not predictions:
        return 0

    from polaris.causal.models import create_prediction_tables
    create_prediction_tables()

    from sqlalchemy import text as sa_text

    count = 0
    with get_connection() as conn:
        for p in predictions:
            conn.execute(
                sa_text(
                    "INSERT INTO causal_predictions "
                    "(chain_link_ids, claim, target_variable, predicted_direction, "
                    "predicted_magnitude, deadline) "
                    "VALUES (:chain, :claim, :target, :dir, :mag, :dl)"
                ),
                {
                    "chain": json.dumps(p.chain.link_ids),
                    "claim": p.claim,
                    "target": p.target_variable,
                    "dir": p.predicted_direction,
                    "mag": p.predicted_magnitude,
                    "dl": p.deadline.isoformat() if p.deadline else None,
                },
            )
            count += 1
        conn.commit()
    return count


def _build_claim(graph: CausalGraph, chain: CausalChain, direction: str) -> str:
    """用因果链构建人可读的预测描述。"""
    parts = []
    for link in chain.links:
        cause = graph.variables.get(link.cause_id)
        effect = graph.variables.get(link.effect_id)
        cause_name = cause.description if cause else "?"
        effect_name = effect.description if effect else "?"
        mag = f"（{link.magnitude}）" if link.magnitude else ""
        parts.append(f"{cause_name} → {effect_name}{mag}")

    trigger = graph.variables.get(chain.cause_id)
    trigger_desc = trigger.description if trigger else "?"
    end = graph.variables.get(chain.effect_id)
    end_desc = end.description if end else "?"

    return f"因 {trigger_desc} {direction}，预测 {end_desc} 将受影响。路径：{'；'.join(parts)}"
