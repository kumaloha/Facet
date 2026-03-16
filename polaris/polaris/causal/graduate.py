"""
因果链毕业 → L1 特征
====================
高置信度的因果链可以"毕业"为正式的 L1 传导特征。

毕业条件：
1. confidence >= 阈值（默认 0.75）
2. 至少 N 次预测验证（默认 3 次）
3. 准确率 >= 阈值（默认 60%）
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import pandas as pd
from sqlalchemy import text as sa_text

from polaris.causal.graph import CausalGraph, Link, Variable
from polaris.db.session import get_connection


@dataclass
class GraduationCandidate:
    """一条可以毕业为 L1 特征的因果链。"""
    link: Link
    cause: Variable
    effect: Variable
    total_predictions: int
    correct_predictions: int
    accuracy: float


def find_candidates(
    graph: CausalGraph,
    min_confidence: float = 0.75,
    min_predictions: int = 3,
    min_accuracy: float = 0.6,
) -> list[GraduationCandidate]:
    """
    找出满足毕业条件的因果链。

    只考虑单条 link（不考虑多跳链），因为 L1 特征定义的是
    一对一的跨域传导关系。
    """
    # 获取每条 link 的预测统计
    stats = _get_link_prediction_stats()

    candidates = []
    for link in graph.links.values():
        if link.confidence < min_confidence:
            continue

        link_stats = stats.get(link.id, (0, 0))
        total, correct = link_stats

        if total < min_predictions:
            continue

        accuracy = correct / total if total > 0 else 0
        if accuracy < min_accuracy:
            continue

        cause = graph.variables.get(link.cause_id)
        effect = graph.variables.get(link.effect_id)
        if cause is None or effect is None:
            continue

        candidates.append(GraduationCandidate(
            link=link,
            cause=cause,
            effect=effect,
            total_predictions=total,
            correct_predictions=correct,
            accuracy=accuracy,
        ))

    candidates.sort(key=lambda c: c.accuracy, reverse=True)
    return candidates


def _get_link_prediction_stats() -> dict[int, tuple[int, int]]:
    """
    获取每条 link 参与的预测统计。

    返回: {link_id: (total_predictions, correct_predictions)}
    """
    try:
        with get_connection() as conn:
            df = pd.read_sql_query(
                sa_text(
                    "SELECT chain_link_ids, outcome "
                    "FROM causal_predictions "
                    "WHERE outcome != 'pending'"
                ),
                conn,
            )
    except Exception:
        return {}

    stats: dict[int, list[int]] = {}  # link_id → [total, correct]

    for _, row in df.iterrows():
        link_ids = json.loads(row["chain_link_ids"])
        outcome = row["outcome"]
        for lid in link_ids:
            if lid not in stats:
                stats[lid] = [0, 0]
            stats[lid][0] += 1
            if outcome == "correct":
                stats[lid][1] += 1

    return {lid: (s[0], s[1]) for lid, s in stats.items()}


def format_feature_definition(candidate: GraduationCandidate) -> str:
    """
    生成 L1 特征的伪代码定义，供人工审核后编入 features/l1/。
    """
    return (
        f"# 毕业自因果链 #{candidate.link.id}\n"
        f"# 置信度: {candidate.link.confidence:.2f} | "
        f"准确率: {candidate.accuracy:.0%} ({candidate.correct_predictions}/{candidate.total_predictions})\n"
        f"#\n"
        f"# {candidate.cause.description} → {candidate.effect.description}\n"
        f"# 机制: {candidate.link.mechanism}\n"
        f"# 量级: {candidate.link.magnitude or '未知'}\n"
        f"# 时滞: {candidate.link.lag or '未知'}\n"
        f"# 条件: {candidate.link.conditions or '无'}\n"
        f"#\n"
        f"# @feature(name='{candidate.cause.name}_to_{candidate.effect.name}',\n"
        f"#          level=FeatureLevel.L1,\n"
        f"#          domain='{candidate.cause.domain}_{candidate.effect.domain}')\n"
        f"# def compute(ctx):\n"
        f"#     ... # TODO: 实现特征计算\n"
    )
