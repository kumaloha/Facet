"""
V1 规则：索罗斯流派 — 反身性
================================
核心问题：市场认知与现实之间，是否存在可利用的偏差？

公司级反身性特征：融资依赖、杠杆动态、预期偏差。
反向 DCF 和市场情绪由引擎模块处理（需外部数据）。
"""

from polaris.principles.dimensions import School
from polaris.principles.rules import rule


def _get(features: dict[str, float], key: str) -> float | None:
    return features.get(f"l0.company.{key}")


# ── 融资依赖（反身性通道）──────────────────────────────────────


@rule("fully_dependent_on_financing", School.SOROS, "融资依赖 > 1（完全靠外部输血）")
def fully_dependent_on_financing(f: dict[str, float]) -> float:
    v = _get(f, "financing_dependency")
    return -2.0 if v is not None and v > 1.0 else 0.0


@rule("partial_financing_dependency", School.SOROS, "融资依赖 > 0.5（部分依赖外部融资）")
def partial_financing_dependency(f: dict[str, float]) -> float:
    v = _get(f, "financing_dependency")
    if v is not None and 0.5 < v <= 1.0:
        return -1.0
    return 0.0


@rule("share_dilution", School.SOROS, "股权稀释率 > 5%（持续稀释股东）")
def share_dilution(f: dict[str, float]) -> float:
    v = _get(f, "share_dilution_rate")
    return -1.0 if v is not None and v > 0.05 else 0.0


@rule("high_cash_burn", School.SOROS, "现金消耗率 > 50%（烧钱速度快）")
def high_cash_burn(f: dict[str, float]) -> float:
    v = _get(f, "cash_burn_rate")
    return -1.5 if v is not None and v > 0.5 else 0.0


# ── 杠杆动态（加速度）──────────────────────────────────────────


@rule("leverage_accelerating", School.SOROS, "杠杆加速度 > 0（不稳定性在积累）")
def leverage_accelerating(f: dict[str, float]) -> float:
    v = _get(f, "leverage_acceleration")
    return -1.5 if v is not None and v > 0 else 0.0


@rule("leverage_decelerating", School.SOROS, "杠杆在减速（去杠杆进行中）")
def leverage_decelerating(f: dict[str, float]) -> float:
    v = _get(f, "leverage_acceleration")
    return 1.0 if v is not None and v < -0.05 else 0.0


# ── 预期偏差信号 ──────────────────────────────────────────────────


@rule("consecutive_misses_signal", School.SOROS, "连续低于预期 > 2 季（叙事裂痕）")
def consecutive_misses_signal(f: dict[str, float]) -> float:
    v = _get(f, "consecutive_misses")
    return -1.5 if v is not None and v > 2 else 0.0


@rule("consecutive_beats_signal", School.SOROS, "连续超预期 > 4 季（正向动量）")
def consecutive_beats_signal(f: dict[str, float]) -> float:
    v = _get(f, "consecutive_beats")
    return 1.0 if v is not None and v > 4 else 0.0


# ── 其他反身性信号 ────────────────────────────────────────────────


@rule("many_operational_issues", School.SOROS, "经营议题 > 10（市场可能忽视的负面密度）")
def many_operational_issues(f: dict[str, float]) -> float:
    v = _get(f, "operational_issue_count")
    return -1.0 if v is not None and v > 10 else 0.0


@rule("goodwill_bubble", School.SOROS, "商誉增速 > 收入增速 20pp（泡沫信号）")
def goodwill_bubble(f: dict[str, float]) -> float:
    v = _get(f, "goodwill_growth_vs_revenue_growth")
    return -1.0 if v is not None and v > 0.20 else 0.0
