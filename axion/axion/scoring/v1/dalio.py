"""
V1 规则：达利欧流派 — 周期定位
================================
核心问题：当前周期环境下，这家公司是安全的还是脆弱的？

公司级脆弱度规则：评估债务健康、利率敏感性、流动性。
宏观象限和风险平价由引擎模块处理（需外部数据）。
"""

from axion.scoring.dimensions import School
from axion.scoring.rules import rule


def _get(features: dict[str, float], key: str) -> float | None:
    return features.get(f"l0.company.{key}")


# ── 债务健康（正面信号）──────────────────────────────────────────


@rule("conservative_leverage", School.DALIO, "净债务/EBITDA < 1（保守杠杆）")
def conservative_leverage(f: dict[str, float]) -> float:
    v = _get(f, "net_debt_to_ebitda")
    return 1.5 if v is not None and v < 1.0 else 0.0


@rule("low_debt_burden", School.DALIO, "利息支出/OCF < 10%（偿债无压力）")
def low_debt_burden(f: dict[str, float]) -> float:
    v = _get(f, "debt_service_burden")
    return 1.0 if v is not None and v < 0.10 else 0.0


@rule("strong_cash_buffer", School.DALIO, "现金/短期债务 > 3（充裕现金缓冲）")
def strong_cash_buffer(f: dict[str, float]) -> float:
    v = _get(f, "cash_to_short_term_debt")
    return 1.0 if v is not None and v > 3.0 else 0.0


# ── 债务健康（负面信号）──────────────────────────────────────────


@rule("high_debt_service", School.DALIO, "利息支出/OCF > 30%（偿债负担过重）")
def high_debt_service(f: dict[str, float]) -> float:
    v = _get(f, "debt_service_burden")
    return -2.0 if v is not None and v > 0.30 else 0.0


@rule("excessive_leverage", School.DALIO, "净债务/EBITDA > 4（杠杆过高）")
def excessive_leverage(f: dict[str, float]) -> float:
    v = _get(f, "net_debt_to_ebitda")
    return -2.0 if v is not None and v > 4.0 else 0.0


@rule("debt_outpacing_revenue", School.DALIO, "债务增速 > 收入增速 10pp（借钱比赚钱快）")
def debt_outpacing_revenue(f: dict[str, float]) -> float:
    v = _get(f, "debt_growth_vs_revenue_growth")
    return -1.0 if v is not None and v > 0.10 else 0.0


# ── 利率敏感性 ────────────────────────────────────────────────────


@rule("high_floating_rate", School.DALIO, "浮动利率债务 > 50%（利率上升直接冲击）")
def high_floating_rate(f: dict[str, float]) -> float:
    v = _get(f, "floating_rate_debt_pct")
    return -1.5 if v is not None and v > 0.50 else 0.0


@rule("high_interest_to_revenue", School.DALIO, "利息支出/收入 > 10%（利息侵蚀利润）")
def high_interest_to_revenue(f: dict[str, float]) -> float:
    v = _get(f, "interest_to_revenue")
    return -1.0 if v is not None and v > 0.10 else 0.0


# ── 流动性 ────────────────────────────────────────────────────────


@rule("refinancing_wall_high", School.DALIO, "到期债务/OCF > 2（再融资压力大）")
def refinancing_wall_high(f: dict[str, float]) -> float:
    v = _get(f, "refinancing_wall")
    return -1.5 if v is not None and v > 2.0 else 0.0


@rule("cash_short_of_current_debt", School.DALIO, "现金/短期债务 < 1（短期流动性不足）")
def cash_short_of_current_debt(f: dict[str, float]) -> float:
    v = _get(f, "cash_to_short_term_debt")
    return -1.5 if v is not None and v < 1.0 else 0.0


@rule("high_current_debt_share", School.DALIO, "短期债务 > 50%（集中到期风险）")
def high_current_debt_share(f: dict[str, float]) -> float:
    v = _get(f, "current_debt_pct")
    return -1.0 if v is not None and v > 0.50 else 0.0
