"""
达利欧·象限引擎（已迁移）
=========================
此模块保留用于向后兼容。

新的象限定位逻辑已迁移至 polaris.chains.dalio._step_regime，
作为达利欧因果链的第 ① 步，输出 CycleRegime（含象限 + 短期/长期周期阶段）。

详见 polaris/docs/dalio_chain_design.md。
"""

from dataclasses import dataclass, field


@dataclass
class QuadrantResult:
    quadrant: str | None = None  # QUAD_1 / QUAD_2 / QUAD_3 / QUAD_4
    growth_gap: float | None = None
    inflation_gap: float | None = None
    equity_risk_premium: float | None = None
    erp_percentile: float | None = None
    discount_rate_percentile: float | None = None
    valuation_signal: str | None = None  # SELL_RISK_ASSETS / BUY_THE_FEAR / HOLD
    discount_rate_slope: float | None = None
    status: str = "needs_external_data"


QUADRANT_NAMES = {
    (True, False): "QUAD_1_GOLDILOCKS",
    (True, True): "QUAD_2_INFLATIONARY_GROWTH",
    (False, True): "QUAD_3_STAGFLATION",
    (False, False): "QUAD_4_DEFLATIONARY_RECESSION",
}


def compute_quadrant(
    actual_growth: float,
    expected_growth: float,
    actual_inflation: float,
    expected_inflation: float,
    discount_rate: float,
    equity_yield: float,
    vix: float,
    erp_history_percentile_fn=None,
    discount_rate_history_percentile_fn=None,
) -> QuadrantResult:
    """计算象限和估值信号。

    percentile_fn: callable(value) -> 0-100 的百分位。
    需要外部提供历史分布的百分位计算函数。
    """
    growth_gap = actual_growth - expected_growth
    inflation_gap = actual_inflation - expected_inflation

    growth_above = growth_gap > 0
    inflation_above = inflation_gap > 0

    quadrant = QUADRANT_NAMES.get((growth_above, inflation_above), "UNKNOWN")

    erp = equity_yield - discount_rate

    # 估值信号
    erp_pct = erp_history_percentile_fn(erp) if erp_history_percentile_fn else None
    dr_pct = discount_rate_history_percentile_fn(discount_rate) if discount_rate_history_percentile_fn else None

    signal = "HOLD"
    if dr_pct is not None and erp_pct is not None:
        if dr_pct > 75 and erp_pct < 25:
            signal = "SELL_RISK_ASSETS"
        elif erp_pct > 90 and vix > 30:
            signal = "BUY_THE_FEAR"

    return QuadrantResult(
        quadrant=quadrant,
        growth_gap=growth_gap,
        inflation_gap=inflation_gap,
        equity_risk_premium=erp,
        erp_percentile=erp_pct,
        discount_rate_percentile=dr_pct,
        valuation_signal=signal,
        status="computed",
    )
