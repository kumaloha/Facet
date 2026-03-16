"""
DCF 引擎
========
正向 DCF（巴菲特·内在价值） + 反向 DCF（索罗斯·隐含增速）。

两者共享 owner_earnings、折现率等基础数据。
折现率 = 10Y Treasury yield，来自外部数据。

使用条件：
- 正向 DCF：公司必须通过巴菲特过滤 + 有 management_guidance
- 反向 DCF：需要当前股价（外部数据）
"""

from dataclasses import dataclass

# 预测年限
PROJECTION_YEARS = 10


@dataclass
class DCFResult:
    intrinsic_value: float | None = None
    valuation_path: str | None = None  # A / B / C / D
    key_assumptions: dict[str, float] | None = None
    status: str = "not_computed"  # valued / unvaluable / divergent / not_computed


@dataclass
class ReverseDCFResult:
    implied_growth_rate: float | None = None
    status: str = "not_computed"  # computed / needs_price / not_computed


def forward_dcf(
    current_owner_earnings: float,
    growth_rate: float,
    discount_rate: float,
    payout_ratio: float = 0.0,
    years: int = PROJECTION_YEARS,
) -> float:
    """正向 DCF：从基本面推算内在价值。

    路径通用公式：
    PV = Σ [OE × (1+g)^t] / (1+r)^t + 永续价值

    永续价值 = 第 N 年 OE / r（假设零增长永续）
    """
    if discount_rate <= 0:
        return 0.0

    pv = 0.0
    oe = current_owner_earnings

    for t in range(1, years + 1):
        oe = oe * (1 + growth_rate)
        pv += oe / (1 + discount_rate) ** t

    # 永续价值
    terminal = oe / discount_rate
    pv += terminal / (1 + discount_rate) ** years

    return pv


def compute_intrinsic_value(
    features: dict[str, float],
    guidance: dict[str, float | None],
    discount_rate: float,
    shares_outstanding: float,
) -> DCFResult:
    """根据可用 guidance 选择计算路径。

    路径优先级：A > B > C > D
    - A: capex guidance + ROIC
    - B: revenue_growth guidance
    - C: EPS guidance
    - D: margin guidance + 历史增速
    """
    oe = features.get("l0.company.owner_earnings")
    if oe is None or oe <= 0:
        return DCFResult(status="unvaluable")

    oe_margin = features.get("l0.company.owner_earnings_margin")
    revenue = features.get("l0.company.revenue")
    payout = features.get("l0.company.dividend_payout_ratio", 0.0)

    # 路径 A: capex + ROIC
    roic = guidance.get("roic_target") or features.get("l0.company.incremental_roic")
    if guidance.get("capex") is not None and roic is not None:
        retention = 1 - payout
        growth = retention * roic
        iv = forward_dcf(oe, growth, discount_rate, payout)
        return DCFResult(
            intrinsic_value=iv / shares_outstanding if shares_outstanding > 0 else iv,
            valuation_path="A",
            key_assumptions={"growth": growth, "roic": roic, "discount_rate": discount_rate},
            status="valued",
        )

    # 路径 B: revenue_growth
    rev_growth = guidance.get("revenue_growth")
    if rev_growth is not None:
        iv = forward_dcf(oe, rev_growth, discount_rate, payout)
        return DCFResult(
            intrinsic_value=iv / shares_outstanding if shares_outstanding > 0 else iv,
            valuation_path="B",
            key_assumptions={"revenue_growth": rev_growth, "discount_rate": discount_rate},
            status="valued",
        )

    # 路径 C: EPS
    eps = guidance.get("eps")
    if eps is not None and shares_outstanding > 0:
        ni_future = eps * shares_outstanding
        oe_ni_ratio = features.get("l0.company.owner_earnings_to_net_income", 1.0)
        oe_future = ni_future * oe_ni_ratio
        # 单期估算
        iv = oe_future / discount_rate if discount_rate > 0 else 0
        return DCFResult(
            intrinsic_value=iv / shares_outstanding if shares_outstanding > 0 else iv,
            valuation_path="C",
            key_assumptions={"eps": eps, "discount_rate": discount_rate},
            status="valued",
        )

    # 路径 D: margin guidance + 历史增速
    margin = guidance.get("operating_margin") or guidance.get("net_margin")
    hist_growth = features.get("l0.company.revenue_growth_yoy")
    if margin is not None and hist_growth is not None and revenue is not None:
        future_rev = revenue * (1 + hist_growth)
        future_oe = future_rev * margin  # 近似
        growth = hist_growth  # 用历史增速
        iv = forward_dcf(future_oe, growth, discount_rate, payout)
        return DCFResult(
            intrinsic_value=iv / shares_outstanding if shares_outstanding > 0 else iv,
            valuation_path="D",
            key_assumptions={"margin": margin, "hist_growth": hist_growth, "discount_rate": discount_rate},
            status="valued",
        )

    return DCFResult(status="unvaluable")


def reverse_dcf(
    current_price: float,
    current_owner_earnings: float,
    discount_rate: float,
    shares_outstanding: float,
    years: int = PROJECTION_YEARS,
) -> ReverseDCFResult:
    """反向 DCF：从股价反推市场隐含增速。

    求解 g 使得 DCF(OE, g, r) = current_price × shares_outstanding
    使用二分法求解。
    """
    if current_owner_earnings <= 0 or shares_outstanding <= 0 or discount_rate <= 0:
        return ReverseDCFResult(status="not_computed")

    market_cap = current_price * shares_outstanding

    # 二分法：增速范围 -50% ~ +100%
    lo, hi = -0.50, 1.00
    for _ in range(100):
        mid = (lo + hi) / 2
        dcf_val = forward_dcf(current_owner_earnings, mid, discount_rate, years=years)
        if dcf_val < market_cap:
            lo = mid
        else:
            hi = mid
        if abs(hi - lo) < 0.0001:
            break

    implied_growth = (lo + hi) / 2
    return ReverseDCFResult(
        implied_growth_rate=implied_growth,
        status="computed",
    )
