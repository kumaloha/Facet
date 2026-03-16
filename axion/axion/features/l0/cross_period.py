"""
L0 跨期特征
============
需要多期历史数据的特征：稳定性、增速、连续性、加速度。
"""

from axion.features.l0._helpers import (
    consecutive_growth,
    consecutive_positive,
    get_item,
    get_item_series,
    safe_div,
    stability,
    yoy_growth,
)
from axion.features.registry import feature
from axion.features.types import ComputeContext, FeatureLevel, FeatureResult

R = FeatureResult


# ══════════════════════════════════════════════════════════════════════
# Section 2: 护城河（跨期部分）
# ══════════════════════════════════════════════════════════════════════


@feature("l0.company.gross_margin_stability", FeatureLevel.L0, "company")
def gross_margin_stability(ctx: ComputeContext) -> FeatureResult | None:
    """近 20 期毛利率标准差。"""
    rev = get_item_series(ctx, "revenue", 20)
    cogs = get_item_series(ctx, "cost_of_revenue", 20)
    if len(rev) < 4 or len(cogs) < 4:
        return None
    # Align by period
    common = rev.index.intersection(cogs.index)
    if len(common) < 4:
        return None
    gm = (rev[common] - cogs[common]) / rev[common]
    gm = gm.replace([float("inf"), float("-inf")], float("nan")).dropna()
    v = stability(gm)
    return R(value=v, detail=f"periods={len(gm)}") if v is not None else None


@feature("l0.company.gross_margin_delta", FeatureLevel.L0, "company")
def gross_margin_delta(ctx: ComputeContext) -> FeatureResult | None:
    """毛利率同比变化。"""
    rev = get_item_series(ctx, "revenue", 8)
    cogs = get_item_series(ctx, "cost_of_revenue", 8)
    if len(rev) < 2 or len(cogs) < 2:
        return None
    common = rev.index.intersection(cogs.index)
    if len(common) < 2:
        return None
    gm = (rev[common] - cogs[common]) / rev[common]
    gm = gm.dropna()
    if len(gm) < 2:
        return None
    return R(value=float(gm.iloc[-1] - gm.iloc[0]))


@feature("l0.company.consecutive_margin_expansion", FeatureLevel.L0, "company")
def consecutive_margin_expansion(ctx: ComputeContext) -> FeatureResult | None:
    """毛利率连续扩张期数。"""
    rev = get_item_series(ctx, "revenue")
    cogs = get_item_series(ctx, "cost_of_revenue")
    if len(rev) < 3 or len(cogs) < 3:
        return None
    common = rev.index.intersection(cogs.index)
    gm = (rev[common] - cogs[common]) / rev[common]
    gm = gm.dropna()
    return R(value=float(consecutive_growth(gm)))


@feature("l0.company.incremental_roic", FeatureLevel.L0, "company")
def incremental_roic(ctx: ComputeContext) -> FeatureResult | None:
    """增量 ROIC = Δ operating_income / Δ invested_capital"""
    oi = get_item_series(ctx, "operating_income", 8)
    ta = get_item_series(ctx, "total_assets", 8)
    cl = get_item_series(ctx, "current_liabilities", 8)
    cash = get_item_series(ctx, "cash_and_equivalents", 8)

    common = oi.index.intersection(ta.index).intersection(cl.index).intersection(cash.index)
    if len(common) < 2:
        return None

    oi_s = oi[common]
    ic = ta[common] - cl[common] - cash[common]

    delta_oi = oi_s.iloc[-1] - oi_s.iloc[0]
    delta_ic = ic.iloc[-1] - ic.iloc[0]
    if delta_ic == 0:
        return None
    return R(value=delta_oi / delta_ic)


# ══════════════════════════════════════════════════════════════════════
# Section 4: 盈利质量（跨期部分）
# ══════════════════════════════════════════════════════════════════════


@feature("l0.company.ocf_growth_vs_ni_growth", FeatureLevel.L0, "company")
def ocf_growth_vs_ni_growth(ctx: ComputeContext) -> FeatureResult | None:
    """OCF 增速 - 净利润增速（正=现金流比利润增长快）。"""
    ocf = get_item_series(ctx, "operating_cash_flow", 8)
    ni = get_item_series(ctx, "net_income", 8)
    ocf_g = yoy_growth(ocf)
    ni_g = yoy_growth(ni)
    if ocf_g is None or ni_g is None:
        return None
    return R(value=ocf_g - ni_g)


@feature("l0.company.receivables_growth_vs_revenue", FeatureLevel.L0, "company")
def receivables_growth_vs_revenue(ctx: ComputeContext) -> FeatureResult | None:
    """应收增速 - 收入增速（>0 = 赊账堆收入）。"""
    ar = get_item_series(ctx, "accounts_receivable", 8)
    rev = get_item_series(ctx, "revenue", 8)
    ar_g = yoy_growth(ar)
    rev_g = yoy_growth(rev)
    if ar_g is None or rev_g is None:
        return None
    return R(value=ar_g - rev_g)


@feature("l0.company.inventory_growth_vs_revenue", FeatureLevel.L0, "company")
def inventory_growth_vs_revenue(ctx: ComputeContext) -> FeatureResult | None:
    """存货增速 - 收入增速（>0 = 卖不动）。"""
    inv = get_item_series(ctx, "inventory", 8)
    rev = get_item_series(ctx, "revenue", 8)
    inv_g = yoy_growth(inv)
    rev_g = yoy_growth(rev)
    if inv_g is None or rev_g is None:
        return None
    return R(value=inv_g - rev_g)


# ══════════════════════════════════════════════════════════════════════
# Section 5: 资本配置（跨期部分）
# ══════════════════════════════════════════════════════════════════════


@feature("l0.company.goodwill_growth_vs_revenue_growth", FeatureLevel.L0, "company")
def goodwill_growth_vs_revenue_growth(ctx: ComputeContext) -> FeatureResult | None:
    """商誉增速 - 收入增速。"""
    gw = get_item_series(ctx, "goodwill", 8)
    rev = get_item_series(ctx, "revenue", 8)
    gw_g = yoy_growth(gw)
    rev_g = yoy_growth(rev)
    if gw_g is None or rev_g is None:
        return None
    return R(value=gw_g - rev_g)


@feature("l0.company.share_count_trend", FeatureLevel.L0, "company")
def share_count_trend(ctx: ComputeContext) -> FeatureResult | None:
    """股本数量同比变化率。"""
    shares = get_item_series(ctx, "basic_weighted_average_shares", 8)
    v = yoy_growth(shares)
    return R(value=v) if v is not None else None


# ══════════════════════════════════════════════════════════════════════
# Section 7: 可预测性（跨期部分）
# ══════════════════════════════════════════════════════════════════════


@feature("l0.company.net_margin_stability", FeatureLevel.L0, "company")
def net_margin_stability(ctx: ComputeContext) -> FeatureResult | None:
    """近 20 期净利率标准差。"""
    ni = get_item_series(ctx, "net_income", 20)
    rev = get_item_series(ctx, "revenue", 20)
    if len(ni) < 4 or len(rev) < 4:
        return None
    common = ni.index.intersection(rev.index)
    if len(common) < 4:
        return None
    nm = ni[common] / rev[common]
    nm = nm.replace([float("inf"), float("-inf")], float("nan")).dropna()
    v = stability(nm)
    return R(value=v, detail=f"periods={len(nm)}") if v is not None else None


@feature("l0.company.revenue_growth_stability", FeatureLevel.L0, "company")
def revenue_growth_stability(ctx: ComputeContext) -> FeatureResult | None:
    """近 20 期收入增速标准差。"""
    rev = get_item_series(ctx, "revenue", 20)
    if len(rev) < 5:
        return None
    growth = rev.pct_change().dropna()
    v = stability(growth)
    return R(value=v, detail=f"periods={len(growth)}") if v is not None else None


@feature("l0.company.ocf_margin_stability", FeatureLevel.L0, "company")
def ocf_margin_stability(ctx: ComputeContext) -> FeatureResult | None:
    """近 20 期经营现金流率标准差。"""
    ocf = get_item_series(ctx, "operating_cash_flow", 20)
    rev = get_item_series(ctx, "revenue", 20)
    if len(ocf) < 4 or len(rev) < 4:
        return None
    common = ocf.index.intersection(rev.index)
    if len(common) < 4:
        return None
    margin = ocf[common] / rev[common]
    margin = margin.replace([float("inf"), float("-inf")], float("nan")).dropna()
    v = stability(margin)
    return R(value=v, detail=f"periods={len(margin)}") if v is not None else None


@feature("l0.company.roe_stability", FeatureLevel.L0, "company")
def roe_stability(ctx: ComputeContext) -> FeatureResult | None:
    """近 20 期 ROE 标准差。"""
    ni = get_item_series(ctx, "net_income", 20)
    eq = get_item_series(ctx, "shareholders_equity", 20)
    if len(ni) < 4 or len(eq) < 4:
        return None
    common = ni.index.intersection(eq.index)
    if len(common) < 4:
        return None
    roe_s = ni[common] / eq[common]
    roe_s = roe_s.replace([float("inf"), float("-inf")], float("nan")).dropna()
    v = stability(roe_s)
    return R(value=v, detail=f"periods={len(roe_s)}") if v is not None else None


@feature("l0.company.consecutive_revenue_growth", FeatureLevel.L0, "company")
def f_consecutive_revenue_growth(ctx: ComputeContext) -> FeatureResult | None:
    """收入连续正增长的期数。"""
    rev = get_item_series(ctx, "revenue")
    return R(value=float(consecutive_growth(rev)))


@feature("l0.company.consecutive_positive_fcf", FeatureLevel.L0, "company")
def f_consecutive_positive_fcf(ctx: ComputeContext) -> FeatureResult | None:
    """自由现金流连续为正的期数。"""
    ocf = get_item_series(ctx, "operating_cash_flow")
    capex = get_item_series(ctx, "capital_expenditures")
    if len(ocf) < 2 or len(capex) < 2:
        return None
    common = ocf.index.intersection(capex.index)
    fcf = ocf[common] - capex[common].abs()
    return R(value=float(consecutive_positive(fcf)))


@feature("l0.company.revenue_growth_yoy", FeatureLevel.L0, "company")
def revenue_growth_yoy(ctx: ComputeContext) -> FeatureResult | None:
    """收入同比增速。"""
    rev = get_item_series(ctx, "revenue", 8)
    v = yoy_growth(rev)
    return R(value=v) if v is not None else None


@feature("l0.company.owner_earnings_growth_yoy", FeatureLevel.L0, "company")
def owner_earnings_growth_yoy(ctx: ComputeContext) -> FeatureResult | None:
    """所有者盈余同比增速。"""
    ni = get_item_series(ctx, "net_income", 8)
    da = get_item_series(ctx, "depreciation_amortization", 8)
    capex = get_item_series(ctx, "capital_expenditures", 8)
    common = ni.index.intersection(da.index).intersection(capex.index)
    if len(common) < 2:
        return None
    oe = ni[common] + da[common] - capex[common].abs()
    v = yoy_growth(oe)
    return R(value=v) if v is not None else None


# ══════════════════════════════════════════════════════════════════════
# Section 12: 达利欧（跨期部分）
# ══════════════════════════════════════════════════════════════════════


@feature("l0.company.debt_growth_vs_revenue_growth", FeatureLevel.L0, "company")
def debt_growth_vs_revenue_growth(ctx: ComputeContext) -> FeatureResult | None:
    """总债务增速 - 收入增速。"""
    debt = get_item_series(ctx, "total_debt", 8)
    rev = get_item_series(ctx, "revenue", 8)
    d_g = yoy_growth(debt)
    r_g = yoy_growth(rev)
    if d_g is None or r_g is None:
        return None
    return R(value=d_g - r_g)


# ══════════════════════════════════════════════════════════════════════
# Section 14: 索罗斯·反身性（跨期部分）
# ══════════════════════════════════════════════════════════════════════


@feature("l0.company.share_dilution_rate", FeatureLevel.L0, "company")
def share_dilution_rate(ctx: ComputeContext) -> FeatureResult | None:
    """(shares_t - shares_t-1) / shares_t-1"""
    shares = get_item_series(ctx, "basic_weighted_average_shares", 8)
    if len(shares) < 2:
        return None
    current = shares.iloc[-1]
    prior = shares.iloc[-2]
    if prior == 0:
        return None
    return R(value=(current - prior) / prior)


@feature("l0.company.leverage_acceleration", FeatureLevel.L0, "company")
def leverage_acceleration(ctx: ComputeContext) -> FeatureResult | None:
    """杠杆加速度 = debt_growth_t - debt_growth_t-1（二阶导）。"""
    debt = get_item_series(ctx, "total_debt", 12)
    if len(debt) < 3:
        return None
    growth = debt.pct_change().dropna()
    if len(growth) < 2:
        return None
    accel = growth.iloc[-1] - growth.iloc[-2]
    return R(value=float(accel))
