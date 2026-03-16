"""
V1 规则：巴菲特流派 — 内在价值
================================
核心问题：这门生意本身好不好？值不值得永久持有？

8 个维度的规则覆盖：
1. 商业模式  2. 护城河  3. 所有者盈余  4. 盈利质量
5. 资本配置  6. 管理层  7. 可预测性  8. 财务安全
"""

from polaris.scoring.dimensions import School
from polaris.scoring.rules import rule


def _get(features: dict[str, float], key: str) -> float | None:
    return features.get(f"l0.company.{key}")


# ── 1. 商业模式 ────────────────────────────────────────────────────


@rule("recurring_revenue", School.BUFFETT, "经常性收入占比 > 50%（收入可预测）")
def recurring_revenue(f: dict[str, float]) -> float:
    v = _get(f, "recurring_revenue_pct")
    return 2.0 if v is not None and v > 0.5 else 0.0


@rule("customer_concentration_risk", School.BUFFETT, "最大客户占比 > 30%（收入集中风险）")
def customer_concentration_risk(f: dict[str, float]) -> float:
    v = _get(f, "top_customer_concentration")
    return -1.5 if v is not None and v > 0.30 else 0.0


@rule("top3_concentration_risk", School.BUFFETT, "前 3 客户 > 60%（高度集中）")
def top3_concentration_risk(f: dict[str, float]) -> float:
    v = _get(f, "top3_customer_concentration")
    return -1.0 if v is not None and v > 0.60 else 0.0


@rule("high_switching_cost", School.BUFFETT, "客户转换成本高（粘性强）")
def high_switching_cost(f: dict[str, float]) -> float:
    v = _get(f, "switching_cost_indicator")
    return 1.5 if v is not None and v >= 2.5 else 0.0


@rule("strong_backlog", School.BUFFETT, "积压订单 > 年收入（可见性好）")
def strong_backlog(f: dict[str, float]) -> float:
    v = _get(f, "backlog_coverage")
    return 1.0 if v is not None and v > 1.0 else 0.0


# ── 2. 护城河 ──────────────────────────────────────────────────────


@rule("gross_margin_high", School.BUFFETT, "毛利率 > 40%（定价权 / 品牌溢价）")
def gross_margin_high(f: dict[str, float]) -> float:
    v = _get(f, "gross_margin")
    return 2.0 if v is not None and v > 0.40 else 0.0


@rule("gross_margin_low", School.BUFFETT, "毛利率 < 20%（无定价权 / 商品化）")
def gross_margin_low(f: dict[str, float]) -> float:
    v = _get(f, "gross_margin")
    return -2.0 if v is not None and v < 0.20 else 0.0


@rule("operating_margin_strong", School.BUFFETT, "营业利润率 > 20%（定价权强）")
def operating_margin_strong(f: dict[str, float]) -> float:
    v = _get(f, "operating_margin")
    return 1.5 if v is not None and v > 0.20 else 0.0


@rule("incremental_roic_high", School.BUFFETT, "增量 ROIC > 15%（增长创造价值）")
def incremental_roic_high(f: dict[str, float]) -> float:
    v = _get(f, "incremental_roic")
    return 2.0 if v is not None and v > 0.15 else 0.0


@rule("margin_expanding", School.BUFFETT, "毛利率连续扩张 > 4 期（护城河在加宽）")
def margin_expanding(f: dict[str, float]) -> float:
    v = _get(f, "consecutive_margin_expansion")
    return 1.0 if v is not None and v > 4 else 0.0


@rule("stable_gross_margin", School.BUFFETT, "毛利率标准差 < 0.03（定价权稳定）")
def stable_gross_margin(f: dict[str, float]) -> float:
    v = _get(f, "gross_margin_stability")
    return 1.0 if v is not None and v < 0.03 else 0.0


# ── 3. 所有者盈余 ──────────────────────────────────────────────────


@rule("light_asset", School.BUFFETT, "OE/NI > 1（轻资本模式）")
def light_asset(f: dict[str, float]) -> float:
    v = _get(f, "owner_earnings_to_net_income")
    return 1.5 if v is not None and v > 1.0 else 0.0


@rule("heavy_asset", School.BUFFETT, "OE/NI < 0.5（重资本，需大量维持性投入）")
def heavy_asset(f: dict[str, float]) -> float:
    v = _get(f, "owner_earnings_to_net_income")
    return -1.5 if v is not None and v < 0.5 else 0.0


@rule("low_capex_intensity", School.BUFFETT, "capex/revenue < 5%（低资本消耗）")
def low_capex_intensity(f: dict[str, float]) -> float:
    v = _get(f, "capex_to_revenue")
    return 1.0 if v is not None and v < 0.05 else 0.0


# ── 4. 盈利质量 ────────────────────────────────────────────────────


@rule("cash_flow_quality", School.BUFFETT, "OCF/NI > 0.8（利润有现金背书）")
def cash_flow_quality(f: dict[str, float]) -> float:
    v = _get(f, "ocf_to_net_income")
    return 2.0 if v is not None and v > 0.8 else 0.0


@rule("cash_flow_divergence", School.BUFFETT, "OCF/NI < 0.5（利润可能不真实）")
def cash_flow_divergence(f: dict[str, float]) -> float:
    v = _get(f, "ocf_to_net_income")
    return -2.0 if v is not None and v < 0.5 else 0.0


@rule("high_accruals", School.BUFFETT, "应计比率 > 10%（利润质量可疑）")
def high_accruals(f: dict[str, float]) -> float:
    v = _get(f, "accruals_ratio")
    return -1.5 if v is not None and v > 0.10 else 0.0


@rule("receivables_outpacing_revenue", School.BUFFETT, "应收增速 > 收入增速 10pp（赊账堆收入）")
def receivables_outpacing_revenue(f: dict[str, float]) -> float:
    v = _get(f, "receivables_growth_vs_revenue")
    return -1.0 if v is not None and v > 0.10 else 0.0


@rule("audit_concern", School.BUFFETT, "非标准审计意见（重大红旗）")
def audit_concern(f: dict[str, float]) -> float:
    v = _get(f, "audit_opinion_type")
    if v is not None and v > 1:  # 非 unqualified
        return -3.0
    return 0.0


@rule("net_margin_positive", School.BUFFETT, "净利率 > 10%（赚钱能力）")
def net_margin_positive(f: dict[str, float]) -> float:
    v = _get(f, "net_margin")
    return 1.0 if v is not None and v > 0.10 else 0.0


# ── 5. 资本配置 ────────────────────────────────────────────────────


@rule("good_shareholder_return", School.BUFFETT, "股东回报率 > 50%（回报股东）")
def good_shareholder_return(f: dict[str, float]) -> float:
    v = _get(f, "shareholder_yield")
    return 1.5 if v is not None and v > 0.50 else 0.0


@rule("excessive_acquisitions", School.BUFFETT, "收购支出 > OCF（收购狂人）")
def excessive_acquisitions(f: dict[str, float]) -> float:
    v = _get(f, "acquisition_spend_to_ocf")
    return -1.5 if v is not None and v > 1.0 else 0.0


@rule("goodwill_inflating", School.BUFFETT, "商誉增速 > 收入增速 20pp（高价收购）")
def goodwill_inflating(f: dict[str, float]) -> float:
    v = _get(f, "goodwill_growth_vs_revenue_growth")
    return -1.0 if v is not None and v > 0.20 else 0.0


# ── 6. 管理层 ──────────────────────────────────────────────────────


@rule("promises_kept", School.BUFFETT, "承诺兑现率 > 70%（管理层诚信）")
def promises_kept(f: dict[str, float]) -> float:
    v = _get(f, "narrative_fulfillment_rate")
    return 2.0 if v is not None and v > 0.70 else 0.0


@rule("promises_broken", School.BUFFETT, "承诺兑现率 < 30%（管理层不可信）")
def promises_broken(f: dict[str, float]) -> float:
    v = _get(f, "narrative_fulfillment_rate")
    return -2.0 if v is not None and v < 0.30 else 0.0


@rule("issues_acknowledged", School.BUFFETT, "问题回应率 > 70%（坦诚面对问题）")
def issues_acknowledged(f: dict[str, float]) -> float:
    v = _get(f, "issue_acknowledgment_rate")
    return 1.0 if v is not None and v > 0.70 else 0.0


@rule("insider_dumping", School.BUFFETT, "内部人卖出/买入 > 5（大量抛售）")
def insider_dumping(f: dict[str, float]) -> float:
    v = _get(f, "insider_selling_vs_buying")
    return -1.5 if v is not None and v > 5.0 else 0.0


@rule("ceo_pay_excessive", School.BUFFETT, "CEO Pay Ratio > 300（薪酬过高）")
def ceo_pay_excessive(f: dict[str, float]) -> float:
    v = _get(f, "ceo_pay_ratio")
    return -1.0 if v is not None and v > 300 else 0.0


@rule("mgmt_skin_in_game", School.BUFFETT, "管理层持股 > 5%（利益对齐）")
def mgmt_skin_in_game(f: dict[str, float]) -> float:
    v = _get(f, "mgmt_ownership_pct")
    return 1.0 if v is not None and v > 5.0 else 0.0


@rule("low_litigation", School.BUFFETT, "无进行中诉讼（治理良好）")
def low_litigation(f: dict[str, float]) -> float:
    v = _get(f, "litigation_count")
    return 1.0 if v is not None and v == 0 else 0.0


# ── 7. 可预测性 ────────────────────────────────────────────────────


@rule("very_stable_margins", School.BUFFETT, "毛利率标准差 < 0.03 + ROE 标准差 < 0.05")
def very_stable_margins(f: dict[str, float]) -> float:
    gm_s = _get(f, "gross_margin_stability")
    roe_s = _get(f, "roe_stability")
    if gm_s is not None and gm_s < 0.03 and roe_s is not None and roe_s < 0.05:
        return 2.0
    return 0.0


@rule("long_growth_streak", School.BUFFETT, "收入连续增长 > 8 期（高可预测性）")
def long_growth_streak(f: dict[str, float]) -> float:
    v = _get(f, "consecutive_revenue_growth")
    return 2.0 if v is not None and v > 8 else 0.0


@rule("consistent_fcf", School.BUFFETT, "FCF 连续为正 > 8 期（现金造血稳定）")
def consistent_fcf(f: dict[str, float]) -> float:
    v = _get(f, "consecutive_positive_fcf")
    return 1.5 if v is not None and v > 8 else 0.0


@rule("high_roe", School.BUFFETT, "ROE > 15%（优秀回报）")
def high_roe(f: dict[str, float]) -> float:
    v = _get(f, "roe")
    return 1.5 if v is not None and v > 0.15 else 0.0


# ── 8. 财务安全 ────────────────────────────────────────────────────


@rule("over_leveraged", School.BUFFETT, "债务/权益 > 2（杠杆过高）")
def over_leveraged(f: dict[str, float]) -> float:
    v = _get(f, "debt_to_equity")
    return -2.0 if v is not None and v > 2.0 else 0.0


@rule("low_interest_coverage", School.BUFFETT, "利息覆盖率 < 3（偿债压力）")
def low_interest_coverage(f: dict[str, float]) -> float:
    v = _get(f, "interest_coverage")
    return -2.0 if v is not None and v < 3.0 else 0.0


@rule("liquidity_tight", School.BUFFETT, "流动比率 < 1（短期偿债风险）")
def liquidity_tight(f: dict[str, float]) -> float:
    v = _get(f, "current_ratio")
    return -1.5 if v is not None and v < 1.0 else 0.0


@rule("slow_debt_repay", School.BUFFETT, "债务/OE > 5 年（还债太慢）")
def slow_debt_repay(f: dict[str, float]) -> float:
    v = _get(f, "debt_to_owner_earnings")
    return -1.0 if v is not None and v > 5.0 else 0.0
