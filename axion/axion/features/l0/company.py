"""
L0 单期特征
============
从 Anchor 表直接计算的单期特征。每个特征只需要当期数据。
跨期特征（stability, growth, consecutive）在 cross_period.py 中。
"""

from axion.features.l0._helpers import get_item, ratio, safe_div
from axion.features.registry import feature
from axion.features.types import ComputeContext, FeatureLevel, FeatureResult

R = FeatureResult  # shorthand


# ══════════════════════════════════════════════════════════════════════
# Section 1: 商业模式质量
# ══════════════════════════════════════════════════════════════════════


@feature("l0.company.recurring_revenue_pct", FeatureLevel.L0, "company")
def recurring_revenue_pct(ctx: ComputeContext) -> FeatureResult | None:
    """经常性收入占比（按金额加权）。"""
    df = ctx.get_downstream_segments()
    if df.empty or "is_recurring" not in df.columns:
        return None
    # 优先按 revenue_amount 加权；无金额时按 segment 数量
    if "revenue_amount" in df.columns and df["revenue_amount"].notna().any():
        valid = df[df["revenue_amount"].notna()]
        total = valid["revenue_amount"].sum()
        if total == 0:
            return None
        recurring = valid.loc[
            valid["is_recurring"].fillna(False), "revenue_amount"
        ].sum()
        pct = recurring / total
    else:
        total = len(df)
        recurring = df["is_recurring"].fillna(False).sum()
        pct = recurring / total if total > 0 else 0
    return R(value=pct, detail=f"recurring/total={pct:.2%}")


@feature("l0.company.top_customer_concentration", FeatureLevel.L0, "company")
def top_customer_concentration(ctx: ComputeContext) -> FeatureResult | None:
    """最大客户收入占比。"""
    df = ctx.get_downstream_segments()
    if df.empty or "revenue_pct" not in df.columns:
        return None
    valid = df[df["revenue_pct"].notna()]
    if valid.empty:
        return None
    return R(value=float(valid["revenue_pct"].max()))


@feature("l0.company.top3_customer_concentration", FeatureLevel.L0, "company")
def top3_customer_concentration(ctx: ComputeContext) -> FeatureResult | None:
    """前 3 大客户收入占比之和。"""
    df = ctx.get_downstream_segments()
    if df.empty or "revenue_pct" not in df.columns:
        return None
    valid = df[df["revenue_pct"].notna()].sort_values("revenue_pct", ascending=False)
    if valid.empty:
        return None
    return R(value=float(valid["revenue_pct"].head(3).sum()))


@feature("l0.company.revenue_type_diversity", FeatureLevel.L0, "company")
def revenue_type_diversity(ctx: ComputeContext) -> FeatureResult | None:
    """不同收入类型的数量。"""
    df = ctx.get_downstream_segments()
    if df.empty or "revenue_type" not in df.columns:
        return None
    types = df["revenue_type"].dropna().nunique()
    return R(value=float(types))


@feature("l0.company.backlog_coverage", FeatureLevel.L0, "company")
def backlog_coverage(ctx: ComputeContext) -> FeatureResult | None:
    """积压订单总额 / 年收入。"""
    df = ctx.get_downstream_segments()
    if df.empty or "backlog" not in df.columns:
        return None
    backlog = df["backlog"].dropna().sum()
    revenue = get_item(ctx, "revenue")
    v = safe_div(backlog, revenue)
    return R(value=v) if v is not None else None


@feature("l0.company.contract_duration_avg", FeatureLevel.L0, "company")
def contract_duration_avg(ctx: ComputeContext) -> FeatureResult | None:
    """客户合同平均时长（月）。"""
    df = ctx.get_downstream_segments()
    if df.empty or "contract_duration_months" not in df.columns:
        return None
    valid = df["contract_duration_months"].dropna()
    if valid.empty:
        return None
    return R(value=float(valid.mean()))


@feature("l0.company.switching_cost_indicator", FeatureLevel.L0, "company")
def switching_cost_indicator(ctx: ComputeContext) -> FeatureResult | None:
    """客户转换成本水平（high=3, medium=2, low=1）。"""
    df = ctx.get_downstream_segments()
    if df.empty or "switching_cost_level" not in df.columns:
        return None
    mapping = {"high": 3.0, "medium": 2.0, "low": 1.0}
    valid = df["switching_cost_level"].dropna().map(mapping).dropna()
    if valid.empty:
        return None
    return R(value=float(valid.mean()))


# ══════════════════════════════════════════════════════════════════════
# Section 2: 护城河（单期部分）
# ══════════════════════════════════════════════════════════════════════


@feature("l0.company.gross_margin", FeatureLevel.L0, "company")
def gross_margin(ctx: ComputeContext) -> FeatureResult | None:
    """毛利率 = (revenue - cost_of_revenue) / revenue"""
    revenue = get_item(ctx, "revenue")
    cogs = get_item(ctx, "cost_of_revenue")
    if revenue is None or cogs is None or revenue == 0:
        return None
    gm = (revenue - cogs) / revenue
    return R(value=gm, detail=f"revenue={revenue:.0f}, cogs={cogs:.0f}")


@feature("l0.company.operating_margin", FeatureLevel.L0, "company")
def operating_margin(ctx: ComputeContext) -> FeatureResult | None:
    """营业利润率 = operating_income / revenue"""
    v = ratio(ctx, "operating_income", "revenue")
    return R(value=v) if v is not None else None


@feature("l0.company.price_increase_history", FeatureLevel.L0, "company")
def price_increase_history(ctx: ComputeContext) -> FeatureResult | None:
    """近期提价次数。"""
    df = ctx.get_pricing_actions()
    if df.empty:
        return None
    increases = df[df["price_change_pct"] > 0]
    return R(value=float(len(increases)))


@feature("l0.company.market_share_stability", FeatureLevel.L0, "company")
def market_share_stability(ctx: ComputeContext) -> FeatureResult | None:
    """公司市占率近 N 期标准差。"""
    df = ctx.get_market_share_data()
    if df.empty or "share_pct" not in df.columns:
        return None
    series = df["share_pct"].dropna()
    if len(series) < 3:
        return None
    return R(value=float(series.std()))


# ══════════════════════════════════════════════════════════════════════
# Section 3: 所有者盈余与资本轻重
# ══════════════════════════════════════════════════════════════════════


@feature("l0.company.owner_earnings", FeatureLevel.L0, "company")
def owner_earnings(ctx: ComputeContext) -> FeatureResult | None:
    """所有者盈余 = net_income + D&A - capex"""
    ni = get_item(ctx, "net_income")
    da = get_item(ctx, "depreciation_amortization")
    capex = get_item(ctx, "capital_expenditures")
    if ni is None or da is None or capex is None:
        return None
    oe = ni + da - abs(capex)
    return R(value=oe, detail=f"ni={ni:.0f}+da={da:.0f}-capex={abs(capex):.0f}")


@feature("l0.company.owner_earnings_margin", FeatureLevel.L0, "company")
def owner_earnings_margin(ctx: ComputeContext) -> FeatureResult | None:
    """所有者盈余率 = owner_earnings / revenue"""
    ni = get_item(ctx, "net_income")
    da = get_item(ctx, "depreciation_amortization")
    capex = get_item(ctx, "capital_expenditures")
    revenue = get_item(ctx, "revenue")
    if ni is None or da is None or capex is None or revenue is None or revenue == 0:
        return None
    oe = ni + da - abs(capex)
    return R(value=oe / revenue)


@feature("l0.company.owner_earnings_to_net_income", FeatureLevel.L0, "company")
def owner_earnings_to_net_income(ctx: ComputeContext) -> FeatureResult | None:
    """OE/NI：>1 轻资本，<1 重资本。"""
    ni = get_item(ctx, "net_income")
    da = get_item(ctx, "depreciation_amortization")
    capex = get_item(ctx, "capital_expenditures")
    if ni is None or da is None or capex is None or ni == 0:
        return None
    oe = ni + da - abs(capex)
    return R(value=oe / ni)


@feature("l0.company.capex_to_revenue", FeatureLevel.L0, "company")
def capex_to_revenue(ctx: ComputeContext) -> FeatureResult | None:
    """资本开支 / 收入。"""
    capex = get_item(ctx, "capital_expenditures")
    revenue = get_item(ctx, "revenue")
    if capex is None or revenue is None or revenue == 0:
        return None
    return R(value=abs(capex) / revenue)


@feature("l0.company.capex_to_ocf", FeatureLevel.L0, "company")
def capex_to_ocf(ctx: ComputeContext) -> FeatureResult | None:
    """资本开支 / 经营现金流。"""
    capex = get_item(ctx, "capital_expenditures")
    ocf = get_item(ctx, "operating_cash_flow")
    if capex is None or ocf is None or ocf == 0:
        return None
    return R(value=abs(capex) / ocf)


@feature("l0.company.depreciation_to_capex", FeatureLevel.L0, "company")
def depreciation_to_capex(ctx: ComputeContext) -> FeatureResult | None:
    """D&A / capex：>1 消化存量，<1 加速扩张。"""
    da = get_item(ctx, "depreciation_amortization")
    capex = get_item(ctx, "capital_expenditures")
    if da is None or capex is None or capex == 0:
        return None
    return R(value=da / abs(capex))


@feature("l0.company.maintenance_capex_ratio", FeatureLevel.L0, "company")
def maintenance_capex_ratio(ctx: ComputeContext) -> FeatureResult | None:
    """维持性 capex / 总 capex（无披露时用 D&A 近似）。"""
    mcapex = get_item(ctx, "maintenance_capex")
    capex = get_item(ctx, "capital_expenditures")
    if capex is None or capex == 0:
        return None
    if mcapex is not None:
        return R(value=mcapex / abs(capex))
    da = get_item(ctx, "depreciation_amortization")
    if da is not None:
        return R(value=da / abs(capex), detail="approximated with D&A")
    return None


# ══════════════════════════════════════════════════════════════════════
# Section 4: 盈利质量
# ══════════════════════════════════════════════════════════════════════


@feature("l0.company.ocf_to_net_income", FeatureLevel.L0, "company")
def ocf_to_net_income(ctx: ComputeContext) -> FeatureResult | None:
    """经营现金流 / 净利润 — 衡量利润真实性。"""
    ocf = get_item(ctx, "operating_cash_flow")
    ni = get_item(ctx, "net_income")
    if ocf is None or ni is None or ni == 0:
        return None
    return R(value=ocf / ni, detail=f"ocf={ocf:.0f}")


@feature("l0.company.accruals_ratio", FeatureLevel.L0, "company")
def accruals_ratio(ctx: ComputeContext) -> FeatureResult | None:
    """(net_income - OCF) / total_assets — 应计项目占比。"""
    ni = get_item(ctx, "net_income")
    ocf = get_item(ctx, "operating_cash_flow")
    ta = get_item(ctx, "total_assets")
    if ni is None or ocf is None or ta is None or ta == 0:
        return None
    return R(value=(ni - ocf) / ta)


@feature("l0.company.goodwill_to_assets", FeatureLevel.L0, "company")
def goodwill_to_assets(ctx: ComputeContext) -> FeatureResult | None:
    """商誉 / 总资产。"""
    v = ratio(ctx, "goodwill", "total_assets")
    return R(value=v) if v is not None else None


@feature("l0.company.related_party_amount_to_revenue", FeatureLevel.L0, "company")
def related_party_amount_to_revenue(ctx: ComputeContext) -> FeatureResult | None:
    """关联交易总额 / 收入。"""
    df = ctx.get_related_party_transactions()
    if df.empty or "amount" not in df.columns:
        return None
    total_rpt = df["amount"].dropna().sum()
    revenue = get_item(ctx, "revenue")
    v = safe_div(total_rpt, revenue)
    return R(value=v) if v is not None else None


@feature("l0.company.related_party_ongoing_count", FeatureLevel.L0, "company")
def related_party_ongoing_count(ctx: ComputeContext) -> FeatureResult | None:
    """持续性关联交易数量。"""
    df = ctx.get_related_party_transactions()
    if df.empty or "is_ongoing" not in df.columns:
        return None
    ongoing = df["is_ongoing"].fillna(False).sum()
    return R(value=float(ongoing))


@feature("l0.company.audit_opinion_type", FeatureLevel.L0, "company")
def audit_opinion_type(ctx: ComputeContext) -> FeatureResult | None:
    """审计意见类型（unqualified=1, qualified=2, adverse=3, disclaimer=4）。"""
    df = ctx.get_audit_opinions()
    if df.empty or "opinion_type" not in df.columns:
        return None
    mapping = {"unqualified": 1.0, "qualified": 2.0, "adverse": 3.0, "disclaimer": 4.0}
    opinion = df.iloc[0]["opinion_type"]
    v = mapping.get(opinion)
    return R(value=v, detail=opinion) if v is not None else None


# ══════════════════════════════════════════════════════════════════════
# Section 5: 资本配置
# ══════════════════════════════════════════════════════════════════════


@feature("l0.company.shareholder_yield", FeatureLevel.L0, "company")
def shareholder_yield(ctx: ComputeContext) -> FeatureResult | None:
    """(dividends + buybacks) / net_income"""
    div = get_item(ctx, "dividends_paid")
    buyback = get_item(ctx, "share_repurchase")
    ni = get_item(ctx, "net_income")
    if ni is None or ni == 0:
        return None
    d = abs(div) if div is not None else 0
    b = abs(buyback) if buyback is not None else 0
    if d == 0 and b == 0:
        return None
    return R(value=(d + b) / ni)


@feature("l0.company.dividend_payout_ratio", FeatureLevel.L0, "company")
def dividend_payout_ratio(ctx: ComputeContext) -> FeatureResult | None:
    """abs(dividends_paid) / net_income"""
    div = get_item(ctx, "dividends_paid")
    ni = get_item(ctx, "net_income")
    if div is None or ni is None or ni == 0:
        return None
    return R(value=abs(div) / ni)


@feature("l0.company.buyback_to_net_income", FeatureLevel.L0, "company")
def buyback_to_net_income(ctx: ComputeContext) -> FeatureResult | None:
    """abs(share_repurchase) / net_income"""
    buyback = get_item(ctx, "share_repurchase")
    ni = get_item(ctx, "net_income")
    if buyback is None or ni is None or ni == 0:
        return None
    return R(value=abs(buyback) / ni)


@feature("l0.company.acquisition_spend_to_ocf", FeatureLevel.L0, "company")
def acquisition_spend_to_ocf(ctx: ComputeContext) -> FeatureResult | None:
    """收购支出 / 经营现金流。"""
    acq = get_item(ctx, "acquisitions_net")
    ocf = get_item(ctx, "operating_cash_flow")
    if acq is None or ocf is None or ocf == 0:
        return None
    return R(value=abs(acq) / ocf)


# ══════════════════════════════════════════════════════════════════════
# Section 6: 管理层品格
# ══════════════════════════════════════════════════════════════════════


@feature("l0.company.narrative_fulfillment_rate", FeatureLevel.L0, "company")
def narrative_fulfillment_rate(ctx: ComputeContext) -> FeatureResult | None:
    """承诺兑现率 = delivered / (delivered + missed + abandoned)。"""
    df = ctx.get_company_narratives()
    if df.empty or "status" not in df.columns:
        return None
    terminal = df[df["status"].isin(["delivered", "missed", "abandoned"])]
    if terminal.empty:
        return None
    delivered = (terminal["status"] == "delivered").sum()
    return R(value=delivered / len(terminal), detail=f"{delivered}/{len(terminal)}")


@feature("l0.company.narrative_count", FeatureLevel.L0, "company")
def narrative_count(ctx: ComputeContext) -> FeatureResult | None:
    """管理层叙事/承诺总数。"""
    df = ctx.get_company_narratives()
    return R(value=float(len(df))) if not df.empty else None


@feature("l0.company.issue_acknowledgment_rate", FeatureLevel.L0, "company")
def issue_acknowledgment_rate(ctx: ComputeContext) -> FeatureResult | None:
    """管理层提及的问题数 / 全部已知问题数。"""
    issues = ctx.get_known_issues()
    acks = ctx.get_management_acknowledgments()
    if issues.empty:
        return None
    acked = len(acks[acks["known_issue_id"].notna()]) if not acks.empty and "known_issue_id" in acks.columns else 0
    return R(value=acked / len(issues), detail=f"{acked}/{len(issues)}")


@feature("l0.company.mgmt_ownership_pct", FeatureLevel.L0, "company")
def mgmt_ownership_pct(ctx: ComputeContext) -> FeatureResult | None:
    """管理层合计持股比例。"""
    df = ctx.get_stock_ownership()
    if df.empty or "percent_of_class" not in df.columns:
        return None
    mgmt = df[df["title"].notna()]
    if mgmt.empty:
        return None
    return R(value=float(mgmt["percent_of_class"].sum()))


@feature("l0.company.ceo_pay_ratio", FeatureLevel.L0, "company")
def ceo_pay_ratio(ctx: ComputeContext) -> FeatureResult | None:
    """CEO 薪酬 / 员工中位数。"""
    df = ctx.get_executive_compensations()
    if df.empty or "pay_ratio" not in df.columns:
        return None
    ceo = df[df["pay_ratio"].notna()]
    if ceo.empty:
        return None
    return R(value=float(ceo.iloc[0]["pay_ratio"]))


@feature("l0.company.exec_stock_award_pct", FeatureLevel.L0, "company")
def exec_stock_award_pct(ctx: ComputeContext) -> FeatureResult | None:
    """高管平均 stock_awards / total_comp。"""
    df = ctx.get_executive_compensations()
    if df.empty:
        return None
    for col in ("stock_awards", "total_comp"):
        if col not in df.columns:
            return None
    valid = df[(df["stock_awards"].notna()) & (df["total_comp"].notna()) & (df["total_comp"] > 0)]
    if valid.empty:
        return None
    ratios = valid["stock_awards"] / valid["total_comp"]
    return R(value=float(ratios.mean()))


@feature("l0.company.insider_selling_vs_buying", FeatureLevel.L0, "company")
def insider_selling_vs_buying(ctx: ComputeContext) -> FeatureResult | None:
    """内部人净卖出/净买入（>1 卖出为主，<1 买入为主）。"""
    df = ctx.get_insider_transactions()
    if df.empty or "transaction_type" not in df.columns:
        return None
    buys = df[df["transaction_type"] == "buy"]["shares"].sum()
    sells = df[df["transaction_type"] == "sell"]["shares"].sum()
    if buys == 0 and sells == 0:
        return None
    if buys == 0:
        return R(value=10.0, detail="all selling, no buying")
    return R(value=sells / buys)


@feature("l0.company.mgmt_turnover_rate", FeatureLevel.L0, "company")
def mgmt_turnover_rate(ctx: ComputeContext) -> FeatureResult | None:
    """高管团队离职率。"""
    df = ctx.get_executive_changes()
    if df.empty or "change_type" not in df.columns:
        return None
    departures = (df["change_type"] == "departed").sum()
    total = len(df)
    if total == 0:
        return None
    return R(value=departures / total)


# ══════════════════════════════════════════════════════════════════════
# Section 7: 可预测性（单期部分）
# ══════════════════════════════════════════════════════════════════════


@feature("l0.company.roe", FeatureLevel.L0, "company")
def roe(ctx: ComputeContext) -> FeatureResult | None:
    """净资产收益率 = net_income / shareholders_equity"""
    v = ratio(ctx, "net_income", "shareholders_equity")
    return R(value=v) if v is not None else None


@feature("l0.company.net_margin", FeatureLevel.L0, "company")
def net_margin(ctx: ComputeContext) -> FeatureResult | None:
    """净利率 = net_income / revenue"""
    v = ratio(ctx, "net_income", "revenue")
    return R(value=v) if v is not None else None


@feature("l0.company.free_cash_flow_margin", FeatureLevel.L0, "company")
def free_cash_flow_margin(ctx: ComputeContext) -> FeatureResult | None:
    """自由现金流率 = (OCF - capex) / revenue"""
    ocf = get_item(ctx, "operating_cash_flow")
    capex = get_item(ctx, "capital_expenditures")
    revenue = get_item(ctx, "revenue")
    if ocf is None or capex is None or revenue is None or revenue == 0:
        return None
    return R(value=(ocf - abs(capex)) / revenue)


# ══════════════════════════════════════════════════════════════════════
# Section 8: 财务安全
# ══════════════════════════════════════════════════════════════════════


@feature("l0.company.debt_to_equity", FeatureLevel.L0, "company")
def debt_to_equity(ctx: ComputeContext) -> FeatureResult | None:
    """total_debt / shareholders_equity"""
    v = ratio(ctx, "total_debt", "shareholders_equity")
    return R(value=v) if v is not None else None


@feature("l0.company.debt_to_owner_earnings", FeatureLevel.L0, "company")
def debt_to_owner_earnings(ctx: ComputeContext) -> FeatureResult | None:
    """total_debt / owner_earnings（几年能还清）。"""
    debt = get_item(ctx, "total_debt")
    ni = get_item(ctx, "net_income")
    da = get_item(ctx, "depreciation_amortization")
    capex = get_item(ctx, "capital_expenditures")
    if debt is None or ni is None or da is None or capex is None:
        return None
    oe = ni + da - abs(capex)
    if oe <= 0:
        return R(value=99.0, detail="OE <= 0, cannot repay")
    return R(value=debt / oe)


@feature("l0.company.interest_coverage", FeatureLevel.L0, "company")
def interest_coverage(ctx: ComputeContext) -> FeatureResult | None:
    """operating_income / interest_expense"""
    v = ratio(ctx, "operating_income", "interest_expense")
    return R(value=v) if v is not None else None


@feature("l0.company.current_ratio", FeatureLevel.L0, "company")
def current_ratio(ctx: ComputeContext) -> FeatureResult | None:
    """current_assets / current_liabilities"""
    v = ratio(ctx, "current_assets", "current_liabilities")
    return R(value=v) if v is not None else None


# ══════════════════════════════════════════════════════════════════════
# Section 12: 达利欧·公司脆弱度
# ══════════════════════════════════════════════════════════════════════


@feature("l0.company.debt_service_burden", FeatureLevel.L0, "company")
def debt_service_burden(ctx: ComputeContext) -> FeatureResult | None:
    """interest_expense / operating_cash_flow"""
    v = ratio(ctx, "interest_expense", "operating_cash_flow")
    return R(value=v) if v is not None else None


@feature("l0.company.net_debt_to_ebitda", FeatureLevel.L0, "company")
def net_debt_to_ebitda(ctx: ComputeContext) -> FeatureResult | None:
    """(total_debt - cash) / EBITDA"""
    debt = get_item(ctx, "total_debt")
    cash = get_item(ctx, "cash_and_equivalents")
    oi = get_item(ctx, "operating_income")
    da = get_item(ctx, "depreciation_amortization")
    if debt is None or oi is None or da is None:
        return None
    net_debt = debt - (cash or 0)
    ebitda = oi + da
    if ebitda <= 0:
        return R(value=99.0, detail="EBITDA <= 0")
    return R(value=net_debt / ebitda)


@feature("l0.company.current_debt_pct", FeatureLevel.L0, "company")
def current_debt_pct(ctx: ComputeContext) -> FeatureResult | None:
    """短期债务本金 / 总本金。"""
    df = ctx.get_debt_obligations()
    if df.empty or "principal" not in df.columns or "is_current" not in df.columns:
        return None
    total = df["principal"].sum()
    if total == 0:
        return None
    current = df.loc[df["is_current"].fillna(False), "principal"].sum()
    return R(value=current / total)


@feature("l0.company.weighted_avg_interest_rate", FeatureLevel.L0, "company")
def weighted_avg_interest_rate(ctx: ComputeContext) -> FeatureResult | None:
    """加权平均利率。"""
    df = ctx.get_debt_obligations()
    if df.empty:
        return None
    for col in ("principal", "interest_rate"):
        if col not in df.columns:
            return None
    valid = df[(df["principal"].notna()) & (df["interest_rate"].notna()) & (df["principal"] > 0)]
    if valid.empty:
        return None
    total_p = valid["principal"].sum()
    if total_p == 0:
        return None
    wavg = (valid["principal"] * valid["interest_rate"]).sum() / total_p
    return R(value=float(wavg))


@feature("l0.company.floating_rate_debt_pct", FeatureLevel.L0, "company")
def floating_rate_debt_pct(ctx: ComputeContext) -> FeatureResult | None:
    """浮动利率债务占比。"""
    df = ctx.get_debt_obligations()
    if df.empty or "is_floating_rate" not in df.columns or "principal" not in df.columns:
        return None
    total = df["principal"].sum()
    if total == 0:
        return None
    floating = df.loc[df["is_floating_rate"].fillna(False), "principal"].sum()
    return R(value=floating / total)


@feature("l0.company.refinancing_wall", FeatureLevel.L0, "company")
def refinancing_wall(ctx: ComputeContext) -> FeatureResult | None:
    """未来 2 年到期债务 / 经营现金流。"""
    df = ctx.get_debt_obligations()
    if df.empty or "maturity_date" not in df.columns or "principal" not in df.columns:
        return None
    ocf = get_item(ctx, "operating_cash_flow")
    if ocf is None or ocf == 0:
        return None
    # Filter for maturities within ~2 years of period (simplified)
    maturing = df[df["maturity_date"].notna()]["principal"].sum()
    return R(value=maturing / ocf, detail="all maturities / OCF")


@feature("l0.company.cash_to_short_term_debt", FeatureLevel.L0, "company")
def cash_to_short_term_debt(ctx: ComputeContext) -> FeatureResult | None:
    """cash / 短期债务。"""
    cash = get_item(ctx, "cash_and_equivalents")
    df = ctx.get_debt_obligations()
    if cash is None or df.empty or "is_current" not in df.columns:
        return None
    current_debt = df.loc[df["is_current"].fillna(False), "principal"].sum()
    if current_debt == 0:
        return R(value=99.0, detail="no current debt")
    return R(value=cash / current_debt)


@feature("l0.company.interest_to_revenue", FeatureLevel.L0, "company")
def interest_to_revenue(ctx: ComputeContext) -> FeatureResult | None:
    """interest_expense / revenue"""
    v = ratio(ctx, "interest_expense", "revenue")
    return R(value=v) if v is not None else None


@feature("l0.company.total_debt_count", FeatureLevel.L0, "company")
def total_debt_count(ctx: ComputeContext) -> FeatureResult | None:
    """债务工具数量。"""
    df = ctx.get_debt_obligations()
    if df.empty:
        return None
    return R(value=float(len(df)))


# ══════════════════════════════════════════════════════════════════════
# Section 14: 索罗斯·反身性强度（单期部分）
# ══════════════════════════════════════════════════════════════════════


@feature("l0.company.financing_dependency", FeatureLevel.L0, "company")
def financing_dependency(ctx: ComputeContext) -> FeatureResult | None:
    """(股权融资 + 新增借款) / OCF"""
    stock = get_item(ctx, "proceeds_from_stock_issuance")
    debt = get_item(ctx, "proceeds_from_debt_issuance")
    ocf = get_item(ctx, "operating_cash_flow")
    if ocf is None or ocf == 0:
        return None
    inflows = (stock or 0) + (debt or 0)
    if inflows == 0:
        return None
    return R(value=inflows / ocf)


@feature("l0.company.equity_issuance_to_capex", FeatureLevel.L0, "company")
def equity_issuance_to_capex(ctx: ComputeContext) -> FeatureResult | None:
    """股权融资 / capex"""
    stock = get_item(ctx, "proceeds_from_stock_issuance")
    capex = get_item(ctx, "capital_expenditures")
    if stock is None or capex is None or capex == 0:
        return None
    return R(value=stock / abs(capex))


@feature("l0.company.cash_burn_rate", FeatureLevel.L0, "company")
def cash_burn_rate(ctx: ComputeContext) -> FeatureResult | None:
    """现金消耗速度 = -FCF / cash（正值=在烧钱）。"""
    ocf = get_item(ctx, "operating_cash_flow")
    capex = get_item(ctx, "capital_expenditures")
    cash = get_item(ctx, "cash_and_equivalents")
    if ocf is None or capex is None or cash is None or cash == 0:
        return None
    fcf = ocf - abs(capex)
    return R(value=-fcf / cash)


@feature("l0.company.secondary_offering_count", FeatureLevel.L0, "company")
def secondary_offering_count(ctx: ComputeContext) -> FeatureResult | None:
    """增发次数。"""
    df = ctx.get_equity_offerings()
    if df.empty:
        return None
    return R(value=float(len(df)))


@feature("l0.company.debt_issuance_to_capex", FeatureLevel.L0, "company")
def debt_issuance_to_capex(ctx: ComputeContext) -> FeatureResult | None:
    """新增借款 / capex"""
    debt_iss = get_item(ctx, "proceeds_from_debt_issuance")
    capex = get_item(ctx, "capital_expenditures")
    if debt_iss is None or capex is None or capex == 0:
        return None
    return R(value=debt_iss / abs(capex))


# ══════════════════════════════════════════════════════════════════════
# 其他：供应链、地域、风险（保留原有特征）
# ══════════════════════════════════════════════════════════════════════


@feature("l0.company.sole_source_pct", FeatureLevel.L0, "company")
def sole_source_pct(ctx: ComputeContext) -> FeatureResult | None:
    """独家供应商占比。"""
    df = ctx.get_upstream_segments()
    if df.empty or "is_sole_source" not in df.columns:
        return None
    total = len(df)
    sole = df["is_sole_source"].fillna(False).sum()
    return R(value=sole / total if total > 0 else 0)


@feature("l0.company.top_region_concentration", FeatureLevel.L0, "company")
def top_region_concentration(ctx: ComputeContext) -> FeatureResult | None:
    """最大地区收入占比。"""
    df = ctx.get_geographic_revenues()
    if df.empty or "revenue_share" not in df.columns:
        return None
    valid = df[df["revenue_share"].notna()]
    if valid.empty:
        return None
    max_share = valid["revenue_share"].max()
    top_region = valid.loc[valid["revenue_share"].idxmax(), "region"]
    return R(value=float(max_share), detail=f"top: {top_region}")


@feature("l0.company.litigation_count", FeatureLevel.L0, "company")
def litigation_count(ctx: ComputeContext) -> FeatureResult | None:
    """进行中的诉讼数量。"""
    df = ctx.get_litigations()
    if df.empty:
        return R(value=0.0)
    active = df[df["status"].isin(["pending", "ongoing"])]
    return R(value=float(len(active)))


@feature("l0.company.operational_issue_count", FeatureLevel.L0, "company")
def operational_issue_count(ctx: ComputeContext) -> FeatureResult | None:
    """经营议题数量。"""
    df = ctx.get_operational_issues()
    return R(value=float(len(df)))


# ══════════════════════════════════════════════════════════════════════
# Section 13: 索罗斯·分析师偏差
# ══════════════════════════════════════════════════════════════════════


@feature("l0.company.analyst_surprise_pct", FeatureLevel.L0, "company")
def analyst_surprise_pct(ctx: ComputeContext) -> FeatureResult | None:
    """最近一期 (actual - consensus) / consensus。"""
    df = ctx.get_analyst_estimates()
    if df.empty:
        return None
    for col in ("actual", "consensus_estimate"):
        if col not in df.columns:
            return None
    valid = df[(df["actual"].notna()) & (df["consensus_estimate"].notna())]
    if valid.empty:
        return None
    last = valid.iloc[-1]
    est = last["consensus_estimate"]
    if est == 0:
        return None
    return R(value=(last["actual"] - est) / abs(est))


@feature("l0.company.consecutive_beats", FeatureLevel.L0, "company")
def consecutive_beats(ctx: ComputeContext) -> FeatureResult | None:
    """连续超预期的季度数。"""
    df = ctx.get_analyst_estimates()
    if df.empty:
        return None
    for col in ("actual", "consensus_estimate"):
        if col not in df.columns:
            return None
    valid = df[(df["actual"].notna()) & (df["consensus_estimate"].notna())]
    if valid.empty:
        return None
    count = 0
    for _, row in valid.iloc[::-1].iterrows():
        if row["actual"] > row["consensus_estimate"]:
            count += 1
        else:
            break
    return R(value=float(count))


@feature("l0.company.consecutive_misses", FeatureLevel.L0, "company")
def consecutive_misses(ctx: ComputeContext) -> FeatureResult | None:
    """连续低于预期的季度数。"""
    df = ctx.get_analyst_estimates()
    if df.empty:
        return None
    for col in ("actual", "consensus_estimate"):
        if col not in df.columns:
            return None
    valid = df[(df["actual"].notna()) & (df["consensus_estimate"].notna())]
    if valid.empty:
        return None
    count = 0
    for _, row in valid.iloc[::-1].iterrows():
        if row["actual"] < row["consensus_estimate"]:
            count += 1
        else:
            break
    return R(value=float(count))
