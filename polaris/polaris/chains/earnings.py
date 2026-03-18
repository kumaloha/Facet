"""
盈余能力检测
============
核心问题: 这门生意赚不赚得到真金白银？

两层检测:
  1. 支出分类
     - 主动投资（好的）: CAPEX、研发 → 投了有没有回报？
     - 被动支出（坏的）: 债务偿还、借新还旧 → 越多越差

  2. 利润真实性
     现金流 - 主动投资 = 调整后现金流
     调整后现金流 vs 利润 → 对得上就没问题

输出: EarningsResult
"""

from __future__ import annotations

from dataclasses import dataclass, field

from polaris.features.types import ComputeContext


@dataclass
class SpendingItem:
    """一项支出。"""
    name: str
    amount: float | None = None
    category: str = ""  # active_investment / passive_burden
    observation: str = ""
    healthy: bool | None = None  # True=健康 / False=有问题 / None=待定


@dataclass
class InvestmentReturn:
    """主动投资的回报验证。"""
    investment_type: str  # capex / rnd
    invested: float | None = None
    return_metric: str = ""
    return_value: float | None = None
    verdict: str = ""  # good / bad / pending / no_data


@dataclass
class EarningsResult:
    # 支出分类
    active_investments: list[SpendingItem] = field(default_factory=list)
    passive_burdens: list[SpendingItem] = field(default_factory=list)

    # 投资回报验证
    investment_returns: list[InvestmentReturn] = field(default_factory=list)

    # 利润真实性
    ocf: float | None = None
    active_total: float | None = None
    adjusted_cf: float | None = None  # OCF - 主动投资
    net_income: float | None = None
    profit_real: bool | None = None  # 调整后现金流 vs 利润是否对得上
    profit_detail: str = ""

    # 所有者盈余
    owner_earnings: float | None = None

    # 趋势稳定性
    stability_signals: list[str] = field(default_factory=list)
    trend: str = ""  # improving / stable / deteriorating / no_data

    # 综合
    verdict: str = ""  # holds / breaks / unclear
    summary: str = ""


def _feat(ctx: ComputeContext, key: str) -> float | None:
    return ctx.features.get(f"l0.company.{key}")


def _item(ctx: ComputeContext, key: str) -> float | None:
    """从 financial_line_items 直接取值。"""
    df = ctx.get_financial_line_items()
    if df.empty:
        return None
    matches = df[df["item_key"] == key]
    return float(matches.iloc[0]["value"]) if not matches.empty else None


def assess_earnings(ctx: ComputeContext) -> EarningsResult:
    r = EarningsResult()

    ocf = _item(ctx, "operating_cash_flow")
    ni = _item(ctx, "net_income")
    capex = _item(ctx, "capital_expenditures")
    da = _item(ctx, "depreciation_amortization")
    rnd = _item(ctx, "rnd_expense")
    interest = _item(ctx, "interest_expense")
    debt_proceeds = _item(ctx, "proceeds_from_debt_issuance")
    revenue = _item(ctx, "revenue")

    r.ocf = ocf
    r.net_income = ni

    if capex is not None:
        capex = abs(capex)  # capex 有时存为负数

    # ══════════════════════════════════════════════════════════
    #  1. 支出分类
    # ══════════════════════════════════════════════════════════

    # ── 主动投资 ──

    if capex is not None:
        capex_rev = capex / revenue if revenue and revenue > 0 else None
        r.active_investments.append(SpendingItem(
            name="CAPEX",
            amount=capex,
            category="active_investment",
            observation=f"CAPEX {capex:,.0f}" + (f" ({capex_rev:.1%} of revenue)" if capex_rev else ""),
            healthy=None,  # 待验证回报
        ))

    if rnd is not None and rnd > 0:
        rnd_rev = rnd / revenue if revenue and revenue > 0 else None
        r.active_investments.append(SpendingItem(
            name="研发",
            amount=rnd,
            category="active_investment",
            observation=f"研发 {rnd:,.0f}" + (f" ({rnd_rev:.1%} of revenue)" if rnd_rev else ""),
            healthy=None,  # 待验证回报
        ))

    # ── 被动支出 ──

    if interest is not None and interest > 0:
        # 利息负担
        interest_coverage = _feat(ctx, "interest_coverage")
        healthy = True if (interest_coverage and interest_coverage > 5) else (
            False if (interest_coverage and interest_coverage < 2) else None)
        r.passive_burdens.append(SpendingItem(
            name="利息支出",
            amount=interest,
            category="passive_burden",
            observation=f"利息 {interest:,.0f}" + (
                f"，利息覆盖率 {interest_coverage:.1f}x" if interest_coverage else ""),
            healthy=healthy,
        ))

    # 借新还旧检测
    if debt_proceeds is not None and debt_proceeds > 0 and interest is not None:
        # 新发债同时有大量利息 → 借新还旧嫌疑
        total_debt = _item(ctx, "total_debt")
        debt_growth = _feat(ctx, "debt_growth_vs_revenue_growth")

        is_refinancing = False
        if total_debt and ocf and total_debt > 0:
            # 如果新债接近偿债额，且总债务没有明显增长用于扩张
            if debt_growth is not None and debt_growth > 0.05:
                # 债务增速超过收入增速 → 债务在膨胀
                is_refinancing = True

        r.passive_burdens.append(SpendingItem(
            name="新增债务",
            amount=debt_proceeds,
            category="passive_burden",
            observation=f"新增借债 {debt_proceeds:,.0f}" + (
                "，债务增速高于收入增速 → 借新还旧嫌疑" if is_refinancing else ""),
            healthy=False if is_refinancing else None,
        ))

    # 债务总体压力
    de = _feat(ctx, "debt_to_equity")
    net_debt_ebitda = _feat(ctx, "net_debt_to_ebitda")
    if de is not None and de > 2.0:
        r.passive_burdens.append(SpendingItem(
            name="高杠杆",
            amount=None,
            category="passive_burden",
            observation=f"D/E = {de:.1f}，杠杆过高",
            healthy=False,
        ))
    if net_debt_ebitda is not None and net_debt_ebitda > 4.0:
        r.passive_burdens.append(SpendingItem(
            name="偿债压力",
            amount=None,
            category="passive_burden",
            observation=f"净债务/EBITDA = {net_debt_ebitda:.1f}，偿债压力大",
            healthy=False,
        ))

    # ══════════════════════════════════════════════════════════
    #  2. 验证主动投资的回报
    # ══════════════════════════════════════════════════════════

    # CAPEX 回报: incremental ROIC
    inc_roic = _feat(ctx, "incremental_roic")
    if capex is not None:
        ret = InvestmentReturn(investment_type="capex", invested=capex)
        if inc_roic is not None:
            ret.return_metric = "incremental_roic"
            ret.return_value = inc_roic
            if inc_roic > 0.15:
                ret.verdict = "good"
                # 标记 CAPEX 为健康
                for s in r.active_investments:
                    if s.name == "CAPEX":
                        s.healthy = True
                        s.observation += f"，增量 ROIC = {inc_roic:.0%} → 投资有回报"
            elif inc_roic > 0:
                ret.verdict = "pending"
                for s in r.active_investments:
                    if s.name == "CAPEX":
                        s.observation += f"，增量 ROIC = {inc_roic:.0%} → 回报一般"
            else:
                ret.verdict = "bad"
                for s in r.active_investments:
                    if s.name == "CAPEX":
                        s.healthy = False
                        s.observation += f"，增量 ROIC = {inc_roic:.0%} → 投资没回报"
        else:
            ret.verdict = "no_data"
        r.investment_returns.append(ret)

    # 研发回报: 暂定合理，长期看收入增速
    if rnd is not None and rnd > 0:
        ret = InvestmentReturn(investment_type="rnd", invested=rnd)
        rev_growth = _feat(ctx, "revenue_growth_yoy")
        oe_growth = _feat(ctx, "owner_earnings_growth_yoy")
        if rev_growth is not None and rev_growth > 0:
            ret.return_metric = "revenue_growth"
            ret.return_value = rev_growth
            ret.verdict = "good"
            for s in r.active_investments:
                if s.name == "研发":
                    s.healthy = True
                    s.observation += f"，收入增速 {rev_growth:.1%} → 研发在产出"
        else:
            ret.verdict = "pending"
            for s in r.active_investments:
                if s.name == "研发":
                    s.observation += " → 暂定合理，待验证产出"
        r.investment_returns.append(ret)

    # ══════════════════════════════════════════════════════════
    #  3. 利润真实性
    # ══════════════════════════════════════════════════════════

    if ocf is not None:
        active_total = sum(s.amount for s in r.active_investments if s.amount is not None)
        r.active_total = active_total
        r.adjusted_cf = ocf - active_total

        # 主动投资是否已验证有回报
        investments_validated = all(
            s.healthy is True for s in r.active_investments if s.amount is not None
        ) and any(s.healthy is True for s in r.active_investments)

        if ni is not None and ni != 0:
            ratio = r.adjusted_cf / ni if ni != 0 else 0

            if ni > 0 and r.adjusted_cf > 0:
                if ratio > 0.5:
                    r.profit_real = True
                    r.profit_detail = (
                        f"调整后现金流 {r.adjusted_cf:,.0f} vs 利润 {ni:,.0f}"
                        f" (比率 {ratio:.2f})，利润有现金支撑")
                else:
                    r.profit_real = True
                    r.profit_detail = (
                        f"调整后现金流 {r.adjusted_cf:,.0f} vs 利润 {ni:,.0f}"
                        f" (比率 {ratio:.2f})，比率偏低但现金流仍为正")
            elif ni > 0 and r.adjusted_cf <= 0:
                # 区分: OCF 本身就差（纸面利润）vs OCF 还行但投资花了太多（投入期）
                ocf_ni_ratio = ocf / ni if ni != 0 else 0
                if ocf_ni_ratio < 0.5:
                    # OCF 本身远低于利润 → 利润质量差，不是投资的问题
                    r.profit_real = False
                    r.profit_detail = (
                        f"OCF {ocf:,.0f} 远低于利润 {ni:,.0f} (OCF/NI={ocf_ni_ratio:.2f})"
                        f"，利润缺乏现金支撑")
                elif investments_validated:
                    # OCF 尚可但主动投资大且有回报 → 投入期正常
                    r.profit_real = True
                    r.profit_detail = (
                        f"调整后现金流 {r.adjusted_cf:,.0f}（OCF/NI={ocf_ni_ratio:.2f} 尚可，"
                        f"主动投资 {active_total:,.0f} 已验证有回报，处于投入期）")
                else:
                    r.profit_real = False
                    r.profit_detail = (
                        f"利润 {ni:,.0f} 但调整后现金流 {r.adjusted_cf:,.0f}"
                        f"，扣除投资后没钱且投资回报未验证")
            elif ni <= 0:
                r.profit_real = False
                r.profit_detail = f"净利润 {ni:,.0f}，亏损"

    # ── 所有者盈余 ──
    oe = _feat(ctx, "owner_earnings")
    r.owner_earnings = oe

    # ══════════════════════════════════════════════════════════
    #  4. 趋势稳定性
    # ══════════════════════════════════════════════════════════

    positive_trend = 0
    negative_trend = 0

    # 现金流稳定性
    ocf_stab = _feat(ctx, "ocf_margin_stability")
    if ocf_stab is not None:
        if ocf_stab < 0.03:
            r.stability_signals.append(f"现金流率标准差 {ocf_stab:.4f}，极稳定")
            positive_trend += 1
        elif ocf_stab > 0.10:
            r.stability_signals.append(f"现金流率标准差 {ocf_stab:.4f}，波动大")
            negative_trend += 1

    # ROE 稳定性
    roe_stab = _feat(ctx, "roe_stability")
    if roe_stab is not None:
        if roe_stab < 0.03:
            r.stability_signals.append(f"ROE 标准差 {roe_stab:.4f}，极稳定")
            positive_trend += 1
        elif roe_stab > 0.08:
            r.stability_signals.append(f"ROE 标准差 {roe_stab:.4f}，波动大")
            negative_trend += 1

    # FCF 连续性
    consec_fcf = _feat(ctx, "consecutive_positive_fcf")
    if consec_fcf is not None:
        if consec_fcf >= 3:
            r.stability_signals.append(f"FCF 连续 {consec_fcf:.0f} 期为正")
            positive_trend += 1
        elif consec_fcf == 0:
            r.stability_signals.append(f"FCF 不稳定")
            negative_trend += 1

    # OE 增速
    oe_growth = _feat(ctx, "owner_earnings_growth_yoy")
    if oe_growth is not None:
        if oe_growth > 0.05:
            r.stability_signals.append(f"所有者盈余增速 {oe_growth:+.1%}")
            positive_trend += 1
        elif oe_growth < -0.10:
            r.stability_signals.append(f"所有者盈余增速 {oe_growth:+.1%}，在恶化")
            negative_trend += 1

    if negative_trend > positive_trend:
        r.trend = "deteriorating"
    elif positive_trend >= 2:
        r.trend = "stable"
    elif positive_trend > 0:
        r.trend = "stable"
    else:
        r.trend = "no_data"

    # ══════════════════════════════════════════════════════════
    #  综合判定
    # ══════════════════════════════════════════════════════════

    bad_investments = [s for s in r.active_investments if s.healthy is False]
    bad_burdens = [s for s in r.passive_burdens if s.healthy is False]

    # 逻辑顺序: 先看投资回报 → 再看利润真实性 → 对不上再找原因（被动支出）

    if bad_investments:
        # 主动投资没回报 → 在浪费钱
        r.verdict = "breaks"
        r.summary = "投资没回报: " + "; ".join(s.observation for s in bad_investments)
    elif r.profit_real is False:
        # 利润和调整后现金流对不上 → 找原因
        if bad_burdens:
            r.verdict = "breaks"
            r.summary = r.profit_detail + "。原因: " + "; ".join(s.observation for s in bad_burdens)
        else:
            r.verdict = "breaks"
            r.summary = r.profit_detail
    elif r.profit_real is True:
        if r.trend == "deteriorating":
            r.verdict = "breaks"
            r.summary = "当期利润真实但盈余能力在恶化: " + "; ".join(r.stability_signals)
        else:
            r.verdict = "holds"
            if oe and oe > 0:
                r.summary = f"所有者盈余 {oe:,.0f}，利润真实"
            else:
                r.summary = "利润真实"
            if r.trend == "stable" and r.stability_signals:
                r.summary += f"，趋势稳定"
            if bad_burdens:
                r.summary += f"（但有 {len(bad_burdens)} 项被动支出需关注）"
    elif ocf is None or ni is None:
        r.verdict = "unclear"
        r.summary = "数据不足"
    else:
        r.verdict = "unclear"
        r.summary = "证据不充分"

    return r


# ══════════════════════════════════════════════════════════════
#  格式化
# ══════════════════════════════════════════════════════════════

def format_earnings(result: EarningsResult) -> str:
    lines = [""]
    lines.append("  盈余能力检测")
    lines.append("  ════════════════════════════════════════════════")

    # 现金流概览
    if result.ocf is not None:
        lines.append(f"\n  经营现金流: {result.ocf:,.0f}")
        if result.net_income is not None:
            lines.append(f"  净利润:     {result.net_income:,.0f}")

    # 主动投资
    if result.active_investments:
        lines.append(f"\n  ▸ 主动投资（好的支出）")
        for s in result.active_investments:
            mark = "●" if s.healthy is True else ("✗" if s.healthy is False else "·")
            lines.append(f"    {mark} {s.observation}")

    # 投资回报验证
    for ret in result.investment_returns:
        if ret.verdict == "good":
            lines.append(f"      → {ret.investment_type} 回报验证: 通过")
        elif ret.verdict == "bad":
            lines.append(f"      → {ret.investment_type} 回报验证: 失败")
        elif ret.verdict == "pending":
            lines.append(f"      → {ret.investment_type} 回报验证: 待观察")

    # 被动支出
    if result.passive_burdens:
        lines.append(f"\n  ▸ 被动支出（坏的支出）")
        for s in result.passive_burdens:
            mark = "●" if s.healthy is True else ("✗" if s.healthy is False else "·")
            lines.append(f"    {mark} {s.observation}")

    # 利润真实性
    if result.profit_detail:
        lines.append(f"\n  ▸ 利润真实性")
        mark = "●" if result.profit_real is True else ("✗" if result.profit_real is False else "?")
        lines.append(f"    {mark} {result.profit_detail}")

    # 趋势稳定性
    if result.stability_signals:
        trend_labels = {"stable": "稳定", "deteriorating": "恶化", "no_data": "数据不足"}
        lines.append(f"\n  ▸ 趋势稳定性: {trend_labels.get(result.trend, result.trend)}")
        for s in result.stability_signals:
            lines.append(f"    · {s}")

    # 所有者盈余
    if result.owner_earnings is not None:
        lines.append(f"\n  所有者盈余: {result.owner_earnings:,.0f}")

    lines.append(f"\n  ════════════════════════════════════════════════")
    verdict_labels = {"holds": "成立", "breaks": "断裂", "unclear": "数据不足"}
    lines.append(f"  {verdict_labels.get(result.verdict, result.verdict)}: {result.summary}")
    lines.append("")
    return "\n".join(lines)
