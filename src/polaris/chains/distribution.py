"""
利润分配检测
============
核心问题: 赚到的钱能到股东手里吗？

股东拿到钱的三条路:
  1. 分红 — 直接发现金
  2. 回购 — 注销股份，每股价值变大
  3. 留存再投资 — 每留存 1 块钱至少创造 1 块钱市场价值

检测逻辑:
  - 有分红或回购 → 真金白银到手
  - 有留存 → 验证留存回报（市值增长 vs 留存金额）
  - 既不分也不回购也不创造价值 → 钱困在公司里
  - 更差: 在稀释股东 / 自肥 / 帝国建设
"""

from __future__ import annotations

from dataclasses import dataclass, field

from polaris.features.types import ComputeContext


@dataclass
class DistributionResult:
    # 真金白银到手
    dividends: float | None = None
    buybacks: float | None = None
    total_returned: float | None = None      # 分红 + 回购
    return_ratio: float | None = None        # 回报/净利润
    cash_to_shareholder: bool | None = None  # 有没有真钱到股东手里
    cash_detail: str = ""

    # 留存回报
    retained: float | None = None            # 留存金额 = NI - 分红 - 回购
    retained_return: float | None = None     # 每留存 1 块钱创造多少价值
    retention_good: bool | None = None
    retention_detail: str = ""

    # 负面信号
    red_flags: list[str] = field(default_factory=list)

    # 综合
    verdict: str = ""  # holds / breaks / unclear
    summary: str = ""


def _feat(ctx: ComputeContext, key: str) -> float | None:
    return ctx.features.get(f"l0.company.{key}")


def _item(ctx: ComputeContext, key: str) -> float | None:
    df = ctx.get_financial_line_items()
    if df.empty:
        return None
    matches = df[df["item_key"] == key]
    return float(matches.iloc[0]["value"]) if not matches.empty else None


def assess_distribution(ctx: ComputeContext) -> DistributionResult:
    r = DistributionResult()

    ni = _item(ctx, "net_income")
    dividends = _item(ctx, "dividends_paid")
    buybacks = _item(ctx, "share_repurchase")
    rpt = _feat(ctx, "related_party_amount_to_revenue")
    dilution = _feat(ctx, "share_dilution_rate")
    ceo_pay = _feat(ctx, "ceo_pay_ratio")
    goodwill = _item(ctx, "goodwill")
    total_assets = _item(ctx, "total_assets")
    oe = _feat(ctx, "owner_earnings")

    if ni is None:
        r.verdict = "unclear"
        r.summary = "数据不足"
        return r

    # ══════════════════════════════════════════════════════════
    #  1. 真金白银到手（分红 + 回购）
    # ══════════════════════════════════════════════════════════

    r.dividends = abs(dividends) if dividends is not None else 0
    r.buybacks = abs(buybacks) if buybacks is not None else 0
    r.total_returned = r.dividends + r.buybacks

    if ni > 0:
        r.return_ratio = r.total_returned / ni
    else:
        r.return_ratio = 0

    if r.total_returned > 0:
        r.cash_to_shareholder = True
        r.cash_detail = (
            f"分红 {r.dividends:,.0f} + 回购 {r.buybacks:,.0f} = {r.total_returned:,.0f}"
            f"（占净利润 {r.return_ratio:.0%}）")
    else:
        r.cash_to_shareholder = False
        r.cash_detail = "无分红无回购，没有真金白银到股东手里"

    # ══════════════════════════════════════════════════════════
    #  2. 留存回报
    # ══════════════════════════════════════════════════════════

    if ni > 0:
        r.retained = ni - r.total_returned
        if r.retained > 0:
            # 验证留存回报: 用 incremental ROIC 近似
            inc_roic = _feat(ctx, "incremental_roic")
            roe = _feat(ctx, "roe")
            roe_vol = _feat(ctx, "roe_stability")
            is_cyclical = roe_vol is not None and roe_vol > 0.08

            if inc_roic is not None:
                r.retained_return = inc_roic
                if inc_roic > 0.15:
                    r.retention_good = True
                    r.retention_detail = (
                        f"留存 {r.retained:,.0f}，增量 ROIC = {inc_roic:.0%}"
                        f" → 每块钱留存在创造价值")
                elif inc_roic > 0:
                    r.retention_good = None
                    r.retention_detail = (
                        f"留存 {r.retained:,.0f}，增量 ROIC = {inc_roic:.0%}"
                        f" → 回报一般")
                elif is_cyclical:
                    # 周期性行业 ROIC 为负可能只是周期低谷
                    r.retention_good = None
                    r.retention_detail = (
                        f"留存 {r.retained:,.0f}，增量 ROIC = {inc_roic:.0%}"
                        f"（周期性行业，需看完整周期）")
                else:
                    r.retention_good = False
                    r.retention_detail = (
                        f"留存 {r.retained:,.0f}，增量 ROIC = {inc_roic:.0%}"
                        f" → 留存在毁灭价值")
            elif roe is not None:
                r.retained_return = roe
                if roe > 0.15:
                    r.retention_good = True
                    r.retention_detail = f"留存 {r.retained:,.0f}，ROE = {roe:.0%} → 回报不错"
                else:
                    r.retention_detail = f"留存 {r.retained:,.0f}，ROE = {roe:.0%}"
            else:
                r.retention_detail = f"留存 {r.retained:,.0f}，缺回报数据"
        elif r.retained <= 0:
            r.retention_detail = "留存为零或负（全部或更多已回馈股东）"
            r.retention_good = True  # 全分了也是好事

    # ══════════════════════════════════════════════════════════
    #  3. 负面信号
    # ══════════════════════════════════════════════════════════

    # 稀释股东
    if dilution is not None and dilution > 0.02:
        r.red_flags.append(f"股权稀释率 {dilution:.1%}，在稀释股东")

    # 管理层自肥
    if ceo_pay is not None and ceo_pay > 300:
        r.red_flags.append(f"CEO Pay Ratio = {ceo_pay:.0f}x，薪酬失控")

    # 关联交易
    if rpt is not None and rpt > 0.03:
        r.red_flags.append(f"关联交易/收入 = {rpt:.1%}，利益输送风险")

    # 帝国建设（商誉暴涨 = 疯狂收购）
    gw_growth = _feat(ctx, "goodwill_growth_vs_revenue_growth")
    if gw_growth is not None and gw_growth > 0.20:
        r.red_flags.append(f"商誉增速超收入增速 {gw_growth:.0%}，疯狂收购")
    elif goodwill is not None and total_assets is not None and total_assets > 0:
        gw_ratio = goodwill / total_assets
        if gw_ratio > 0.30:
            r.red_flags.append(f"商誉/总资产 = {gw_ratio:.0%}，大量收购堆积")

    # 囤现金不作为
    cash = _item(ctx, "cash_and_equivalents")
    if (cash is not None and ni is not None and ni > 0
            and r.total_returned == 0 and cash > ni * 5):
        r.red_flags.append(f"现金 {cash:,.0f} 是利润的 {cash/ni:.0f} 倍，既不分也不投")

    # ══════════════════════════════════════════════════════════
    #  综合判定
    # ══════════════════════════════════════════════════════════

    severe_flags = len(r.red_flags)

    if severe_flags >= 3:
        r.verdict = "breaks"
        r.summary = f"{severe_flags} 个负面信号: " + "; ".join(r.red_flags)
    elif not r.cash_to_shareholder and r.retention_good is False:
        # 没分钱 + 留存还在毁灭价值
        r.verdict = "breaks"
        r.summary = "没有真金白银到股东 + 留存在毁灭价值"
    elif not r.cash_to_shareholder and r.retention_good is True:
        # Berkshire/Amazon 模式: 不分钱但每块钱留存都在创造价值
        # 巴菲特: "如果公司能以高回报再投资，不分红是对股东最好的选择"
        r.verdict = "holds"
        r.summary = f"不分红不回购，全部留存再投资。{r.retention_detail}"
    elif not r.cash_to_shareholder and r.retention_good is not True:
        # 没分钱 + 留存回报不确定
        if severe_flags >= 1:
            r.verdict = "breaks"
            r.summary = "没有真金白银到股东 + " + "; ".join(r.red_flags)
        else:
            r.verdict = "unclear"
            r.summary = "没有真金白银到股东，留存回报待验证"
    elif r.cash_to_shareholder and r.retention_good is False:
        # 有分钱但留存在毁灭价值
        r.verdict = "unclear"
        r.summary = f"有回馈（{r.return_ratio:.0%}）但留存部分在毁灭价值"
    elif r.cash_to_shareholder:
        r.verdict = "holds"
        r.summary = r.cash_detail
        if r.retention_good is True:
            r.summary += "，留存回报好"
        if severe_flags >= 1:
            r.summary += f"（但有 {severe_flags} 个负面信号）"
    else:
        r.verdict = "unclear"
        r.summary = "数据不足"

    return r


# ══════════════════════════════════════════════════════════════
#  格式化
# ══════════════════════════════════════════════════════════════

def format_distribution(result: DistributionResult) -> str:
    lines = [""]
    lines.append("  利润分配检测")
    lines.append("  ════════════════════════════════════════════════")

    # 真金白银
    mark = "●" if result.cash_to_shareholder else "✗"
    lines.append(f"\n  {mark} 真金白银到股东")
    lines.append(f"    {result.cash_detail}")

    # 留存回报
    if result.retained is not None and result.retained > 0:
        mark = "●" if result.retention_good is True else (
            "✗" if result.retention_good is False else "?")
        lines.append(f"\n  {mark} 留存再投资")
        lines.append(f"    {result.retention_detail}")

    # 负面信号
    if result.red_flags:
        lines.append(f"\n  ⚠ 负面信号")
        for flag in result.red_flags:
            lines.append(f"    [-] {flag}")

    lines.append(f"\n  ════════════════════════════════════════════════")
    verdict_labels = {"holds": "成立", "breaks": "断裂", "unclear": "存疑"}
    lines.append(f"  {verdict_labels.get(result.verdict, result.verdict)}: {result.summary}")
    lines.append("")
    return "\n".join(lines)
