"""
管理层人格评估
==============
Polaris 输出人格判断，Axion 做主观决策（"是否愿意交朋友"）。

四维评估:
  言: 公开言论，使命/愿景/价值观是否动摇
  行: 资金配置、公司文化、人事决策
  言行一致性: 说到做到了吗
  信念坚定度: strong / moderate / weak / unknown
"""

from __future__ import annotations

from dataclasses import dataclass, field

from polaris.features.types import ComputeContext


@dataclass
class CharacterResult:
    # ── 言 ──
    mission_statements: list[str] = field(default_factory=list)
    mission_consistent: bool | None = None
    words_detail: str = ""

    # ── 行 ──
    capital_rational: bool | None = None
    capital_evidence: list[str] = field(default_factory=list)
    culture_signals: list[str] = field(default_factory=list)
    people_decisions: list[str] = field(default_factory=list)
    actions_detail: str = ""

    # ── 言行一致 ──
    fulfillment_rate: float | None = None
    words_match_actions: bool | None = None
    consistency_detail: str = ""

    # ── 信念坚定度 ──
    conviction: str = "unknown"  # strong / moderate / weak / unknown
    conviction_detail: str = ""

    # 综合
    summary: str = ""


def _feat(ctx: ComputeContext, key: str) -> float | None:
    return ctx.features.get(f"l0.company.{key}")


def assess_character(ctx: ComputeContext) -> CharacterResult:
    r = CharacterResult()

    # ══════════════════════════════════════════════════════════
    #  言: 使命/愿景
    # ══════════════════════════════════════════════════════════

    narr = ctx.get_company_narratives()
    if not narr.empty and "narrative" in narr.columns:
        narratives = narr["narrative"].dropna().tolist()
        r.mission_statements = narratives[:10]

        if len(narratives) >= 3:
            # 有持续叙事 = 有在表达愿景
            r.mission_consistent = True
            r.words_detail = f"有 {len(narratives)} 条公开承诺/愿景"
        elif narratives:
            r.words_detail = f"仅 {len(narratives)} 条公开承诺，样本少"
    else:
        r.words_detail = "无公开承诺/愿景数据"

    # TODO: 跨期对比使命是否变化（需 Anchor 提取 mission_statement 跨期数据）

    # ══════════════════════════════════════════════════════════
    #  行: 资金配置
    # ══════════════════════════════════════════════════════════

    rational_signals = 0
    irrational_signals = 0

    sy = _feat(ctx, "shareholder_yield")
    if sy is not None:
        if sy > 0.3:
            r.capital_evidence.append(f"股东回报率 {sy:.0%}，大量回馈")
            rational_signals += 1
        elif sy < 0:
            r.capital_evidence.append(f"股东回报率 {sy:.0%}，净稀释")
            irrational_signals += 1

    acq = _feat(ctx, "acquisition_spend_to_ocf")
    if acq is not None and acq > 1.0:
        r.capital_evidence.append(f"收购/OCF = {acq:.1f}x，收购成瘾")
        irrational_signals += 1

    rpt = _feat(ctx, "related_party_amount_to_revenue")
    if rpt is not None and rpt > 0.05:
        r.capital_evidence.append(f"关联交易/收入 = {rpt:.1%}，自肥嫌疑")
        irrational_signals += 1

    ceo_pay = _feat(ctx, "ceo_pay_ratio")
    if ceo_pay is not None:
        if ceo_pay > 300:
            r.capital_evidence.append(f"CEO Pay Ratio = {ceo_pay:.0f}x，薪酬失控")
            irrational_signals += 1
        elif ceo_pay < 150:
            r.capital_evidence.append(f"CEO Pay Ratio = {ceo_pay:.0f}x，合理")
            rational_signals += 1

    inc_roic = _feat(ctx, "incremental_roic")
    roe_vol = _feat(ctx, "roe_stability")
    is_cyclical = roe_vol is not None and roe_vol > 0.08

    if inc_roic is not None:
        if inc_roic > 0.15:
            r.capital_evidence.append(f"ROIC = {inc_roic:.0%}，资本配置有效")
            rational_signals += 1
        elif inc_roic < 0 and not is_cyclical:
            r.capital_evidence.append(f"ROIC = {inc_roic:.0%}，投资在毁灭价值")
            irrational_signals += 1
        elif inc_roic < 0 and is_cyclical:
            r.capital_evidence.append(f"ROIC = {inc_roic:.0%}（周期性行业，需看完整周期）")

    r.capital_rational = rational_signals > irrational_signals if (rational_signals + irrational_signals) > 0 else None
    if r.capital_rational is True:
        r.actions_detail = "资金配置理性"
    elif r.capital_rational is False:
        r.actions_detail = "资金配置有问题"
    else:
        r.actions_detail = "资金配置数据不足"

    # ══════════════════════════════════════════════════════════
    #  行: 公司文化
    # ══════════════════════════════════════════════════════════

    exec_changes = ctx.get_executive_changes()
    if not exec_changes.empty and "change_type" in exec_changes.columns:
        departures = exec_changes[exec_changes["change_type"] == "departed"]
        joins = exec_changes[exec_changes["change_type"] == "joined"]

        if len(departures) >= 3:
            r.culture_signals.append(f"{len(departures)} 名高管离职，团队不稳定")
            irrational_signals += 1
        if len(joins) > 0:
            r.people_decisions.append(f"{len(joins)} 名新高管加入")

        # 短期内大量变动 = 文化问题
        if len(departures) + len(joins) >= 5:
            r.culture_signals.append("高管大量变动，管理层不稳定")

    # 管理层持股 = 利益绑定
    own = _feat(ctx, "mgmt_ownership_pct")
    if own is not None:
        if own > 5:
            r.culture_signals.append(f"管理层持股 {own:.1f}%，利益绑定")
            rational_signals += 1
        elif own < 0.5:
            r.culture_signals.append(f"管理层持股仅 {own:.1f}%，利益不对齐")

    # ══════════════════════════════════════════════════════════
    #  言行一致性
    # ══════════════════════════════════════════════════════════

    fr = _feat(ctx, "narrative_fulfillment_rate")
    if fr is not None:
        r.fulfillment_rate = fr
        if fr > 0.7:
            r.words_match_actions = True
            r.consistency_detail = f"承诺兑现率 {fr:.0%}，说到做到"
        elif fr > 0.4:
            r.words_match_actions = None
            r.consistency_detail = f"承诺兑现率 {fr:.0%}，偶有食言"
        else:
            r.words_match_actions = False
            r.consistency_detail = f"承诺兑现率 {fr:.0%}，严重言行不一"
    else:
        r.consistency_detail = "无承诺兑现数据"

    # ══════════════════════════════════════════════════════════
    #  信念坚定度
    # ══════════════════════════════════════════════════════════

    strong_signals = 0
    weak_signals = 0

    if r.words_match_actions is True:
        strong_signals += 1
    elif r.words_match_actions is False:
        weak_signals += 1

    if r.capital_rational is True:
        strong_signals += 1
    elif r.capital_rational is False:
        weak_signals += 1

    # mission_consistent 只有在言行也一致时才加分
    # 有很多叙事但一半 miss = 话说得漂亮但做不到，不该加分
    if r.mission_consistent is True and r.words_match_actions is True:
        strong_signals += 1
    elif r.mission_consistent is True and r.words_match_actions is False:
        weak_signals += 1  # 说得多做不到 = 减分

    # 持股 > 5% 一般是利益绑定，但要排除：
    # 创始人通过双重股权结构控制公司 = 权力集中，不是利益对齐
    # 判断: 如果叙事兑现率低（< 0.5）+ 高持股 = 有权力但不负责
    if own is not None and own > 5:
        if fr is not None and fr < 0.5:
            # 持股高但承诺兑现率低 → 有权力没纪律
            weak_signals += 1
            r.culture_signals.append(
                f"高持股 {own:.1f}% 但叙事兑现率仅 {fr:.0%} → 有权力但缺纪律")
        else:
            strong_signals += 1

    # 一票否决: 投资回报为负 → 不管其他信号多好，信念都存疑
    # 但周期性行业（ROE 波动大）的负 ROIC 可能只是周期低谷，不直接否决
    if inc_roic is not None and inc_roic < 0 and not is_cyclical:
        r.conviction = "weak"
        r.conviction_detail = f"投资回报为负 (ROIC={inc_roic:.0%})，资金在毁灭价值"
    elif weak_signals >= 2:
        r.conviction = "weak"
        r.conviction_detail = "多项负面信号，信念存疑"
    elif strong_signals >= 3:
        r.conviction = "strong"
        r.conviction_detail = "言行一致 + 资金理性 + 利益绑定"
    elif strong_signals >= 1 and weak_signals == 0:
        r.conviction = "moderate"
        r.conviction_detail = "有正面信号，无明显负面"
    elif strong_signals == 0 and weak_signals == 0:
        r.conviction = "unknown"
        r.conviction_detail = "数据不足"
    else:
        r.conviction = "moderate"
        r.conviction_detail = "正负信号并存"

    # 综合
    r.summary = r.conviction_detail

    return r


def format_character(result: CharacterResult) -> str:
    lines = [""]
    lines.append("  管理层人格评估")
    lines.append("  ════════════════════════════════════════════════")

    # 言
    lines.append(f"\n  ▸ 言（公开言论）")
    lines.append(f"    {result.words_detail}")
    if result.mission_statements:
        for ms in result.mission_statements[:3]:
            lines.append(f"    · \"{ms}\"")

    # 行
    lines.append(f"\n  ▸ 行（资金配置）")
    if result.capital_evidence:
        for ev in result.capital_evidence:
            lines.append(f"    · {ev}")
    else:
        lines.append(f"    无数据")

    if result.culture_signals:
        lines.append(f"\n  ▸ 行（公司文化）")
        for s in result.culture_signals:
            lines.append(f"    · {s}")

    # 言行一致
    lines.append(f"\n  ▸ 言行一致性")
    mark = "●" if result.words_match_actions is True else (
        "✗" if result.words_match_actions is False else "?")
    lines.append(f"    {mark} {result.consistency_detail}")

    # 信念
    conv_marks = {"strong": "●", "moderate": "?", "weak": "✗", "unknown": "?"}
    conv_labels = {"strong": "坚定", "moderate": "一般", "weak": "存疑", "unknown": "数据不足"}
    lines.append(f"\n  ▸ 信念坚定度")
    lines.append(f"    {conv_marks.get(result.conviction, '?')} "
                 f"{conv_labels.get(result.conviction, result.conviction)}: {result.conviction_detail}")

    lines.append(f"\n  ════════════════════════════════════════════════")
    lines.append(f"  {result.summary}")
    lines.append("")
    return "\n".join(lines)
