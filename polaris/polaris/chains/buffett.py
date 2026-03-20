"""
巴菲特因果链（双线结构）
========================

线 1: 生意评估（因果链，断裂即停）
  1. 护城河
  2. 盈余能力
  3. 利润能到股东手里吗
  4. 可预测
  5. 可估值
  6. 安全边际

线 2: 人和环境（并行评估，全部跑完）
  A. 诚信
  B. 管理层人格
  C. 风险

两条线都跑完，交叉判断。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from polaris.features.types import ComputeContext
from polaris.chains.moat import assess_moat, MoatResult, format_moat


class Verdict(str, Enum):
    HOLDS = "holds"
    BREAKS = "breaks"
    UNCLEAR = "unclear"


@dataclass
class ChainLink:
    name: str
    principle: str
    verdict: Verdict = Verdict.UNCLEAR
    detail: str = ""
    evidence: list[str] = field(default_factory=list)


# ── 线 2 数据结构 ────────────────────────────────────────────

@dataclass
class IntegrityResult:
    """诚信检测结果。"""
    verdict: Verdict = Verdict.UNCLEAR
    hard_fail: list[str] = field(default_factory=list)      # SEC处罚、审计非标
    known_issues: list[str] = field(default_factory=list)    # 已知问题
    acknowledged: list[str] = field(default_factory=list)    # 管理层承认的
    hidden: list[str] = field(default_factory=list)          # 差集 = 藏的
    detail: str = ""


@dataclass
class ManagementCharacter:
    """管理层人格评估（Polaris 输出，Axion 做主观判断）。"""
    # 言
    mission: str = ""                    # 使命/愿景
    mission_changes: int = 0             # 使命变过几次
    mission_consistent: bool | None = None
    public_statements: list[str] = field(default_factory=list)  # 关键公开言论

    # 行
    capital_allocation_rational: bool | None = None  # 资金配置是否理性
    capital_evidence: list[str] = field(default_factory=list)
    culture_signals: list[str] = field(default_factory=list)    # 公司文化信号
    people_decisions: list[str] = field(default_factory=list)   # 关键人事决策

    # 言行一致性
    words_match_actions: bool | None = None
    consistency_evidence: list[str] = field(default_factory=list)

    # 信念坚定度
    conviction_level: str = "unknown"  # strong / moderate / weak / unknown
    conviction_evidence: list[str] = field(default_factory=list)

    detail: str = ""


@dataclass
class RiskAssessment:
    """风险评估。"""
    catastrophic: list[str] = field(default_factory=list)  # 灾难性风险（不能买）
    significant: list[str] = field(default_factory=list)    # 重大风险（需更大安全边际）
    moderate: list[str] = field(default_factory=list)       # 中等风险
    detail: str = ""


@dataclass
class BuffettResult:
    # 线 1
    line1_links: list[ChainLink] = field(default_factory=list)
    line1_broken_at: str | None = None
    moat: MoatResult | None = None

    # 线 2
    integrity: IntegrityResult | None = None
    management: ManagementCharacter | None = None
    risk: RiskAssessment | None = None

    # 综合
    conclusion: str = ""


# ── helpers ──────────────────────────────────────────────────

def _feat(ctx: ComputeContext, key: str) -> float | None:
    return ctx.features.get(f"l0.company.{key}")


# ══════════════════════════════════════════════════════════════
#  线 1: 生意评估
# ══════════════════════════════════════════════════════════════

def _link_moat(ctx: ComputeContext) -> tuple[ChainLink, MoatResult]:
    moat = assess_moat(ctx)
    link = ChainLink(name="护城河", principle="有持久的竞争优势吗")

    if moat.depth == "none":
        link.verdict = Verdict.BREAKS
        link.detail = moat.summary
    elif moat.depth == "unknown":
        link.verdict = Verdict.UNCLEAR
        link.detail = moat.summary
    else:
        link.verdict = Verdict.HOLDS
        link.detail = moat.summary

    return link, moat


def _link_earnings_power(ctx: ComputeContext) -> ChainLink:
    link = ChainLink(name="盈余能力", principle="护城河能转化为真实的所有者盈余吗")

    oe = _feat(ctx, "owner_earnings")
    oe_margin = _feat(ctx, "owner_earnings_margin")
    ocf_ni = _feat(ctx, "ocf_to_net_income")
    capex_rev = _feat(ctx, "capex_to_revenue")
    accruals = _feat(ctx, "accruals_ratio")

    # 所有者盈余为正
    if oe is not None and oe > 0:
        link.evidence.append(f"所有者盈余 {oe:,.0f}，为正")
    elif oe is not None:
        link.verdict = Verdict.BREAKS
        link.detail = f"所有者盈余 {oe:,.0f}，为负"
        return link

    # 现金流背书
    if ocf_ni is not None:
        if ocf_ni > 0.8:
            link.evidence.append(f"OCF/NI = {ocf_ni:.2f}，利润有现金背书")
        elif ocf_ni < 0.4:
            link.verdict = Verdict.BREAKS
            link.detail = f"OCF/NI = {ocf_ni:.2f}，利润大部分不是现金"
            return link
        else:
            link.evidence.append(f"OCF/NI = {ocf_ni:.2f}，现金流质量一般")

    # 资本轻重
    if capex_rev is not None:
        if capex_rev < 0.05:
            link.evidence.append(f"capex/revenue = {capex_rev:.1%}，轻资本")
        elif capex_rev > 0.20:
            link.evidence.append(f"capex/revenue = {capex_rev:.1%}，重资本")

    # 应计
    if accruals is not None and accruals > 0.10:
        link.evidence.append(f"应计比率 {accruals:.2%}，利润可能有水分")

    if oe is not None and oe > 0 and (ocf_ni is None or ocf_ni >= 0.5):
        link.verdict = Verdict.HOLDS
        link.detail = f"所有者盈余 {oe:,.0f}" + (f"，OE margin {oe_margin:.0%}" if oe_margin else "")
    elif not link.evidence:
        link.verdict = Verdict.UNCLEAR
        link.detail = "数据不足"

    return link


def _link_profit_distribution(ctx: ComputeContext) -> ChainLink:
    link = ChainLink(name="利润分配", principle="利润能到股东手里吗")

    sy = _feat(ctx, "shareholder_yield")
    rpt = _feat(ctx, "related_party_amount_to_revenue")
    acq = _feat(ctx, "acquisition_spend_to_ocf")

    if sy is not None:
        if sy > 0.3:
            link.evidence.append(f"股东回报率 {sy:.0%}，大量回馈")
        elif sy < 0:
            link.evidence.append(f"股东回报率 {sy:.0%}，在稀释")
        else:
            link.evidence.append(f"股东回报率 {sy:.0%}")

    if rpt is not None and rpt > 0.05:
        link.evidence.append(f"关联交易/收入 = {rpt:.1%}，利益输送风险")

    if acq is not None and acq > 1.0:
        link.evidence.append(f"收购支出/OCF = {acq:.1f}x，疯狂收购")

    # 判定
    if rpt is not None and rpt > 0.10:
        link.verdict = Verdict.BREAKS
        link.detail = "关联交易过高，利润在流失"
    elif sy is not None and sy > 0.2:
        link.verdict = Verdict.HOLDS
        link.detail = f"股东回报率 {sy:.0%}"
    elif sy is not None and sy < -0.05:
        link.verdict = Verdict.BREAKS
        link.detail = "净稀释股东"
    elif not link.evidence:
        link.verdict = Verdict.UNCLEAR
        link.detail = "数据不足"
    else:
        link.verdict = Verdict.HOLDS
        link.detail = "; ".join(link.evidence)

    return link


def _link_predictability(ctx: ComputeContext) -> ChainLink:
    link = ChainLink(name="可预测", principle="未来现金流能预测吗")

    consec_rev = _feat(ctx, "consecutive_revenue_growth")
    consec_fcf = _feat(ctx, "consecutive_positive_fcf")
    gm_stab = _feat(ctx, "gross_margin_stability")
    nm_stab = _feat(ctx, "net_margin_stability")

    if consec_rev is not None:
        link.evidence.append(f"收入连续增长 {consec_rev:.0f} 期")
    if consec_fcf is not None:
        link.evidence.append(f"FCF 连续 {consec_fcf:.0f} 期为正")
    if gm_stab is not None:
        link.evidence.append(f"毛利率标准差 {gm_stab:.4f}")
    if nm_stab is not None:
        link.evidence.append(f"净利率标准差 {nm_stab:.4f}")

    # 判定
    positive = 0
    if consec_rev is not None and consec_rev >= 3:
        positive += 1
    if consec_fcf is not None and consec_fcf >= 3:
        positive += 1
    if gm_stab is not None and gm_stab < 0.05:
        positive += 1

    if positive >= 2:
        link.verdict = Verdict.HOLDS
        link.detail = "收入、利润率、现金流均有可预测性"
    elif positive == 1:
        link.verdict = Verdict.HOLDS
        link.detail = "有一定可预测性"
    elif link.evidence:
        link.verdict = Verdict.UNCLEAR
        link.detail = "可预测性证据不足"
    else:
        link.verdict = Verdict.UNCLEAR
        link.detail = "数据不足"

    return link


def _link_valuation(ctx: ComputeContext, market: dict | None) -> ChainLink:
    link = ChainLink(name="可估值", principle="能算出内在价值吗")

    oe = _feat(ctx, "owner_earnings")
    if oe is None or oe <= 0:
        link.verdict = Verdict.BREAKS
        link.detail = "无正向盈余"
        return link

    if market is None:
        link.verdict = Verdict.UNCLEAR
        link.detail = "缺市场数据"
        return link

    dr = market.get("discount_rate")
    shares = market.get("shares_outstanding")
    guidance = market.get("guidance", {})

    if dr is None or shares is None:
        link.verdict = Verdict.UNCLEAR
        link.detail = "缺折现率或股数"
        return link

    from polaris.principles.engines.dcf import compute_intrinsic_value
    dcf = compute_intrinsic_value(features=ctx.features, guidance=guidance,
                                   discount_rate=dr, shares_outstanding=shares)

    if dcf.status == "valued" and dcf.intrinsic_value is not None:
        link.verdict = Verdict.HOLDS
        link.detail = f"内在价值 ${dcf.intrinsic_value:,.2f}/股 (路径 {dcf.valuation_path})"
        link.evidence.append(f"DCF: ${dcf.intrinsic_value:,.2f}")
    else:
        link.verdict = Verdict.BREAKS
        link.detail = f"DCF 失败: {dcf.status}"

    return link


def _link_margin_of_safety(ctx: ComputeContext, market: dict | None, valuation_link: ChainLink) -> ChainLink:
    link = ChainLink(name="安全边际", principle="当前价格够便宜吗")

    if valuation_link.verdict != Verdict.HOLDS:
        link.verdict = Verdict.UNCLEAR
        link.detail = "无内在价值"
        return link

    if market is None or market.get("price") is None:
        link.verdict = Verdict.UNCLEAR
        link.detail = "缺股价数据"
        return link

    # 从 evidence 提取内在价值
    iv = None
    for ev in valuation_link.evidence:
        if ev.startswith("DCF: $"):
            iv = float(ev.replace("DCF: $", "").replace(",", ""))
            break

    if iv is None:
        link.verdict = Verdict.UNCLEAR
        link.detail = "无法提取内在价值"
        return link

    price = market["price"]
    mos = (iv - price) / iv
    link.evidence.append(f"股价 ${price:,.2f}")
    link.evidence.append(f"内在价值 ${iv:,.2f}")
    link.evidence.append(f"安全边际 {mos:.1%}")

    if mos > 0.25:
        link.verdict = Verdict.HOLDS
        link.detail = f"安全边际 {mos:.1%}，充足"
    elif mos > 0:
        link.verdict = Verdict.HOLDS
        link.detail = f"安全边际 {mos:.1%}，偏薄"
    else:
        link.verdict = Verdict.BREAKS
        link.detail = f"安全边际 {mos:.1%}，无折扣"

    return link


# ══════════════════════════════════════════════════════════════
#  线 2A: 诚信
# ══════════════════════════════════════════════════════════════

def _check_integrity(ctx: ComputeContext) -> IntegrityResult:
    result = IntegrityResult()

    # ── 硬证据（一票否决）──
    audit = ctx.get_audit_opinions()
    if not audit.empty and "opinion_type" in audit.columns:
        non_standard = audit[audit["opinion_type"] != "unqualified"]
        if not non_standard.empty:
            result.hard_fail.append(f"审计意见非标准: {non_standard['opinion_type'].tolist()}")

    # ── 已知问题（财报暴露 + 第三方）──
    # 财报暴露
    de = _feat(ctx, "debt_to_equity")
    if de is not None and de > 3.0:
        result.known_issues.append(f"债务/权益 = {de:.1f}，高杠杆")

    ic = _feat(ctx, "interest_coverage")
    if ic is not None and ic < 2.0:
        result.known_issues.append(f"利息覆盖率 = {ic:.1f}，偿债压力")

    ocf_ni = _feat(ctx, "ocf_to_net_income")
    if ocf_ni is not None and ocf_ni < 0.5:
        result.known_issues.append(f"OCF/NI = {ocf_ni:.2f}，现金流背离利润")

    # 第三方
    ki = ctx.get_known_issues()
    if not ki.empty and "issue_description" in ki.columns:
        for _, row in ki.iterrows():
            result.known_issues.append(row["issue_description"])

    # ── 管理层承认的 ──
    ma = ctx.get_management_acknowledgments()
    if not ma.empty and "issue_description" in ma.columns:
        for _, row in ma.iterrows():
            result.acknowledged.append(row["issue_description"])

    # ── 差集: 藏了什么 ──
    # 简单匹配: known_issues 中管理层没提到的
    ack_text = " ".join(result.acknowledged).lower()
    for issue in result.known_issues:
        # 粗略匹配: 看 issue 的关键词是否出现在承认文本中
        keywords = [w for w in issue.lower().split() if len(w) > 2]
        mentioned = any(kw in ack_text for kw in keywords) if keywords else False
        if not mentioned and result.acknowledged:
            result.hidden.append(issue)

    # ── 判定 ──
    if result.hard_fail:
        result.verdict = Verdict.BREAKS
        result.detail = "硬证据: " + "; ".join(result.hard_fail)
    elif len(result.hidden) >= 3:
        result.verdict = Verdict.BREAKS
        result.detail = f"管理层隐瞒了 {len(result.hidden)} 个已知问题"
    elif result.hidden:
        result.verdict = Verdict.UNCLEAR
        result.detail = f"管理层可能隐瞒了 {len(result.hidden)} 个问题"
    elif result.known_issues and not result.acknowledged:
        result.verdict = Verdict.UNCLEAR
        result.detail = "有已知问题但无管理层回应数据"
    elif not result.known_issues:
        result.verdict = Verdict.HOLDS
        result.detail = "未发现明显问题"
    else:
        result.verdict = Verdict.HOLDS
        result.detail = "已知问题管理层均有回应"

    return result


# ══════════════════════════════════════════════════════════════
#  线 2B: 管理层人格
# ══════════════════════════════════════════════════════════════

def _assess_management_character(ctx: ComputeContext) -> ManagementCharacter:
    mc = ManagementCharacter()

    # ── 言: 使命/愿景/价值观 ──
    narr = ctx.get_company_narratives()
    if not narr.empty and "narrative" in narr.columns:
        narratives = narr["narrative"].dropna().tolist()
        mc.public_statements = narratives[:5]

        # 使命一致性: 简单检测是否有同一主题反复出现
        if len(narratives) >= 3:
            mc.mission_consistent = True  # 有持续叙事
            mc.conviction_evidence.append(f"有 {len(narratives)} 条公开叙事/承诺")

    # TODO: Anchor 提取 mission_statement 跨期对比
    # mc.mission_changes = 跨期使命变化次数

    # ── 行: 资金配置 ──
    sy = _feat(ctx, "shareholder_yield")
    capex_rev = _feat(ctx, "capex_to_revenue")
    acq = _feat(ctx, "acquisition_spend_to_ocf")
    rpt = _feat(ctx, "related_party_amount_to_revenue")

    rational_signals = 0
    irrational_signals = 0

    if sy is not None:
        if sy > 0.3:
            mc.capital_evidence.append(f"股东回报率 {sy:.0%}，大量回馈")
            rational_signals += 1
        elif sy < 0:
            mc.capital_evidence.append(f"股东回报率 {sy:.0%}，净稀释")
            irrational_signals += 1

    if acq is not None and acq > 1.0:
        mc.capital_evidence.append(f"收购/OCF = {acq:.1f}x，收购成瘾")
        irrational_signals += 1

    if rpt is not None and rpt > 0.05:
        mc.capital_evidence.append(f"关联交易/收入 = {rpt:.1%}，自肥")
        irrational_signals += 1

    mc.capital_allocation_rational = rational_signals > irrational_signals if (rational_signals + irrational_signals) > 0 else None

    # ── 行: 公司文化 ──
    exec_changes = ctx.get_executive_changes()
    if not exec_changes.empty and "change_type" in exec_changes.columns:
        departures = exec_changes[exec_changes["change_type"] == "departed"]
        joins = exec_changes[exec_changes["change_type"] == "joined"]
        if len(departures) > 0:
            mc.people_decisions.append(f"{len(departures)} 名高管离职")
        if len(joins) > 0:
            mc.people_decisions.append(f"{len(joins)} 名高管入职")
        if len(departures) >= 3:
            mc.culture_signals.append("高管频繁离职，文化可能有问题")

    # ── 言行一致性 ──
    fulfillment = _feat(ctx, "narrative_fulfillment_rate")
    if fulfillment is not None:
        if fulfillment > 0.7:
            mc.words_match_actions = True
            mc.consistency_evidence.append(f"承诺兑现率 {fulfillment:.0%}，说到做到")
        elif fulfillment < 0.3:
            mc.words_match_actions = False
            mc.consistency_evidence.append(f"承诺兑现率 {fulfillment:.0%}，言行不一")
        else:
            mc.consistency_evidence.append(f"承诺兑现率 {fulfillment:.0%}")

    # ── 信念坚定度 ──
    if mc.words_match_actions is True and mc.capital_allocation_rational is True:
        mc.conviction_level = "strong"
        mc.detail = "言行一致，资金配置理性，信念坚定"
    elif mc.words_match_actions is False:
        mc.conviction_level = "weak"
        mc.detail = "言行不一，信念存疑"
    elif mc.capital_allocation_rational is False:
        mc.conviction_level = "weak"
        mc.detail = "资金配置不理性"
    elif mc.words_match_actions is True or mc.capital_allocation_rational is True:
        mc.conviction_level = "moderate"
        mc.detail = "部分证据支持"
    else:
        mc.conviction_level = "unknown"
        mc.detail = "数据不足"

    return mc


# ══════════════════════════════════════════════════════════════
#  线 2C: 风险
# ══════════════════════════════════════════════════════════════

def _assess_risk(ctx: ComputeContext) -> RiskAssessment:
    ra = RiskAssessment()

    # ── 地缘政治 / 地理集中度 ──
    top_region = _feat(ctx, "top_region_concentration")
    geo = ctx.get_geographic_revenues()
    if top_region is not None and top_region > 0.60:
        ra.significant.append(f"收入地理集中: 最大区域占 {top_region:.0%}")
    if not geo.empty and "region" in geo.columns:
        high_risk_regions = ["Taiwan", "台湾", "Russia", "俄罗斯", "Iran", "伊朗"]
        for _, row in geo.iterrows():
            region = str(row.get("region", ""))
            share = row.get("revenue_share")
            if any(hr.lower() in region.lower() for hr in high_risk_regions) and share and share > 0.2:
                ra.catastrophic.append(f"收入依赖高风险地区: {region} ({share:.0%})")

    # ── 监管风险 ──
    cd = ctx.get_competitive_dynamics()
    if not cd.empty and "event_type" in cd.columns:
        reg = cd[cd["event_type"] == "regulatory_change"]
        for _, row in reg.iterrows():
            desc = str(row.get("event_description", ""))
            ra.significant.append(f"监管变化: {desc}")

    # ── 客户集中度 ──
    top_cust = _feat(ctx, "top_customer_concentration")
    if top_cust is not None and top_cust > 0.30:
        ra.significant.append(f"最大客户占收入 {top_cust:.0%}")
    if top_cust is not None and top_cust > 0.50:
        ra.catastrophic.append(f"严重客户集中: 最大客户占 {top_cust:.0%}")

    # ── 关键人依赖 ──
    mgmt_own = _feat(ctx, "mgmt_ownership_pct")
    exec_changes = ctx.get_executive_changes()
    # 如果创始人/CEO 持股极高且无继任计划
    if mgmt_own is not None and mgmt_own > 30:
        ra.moderate.append(f"创始人/CEO 持股 {mgmt_own:.0%}，关键人依赖")

    # ── 供应链集中 ──
    sole_source = _feat(ctx, "sole_source_pct")
    if sole_source is not None and sole_source > 0.5:
        ra.significant.append(f"供应商 sole source 占比 {sole_source:.0%}")

    # ── 财务结构 ──
    de = _feat(ctx, "debt_to_equity")
    if de is not None and de > 3.0:
        ra.significant.append(f"高杠杆: D/E = {de:.1f}")

    # ── 技术颠覆 ──
    if not cd.empty and "event_type" in cd.columns:
        tech = cd[cd["event_type"].isin(["product_launch", "new_entry"])]
        for _, row in tech.iterrows():
            desc = str(row.get("event_description", ""))
            disrupt_kw = ["颠覆", "替代", "范式", "disrupt", "paradigm", "obsolete"]
            if any(kw in desc for kw in disrupt_kw):
                ra.significant.append(f"技术颠覆风险: {desc}")

    # ── 汇总 ──
    if ra.catastrophic:
        ra.detail = "存在灾难性风险: " + "; ".join(ra.catastrophic)
    elif ra.significant:
        ra.detail = f"{len(ra.significant)} 项重大风险"
    elif ra.moderate:
        ra.detail = f"{len(ra.moderate)} 项中等风险"
    else:
        ra.detail = "未发现重大风险"

    return ra


# ══════════════════════════════════════════════════════════════
#  主入口
# ══════════════════════════════════════════════════════════════

def evaluate(
    ctx: ComputeContext,
    market_context: dict | None = None,
) -> BuffettResult:
    result = BuffettResult()

    # ── 线 1: 生意评估 ──
    # 1. 护城河
    moat_link, moat_result = _link_moat(ctx)
    result.moat = moat_result
    result.line1_links.append(moat_link)

    if moat_link.verdict == Verdict.BREAKS:
        result.line1_broken_at = "护城河"
    else:
        # 2. 盈余能力
        earnings = _link_earnings_power(ctx)
        result.line1_links.append(earnings)
        if earnings.verdict == Verdict.BREAKS:
            result.line1_broken_at = "盈余能力"
        else:
            # 3. 利润分配
            dist = _link_profit_distribution(ctx)
            result.line1_links.append(dist)
            if dist.verdict == Verdict.BREAKS:
                result.line1_broken_at = "利润分配"
            else:
                # 4. 可预测
                pred = _link_predictability(ctx)
                result.line1_links.append(pred)
                if pred.verdict == Verdict.BREAKS:
                    result.line1_broken_at = "可预测"
                else:
                    # 5. 可估值
                    val = _link_valuation(ctx, market_context)
                    result.line1_links.append(val)
                    if val.verdict == Verdict.BREAKS:
                        result.line1_broken_at = "可估值"
                    else:
                        # 6. 安全边际
                        mos = _link_margin_of_safety(ctx, market_context, val)
                        result.line1_links.append(mos)
                        if mos.verdict == Verdict.BREAKS:
                            result.line1_broken_at = "安全边际"

    # ── 线 2: 人和环境（全部跑完）──
    result.integrity = _check_integrity(ctx)
    result.management = _assess_management_character(ctx)
    result.risk = _assess_risk(ctx)

    # ── 综合判断 ──
    line1_ok = result.line1_broken_at is None
    integrity_ok = result.integrity.verdict != Verdict.BREAKS
    risk_ok = not result.risk.catastrophic

    if line1_ok and integrity_ok and risk_ok:
        result.conclusion = "生意好 + 人可信 + 风险可控 → 可以投资"
    elif line1_ok and not risk_ok:
        result.conclusion = f"好生意但有灾难性风险 → 不能买（{result.risk.detail}）"
    elif line1_ok and not integrity_ok:
        result.conclusion = f"好生意但诚信存疑 → 数据不可信（{result.integrity.detail}）"
    elif result.line1_broken_at:
        result.conclusion = f"生意链断裂于「{result.line1_broken_at}」"
    else:
        result.conclusion = "评估不完整"

    return result


# ══════════════════════════════════════════════════════════════
#  格式化
# ══════════════════════════════════════════════════════════════

_VERDICT_MARK = {Verdict.HOLDS: "●", Verdict.BREAKS: "✗", Verdict.UNCLEAR: "?"}


def format_buffett(result: BuffettResult) -> str:
    lines = [""]
    lines.append("  巴菲特因果链")
    lines.append("  ════════════════════════════════════════════════")

    # 线 1
    lines.append("\n  线 1: 生意评估")
    for i, link in enumerate(result.line1_links):
        mark = _VERDICT_MARK[link.verdict]
        lines.append(f"    {mark} {link.name}: {link.detail}")
        for ev in link.evidence:
            lines.append(f"      · {ev}")
        if link.verdict == Verdict.BREAKS:
            lines.append(f"    ╳ 链断裂")
            break
        if i < len(result.line1_links) - 1 and link.verdict == Verdict.HOLDS:
            lines.append(f"    ↓")

    # 线 2
    lines.append("\n  线 2: 人和环境")

    # 诚信
    if result.integrity:
        ig = result.integrity
        mark = _VERDICT_MARK[ig.verdict]
        lines.append(f"    {mark} 诚信: {ig.detail}")
        if ig.hidden:
            lines.append(f"      管理层隐瞒: {ig.hidden}")
        if ig.hard_fail:
            lines.append(f"      硬证据: {ig.hard_fail}")

    # 管理层人格
    if result.management:
        mc = result.management
        conv_labels = {"strong": "坚定", "moderate": "一般", "weak": "存疑", "unknown": "数据不足"}
        lines.append(f"    {'●' if mc.conviction_level == 'strong' else ('✗' if mc.conviction_level == 'weak' else '?')} "
                     f"管理层人格: 信念{conv_labels.get(mc.conviction_level, mc.conviction_level)}")
        if mc.consistency_evidence:
            for ev in mc.consistency_evidence:
                lines.append(f"      言行: {ev}")
        if mc.capital_evidence:
            for ev in mc.capital_evidence:
                lines.append(f"      配置: {ev}")
        if mc.culture_signals:
            for s in mc.culture_signals:
                lines.append(f"      文化: {s}")

    # 风险
    if result.risk:
        ra = result.risk
        if ra.catastrophic:
            lines.append(f"    ✗ 风险: 灾难性")
            for r in ra.catastrophic:
                lines.append(f"      ⚠ {r}")
        elif ra.significant:
            lines.append(f"    ? 风险: {len(ra.significant)} 项重大")
            for r in ra.significant:
                lines.append(f"      · {r}")
        elif ra.moderate:
            lines.append(f"    ● 风险: {len(ra.moderate)} 项中等，可控")
        else:
            lines.append(f"    ● 风险: 未发现重大风险")

    lines.append(f"\n  ════════════════════════════════════════════════")
    lines.append(f"  {result.conclusion}")
    lines.append("")
    return "\n".join(lines)
