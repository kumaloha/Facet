"""
巴菲特因果链
============
护城河 → 盈余能力 → 管理层可信 → 可预测 → 可估值 → 安全边际

每一环是一个论点（thesis）。
我们反向找证据来支持或反驳它。
一条强证据就够。任何一环断裂，链终止。

输入: ComputeContext（可以看原始 Anchor 数据）+ 已算特征 + 市场数据
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from polaris.features.types import ComputeContext


# ── 数据结构 ─────────────────────────────────────────────────


class Verdict(str, Enum):
    HOLDS = "holds"
    BREAKS = "breaks"
    UNCLEAR = "unclear"


@dataclass
class Finding:
    """一条证据发现。"""
    source: str           # 证据来源 (表名 / 特征名)
    observation: str      # 自然语言描述：我们看到了什么
    supports: bool | None  # True / False / None(不确定)


@dataclass
class Probe:
    """一次证据搜寻。我们去某个地方找某类证据。"""
    looking_for: str       # 我们在找什么
    where: str             # 去哪里找（表名或特征名）
    found: bool = False    # 找到了吗
    findings: list[Finding] = field(default_factory=list)


@dataclass
class ChainLink:
    """因果链的一环。"""
    name: str
    thesis: str            # 这一环在验证什么论点
    verdict: Verdict = Verdict.UNCLEAR
    probes: list[Probe] = field(default_factory=list)
    reasoning: str = ""    # 为什么这样判定


@dataclass
class BuffettChainResult:
    links: list[ChainLink] = field(default_factory=list)
    broken_at: str | None = None
    conclusion: str = ""
    intrinsic_value: float | None = None
    margin_of_safety: float | None = None


# ── helpers ──────────────────────────────────────────────────

def _feat(ctx: ComputeContext, key: str) -> float | None:
    return ctx.features.get(f"l0.company.{key}")


# ── 链环 1: 护城河 ──────────────────────────────────────────

def _link_moat(ctx: ComputeContext) -> ChainLink:
    link = ChainLink(
        name="护城河",
        thesis="这门生意有结构性竞争优势，能阻止竞争者侵蚀利润",
    )

    # ── 探针 A: 从毛利率找定价权证据 ──
    probe_a = Probe(
        looking_for="定价权——毛利率高且稳定说明产品不是商品",
        where="financial_line_items + cross_period",
    )
    gm = _feat(ctx, "gross_margin")
    gm_stab = _feat(ctx, "gross_margin_stability")
    gm_delta = _feat(ctx, "gross_margin_delta")

    if gm is not None:
        if gm > 0.40:
            probe_a.found = True
            probe_a.findings.append(Finding(
                "gross_margin", f"毛利率 {gm:.0%}，远高于商品化水平", True))
            if gm_stab is not None and gm_stab < 0.03:
                probe_a.findings.append(Finding(
                    "gross_margin_stability", f"且标准差仅 {gm_stab:.4f}，极稳定", True))
            if gm_delta is not None and gm_delta > 0:
                probe_a.findings.append(Finding(
                    "gross_margin_delta", f"同比还在扩张 +{gm_delta:.2%}", True))
        elif gm < 0.20:
            probe_a.found = True
            probe_a.findings.append(Finding(
                "gross_margin", f"毛利率仅 {gm:.0%}，接近商品化，没有定价权", False))
        else:
            probe_a.findings.append(Finding(
                "gross_margin", f"毛利率 {gm:.0%}，中等水平，不能单独证明定价权", None))
    link.probes.append(probe_a)

    # ── 探针 B: 从客户结构找锁定效应 ──
    probe_b = Probe(
        looking_for="客户锁定——经常性收入、长合同、订阅模式",
        where="downstream_segments",
    )
    ds = ctx.get_downstream_segments()
    if not ds.empty:
        # 看收入类型
        if "revenue_type" in ds.columns:
            types = ds["revenue_type"].dropna().unique().tolist()
            sticky_types = [t for t in types if t in ("subscription", "license", "saas", "recurring")]
            if sticky_types:
                probe_b.found = True
                probe_b.findings.append(Finding(
                    "downstream_segments.revenue_type",
                    f"收入类型含 {', '.join(sticky_types)}，有客户粘性", True))

        # 看合同时长
        if "contract_duration" in ds.columns:
            durations = ds["contract_duration"].dropna()
            if not durations.empty:
                probe_b.found = True
                probe_b.findings.append(Finding(
                    "downstream_segments.contract_duration",
                    f"合同时长: {durations.tolist()}", True))

        # 看经常性收入占比
        if "is_recurring" in ds.columns:
            recurring_count = ds["is_recurring"].fillna(False).sum()
            total = len(ds)
            if recurring_count > 0:
                pct = recurring_count / total
                probe_b.found = True
                probe_b.findings.append(Finding(
                    "downstream_segments.is_recurring",
                    f"{recurring_count}/{total} 个客户/segment 是经常性收入 ({pct:.0%})",
                    True if pct > 0.5 else None))

        # 看积压订单
        if "backlog" in ds.columns:
            backlogs = ds["backlog"].dropna()
            if not backlogs.empty:
                total_backlog = backlogs.sum()
                probe_b.findings.append(Finding(
                    "downstream_segments.backlog",
                    f"积压订单合计 {total_backlog:,.0f}", True if total_backlog > 0 else None))
    else:
        probe_b.findings.append(Finding(
            "downstream_segments", "无客户结构数据", None))
    link.probes.append(probe_b)

    # ── 探针 C: 从提价历史找直接定价权证据 ──
    probe_c = Probe(
        looking_for="提价历史——有过提价且没丢客户",
        where="pricing_actions",
    )
    pa = ctx.get_pricing_actions()
    if not pa.empty:
        probe_c.found = True
        probe_c.findings.append(Finding(
            "pricing_actions", f"有 {len(pa)} 条提价记录", True))
    else:
        probe_c.findings.append(Finding(
            "pricing_actions", "无提价历史数据", None))
    link.probes.append(probe_c)

    # ── 探针 D: 从供应商结构找反面证据 ──
    probe_d = Probe(
        looking_for="供应链风险——sole source 多意味着护城河可能在上游不在自己",
        where="upstream_segments",
    )
    us = ctx.get_upstream_segments()
    if not us.empty and "is_sole_source" in us.columns:
        sole = us["is_sole_source"].fillna(False).sum()
        total = len(us)
        if sole > 0:
            probe_d.found = True
            pct = sole / total
            probe_d.findings.append(Finding(
                "upstream_segments.is_sole_source",
                f"{sole}/{total} 个供应商是 sole source ({pct:.0%})",
                False if pct > 0.5 else None))
    link.probes.append(probe_d)

    # ── 判定 ──
    supporting_probes = [p for p in link.probes if p.found and
                         any(f.supports is True for f in p.findings)]
    contradicting = [f for p in link.probes for f in p.findings if f.supports is False]

    if any(f.source == "gross_margin" and f.supports is False for p in link.probes for f in p.findings):
        link.verdict = Verdict.BREAKS
        link.reasoning = "毛利率过低，没有定价权，护城河不存在"
    elif len(supporting_probes) >= 1:
        link.verdict = Verdict.HOLDS
        sources = [p.looking_for.split("——")[0] for p in supporting_probes]
        link.reasoning = f"找到 {len(supporting_probes)} 条支持证据: {', '.join(sources)}"
        if contradicting:
            link.reasoning += f"；但也有 {len(contradicting)} 条风险信号需关注"
    elif contradicting:
        link.verdict = Verdict.BREAKS
        link.reasoning = "找到的证据以反面为主"
    else:
        link.verdict = Verdict.UNCLEAR
        link.reasoning = "数据不足以判定"

    return link


# ── 链环 2: 盈余能力 ────────────────────────────────────────

def _link_earnings_power(ctx: ComputeContext) -> ChainLink:
    link = ChainLink(
        name="盈余能力",
        thesis="护城河能转化为真金白银的所有者盈余，而不只是纸面利润",
    )

    # ── 探针 A: 现金流 vs 利润 ──
    probe_a = Probe(
        looking_for="利润是真钱还是应计——OCF 和 NI 的关系",
        where="financial_line_items",
    )
    ocf_ni = _feat(ctx, "ocf_to_net_income")
    accruals = _feat(ctx, "accruals_ratio")
    if ocf_ni is not None:
        if ocf_ni > 0.8:
            probe_a.found = True
            probe_a.findings.append(Finding(
                "ocf_to_net_income", f"OCF/NI = {ocf_ni:.2f}，利润有现金背书", True))
        elif ocf_ni < 0.5:
            probe_a.found = True
            probe_a.findings.append(Finding(
                "ocf_to_net_income",
                f"OCF/NI = {ocf_ni:.2f}，大部分利润不是现金，可能不真实", False))
        else:
            probe_a.findings.append(Finding(
                "ocf_to_net_income", f"OCF/NI = {ocf_ni:.2f}，中等", None))
    if accruals is not None and accruals > 0.10:
        probe_a.found = True
        probe_a.findings.append(Finding(
            "accruals_ratio", f"应计比率 {accruals:.2%}，利润可能有水分", False))
    link.probes.append(probe_a)

    # ── 探针 B: 资本轻重 ──
    probe_b = Probe(
        looking_for="赚钱不需要大量资本投入——轻资本模式",
        where="financial_line_items",
    )
    capex_rev = _feat(ctx, "capex_to_revenue")
    oe_to_ni = _feat(ctx, "owner_earnings_to_net_income")
    if capex_rev is not None:
        if capex_rev < 0.05:
            probe_b.found = True
            probe_b.findings.append(Finding(
                "capex_to_revenue", f"capex/revenue = {capex_rev:.1%}，轻资本", True))
        elif capex_rev > 0.15:
            probe_b.found = True
            probe_b.findings.append(Finding(
                "capex_to_revenue", f"capex/revenue = {capex_rev:.1%}，重资本", False))
        else:
            probe_b.findings.append(Finding(
                "capex_to_revenue", f"capex/revenue = {capex_rev:.1%}，中等", None))
    if oe_to_ni is not None:
        if oe_to_ni < 0.5:
            probe_b.found = True
            probe_b.findings.append(Finding(
                "owner_earnings_to_net_income",
                f"OE/NI = {oe_to_ni:.2f}，大量利润被资本支出吃掉", False))
    link.probes.append(probe_b)

    # ── 探针 C: 所有者盈余绝对值 ──
    probe_c = Probe(
        looking_for="所有者盈余为正且有意义",
        where="computed features",
    )
    oe = _feat(ctx, "owner_earnings")
    oe_margin = _feat(ctx, "owner_earnings_margin")
    if oe is not None:
        if oe > 0:
            probe_c.found = True
            desc = f"所有者盈余 {oe:,.0f}"
            if oe_margin is not None:
                desc += f"，OE margin = {oe_margin:.0%}"
            probe_c.findings.append(Finding("owner_earnings", desc, True))
        else:
            probe_c.found = True
            probe_c.findings.append(Finding(
                "owner_earnings", f"所有者盈余 {oe:,.0f}，为负", False))
    link.probes.append(probe_c)

    # ── 判定 ──
    hard_fail = (oe is not None and oe <= 0) or (ocf_ni is not None and ocf_ni < 0.4)
    supporting = [p for p in link.probes if p.found and any(f.supports is True for f in p.findings)]
    contradicting = [f for p in link.probes for f in p.findings if f.supports is False]

    if hard_fail:
        link.verdict = Verdict.BREAKS
        link.reasoning = "所有者盈余为负或现金流严重背离利润"
    elif len(supporting) >= 2:
        link.verdict = Verdict.HOLDS
        link.reasoning = "利润有现金背书，资本轻，盈余真实"
    elif contradicting and not supporting:
        link.verdict = Verdict.BREAKS
        link.reasoning = "盈余质量差"
    elif supporting:
        link.verdict = Verdict.HOLDS
        link.reasoning = "有证据支持盈余能力"
    else:
        link.verdict = Verdict.UNCLEAR
        link.reasoning = "数据不足"

    return link


# ── 链环 3: 管理层可信 ──────────────────────────────────────

def _link_management(ctx: ComputeContext) -> ChainLink:
    link = ChainLink(
        name="管理层",
        thesis="管理层诚信且理性，赚到的钱能到股东手里",
    )

    # ── 探针 A: 承诺兑现 ──
    probe_a = Probe(
        looking_for="管理层说到做到——从 narratives 的兑现率看",
        where="company_narratives",
    )
    narr = ctx.get_company_narratives()
    if not narr.empty and "status" in narr.columns:
        total = len(narr)
        delivered = (narr["status"] == "delivered").sum()
        missed = (narr["status"].isin(["missed", "abandoned"])).sum()
        probe_a.found = True
        rate = delivered / total if total > 0 else 0
        probe_a.findings.append(Finding(
            "company_narratives",
            f"{delivered}/{total} 个承诺兑现 ({rate:.0%})，{missed} 个失败",
            True if rate > 0.6 else (False if rate < 0.3 else None)))
    else:
        probe_a.findings.append(Finding("company_narratives", "无承诺兑现数据", None))
    link.probes.append(probe_a)

    # ── 探针 B: 利益对齐 ──
    probe_b = Probe(
        looking_for="管理层自己持股——利益绑定",
        where="stock_ownership",
    )
    own = ctx.get_stock_ownership()
    if not own.empty and "percent_of_class" in own.columns:
        # 找管理层持股
        mgmt = own[own["title"].notna()]
        if not mgmt.empty:
            total_pct = mgmt["percent_of_class"].sum()
            probe_b.found = True
            probe_b.findings.append(Finding(
                "stock_ownership",
                f"管理层合计持股 {total_pct:.1f}%",
                True if total_pct > 3 else (False if total_pct < 0.5 else None)))
    link.probes.append(probe_b)

    # ── 探针 C: 资本配置——钱去哪了 ──
    probe_c = Probe(
        looking_for="资本配置理性——分红回购 vs 乱收购乱花钱",
        where="financial_line_items",
    )
    sy = _feat(ctx, "shareholder_yield")
    if sy is not None:
        probe_c.found = True
        if sy > 0.3:
            probe_c.findings.append(Finding(
                "shareholder_yield", f"股东回报率 {sy:.0%}，大量回馈股东", True))
        elif sy < 0:
            probe_c.findings.append(Finding(
                "shareholder_yield", f"股东回报率 {sy:.0%}，在稀释股东", False))
        else:
            probe_c.findings.append(Finding(
                "shareholder_yield", f"股东回报率 {sy:.0%}", None))
    link.probes.append(probe_c)

    # ── 探针 D: 红旗 ──
    probe_d = Probe(
        looking_for="红旗——关联交易、诉讼、薪酬失控",
        where="multiple tables",
    )
    rpt_val = _feat(ctx, "related_party_amount_to_revenue")
    if rpt_val is not None and rpt_val > 0.05:
        probe_d.found = True
        probe_d.findings.append(Finding(
            "related_party_transactions",
            f"关联交易/收入 = {rpt_val:.1%}，利益输送风险", False))

    lit = ctx.get_litigations()
    if not lit.empty:
        pending = lit[lit["status"].isin(["pending", "ongoing"])] if "status" in lit.columns else lit
        if not pending.empty:
            probe_d.found = True
            probe_d.findings.append(Finding(
                "litigations", f"{len(pending)} 件进行中诉讼", False))

    pay = _feat(ctx, "ceo_pay_ratio")
    if pay is not None and pay > 300:
        probe_d.found = True
        probe_d.findings.append(Finding(
            "ceo_pay_ratio", f"CEO Pay Ratio = {pay:.0f}x，薪酬失控", False))
    link.probes.append(probe_d)

    # ── 判定 ──
    supporting = [p for p in link.probes if p.found and any(f.supports is True for f in p.findings)]
    red_flags = [f for p in link.probes for f in p.findings if f.supports is False]

    fulfillment = _feat(ctx, "narrative_fulfillment_rate")
    if fulfillment is not None and fulfillment < 0.3:
        link.verdict = Verdict.BREAKS
        link.reasoning = f"承诺兑现率仅 {fulfillment:.0%}，管理层不可信"
    elif len(red_flags) >= 3:
        link.verdict = Verdict.BREAKS
        link.reasoning = f"{len(red_flags)} 个管理层红旗"
    elif supporting and len(red_flags) <= 1:
        link.verdict = Verdict.HOLDS
        link.reasoning = "管理层诚信有证据支持，无严重红旗"
    elif red_flags:
        link.verdict = Verdict.BREAKS
        link.reasoning = "红旗多于正面证据"
    else:
        link.verdict = Verdict.UNCLEAR
        link.reasoning = "数据不足"

    return link


# ── 链环 4: 可预测性 ────────────────────────────────────────

def _link_predictability(ctx: ComputeContext) -> ChainLink:
    link = ChainLink(
        name="可预测性",
        thesis="未来的现金流可以被合理预测，而不是在赌",
    )

    # ── 探针 A: 收入趋势 ──
    probe_a = Probe(
        looking_for="收入是持续增长还是起伏不定",
        where="cross_period features",
    )
    consec = _feat(ctx, "consecutive_revenue_growth")
    rev_g = _feat(ctx, "revenue_growth_yoy")
    if consec is not None:
        probe_a.found = True
        if consec >= 3:
            desc = f"收入连续增长 {consec:.0f} 期"
            if rev_g is not None:
                desc += f"，最近同比 +{rev_g:.1%}"
            probe_a.findings.append(Finding("consecutive_revenue_growth", desc, True))
        else:
            probe_a.findings.append(Finding(
                "consecutive_revenue_growth",
                f"收入仅连续增长 {consec:.0f} 期，趋势不稳", None))
    link.probes.append(probe_a)

    # ── 探针 B: 利润率稳定性 ──
    probe_b = Probe(
        looking_for="利润率是否稳定——不稳定说明生意受外部冲击大",
        where="cross_period features",
    )
    nm_stab = _feat(ctx, "net_margin_stability")
    gm_stab = _feat(ctx, "gross_margin_stability")
    roe_stab = _feat(ctx, "roe_stability")
    found_any = False
    if gm_stab is not None:
        probe_b.findings.append(Finding(
            "gross_margin_stability", f"毛利率标准差 {gm_stab:.4f}",
            True if gm_stab < 0.03 else (False if gm_stab > 0.10 else None)))
        found_any = True
    if nm_stab is not None:
        probe_b.findings.append(Finding(
            "net_margin_stability", f"净利率标准差 {nm_stab:.4f}",
            True if nm_stab < 0.03 else (False if nm_stab > 0.10 else None)))
        found_any = True
    if roe_stab is not None:
        probe_b.findings.append(Finding(
            "roe_stability", f"ROE 标准差 {roe_stab:.4f}",
            True if roe_stab < 0.05 else None))
        found_any = True
    probe_b.found = found_any
    link.probes.append(probe_b)

    # ── 探针 C: 自由现金流连续性 ──
    probe_c = Probe(
        looking_for="FCF 是否稳定为正——能持续造血",
        where="cross_period features",
    )
    consec_fcf = _feat(ctx, "consecutive_positive_fcf")
    if consec_fcf is not None:
        probe_c.found = True
        probe_c.findings.append(Finding(
            "consecutive_positive_fcf",
            f"FCF 连续 {consec_fcf:.0f} 期为正",
            True if consec_fcf >= 3 else False))
    link.probes.append(probe_c)

    # ── 判定 ──
    supporting = [p for p in link.probes if p.found and any(f.supports is True for f in p.findings)]
    contradicting = [f for p in link.probes for f in p.findings if f.supports is False]

    if len(contradicting) > len(supporting):
        link.verdict = Verdict.BREAKS
        link.reasoning = "业绩波动大或不持续，无法合理预测"
    elif len(supporting) >= 2:
        link.verdict = Verdict.HOLDS
        link.reasoning = "收入、利润率、现金流均显示可预测性"
    elif supporting:
        link.verdict = Verdict.HOLDS
        link.reasoning = "有一定可预测性"
    else:
        link.verdict = Verdict.UNCLEAR
        link.reasoning = "数据不足"

    return link


# ── 链环 5: 可估值 ──────────────────────────────────────────

def _link_valuation(ctx: ComputeContext, market: dict | None) -> ChainLink:
    link = ChainLink(
        name="可估值",
        thesis="能算出一个可信的内在价值",
    )

    oe = _feat(ctx, "owner_earnings")

    probe_a = Probe(looking_for="正向所有者盈余 + DCF 计算", where="DCF engine")
    if oe is None or oe <= 0:
        probe_a.findings.append(Finding("owner_earnings", "所有者盈余为负或不存在", False))
        link.probes.append(probe_a)
        link.verdict = Verdict.BREAKS
        link.reasoning = "无正向盈余，无法做 DCF"
        return link

    if market is None:
        probe_a.findings.append(Finding("market_context", "缺少市场数据", None))
        link.probes.append(probe_a)
        link.verdict = Verdict.UNCLEAR
        link.reasoning = "需要折现率和股数才能做 DCF"
        return link

    dr = market.get("discount_rate")
    shares = market.get("shares_outstanding")
    guidance = market.get("guidance", {})

    if dr is None or shares is None:
        probe_a.findings.append(Finding("market_context", "缺少折现率或股数", None))
        link.probes.append(probe_a)
        link.verdict = Verdict.UNCLEAR
        link.reasoning = "市场数据不完整"
        return link

    from polaris.principles.engines.dcf import compute_intrinsic_value
    dcf = compute_intrinsic_value(
        features=ctx.features, guidance=guidance,
        discount_rate=dr, shares_outstanding=shares,
    )

    if dcf.status == "valued" and dcf.intrinsic_value is not None:
        probe_a.found = True
        probe_a.findings.append(Finding(
            "dcf_engine",
            f"DCF 路径 {dcf.valuation_path}: 内在价值 ${dcf.intrinsic_value:,.2f}/股",
            True))
        if dcf.key_assumptions:
            for k, v in dcf.key_assumptions.items():
                fmt = f"{v:.2%}" if isinstance(v, float) and abs(v) < 1 else f"{v}"
                probe_a.findings.append(Finding(
                    f"assumption.{k}", f"关键假设: {k} = {fmt}", None))
        link.verdict = Verdict.HOLDS
        link.reasoning = f"内在价值 ${dcf.intrinsic_value:,.2f}/股"
    else:
        probe_a.found = True
        probe_a.findings.append(Finding("dcf_engine", f"DCF 失败: {dcf.status}", False))
        link.verdict = Verdict.BREAKS
        link.reasoning = f"无法完成估值: {dcf.status}"

    link.probes.append(probe_a)
    return link


# ── 链环 6: 安全边际 ────────────────────────────────────────

def _link_margin_of_safety(intrinsic: float | None, market: dict | None) -> ChainLink:
    link = ChainLink(
        name="安全边际",
        thesis="当前价格远低于内在价值，提供足够的容错空间",
    )

    probe = Probe(looking_for="价格 vs 内在价值", where="market data + DCF")

    if intrinsic is None or market is None or market.get("price") is None:
        probe.findings.append(Finding("price", "缺少价格或内在价值", None))
        link.probes.append(probe)
        link.verdict = Verdict.UNCLEAR
        link.reasoning = "无法比较"
        return link

    price = market["price"]
    mos = (intrinsic - price) / intrinsic

    probe.found = True
    probe.findings.append(Finding("price", f"当前股价 ${price:,.2f}", None))
    probe.findings.append(Finding("intrinsic_value", f"内在价值 ${intrinsic:,.2f}", None))

    if mos > 0.25:
        probe.findings.append(Finding("margin_of_safety", f"安全边际 {mos:.1%}", True))
        link.verdict = Verdict.HOLDS
        link.reasoning = f"安全边际 {mos:.1%}，价格远低于内在价值"
    elif mos > 0:
        probe.findings.append(Finding("margin_of_safety", f"安全边际 {mos:.1%}，偏薄", True))
        link.verdict = Verdict.HOLDS
        link.reasoning = f"有安全边际但不充裕 ({mos:.1%})"
    else:
        probe.findings.append(Finding("margin_of_safety", f"安全边际 {mos:.1%}，无折扣", False))
        link.verdict = Verdict.BREAKS
        link.reasoning = f"股价高于内在价值，没有安全边际"

    link.probes.append(probe)
    return link


# ══════════════════════════════════════════════════════════════
#  主入口
# ══════════════════════════════════════════════════════════════

def evaluate_buffett_chain(
    ctx: ComputeContext,
    market_context: dict | None = None,
) -> BuffettChainResult:
    """执行巴菲特因果链。逐环验证，断裂即停。"""

    result = BuffettChainResult()

    chain_fns = [
        lambda: _link_moat(ctx),
        lambda: _link_earnings_power(ctx),
        lambda: _link_management(ctx),
        lambda: _link_predictability(ctx),
        lambda: _link_valuation(ctx, market_context),
    ]

    intrinsic_value = None

    for fn in chain_fns:
        link = fn()
        result.links.append(link)

        # 从估值环提取内在价值
        if link.name == "可估值" and link.verdict == Verdict.HOLDS:
            for p in link.probes:
                for f in p.findings:
                    if f.source == "dcf_engine" and f.supports is True:
                        # 从 description 提取不好，存到 result 上
                        from polaris.principles.engines.dcf import compute_intrinsic_value
                        dcf = compute_intrinsic_value(
                            features=ctx.features,
                            guidance=market_context.get("guidance", {}),
                            discount_rate=market_context["discount_rate"],
                            shares_outstanding=market_context["shares_outstanding"],
                        )
                        intrinsic_value = dcf.intrinsic_value
                        result.intrinsic_value = intrinsic_value
                        break

        if link.verdict == Verdict.BREAKS:
            result.broken_at = link.name
            result.conclusion = f"链断裂于「{link.name}」— {link.reasoning}"
            return result

    # 全部成立 → 安全边际
    mos_link = _link_margin_of_safety(intrinsic_value, market_context)
    result.links.append(mos_link)

    if mos_link.verdict == Verdict.HOLDS:
        if intrinsic_value and market_context and market_context.get("price"):
            result.margin_of_safety = (intrinsic_value - market_context["price"]) / intrinsic_value
        result.conclusion = f"因果链闭合 — 安全边际 {result.margin_of_safety:.1%}" if result.margin_of_safety else "因果链闭合"
    elif mos_link.verdict == Verdict.BREAKS:
        result.broken_at = "安全边际"
        result.conclusion = f"链断裂于「安全边际」— {mos_link.reasoning}"
    else:
        result.conclusion = "因果链基本成立，但缺价格数据"

    return result


# ══════════════════════════════════════════════════════════════
#  格式化输出
# ══════════════════════════════════════════════════════════════

_VERDICT_MARK = {
    Verdict.HOLDS: "●",
    Verdict.BREAKS: "✗",
    Verdict.UNCLEAR: "?",
}


def format_buffett_chain(result: BuffettChainResult) -> str:
    lines = [""]
    lines.append("  巴菲特因果链")
    lines.append("  ════════════════════════════════════════════════════")

    for i, link in enumerate(result.links):
        mark = _VERDICT_MARK[link.verdict]
        lines.append("")
        lines.append(f"  {mark} [{link.name}]  {link.thesis}")

        for probe in link.probes:
            if probe.findings:
                lines.append(f"    找: {probe.looking_for}")
                lines.append(f"    源: {probe.where}")
                for f in probe.findings:
                    sup = "+" if f.supports is True else ("-" if f.supports is False else " ")
                    lines.append(f"      [{sup}] {f.observation}")
            elif not probe.found:
                lines.append(f"    找: {probe.looking_for}")
                lines.append(f"      [ ] 无数据")

        lines.append(f"    ⇒ {link.reasoning}")

        if link.verdict == Verdict.BREAKS:
            lines.append(f"\n  ╳ 链断裂，后续不再评估")
            break

        if i < len(result.links) - 1 and link.verdict == Verdict.HOLDS:
            lines.append(f"    ↓")

    lines.append("")
    lines.append(f"  ════════════════════════════════════════════════════")
    lines.append(f"  {result.conclusion}")
    lines.append("")
    return "\n".join(lines)
