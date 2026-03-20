"""
护城河检测
==========
完整决策树，所有路径实现。缺数据时返回"无数据"。

决策树:
  Step 0: 伪护城河排除
  Step 1: 涨价测试（路由：品牌 vs 成本优势）
  Step 2: 竞品进攻测试
  Step 3: 无形资产检测（品牌信任/专利/牌照/know-how/独占资源）
  Step 4: 转换成本检测（系统嵌入/数据迁移/学习成本/生态锁定/合同/风险不对称）
  Step 5: 网络效应检测（直接/双边/数据/本地）
  Step 6: 成本优势检测（同行对比/低谷存活/规模经济/地理/独占资源/反定位）
  Step 7: 有效规模检测（自然垄断/利基）
  Step 8: 深度判定
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from polaris.features.types import ComputeContext


# ── 数据结构 ─────────────────────────────────────────────────


class EvidenceStrength(str, Enum):
    BEHAVIORAL = "behavioral"
    STRUCTURAL = "structural"
    LEADING = "leading"
    INDIRECT = "indirect"


@dataclass
class Evidence:
    source: str
    observation: str
    supports: bool | None
    strength: EvidenceStrength = EvidenceStrength.INDIRECT


@dataclass
class MoatSubtype:
    name: str
    detected: bool | None = None
    evidence: list[Evidence] = field(default_factory=list)
    detail: str = ""


@dataclass
class MoatCategory:
    name: str
    subtypes: list[MoatSubtype] = field(default_factory=list)

    @property
    def detected(self) -> bool | None:
        results = [s.detected for s in self.subtypes if s.detected is not None]
        if any(r is True for r in results):
            return True
        if all(r is False for r in results):
            return False
        return None

    @property
    def detected_names(self) -> list[str]:
        return [s.name for s in self.subtypes if s.detected is True]


@dataclass
class PricingTestResult:
    has_data: bool = False
    raised_price: bool = False
    share_held: bool | None = None
    more_expensive: bool | None = None
    routes_to: str = ""  # brand / cost_advantage / pricing_power_untyped / none
    evidence: list[Evidence] = field(default_factory=list)


@dataclass
class MoatResult:
    categories: list[MoatCategory] = field(default_factory=list)
    pricing_test: PricingTestResult | None = None
    depth: str = "unknown"
    summary: str = ""
    anti_moat: list[Evidence] = field(default_factory=list)

    @property
    def all_detected(self) -> list[str]:
        return [name for c in self.categories for name in c.detected_names]


# ── helpers ──────────────────────────────────────────────────

def _feat(ctx: ComputeContext, key: str) -> float | None:
    return ctx.features.get(f"l0.company.{key}")


def _ev(source, obs, supports, strength=EvidenceStrength.INDIRECT):
    return Evidence(source, obs, supports, strength)


def _no_data(sub: MoatSubtype, table: str, desc: str):
    """标记某子类型缺少数据。"""
    sub.evidence.append(_ev(table, f"无{desc}数据", None))


# ══════════════════════════════════════════════════════════════
#  Step 0: 伪护城河排除
# ══════════════════════════════════════════════════════════════

def _check_anti_moat(ctx: ComputeContext) -> list[Evidence]:
    anti = []
    gm = _feat(ctx, "gross_margin")
    nm = _feat(ctx, "net_margin")
    oe = _feat(ctx, "owner_earnings")
    dilution = _feat(ctx, "share_dilution_rate")

    if gm is not None and gm < 0.15 and nm is not None and nm < 0:
        anti.append(_ev("margins",
            f"毛利率 {gm:.0%} + 净利率 {nm:.0%}，低价亏损换份额", False))
    if oe is not None and oe < 0:
        anti.append(_ev("owner_earnings", f"所有者盈余 {oe:,.0f}，在烧钱", False))
    if dilution is not None and dilution > 0.05:
        anti.append(_ev("share_dilution_rate", f"股权稀释率 {dilution:.1%}", False))
    return anti


# ══════════════════════════════════════════════════════════════
#  Step 1: 涨价测试（路由）
# ══════════════════════════════════════════════════════════════

def _pricing_test(ctx: ComputeContext) -> PricingTestResult:
    pt = PricingTestResult()
    pa = ctx.get_pricing_actions()
    ms = ctx.get_market_share_data()

    if pa.empty:
        return pt

    if "price_change_pct" not in pa.columns:
        return pt

    pt.has_data = True

    price_ups = pa[pa["price_change_pct"] > 0]
    price_downs = pa[pa["price_change_pct"] < 0]

    # ── 降价检测：反面信号 ──
    if not price_downs.empty:
        avg_cut = price_downs["price_change_pct"].mean()
        pt.evidence.append(_ev("pricing_actions",
            f"有 {len(price_downs)} 条降价记录（平均 {avg_cut:+.0%}）→ 在打价格战",
            False, EvidenceStrength.BEHAVIORAL))

        # 降价 + 份额涨 = 靠降价抢份额，不是定价权
        if not ms.empty and "share" in ms.columns:
            shares = ms["share"].dropna()
            if len(shares) >= 2 and shares.iloc[-1] >= shares.iloc[0]:
                pt.evidence.append(_ev("pricing_actions",
                    f"降价后份额 {shares.iloc[0]:.1%} → {shares.iloc[-1]:.1%}（靠降价抢份额，不是护城河）",
                    False, EvidenceStrength.BEHAVIORAL))

        # 如果只有降价没有涨价 → 直接判无定价权
        if price_ups.empty:
            pt.routes_to = "no_pricing_power"
            return pt

    # ── 涨价检测 ──
    if price_ups.empty:
        return pt

    pt.raised_price = True
    pt.evidence.append(_ev("pricing_actions", f"有 {len(price_ups)} 条提价记录",
                           None, EvidenceStrength.BEHAVIORAL))

    # 份额变化
    if not ms.empty and "share" in ms.columns:
        shares = ms["share"].dropna()
        if len(shares) >= 2:
            trend = shares.iloc[-1] - shares.iloc[0]
            pt.share_held = trend >= 0
            sup = True if trend >= 0 else False
            pt.evidence.append(_ev("market_share_data",
                f"提价后份额 {shares.iloc[0]:.1%} → {shares.iloc[-1]:.1%}，"
                f"{'稳定/上升' if trend >= 0 else '下降'}",
                sup, EvidenceStrength.BEHAVIORAL))
    elif "volume_impact_pct" in pa.columns:
        vol = pa["volume_impact_pct"].dropna()
        if not vol.empty:
            pt.share_held = vol.mean() >= 0
            pt.evidence.append(_ev("pricing_actions.volume_impact",
                f"提价后销量变化 {vol.mean():+.1%}",
                pt.share_held, EvidenceStrength.BEHAVIORAL))

    if not pt.share_held:
        pt.routes_to = "none"
        return pt

    # 相对价格: 涨完比竞品贵还是便宜？
    if "price_vs_peers" in pa.columns:
        pvp = pa["price_vs_peers"].dropna()
        if not pvp.empty:
            latest = pvp.iloc[-1]
            pt.more_expensive = latest in ("higher", "premium")
            if not pt.more_expensive:
                pt.more_expensive = False
            pt.evidence.append(_ev("pricing_actions.price_vs_peers",
                f"涨价后相对竞品: {latest}", None, EvidenceStrength.BEHAVIORAL))

    # 从同行毛利率间接推断
    if pt.more_expensive is None:
        peers = ctx.get_peer_financials()
        gm = _feat(ctx, "gross_margin")
        if not peers.empty and gm is not None and "metric" in peers.columns:
            peer_gm = peers[peers["metric"] == "gross_margin"]["value"].dropna()
            if not peer_gm.empty:
                peer_avg = peer_gm.mean()
                if gm > peer_avg + 0.05:
                    # 毛利率显著高于同行 → 推断售价高于竞品（品牌溢价）
                    pt.more_expensive = True
                    pt.evidence.append(_ev("peer_financials",
                        f"毛利率 {gm:.0%} 远高于同行 {peer_avg:.0%}，推断售价高于竞品",
                        None, EvidenceStrength.STRUCTURAL))
                elif gm < peer_avg - 0.05:
                    pt.more_expensive = False
                    pt.evidence.append(_ev("peer_financials",
                        f"毛利率 {gm:.0%} 低于同行 {peer_avg:.0%}，推断售价不高于竞品",
                        None, EvidenceStrength.STRUCTURAL))

    # 路由
    if pt.share_held:
        if pt.more_expensive is True:
            pt.routes_to = "brand"
        elif pt.more_expensive is False:
            pt.routes_to = "cost_advantage"
        else:
            pt.routes_to = "pricing_power_untyped"

    return pt


# ══════════════════════════════════════════════════════════════
#  Step 3: 无形资产
# ══════════════════════════════════════════════════════════════

def _check_intangible(ctx: ComputeContext, pt: PricingTestResult) -> MoatCategory:
    cat = MoatCategory(name="无形资产")

    # ── 品牌·定价权（涨价测试路由）──
    brand = MoatSubtype(name="品牌·定价权")
    if pt.routes_to == "brand":
        brand.detected = True
        brand.evidence = list(pt.evidence)
        brand.detail = "涨价 + 份额不跌 + 比竞品贵 → 品牌溢价确认"
    elif pt.routes_to == "pricing_power_untyped":
        brand.evidence = list(pt.evidence)
        brand.detail = "有定价能力，但不确定是品牌还是成本优势（缺相对价格数据）"
    elif pt.has_data and pt.share_held is False:
        brand.detected = False
        brand.evidence = list(pt.evidence)
        brand.detail = "涨价后份额下降，无定价权"
    cat.subtypes.append(brand)

    # ── 品牌·信任默选（社交信号/NPS）──
    trust = MoatSubtype(name="品牌·信任默选")
    bs = ctx.get_brand_signals()
    if not bs.empty and "signal_type" in bs.columns:
        positive = bs[bs["signal_type"].isin(["viral_praise", "organic_mention"])]
        negative = bs[bs["signal_type"].isin(["pr_crisis", "kol_attack", "quality_incident"])]
        if not positive.empty:
            trust.evidence.append(_ev("brand_signals",
                f"{len(positive)} 条正面品牌信号",
                True, EvidenceStrength.LEADING))
        if not negative.empty:
            trust.evidence.append(_ev("brand_signals",
                f"{len(negative)} 条负面品牌信号（{', '.join(negative['signal_type'].unique())}）",
                False, EvidenceStrength.LEADING))
        if "sentiment_score" in bs.columns:
            avg_sent = bs["sentiment_score"].dropna().mean()
            if not pd.isna(avg_sent):
                trust.evidence.append(_ev("brand_signals",
                    f"平均情感分 {avg_sent:.2f}",
                    True if avg_sent > 0.3 else (False if avg_sent < -0.3 else None),
                    EvidenceStrength.LEADING))
        supports = [e for e in trust.evidence if e.supports is True]
        opposes = [e for e in trust.evidence if e.supports is False]
        if supports and not opposes:
            trust.detected = True
            trust.detail = "品牌口碑正面"
        elif opposes:
            trust.detail = "品牌有负面信号"
    else:
        _no_data(trust, "brand_signals", "品牌舆情/NPS")
    cat.subtypes.append(trust)

    # ── 专利/IP ──
    patent = MoatSubtype(name="专利/IP")
    # 从 competitive_dynamics 找专利相关事件
    cd = ctx.get_competitive_dynamics()
    patent_found = False
    if not cd.empty and "event_type" in cd.columns:
        patent_events = cd[cd["event_type"].isin(["patent_challenge", "patent_expiration"])]
        if not patent_events.empty:
            patent_found = True
            for _, row in patent_events.iterrows():
                outcome = str(row.get("outcome_description", ""))
                if row["event_type"] == "patent_expiration":
                    patent.evidence.append(_ev("competitive_dynamics",
                        f"专利过期: {row.get('event_description', '')}",
                        False, EvidenceStrength.LEADING))
                elif row["event_type"] == "patent_challenge":
                    # 挑战结果: 对手输了 = 专利有效 = 正面; 赢了 = 专利失效 = 负面
                    defended_kw = ["有效", "裁定", "支付", "维持", "valid", "upheld",
                                   "pay", "license", "仍需"]
                    lost_kw = ["无效", "撤销", "失效", "invalid", "revoked"]
                    if any(kw in outcome for kw in defended_kw):
                        patent.evidence.append(_ev("competitive_dynamics",
                            f"专利挑战被击退: {outcome}",
                            True, EvidenceStrength.BEHAVIORAL))
                        patent.detected = True
                        patent.detail = "专利经受住挑战，壁垒有效"
                    elif any(kw in outcome for kw in lost_kw):
                        patent.evidence.append(_ev("competitive_dynamics",
                            f"专利被挑战成功: {outcome}",
                            False, EvidenceStrength.BEHAVIORAL))
                    else:
                        patent.evidence.append(_ev("competitive_dynamics",
                            f"专利挑战中: {row.get('event_description', '')}",
                            None, EvidenceStrength.LEADING))
    if not patent_found:
        _no_data(patent, "patent_events", "专利")
    cat.subtypes.append(patent)

    # ── 牌照/特许经营权 ──
    license_ = MoatSubtype(name="牌照/特许经营权")
    # 从 competitive_dynamics 找监管壁垒证据
    if not cd.empty and "event_type" in cd.columns:
        reg = cd[cd["event_type"] == "regulatory_change"]
        if not reg.empty:
            for _, row in reg.iterrows():
                desc = row.get("event_description", "")
                outcome = row.get("outcome_description", "")
                # 监管放开 = 牌照护城河削弱
                open_kw = ["放开", "新发", "降低门槛", "deregulate", "new license"]
                if any(kw in desc + outcome for kw in open_kw):
                    license_.evidence.append(_ev("competitive_dynamics",
                        f"监管变化: {desc}", False, EvidenceStrength.LEADING))
                    license_.detail = "监管在放开，牌照壁垒削弱"
                else:
                    license_.evidence.append(_ev("competitive_dynamics",
                        f"监管事件: {desc}", None, EvidenceStrength.LEADING))
    # TODO: company_profile.industry_type 或 licenses 表
    if not license_.evidence:
        _no_data(license_, "company_profile", "牌照/特许经营权")
    cat.subtypes.append(license_)

    # ── 商业秘密/know-how ──
    secret = MoatSubtype(name="商业秘密/know-how")
    # 正面: 竞品试图复制但失败
    if not cd.empty and "outcome_description" in cd.columns:
        for _, row in cd.iterrows():
            outcome = str(row.get("outcome_description", ""))
            replicate_kw = ["良率", "复制", "追赶", "replicate", "catch up", "yield"]
            fail_kw = ["失败", "远低于", "无法", "failed", "unable"]
            if any(rk in outcome for rk in replicate_kw) and any(fk in outcome for fk in fail_kw):
                secret.evidence.append(_ev("competitive_dynamics",
                    f"竞品尝试复制但失败: {outcome}",
                    True, EvidenceStrength.BEHAVIORAL))
                secret.detected = True
                secret.detail = "竞品无法复制核心能力"

    # 负面: 核心人员流失
    exec_changes = ctx.get_executive_changes()
    if not exec_changes.empty and "change_type" in exec_changes.columns:
        departures = exec_changes[exec_changes["change_type"] == "departed"]
        if len(departures) >= 3:
            secret.evidence.append(_ev("executive_changes",
                f"{len(departures)} 名高管离职，know-how 流失风险",
                False, EvidenceStrength.LEADING))

    if not secret.evidence:
        _no_data(secret, "competitive_dynamics+executive_changes", "know-how")
    cat.subtypes.append(secret)

    # ── 独占资源 ──
    resource = MoatSubtype(name="独占资源")
    # 从 upstream_segments 看是否有独占供给
    us = ctx.get_upstream_segments()
    if not us.empty and "is_sole_source" in us.columns:
        # 如果公司自己就是资源所有者（矿企等），sole_source 是自己的资源
        # 如果依赖别人的 sole_source，那是风险不是护城河
        pass
    # TODO: 需要 company_profile 中的资源属性（矿权、独占数据源等）
    if not resource.evidence:
        _no_data(resource, "company_profile", "独占资源")
    cat.subtypes.append(resource)

    return cat


# ══════════════════════════════════════════════════════════════
#  Step 4: 转换成本
# ══════════════════════════════════════════════════════════════

def _check_switching_cost(ctx: ComputeContext) -> MoatCategory:
    cat = MoatCategory(name="转换成本")
    ds = ctx.get_downstream_segments()
    cd = ctx.get_competitive_dynamics()

    # ── 系统嵌入 ──
    embedded = MoatSubtype(name="系统嵌入")
    if not ds.empty:
        if "switching_cost_level" in ds.columns:
            high = (ds["switching_cost_level"].dropna() == "high").sum()
            if high > 0:
                embedded.evidence.append(_ev("downstream_segments",
                    f"{high} 个客户转换成本为 high",
                    True, EvidenceStrength.STRUCTURAL))
    # 行为证据: 客户评估替代品但最终留下
    if not cd.empty and "event_description" in cd.columns:
        eval_kw = ["评估", "备选", "替代", "考虑", "evaluated", "alternative", "considered"]
        stayed_kw = ["留在", "仍", "无法替代", "回流", "stayed", "returned", "留下"]
        for _, row in cd.iterrows():
            desc = str(row.get("event_description", ""))
            outcome = str(row.get("outcome_description", ""))
            if any(kw in desc + outcome for kw in eval_kw):
                if any(kw in outcome for kw in stayed_kw):
                    embedded.evidence.append(_ev("competitive_dynamics",
                        f"客户评估替代品但最终留下（{row.get('competitor_name', '')}）",
                        True, EvidenceStrength.BEHAVIORAL))
    supports = [e for e in embedded.evidence if e.supports is True]
    if any(e.strength == EvidenceStrength.BEHAVIORAL for e in supports):
        embedded.detected = True
        embedded.detail = "客户想走但走不了"
    elif supports:
        embedded.detected = True
        embedded.detail = "结构证据支持"
    cat.subtypes.append(embedded)

    # ── 数据迁移成本 ──
    data_mig = MoatSubtype(name="数据迁移成本")
    if not ds.empty and "revenue_type" in ds.columns:
        data_types = [t for t in ds["revenue_type"].dropna().unique()
                      if t in ("saas", "platform", "database", "analytics")]
        if data_types:
            data_mig.evidence.append(_ev("downstream_segments",
                f"数据密集型收入: {', '.join(data_types)}",
                None, EvidenceStrength.STRUCTURAL))
            data_mig.detail = "收入类型暗示数据迁移成本，需更多证据"
    # 从 competitive_dynamics 看是否有迁移工具事件
    if not cd.empty and "event_type" in cd.columns:
        mig_tools = cd[cd["event_type"] == "migration_tool"]
        if not mig_tools.empty:
            data_mig.evidence.append(_ev("competitive_dynamics",
                f"竞品推出 {len(mig_tools)} 个迁移工具 → 数据迁移壁垒在降低",
                False, EvidenceStrength.LEADING))
            data_mig.detail = "竞品在降低迁移门槛，数据迁移壁垒削弱"
    cat.subtypes.append(data_mig)

    # ── 学习成本 ──
    learning = MoatSubtype(name="学习成本")
    # TODO: 需要产品复杂度/培训周期数据
    # 间接: 如果有 switching_cost_level = high 且产品是专业工具类
    if not learning.evidence:
        _no_data(learning, "product_complexity", "学习成本")
    cat.subtypes.append(learning)

    # ── 生态锁定 ──
    ecosystem = MoatSubtype(name="生态锁定")
    if not ds.empty and "revenue_type" in ds.columns:
        types = ds["revenue_type"].dropna().unique().tolist()
        sticky = [t for t in types if t in (
            "subscription", "saas", "license", "recurring", "maintenance",
            "ad_revenue", "transaction_fee")]
        if len(sticky) >= 3:
            ecosystem.evidence.append(_ev("downstream_segments",
                f"多种粘性收入类型共存: {', '.join(sticky)}",
                True, EvidenceStrength.STRUCTURAL))
            ecosystem.detected = True
            ecosystem.detail = "多产品/服务交叉依赖"
    # 反面: 客户评估替代品 → 想走，不是生态锁定
    if not cd.empty and "event_description" in cd.columns:
        eval_kw = ["评估", "备选", "替代", "考虑", "evaluated", "alternative"]
        for _, row in cd.iterrows():
            desc = str(row.get("event_description", "")) + str(row.get("outcome_description", ""))
            if any(kw in desc for kw in eval_kw):
                ecosystem.evidence.append(_ev("competitive_dynamics",
                    f"客户评估替代品（{row.get('competitor_name', '')}），说明想走",
                    False, EvidenceStrength.BEHAVIORAL))
                if ecosystem.detected:
                    ecosystem.detected = None
                    ecosystem.detail = "客户在评估替代品，不算生态锁定"
    cat.subtypes.append(ecosystem)

    # ── 合同约束 ──
    contract = MoatSubtype(name="合同约束")
    if not ds.empty and "contract_duration" in ds.columns:
        durations = ds["contract_duration"].dropna()
        long = [d for d in durations if isinstance(d, str) and
                any(x in d for x in ["3", "4", "5", "long", "multi"])]
        if long:
            contract.detected = True
            contract.evidence.append(_ev("downstream_segments",
                f"长期合同: {long}", True, EvidenceStrength.STRUCTURAL))
            contract.detail = "有长期合同约束"
    cat.subtypes.append(contract)

    # ── 风险不对称 ──
    risk_asym = MoatSubtype(name="风险不对称")
    # 产品在客户总成本中占比小 + 出事代价大
    # 从 downstream_segments 看产品类型
    if not ds.empty:
        # 检查是否有 product_criticality 字段
        if "product_criticality" in ds.columns:
            critical = ds[ds["product_criticality"] == "high"]
            if not critical.empty:
                risk_asym.evidence.append(_ev("downstream_segments",
                    f"{len(critical)} 个客户产品关键性为 high",
                    True, EvidenceStrength.STRUCTURAL))
                risk_asym.detected = True
                risk_asym.detail = "产品便宜但出事代价大，客户不敢换"
        # 检查是否有 cost_share_pct 字段（产品在客户总成本中的占比）
        if "cost_share_pct" in ds.columns:
            low_share = ds[ds["cost_share_pct"] < 0.05]
            if not low_share.empty:
                risk_asym.evidence.append(_ev("downstream_segments",
                    f"产品在客户成本中占比 <5%",
                    True, EvidenceStrength.STRUCTURAL))
    if not risk_asym.evidence:
        _no_data(risk_asym, "downstream_segments", "产品关键性")
    cat.subtypes.append(risk_asym)

    return cat


# ══════════════════════════════════════════════════════════════
#  Step 5: 网络效应
# ══════════════════════════════════════════════════════════════

def _check_network_effect(ctx: ComputeContext) -> MoatCategory:
    cat = MoatCategory(name="网络效应")
    cd = ctx.get_competitive_dynamics()
    ds = ctx.get_downstream_segments()

    # ── 直接/双边网络效应 ──
    # 前提: 必须有平台型收入结构才可能有网络效应
    # 竞品进攻失败不能单独证明网络效应（汽车打赢价格战 ≠ 网络效应）
    direct = MoatSubtype(name="直接/双边网络效应")

    # 先检查是否有平台特征
    has_platform_structure = False
    if not ds.empty and "revenue_type" in ds.columns:
        types = ds["revenue_type"].dropna().str.lower().unique().tolist()
        platform = [t for t in types if t in ("marketplace", "platform", "transaction_fee", "ad_revenue")]
        if platform:
            has_platform_structure = True
            direct.evidence.append(_ev("downstream_segments",
                f"平台型收入: {', '.join(platform)}", True, EvidenceStrength.STRUCTURAL))

    # 竞品进攻记录（只有在有平台结构时才归为网络效应）
    if not cd.empty and "event_type" in cd.columns:
        attacks = cd[cd["event_type"].isin(["new_entry", "product_launch", "price_war"])]
        if not attacks.empty:
            for _, row in attacks.iterrows():
                direct.evidence.append(_ev("competitive_dynamics",
                    f"竞品进攻: {row.get('competitor_name', '?')} — {row.get('event_description', '')}",
                    None, EvidenceStrength.BEHAVIORAL))
                outcome = row.get("outcome_description", "")
                share_chg = row.get("outcome_market_share_change")
                if outcome:
                    if share_chg is not None:
                        sup = True if share_chg >= 0 else False
                        direct.evidence.append(_ev("competitive_dynamics",
                            f"结果: {outcome}（份额变化 {share_chg:+.1%}）",
                            sup, EvidenceStrength.BEHAVIORAL))
                    else:
                        fail_kw = ["失败", "关停", "退出", "abandoned", "shut down", "failed"]
                        sup = True if any(kw in outcome for kw in fail_kw) else None
                        direct.evidence.append(_ev("competitive_dynamics",
                            f"结果: {outcome}", sup, EvidenceStrength.BEHAVIORAL))

            supports = [e for e in direct.evidence if e.supports is True]
            opposes = [e for e in direct.evidence if e.supports is False]

            if has_platform_structure:
                # 有平台结构 + 竞品失败 → 网络效应
                if supports and not opposes:
                    direct.detected = True
                    direct.detail = f"平台型业务 + 竞品进攻 {len(attacks)} 次均未撼动"
                elif supports and opposes:
                    direct.detected = True
                    direct.detail = f"有胜有败（{len(supports)} 防住，{len(opposes)} 被蚕食），网络效应在削弱"
                elif opposes:
                    direct.detected = False
                    direct.detail = "竞品进攻取得成效"
            else:
                # 无平台结构 → 竞品失败可能是成本优势/品牌，不归网络效应
                if supports:
                    direct.detail = f"竞品进攻 {len(attacks)} 次防住，但非平台型业务，不归网络效应"
    else:
        _no_data(direct, "competitive_dynamics", "竞品进攻")

    cat.subtypes.append(direct)

    # ── 数据网络效应 ──
    data_ne = MoatSubtype(name="数据网络效应")
    # 需要: 数据量 → 产品质量的正相关证据
    # 从 non_financial_kpis 看用户量/数据量/模型质量指标
    kpis = ctx.get_non_financial_kpis() if hasattr(ctx, 'get_non_financial_kpis') else pd.DataFrame()
    if not kpis.empty and "kpi_name" in kpis.columns:
        data_kpis = kpis[kpis["kpi_name"].str.contains("data|user|mau|dau|query", case=False, na=False)]
        if not data_kpis.empty:
            data_ne.evidence.append(_ev("non_financial_kpis",
                f"有数据相关 KPI: {data_kpis['kpi_name'].tolist()}",
                None, EvidenceStrength.STRUCTURAL))
    if not data_ne.evidence:
        _no_data(data_ne, "non_financial_kpis", "数据网络效应")
    cat.subtypes.append(data_ne)

    # ── 本地网络效应 ──
    local = MoatSubtype(name="本地网络效应")
    ms = ctx.get_market_share_data()
    if not ms.empty and "market_segment" in ms.columns:
        # 如果有城市级份额数据
        local_segments = ms[ms["market_segment"].str.contains("市|city|local|区域", case=False, na=False)]
        if not local_segments.empty:
            local.evidence.append(_ev("market_share_data",
                f"有 {len(local_segments)} 条本地市占率数据",
                None, EvidenceStrength.STRUCTURAL))
    if not local.evidence:
        _no_data(local, "market_share_data", "本地市占率")
    cat.subtypes.append(local)

    return cat


# ══════════════════════════════════════════════════════════════
#  Step 6: 成本优势
# ══════════════════════════════════════════════════════════════

def _check_cost_advantage(ctx: ComputeContext, pt: PricingTestResult) -> MoatCategory:
    cat = MoatCategory(name="成本优势")
    cd = ctx.get_competitive_dynamics()

    # 公司整体利润率（用于间接 fallback 和涨价测试路由）
    gm = _feat(ctx, "gross_margin")
    op_margin = _feat(ctx, "operating_margin")
    nm = _feat(ctx, "net_margin")

    # ── 涨价测试路由: 成本优势型定价 ──
    if pt.routes_to == "cost_advantage":
        pricing_cost = MoatSubtype(name="定价能力（成本优势型）")
        pricing_cost.detected = True
        pricing_cost.evidence = list(pt.evidence)
        pricing_cost.detail = "涨价 + 份额不跌 + 比竞品便宜 → 成本优势型定价"
        cat.subtypes.append(pricing_cost)

    # ── 低谷存活 ──
    survival = MoatSubtype(name="低谷存活")
    if not cd.empty and "event_type" in cd.columns:
        downturns = cd[cd["event_type"].isin(["industry_downturn", "price_war"])]
        if not downturns.empty:
            for _, row in downturns.iterrows():
                survival.evidence.append(_ev("competitive_dynamics",
                    f"行业低谷: {row.get('event_description', '')}",
                    None, EvidenceStrength.BEHAVIORAL))
                outcome = str(row.get("outcome_description", ""))
                survived_kw = ["盈利", "正常", "扩张", "收购", "存活", "活下来", "偿债",
                               "profitable", "survived", "deleverag", "repaid"]
                if any(kw in outcome for kw in survived_kw):
                    survival.evidence.append(_ev("competitive_dynamics",
                        f"结果: {outcome}", True, EvidenceStrength.BEHAVIORAL))
            if any(e.supports is True for e in survival.evidence):
                survival.detected = True
                survival.detail = "行业低谷存活，成本优势确认"
    if not survival.evidence:
        _no_data(survival, "competitive_dynamics", "行业低谷")
    cat.subtypes.append(survival)

    # ── 同行对比 ──
    # 如果 peer_financials 有 segment 字段 → 按业务线分别比
    # 如果没有 → 用公司整体指标比（向后兼容）
    peer_cmp = MoatSubtype(name="同行对比")
    peers = ctx.get_peer_financials()
    ds = ctx.get_downstream_segments()

    has_segment_peers = (not peers.empty and "segment" in peers.columns
                         and peers["segment"].notna().any())

    if has_segment_peers:
        # ── 分业务线对比 ──
        # 每条业务线用自己的 segment_gross_margin vs 该业务线的同行
        seg_margins = {}
        if not ds.empty and "segment_gross_margin" in ds.columns and "customer_name" in ds.columns:
            for _, row in ds.iterrows():
                name = row.get("customer_name", "")
                sgm = row.get("segment_gross_margin")
                pct = row.get("revenue_pct", 0)
                if sgm is not None and name:
                    seg_margins[name] = (sgm, pct)

        # ── 分业务线对比: 和最强竞对比，不是平均 ──
        # 护城河是"能不能打赢最强的"，不是"比平均好就行"
        segments = peers["segment"].dropna().unique()
        seg_wins = 0
        seg_losses = 0
        for seg in segments:
            seg_peers = peers[peers["segment"] == seg]
            seg_gm_peers = seg_peers[seg_peers["metric"] == "gross_margin"]
            if seg_gm_peers.empty:
                continue

            # 找最强竞对（该指标最高的那个）
            best_idx = seg_gm_peers["value"].idxmax()
            best_val = seg_gm_peers.loc[best_idx, "value"]
            best_name = seg_gm_peers.loc[best_idx, "peer_name"]
            all_names = list(dict.fromkeys(seg_gm_peers["peer_name"].tolist()))

            my_val = seg_margins.get(seg, (None, 0))[0] if seg in seg_margins else None
            pct = seg_margins.get(seg, (0, 0))[1] if seg in seg_margins else 0

            if my_val is None:
                continue

            diff = my_val - best_val
            if diff > 0.03:
                # 比最强竞对还高 → 真护城河
                peer_cmp.evidence.append(_ev("peer_financials",
                    f"[{seg} {pct:.0%}] 毛利率 {my_val:.1%} vs 最强竞对 {best_name} {best_val:.1%}"
                    f"（+{diff:.1%}）",
                    True, EvidenceStrength.STRUCTURAL))
                seg_wins += 1
            elif diff < -0.03:
                peer_cmp.evidence.append(_ev("peer_financials",
                    f"[{seg} {pct:.0%}] 毛利率 {my_val:.1%} vs 最强竞对 {best_name} {best_val:.1%}"
                    f"（{diff:+.1%}）",
                    False, EvidenceStrength.STRUCTURAL))
                seg_losses += 1
            else:
                peer_cmp.evidence.append(_ev("peer_financials",
                    f"[{seg} {pct:.0%}] 毛利率 {my_val:.1%} vs 最强竞对 {best_name} {best_val:.1%}"
                    f"（持平）",
                    None, EvidenceStrength.STRUCTURAL))

        if seg_wins > seg_losses:
            peer_cmp.detected = True
            peer_cmp.detail = f"分业务线 vs 最强竞对: {seg_wins} 个领先，{seg_losses} 个落后"
        elif seg_losses > 0:
            peer_cmp.detail = f"分业务线 vs 最强竞对: {seg_wins} 个领先，{seg_losses} 个落后"
    elif not peers.empty and "metric" in peers.columns:
        # ── 整体对比（无 segment 数据时）: 每个指标比最强竞对 ──
        gm = _feat(ctx, "gross_margin")
        op_margin = _feat(ctx, "operating_margin")
        nm = _feat(ctx, "net_margin")
        for metric_name, my_val, label in [
            ("gross_margin", gm, "毛利率"),
            ("operating_margin", op_margin, "营业利润率"),
            ("net_margin", nm, "净利率"),
        ]:
            if my_val is None:
                continue
            metric_peers = peers[peers["metric"] == metric_name]
            peer_vals = metric_peers["value"].dropna()
            if peer_vals.empty:
                continue
            # 找最强竞对
            best_idx = peer_vals.idxmax()
            best_val = peer_vals.loc[best_idx]
            best_name = metric_peers.loc[best_idx, "peer_name"]
            diff = my_val - best_val
            if diff > 0.03:
                peer_cmp.evidence.append(_ev("peer_financials",
                    f"{label}: 本公司 {my_val:.1%} vs 最强竞对 {best_name} {best_val:.1%}"
                    f"（+{diff:.1%}）",
                    True, EvidenceStrength.STRUCTURAL))
            elif diff < -0.03:
                peer_cmp.evidence.append(_ev("peer_financials",
                    f"{label}: 本公司 {my_val:.1%} vs 最强竞对 {best_name} {best_val:.1%}"
                    f"（{diff:+.1%}）",
                    False, EvidenceStrength.STRUCTURAL))
        if any(e.supports is True for e in peer_cmp.evidence):
            peer_cmp.detected = True
            peer_cmp.detail = "利润率高于最强竞对"
    else:
        _no_data(peer_cmp, "peer_financials", "同行对比")
    cat.subtypes.append(peer_cmp)

    # ── 规模经济 ──
    scale = MoatSubtype(name="规模经济")
    # 需要: 行业内收入排名 + 固定成本占比
    if not peers.empty and "metric" in peers.columns:
        peer_rev = peers[peers["metric"] == "revenue"]["value"].dropna()
        my_rev = _feat(ctx, "revenue") or (ctx.get_financial_line_items()
            .query("item_key == 'revenue'")["value"].iloc[0]
            if not ctx.get_financial_line_items().empty else None)
        if not peer_rev.empty and my_rev is not None:
            if my_rev > peer_rev.max():
                scale.evidence.append(_ev("peer_financials",
                    f"收入规模大于所有对标同行",
                    True, EvidenceStrength.STRUCTURAL))
                scale.detail = "收入规模行业领先，可能有规模经济"
    if not scale.evidence:
        _no_data(scale, "peer_financials", "行业规模排名")
    cat.subtypes.append(scale)

    # ── 间接 fallback ──
    process = MoatSubtype(name="流程优势（间接）")
    if gm is not None and op_margin is not None:
        if gm < 0.30 and op_margin > 0.02 and (nm is None or nm > 0):
            if not any(s.detected for s in cat.subtypes):
                process.evidence.append(_ev("margins",
                    f"毛利率 {gm:.0%} 但营业利润率 {op_margin:.0%}",
                    None, EvidenceStrength.INDIRECT))
                process.detail = "低毛利但盈利，暗示效率优势，需行业对比确认"
    cat.subtypes.append(process)

    # ── 地理优势 ──
    geo = MoatSubtype(name="地理优势")
    # 需要: 产品重量/运输成本 + 产能位置 vs 需求位置
    _no_data(geo, "product_logistics", "地理优势")
    cat.subtypes.append(geo)

    # ── 独占低成本资源 ──
    low_resource = MoatSubtype(name="独占低成本资源")
    # 需要: 资源品位/成本对比
    _no_data(low_resource, "resource_data", "独占资源成本")
    cat.subtypes.append(low_resource)

    # ── 反定位 ──
    counter = MoatSubtype(name="反定位")
    if not cd.empty and "event_type" in cd.columns:
        # 对手宣布跟进你的模式 = 反定位可能被破
        follows = cd[cd["event_type"] == "product_launch"]
        for _, row in follows.iterrows():
            outcome = str(row.get("outcome_description", ""))
            cannibalize_kw = ["自毁", "蚕食", "冲突", "cannibalize", "conflict"]
            if any(kw in outcome for kw in cannibalize_kw):
                counter.evidence.append(_ev("competitive_dynamics",
                    f"对手跟进但自毁: {outcome}",
                    True, EvidenceStrength.BEHAVIORAL))
                counter.detected = True
                counter.detail = "对手抄你的模式会自毁，反定位确认"
    if not counter.evidence:
        _no_data(counter, "competitive_dynamics", "反定位")
    cat.subtypes.append(counter)

    return cat


# ══════════════════════════════════════════════════════════════
#  Step 7: 有效规模
# ══════════════════════════════════════════════════════════════

def _check_efficient_scale(ctx: ComputeContext) -> MoatCategory:
    cat = MoatCategory(name="有效规模")
    cd = ctx.get_competitive_dynamics()
    ms = ctx.get_market_share_data()

    # ── 自然垄断/寡头 ──
    monopoly = MoatSubtype(name="自然垄断/寡头")
    if not cd.empty and "event_type" in cd.columns:
        entries = cd[cd["event_type"] == "new_entry"]
        exits = cd[cd["event_type"] == "exit"]
        if not exits.empty and entries.empty:
            monopoly.evidence.append(_ev("competitive_dynamics",
                f"有 {len(exits)} 家退出，无新进入者",
                True, EvidenceStrength.BEHAVIORAL))
            monopoly.detected = True
            monopoly.detail = "玩家在减少，市场趋向寡头"
        elif not entries.empty:
            for _, row in entries.iterrows():
                outcome = str(row.get("outcome_description", ""))
                fail_kw = ["失败", "亏损", "退出", "failed", "loss"]
                if any(kw in outcome for kw in fail_kw):
                    monopoly.evidence.append(_ev("competitive_dynamics",
                        f"新进入者{row.get('competitor_name', '')}失败: {outcome}",
                        True, EvidenceStrength.BEHAVIORAL))
            if any(e.supports is True for e in monopoly.evidence):
                monopoly.detected = True
                monopoly.detail = "新进入者无法生存，有效规模壁垒"
    if not monopoly.evidence:
        _no_data(monopoly, "competitive_dynamics", "行业进入退出")
    cat.subtypes.append(monopoly)

    # ── 地理/品类利基 ──
    niche = MoatSubtype(name="地理/品类利基")
    if not ms.empty and "share" in ms.columns:
        shares = ms["share"].dropna()
        if not shares.empty and shares.iloc[-1] > 0.50:
            niche.evidence.append(_ev("market_share_data",
                f"市占率 {shares.iloc[-1]:.0%}，细分市场主导",
                True, EvidenceStrength.STRUCTURAL))
            niche.detected = True
            niche.detail = "高市占率暗示有效规模"
    if not niche.evidence:
        _no_data(niche, "market_share_data", "利基市场")
    cat.subtypes.append(niche)

    return cat


# ══════════════════════════════════════════════════════════════
#  Step 8: 深度判定
# ══════════════════════════════════════════════════════════════

def _assess_depth(categories: list[MoatCategory], anti: list[Evidence]) -> str:
    if anti:
        return "none"

    all_detected = []
    has_behavioral = False
    for cat in categories:
        for sub in cat.subtypes:
            if sub.detected is True:
                all_detected.append(sub)
                if any(e.strength == EvidenceStrength.BEHAVIORAL and e.supports is True
                       for e in sub.evidence):
                    has_behavioral = True

    if not all_detected:
        return "unknown" if any(cat.detected is None for cat in categories) else "none"

    if has_behavioral and len(all_detected) >= 2:
        return "extreme"
    elif has_behavioral:
        return "deep"
    elif len(all_detected) >= 2:
        return "deep"
    else:
        return "shallow"


# ══════════════════════════════════════════════════════════════
#  主入口
# ══════════════════════════════════════════════════════════════

def assess_moat(ctx: ComputeContext) -> MoatResult:
    anti = _check_anti_moat(ctx)
    if anti:
        return MoatResult(anti_moat=anti, depth="none",
            summary="伪护城河: " + "; ".join(e.observation for e in anti))

    pt = _pricing_test(ctx)
    categories = [
        _check_intangible(ctx, pt),
        _check_switching_cost(ctx),
        _check_network_effect(ctx),
        _check_cost_advantage(ctx, pt),
        _check_efficient_scale(ctx),
    ]

    depth = _assess_depth(categories, anti)

    # ── 市场份额下滑 → 护城河正在瓦解，降级深度 ──
    ms = ctx.get_market_share_data()
    share_declining = False
    share_decline_pct = 0.0
    if not ms.empty and "share" in ms.columns:
        shares = ms["share"].dropna()
        if len(shares) >= 2:
            share_decline_pct = shares.iloc[-1] - shares.iloc[0]
            if share_decline_pct <= -0.049:
                share_declining = True

    if share_declining and depth in ("extreme", "deep"):
        # 份额大幅下滑 → 护城河在瓦解，降一级
        depth = "deep" if depth == "extreme" else "shallow"
        anti.append(_ev("market_share_data",
            f"市占率下滑 {share_decline_pct:+.1%}，护城河正在瓦解",
            False, EvidenceStrength.BEHAVIORAL))

    detected = [name for c in categories for name in c.detected_names]

    if detected:
        summary = f"护城河: {', '.join(detected)} (深度: {depth})"
        if share_declining:
            summary += f"（但份额下滑 {share_decline_pct:+.1%}，护城河在弱化）"
    else:
        has_signal = any(sub.detail for cat in categories for sub in cat.subtypes)
        summary = "有间接信号但缺行为证据确认" if has_signal else "未检测到护城河"

    return MoatResult(categories=categories, pricing_test=pt,
                      depth=depth, summary=summary, anti_moat=anti)


# ══════════════════════════════════════════════════════════════
#  格式化
# ══════════════════════════════════════════════════════════════

_DEPTH_LABELS = {
    "extreme": "极深", "deep": "深", "shallow": "浅",
    "none": "无", "unknown": "数据不足",
}


def _cat_has_signal(cat: MoatCategory) -> bool:
    for sub in cat.subtypes:
        if sub.detected is not None:
            return True
        if any(e.supports is not None for e in sub.evidence):
            return True
        if sub.detail:
            return True
    return False


# pandas import needed for brand_signals check
import pandas as pd  # noqa: E402


def format_moat(result: MoatResult) -> str:
    lines = [""]
    lines.append("  护城河检测")
    lines.append("  ════════════════════════════════════════════════")

    if result.pricing_test and result.pricing_test.has_data:
        pt = result.pricing_test
        route_labels = {
            "brand": "→ 品牌定价权",
            "cost_advantage": "→ 成本优势型定价",
            "pricing_power_untyped": "→ 有定价能力，类型待定",
            "none": "→ 无定价权",
            "no_pricing_power": "→ 在降价，无定价权",
        }
        lines.append(f"\n  ▸ 涨价测试: {route_labels.get(pt.routes_to, pt.routes_to)}")
        for ev in pt.evidence:
            sup = "+" if ev.supports is True else ("-" if ev.supports is False else " ")
            lines.append(f"    [{sup}] {ev.observation}")

    shown = [c for c in result.categories if _cat_has_signal(c)]
    skipped = [c for c in result.categories if not _cat_has_signal(c)]

    for cat in shown:
        mark = "●" if cat.detected_names else ("✗" if cat.detected is False else "?")
        lines.append(f"\n  {mark} {cat.name}")
        for sub in cat.subtypes:
            has_real = (sub.detected is not None or sub.detail or
                       any(e.supports is not None for e in sub.evidence))
            if not has_real:
                continue
            sub_mark = "●" if sub.detected is True else ("✗" if sub.detected is False else "·")
            lines.append(f"    {sub_mark} {sub.name}")
            for ev in sub.evidence:
                sup = "+" if ev.supports is True else ("-" if ev.supports is False else " ")
                lines.append(f"      [{sup}] ({ev.strength.value}) {ev.observation}")
            if sub.detail:
                lines.append(f"      ⇒ {sub.detail}")

    if skipped:
        lines.append(f"\n  · {', '.join(c.name for c in skipped)}: 无相关数据")

    if result.anti_moat:
        lines.append(f"\n  ⚠ 伪护城河信号")
        for ev in result.anti_moat:
            lines.append(f"    [-] {ev.observation}")

    lines.append(f"\n  ════════════════════════════════════════════════")
    lines.append(f"  深度: {_DEPTH_LABELS.get(result.depth, result.depth)}")
    lines.append(f"  {result.summary}")
    lines.append("")
    return "\n".join(lines)
