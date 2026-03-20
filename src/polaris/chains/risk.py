"""
风险评估
========
三级风险:
  灾难性: 不能买（地缘、极度客户集中、监管灭顶）
  重大: 需更大安全边际
  中等: 关注但不影响决策

即使生意好、人好，风险太大也不能买（台积电案例）。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from polaris.features.types import ComputeContext


@dataclass
class RiskItem:
    category: str      # geopolitical / regulatory / key_person / tech_disruption /
                       # concentration / financial / supply_chain
    level: str         # catastrophic / significant / moderate
    description: str


@dataclass
class RiskResult:
    risks: list[RiskItem] = field(default_factory=list)
    has_catastrophic: bool = False
    summary: str = ""

    @property
    def catastrophic(self) -> list[RiskItem]:
        return [r for r in self.risks if r.level == "catastrophic"]

    @property
    def significant(self) -> list[RiskItem]:
        return [r for r in self.risks if r.level == "significant"]

    @property
    def moderate(self) -> list[RiskItem]:
        return [r for r in self.risks if r.level == "moderate"]


def _feat(ctx: ComputeContext, key: str) -> float | None:
    return ctx.features.get(f"l0.company.{key}")


def assess_risk(ctx: ComputeContext, home_market: str = "") -> RiskResult:
    """风险评估。

    home_market: 公司注册地/主要运营市场（如 "China"），
                 收入集中在 home_market 不算地缘风险。
    """
    r = RiskResult()
    home = home_market.lower()

    # ══════════════════════════════════════════════════════════
    #  地缘政治
    # ══════════════════════════════════════════════════════════

    geo = ctx.get_geographic_revenues()
    top_region = _feat(ctx, "top_region_concentration")

    HIGH_RISK_REGIONS = ["taiwan", "台湾", "russia", "俄罗斯", "iran", "伊朗",
                         "north korea", "朝鲜", "ukraine", "乌克兰"]
    MEDIUM_RISK_REGIONS = ["china", "中国", "turkey", "土耳其",
                           "brazil", "巴西", "argentina", "阿根廷"]

    if not geo.empty and "region" in geo.columns:
        for _, row in geo.iterrows():
            region = str(row.get("region", "")).lower()
            share = row.get("revenue_share")
            if share is None:
                continue

            # 本土市场不算地缘风险
            if home and (home in region or region in home):
                continue

            if any(hr in region for hr in HIGH_RISK_REGIONS) and share > 0.20:
                r.risks.append(RiskItem("geopolitical", "catastrophic",
                    f"收入 {share:.0%} 依赖高风险地区: {row.get('region', '')}"))
            elif any(hr in region for hr in HIGH_RISK_REGIONS) and share > 0.05:
                r.risks.append(RiskItem("geopolitical", "significant",
                    f"收入 {share:.0%} 来自高风险地区: {row.get('region', '')}"))
            elif any(mr in region for mr in MEDIUM_RISK_REGIONS) and share > 0.50:
                r.risks.append(RiskItem("geopolitical", "significant",
                    f"收入 {share:.0%} 高度集中于: {row.get('region', '')}"))

    # 地理集中度（排除稳定市场：美国、日本、欧洲等）
    STABLE_REGIONS = ["united states", "美国", "japan", "日本", "germany", "德国",
                      "uk", "英国", "france", "法国", "canada", "加拿大",
                      "australia", "澳大利亚", "europe", "欧洲"]
    if top_region is not None and top_region > 0.70:
        # 看最大区域是不是稳定市场
        top_is_stable = False
        if not geo.empty and "region" in geo.columns and "revenue_share" in geo.columns:
            top_row = geo.loc[geo["revenue_share"].idxmax()]
            top_name = str(top_row.get("region", "")).lower()
            top_is_stable = any(sr in top_name for sr in STABLE_REGIONS)
        if not top_is_stable:
            r.risks.append(RiskItem("concentration", "significant",
                f"收入地理集中: 最大区域占 {top_region:.0%}"))

    # ══════════════════════════════════════════════════════════
    #  市场份额趋势（份额下跌 = 风险信号）
    # ══════════════════════════════════════════════════════════

    ms = ctx.get_market_share_data()
    if not ms.empty and "share" in ms.columns:
        shares = ms["share"].dropna()
        if len(shares) >= 2:
            trend = shares.iloc[-1] - shares.iloc[0]
            if trend < -0.05:
                r.risks.append(RiskItem("market_share", "significant",
                    f"市场份额下滑: {shares.iloc[0]:.1%} → {shares.iloc[-1]:.1%} ({trend:+.1%})"))

    # ══════════════════════════════════════════════════════════
    #  客户集中（区分具名客户 vs 业务线/泛称）
    # ══════════════════════════════════════════════════════════

    ds = ctx.get_downstream_segments()
    # 泛称过滤：这些名字是业务线/品类/渠道/用户群体，不是具名客户
    GENERIC_NAMES = {"消费者", "用户", "其他", "others", "other", "mass market",
                     "会员", "零售", "批发", "企业", "政府", "smb",
                     "汽车", "电池", "储能", "零部件", "手机", "电商",
                     "国内", "国际", "线上", "线下", "个人", "机构",
                     # 行业/品类泛称
                     "广告", "广告主", "游戏", "玩家", "数据中心", "云",
                     "服务", "金融", "科技", "物流", "本地生活",
                     "经销商", "直销", "渠道", "合作伙伴", "瓶装",
                     "设备", "输变电", "新能源", "多晶硅", "光伏",
                     "新材料", "煤炭", "矿", "冶炼", "贸易",
                     # 英文泛称
                     "advertiser", "consumer", "gamer", "player",
                     "data center", "cloud", "enterprise", "retail",
                     "distributor", "dealer", "wholesale", "oem",
                     # 产品名/收入类型
                     "iphone", "mac", "ipad", "gpu", "服务收入",
                     "可穿戴", "wearable", "subscription",
                     # 大宗商品/资源/能源
                     "原油", "天然气", "石油", "煤", "铜", "金", "锌", "锂",
                     "化工", "中游", "运输", "碳捕获", "ngl",
                     "oil", "gas", "crude", "chemical", "midstream",
                     "pipeline", "carbon", "refining", "mineral",
                     # 金融/支付/保险
                     "持卡人", "商户", "借款人", "储户", "投保人",
                     "cardholder", "merchant", "borrower", "depositor",
                     "policyholder", "patient", "患者",
                     # 餐饮/加盟
                     "加盟", "自营", "门店", "franchise", "store",
                     "配送", "外卖", "delivery"}

    has_named_customer = False
    max_named_pct = 0.0
    named_count = 0
    if not ds.empty and "customer_name" in ds.columns and "revenue_pct" in ds.columns:
        for _, row in ds.iterrows():
            name = str(row.get("customer_name", "")).lower().strip()
            pct = row.get("revenue_pct")
            if pct is None:
                continue
            # 判断是否为具名客户（不在泛称列表中）
            is_generic = any(g in name for g in GENERIC_NAMES)
            # 如果名字包含"产品"/"业务"/"部门"/"收入"/"客户"等也算泛称
            segment_kw = ["产品", "业务", "部门", "收入", "客户", "segment",
                          "division", "line", "revenue", "income", "category"]
            if any(kw in name for kw in segment_kw):
                is_generic = True
            if not is_generic:
                has_named_customer = True
                named_count += 1
                if pct > max_named_pct:
                    max_named_pct = pct

    if has_named_customer:
        if max_named_pct > 0.50:
            r.risks.append(RiskItem("concentration", "catastrophic",
                f"最大具名客户占收入 {max_named_pct:.0%}，严重依赖"))
        elif max_named_pct > 0.30:
            r.risks.append(RiskItem("concentration", "significant",
                f"最大具名客户占收入 {max_named_pct:.0%}"))

    # ══════════════════════════════════════════════════════════
    #  供应链
    # ══════════════════════════════════════════════════════════

    sole_source = _feat(ctx, "sole_source_pct")
    if sole_source is not None and sole_source > 0.50:
        r.risks.append(RiskItem("supply_chain", "significant",
            f"供应商 sole source 占比 {sole_source:.0%}"))

    # ══════════════════════════════════════════════════════════
    #  关键人依赖 — 三层抗替换模型
    # ══════════════════════════════════════════════════════════
    # 如果明天 CEO 被公交车撞了，公司还能运转吗？
    #
    #   层 1: 品牌/产品 — 消费者买的是品牌不是 CEO（最强）
    #         茅台、可口可乐换谁当家都照样卖酒卖饮料
    #   层 2: 公司文化/制度 — 流程、文化、激励机制能延续（强）
    #         伯克希尔的去中心化、腾讯的赛马制
    #   层 3: 个人能力 — 战略/执行靠一个人推动（脆弱）
    #         OXY（Hollub）、英伟达（Jensen）
    #
    # 品牌驱动也不是万能保护——如果新 CEO 搞帝国建设，品牌在但纪律崩。
    # 所以品牌层只免除"护城河依赖个人"的风险，不免除"资本纪律依赖个人"。

    mgmt_own = _feat(ctx, "mgmt_ownership_pct")
    if mgmt_own is not None and mgmt_own > 30:
        r.risks.append(RiskItem("key_person", "significant",
            f"创始人/CEO 持股 {mgmt_own:.1f}%，关键人依赖"))

    # ── 判断护城河的抗替换层级 ──

    # 层 1: 品牌/成瘾品/基础设施 → 护城河不依赖个人
    brand_driven_categories = {"liquor", "beverage", "tobacco", "alcohol", "beer",
                               "wine", "coffee", "insurance", "utility", "payment",
                               "operating_system"}
    ds_cats = set()
    if not ds.empty and "product_category" in ds.columns:
        ds_cats = set(ds["product_category"].dropna().str.lower().unique())
    is_brand_driven = bool(ds_cats & brand_driven_categories)

    # 层 2: 公司文化信号 — 高管团队稳定 + 多元收入 = 不是一人公司
    exec_changes = ctx.get_executive_changes()
    has_successor_signal = False
    high_turnover = False
    if not exec_changes.empty and "change_type" in exec_changes.columns:
        joined = exec_changes[exec_changes["change_type"] == "joined"]
        departed = exec_changes[exec_changes["change_type"] == "departed"]
        has_successor_signal = len(joined) >= 1
        high_turnover = len(departed) >= 3  # 大量离职 = 文化不稳定

    # 多业务线 = 去中心化信号（不是单一赌注）
    n_segments = len(ds_cats)
    is_diversified = n_segments >= 3

    # 综合判断文化层
    has_culture_resilience = (has_successor_signal or is_diversified) and not high_turnover

    # ── 关键人风险评估 ──
    fulfillment = _feat(ctx, "narrative_fulfillment_rate")

    if fulfillment is not None and fulfillment > 0.7:
        if is_brand_driven and has_culture_resilience:
            # 品牌 + 文化双保险 → 不标风险
            pass
        elif is_brand_driven and not has_culture_resilience:
            # 品牌在但文化/制度弱 → 轻微风险
            # 换人后品牌不会消失，但资本纪律可能走样
            r.risks.append(RiskItem("key_person", "moderate",
                f"品牌驱动型生意，但无明确继任计划/文化制度保障 → "
                f"换人后护城河在，但资本纪律可能变"))
        elif not is_brand_driven and has_culture_resilience:
            # 非品牌但有文化/制度缓冲 → 中等风险
            r.risks.append(RiskItem("key_person", "moderate",
                f"非品牌驱动（成功依赖执行），"
                f"但有文化/继任缓冲"))
        elif not is_brand_driven:
            # 非品牌 + 无文化保障 → 显著风险
            detail_parts = [f"叙事兑现 {fulfillment:.0%}"]
            if not has_successor_signal:
                detail_parts.append("无继任信号")
            if not is_diversified:
                detail_parts.append("业务集中")
            r.risks.append(RiskItem("key_person", "significant",
                f"管理层优秀但成功高度依赖个人: {'; '.join(detail_parts)}"))

    # ══════════════════════════════════════════════════════════
    #  财务结构
    # ══════════════════════════════════════════════════════════
    # 特殊业务需豁免:
    #   - 负权益公司（回购导致）的 D/E 无意义
    #   - 银行/保险/支付的利息支出是经营成本
    equity_val = None
    fli = ctx.get_financial_line_items()
    if not fli.empty:
        eq_rows = fli[fli["item_key"] == "shareholders_equity"]
        if not eq_rows.empty:
            equity_val = float(eq_rows.iloc[0]["value"])
    is_financial = bool(ds_cats & {"banking", "insurance", "payment"})

    de = _feat(ctx, "debt_to_equity")
    # D/E: 负权益或极低正权益（回购导致）的公司，D/E 失真，跳过
    # 判断方法: 如果权益为负，或 D/E > 10 且公司在大量回购，大概率是回购导致的
    sy = _feat(ctx, "shareholder_yield")
    # 权益失真的判断:
    #   - 权益为负（回购/收购导致）
    #   - D/E > 10 且在大量回购（sy > 0.3）
    #   - 权益极低（< 总资产 5%）— 刚从负转正的过渡期（如甲骨文）
    ta_val = None
    ta_rows = fli[fli["item_key"] == "total_assets"] if not fli.empty else pd.DataFrame()
    if not ta_rows.empty:
        ta_val = float(ta_rows.iloc[0]["value"])
    equity_tiny = (equity_val is not None and ta_val is not None and ta_val > 0
                   and equity_val / ta_val < 0.05)
    equity_distorted = (
        (equity_val is not None and equity_val <= 0) or
        (de is not None and de > 10 and sy is not None and sy > 0.3) or
        equity_tiny
    )
    if de is not None and not equity_distorted and equity_val is not None and equity_val > 0:
        if de > 5.0:
            r.risks.append(RiskItem("financial", "catastrophic",
                f"D/E = {de:.1f}，极高杠杆"))
        elif de > 3.0:
            r.risks.append(RiskItem("financial", "significant",
                f"D/E = {de:.1f}，高杠杆"))

    ic = _feat(ctx, "interest_coverage")
    # 利息覆盖率: 银行/保险/支付豁免（利息是经营成本）
    if ic is not None and ic < 1.5 and not is_financial:
        r.risks.append(RiskItem("financial", "catastrophic",
            f"利息覆盖率 = {ic:.1f}，可能无法偿债"))

    # ══════════════════════════════════════════════════════════
    #  监管 / 政策突变
    # ══════════════════════════════════════════════════════════

    cd = ctx.get_competitive_dynamics()
    if not cd.empty and "event_type" in cd.columns:
        reg = cd[cd["event_type"] == "regulatory_change"]
        for _, row in reg.iterrows():
            desc = str(row.get("event_description", ""))
            # 灭顶性政策（行业被禁/强制退市）
            ban_kw = ["禁止", "取缔", "全面整顿", "强制退市", "ban", "shutdown", "双减"]
            if any(kw in desc for kw in ban_kw):
                r.risks.append(RiskItem("regulatory", "catastrophic",
                    f"政策灭顶: {desc}"))
            else:
                r.risks.append(RiskItem("regulatory", "significant",
                    f"监管变化: {desc}"))

        # 技术颠覆
        tech = cd[cd["event_type"].isin(["product_launch", "new_entry"])]
        for _, row in tech.iterrows():
            desc = str(row.get("event_description", ""))
            disrupt_kw = ["颠覆", "替代", "范式", "disrupt", "paradigm", "obsolete", "革命"]
            if any(kw in desc for kw in disrupt_kw):
                r.risks.append(RiskItem("tech_disruption", "significant",
                    f"技术颠覆: {desc}"))

    # ══════════════════════════════════════════════════════════
    #  诉讼 / 巨额负债风险
    # ══════════════════════════════════════════════════════════

    lit = ctx.get_litigations()
    if not lit.empty:
        # 统计进行中诉讼
        pending = lit
        if "status" in lit.columns:
            pending = lit[lit["status"].isin(["pending", "ongoing"])]

        if not pending.empty:
            count = len(pending)

            # 看诉讼金额 vs 公司规模
            total_claimed = 0
            if "claimed_amount" in pending.columns:
                total_claimed = pending["claimed_amount"].dropna().sum()

            revenue = _feat(ctx, "gross_margin")  # 先拿 equity 来比
            equity = None
            fli = ctx.get_financial_line_items()
            if not fli.empty:
                eq_rows = fli[fli["item_key"] == "shareholders_equity"]
                if not eq_rows.empty:
                    equity = float(eq_rows.iloc[0]["value"])

            if total_claimed > 0 and equity is not None and equity > 0:
                claim_ratio = total_claimed / equity
                if claim_ratio > 0.5:
                    r.risks.append(RiskItem("litigation", "catastrophic",
                        f"{count} 件诉讼，索赔总额/权益 = {claim_ratio:.0%}，可能致命"))
                elif claim_ratio > 0.1:
                    r.risks.append(RiskItem("litigation", "significant",
                        f"{count} 件诉讼，索赔总额/权益 = {claim_ratio:.0%}"))
                else:
                    r.risks.append(RiskItem("litigation", "moderate",
                        f"{count} 件诉讼，索赔金额相对可控"))
            elif count >= 5:
                r.risks.append(RiskItem("litigation", "significant",
                    f"{count} 件进行中诉讼"))
            elif count >= 1:
                r.risks.append(RiskItem("litigation", "moderate",
                    f"{count} 件进行中诉讼"))

    # ══════════════════════════════════════════════════════════
    #  疫情/黑天鹅暴露度
    # ══════════════════════════════════════════════════════════

    # 检测行业是否属于黑天鹅高暴露行业
    ds = ctx.get_downstream_segments()
    if not ds.empty and "product_category" in ds.columns:
        categories = ds["product_category"].dropna().str.lower().unique().tolist()
        # 高暴露行业: 线下强依赖、人员密集、跨境
        high_exposure = {"airline", "aviation", "hotel", "tourism", "travel",
                        "restaurant", "cinema", "live_event", "cruise",
                        "retail_physical", "gym", "fitness"}
        exposed = [c for c in categories if c in high_exposure]
        if exposed:
            r.risks.append(RiskItem("black_swan", "significant",
                f"行业属于黑天鹅高暴露: {', '.join(exposed)}（疫情/自然灾害可致收入归零）"))

    # 也从 revenue_type 推断
    if not ds.empty and "revenue_type" in ds.columns:
        rev_types = ds["revenue_type"].dropna().str.lower().unique().tolist()
        physical_types = [t for t in rev_types if t in (
            "ticket", "physical_retail", "dine_in", "room_night")]
        if physical_types:
            r.risks.append(RiskItem("black_swan", "significant",
                f"收入依赖线下场景: {', '.join(physical_types)}"))

    # ══════════════════════════════════════════════════════════
    #  货币风险
    # ══════════════════════════════════════════════════════════

    # 收入分散在多个新兴市场 = 货币风险
    if not geo.empty and "region" in geo.columns:
        VOLATILE_CURRENCY = ["argentina", "阿根廷", "turkey", "土耳其",
                            "nigeria", "尼日利亚", "egypt", "埃及",
                            "pakistan", "巴基斯坦", "venezuela", "委内瑞拉"]
        for _, row in geo.iterrows():
            region = str(row.get("region", "")).lower()
            share = row.get("revenue_share")
            if share and share > 0.10:
                # 本土市场不算货币风险
                if home and (home in region or region in home):
                    continue
                if any(vc in region for vc in VOLATILE_CURRENCY):
                    r.risks.append(RiskItem("currency", "significant",
                        f"收入 {share:.0%} 来自货币高波动地区: {row.get('region', '')}"))

    # ══════════════════════════════════════════════════════════
    #  综合
    # ══════════════════════════════════════════════════════════

    r.has_catastrophic = bool(r.catastrophic)

    if r.has_catastrophic:
        r.summary = f"存在 {len(r.catastrophic)} 项灾难性风险 → 不能买"
    elif r.significant:
        r.summary = f"{len(r.significant)} 项重大风险，需更大安全边际"
    elif r.moderate:
        r.summary = f"{len(r.moderate)} 项中等风险，可控"
    else:
        r.summary = "未发现重大风险"

    return r


def format_risk(result: RiskResult) -> str:
    lines = [""]
    lines.append("  风险评估")
    lines.append("  ════════════════════════════════════════════════")

    if result.catastrophic:
        lines.append(f"\n  ✗ 灾难性风险（不能买）")
        for ri in result.catastrophic:
            lines.append(f"    ⚠ [{ri.category}] {ri.description}")

    if result.significant:
        lines.append(f"\n  ? 重大风险（需更大安全边际）")
        for ri in result.significant:
            lines.append(f"    · [{ri.category}] {ri.description}")

    if result.moderate:
        lines.append(f"\n  ● 中等风险（关注）")
        for ri in result.moderate:
            lines.append(f"    · [{ri.category}] {ri.description}")

    if not result.risks:
        lines.append(f"\n  ● 未发现重大风险")

    lines.append(f"\n  ════════════════════════════════════════════════")
    lines.append(f"  {result.summary}")
    lines.append("")
    return "\n".join(lines)
