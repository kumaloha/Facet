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


def assess_risk(ctx: ComputeContext) -> RiskResult:
    r = RiskResult()

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
    #  客户集中
    # ══════════════════════════════════════════════════════════

    top_cust = _feat(ctx, "top_customer_concentration")
    if top_cust is not None:
        if top_cust > 0.50:
            r.risks.append(RiskItem("concentration", "catastrophic",
                f"最大客户占收入 {top_cust:.0%}，严重依赖"))
        elif top_cust > 0.30:
            r.risks.append(RiskItem("concentration", "significant",
                f"最大客户占收入 {top_cust:.0%}"))

    top3 = _feat(ctx, "top3_customer_concentration")
    if top3 is not None and top3 > 0.70:
        r.risks.append(RiskItem("concentration", "significant",
            f"前三客户占收入 {top3:.0%}"))

    # ══════════════════════════════════════════════════════════
    #  供应链
    # ══════════════════════════════════════════════════════════

    sole_source = _feat(ctx, "sole_source_pct")
    if sole_source is not None and sole_source > 0.50:
        r.risks.append(RiskItem("supply_chain", "significant",
            f"供应商 sole source 占比 {sole_source:.0%}"))

    # ══════════════════════════════════════════════════════════
    #  关键人依赖
    # ══════════════════════════════════════════════════════════

    mgmt_own = _feat(ctx, "mgmt_ownership_pct")
    if mgmt_own is not None and mgmt_own > 30:
        r.risks.append(RiskItem("key_person", "significant",
            f"创始人/CEO 持股 {mgmt_own:.0%}，关键人依赖"))

    # ══════════════════════════════════════════════════════════
    #  财务结构
    # ══════════════════════════════════════════════════════════

    de = _feat(ctx, "debt_to_equity")
    if de is not None and de > 5.0:
        r.risks.append(RiskItem("financial", "catastrophic",
            f"D/E = {de:.1f}，极高杠杆"))
    elif de is not None and de > 3.0:
        r.risks.append(RiskItem("financial", "significant",
            f"D/E = {de:.1f}，高杠杆"))

    ic = _feat(ctx, "interest_coverage")
    if ic is not None and ic < 1.5:
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
