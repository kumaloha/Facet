"""
可预测性检测
============
核心问题: 未来的现金流能预测吗？

三条规则:
  1. 护城河不够 → 不可预测
  2. 财务有病（债务压力大）→ 可预测会变差
  3. 业务类型:
     必需品 / 成瘾品 / 基础设施 → 可预测
     不是这三类 → 不可预测

业务分类来源: downstream_segments.product_category 或 company_profile.industry
"""

from __future__ import annotations

from dataclasses import dataclass, field

from polaris.features.types import ComputeContext


# 可预测的业务类型
PREDICTABLE_CATEGORIES = {
    # 必需品
    "food", "beverage", "grocery", "healthcare", "pharma", "medical",
    "water", "sanitation", "housing",
    "smartphone", "consumer_electronics",  # 现代必需品
    # 成瘾品
    "tobacco", "alcohol", "liquor", "beer", "wine", "gaming", "gambling",
    "social_media", "caffeine", "coffee",
    # 基础设施
    "utility", "electricity", "gas", "telecom", "railroad", "pipeline",
    "waste", "water_utility", "toll_road", "airport", "port",
    "insurance", "banking", "payment",
    "operating_system", "cloud_infrastructure",  # 数字基础设施
}

# 不可预测的业务类型
UNPREDICTABLE_CATEGORIES = {
    "fashion", "luxury_fashion", "trend", "entertainment", "movie",
    "media", "advertising", "crypto", "speculative",
}


@dataclass
class PredictabilityResult:
    # 护城河引用
    moat_sufficient: bool | None = None
    moat_detail: str = ""

    # 财务健康
    financial_sick: bool | None = None
    financial_detail: str = ""

    # 业务类型
    business_categories: list[str] = field(default_factory=list)
    category_predictable: bool | None = None
    category_detail: str = ""

    # 综合
    verdict: str = ""  # holds / breaks / unclear
    summary: str = ""


def _feat(ctx: ComputeContext, key: str) -> float | None:
    return ctx.features.get(f"l0.company.{key}")


def assess_predictability(ctx: ComputeContext, moat_depth: str = "unknown") -> PredictabilityResult:
    r = PredictabilityResult()

    # ══════════════════════════════════════════════════════════
    #  1. 护城河不够 → 不可预测
    # ══════════════════════════════════════════════════════════

    if moat_depth in ("extreme", "deep"):
        r.moat_sufficient = True
        r.moat_detail = f"护城河深度: {moat_depth}"
    elif moat_depth == "shallow":
        r.moat_sufficient = None
        r.moat_detail = "护城河浅，可预测性存疑"
    else:
        r.moat_sufficient = False
        r.moat_detail = f"护城河不足 ({moat_depth})，未来不可预测"

    # ══════════════════════════════════════════════════════════
    #  2. 财务有病 → 可预测会变差
    # ══════════════════════════════════════════════════════════

    de = _feat(ctx, "debt_to_equity")
    ic = _feat(ctx, "interest_coverage")
    net_debt_ebitda = _feat(ctx, "net_debt_to_ebitda")

    sick_signals = []
    if de is not None and de > 3.0:
        sick_signals.append(f"D/E = {de:.1f}")
    if ic is not None and ic < 2.0:
        sick_signals.append(f"利息覆盖率 = {ic:.1f}")
    if net_debt_ebitda is not None and net_debt_ebitda > 5.0:
        sick_signals.append(f"净债务/EBITDA = {net_debt_ebitda:.1f}")

    if sick_signals:
        r.financial_sick = True
        r.financial_detail = "财务有病: " + ", ".join(sick_signals) + " → 可预测会持续恶化"
    else:
        r.financial_sick = False
        r.financial_detail = "财务健康"

    # ══════════════════════════════════════════════════════════
    #  3. 业务类型
    # ══════════════════════════════════════════════════════════

    ds = ctx.get_downstream_segments()
    categories_found = []

    # 从 downstream_segments 的 product_category 字段
    if not ds.empty and "product_category" in ds.columns:
        categories_found = ds["product_category"].dropna().str.lower().unique().tolist()

    # 也从 revenue_type 推断
    if not ds.empty and "revenue_type" in ds.columns:
        rev_types = ds["revenue_type"].dropna().str.lower().unique().tolist()
        # 某些 revenue_type 暗示业务类型
        for rt in rev_types:
            if rt in ("utility", "toll", "insurance", "banking"):
                categories_found.append(rt)

    r.business_categories = categories_found

    if categories_found:
        predictable = [c for c in categories_found if c in PREDICTABLE_CATEGORIES]
        unpredictable = [c for c in categories_found if c in UNPREDICTABLE_CATEGORIES]

        if predictable:
            r.category_predictable = True
            r.category_detail = f"业务类型: {', '.join(predictable)} → 必需品/成瘾品/基础设施"
        elif unpredictable:
            r.category_predictable = False
            r.category_detail = f"业务类型: {', '.join(unpredictable)} → 潮流/投机性，不可预测"
        else:
            r.category_predictable = None
            r.category_detail = f"业务类型: {', '.join(categories_found)} → 未归类"
    else:
        r.category_predictable = None
        r.category_detail = "无业务类型数据"

    # ══════════════════════════════════════════════════════════
    #  综合判定
    # ══════════════════════════════════════════════════════════

    # 财务有病 → 可预测（会变差），这不是好事
    if r.financial_sick:
        r.verdict = "breaks"
        r.summary = r.financial_detail
        return r

    # 护城河不够 → 不可预测
    if r.moat_sufficient is False:
        r.verdict = "breaks"
        r.summary = r.moat_detail
        return r

    # 业务类型判断
    if r.category_predictable is True:
        r.verdict = "holds"
        r.summary = r.category_detail
        if r.moat_sufficient is True:
            r.summary += f"，护城河 {moat_depth}"
    elif r.category_predictable is False:
        r.verdict = "breaks"
        r.summary = r.category_detail
    elif r.moat_sufficient is True:
        # 没有业务分类但护城河强 → 给个存疑
        r.verdict = "unclear"
        r.summary = f"护城河 {moat_depth}，但缺业务类型数据无法确认可预测性"
    else:
        r.verdict = "unclear"
        r.summary = "数据不足"

    return r


# ══════════════════════════════════════════════════════════════
#  格式化
# ══════════════════════════════════════════════════════════════

def format_predictability(result: PredictabilityResult) -> str:
    lines = [""]
    lines.append("  可预测性检测")
    lines.append("  ════════════════════════════════════════════════")

    # 护城河
    mark = "●" if result.moat_sufficient is True else (
        "✗" if result.moat_sufficient is False else "?")
    lines.append(f"\n  {mark} 护城河: {result.moat_detail}")

    # 财务
    mark = "●" if not result.financial_sick else "✗"
    lines.append(f"  {mark} 财务: {result.financial_detail}")

    # 业务类型
    mark = "●" if result.category_predictable is True else (
        "✗" if result.category_predictable is False else "?")
    lines.append(f"  {mark} 业务: {result.category_detail}")

    lines.append(f"\n  ════════════════════════════════════════════════")
    verdict_labels = {"holds": "可预测", "breaks": "不可预测", "unclear": "存疑"}
    lines.append(f"  {verdict_labels.get(result.verdict, result.verdict)}: {result.summary}")
    lines.append("")
    return "\n".join(lines)
