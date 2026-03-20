"""
达利欧五大力量认知框架
======================

五大力量同时作用于经济和资产价格:
  1. 债务/信贷周期 — 已有因果引擎(dalio.py)
  2. 内部秩序 — 贫富分化、政治极化、社会凝聚力
  3. 外部秩序 — 大国博弈、地缘政治、贸易冲突
  4. 自然之力 — 疫情、气候、资源冲击
  5. 人类创造力/技术 — 革命性技术、生产率跳升

每个力量:
  - 数据层: 客观指标（系统自动采集）
  - 认知层: 系统的理解（系统判断，标注为系统观点）
  - 决策层: 人的最终判断（人填入，覆盖系统判断）

系统辅助人思考，不替代人决策。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class ForceDirection(str, Enum):
    STRONGLY_POSITIVE = "strongly_positive"    # 这个力量强烈利好经济/资产
    POSITIVE = "positive"
    NEUTRAL = "neutral"
    NEGATIVE = "negative"
    STRONGLY_NEGATIVE = "strongly_negative"    # 这个力量强烈利空


@dataclass
class Indicator:
    """单个指标。"""
    name: str
    value: float | None = None
    unit: str = ""
    trend: str = ""           # "improving" / "deteriorating" / "stable" / "unknown"
    context: str = ""         # 这个数字意味着什么（人可读）


@dataclass
class ForceAssessment:
    """对一个力量的认知——分三层。"""

    # 身份
    force_name: str
    force_id: int             # 1-5

    # 数据层（客观，系统自动）
    indicators: list[Indicator] = field(default_factory=list)

    # 认知层（系统的理解，可能错）
    system_direction: ForceDirection = ForceDirection.NEUTRAL
    system_reasoning: str = ""         # 系统为什么这么判断
    system_confidence: float = 0.0     # 系统对自己判断的信心 0-1
    system_highlights: list[str] = field(default_factory=list)  # 系统认为值得注意的点
    system_contradictions: list[str] = field(default_factory=list)  # 系统发现的矛盾

    # 决策层（人的判断，覆盖系统）
    human_direction: ForceDirection | None = None
    human_reasoning: str = ""
    human_override: bool = False       # 人是否覆盖了系统判断

    @property
    def effective_direction(self) -> ForceDirection:
        """最终生效的方向——人的判断优先。"""
        if self.human_override and self.human_direction is not None:
            return self.human_direction
        return self.system_direction


@dataclass
class FiveForcesView:
    """五大力量的完整视图。"""
    forces: list[ForceAssessment] = field(default_factory=list)
    principal_contradiction: str = ""   # 系统认为的主要矛盾
    human_principal_contradiction: str = ""  # 人认为的主要矛盾
    snapshot_date: str = ""

    def get_force(self, force_id: int) -> ForceAssessment | None:
        for f in self.forces:
            if f.force_id == force_id:
                return f
        return None


# ══════════════════════════════════════════════════════════════
#  Force 1: 债务/信贷周期
# ══════════════════════════════════════════════════════════════
# 已有完整实现在 dalio.py 的因果引擎
# 这里只定义接口，实际计算委托给因果引擎


def assess_force1_debt_cycle(macro_data: dict) -> ForceAssessment:
    """Force 1: 债务/信贷周期。委托给现有因果引擎。"""
    f = ForceAssessment(force_name="债务/信贷周期", force_id=1)

    # 指标
    mappings = [
        ("fed_funds_rate", "基准利率", "%"),
        ("credit_growth", "信贷增速", "%"),
        ("total_debt_to_gdp", "总债务/GDP", "%"),
        ("unemployment_rate", "失业率", "%"),
        ("cpi_actual", "CPI", "%"),
        ("gdp_growth_actual", "GDP增速", "%"),
    ]
    for key, name, unit in mappings:
        val = macro_data.get(key)
        if val is not None:
            f.indicators.append(Indicator(name=name, value=val, unit=unit))

    # 系统判断（简化版——完整版在 dalio.py 因果引擎）
    gdp = macro_data.get("gdp_growth_actual", 0)
    credit = macro_data.get("credit_growth", 0)
    cpi = macro_data.get("cpi_actual", 0)

    if gdp < 0 and credit < 0:
        f.system_direction = ForceDirection.STRONGLY_NEGATIVE
        f.system_reasoning = f"GDP {gdp}% + 信贷 {credit}% = 衰退+信贷收缩"
    elif gdp < 1 or credit < 0:
        f.system_direction = ForceDirection.NEGATIVE
        f.system_reasoning = "增长放缓或信贷收缩"
    elif gdp > 3 and credit > 5:
        f.system_direction = ForceDirection.POSITIVE
        f.system_reasoning = f"GDP {gdp}% + 信贷扩张 = 扩张期"
    else:
        f.system_direction = ForceDirection.NEUTRAL
        f.system_reasoning = "周期中段"

    if cpi > 5:
        f.system_highlights.append(f"⚠ 通胀 {cpi}% 远超目标")
    if credit < -3:
        f.system_highlights.append(f"⚠ 信贷收缩 {credit}% — 历史上常伴随衰退")

    f.system_confidence = 0.7  # 债务周期是我们最成熟的模块
    return f


# ══════════════════════════════════════════════════════════════
#  Force 2: 内部秩序
# ══════════════════════════════════════════════════════════════


def assess_force2_internal_order(data: dict | None = None) -> ForceAssessment:
    """Force 2: 内部秩序——贫富分化、政治极化、社会稳定。"""
    f = ForceAssessment(force_name="内部秩序", force_id=2)

    if data:
        if "gini_coefficient" in data:
            f.indicators.append(Indicator(
                name="基尼系数", value=data["gini_coefficient"],
                context="越高越不平等。美国约0.39，北欧约0.25"))
        if "political_polarization" in data:
            f.indicators.append(Indicator(
                name="政治极化指数", value=data["political_polarization"],
                context="跨党派投票率、国会僵局频率"))
        if "social_unrest_index" in data:
            f.indicators.append(Indicator(
                name="社会动荡指数", value=data["social_unrest_index"],
                context="罢工、抗议、暴力事件频率"))

    f.system_direction = ForceDirection.NEUTRAL
    f.system_reasoning = "数据不足，需要人类输入判断"
    f.system_confidence = 0.2  # 很低——这个维度主要靠人判断
    f.system_highlights.append("这个力量主要靠人的判断，系统只能提供框架")
    return f


# ══════════════════════════════════════════════════════════════
#  Force 3: 外部秩序
# ══════════════════════════════════════════════════════════════


def assess_force3_external_order(data: dict | None = None) -> ForceAssessment:
    """Force 3: 外部秩序——大国博弈、地缘政治、贸易冲突。"""
    f = ForceAssessment(force_name="外部秩序", force_id=3)

    if data:
        if "trade_conflict_intensity" in data:
            f.indicators.append(Indicator(
                name="贸易冲突强度", value=data["trade_conflict_intensity"],
                context="关税覆盖率、贸易限制数量"))
        if "military_tension" in data:
            f.indicators.append(Indicator(
                name="军事紧张度", value=data["military_tension"],
                context="冲突热点数量、军费增速"))
        if "sanctions_count" in data:
            f.indicators.append(Indicator(
                name="制裁数量", value=data["sanctions_count"]))

    f.system_direction = ForceDirection.NEUTRAL
    f.system_reasoning = "地缘政治判断主要靠人——系统无法自动评估"
    f.system_confidence = 0.1
    f.system_highlights.append("地缘风险是定性判断，需要人输入")
    return f


# ══════════════════════════════════════════════════════════════
#  Force 4: 自然之力
# ══════════════════════════════════════════════════════════════


def assess_force4_nature(data: dict | None = None) -> ForceAssessment:
    """Force 4: 自然之力——疫情、气候灾害、资源冲击。"""
    f = ForceAssessment(force_name="自然之力", force_id=4)

    if data:
        if "pandemic_active" in data:
            f.indicators.append(Indicator(
                name="活跃疫情", value=1 if data["pandemic_active"] else 0,
                context="是否有全球性大流行"))
        if "climate_damage_pct_gdp" in data:
            f.indicators.append(Indicator(
                name="气候损失/GDP", value=data["climate_damage_pct_gdp"], unit="%"))
        if "commodity_supply_shock" in data:
            f.indicators.append(Indicator(
                name="大宗供给冲击", value=data["commodity_supply_shock"],
                context="正值=供给不足，负值=供给过剩"))

    f.system_direction = ForceDirection.NEUTRAL
    f.system_reasoning = "无活跃自然冲击（如有请人类输入）"
    f.system_confidence = 0.3
    return f


# ══════════════════════════════════════════════════════════════
#  Force 5: 人类创造力/技术
# ══════════════════════════════════════════════════════════════


def assess_force5_technology(data: dict | None = None) -> ForceAssessment:
    """Force 5: 人类创造力/技术革命。

    达利欧: "尽一切可能提升生产率——长期来看这才是最重要的。"
    当革命性技术出现时，它改变生产率趋势线本身。
    """
    f = ForceAssessment(force_name="人类创造力/技术", force_id=5)

    if data:
        if "productivity_growth" in data:
            f.indicators.append(Indicator(
                name="生产率增速", value=data["productivity_growth"], unit="%",
                context="长期趋势约1.5%。超过2%可能有技术革命"))
        if "tech_sector_profit_growth" in data:
            f.indicators.append(Indicator(
                name="科技板块利润增速", value=data["tech_sector_profit_growth"], unit="%",
                context="反映技术变革的商业化程度"))
        if "vc_investment_growth" in data:
            f.indicators.append(Indicator(
                name="VC投资增速", value=data["vc_investment_growth"], unit="%",
                context="反映对新技术的资本投入"))
        if "patent_growth" in data:
            f.indicators.append(Indicator(
                name="专利申请增速", value=data["patent_growth"], unit="%"))
        if "ai_adoption_rate" in data:
            f.indicators.append(Indicator(
                name="AI采用率", value=data["ai_adoption_rate"], unit="%",
                context="企业使用AI的比例"))

    # 系统判断（可以被人覆盖）
    prod = (data or {}).get("productivity_growth")
    tech_profit = (data or {}).get("tech_sector_profit_growth")

    if prod is not None and prod > 2.5:
        f.system_direction = ForceDirection.STRONGLY_POSITIVE
        f.system_reasoning = f"生产率增速 {prod}% 远超趋势——可能有革命性技术正在展开"
    elif tech_profit is not None and tech_profit > 30:
        f.system_direction = ForceDirection.POSITIVE
        f.system_reasoning = f"科技利润暴增 {tech_profit}%——技术变革正在商业化"
    else:
        f.system_direction = ForceDirection.NEUTRAL
        f.system_reasoning = "无明确的技术革命信号（但系统可能遗漏——请人类判断）"

    f.system_confidence = 0.3  # 技术革命的判断主要靠人
    f.system_highlights.append(
        "达利欧: 生产率趋势线是长期最重要的力量。"
        "当革命性技术出现时(如AI)，所有基于旧趋势线的分析都要重新校准。"
    )
    return f


# ══════════════════════════════════════════════════════════════
#  综合：五大力量视图
# ══════════════════════════════════════════════════════════════


def build_five_forces_view(
    macro_data: dict | None = None,
    internal_data: dict | None = None,
    external_data: dict | None = None,
    nature_data: dict | None = None,
    tech_data: dict | None = None,
) -> FiveForcesView:
    """构建五大力量视图。

    系统自动填充能填的，标注信心水平。
    人来判断系统填不了的，特别是主要矛盾。
    """
    view = FiveForcesView()

    view.forces = [
        assess_force1_debt_cycle(macro_data or {}),
        assess_force2_internal_order(internal_data),
        assess_force3_external_order(external_data),
        assess_force4_nature(nature_data),
        assess_force5_technology(tech_data),
    ]

    # 系统尝试判断主要矛盾（信心低——通常应由人判断）
    strongest = max(view.forces, key=lambda f: abs(
        {"strongly_positive": 2, "positive": 1, "neutral": 0,
         "negative": -1, "strongly_negative": -2}[f.system_direction.value]
    ) * f.system_confidence)

    if strongest.system_confidence > 0.5:
        view.principal_contradiction = (
            f"系统认为 Force {strongest.force_id}({strongest.force_name}) "
            f"是当前主要矛盾: {strongest.system_reasoning}"
        )
    else:
        view.principal_contradiction = "系统信心不足以判断主要矛盾——请人类输入"

    return view


# ══════════════════════════════════════════════════════════════
#  格式化
# ══════════════════════════════════════════════════════════════


def format_five_forces(view: FiveForcesView) -> str:
    """格式化五大力量报告。"""
    dir_labels = {
        ForceDirection.STRONGLY_POSITIVE: "强利好 ▲▲",
        ForceDirection.POSITIVE: "利好 ▲",
        ForceDirection.NEUTRAL: "中性 ─",
        ForceDirection.NEGATIVE: "利空 ▼",
        ForceDirection.STRONGLY_NEGATIVE: "强利空 ▼▼",
    }

    lines = [""]
    lines.append("  达利欧五大力量")
    lines.append("  ════════════════════════════════════════════════")

    for f in view.forces:
        eff = f.effective_direction
        override = " [人工覆盖]" if f.human_override else ""
        conf = f"信心{f.system_confidence:.0%}"
        lines.append(f"\n  Force {f.force_id}: {f.force_name}")
        lines.append(f"    方向: {dir_labels[eff]}{override} ({conf})")
        lines.append(f"    判断: {f.system_reasoning}")

        for ind in f.indicators:
            val_str = f"{ind.value:.1f}{ind.unit}" if ind.value is not None else "无数据"
            trend_str = f" ({ind.trend})" if ind.trend else ""
            lines.append(f"    · {ind.name}: {val_str}{trend_str}")

        for h in f.system_highlights:
            lines.append(f"    ⚡ {h}")

        for c in f.system_contradictions:
            lines.append(f"    ⚠ 矛盾: {c}")

        if f.human_reasoning:
            lines.append(f"    👤 人的判断: {f.human_reasoning}")

    lines.append(f"\n  {'═' * 48}")
    if view.human_principal_contradiction:
        lines.append(f"  主要矛盾 [人]: {view.human_principal_contradiction}")
    else:
        lines.append(f"  主要矛盾 [系统]: {view.principal_contradiction}")

    lines.append("")
    return "\n".join(lines)
