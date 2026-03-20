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
    """Force 1: 债务/信贷周期。

    两层判断:
      总量层: GDP/CPI/利率/信贷增速 → 周期位置
      结构层: 房贷逾期率/金融杠杆/贷款标准/家庭偿付比 → 脆弱度

    总量指标看起来正常但结构指标恶化 = 最危险的状态（2007）。
    """
    f = ForceAssessment(force_name="债务/信贷周期", force_id=1)
    d = macro_data

    # ── 总量指标 ──
    total_mappings = [
        ("fed_funds_rate", "基准利率", "%", ""),
        ("credit_growth", "信贷增速", "%", ""),
        ("total_debt_to_gdp", "总债务/GDP", "%", ">300%=高杠杆经济体"),
        ("unemployment_rate", "失业率", "%", ""),
        ("cpi_actual", "CPI", "%", ""),
        ("gdp_growth_actual", "GDP增速", "%", ""),
    ]
    for key, name, unit, ctx in total_mappings:
        val = d.get(key)
        if val is not None:
            f.indicators.append(Indicator(name=name, value=val, unit=unit, context=ctx))

    # ── 结构指标（2007 case 发现的关键数据）──
    structural_mappings = [
        ("mortgage_delinquency", "房贷逾期率", "%",
         "信贷质量最直接的指标。>4%=压力显现, >8%=危机"),
        ("mortgage_debt_service", "房贷偿付比/收入", "%",
         "家庭用多少收入还房贷。>7%=负担重"),
        ("household_debt_gdp", "家庭债务/GDP", "%",
         "家庭整体杠杆。>80%=历史高位"),
        ("financial_leverage", "金融杠杆指数", "",
         "芝加哥联储子指数。>0=杠杆高于均值, >1=极端"),
        ("lending_standards", "贷款标准收紧%", "%",
         "银行在收紧贷款吗？>20%=显著收紧, >40%=恐慌性收紧。"
         "这是最前瞻的指标——银行先收紧,然后信贷才收缩"),
        ("case_shiller_hpi", "Case-Shiller房价指数", "",
         "房价走势。同比下跌=泡沫破裂信号"),
    ]
    structural_count = 0
    for key, name, unit, ctx in structural_mappings:
        val = d.get(key)
        if val is not None:
            structural_count += 1
            f.indicators.append(Indicator(name=name, value=val, unit=unit, context=ctx))

    # ── 系统判断: 总量 + 结构 综合 ──
    gdp = d.get("gdp_growth_actual", 0)
    credit = d.get("credit_growth", 0)
    cpi = d.get("cpi_actual", 0)

    # 总量判断
    if gdp < 0 and credit < 0:
        total_signal = "衰退+信贷收缩"
        total_score = -2
    elif gdp < 1 or credit < 0:
        total_signal = "增长放缓或信贷收缩"
        total_score = -1
    elif gdp > 3 and credit > 5:
        total_signal = f"GDP {gdp}%+信贷扩张=扩张期"
        total_score = 1
    else:
        total_signal = "周期中段"
        total_score = 0

    # 结构判断（红旗计数）
    red_flags = []
    delinquency = d.get("mortgage_delinquency")
    if delinquency is not None and delinquency > 4:
        red_flags.append(f"房贷逾期率{delinquency}%")
    fin_leverage = d.get("financial_leverage")
    if fin_leverage is not None and fin_leverage > 0.5:
        red_flags.append(f"金融杠杆指数{fin_leverage:+.1f}(高于均值)")
    lending_std = d.get("lending_standards")
    if lending_std is not None and lending_std > 20:
        red_flags.append(f"贷款标准收紧{lending_std}%")
    hh_debt = d.get("household_debt_gdp")
    if hh_debt is not None and hh_debt > 80:
        red_flags.append(f"家庭债务/GDP {hh_debt}%(历史高位)")
    mort_service = d.get("mortgage_debt_service")
    if mort_service is not None and mort_service > 7:
        red_flags.append(f"房贷偿付比{mort_service}%(负担重)")

    # 综合: 总量 × 结构
    if len(red_flags) >= 3:
        f.system_direction = ForceDirection.STRONGLY_NEGATIVE
        f.system_reasoning = f"结构性风险极端({len(red_flags)}个红旗): {'; '.join(red_flags[:3])}"
        f.system_confidence = 0.9
    elif len(red_flags) >= 1:
        if total_score >= 0:
            f.system_direction = ForceDirection.NEGATIVE
            f.system_reasoning = f"表面正常但结构恶化: 总量={total_signal}, 但{'; '.join(red_flags)}"
            f.system_confidence = 0.7
            f.system_contradictions.append(
                f"总量指标说'{total_signal}'但结构指标有{len(red_flags)}个红旗——"
                "这是最危险的状态(泡沫顶部总量指标总是滞后)")
        else:
            f.system_direction = ForceDirection.STRONGLY_NEGATIVE
            f.system_reasoning = f"总量恶化+结构恶化: {total_signal} + {'; '.join(red_flags)}"
            f.system_confidence = 0.85
    elif total_score <= -2:
        f.system_direction = ForceDirection.STRONGLY_NEGATIVE
        f.system_reasoning = f"总量严重恶化: {total_signal}"
        f.system_confidence = 0.7
    elif total_score < 0:
        f.system_direction = ForceDirection.NEGATIVE
        f.system_reasoning = total_signal
        f.system_confidence = 0.6
    elif total_score > 0:
        f.system_direction = ForceDirection.POSITIVE
        f.system_reasoning = total_signal
        f.system_confidence = 0.6
    else:
        f.system_direction = ForceDirection.NEUTRAL
        f.system_reasoning = total_signal
        f.system_confidence = 0.5

    # 高亮
    if cpi > 5:
        f.system_highlights.append(f"⚠ 通胀 {cpi}% 远超目标")
    if credit < -3:
        f.system_highlights.append(f"⚠ 信贷收缩 {credit}% — 历史上常伴随衰退")
    if lending_std is not None and lending_std > 30:
        f.system_highlights.append(
            f"⚠ 银行贷款标准大幅收紧({lending_std}%) — 这是信贷危机的前兆")
    if delinquency is not None and delinquency > 6:
        f.system_highlights.append(
            f"⚠ 房贷逾期率 {delinquency}% — 房地产信贷质量严重恶化")

    if structural_count == 0:
        f.system_highlights.append(
            "注: 缺少结构性数据(逾期率/杠杆/贷款标准)——仅靠总量判断，信心有限")

    return f


# ══════════════════════════════════════════════════════════════
#  Force 2: 内部秩序
# ══════════════════════════════════════════════════════════════


def assess_force2_internal_order(data: dict | None = None) -> ForceAssessment:
    """Force 2: 内部秩序——贫富分化、政治极化、社会稳定。

    达利欧: 内部秩序恶化→政策不可预测→资产波动加大→避险需求上升
    历史模式: 贫富分化极端+政治极化→民粹主义→激进政策(加税/管制/贸易保护)
    """
    f = ForceAssessment(force_name="内部秩序", force_id=2)
    d = data or {}

    # ── 可量化指标 ──
    # 收入不平等 (FRED: GINIALLRH)
    gini = d.get("gini_coefficient")
    if gini is not None:
        trend = "deteriorating" if gini > 0.40 else "stable"
        f.indicators.append(Indicator(
            name="基尼系数", value=gini, trend=trend,
            context="美国~0.49(高), 北欧~0.27(低). >0.40进入不稳定区间"))

    # 实际工资增速 vs 生产率增速（差距=分配不公）
    wage_gap = d.get("wage_productivity_gap")
    if wage_gap is not None:
        f.indicators.append(Indicator(
            name="工资-生产率缺口", value=wage_gap, unit="%",
            trend="deteriorating" if wage_gap < -1 else "stable",
            context="负值=劳动者没有分享到生产率增长的收益"))

    # 财政赤字/GDP（反映分配政策的激进程度）
    deficit = d.get("fiscal_deficit_to_gdp")
    if deficit is not None:
        f.indicators.append(Indicator(
            name="财政赤字/GDP", value=deficit, unit="%",
            context=">5%=积极财政(可能是应对不平等的再分配)"))

    # 民众信心/消费者信心指数
    consumer_conf = d.get("consumer_confidence")
    if consumer_conf is not None:
        f.indicators.append(Indicator(
            name="消费者信心", value=consumer_conf,
            context="<60=悲观, 60-80=中性, >80=乐观"))

    # ── 系统判断 ──
    signals = []
    if gini is not None and gini > 0.45:
        signals.append("贫富分化极端")
    if wage_gap is not None and wage_gap < -2:
        signals.append("工资严重落后于生产率")
    if deficit is not None and deficit > 8:
        signals.append("财政赤字极高(可能是大规模再分配)")

    if len(signals) >= 2:
        f.system_direction = ForceDirection.NEGATIVE
        f.system_reasoning = f"内部压力上升: {'; '.join(signals)}"
        f.system_confidence = 0.4
    elif signals:
        f.system_direction = ForceDirection.NEGATIVE
        f.system_reasoning = f"内部存在压力: {signals[0]}"
        f.system_confidence = 0.3
    else:
        f.system_direction = ForceDirection.NEUTRAL
        f.system_reasoning = "无明显内部秩序压力（或数据不足）"
        f.system_confidence = 0.2

    f.system_highlights.append(
        "达利欧: 贫富分化→政治极化→民粹主义→激进政策(加税/管制/贸易保护)"
        "→ 对企业盈利和资产价格不利")
    f.system_highlights.append(
        "关键判断需要人: 当前政治环境是否已经极化到影响政策可预测性？")
    return f


# ══════════════════════════════════════════════════════════════
#  Force 3: 外部秩序
# ══════════════════════════════════════════════════════════════


def assess_force3_external_order(data: dict | None = None) -> ForceAssessment:
    """Force 3: 外部秩序——大国博弈、地缘政治、贸易冲突。

    达利欧: 大国冲突→供应链重构→通胀压力→资本流动变化→汇率波动
    历史模式: 守成大国 vs 崛起大国的竞争（修昔底德陷阱）
    """
    f = ForceAssessment(force_name="外部秩序", force_id=3)
    d = data or {}

    # ── 可量化指标 ──
    # 贸易数据变化（出口增速骤降=贸易冲突加剧）
    export_growth = d.get("export_growth")
    if export_growth is not None:
        f.indicators.append(Indicator(
            name="出口增速", value=export_growth, unit="%",
            trend="deteriorating" if export_growth < 0 else "stable",
            context="负增长可能反映贸易壁垒或需求萎缩"))

    # 军费/GDP（上升=紧张加剧）
    military_gdp = d.get("military_spending_gdp")
    if military_gdp is not None:
        f.indicators.append(Indicator(
            name="军费/GDP", value=military_gdp, unit="%",
            context="美国~3.5%, 中国~1.7%. 上升=安全环境恶化"))

    # 关税率变化
    tariff_rate = d.get("avg_tariff_rate")
    if tariff_rate is not None:
        f.indicators.append(Indicator(
            name="平均关税率", value=tariff_rate, unit="%",
            context="2019前~1.5%, 贸易战后~3%+, Trump 2.0→更高"))

    # 外汇储备变化（下降=资本外流/地缘压力）
    fx_reserve_change = d.get("fx_reserve_change_pct")
    if fx_reserve_change is not None:
        f.indicators.append(Indicator(
            name="外汇储备变化", value=fx_reserve_change, unit="%",
            trend="deteriorating" if fx_reserve_change < -5 else "stable"))

    # 油价波动（地缘代理指标——中东冲突→油价飙升）
    oil_change = d.get("oil_price_yoy")
    if oil_change is not None:
        f.indicators.append(Indicator(
            name="油价同比", value=oil_change, unit="%",
            context=">50%通常有地缘供给冲击"))

    # ── 系统判断 ──
    signals = []
    if tariff_rate is not None and tariff_rate > 3:
        signals.append(f"关税率 {tariff_rate}% (贸易壁垒上升)")
    if oil_change is not None and oil_change > 40:
        signals.append(f"油价暴涨 {oil_change}% (可能有地缘冲击)")
    if export_growth is not None and export_growth < -10:
        signals.append(f"出口暴跌 {export_growth}% (贸易收缩)")

    if len(signals) >= 2:
        f.system_direction = ForceDirection.STRONGLY_NEGATIVE
        f.system_reasoning = f"外部秩序显著恶化: {'; '.join(signals)}"
        f.system_confidence = 0.4
    elif signals:
        f.system_direction = ForceDirection.NEGATIVE
        f.system_reasoning = f"外部存在压力: {signals[0]}"
        f.system_confidence = 0.3
    else:
        f.system_direction = ForceDirection.NEUTRAL
        f.system_reasoning = "无明显外部秩序恶化信号（或数据不足）"
        f.system_confidence = 0.2

    f.system_highlights.append(
        "达利欧: 大国冲突→供应链重构→成本上升→通胀→利率上升→资产价格调整")
    f.system_highlights.append(
        "关键判断需要人: 当前地缘冲突是否可能升级？供应链脱钩程度如何？")
    return f


# ══════════════════════════════════════════════════════════════
#  Force 4: 自然之力
# ══════════════════════════════════════════════════════════════


def assess_force4_nature(data: dict | None = None) -> ForceAssessment:
    """Force 4: 自然之力——疫情、气候灾害、资源冲击。

    达利欧: 自然之力是不可预测但影响巨大的外生冲击。
    特点: 突发性强、持续时间不确定、政策响应决定后果好坏。
    COVID 是近代最典型案例——彻底改变了财政/货币政策范式。
    """
    f = ForceAssessment(force_name="自然之力", force_id=4)
    d = data or {}

    # ── 疫情 ──
    pandemic = d.get("pandemic_active", False)
    if pandemic:
        f.indicators.append(Indicator(
            name="活跃疫情", value=1,
            context="全球大流行进行中"))
        pandemic_severity = d.get("pandemic_severity", "unknown")
        f.indicators.append(Indicator(
            name="严重程度", value=None,
            context=f"{pandemic_severity}"))

    # ── 极端天气/气候 ──
    climate_cost = d.get("climate_damage_pct_gdp")
    if climate_cost is not None:
        f.indicators.append(Indicator(
            name="气候灾害损失/GDP", value=climate_cost, unit="%",
            context=">1%=显著经济冲击"))

    # ── 供给冲击 ──
    food_inflation = d.get("food_price_yoy")
    if food_inflation is not None:
        f.indicators.append(Indicator(
            name="食品价格同比", value=food_inflation, unit="%",
            context=">10%=可能有供给冲击（旱灾/出口禁令）"))

    energy_disruption = d.get("energy_supply_disruption")
    if energy_disruption is not None:
        f.indicators.append(Indicator(
            name="能源供给中断", value=energy_disruption,
            context="百万桶/天的损失量"))

    # ── 系统判断 ──
    if pandemic:
        f.system_direction = ForceDirection.STRONGLY_NEGATIVE
        f.system_reasoning = "活跃疫情——经济活动受限，政策不确定性极高"
        f.system_confidence = 0.7
    elif climate_cost is not None and climate_cost > 1.0:
        f.system_direction = ForceDirection.NEGATIVE
        f.system_reasoning = f"气候灾害显著: 损失 {climate_cost}% GDP"
        f.system_confidence = 0.5
    elif food_inflation is not None and food_inflation > 15:
        f.system_direction = ForceDirection.NEGATIVE
        f.system_reasoning = f"食品价格飙升 {food_inflation}%——可能有供给冲击"
        f.system_confidence = 0.4
    else:
        f.system_direction = ForceDirection.NEUTRAL
        f.system_reasoning = "无明显自然冲击"
        f.system_confidence = 0.5

    f.system_highlights.append(
        "自然冲击的特点: 突发、不可预测、政策响应决定后果。"
        "系统能检测正在发生的冲击，但不能预测。")
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

    # R&D 支出增速
    if data and "rd_spending_growth" in data:
        f.indicators.append(Indicator(
            name="R&D支出增速", value=data["rd_spending_growth"], unit="%",
            context="企业研发投入反映对未来技术的信心"))

    # 新行业占 GDP 比重
    if data and "new_sector_gdp_share" in data:
        f.indicators.append(Indicator(
            name="新兴行业/GDP", value=data["new_sector_gdp_share"], unit="%",
            context="上升=结构性转型正在发生"))

    # ── 系统判断 ──
    prod = (data or {}).get("productivity_growth")
    tech_profit = (data or {}).get("tech_sector_profit_growth")
    vc = (data or {}).get("vc_investment_growth")

    signals_positive = []
    if prod is not None and prod > 2.0:
        signals_positive.append(f"生产率增速 {prod}% 超趋势(~1.5%)")
    if tech_profit is not None and tech_profit > 25:
        signals_positive.append(f"科技利润增速 {tech_profit}%")
    if vc is not None and vc > 20:
        signals_positive.append(f"VC 投资增速 {vc}%")

    if len(signals_positive) >= 2:
        f.system_direction = ForceDirection.STRONGLY_POSITIVE
        f.system_reasoning = f"多个技术革命信号: {'; '.join(signals_positive)}"
        f.system_confidence = 0.5
    elif signals_positive:
        f.system_direction = ForceDirection.POSITIVE
        f.system_reasoning = f"技术变革信号: {signals_positive[0]}"
        f.system_confidence = 0.3
    else:
        f.system_direction = ForceDirection.NEUTRAL
        f.system_reasoning = "无明确的技术革命信号——但系统可能遗漏，请人判断"
        f.system_confidence = 0.2

    f.system_highlights.append(
        "达利欧三条规则的第三条: '尽一切可能提升生产率——长期来看这才是最重要的。'"
    )
    f.system_highlights.append(
        "当革命性技术出现时(蒸汽机/电力/互联网/AI)，生产率趋势线本身改变，"
        "所有基于旧趋势线的周期分析都要重新校准。"
    )

    # 矛盾检测
    if prod is not None and prod > 2.0 and (data or {}).get("gdp_growth_actual", 0) < 1:
        f.system_contradictions.append(
            "生产率在上升但 GDP 增速低——可能是技术对就业的替代效应，或者采用还在早期")
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
