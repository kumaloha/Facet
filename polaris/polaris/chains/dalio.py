"""
达利欧因果链（双线结构）
========================

线 1: 周期定位 → 类型选择（串行，前一步是后一步的前提）
  ① 宏观象限定位
  ② 传导暴露分析
  ③ 主动押注生成
  ④ 风险平价基线

线 2: 认知盲区（并行，始终运行）
  A. 尾部风险
  B. 政策路径

两线交叉 → 生成对冲规格 → 输出 DalioResult。

与巴菲特链的关键区别:
- 输入是 MacroContext（宏观数据），不是 ComputeContext（公司数据）
- 作用对象是资产类型，不是单一公司
- 结论有时效性，环境变则结论变
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import math

from polaris.principles.dimensions import (
    CycleRegime,
    DalioResult,
    HedgeSpec,
    SchoolScore,
    School,
    Tilt,
)


# ── 宏观上下文 ─────────────────────────────────────────────────



@dataclass
class MacroSnapshot:
    """单期宏观数据快照（用于构建时间序列）。"""
    date: str                                    # YYYY-MM-DD 或 YYYY-MM
    gdp_growth: float | None = None              # GDP 同比增速 (%)
    cpi: float | None = None                     # CPI 同比 (%)
    fed_funds_rate: float | None = None          # 联邦基金利率 (%)
    credit_growth: float | None = None           # 信贷同比增速 (%)
    unemployment_rate: float | None = None       # 失业率 (%)
    treasury_10y: float | None = None            # 10Y 国债收益率 (%)
    treasury_2y: float | None = None             # 2Y 国债收益率 (%)
    total_debt_to_gdp: float | None = None       # 总债务/GDP (%)
    vix: float | None = None                     # VIX 波动率指数


@dataclass
class CountryProfile:
    """国别经济结构参数 — 让因果引擎适应不同国家的国情。

    这不是参数调优，是结构性差异：
    - 美国 70% GDP 是消费 → 消费者健康权重高
    - 中国 30% GDP 是房地产 → 债务/信贷权重高
    - 日本出口驱动 → 汇率比利率重要
    - 欧洲多国一币 → 主权利差风险
    """
    name: str = "default"

    # 经济结构权重（0-1，影响各机制节点的相对重要性）
    consumption_weight: float = 0.5       # 消费在 GDP 中的重要性
    credit_channel_weight: float = 0.5    # 信贷传导的重要性（中国极高）
    property_sensitivity: float = 0.3     # 房地产对经济的敏感度
    export_sensitivity: float = 0.3       # 出口/汇率对经济的敏感度
    policy_activism: float = 0.5          # 政府干预经济的力度

    # 传导效率修正
    policy_to_equity: float = 0.5         # 政策宽松→股市的传导效率（美国高，中国低）
    safe_haven_currency: bool = False     # 本币是避险货币（美元、日元、瑞郎）
    sovereign_spread_risk: bool = False   # 有主权利差风险（欧元区边缘国）


# 预定义的国别 profile
COUNTRY_PROFILES = {
    "US": CountryProfile(
        name="US",
        consumption_weight=0.7,       # 美国 GDP 70% 是消费
        credit_channel_weight=0.5,    # 市场化信贷
        property_sensitivity=0.3,
        export_sensitivity=0.15,
        policy_activism=0.5,
        policy_to_equity=0.7,         # 美国政策→股市传导高效（QE→估值扩张）
        safe_haven_currency=True,
    ),
    "JP": CountryProfile(
        name="JP",
        consumption_weight=0.55,
        credit_channel_weight=0.6,
        property_sensitivity=0.4,
        export_sensitivity=0.5,
        policy_activism=0.7,
        policy_to_equity=0.6,         # 安倍 QE 对股市有效但不如美国
    ),
    "EU": CountryProfile(
        name="EU",
        consumption_weight=0.5,
        credit_channel_weight=0.6,
        property_sensitivity=0.25,
        export_sensitivity=0.4,
        policy_activism=0.4,
        policy_to_equity=0.5,         # ECB QE 对股市效果一般
        sovereign_spread_risk=True,
    ),
    "CN": CountryProfile(
        name="CN",
        consumption_weight=0.35,
        credit_channel_weight=0.3,
        property_sensitivity=0.6,
        export_sensitivity=0.35,
        policy_activism=0.9,
        policy_to_equity=0.2,         # 中国政策→股市传导极低（信贷流入基建/地产，不流入盈利）
    ),
}


@dataclass
class MacroContext:
    """达利欧链的输入: 宏观经济数据快照。

    所有字段可选 — 缺失字段会导致对应环节产出 UNCLEAR。
    数据源: FRED API / 手动输入。
    """
    # 增长
    gdp_growth_actual: float | None = None       # 实际 GDP 同比增速 (%)
    gdp_growth_expected: float | None = None      # 市场预期 GDP 增速 (%)

    # 通胀
    cpi_actual: float | None = None               # 实际 CPI 同比 (%)
    cpi_expected: float | None = None             # 市场预期 CPI (%)

    # 利率
    fed_funds_rate: float | None = None           # 联邦基金利率 (%)
    treasury_10y: float | None = None             # 10 年期国债收益率 (%)
    treasury_2y: float | None = None              # 2 年期国债收益率 (%)

    # 信贷
    credit_growth: float | None = None            # 银行信贷同比增速 (%)
    total_debt_to_gdp: float | None = None        # 总债务/GDP (%)

    # 债务结构拆解（可选，有则精细化偿债负担计算）
    household_debt_to_income: float | None = None  # 家庭债务/可支配收入 (%)
    corporate_debt_to_gdp: float | None = None     # 非金融企业债务/GDP (%)
    government_debt_to_gdp: float | None = None    # 政府债务/GDP (%)
    financial_sector_leverage: float | None = None  # 金融部门杠杆率 (倍)

    # 市场
    vix: float | None = None                      # VIX 波动率指数
    sp500_earnings_yield: float | None = None     # S&P 500 盈利收益率 (%)
    dxy_yoy: float | None = None                  # 美元指数同比变化 (%, 正=强美元)

    # 就业
    unemployment_rate: float | None = None        # 失业率 (%)
    initial_claims: float | None = None           # 初次申请失业金人数 (千)

    # 实体经济高频
    pmi_manufacturing: float | None = None        # 制造业 PMI (50=荣枯线)
    pmi_services: float | None = None             # 服务业 PMI
    retail_sales_growth: float | None = None      # 零售销售同比 (%)
    housing_starts_growth: float | None = None    # 新屋开工同比 (%)
    industrial_production_growth: float | None = None  # 工业产出同比 (%)

    # 企业盈利
    earnings_growth: float | None = None          # S&P 500 EPS 同比增速 (%)
    earnings_revision: float | None = None        # 盈利修正比 (上修-下修)/(总数), -1到+1

    # 流动性
    m2_growth: float | None = None                # M2 货币供应同比增速 (%)

    # 政策空间
    fiscal_deficit_to_gdp: float | None = None    # 财政赤字/GDP (%)

    # 历史上下文（用于百分位归一化，避免硬编码阈值）
    # 如果提供，机制节点用百分位而非绝对值做归一化
    # 不同国家的"正常"水平完全不同，这些字段让引擎自适应
    hist_rate_median: float | None = None          # 历史利率中位数 (%)
    hist_rate_p25: float | None = None             # 历史利率 25th 百分位
    hist_rate_p75: float | None = None             # 历史利率 75th 百分位
    hist_unemployment_median: float | None = None  # 历史失业率中位数
    hist_gdp_median: float | None = None           # 历史 GDP 增速中位数
    hist_credit_growth_median: float | None = None # 历史信贷增速中位数 (%)
    hist_cpi_median: float | None = None           # 历史 CPI 中位数 (%)

    # 国别 profile（影响因果引擎权重）
    country: str = "US"                           # US / JP / EU / CN

    # ── 替代数据三角验证（官方数据可能失真的市场通用）──
    # 原则: 用多个独立、难以操纵的数据源交叉验证官方统计
    #
    # 物理活动指标（难以伪造，反映真实经济产出）
    alt_electricity_growth: float | None = None     # 发电量/用电量同比 (%)
    alt_freight_growth: float | None = None         # 货运量同比 (%) — 铁路/公路/港口
    alt_cement_steel_growth: float | None = None    # 水泥+钢铁产量同比 (%) — 工业/基建代理
    # 贸易验证（可被交易对手国交叉验证）
    alt_export_growth: float | None = None          # 出口同比 (%)
    alt_import_growth: float | None = None          # 进口同比 (%) — 最难伪造（对手国有记录）
    # 消费验证（微观高频数据）
    alt_auto_sales_growth: float | None = None      # 汽车销量同比 (%)
    alt_retail_pmi: float | None = None             # 零售/服务业 PMI（如财新服务业 PMI）
    # 房地产（新兴市场通用的关键风险部门）
    alt_property_investment_growth: float | None = None  # 房地产/建筑投资同比 (%)
    alt_property_sales_growth: float | None = None      # 房屋销售面积/金额同比 (%)
    alt_land_revenue_growth: float | None = None        # 土地出让/拍卖收入同比 (%)
    # 信贷真实规模（含表外/影子银行）
    alt_broad_credit_growth: float | None = None    # 广义信贷同比 (%) — 社融/含影子银行
    # 资本流动（金融市场信号）
    alt_fx_reserve_change: float | None = None      # 外汇储备变化 (十亿$, 负=外流)
    alt_capital_flow: float | None = None           # 资本账户净流入 (十亿$, 负=外流)

    # 快照时间
    snapshot_date: str = ""                       # YYYY-MM-DD

    # ── 轨迹信号（从时间序列计算，非手工输入）──
    gdp_momentum: float | None = None            # GDP 增速的变化量 (当期 - 上期, pp)
    credit_impulse: float | None = None          # 信贷增速的变化量 (二阶导数, pp)
    inflation_momentum: float | None = None      # CPI 的变化量 (pp)
    rate_direction: float | None = None          # 利率的变化量 (pp, 正=加息 负=降息)
    unemployment_direction: float | None = None  # 失业率的变化量 (pp, 正=恶化)

    @classmethod
    def from_series(
        cls,
        snapshots: list[MacroSnapshot],
        expected_gdp: float = 2.0,
        expected_cpi: float = 2.0,
    ) -> "MacroContext":
        """从多期快照计算当期值 + 轨迹信号。最后一个 snapshot 是当期。

        至少需要 2 个快照才能计算轨迹；1 个快照只填当期值。
        """
        if not snapshots:
            return cls()

        curr = snapshots[-1]

        ctx = cls(
            gdp_growth_actual=curr.gdp_growth,
            gdp_growth_expected=expected_gdp,
            cpi_actual=curr.cpi,
            cpi_expected=expected_cpi,
            fed_funds_rate=curr.fed_funds_rate,
            credit_growth=curr.credit_growth,
            unemployment_rate=curr.unemployment_rate,
            treasury_10y=curr.treasury_10y,
            treasury_2y=curr.treasury_2y,
            total_debt_to_gdp=curr.total_debt_to_gdp,
            vix=curr.vix,
            snapshot_date=curr.date,
        )

        if len(snapshots) < 2:
            return ctx

        prev = snapshots[-2]

        # 一阶差分: 当期 - 上期
        def _delta(a: float | None, b: float | None) -> float | None:
            if a is not None and b is not None:
                return round(a - b, 4)
            return None

        ctx.gdp_momentum = _delta(curr.gdp_growth, prev.gdp_growth)
        ctx.credit_impulse = _delta(curr.credit_growth, prev.credit_growth)
        ctx.inflation_momentum = _delta(curr.cpi, prev.cpi)
        ctx.rate_direction = _delta(curr.fed_funds_rate, prev.fed_funds_rate)
        ctx.unemployment_direction = _delta(curr.unemployment_rate, prev.unemployment_rate)

        return ctx


class Verdict(str, Enum):
    HOLDS = "holds"
    BREAKS = "breaks"
    UNCLEAR = "unclear"


@dataclass
class ChainStep:
    """链中的一步。"""
    name: str
    principle: str
    verdict: Verdict = Verdict.UNCLEAR
    detail: str = ""
    evidence: list[str] = field(default_factory=list)


@dataclass
class MechanismNode:
    """因果图中间机制节点。"""
    name: str
    value: float          # -1.0 到 +1.0
    confidence: float     # 0-1
    inputs_used: list[str] = field(default_factory=list)
    detail: str = ""


@dataclass
class AssetImpact:
    """因果图到单个资产的汇总影响。"""
    asset_type: str
    raw_score: float
    contributing_paths: list[str] = field(default_factory=list)
    direction: str = ""
    magnitude: float = 0.0


@dataclass
class CausalGraphResult:
    """因果传导引擎完整输出。"""
    nodes: dict[str, MechanismNode] = field(default_factory=dict)
    asset_impacts: list[AssetImpact] = field(default_factory=list)
    converged_in: int = 0
    feedback_active: list[str] = field(default_factory=list)


@dataclass
class TailRiskResult:
    """尾部风险评估。"""
    risks: list[str] = field(default_factory=list)
    severity: str = "none"  # none / moderate / severe
    detail: str = ""


@dataclass
class PolicyPathResult:
    """政策路径分析。"""
    rate_room: float | None = None        # 降息空间 (bp)
    fiscal_room: str = "unknown"          # ample / limited / exhausted
    likely_tools: list[str] = field(default_factory=list)
    impact_on_assets: dict[str, str] = field(default_factory=dict)
    detail: str = ""


@dataclass
class DalioChainResult:
    """达利欧链完整输出（内部结构，转换为 DalioResult 输出）。"""
    # 线 1
    steps: list[ChainStep] = field(default_factory=list)
    regime: CycleRegime | None = None
    active_tilts: list[Tilt] = field(default_factory=list)
    risk_parity_baseline: dict[str, float] = field(default_factory=dict)

    # 线 2
    tail_risk: TailRiskResult | None = None
    policy_path: PolicyPathResult | None = None

    # 交叉判断输出
    hedge_specs: list[HedgeSpec] = field(default_factory=list)

    # 综合
    conclusion: str = ""


# ══════════════════════════════════════════════════════════════
#  线 1: 周期定位 → 类型选择
# ══════════════════════════════════════════════════════════════


# ── ① 宏观象限定位 ────────────────────────────────────────────

QUADRANT_NAMES = {
    (True, True): "growth_up_inflation_up",
    (True, False): "growth_up_inflation_down",
    (False, True): "growth_down_inflation_up",
    (False, False): "growth_down_inflation_down",
}

# 四象限下各资产类型的天然偏好
QUADRANT_ASSET_BIAS: dict[str, dict[str, str]] = {
    "growth_up_inflation_up": {
        "equity_cyclical": "overweight",
        "commodity": "overweight",
        "nominal_bond": "underweight",
        "inflation_linked_bond": "overweight",
        "equity_defensive": "neutral",
        "gold": "neutral",
        "cash": "underweight",
    },
    "growth_up_inflation_down": {
        "equity_cyclical": "overweight",
        "equity_defensive": "overweight",
        "nominal_bond": "overweight",
        "commodity": "underweight",
        "inflation_linked_bond": "underweight",
        "gold": "underweight",
        "cash": "underweight",
    },
    "growth_down_inflation_up": {
        # 滞胀 — 最难的象限
        "commodity": "overweight",
        "inflation_linked_bond": "overweight",
        "gold": "overweight",
        "cash": "overweight",             # 滞胀中现金是避风港
        "equity_cyclical": "underweight",
        "equity_defensive": "underweight",
        "nominal_bond": "underweight",
    },
    "growth_down_inflation_down": {
        "nominal_bond": "overweight",
        "equity_defensive": "neutral",
        "cash": "overweight",
        "gold": "neutral",
        "equity_cyclical": "underweight",
        "commodity": "underweight",
        "inflation_linked_bond": "underweight",
    },
}


def _detect_short_cycle_phase(macro: MacroContext) -> tuple[str, list[str]]:
    """判断短期债务周期阶段。

    策略:
      1. 轨迹信号可用时 → 优先用轨迹（方向比绝对值更重要）
      2. 轨迹信号为 None → 降级回绝对阈值逻辑（向后兼容）

    Returns: (phase, evidence_list)
    """
    evidence: list[str] = []

    rate = macro.fed_funds_rate
    credit = macro.credit_growth
    unemp = macro.unemployment_rate
    gdp = macro.gdp_growth_actual

    # 轨迹信号
    gdp_m = macro.gdp_momentum
    ci = macro.credit_impulse
    inf_m = macro.inflation_momentum
    rate_d = macro.rate_direction
    unemp_d = macro.unemployment_direction

    has_trajectory = any(v is not None for v in (gdp_m, ci, rate_d, unemp_d))

    # ── 收集 evidence ──
    if rate is not None:
        evidence.append(f"联邦基金利率 {rate:.2f}%")
    if credit is not None:
        evidence.append(f"信贷增速 {credit:.1f}%")
    if unemp is not None:
        evidence.append(f"失业率 {unemp:.1f}%")
    if gdp is not None:
        evidence.append(f"GDP 增速 {gdp:.1f}%")
    if gdp_m is not None:
        evidence.append(f"GDP 动量 {gdp_m:+.2f}pp")
    if ci is not None:
        evidence.append(f"信贷脉冲 {ci:+.2f}pp")
    if rate_d is not None:
        evidence.append(f"利率方向 {rate_d:+.2f}pp")
    if unemp_d is not None:
        evidence.append(f"失业方向 {unemp_d:+.2f}pp")

    if rate is None and credit is None and not has_trajectory:
        return "unknown", ["缺少利率、信贷和轨迹数据"]

    # ══════════════════════════════════════════════════════════
    #  路径 A: 轨迹驱动（至少有信贷脉冲或 GDP 动量）
    # ══════════════════════════════════════════════════════════
    if has_trajectory and (ci is not None or gdp_m is not None):
        # 便捷布尔量 — None 被视为"不确定"，不参与判断
        credit_turning_positive = ci is not None and ci > 0
        credit_positive = ci is not None and ci > 0 and credit is not None and credit > 0
        credit_weakening = ci is not None and ci < 0 and credit is not None and credit > 0
        credit_negative = ci is not None and ci < 0
        gdp_accelerating = gdp_m is not None and gdp_m > 0
        gdp_decelerating = gdp_m is not None and gdp_m < 0
        gdp_decel_slowing = gdp_m is not None and gdp_m < 0  # 跌幅本身在缩小需要三期数据，暂用 decel
        rate_falling = rate_d is not None and rate_d < 0
        rate_low_or_falling = rate_falling or (rate is not None and rate < 2.0)
        unemp_rising = unemp_d is not None and unemp_d > 0
        unemp_falling = unemp_d is not None and unemp_d < 0
        unemp_very_low = unemp is not None and unemp < 4.5

        # ── GDP 极端负值: 不管其他信号都是紧缩 ──
        if gdp is not None and gdp < -5.0:
            if unemp is not None and unemp > 6.0:
                return "late_contraction", evidence
            return "mid_contraction", evidence

        # ── late_contraction: 信贷脉冲触底(仍负但跌幅缩小或转正) + GDP 跌幅缩小 + 利率快速下降 ──
        if credit_turning_positive and rate_falling and gdp_decelerating:
            return "late_contraction", evidence

        # ── early_expansion: 信贷脉冲转正 + GDP 加速 + 利率低位或下降 ──
        if credit_turning_positive and gdp_accelerating and rate_low_or_falling:
            return "early_expansion", evidence

        # ── late_expansion: 信贷脉冲开始减弱(仍正但下降) + GDP 高位 + 失业极低 ──
        if credit_weakening and gdp is not None and gdp > 2.0 and unemp_very_low:
            return "late_expansion", evidence

        # ── early_contraction: 信贷脉冲转负 + GDP 减速 + 失业开始上升 ──
        if credit_negative and gdp_decelerating and unemp_rising:
            return "early_contraction", evidence

        # ── early_contraction (宽松): 信贷脉冲转负 + GDP 减速（无就业数据）──
        if credit_negative and gdp_decelerating:
            return "early_contraction", evidence

        # ── mid_contraction: 信贷脉冲为负 + GDP 持续减速 + 失业上升 ──
        if credit_negative and gdp_decelerating and unemp_rising:
            return "mid_contraction", evidence

        # ── mid_contraction (宽松): 信贷脉冲为负 + GDP 弱 ──
        if credit_negative and gdp is not None and gdp < 1.5:
            return "mid_contraction", evidence

        # ── mid_expansion: 信贷脉冲为正 + GDP 加速或稳定 + 失业下降 ──
        if credit_positive and (gdp_accelerating or (gdp_m is not None and abs(gdp_m) < 0.3)):
            return "mid_expansion", evidence

        # ── mid_expansion (宽松): 信贷脉冲为正 + GDP 不在减速 ──
        if credit_positive and not gdp_decelerating:
            return "mid_expansion", evidence

        # ── 达利欧核心: 信贷脉冲强烈为负时，信贷领先实体经济 ──
        # 即使 GDP 还在加速，信贷急剧收缩预示着衰退即将到来
        if ci is not None and ci < -2.0:
            return "early_contraction", evidence

        # ── 轨迹有但没匹配到明确阶段 → 用 GDP 方向兜底 ──
        if gdp_accelerating:
            return "mid_expansion", evidence
        if gdp_decelerating:
            return "early_contraction", evidence

        # 有轨迹但全部不够判断 → 落入绝对阈值路径

    # ══════════════════════════════════════════════════════════
    #  路径 B: 绝对阈值（向后兼容，无轨迹信号时使用）
    # ══════════════════════════════════════════════════════════
    signals = {
        "rate_high": rate is not None and rate > 3.0,
        "rate_mid": rate is not None and 1.5 <= rate <= 3.0,
        "rate_low": rate is not None and rate < 1.5,
        "credit_expanding": credit is not None and credit > 3.0,
        "credit_slow": credit is not None and 0 <= credit <= 3.0,
        "credit_contracting": credit is not None and credit < 0,
        "unemp_low": unemp is not None and unemp < 4.5,
        "unemp_mid": unemp is not None and 4.5 <= unemp <= 6.0,
        "unemp_high": unemp is not None and unemp > 6.0,
        "gdp_strong": gdp is not None and gdp > 2.5,
        "gdp_moderate": gdp is not None and 1.5 < gdp <= 2.5,
        "gdp_weak": gdp is not None and 0 <= gdp <= 1.5,
        "gdp_negative": gdp is not None and gdp < 0,
    }

    # ── GDP 极端负值: 不管其他信号都是紧缩 ──
    if gdp is not None and gdp < -5.0:
        if signals["unemp_high"]:
            return "late_contraction", evidence
        return "mid_contraction", evidence

    # ── 明确的紧缩末期: 信贷收缩 + 失业高 ──
    if signals["credit_contracting"] and signals["unemp_high"]:
        return "late_contraction", evidence

    # ── 扩张初期: 低利率 + 信贷扩张 + GDP 不能是负的 ──
    if signals["rate_low"] and signals["credit_expanding"] and not signals["gdp_negative"]:
        return "early_expansion", evidence

    # ── 扩张末期: 利率偏高 + 信贷仍在扩张 + 低失业 ──
    if signals["rate_high"] and signals["credit_expanding"] and signals["unemp_low"]:
        return "late_expansion", evidence

    # ── 紧缩初期: 利率偏高 + (信贷收缩 或 GDP弱/负) ──
    if signals["rate_high"] and (signals["credit_contracting"] or signals["gdp_weak"] or signals["gdp_negative"]):
        return "early_contraction", evidence

    # ── 中间扩张: GDP 强 + 信贷扩张（利率中间区间）──
    if signals["gdp_strong"] and signals["credit_expanding"]:
        return "mid_expansion", evidence

    # ── 中间扩张 (放宽): GDP 温和 + 信贷扩张 + 失业不高 ──
    if signals["gdp_moderate"] and signals["credit_expanding"] and not signals["unemp_high"]:
        return "mid_expansion", evidence

    # ── 中间紧缩: GDP 弱或负 + 信贷放缓/收缩 ──
    if (signals["gdp_weak"] or signals["gdp_negative"]) and (signals["credit_contracting"] or signals["credit_slow"]):
        return "mid_contraction", evidence

    # ── 中间紧缩 (放宽): GDP 负增长（不管信贷）──
    if signals["gdp_negative"]:
        return "mid_contraction", evidence

    # ── GDP 弱 + 失业上升 ──
    if signals["gdp_weak"] and signals["unemp_high"]:
        return "mid_contraction", evidence

    # ── 还没匹配: 用 GDP 方向做最后兜底 ──
    if signals["gdp_strong"]:
        return "mid_expansion", evidence
    if signals["gdp_moderate"]:
        return "mid_expansion", evidence
    if signals["gdp_weak"] or signals["gdp_negative"]:
        return "mid_contraction", evidence

    return "ambiguous", evidence


def _detect_long_cycle_phase(macro: MacroContext) -> tuple[str, list[str]]:
    """判断长期债务周期阶段。

    Returns: (phase, evidence_list)
    """
    evidence = []
    debt_gdp = macro.total_debt_to_gdp
    rate = macro.fed_funds_rate

    if debt_gdp is None:
        return "unknown", ["缺少总债务/GDP 数据"]

    evidence.append(f"总债务/GDP {debt_gdp:.0f}%")

    if debt_gdp < 200:
        return "early_leverage", evidence
    elif debt_gdp < 300:
        if rate is not None:
            evidence.append(f"利率 {rate:.2f}%")
        return "mid_leverage", evidence
    else:
        # 高杠杆 — 区分丑陋去杠杆和漂亮去杠杆
        if rate is not None and rate < 1.0:
            evidence.append(f"利率 {rate:.2f}%，接近零下界")
            return "deleveraging_ugly", evidence
        elif rate is not None:
            evidence.append(f"利率 {rate:.2f}%，仍有降息空间")
            return "late_leverage", evidence
        return "late_leverage", evidence


def _step_regime(macro: MacroContext) -> tuple[ChainStep, CycleRegime | None]:
    """① 宏观象限定位。"""
    step = ChainStep(name="宏观象限定位", principle="我们在周期的什么位置")

    # 四象限
    gdp_a, gdp_e = macro.gdp_growth_actual, macro.gdp_growth_expected
    cpi_a, cpi_e = macro.cpi_actual, macro.cpi_expected

    if gdp_a is None or cpi_a is None:
        step.verdict = Verdict.UNCLEAR
        step.detail = "缺少 GDP 或 CPI 数据，无法定位象限"
        return step, None

    # 如果没有预期值，用 2.0% 和 2.0% 作为默认锚点
    gdp_e = gdp_e if gdp_e is not None else 2.0
    cpi_e = cpi_e if cpi_e is not None else 2.0

    growth_above = gdp_a > gdp_e
    inflation_above = cpi_a > cpi_e
    quadrant = QUADRANT_NAMES[(growth_above, inflation_above)]

    step.evidence.append(f"GDP: 实际 {gdp_a:.1f}% vs 预期 {gdp_e:.1f}%")
    step.evidence.append(f"CPI: 实际 {cpi_a:.1f}% vs 预期 {cpi_e:.1f}%")
    step.evidence.append(f"象限: {quadrant}")

    # 短期周期
    short_phase, short_ev = _detect_short_cycle_phase(macro)
    step.evidence.extend(short_ev)

    # 长期周期
    long_phase, long_ev = _detect_long_cycle_phase(macro)
    step.evidence.extend(long_ev)

    # 置信度: 有多少输入数据
    data_completeness = sum([
        gdp_a is not None, cpi_a is not None,
        macro.fed_funds_rate is not None, macro.credit_growth is not None,
        macro.total_debt_to_gdp is not None, macro.unemployment_rate is not None,
    ]) / 6.0
    confidence = data_completeness * (0.8 if short_phase != "ambiguous" else 0.5)

    # gap 幅度
    growth_gap = gdp_a - gdp_e
    inflation_gap = cpi_a - cpi_e

    # 零利率 QE 体制检测
    # 条件: 利率在零下界(< 0.5%) + 信贷仍在扩张(政策驱动,非市场自然)
    zero_bound_qe = (
        macro.fed_funds_rate is not None
        and macro.fed_funds_rate < 0.5
        and macro.credit_growth is not None
        and macro.credit_growth > 2.0
    )
    if zero_bound_qe:
        step.evidence.append("⚡ 零利率 QE 体制: 利率零下界 + 政策驱动信贷扩张")

    regime = CycleRegime(
        quadrant=quadrant,
        short_cycle_phase=short_phase,
        long_cycle_phase=long_phase,
        confidence=confidence,
        growth_gap=growth_gap,
        inflation_gap=inflation_gap,
        zero_bound_qe=zero_bound_qe,
    )

    step.verdict = Verdict.HOLDS
    step.detail = (
        f"{quadrant} | 短期: {short_phase} | 长期: {long_phase}"
        f" (置信度 {confidence:.0%})"
    )

    return step, regime


# ── ② 传导暴露分析 ────────────────────────────────────────────

# 各短期周期阶段对资产类型的传导方向
CYCLE_PHASE_TRANSMISSION: dict[str, dict[str, str]] = {
    "early_expansion": {
        "equity_cyclical": "strong_tailwind",   # 低利率 + 信贷宽松 → 周期股起飞
        "equity_defensive": "mild_headwind",    # 资金从防御转向进攻
        "nominal_bond": "headwind",             # 利率见底即将上升
        "commodity": "tailwind",                # 需求回升
        "gold": "headwind",                     # 风险偏好上升
    },
    "mid_expansion": {
        "equity_cyclical": "tailwind",          # 增长持续，周期股仍有空间
        "equity_defensive": "neutral",
        "nominal_bond": "neutral",              # 利率稳定期，债券不涨不跌
        "commodity": "neutral",                 # 需求稳定但非强信号
        "gold": "mild_headwind",                # 风险偏好高
    },
    "late_expansion": {
        "equity_cyclical": "mild_headwind",     # 估值高 + 利率升
        "equity_defensive": "neutral",
        "nominal_bond": "headwind",             # 利率高位
        "commodity": "strong_tailwind",         # 通胀上行
        "gold": "tailwind",                     # 通胀对冲
    },
    "early_contraction": {
        "equity_cyclical": "strong_headwind",   # 需求下降 + 融资收紧
        "equity_defensive": "tailwind",         # 避风港
        "nominal_bond": "tailwind",             # 降息预期
        "commodity": "headwind",                # 需求收缩
        "gold": "tailwind",                     # 避险
        "cash": "tailwind",                     # 避险 + 等待机会
    },
    "mid_contraction": {
        "equity_cyclical": "headwind",          # 需求持续疲弱
        "equity_defensive": "tailwind",         # 防御持续受益
        "nominal_bond": "tailwind",             # 降息持续
        "commodity": "headwind",                # 需求底部
        "gold": "tailwind",                     # 不确定性高
        "cash": "tailwind",                     # 保值
    },
    "late_contraction": {
        "equity_cyclical": "mild_tailwind",     # 开始见底
        "equity_defensive": "neutral",
        "nominal_bond": "strong_tailwind",      # 降息进行中
        "commodity": "headwind",                # 需求底部
        "gold": "strong_tailwind",              # 不确定性最大
        "cash": "tailwind",                     # 等待机会
    },
}


def _step_transmission(regime: CycleRegime) -> ChainStep:
    """② 传导暴露分析: 当前周期阶段如何影响各资产类型。"""
    step = ChainStep(name="传导暴露分析", principle="宏观力量如何传导到各资产类型")

    phase = regime.short_cycle_phase
    transmission = CYCLE_PHASE_TRANSMISSION.get(phase)

    if transmission is None:
        step.verdict = Verdict.UNCLEAR
        step.detail = f"短期周期阶段 '{phase}' 无明确传导映射"
        return step

    for asset_type, direction in transmission.items():
        step.evidence.append(f"{asset_type}: {direction}")

    step.verdict = Verdict.HOLDS
    step.detail = f"{phase} 阶段传导映射完成，{len(transmission)} 个资产类型"

    return step


# ── ③ 主动押注生成 ────────────────────────────────────────────

# 押注的幅度映射
_DIRECTION_MAGNITUDE = {
    "strong_tailwind": ("overweight", 0.7),
    "tailwind": ("overweight", 0.4),
    "mild_tailwind": ("overweight", 0.2),
    "neutral": (None, 0.0),
    "mild_headwind": ("underweight", 0.2),
    "headwind": ("underweight", 0.4),
    "strong_headwind": ("underweight", 0.7),
}

# 每个象限+周期组合的特征性 thesis 模板
_THESIS_TEMPLATES: dict[str, dict[str, str]] = {
    "early_expansion": {
        "equity_cyclical": "低利率 + 信贷扩张初期 → 被压制的需求释放 → 周期股率先受益",
        "nominal_bond": "利率见底 → 债券价格即将承压",
        "commodity": "实体需求回升 → 大宗商品价格上行",
    },
    "mid_expansion": {
        "equity_cyclical": "增长持续 + 盈利上修 → 周期股仍有空间",
        "commodity": "需求持续增长 → 大宗商品稳步上行",
    },
    "late_expansion": {
        "commodity": "通胀上行 + 产能接近极限 → 大宗商品涨价加速",
        "equity_cyclical": "估值偏高 + 利率高位 → 周期股上行空间有限",
        "gold": "通胀上行 → 黄金作为通胀对冲",
    },
    "early_contraction": {
        "equity_cyclical": "需求下降 + 融资收紧 → 高杠杆周期股首当其冲",
        "equity_defensive": "资金从周期转向防御 → 必需消费/医药/公用事业受益",
        "nominal_bond": "降息预期升温 → 长端利率下行 → 债券价格上涨",
    },
    "mid_contraction": {
        "equity_cyclical": "需求持续疲弱 → 盈利下修未结束",
        "equity_defensive": "防御板块持续获得资金流入",
        "nominal_bond": "降息持续 → 债券牛市",
    },
    "late_contraction": {
        "nominal_bond": "央行积极降息 → 债券牛市中段",
        "gold": "政策不确定性最大 → 黄金作为终极避险",
        "equity_cyclical": "估值见底 → 开始左侧布局",
    },
}

# 失效触发条件模板
_DECAY_TEMPLATES: dict[str, str] = {
    "early_expansion": "信贷增速转负 或 失业率开始上升",
    "mid_expansion": "信贷增速放缓至 < 3% 或 收益率曲线开始平坦化",
    "late_expansion": "GDP 增速连续 2 季下降 或 收益率曲线倒挂",
    "early_contraction": "央行开始降息 且 信贷增速触底回升",
    "mid_contraction": "信贷增速触底 且 领先指标拐头",
    "late_contraction": "信贷增速转正 且 PMI > 50",
}

# ══════════════════════════════════════════════════════════════
#  因果传导引擎
# ══════════════════════════════════════════════════════════════

ASSET_CAUSAL_MAP: dict[str, list[tuple[str, float, float]]] = {
    "equity_cyclical": [
        ("corporate_health", 0.30, +1),
        ("credit_availability", 0.20, +1),
        ("consumer_health", 0.20, +1),
        ("policy_response", 0.15, +1),
        ("inflation_pressure", 0.15, -1),  # 高通胀→估值收缩（但仅正值生效，见下方 clamp）
    ],
    "equity_defensive": [
        ("consumer_health", 0.40, -1), ("corporate_health", 0.30, -1),
        ("default_pressure", 0.30, +1),
    ],
    "nominal_bond": [
        ("inflation_pressure", 0.30, -1),      # 通胀低→债券好
        ("default_pressure", 0.30, +1),         # 避险需求→国债好（提权: 危机时最强驱动）
        ("policy_response", 0.20, +1),          # 宽松→债券好
        ("debt_service_burden", 0.10, -1),      # 偿债重→利率有下降空间→债券好
        ("credit_availability", 0.10, -1),      # 信贷宽→经济好→利率上→债券差
    ],
    "commodity": [
        ("inflation_pressure", 0.55, +1),  # 大宗受通胀/供给驱动为主
        ("consumer_health", 0.20, +1),      # 需求次要
        ("corporate_health", 0.25, +1),
    ],
    "gold": [
        ("default_pressure", 0.30, +1),      # 系统性风险→黄金避险（最重要）
        ("inflation_pressure", 0.30, +1),     # 通胀→黄金保值
        ("consumer_health", 0.20, -1),        # 经济弱→避险
        ("policy_response", 0.20, +1),        # 宽松/印钱→黄金涨
    ],
    "cash": [
        ("default_pressure", 0.30, +1),        # 高违约风险→持现金避险
        ("credit_availability", 0.25, -1),     # 信贷紧缩→持现金（提权: 流动性收缩最直接的受益者）
        ("consumer_health", 0.20, -1),         # 消费弱→持现金
        ("corporate_health", 0.20, -1),        # 企业弱→持现金
        ("inflation_pressure", 0.05, +1),      # 微弱: 高通胀→其他资产都跌→相对持现金
    ],
    "inflation_linked_bond": [
        ("inflation_pressure", 0.60, +1), ("policy_response", 0.20, +1),
        ("consumer_health", 0.20, +1),
    ],
}


def _weighted_tanh(components: list[float], weights: list[float]) -> float:
    """加权 tanh 激活: 将多个输入信号合成为 [-1, 1] 区间的输出。"""
    if not components or not weights:
        return 0.0
    total_w = sum(weights)
    if total_w == 0:
        return 0.0
    ws = sum(c * w for c, w in zip(components, weights)) / total_w
    return math.tanh(ws)


def _node_confidence(n_available: int, n_total: int) -> float:
    """节点置信度: 可用输入数 / 总输入数。"""
    return min(1.0, n_available / n_total) if n_total > 0 else 0.0


def _normalize_vs_history(
    value: float,
    median: float | None,
    p25: float | None,
    p75: float | None,
    fallback_center: float,
    fallback_scale: float,
) -> float:
    """百分位归一化: 用历史分布位置而非绝对值。

    有历史数据时: (value - median) / (p75 - p25) → IQR 归一化
    无历史数据时: (value - fallback_center) / fallback_scale → 硬编码降级

    输出大致在 [-2, +2] 范围（极端值可能超出）。
    正值 = 高于历史中位数。
    """
    if median is not None and p75 is not None and p25 is not None:
        iqr = p75 - p25
        if iqr > 0:
            return (value - median) / iqr
    return (value - fallback_center) / fallback_scale


# ── 7 个机制节点计算 ──────────────────────────────────────────


def _compute_debt_service_burden(macro: MacroContext) -> MechanismNode:
    """偿债负担: 利率 × 债务水平 × 结构脆弱度。

    达利欧核心: 偿债负担 = 利率 × 债务存量。但同样的总债务，
    结构不同风险完全不同:
      - 政府债务（可印钱偿还）→ 风险低
      - 家庭债务（不可印钱）→ 风险高，尤其浮动利率
      - 企业债务 → 中等，看信贷周期
      - 金融部门杠杆 → 乘数效应，崩盘时放大一切

    有结构数据时精细化计算，无则用总量近似。
    """
    components: list[float] = []
    weights: list[float] = []
    inputs_used: list[str] = []
    n_total = 6  # rate*debt, rate_dir, household, corporate, government, financial

    rate = macro.fed_funds_rate
    rate_d = macro.rate_direction

    # ── 结构化路径: 分部门计算 ──
    has_structure = any(v is not None for v in (
        macro.household_debt_to_income,
        macro.corporate_debt_to_gdp,
        macro.government_debt_to_gdp,
    ))

    if has_structure and rate is not None:
        # 用实际利率而非名义利率: 通胀高时名义偿债重但收入也在名义增长
        inflation = macro.cpi_actual if macro.cpi_actual is not None else (macro.cpi_expected or 2.0)
        real_rate = rate - inflation
        rate_norm = max(real_rate, 0) / 3.0  # 实际利率 3% → 1.0, 负实际利率 → 0(无负担)

        # 家庭债务: 最脆弱（不可印钱、直接影响消费、浮动利率暴露大）
        if macro.household_debt_to_income is not None:
            hh = macro.household_debt_to_income
            # 100% = 中性, 130%+ = 危险 (2007 美国到了 130%)
            hh_burden = -rate_norm * (hh / 100.0)
            components.append(hh_burden)
            weights.append(1.2)  # 家庭权重最高
            inputs_used.append(f"家庭: rate={rate:.1f}% × DTI={hh:.0f}%")

        # 企业债务: 看信贷周期
        if macro.corporate_debt_to_gdp is not None:
            corp = macro.corporate_debt_to_gdp
            # 70%GDP = 中性, 100%+ = 危险
            corp_burden = -rate_norm * (corp / 70.0)
            components.append(corp_burden)
            weights.append(0.8)
            inputs_used.append(f"企业: rate={rate:.1f}% × corp_debt={corp:.0f}%GDP")

        # 政府债务: 风险较低（能印钱），但极高时也有问题
        if macro.government_debt_to_gdp is not None:
            gov = macro.government_debt_to_gdp
            # 60%GDP = 安全, 100%+ = 开始有约束, 150%+ = 日本化
            gov_burden = -rate_norm * max(0, (gov - 60)) / 100.0
            components.append(gov_burden)
            weights.append(0.3)  # 政府权重最低（可印钱）
            inputs_used.append(f"政府: debt={gov:.0f}%GDP")

        # 金融部门杠杆: 乘数效应
        if macro.financial_sector_leverage is not None:
            fin = macro.financial_sector_leverage
            # 15x = 中性, 25x+ = 危险 (2007 投行到了 30x+)
            fin_risk = -(fin - 15) / 10.0  # 25x → -1.0
            components.append(fin_risk)
            weights.append(0.7)  # 金融杠杆权重高（崩盘时放大一切）
            inputs_used.append(f"金融杠杆: {fin:.0f}x")

    elif rate is not None and macro.total_debt_to_gdp is not None:
        # ── 降级路径: 用总量近似 ──
        debt_gdp = macro.total_debt_to_gdp
        inflation = macro.cpi_actual if macro.cpi_actual is not None else (macro.cpi_expected or 2.0)
        real_rate = rate - inflation
        r_norm = max(real_rate, 0) / 3.0
        burden = -r_norm * (debt_gdp / 250.0)
        components.append(burden)
        weights.append(1.5)
        inputs_used.append(f"总量: real_rate={real_rate:+.1f}% × debt/GDP={debt_gdp:.0f}%")

    # ── 利率方向: 加息中更痛苦 ──
    if rate_d is not None:
        components.append(-rate_d / 1.5)
        weights.append(0.5)
        inputs_used.append(f"rate_dir={rate_d:+.2f}")

    n_avail = len(components)
    value = _weighted_tanh(components, weights)
    confidence = _node_confidence(n_avail, n_total)
    detail = f"偿债负担 {value:+.3f}" + (" [结构化]" if has_structure else " [总量]")

    return MechanismNode(
        name="debt_service_burden", value=value,
        confidence=confidence, inputs_used=inputs_used, detail=detail,
    )


def _compute_credit_availability(macro: MacroContext) -> MechanismNode:
    """信贷可得性: 信贷增速 + 脉冲 - 利率方向。value > 0 = 宽松。"""
    components: list[float] = []
    weights: list[float] = []
    inputs_used: list[str] = []
    n_total = 3

    credit = macro.credit_growth
    ci = macro.credit_impulse
    rate_d = macro.rate_direction

    if credit is not None:
        # 百分位归一化: 中国 15% 是正常的，美国 5% 才是正常的
        credit_z = _normalize_vs_history(
            credit,
            macro.hist_credit_growth_median, None, None,
            fallback_center=5.0, fallback_scale=10.0,
        )
        components.append(credit_z)
        weights.append(1.0)
        inputs_used.append(f"credit={credit:+.1f}%" + (f" (z={credit_z:+.2f})" if macro.hist_credit_growth_median else ""))

    if ci is not None:
        components.append(ci / 3.0)
        weights.append(1.0)
        inputs_used.append(f"credit_impulse={ci:+.2f}")

    if rate_d is not None:
        components.append(-rate_d / 1.5)
        weights.append(0.8)
        inputs_used.append(f"rate_dir={rate_d:+.2f}")

    # M2 货币供应: 流动性的直接度量
    if macro.m2_growth is not None:
        m2_signal = (macro.m2_growth - 5.0) / 8.0  # 5%=中性, 13%=+1(QE), -3%=-1(紧缩)
        components.append(m2_signal)
        weights.append(0.6)
        inputs_used.append(f"m2={macro.m2_growth:+.1f}%")

    value = _weighted_tanh(components, weights)
    confidence = _node_confidence(len(components), n_total + 1)
    detail = f"信贷可得性 {value:+.3f}"

    return MechanismNode(
        name="credit_availability", value=value,
        confidence=confidence, inputs_used=inputs_used, detail=detail,
    )


def _compute_policy_response(macro: MacroContext) -> MechanismNode:
    """政策响应: 实际利率 + 利率方向 + 财政刺激。value > 0 = 宽松。

    普适改进: 用实际利率(名义-通胀)替代名义利率。
    1980年: 名义18%看起来极紧，但通胀13.5%→实际4.5%（确实紧但没那么极端）。
    2022年: 名义3.25%看起来中性，但通胀8.3%→实际-5%（实际上极度宽松）。
    """
    components: list[float] = []
    weights: list[float] = []
    inputs_used: list[str] = []
    n_total = 4

    rate = macro.fed_funds_rate
    rate_d = macro.rate_direction
    cpi = macro.cpi_actual
    fiscal = macro.fiscal_deficit_to_gdp

    # ── 核心: 实际利率 ──
    if rate is not None:
        # 计算实际利率 = 名义利率 - 通胀
        inflation = cpi if cpi is not None else (macro.cpi_expected or 2.0)
        real_rate = rate - inflation

        # 用历史百分位归一化实际利率（如果有），否则用固定中性 1%
        # 实际利率中性 ≈ 1%（长期均衡实际利率）
        real_rate_z = _normalize_vs_history(
            real_rate,
            macro.hist_rate_median, macro.hist_rate_p25, macro.hist_rate_p75,
            fallback_center=1.0, fallback_scale=2.0,
        )
        rate_stance = -real_rate_z  # 实际利率高于中性 = 紧缩
        components.append(rate_stance)
        weights.append(1.5)  # 实际利率是最重要的政策信号
        inputs_used.append(f"real_rate={real_rate:+.1f}% (名义{rate:.1f}%-通胀{inflation:.1f}%)")

    # ── 利率方向 ──
    if rate_d is not None:
        components.append(-rate_d / 1.0)
        weights.append(0.8)
        inputs_used.append(f"rate_dir={rate_d:+.2f}")

    # ── 财政刺激 ──
    if fiscal is not None:
        # 赤字/GDP > 3% = 净刺激, < 3% = 紧缩
        fiscal_stance = (fiscal - 3.0) / 5.0
        components.append(fiscal_stance)
        weights.append(0.5)
        inputs_used.append(f"fiscal={fiscal:.1f}%GDP")

    # ── 零利率信号 ──
    # 当名义利率 ≈ 0，意味着常规工具耗尽，这本身是一个信号
    if rate is not None and rate < 0.5:
        # 零下界: 政策空间耗尽 → 未来宽松依赖非常规工具(QE)
        # 这对金融条件是宽松的，但空间有限
        components.append(0.3)
        weights.append(0.5)
        inputs_used.append("零下界: 常规工具耗尽")

    value = _weighted_tanh(components, weights)
    confidence = _node_confidence(len(components), n_total)
    detail = f"政策响应 {value:+.3f}"

    return MechanismNode(
        name="policy_response", value=value,
        confidence=confidence, inputs_used=inputs_used, detail=detail,
    )


def _compute_consumer_health(
    macro: MacroContext,
    debt_service: MechanismNode,
    credit_avail: MechanismNode,
) -> MechanismNode:
    """消费者健康: 失业率 + 通胀侵蚀 + 偿债负担传导 + 信贷可得性传导。"""
    components: list[float] = []
    weights: list[float] = []
    inputs_used: list[str] = []
    n_total = 5  # unemp, unemp_dir, inflation_erosion, debt_service, credit

    unemp = macro.unemployment_rate
    unemp_d = macro.unemployment_direction

    if unemp is not None:
        unemp_z = _normalize_vs_history(
            unemp,
            macro.hist_unemployment_median, None, None,
            fallback_center=5.0, fallback_scale=2.0,
        )
        components.append(-unemp_z)  # 高于中位数 = 不健康
        weights.append(1.0)
        inputs_used.append(f"unemp={unemp:.1f}%")

    if unemp_d is not None:
        components.append(-unemp_d)
        weights.append(0.8)
        inputs_used.append(f"unemp_dir={unemp_d:+.2f}")

    # 初次申请失业金: 比失业率更高频的劳动力信号
    if macro.initial_claims is not None:
        # 200k=健康, 300k=紧张, 400k+=危机
        claims_signal = -(macro.initial_claims - 250) / 150
        components.append(claims_signal)
        weights.append(0.6)
        inputs_used.append(f"claims={macro.initial_claims:.0f}k")

    # 零售销售: 直接反映消费支出
    if macro.retail_sales_growth is not None:
        retail_signal = macro.retail_sales_growth / 5.0
        components.append(retail_signal)
        weights.append(0.5)
        inputs_used.append(f"retail={macro.retail_sales_growth:+.1f}%")

    # 通胀侵蚀: 高通胀削弱真实购买力（即使失业率低）
    cpi = macro.cpi_actual
    cpi_target = macro.cpi_expected if macro.cpi_expected is not None else 2.0
    if cpi is not None and cpi > cpi_target:
        inflation_erosion = -(cpi - cpi_target) / 4.0  # CPI 超 4pp → -1.0
        components.append(inflation_erosion)
        weights.append(0.7)
        inputs_used.append(f"inflation_erosion={inflation_erosion:+.2f}")

    # GDP 动量前瞻传导: GDP 改善领先就业改善 2-3 个季度
    # 达利欧: "看二阶导数——GDP 跌幅收窄意味着消费者即将改善"
    gdp_m = macro.gdp_momentum
    if gdp_m is not None and abs(gdp_m) > 1.0:
        # GDP 动量强（>1pp）时对消费者有前瞻性影响
        gdp_forward = gdp_m / 4.0  # +4.7pp → +1.2（强正向信号）
        components.append(gdp_forward)
        weights.append(0.8)  # 权重跟失业率相当——二阶导数跟水平一样重要
        inputs_used.append(f"gdp_momentum_forward={gdp_forward:+.2f}")

    # 上游传导
    components.append(debt_service.value * debt_service.confidence)
    weights.append(0.7)
    inputs_used.append(f"debt_service={debt_service.value:+.2f}")

    components.append(credit_avail.value * credit_avail.confidence)
    weights.append(0.5)
    inputs_used.append(f"credit_avail={credit_avail.value:+.2f}")

    value = _weighted_tanh(components, weights)
    n_avail = sum(1 for x in [unemp, unemp_d, gdp_m] if x is not None) + 2
    confidence = _node_confidence(n_avail, n_total)
    detail = f"消费者健康 {value:+.3f}"

    return MechanismNode(
        name="consumer_health", value=value,
        confidence=confidence, inputs_used=inputs_used, detail=detail,
    )


def _compute_corporate_health(
    macro: MacroContext,
    consumer: MechanismNode,
    credit_avail: MechanismNode,
) -> MechanismNode:
    """企业健康: GDP 增速 + 动量 + 消费者传导 + 信贷可得性传导。"""
    components: list[float] = []
    weights: list[float] = []
    inputs_used: list[str] = []
    n_total = 4

    gdp = macro.gdp_growth_actual
    gdp_m = macro.gdp_momentum

    if gdp is not None:
        gdp_z = _normalize_vs_history(
            gdp,
            macro.hist_gdp_median, None, None,
            fallback_center=2.0, fallback_scale=3.0,
        )
        components.append(gdp_z)  # 高于中位数 = 健康
        weights.append(1.0)
        inputs_used.append(f"gdp={gdp:+.1f}%")

    if gdp_m is not None:
        components.append(gdp_m / 2.0)
        weights.append(0.8)
        inputs_used.append(f"gdp_momentum={gdp_m:+.2f}")

    # PMI: 实时经济活动（比 GDP 快 1-2 个月）
    if macro.pmi_manufacturing is not None:
        pmi_signal = (macro.pmi_manufacturing - 50) / 10  # 50=中性, 60=+1, 40=-1
        components.append(pmi_signal)
        weights.append(0.7)
        inputs_used.append(f"pmi_mfg={macro.pmi_manufacturing:.0f}")

    # 盈利增速: 最直接的企业健康指标
    if macro.earnings_growth is not None:
        eg_signal = macro.earnings_growth / 15.0  # +15%→+1, -15%→-1
        components.append(eg_signal)
        weights.append(0.8)
        inputs_used.append(f"earnings={macro.earnings_growth:+.0f}%")

    # 盈利修正: 分析师在上修还是下修（领先指标）
    if macro.earnings_revision is not None:
        components.append(macro.earnings_revision)  # 已经是 -1 到 +1
        weights.append(0.5)
        inputs_used.append(f"revision={macro.earnings_revision:+.2f}")

    # 工业产出: 制造业实际产出
    if macro.industrial_production_growth is not None:
        ip_signal = macro.industrial_production_growth / 5.0
        components.append(ip_signal)
        weights.append(0.4)
        inputs_used.append(f"ind_prod={macro.industrial_production_growth:+.1f}%")

    # 上游传导
    components.append(consumer.value * consumer.confidence)
    weights.append(0.6)
    inputs_used.append(f"consumer={consumer.value:+.2f}")

    components.append(credit_avail.value * credit_avail.confidence)
    weights.append(0.5)
    inputs_used.append(f"credit_avail={credit_avail.value:+.2f}")

    value = _weighted_tanh(components, weights)
    n_avail = sum(1 for x in [gdp, gdp_m] if x is not None) + 2
    confidence = _node_confidence(n_avail, n_total)
    detail = f"企业健康 {value:+.3f}"

    return MechanismNode(
        name="corporate_health", value=value,
        confidence=confidence, inputs_used=inputs_used, detail=detail,
    )


def _compute_default_pressure(
    debt_service: MechanismNode,
    consumer: MechanismNode,
    corporate: MechanismNode,
) -> MechanismNode:
    """违约压力: 偿债重 + 消费者差 + 企业差 → 违约升高。value > 0 = 高违约压力。"""
    components = [
        -debt_service.value,
        -consumer.value,
        -corporate.value,
    ]
    weights = [1.0, 0.8, 0.8]
    inputs_used = [
        f"-debt_service={-debt_service.value:+.2f}",
        f"-consumer={-consumer.value:+.2f}",
        f"-corporate={-corporate.value:+.2f}",
    ]

    value = _weighted_tanh(components, weights)
    # 所有输入都是上游节点，总是可用
    avg_conf = (debt_service.confidence + consumer.confidence + corporate.confidence) / 3
    detail = f"违约压力 {value:+.3f}"

    return MechanismNode(
        name="default_pressure", value=value,
        confidence=avg_conf, inputs_used=inputs_used, detail=detail,
    )


def _compute_inflation_pressure(
    macro: MacroContext,
    consumer: MechanismNode,
) -> MechanismNode:
    """通胀压力: CPI 偏离目标 + 通胀动量 + 消费者需求拉动。value > 0 = 通胀压力大。"""
    components: list[float] = []
    weights: list[float] = []
    inputs_used: list[str] = []
    n_total = 3

    cpi = macro.cpi_actual
    cpi_e = macro.cpi_expected if macro.cpi_expected is not None else 2.0
    inf_m = macro.inflation_momentum

    if cpi is not None:
        # 用百分位归一化 CPI 偏差（不同国家通胀中枢不同）
        cpi_z = _normalize_vs_history(
            cpi - cpi_e,  # 偏差而非绝对值
            0.0 if macro.hist_cpi_median is None else (macro.hist_cpi_median - cpi_e),
            None, None,
            fallback_center=0.0, fallback_scale=3.0,
        )
        components.append(cpi_z)
        weights.append(1.0)
        inputs_used.append(f"cpi={cpi:.1f}% vs target={cpi_e:.1f}%")

    if inf_m is not None:
        components.append(inf_m / 1.5)
        weights.append(0.8)
        inputs_used.append(f"inflation_momentum={inf_m:+.2f}")

    # 消费者需求拉动通胀
    components.append(consumer.value * 0.5)
    weights.append(0.5)
    inputs_used.append(f"consumer_pull={consumer.value * 0.5:+.2f}")

    # 美元强度: 强美元压低进口通胀，弱美元推升通胀（大宗商品美元计价）
    if macro.dxy_yoy is not None:
        dollar_effect = -macro.dxy_yoy / 10.0  # DXY +10% → 通胀压力 -1.0
        components.append(dollar_effect)
        weights.append(0.4)
        inputs_used.append(f"dollar={macro.dxy_yoy:+.0f}%")

    value = _weighted_tanh(components, weights)
    n_avail = sum(1 for x in [cpi, inf_m, macro.dxy_yoy] if x is not None) + 1
    confidence = _node_confidence(n_avail, n_total + 1)
    detail = f"通胀压力 {value:+.3f}"

    return MechanismNode(
        name="inflation_pressure", value=value,
        confidence=confidence, inputs_used=inputs_used, detail=detail,
    )


# ── 反馈循环与传播 ────────────────────────────────────────────

MAX_FEEDBACK_ITERATIONS = 3
DAMPING_FACTOR = 0.5


def _apply_data_triangulation(macro: MacroContext) -> MacroContext:
    """替代数据三角验证: 用多个独立数据源交叉验证官方统计。

    适用于任何可能存在数据失真的市场（中国、越南、印度等）。

    原则:
    1. 物理活动不可伪造 — 发电量、货运量、水泥钢铁产量
    2. 贸易对手可验证 — 中国的进口 = 其他国家的对华出口
    3. 广义信贷 > 银行信贷 — 含影子银行/表外
    4. 房地产是新兴市场的放大器 — 崩盘时拖累远超 GDP 反映
    5. 多指标综合判断 — 不依赖任何单一指标
    6. 悲观偏差 — 指标矛盾时偏向悲观方（政府有动机隐瞒坏消息，美国非农也改）
    """
    # 目前只对标记了替代数据的场景启用
    has_alt = any(v is not None for v in (
        macro.alt_electricity_growth, macro.alt_freight_growth,
        macro.alt_import_growth, macro.alt_property_investment_growth,
        macro.alt_broad_credit_growth,
    ))
    if not has_alt:
        return macro

    corrections: list[str] = []
    profile = COUNTRY_PROFILES.get(macro.country, CountryProfile())

    # ── 第 1 层: 物理活动指数 vs 官方 GDP ──
    # 加权合成真实经济活动指数
    activity_components: list[tuple[str, float, float]] = []
    if macro.alt_electricity_growth is not None:
        activity_components.append(("电力", macro.alt_electricity_growth, 0.30))
    if macro.alt_freight_growth is not None:
        activity_components.append(("货运", macro.alt_freight_growth, 0.25))
    if macro.alt_cement_steel_growth is not None:
        activity_components.append(("工业材料", macro.alt_cement_steel_growth, 0.20))
    if macro.alt_import_growth is not None:
        activity_components.append(("进口", macro.alt_import_growth, 0.25))

    if len(activity_components) >= 2 and macro.gdp_growth_actual is not None:
        total_w = sum(w for _, _, w in activity_components)
        activity_index = sum(v * w for _, v, w in activity_components) / total_w
        official_gdp = macro.gdp_growth_actual

        gap = official_gdp - activity_index
        if abs(gap) > 2.0:
            # 悲观偏差: 官方 GDP 向替代数据靠拢 60%（而不是 50%）
            # 因为政府有动机高报 GDP，替代数据更可能接近真实
            corrected = official_gdp - gap * 0.6
            sources = ", ".join(f"{n}{v:+.0f}%" for n, v, _ in activity_components)
            corrections.append(f"GDP: 官方{official_gdp:.1f}% vs 活动指数{activity_index:.1f}% ({sources}) → {corrected:.1f}%")
            macro.gdp_growth_actual = corrected

    # ── 第 2 层: 广义信贷替代银行信贷 ──
    if macro.alt_broad_credit_growth is not None and macro.credit_growth is not None:
        corrections.append(f"信贷: 银行{macro.credit_growth:.1f}% → 广义{macro.alt_broad_credit_growth:.1f}%")
        macro.credit_growth = macro.alt_broad_credit_growth

    # ── 第 3 层: 房地产部门修正 ──
    property_signals: list[float] = []
    if macro.alt_property_investment_growth is not None:
        property_signals.append(macro.alt_property_investment_growth)
    if macro.alt_property_sales_growth is not None:
        property_signals.append(macro.alt_property_sales_growth)
    if macro.alt_land_revenue_growth is not None:
        property_signals.append(macro.alt_land_revenue_growth)

    if property_signals and macro.gdp_growth_actual is not None:
        avg_property = sum(property_signals) / len(property_signals)
        sensitivity = profile.property_sensitivity
        if avg_property < -10.0:
            # 房地产严重下滑 → GDP 下调
            drag = avg_property * sensitivity * 0.1
            macro.gdp_growth_actual += drag
            corrections.append(f"房地产拖累: 均值{avg_property:.0f}% × 敏感度{sensitivity:.1f} → GDP {drag:+.1f}pp")

    # ── 第 4 层: 贸易交叉验证 ──
    if (macro.alt_import_growth is not None
        and macro.gdp_growth_actual is not None
        and macro.alt_import_growth < -5.0
        and macro.gdp_growth_actual > 3.0):
        macro.gdp_growth_actual *= 0.8
        corrections.append(f"交叉验证: 进口{macro.alt_import_growth:.0f}% 但 GDP {macro.gdp_growth_actual/.8:.1f}% → 下调20%")

    # ── 第 5 层: 全面恶化综合检测 ──
    distress_signals: list[str] = []
    if macro.alt_electricity_growth is not None and macro.alt_electricity_growth < 2.0:
        distress_signals.append(f"电力{macro.alt_electricity_growth:.0f}%")
    if macro.alt_freight_growth is not None and macro.alt_freight_growth < 0:
        distress_signals.append(f"货运{macro.alt_freight_growth:.0f}%")
    if macro.alt_import_growth is not None and macro.alt_import_growth < -5.0:
        distress_signals.append(f"进口{macro.alt_import_growth:.0f}%")
    if macro.alt_property_investment_growth is not None and macro.alt_property_investment_growth < -5.0:
        distress_signals.append(f"地产投资{macro.alt_property_investment_growth:.0f}%")
    if macro.alt_land_revenue_growth is not None and macro.alt_land_revenue_growth < -15.0:
        distress_signals.append(f"土地收入{macro.alt_land_revenue_growth:.0f}%")
    if macro.alt_property_sales_growth is not None and macro.alt_property_sales_growth < -15.0:
        distress_signals.append(f"房销{macro.alt_property_sales_growth:.0f}%")
    if macro.alt_auto_sales_growth is not None and macro.alt_auto_sales_growth < -10.0:
        distress_signals.append(f"汽车{macro.alt_auto_sales_growth:.0f}%")
    if macro.alt_capital_flow is not None and macro.alt_capital_flow < -50.0:
        distress_signals.append(f"资本外流{macro.alt_capital_flow:.0f}B$")

    if len(distress_signals) >= 3 and macro.gdp_growth_actual is not None and macro.gdp_growth_actual > 0:
        corrections.append(f"⚠ {len(distress_signals)} 个恶化信号: {'; '.join(distress_signals)}")
        macro.gdp_growth_actual = max(macro.gdp_growth_actual * 0.3, -2.0)
        corrections.append(f"GDP 强制下调至 {macro.gdp_growth_actual:.1f}%")

    if corrections:
        macro._china_corrections = corrections  # type: ignore

    return macro


def _propagate_causal_graph(macro: MacroContext) -> CausalGraphResult:
    """因果传导引擎主函数: 前向计算 + 反馈循环 + 资产影响映射。"""
    # 替代数据三角验证（任何有替代数据的市场）
    macro = _apply_data_triangulation(macro)
    # 第 0 轮: 前向计算所有节点
    debt_service = _compute_debt_service_burden(macro)
    credit_avail = _compute_credit_availability(macro)
    policy = _compute_policy_response(macro)
    consumer = _compute_consumer_health(macro, debt_service, credit_avail)
    corporate = _compute_corporate_health(macro, consumer, credit_avail)
    default_press = _compute_default_pressure(debt_service, consumer, corporate)
    inflation = _compute_inflation_pressure(macro, consumer)

    feedback_active: list[str] = []
    converged_in = 0

    # 第 1-3 轮: 反馈循环
    for i in range(1, MAX_FEEDBACK_ITERATIONS + 1):
        damping = DAMPING_FACTOR ** i
        prev_credit = credit_avail.value
        prev_policy = policy.value

        # 反馈 1: default → credit (违约高 → 惜贷)
        credit_delta = -default_press.value * 0.4 * damping
        new_credit_val = math.tanh(
            math.atanh(max(-0.999, min(0.999, credit_avail.value))) + credit_delta
        )
        credit_avail = MechanismNode(
            name="credit_availability", value=new_credit_val,
            confidence=credit_avail.confidence,
            inputs_used=credit_avail.inputs_used + [f"违约反馈(轮{i})"],
            detail=credit_avail.detail + f" | 违约反馈 {credit_delta:+.3f}",
        )

        # 反馈 2: inflation → policy (通胀高 → 央行紧缩)
        policy_delta = -inflation.value * 0.3 * damping
        new_policy_val = math.tanh(
            math.atanh(max(-0.999, min(0.999, policy.value))) + policy_delta
        )
        policy = MechanismNode(
            name="policy_response", value=new_policy_val,
            confidence=policy.confidence,
            inputs_used=policy.inputs_used + [f"通胀反馈(轮{i})"],
            detail=policy.detail + f" | 通胀反馈 {policy_delta:+.3f}",
        )

        # 重新传播下游
        consumer = _compute_consumer_health(macro, debt_service, credit_avail)
        corporate = _compute_corporate_health(macro, consumer, credit_avail)
        default_press = _compute_default_pressure(debt_service, consumer, corporate)
        inflation = _compute_inflation_pressure(macro, consumer)

        delta = abs(new_credit_val - prev_credit) + abs(new_policy_val - prev_policy)
        converged_in = i

        if abs(credit_delta) > 0.01 or abs(policy_delta) > 0.01:
            if credit_delta < -0.01:
                feedback_active.append(
                    f"债务螺旋: 违约→惜贷→更多违约 (轮{i}, Δ={credit_delta:+.3f})"
                )
            if policy_delta < -0.01:
                feedback_active.append(
                    f"通胀紧缩: 通胀→加息→经济降温 (轮{i}, Δ={policy_delta:+.3f})"
                )

        if delta < 0.01:
            break

    # ── 偿债滞后传导 ──
    # 普世规则: 偿债负担极重时，企业/消费者看起来还好只是时间滞后
    # 达利欧: "债务偿付率上升 → 消费和投资必然下降，只是时间问题"
    # 当 debt_service 极端 + corporate/consumer 仍为正 → 施加前瞻性折扣
    if debt_service.value < -0.5 and (corporate.value > 0 or consumer.value > 0):
        lag_penalty = debt_service.value * 0.5  # 偿债 -0.8 → penalty -0.4
        if corporate.value > 0:
            corporate = MechanismNode(
                name="corporate_health",
                value=math.tanh(math.atanh(max(-0.999, min(0.999, corporate.value))) + lag_penalty),
                confidence=corporate.confidence,
                inputs_used=corporate.inputs_used + [f"偿债滞后({lag_penalty:+.2f})"],
                detail=corporate.detail + f" | 偿债滞后 {lag_penalty:+.2f}",
            )
        if consumer.value > 0:
            consumer = MechanismNode(
                name="consumer_health",
                value=math.tanh(math.atanh(max(-0.999, min(0.999, consumer.value))) + lag_penalty * 0.7),
                confidence=consumer.confidence,
                inputs_used=consumer.inputs_used + [f"偿债滞后({lag_penalty * 0.7:+.2f})"],
                detail=consumer.detail + f" | 偿债滞后 {lag_penalty * 0.7:+.2f}",
            )
        default_press = _compute_default_pressure(debt_service, consumer, corporate)
        feedback_active.append(
            f"偿债滞后: debt_service={debt_service.value:+.2f} → 前瞻折扣 {lag_penalty:+.2f}"
        )

    nodes = {
        "debt_service_burden": debt_service,
        "credit_availability": credit_avail,
        "policy_response": policy,
        "consumer_health": consumer,
        "corporate_health": corporate,
        "default_pressure": default_press,
        "inflation_pressure": inflation,
    }

    profile = COUNTRY_PROFILES.get(macro.country, CountryProfile())
    asset_impacts = _compute_asset_impacts(nodes, profile, macro)

    return CausalGraphResult(
        nodes=nodes,
        asset_impacts=asset_impacts,
        converged_in=converged_in,
        feedback_active=feedback_active,
    )


def _compute_asset_impacts(
    nodes: dict[str, MechanismNode],
    profile: CountryProfile | None = None,
    macro: MacroContext | None = None,
) -> list[AssetImpact]:
    """从机制节点计算资产影响 — 主导机制加权 + 轨迹放大 + 国别适配 + 相对排名。

    普适方案:
    1. 识别主导机制
    2. 轨迹一致性放大（有轨迹数据时）
    3. 国别 profile 调整各机制节点对资产的传导权重
    4. 分组相对排名决定方向
    """
    if profile is None:
        profile = CountryProfile()
    # ── 识别主导机制 ──
    node_strengths = [
        (name, abs(n.value) * n.confidence, n)
        for name, n in nodes.items()
    ]
    node_strengths.sort(key=lambda x: x[1], reverse=True)
    dominant_names = {node_strengths[0][0]}
    if len(node_strengths) > 1 and node_strengths[1][1] > node_strengths[0][1] * 0.7:
        dominant_names.add(node_strengths[1][0])  # 第二强的如果接近第一（>70%），也算主导

    # ── 轨迹放大器: 用轨迹方向调整节点值 ──
    # 原理: 如果节点当前值和轨迹方向一致（例如 corporate 为负且 GDP 在减速），
    # 说明情况在恶化 → 放大信号。如果矛盾（corporate 为正但 GDP 在减速），
    # 说明当前状态可能不可持续 → 衰减信号。
    trajectory_adjusted = dict(nodes)  # 浅拷贝
    if macro is not None:
        trajectory_map = {
            # node_name: (trajectory_signal, description)
            "corporate_health": (macro.gdp_momentum, "GDP动量"),
            "consumer_health": (macro.unemployment_direction, "失业方向"),  # 注意: 反向
            "credit_availability": (macro.credit_impulse, "信贷脉冲"),
            "inflation_pressure": (macro.inflation_momentum, "通胀动量"),
            "policy_response": (macro.rate_direction, "利率方向"),  # 注意: 反向
        }
        for node_name, (traj, desc) in trajectory_map.items():
            if traj is None or node_name not in nodes:
                continue
            node = nodes[node_name]
            # 方向映射: 有些轨迹信号跟节点方向相反
            traj_direction = traj
            if node_name == "consumer_health":
                traj_direction = -traj  # 失业上升 = consumer 方向为负
            elif node_name == "policy_response":
                traj_direction = -traj  # 利率上升 = policy 方向为负(紧缩)

            # 一致性: 节点值和轨迹方向同号 = 一致
            consistent = (node.value > 0 and traj_direction > 0) or (node.value < 0 and traj_direction < 0)
            if consistent:
                # 放大 15%: 趋势在延续
                amplifier = 1.15
            elif (node.value > 0.1 and traj_direction < -0.3) or (node.value < -0.1 and traj_direction > 0.3):
                # 衰减 10%: 当前状态可能不可持续（保守衰减）
                amplifier = 0.9
            else:
                amplifier = 1.0

            if amplifier != 1.0:
                trajectory_adjusted[node_name] = MechanismNode(
                    name=node.name,
                    value=max(-1.0, min(1.0, node.value * amplifier)),
                    confidence=node.confidence,
                    inputs_used=node.inputs_used,
                    detail=node.detail + f" | 轨迹×{amplifier:.1f}({desc})",
                )

    nodes = trajectory_adjusted

    # ── 计算每个资产的加权分数 ──
    all_scores: dict[str, tuple[float, list[str]]] = {}

    for asset_type, sources in ASSET_CAUSAL_MAP.items():
        score = 0.0
        paths: list[str] = []
        total_w = 0.0

        for node_name, base_weight, sign in sources:
            node = nodes[node_name]

            # 国别结构调整: 不同国家不同机制的重要性不同
            country_multiplier = 1.0
            if node_name == "consumer_health":
                country_multiplier = profile.consumption_weight / 0.5  # US: 1.4, CN: 0.7
            elif node_name == "credit_availability":
                country_multiplier = profile.credit_channel_weight / 0.5  # CN: 1.6, US: 1.0
            elif node_name == "debt_service_burden":
                # 房地产敏感的经济体，偿债负担影响更大
                country_multiplier = 1.0 + profile.property_sensitivity
            elif node_name == "policy_response":
                if asset_type in ("equity_cyclical", "equity_defensive"):
                    # 政策→股市的传导效率因国而异
                    country_multiplier = profile.policy_to_equity / 0.5  # CN: 0.4, US: 1.4
                else:
                    # 政策→债券/商品/黄金的传导相对稳定
                    country_multiplier = 0.7 + profile.policy_activism * 0.6

            # 主导机制权重放大，非主导缩小
            if node_name in dominant_names:
                effective_weight = base_weight * 1.8 * country_multiplier
            else:
                effective_weight = base_weight * 0.6 * country_multiplier

            # 通胀→股票的单向性: 高通胀压制股票，但低通胀不应推升股票
            # （低通胀可能是需求崩溃信号，不是利好）
            node_value = node.value
            if node_name == "inflation_pressure" and asset_type == "equity_cyclical" and sign == -1:
                node_value = max(node_value, 0)  # 只让正通胀压力生效

            contrib = node_value * sign * effective_weight * node.confidence
            score += contrib
            total_w += effective_weight * node.confidence

            if abs(contrib) > 0.02:
                word = "利好" if contrib > 0 else "利空"
                dom_mark = "★" if node_name in dominant_names else ""
                paths.append(f"{dom_mark}{node_name}({node.value:+.2f})→{word}")

        if total_w > 0:
            score /= total_w
        score = max(-1.0, min(1.0, score))
        all_scores[asset_type] = (score, paths)

    # ── 二阶导数奖励: 经济从衰退中加速恢复时从防守转进攻 ──
    # 达利欧: "看二阶导数——跌幅收窄且加速 = 该买进攻型了"
    # 条件: GDP 仍为负或低增长 + 但动量强正（从坑里往上爬）
    if (macro is not None
        and macro.gdp_momentum is not None and macro.gdp_momentum > 2.0
        and macro.gdp_growth_actual is not None and macro.gdp_growth_actual < 1.5):
        # 经济仍差但在快速改善 = 触底反弹信号
        momentum_bonus = min(macro.gdp_momentum / 10.0, 0.3)
        if "equity_cyclical" in all_scores:
            old_score, old_paths = all_scores["equity_cyclical"]
            all_scores["equity_cyclical"] = (old_score + momentum_bonus, old_paths + [f"二阶导数奖励+{momentum_bonus:.2f}"])
        if "equity_defensive" in all_scores:
            old_score, old_paths = all_scores["equity_defensive"]
            all_scores["equity_defensive"] = (old_score - momentum_bonus * 0.5, old_paths)

    # ── 分组相对排名 ──
    RISK_GROUP = {"equity_cyclical", "equity_defensive", "commodity"}
    SAFE_GROUP = {"nominal_bond", "gold", "cash", "inflation_linked_bond"}

    risk_scores = {k: v for k, v in all_scores.items() if k in RISK_GROUP}
    safe_scores = {k: v for k, v in all_scores.items() if k in SAFE_GROUP}

    impacts: list[AssetImpact] = []

    for group_scores in [risk_scores, safe_scores]:
        if not group_scores:
            continue
        ranked = sorted(group_scores.items(), key=lambda x: x[1][0], reverse=True)
        n = len(ranked)
        # 每组内: 分数最高的 overweight, 最低的 underweight
        # 中间的看绝对值: 正→ow, 负→uw, 接近零→skip
        for rank, (asset_type, (score, paths)) in enumerate(ranked):
            if rank == 0:
                direction = "overweight"
            elif rank == n - 1:
                direction = "underweight"
            elif score > 0.05:
                direction = "overweight"
            elif score < -0.05:
                direction = "underweight"
            else:
                continue

            magnitude = round(abs(score), 2)
            impacts.append(AssetImpact(
                asset_type=asset_type,
                raw_score=round(score, 3),
                contributing_paths=paths[:3],
                direction=direction,
                magnitude=max(magnitude, 0.05),
            ))

    impacts.sort(key=lambda x: (-1 if x.direction == "overweight" else 1, -x.magnitude))
    return impacts




def _project_macro(macro: MacroContext, steps: int = 2) -> MacroContext:
    """投射未来宏观状态: 用轨迹信号阻尼外推 N 步。

    达利欧的核心: 不看现在在哪，看按这个轨迹走下去会到哪。
    steps=2 ≈ 6 个月（2 个季度）。

    用阻尼外推而非线性外推: 趋势每一步衰减 50%。
    这防止极端动量（如 CPI 每季度 -2pp）产生不合理的投射值（如 CPI=1.6%）。
    """
    DAMPING = 0.5  # 每步衰减 50%
    projected = MacroContext(
        gdp_growth_expected=macro.gdp_growth_expected,
        cpi_expected=macro.cpi_expected,
        snapshot_date=macro.snapshot_date,
    )

    def _damped_projection(current: float, momentum: float | None, n: int) -> float:
        """阻尼外推: 每步衰减，防止极端值。"""
        if momentum is None:
            return current
        total_delta = 0.0
        for i in range(n):
            total_delta += momentum * (DAMPING ** i)
        return current + total_delta

    # GDP: 按动量阻尼外推
    if macro.gdp_growth_actual is not None:
        projected.gdp_growth_actual = _damped_projection(
            macro.gdp_growth_actual, macro.gdp_momentum, steps)
        projected.gdp_momentum = macro.gdp_momentum

    # 信贷: 按脉冲阻尼外推
    if macro.credit_growth is not None:
        projected.credit_growth = _damped_projection(
            macro.credit_growth, macro.credit_impulse, steps)
        projected.credit_impulse = macro.credit_impulse

    # CPI: 按动量阻尼外推，且不低于 -1%（通缩有下界）
    if macro.cpi_actual is not None:
        projected.cpi_actual = max(-1.0, _damped_projection(
            macro.cpi_actual, macro.inflation_momentum, steps))
        projected.inflation_momentum = macro.inflation_momentum

    # 利率: 按方向阻尼外推
    if macro.fed_funds_rate is not None:
        projected.fed_funds_rate = max(0.0, _damped_projection(
            macro.fed_funds_rate, macro.rate_direction, steps))
        projected.rate_direction = macro.rate_direction

    # 失业率: 按方向阻尼外推
    if macro.unemployment_rate is not None:
        projected.unemployment_rate = max(0.0, _damped_projection(
            macro.unemployment_rate, macro.unemployment_direction, steps))
        projected.unemployment_direction = macro.unemployment_direction

    # 非轨迹字段直接复制
    projected.treasury_10y = macro.treasury_10y
    projected.treasury_2y = macro.treasury_2y
    projected.total_debt_to_gdp = macro.total_debt_to_gdp
    projected.vix = macro.vix
    projected.sp500_earnings_yield = macro.sp500_earnings_yield
    projected.fiscal_deficit_to_gdp = macro.fiscal_deficit_to_gdp

    return projected


def _blend_asset_impacts(
    current: list[AssetImpact],
    projected: list[AssetImpact],
    current_weight: float = 0.5,
) -> list[AssetImpact]:
    """混合当前和投射的资产影响，用相对排名决定方向。"""
    proj_weight = 1.0 - current_weight

    # 收集所有 asset types 的混合分数
    current_map = {i.asset_type: i for i in current}
    projected_map = {i.asset_type: i for i in projected}
    all_types = set(list(current_map.keys()) + list(projected_map.keys()))

    scores: dict[str, tuple[float, list[str]]] = {}
    for asset_type in all_types:
        c = current_map.get(asset_type)
        p = projected_map.get(asset_type)
        c_score = c.raw_score if c else 0.0
        p_score = p.raw_score if p else 0.0
        raw = c_score * current_weight + p_score * proj_weight

        paths = []
        if c and c.contributing_paths:
            paths.append(f"当前: {c.contributing_paths[0]}")
        if p and p.contributing_paths:
            paths.append(f"投射: {p.contributing_paths[0]}")

        scores[asset_type] = (raw, paths)

    # 相对排名
    ranked = sorted(scores.items(), key=lambda x: x[1][0], reverse=True)
    n = len(ranked)
    n_ow = min(3, n)
    n_uw = min(3, n)

    blended: list[AssetImpact] = []
    for rank, (asset_type, (raw, paths)) in enumerate(ranked):
        if rank < n_ow:
            direction = "overweight"
        elif rank >= n - n_uw:
            direction = "underweight"
        else:
            continue

        blended.append(AssetImpact(
            asset_type=asset_type,
            raw_score=round(raw, 3),
            contributing_paths=paths,
            direction=direction,
            magnitude=max(round(abs(raw), 2), 0.05),
        ))

    blended.sort(key=lambda x: (-1 if x.direction == "overweight" else 1, -x.magnitude))
    return blended


def _step_tilts(regime: CycleRegime, macro: MacroContext) -> tuple[ChainStep, list[Tilt]]:
    """③ 主动押注生成 — 因果预演引擎: 当前状态 + 投射未来 → 前瞻决策。"""
    step = ChainStep(
        name="主动押注生成",
        principle="因果预演: 当前状态(30%) + 投射6个月后(70%) → 前瞻决策",
    )

    # 运行因果图: 当前状态
    graph_now = _propagate_causal_graph(macro)

    # 运行因果图: 投射未来 (如果有轨迹数据)
    has_trajectory = any(v is not None for v in (
        macro.gdp_momentum, macro.credit_impulse,
        macro.inflation_momentum, macro.rate_direction,
    ))

    if False:  # 投射暂停: 轨迹放大器已在因果图层面处理(v2方案)
        projected = _project_macro(macro, steps=2)
        graph_future = _propagate_causal_graph(projected)
        blended_impacts = _blend_asset_impacts(
            graph_now.asset_impacts, graph_future.asset_impacts,
            current_weight=0.5,
        )

        # Evidence: 当前 vs 投射对比
        step.evidence.append("── 当前状态 ──")
        for name, node in graph_now.nodes.items():
            step.evidence.append(f"  [{name}] {node.value:+.2f}")

        step.evidence.append("── 投射 6 个月后 ──")
        for name, node in graph_future.nodes.items():
            now_val = graph_now.nodes[name].value
            delta = node.value - now_val
            if abs(delta) > 0.05:
                arrow = "↑" if delta > 0 else "↓"
                step.evidence.append(f"  [{name}] {now_val:+.2f} → {node.value:+.2f} ({arrow}{abs(delta):.2f})")

        for loop in graph_now.feedback_active + graph_future.feedback_active:
            step.evidence.append(f"🔄 {loop}")

        impacts = blended_impacts
        detail_suffix = f"预演模式 (当前30%+投射70%), 收敛 {graph_now.converged_in}/{graph_future.converged_in} 轮"
    else:
        impacts = graph_now.asset_impacts
        # 无轨迹时显示当前节点
        for name, node in graph_now.nodes.items():
            step.evidence.append(f"[{name}] {node.value:+.2f} (置信度 {node.confidence:.0%})")
        for loop in graph_now.feedback_active:
            step.evidence.append(f"🔄 {loop}")
        detail_suffix = f"静态模式 (无轨迹), 收敛 {graph_now.converged_in} 轮"

    if impacts:
        decay = _DECAY_TEMPLATES.get(regime.short_cycle_phase, "周期阶段发生变化")
        tilts: list[Tilt] = []
        for impact in impacts:
            thesis = (
                " + ".join(impact.contributing_paths[:3])
                if impact.contributing_paths
                else f"因果评分 {impact.raw_score:+.2f}"
            )
            tilts.append(Tilt(
                asset_type=impact.asset_type,
                direction=impact.direction,
                magnitude=impact.magnitude,
                thesis=thesis,
                confidence=regime.confidence,
                decay_trigger=decay,
            ))
            arrow = "▲" if impact.direction == "overweight" else "▼"
            step.evidence.append(f"{arrow} {impact.asset_type} {impact.magnitude:.0%}")

        step.verdict = Verdict.HOLDS
        step.detail = f"因果引擎: {len(tilts)} 个押注 ({detail_suffix})"
        return step, tilts

    # ── Fallback ──
    return _step_tilts_legacy(regime, macro)


def _step_tilts_legacy(regime: CycleRegime, macro: MacroContext) -> tuple[ChainStep, list[Tilt]]:
    """③ 主动押注生成（旧查找表逻辑，作为 fallback）。"""
    step = ChainStep(name="主动押注生成", principle="基于周期判断生成方向性押注")
    tilts: list[Tilt] = []

    phase = regime.short_cycle_phase
    transmission = CYCLE_PHASE_TRANSMISSION.get(phase, {})
    quadrant_bias = QUADRANT_ASSET_BIAS.get(regime.quadrant, {})
    thesis_templates = _THESIS_TEMPLATES.get(phase, {})
    decay = _DECAY_TEMPLATES.get(phase, "周期阶段发生变化")

    if not transmission:
        step.verdict = Verdict.UNCLEAR
        step.detail = f"无法为 '{phase}' 阶段生成押注"
        return step, tilts

    # ── gap 幅度 → 象限权重 ──
    # gap 大(> 2pp): 象限信号强，覆盖周期
    # gap 中(1-2pp): 象限信号中等，跟周期交叉
    # gap 小(< 1pp): 象限信号弱，周期主导
    abs_growth_gap = abs(regime.growth_gap)
    abs_inflation_gap = abs(regime.inflation_gap)
    avg_gap = (abs_growth_gap + abs_inflation_gap) / 2

    if avg_gap > 2.0:
        quad_strength = "strong"
        quad_base_magnitude = 0.5
    elif avg_gap > 1.0:
        quad_strength = "medium"
        quad_base_magnitude = 0.35
    else:
        quad_strength = "weak"
        quad_base_magnitude = 0.15  # gap 小时象限信号很弱

    step.evidence.append(
        f"gap 幅度: 增长 {regime.growth_gap:+.1f}pp, 通胀 {regime.inflation_gap:+.1f}pp"
        f" → 象限权重 {quad_strength}"
    )

    # ── 合并周期传导 + 象限偏好 → 最终押注 ──
    all_asset_types = set(list(transmission.keys()) + list(quadrant_bias.keys()))

    for asset_type in all_asset_types:
        cycle_dir_str = transmission.get(asset_type)
        quad_dir = quadrant_bias.get(asset_type)

        cycle_mapped = _DIRECTION_MAGNITUDE.get(cycle_dir_str) if cycle_dir_str else None
        cycle_direction = cycle_mapped[0] if cycle_mapped else None
        cycle_magnitude = cycle_mapped[1] if cycle_mapped else 0.0

        # 将象限偏好映射为方向和幅度（幅度受 gap 大小调节）
        quad_direction = quad_dir if quad_dir in ("overweight", "underweight") else None
        quad_magnitude = quad_base_magnitude if quad_direction else 0.0

        # ── 通胀敏感资产: 通胀 gap 方向特殊处理 ──
        # commodity 和 nominal_bond 对通胀方向高度敏感
        # 当通胀 gap 方向与象限给出的资产方向一致时，通胀维度单独强化/弱化
        if asset_type in ("nominal_bond", "commodity", "inflation_linked_bond"):
            if abs_inflation_gap < 1.0:
                # 通胀 gap 微小 → 通胀维度的象限信号不可靠，让周期主导
                if quad_direction and cycle_direction and quad_direction != cycle_direction:
                    quad_direction = None
                    quad_magnitude = 0.0

        # ── QE 体制修正 ──
        # 零利率 QE 下: 股债关系被政策扭曲
        # 区分 QE 初期（危机刚爆发，VIX 高，不确定性大）和 QE 成熟期（市场平静，taper 风险）
        qe_override = None
        if regime.zero_bound_qe:
            qe_early = macro.vix is not None and macro.vix > 25  # 高波动 = QE 初期
            if asset_type == "equity_cyclical":
                # QE → 流动性推升估值 → 周期股受益（初期和成熟期都成立）
                qe_override = ("overweight", 0.4, "QE 流动性推升估值")
            elif asset_type == "nominal_bond":
                if qe_early:
                    # QE 初期: 央行正在买债 → 债券受益
                    qe_override = ("overweight", 0.3, "QE 初期央行购债 → 债券受益")
                else:
                    # QE 成熟期: 利率已在底部，taper 信号 → 债券脆弱
                    qe_override = ("underweight", 0.3, "QE 成熟期，taper 风险")
            elif asset_type == "gold":
                if qe_early:
                    # QE 初期: 极端不确定性 + 印钱 → 黄金避险 + 贬值预期
                    qe_override = ("overweight", 0.5, "QE 初期: 不确定性 + 印钱 → 黄金")
                elif regime.inflation_gap > 0:
                    qe_override = ("overweight", 0.4, "QE + 通胀回升 → 货币贬值")
                else:
                    # QE 成熟期 + 通胀不来 → 贬值叙事被证伪
                    qe_override = ("underweight", 0.2, "QE 但通胀不来 → 贬值叙事被证伪")

        if cycle_direction and quad_direction and cycle_direction == quad_direction:
            # 共振: 取较大幅度 + 加成
            direction = cycle_direction
            magnitude = min(1.0, max(cycle_magnitude, quad_magnitude) + 0.15)
            confirmation = "（象限+周期共振）"
        elif cycle_direction and quad_direction and cycle_direction != quad_direction:
            # 矛盾: 谁赢取决于 gap 强度
            if quad_strength == "strong":
                # gap 大: 象限覆盖
                direction = quad_direction
                magnitude = max(0.15, quad_magnitude - 0.1)
                confirmation = f"（象限覆盖周期 — gap {avg_gap:.1f}pp 强信号）"
            else:
                # gap 小/中: 周期主导
                direction = cycle_direction
                magnitude = max(0.1, cycle_magnitude - 0.1)
                confirmation = f"（周期主导 — 象限 gap 仅 {avg_gap:.1f}pp）"
        elif cycle_direction:
            direction = cycle_direction
            magnitude = cycle_magnitude
            confirmation = ""
        elif quad_direction:
            direction = quad_direction
            magnitude = quad_magnitude
            confirmation = "（仅象限信号）"
        else:
            continue  # neutral 两侧

        # ── QE override: 在所有常规逻辑之后，QE 体制直接覆盖 ──
        if qe_override:
            qe_dir, qe_mag, qe_reason = qe_override
            if qe_dir != direction:
                direction = qe_dir
                magnitude = qe_mag
                confirmation = f"（⚡ QE 覆盖: {qe_reason}）"
            else:
                # QE 跟常规逻辑方向一致，加强
                magnitude = min(1.0, max(magnitude, qe_mag))
                confirmation += f" ⚡QE 确认"

        thesis = thesis_templates.get(asset_type, f"{phase}/{regime.quadrant} → {direction}")

        tilt = Tilt(
            asset_type=asset_type,
            direction=direction,
            magnitude=round(magnitude, 2),
            thesis=thesis + confirmation,
            confidence=regime.confidence,
            decay_trigger=decay,
        )
        tilts.append(tilt)
        step.evidence.append(
            f"{direction} {asset_type} {magnitude:.0%}{confirmation}"
        )

    # 按幅度排序，最强的在前
    tilts.sort(key=lambda t: t.magnitude, reverse=True)

    step.verdict = Verdict.HOLDS
    step.detail = f"生成 {len(tilts)} 个主动押注"

    return step, tilts


# ── ④ 风险平价基线 ────────────────────────────────────────────

# 经典全天候近似权重（Bridgewater All Weather 公开披露的大致比例）
DEFAULT_RISK_PARITY: dict[str, float] = {
    "equity": 0.30,
    "long_term_bond": 0.40,
    "intermediate_bond": 0.15,
    "commodity": 0.075,
    "gold": 0.075,
}

# 各资产类别的代理 ETF（用于计算波动率）
ASSET_CLASS_ETFS: dict[str, str] = {
    "equity": "SPY",             # S&P 500
    "long_term_bond": "TLT",     # 20+ Year Treasury
    "intermediate_bond": "IEF",  # 7-10 Year Treasury
    "commodity": "DBC",          # Invesco DB Commodity
    "gold": "GLD",               # SPDR Gold
}


def _compute_equal_risk_weights(
    volatilities: dict[str, float],
) -> dict[str, float]:
    """等风险贡献: 每个资产类别贡献相等的组合风险。

    简化版: 权重与波动率成反比。
    完整版需要相关性矩阵做优化，暂用此近似。
    """
    if not volatilities:
        return {}

    inv_vols = {k: 1.0 / v for k, v in volatilities.items() if v > 0}
    if not inv_vols:
        return {}

    total = sum(inv_vols.values())
    return {k: round(v / total, 4) for k, v in inv_vols.items()}


def _fetch_volatilities(lookback_days: int = 252) -> dict[str, float] | None:
    """从 Anchor DB 获取各资产类别代理 ETF 的年化波动率。

    如果数据不足（< 60 天），返回 None，降级到默认权重。
    """
    try:
        from polaris.db.anchor import query_df_safe
    except Exception:
        return None

    vols: dict[str, float] = {}
    for asset_class, etf in ASSET_CLASS_ETFS.items():
        df = query_df_safe(
            "SELECT price_close FROM stock_quotes "
            "WHERE ticker = :ticker ORDER BY trade_date DESC LIMIT :n",
            {"ticker": etf, "n": lookback_days},
        )
        if df.empty or len(df) < 60:
            return None  # 数据不足，全部降级

        prices = df["price_close"].astype(float).values[::-1]  # 时间正序
        # 日收益率 → 年化波动率
        import numpy as np
        returns = np.diff(np.log(prices))
        vol = float(np.std(returns) * np.sqrt(252))
        vols[asset_class] = vol

    return vols


def _step_risk_parity() -> tuple[ChainStep, dict[str, float]]:
    """④ 全天候底仓 — 等风险贡献优化。

    使用独立的 all_weather 模块:
    1. 有 ETF 数据 → ERC 优化（波动率 + 相关性矩阵）
    2. 无数据 → Bridgewater 公开近似权重
    """
    from polaris.chains.all_weather import build_all_weather

    step = ChainStep(
        name="全天候底仓",
        principle="等风险贡献 — 每个资产贡献相等的组合风险，任何环境存活",
    )

    aw = build_all_weather()

    step.verdict = Verdict.HOLDS
    step.detail = aw.detail

    for asset, w in sorted(aw.weights.items(), key=lambda x: -x[1]):
        rc = aw.risk_contributions.get(asset)
        vol = aw.risk_metrics.volatilities.get(asset) if aw.risk_metrics else None
        parts = [f"{asset}: {w:.1%}"]
        if vol:
            parts.append(f"vol={vol:.1%}")
        if rc:
            parts.append(f"RC={rc:.1%}")
        step.evidence.append("  ".join(parts))

    if aw.portfolio_volatility > 0:
        step.evidence.append(f"组合波动率: {aw.portfolio_volatility:.1%}")

    # 四象限覆盖检查
    for q, assets in aw.quadrant_coverage.items():
        if not assets:
            step.evidence.append(f"⚠ {q} 无覆盖!")

    return step, aw.weights


# ══════════════════════════════════════════════════════════════
#  线 2: 认知盲区
# ══════════════════════════════════════════════════════════════


def _assess_tail_risk(macro: MacroContext, regime: CycleRegime | None) -> TailRiskResult:
    """A. 尾部风险检测。

    "What you don't know is more important than what you know."
    """
    result = TailRiskResult()

    # ── 收益率曲线倒挂 → 衰退先行指标 ──
    t10 = macro.treasury_10y
    t2 = macro.treasury_2y
    if t10 is not None and t2 is not None:
        spread = t10 - t2
        if spread < 0:
            result.risks.append(f"收益率曲线倒挂 (10Y-2Y = {spread:.2f}%)，衰退先行指标")
        elif spread < 0.3:
            result.risks.append(f"收益率曲线接近倒挂 (10Y-2Y = {spread:.2f}%)")

    # ── VIX 异常 ──
    if macro.vix is not None:
        if macro.vix > 35:
            result.risks.append(f"VIX = {macro.vix:.0f}，市场恐慌水平")
        elif macro.vix < 12:
            result.risks.append(f"VIX = {macro.vix:.0f}，异常平静 — 可能在暴风雨前")

    # ── 长期周期末端 ──
    if regime and regime.long_cycle_phase in ("late_leverage", "deleveraging_ugly"):
        result.risks.append(
            f"长期债务周期处于 {regime.long_cycle_phase}，系统性风险偏高"
        )

    # ── 高债务 + 高利率 = 债务螺旋风险 ──
    if macro.total_debt_to_gdp is not None and macro.fed_funds_rate is not None:
        if macro.total_debt_to_gdp > 300 and macro.fed_funds_rate > 4:
            result.risks.append(
                f"高债务({macro.total_debt_to_gdp:.0f}% GDP)"
                f" + 高利率({macro.fed_funds_rate:.1f}%)，债务偿付螺旋风险"
            )

    # ── 过热泡沫检测（日本 1990 模式）──
    # 达利欧: "最危险的时刻是一切看起来最好的时候"
    # 信号: GDP 高 + 失业低 + 信贷疯涨 + 债务结构脆弱
    overheating_signals = 0
    overheating_details = []
    if macro.gdp_growth_actual is not None and macro.gdp_growth_actual > 3.5:
        overheating_signals += 1
        overheating_details.append(f"GDP {macro.gdp_growth_actual:.1f}%(过热)")
    if macro.unemployment_rate is not None and macro.unemployment_rate < 3.0:
        overheating_signals += 1
        overheating_details.append(f"失业率 {macro.unemployment_rate:.1f}%(极低)")
    if macro.credit_growth is not None and macro.credit_growth > 10.0:
        overheating_signals += 1
        overheating_details.append(f"信贷增速 {macro.credit_growth:.0f}%(狂热)")
    if macro.financial_sector_leverage is not None and macro.financial_sector_leverage > 20:
        overheating_signals += 1
        overheating_details.append(f"金融杠杆 {macro.financial_sector_leverage:.0f}x(危险)")
    if macro.household_debt_to_income is not None and macro.household_debt_to_income > 110:
        overheating_signals += 1
        overheating_details.append(f"家庭 DTI {macro.household_debt_to_income:.0f}%(过高)")
    if macro.corporate_debt_to_gdp is not None and macro.corporate_debt_to_gdp > 100:
        overheating_signals += 1
        overheating_details.append(f"企业债 {macro.corporate_debt_to_gdp:.0f}%GDP(过高)")

    if overheating_signals >= 3:
        result.risks.append(
            f"过热泡沫警告: {'; '.join(overheating_details)}。"
            f"一切看起来很好但杠杆极端 — 达利欧: 泡沫最危险的时刻是顶部"
        )

    # ── 外部冲击信号（VIX 高但本国宏观数据正常）──
    # 2015 中国股灾模式: 美国 GDP 正常但 VIX 飙升 = 外部冲击传导
    if (macro.vix is not None and macro.vix > 30
        and macro.gdp_growth_actual is not None and macro.gdp_growth_actual > 1.5
        and macro.unemployment_rate is not None and macro.unemployment_rate < 6.0):
        result.risks.append(
            f"外部冲击: VIX={macro.vix:.0f} 但本国宏观正常(GDP {macro.gdp_growth_actual:.1f}%)"
            f" — 冲击来源可能在海外"
        )

    # 严重度判断
    if len(result.risks) >= 3:
        result.severity = "severe"
        result.detail = f"{len(result.risks)} 个尾部风险信号，系统性风险偏高"
    elif result.risks:
        result.severity = "moderate"
        result.detail = f"{len(result.risks)} 个尾部风险信号"
    else:
        result.severity = "none"
        result.detail = "未检测到明显尾部风险"

    return result


def _assess_policy_path(macro: MacroContext) -> PolicyPathResult:
    """B. 政策路径分析。

    达利欧的四个政策工具: 降息、QE、财政刺激、货币贬值。
    """
    result = PolicyPathResult()

    # ── 降息空间 ──
    if macro.fed_funds_rate is not None:
        result.rate_room = macro.fed_funds_rate * 100  # 转为 bp
        if macro.fed_funds_rate > 4:
            result.likely_tools.append("降息（空间充足）")
            result.impact_on_assets["nominal_bond"] = "利好（降息 → 债券涨）"
            result.impact_on_assets["equity_cyclical"] = "利好（融资成本下降）"
        elif macro.fed_funds_rate > 1:
            result.likely_tools.append("降息（空间有限）")
        else:
            result.likely_tools.append("降息空间耗尽 → 可能转向 QE")
            result.impact_on_assets["gold"] = "利好（QE → 货币贬值预期）"

    # ── 财政空间 ──
    if macro.fiscal_deficit_to_gdp is not None:
        deficit = macro.fiscal_deficit_to_gdp
        if deficit < 3:
            result.fiscal_room = "ample"
            result.likely_tools.append("财政刺激（空间充足）")
        elif deficit < 6:
            result.fiscal_room = "limited"
            result.likely_tools.append("财政刺激（空间有限，可能增加国债供给）")
            result.impact_on_assets["nominal_bond"] = "不确定（刺激利好 vs 供给利空）"
        else:
            result.fiscal_room = "exhausted"
            result.likely_tools.append("财政空间耗尽 → 依赖货币政策")

    # ── 漂亮去杠杆条件 ──
    has_rate_room = macro.fed_funds_rate is not None and macro.fed_funds_rate > 2
    has_fiscal_room = result.fiscal_room in ("ample", "limited")
    if has_rate_room and has_fiscal_room:
        result.detail = "漂亮去杠杆条件具备: 降息+财政工具均可用"
    elif has_rate_room or has_fiscal_room:
        result.detail = "部分政策工具可用，去杠杆难度适中"
    else:
        result.detail = "政策空间狭窄 — 去杠杆风险上升，可能被迫货币贬值"
        result.likely_tools.append("货币贬值（被动）")
        result.impact_on_assets["gold"] = "强烈利好"
        result.impact_on_assets["commodity"] = "利好（本币贬值 → 商品涨）"

    if not result.likely_tools:
        result.detail = "缺少政策数据，无法评估"

    return result


# ══════════════════════════════════════════════════════════════
#  交叉判断 → 对冲规格
# ══════════════════════════════════════════════════════════════

# 每个象限的"反面情景"
_COUNTER_SCENARIOS: dict[str, str] = {
    "growth_up_inflation_up": "growth_down_inflation_up（滞胀）",
    "growth_up_inflation_down": "growth_down_inflation_up（滞胀意外）",
    "growth_down_inflation_up": "growth_up_inflation_down（通胀缓解+增长恢复）",
    "growth_down_inflation_down": "growth_down_inflation_up（通缩→滞胀转化）",
}

# 各象限下最大的裸露敞口
_UNHEDGED_EXPOSURES: dict[str, str] = {
    "growth_up_inflation_up": "名义债券 — 如果增长突然转弱，债券空头会被轧",
    "growth_up_inflation_down": "通胀对冲资产不足 — 如果通胀意外上行",
    "growth_down_inflation_up": "股票和债券双杀 — 最危险象限，需要大宗和黄金对冲",
    "growth_down_inflation_down": "周期股 — 如果经济V型反弹会踏空",
}


def _generate_hedge_specs(
    regime: CycleRegime | None,
    tilts: list[Tilt],
    tail_risk: TailRiskResult,
    policy_path: PolicyPathResult,
) -> list[HedgeSpec]:
    """交叉判断: 线 1 押注 + 线 2 盲区 → 对冲规格。"""
    specs: list[HedgeSpec] = []

    if regime is None:
        return specs

    # ── 基于象限的反面情景对冲 ──
    counter = _COUNTER_SCENARIOS.get(regime.quadrant, "未知反面情景")
    unhedged = _UNHEDGED_EXPOSURES.get(regime.quadrant, "未知裸露敞口")

    # 置信度越低，需要的对冲越多（可接受亏损越小）
    max_loss = 0.05 + regime.confidence * 0.10  # 置信度 0→5%, 1→15%

    specs.append(HedgeSpec(
        protects_against=counter,
        max_acceptable_loss=round(max_loss, 2),
        unhedged_exposure=unhedged,
    ))

    # ── 尾部风险追加对冲 ──
    if tail_risk.severity == "severe":
        specs.append(HedgeSpec(
            protects_against="系统性风险事件（多个尾部信号共振）",
            max_acceptable_loss=0.03,
            unhedged_exposure="全组合 — 极端情景下相关性趋 1",
        ))
    elif tail_risk.severity == "moderate":
        for risk in tail_risk.risks[:2]:
            specs.append(HedgeSpec(
                protects_against=risk,
                max_acceptable_loss=0.08,
                unhedged_exposure="与该风险高相关的持仓",
            ))

    # ── 政策路径对冲 ──
    if policy_path.fiscal_room == "exhausted" and policy_path.rate_room is not None and policy_path.rate_room < 150:
        specs.append(HedgeSpec(
            protects_against="政策空间耗尽 → 被迫货币贬值/债务违约",
            max_acceptable_loss=0.05,
            unhedged_exposure="本币资产 — 贬值风险",
        ))

    return specs


# ══════════════════════════════════════════════════════════════
#  主入口
# ══════════════════════════════════════════════════════════════


def evaluate(macro: MacroContext) -> DalioChainResult:
    """达利欧因果链主入口。

    输入: 宏观经济数据快照
    输出: 周期定位 + 主动押注 + 对冲规格
    """
    result = DalioChainResult()

    # ── 线 1: 周期定位 → 类型选择 ──

    # ① 宏观象限定位
    regime_step, regime = _step_regime(macro)
    result.steps.append(regime_step)
    result.regime = regime

    if regime is None:
        result.conclusion = "宏观数据不足，无法定位周期 → 停留在风险平价底仓"
        # 只跑风险平价
        rp_step, rp_weights = _step_risk_parity()
        result.steps.append(rp_step)
        result.risk_parity_baseline = rp_weights
        return result

    # ② 传导暴露分析
    trans_step = _step_transmission(regime)
    result.steps.append(trans_step)

    # ③ 主动押注生成
    tilts_step, tilts = _step_tilts(regime, macro)
    result.steps.append(tilts_step)
    result.active_tilts = tilts

    # ④ 风险平价基线
    rp_step, rp_weights = _step_risk_parity()
    result.steps.append(rp_step)
    result.risk_parity_baseline = rp_weights

    # ── 线 2: 认知盲区（并行）──
    result.tail_risk = _assess_tail_risk(macro, regime)
    result.policy_path = _assess_policy_path(macro)

    # 尾部风险不再直接翻转 tilts — 通过主导机制加权 + 相对排名已内化：
    # 当 default_pressure 高时，它成为主导机制，自然压低风险资产排名、提升避险资产排名。

    # ── 交叉判断 → 对冲规格 ──
    result.hedge_specs = _generate_hedge_specs(
        regime, tilts, result.tail_risk, result.policy_path,
    )

    # ── 综合结论 ──
    ow = [t for t in tilts if t.direction == "overweight"]
    uw = [t for t in tilts if t.direction == "underweight"]
    ow_str = ", ".join(t.asset_type for t in ow[:3]) if ow else "无"
    uw_str = ", ".join(t.asset_type for t in uw[:3]) if uw else "无"

    tail_warn = ""
    if result.tail_risk.severity == "severe":
        tail_warn = " ⚠ 尾部风险严重，加强对冲"
    elif result.tail_risk.severity == "moderate":
        tail_warn = " ⚠ 存在尾部风险信号"

    result.conclusion = (
        f"周期: {regime.short_cycle_phase} | 象限: {regime.quadrant}\n"
        f"  超配: {ow_str}\n"
        f"  低配: {uw_str}\n"
        f"  对冲: {len(result.hedge_specs)} 个保护情景"
        f"{tail_warn}"
    )

    return result


# ══════════════════════════════════════════════════════════════
#  转换为 DalioResult（对接 scorer）
# ══════════════════════════════════════════════════════════════


def to_dalio_result(chain: DalioChainResult) -> DalioResult:
    """将链结果转换为 DalioResult（dimensions.py 定义的标准输出）。"""
    # 信号
    if chain.regime is None:
        signal = "数据不足"
    elif chain.tail_risk and chain.tail_risk.severity == "severe":
        signal = "高风险 — 加强对冲"
    elif chain.active_tilts:
        top = chain.active_tilts[0]
        signal = f"{top.direction} {top.asset_type}"
    else:
        signal = "风险平价底仓"

    return DalioResult(
        school_score=SchoolScore(
            school=School.DALIO,
            score=0.0,  # 达利欧链不产生 1-10 分
            raw_points=0.0,
            signal=signal,
        ),
        regime=chain.regime,
        risk_parity_baseline=chain.risk_parity_baseline,
        active_tilts=chain.active_tilts,
        hedge_specs=chain.hedge_specs,
    )


# ══════════════════════════════════════════════════════════════
#  格式化
# ══════════════════════════════════════════════════════════════

_VERDICT_MARK = {Verdict.HOLDS: "●", Verdict.BREAKS: "✗", Verdict.UNCLEAR: "?"}


def format_dalio(chain: DalioChainResult) -> str:
    """格式化达利欧链报告（终端输出）。"""
    lines = [""]
    lines.append("  达利欧因果链")
    lines.append("  ════════════════════════════════════════════════")

    # 轨迹信号（如果有 regime，从 chain 的第一个 step 推断 macro 来源）
    # 轨迹信息已在 evidence 中，此处无需额外处理

    # 线 1: 各步骤
    lines.append("\n  线 1: 周期定位 → 类型选择")
    for i, step in enumerate(chain.steps):
        mark = _VERDICT_MARK[step.verdict]
        lines.append(f"    {mark} {step.name}: {step.detail}")
        for ev in step.evidence:
            lines.append(f"      · {ev}")
        if i < len(chain.steps) - 1 and step.verdict == Verdict.HOLDS:
            lines.append("    ↓")

    # 主动押注
    if chain.active_tilts:
        lines.append("\n  主动押注:")
        for t in chain.active_tilts:
            arrow = "▲" if t.direction == "overweight" else "▼"
            lines.append(
                f"    {arrow} {t.asset_type}  {t.magnitude:.0%}"
                f"  (置信度 {t.confidence:.0%})"
            )
            lines.append(f"      逻辑: {t.thesis}")
            lines.append(f"      失效: {t.decay_trigger}")

    # 风险平价
    if chain.risk_parity_baseline:
        lines.append("\n  风险平价底仓:")
        for asset, w in chain.risk_parity_baseline.items():
            lines.append(f"    {asset}: {w:.1%}")

    # 线 2: 认知盲区
    lines.append("\n  线 2: 认知盲区")

    if chain.tail_risk:
        tr = chain.tail_risk
        severity_label = {"none": "无", "moderate": "中等", "severe": "严重"}
        lines.append(f"    尾部风险: {severity_label.get(tr.severity, tr.severity)}")
        for r in tr.risks:
            lines.append(f"      ⚠ {r}")

    if chain.policy_path:
        pp = chain.policy_path
        lines.append(f"    政策路径: {pp.detail}")
        if pp.rate_room is not None:
            lines.append(f"      降息空间: {pp.rate_room:.0f}bp")
        lines.append(f"      财政空间: {pp.fiscal_room}")
        for tool in pp.likely_tools:
            lines.append(f"      工具: {tool}")
        for asset, impact in pp.impact_on_assets.items():
            lines.append(f"      {asset}: {impact}")

    # 对冲规格
    if chain.hedge_specs:
        lines.append("\n  对冲规格 (→ Axion):")
        for i, h in enumerate(chain.hedge_specs, 1):
            lines.append(f"    [{i}] 保护: {h.protects_against}")
            lines.append(f"        最大可接受亏损: {h.max_acceptable_loss:.0%}")
            lines.append(f"        裸露敞口: {h.unhedged_exposure}")

    lines.append(f"\n  ════════════════════════════════════════════════")
    lines.append(f"  {chain.conclusion}")
    lines.append("")
    return "\n".join(lines)
