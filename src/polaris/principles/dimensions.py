"""
三流派评分框架
=============
巴菲特：内在价值 — 这门生意本身好不好？值不值得永久持有？
达利欧：周期定位 — 当前周期环境下，这家公司是安全的还是脆弱的？
索罗斯：反身性   — 市场认知与现实的偏差，是否存在可利用的反身性循环？
"""

from dataclasses import dataclass, field
from enum import Enum


class School(str, Enum):
    BUFFETT = "buffett"
    DALIO = "dalio"
    SOROS = "soros"


SCHOOL_LABELS = {
    School.BUFFETT: "巴菲特·内在价值",
    School.DALIO: "达利欧·周期定位",
    School.SOROS: "索罗斯·反身性",
}

SCHOOL_QUESTIONS = {
    School.BUFFETT: "这门生意本身好不好？值不值得永久持有？",
    School.DALIO: "当前周期环境下，应该持有什么类型的资产？主动押注的风险收益结构是什么？",
    School.SOROS: "市场认知与现实之间，是否存在可利用的偏差？",
}


@dataclass
class Driver:
    """评分驱动因子。"""
    rule_name: str
    contribution: float
    description: str = ""


@dataclass
class SchoolScore:
    """单流派评分结果。"""
    school: School
    score: float          # 1-10
    raw_points: float
    signal: str = ""      # 巴菲特: 值得持有/不值得持有/无法估值
                          # 达利欧: 安全/脆弱/极度脆弱
                          # 索罗斯: 中性/正向加强/正向脆弱/负向过度
    drivers: list[Driver] = field(default_factory=list)


# ── 巴菲特·结构化输出 ──────────────────────────────────────────────


@dataclass
class BuffettResult:
    """巴菲特流派完整输出：过滤 + 评分 + 内在价值。"""
    school_score: SchoolScore
    filters_passed: bool = False
    filter_details: dict[str, bool] = field(default_factory=dict)
    # 内在价值（仅通过过滤时计算）
    intrinsic_value: float | None = None
    valuation_path: str | None = None   # A / B / C / D
    valuation_status: str = "unvaluable"  # valued / unvaluable / divergent
    key_assumptions: dict[str, float] = field(default_factory=dict)


# ── 达利欧·结构化输出 ──────────────────────────────────────────────


@dataclass
class CycleRegime:
    """宏观周期定位。"""
    quadrant: str                     # growth_up_inflation_up / growth_up_inflation_down
                                      # growth_down_inflation_up / growth_down_inflation_down
    short_cycle_phase: str            # early_expansion / late_expansion
                                      # early_contraction / late_contraction
    long_cycle_phase: str             # early_leverage / mid_leverage / late_leverage
                                      # deleveraging_ugly / deleveraging_beautiful
    confidence: float = 0.0           # 定位置信度 0-1

    # gap 幅度 — 象限内的定量信号
    growth_gap: float = 0.0           # 实际-预期 GDP 增速 (pp)
    inflation_gap: float = 0.0        # 实际-预期 CPI (pp)

    # 特殊体制标记
    zero_bound_qe: bool = False       # 利率在零下界 + 信贷靠政策扩张（QE 体制）


@dataclass
class Tilt:
    """单个主动押注。"""
    asset_type: str                   # defensive_equity / cyclical_equity / commodity
                                      # nominal_bond / inflation_linked_bond / cash / gold
    direction: str                    # overweight / underweight
    magnitude: float                  # 偏移幅度 0-1 (0=不动, 1=极端)
    thesis: str                       # 押注的因果逻辑
    confidence: float                 # 这个判断的置信度 0-1
    decay_trigger: str                # 什么信号出现时这个押注失效


@dataclass
class HedgeSpec:
    """对冲需求规格 — Polaris 输出，Axion 执行。"""
    protects_against: str             # 对冲的反面情景描述
    max_acceptable_loss: float        # 该情景下可接受的最大亏损 (%)
    unhedged_exposure: str            # 当前配置在该情景下最大的裸露敞口


@dataclass
class DalioResult:
    """达利欧流派完整输出：周期定位 → 主动押注 → 对冲规格。

    三层结构:
      底层: risk_parity_baseline — 全天候底仓，不依赖任何判断，始终存在
      中层: active_tilts — 主动偏移，利润来源，基于周期判断的方向性押注
      顶层: hedge_specs — 尾部保护，截断"判断错了"的最大损失
    """
    school_score: SchoolScore

    # ── 第 ① 步: 宏观象限定位 ──
    regime: CycleRegime | None = None

    # ── 第 ② 步: 类型选择 ──
    # 底层: 风险平价基线（被动，不依赖判断）
    risk_parity_baseline: dict[str, float] = field(default_factory=dict)
    # 中层: 主动押注（利润来源）
    active_tilts: list[Tilt] = field(default_factory=list)

    # ── 对冲接口 → Axion ──
    # 顶层: 每个主动押注的反面情景保护
    hedge_specs: list[HedgeSpec] = field(default_factory=list)

    # ── 第 ③ 步: 载体落地（暂缓，数据管线就绪后实现）──
    # carrier_filters: list[CarrierFilter] — 在选定类型中筛选具体公司


# ── 索罗斯·结构化输出 ──────────────────────────────────────────────


@dataclass
class SorosResult:
    """索罗斯流派完整输出：预期偏差 + 反身性阶段。"""
    school_score: SchoolScore
    # 预期偏差（需外部价格数据做反向 DCF）
    implied_growth_rate: float | None = None
    actual_growth_rate: float | None = None
    expectation_gap: float | None = None
    gap_trend: float | None = None
    # 反身性阶段
    reflexivity_phase: str = "unknown"
    financing_dependency: float | None = None
    leverage_acceleration: float | None = None
    vulnerability_if_reversal: str | None = None


# ── 流派内子维度（用于规则分组）──────────────────────────────────


class BuffettDimension(str, Enum):
    BUSINESS_MODEL = "business_model"
    MOAT = "moat"
    OWNER_EARNINGS = "owner_earnings"
    EARNINGS_QUALITY = "earnings_quality"
    CAPITAL_ALLOCATION = "capital_allocation"
    MANAGEMENT = "management"
    PREDICTABILITY = "predictability"
    FINANCIAL_SAFETY = "financial_safety"


class DalioDimension(str, Enum):
    # 第 ① 步: 宏观定位
    CYCLE_REGIME = "cycle_regime"
    # 第 ② 步: 类型选择
    TRANSMISSION = "transmission"         # 宏观→公司传导通道分析
    RISK_PARITY = "risk_parity"           # 全天候底仓配置
    ACTIVE_TILTS = "active_tilts"         # 主动偏移押注
    # 对冲
    TAIL_RISK = "tail_risk"              # 尾部风险检测
    POLICY_PATH = "policy_path"          # 政策路径分析
    # 第 ③ 步: 载体落地（条件化的公司级指标）
    DEBT_HEALTH = "debt_health"           # 条件化于周期阶段的债务健康
    RATE_SENSITIVITY = "rate_sensitivity"
    LIQUIDITY = "liquidity"


class SorosDimension(str, Enum):
    FINANCING_DEPENDENCY = "financing_dep"
    LEVERAGE_DYNAMICS = "leverage_dynamics"
    EXPECTATIONS = "expectations"
