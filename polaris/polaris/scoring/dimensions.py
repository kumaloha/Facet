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
    School.DALIO: "当前周期环境下，这家公司是安全的还是脆弱的？",
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
class DalioResult:
    """达利欧流派完整输出：象限 + 风险平价 + 公司脆弱度。"""
    school_score: SchoolScore
    # 象限（需外部宏观数据）
    quadrant: str | None = None       # QUAD_1 / QUAD_2 / QUAD_3 / QUAD_4
    growth_gap: float | None = None
    inflation_gap: float | None = None
    valuation_signal: str | None = None  # SELL_RISK_ASSETS / BUY_THE_FEAR / HOLD
    # 风险平价（需外部价格数据）
    risk_parity_weights: dict[str, float] | None = None
    # 公司级
    asset_bucket: str | None = None
    vulnerability_score: float | None = None
    vulnerability_drivers: list[Driver] = field(default_factory=list)


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
    DEBT_HEALTH = "debt_health"
    RATE_SENSITIVITY = "rate_sensitivity"
    LIQUIDITY = "liquidity"


class SorosDimension(str, Enum):
    FINANCING_DEPENDENCY = "financing_dep"
    LEVERAGE_DYNAMICS = "leverage_dynamics"
    EXPECTATIONS = "expectations"
