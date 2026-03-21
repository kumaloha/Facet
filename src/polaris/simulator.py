"""
预演模块 — 帝国周期 + 三层嵌套框架
====================================

达利欧完整框架的三层嵌套:
  1. 帝国周期（国家兴衰六阶段）→ 约束终点
  2. 五大力量（F1a短债务/F1b长债务/F2内部/F3外部/F4自然/F5技术）→ 约束路径空间
  3. 短周期+偏差 → 搜索具体路径（哪个弱点先爆）

所有判断基于: 百分位(在历史中的位置) + 趋势(方向和加速度)
"""

from __future__ import annotations

from dataclasses import dataclass, field

from anchor.compute.percentile_trend import (
    IndicatorAssessment,
    TrendTier,
    assess_from_fred_history,
    compute_percentile,
)
from polaris.chains.forces_pure import (
    FORCE1A_INDICATORS,
    FORCE1B_INDICATORS,
    FORCE2_INDICATORS,
    FORCE3_INDICATORS,
    FORCE4_INDICATORS,
    FORCE5_INDICATORS,
    _build_derived_series,
)


# ━━ 数据结构 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@dataclass
class EmpireStage:
    """帝国周期阶段"""
    stage: int              # 1-6
    stage_name: str         # "new_order"/"building"/"prosperity"/"overextension"/"conflict"/"restructuring"
    confidence: float
    evidence: list[str]
    destination: str        # 这个阶段的必然终点描述
    key_variable: str       # 当前阶段的关键变量


@dataclass
class MonetaryPolicyConflict:
    """货币政策多重约束冲突"""
    conflicting_mandates: list[str]     # 哪些责任在互相打架
    most_likely_sacrifice: str          # 最可能被牺牲的
    severity: float                     # 冲突严重度 0-1


@dataclass
class ForceStatus:
    """单个力量的当前状态"""
    force_id: str           # "f1a"/"f1b"/"f2"/"f3"/"f4"/"f5"
    force_name: str
    role_in_empire: str     # 在帝国周期中的角色
    status: str             # "accelerating_decline"/"declining"/"stable"/"improving"/"key_variable"
    percentile: float       # 综合百分位
    evidence: list[str]


@dataclass
class Vulnerability:
    """脆弱点"""
    location: str
    severity: float
    mechanism: str          # 因果传导链
    trigger: str
    empire_context: str     # 在帝国周期中为什么这个点重要


@dataclass
class SimulationResult:
    """完整预演结果"""
    # 第一层: 帝国周期
    empire_stage: EmpireStage

    # 第二层: 五力机制
    forces: list[ForceStatus]
    monetary_conflict: MonetaryPolicyConflict | None

    # 第三层: 具体路径
    vulnerabilities: list[Vulnerability]     # 最多2个
    next_step: str                           # 下一步最可能发生什么
    key_question: str                        # 当前最关键的一个问题

    snapshot_date: str

    # 玩家地图（可选，谁在买/卖/有压力）
    player_map: object | None = None

    # 新闻上下文（可选，用于给脆弱点提供具体解释）
    news_context: list = field(default_factory=list)


# ━━ 辅助 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


# 所有需要评估的指标（合并各力量列表 + 补充）
_ALL_INDICATORS = (
    FORCE1A_INDICATORS + FORCE1B_INDICATORS
    + FORCE2_INDICATORS + FORCE3_INDICATORS
    + FORCE4_INDICATORS + FORCE5_INDICATORS
)


def _assess_indicators(
    indicator_list: list[tuple[str, str, bool | None]],
    derived: dict,
    month: str,
) -> dict[str, IndicatorAssessment]:
    """批量评估指标，返回 {derived_key: assessment}。"""
    results: dict[str, IndicatorAssessment] = {}
    for data_key, display_name, higher_is_worse in indicator_list:
        series = derived.get(data_key)
        if series is None:
            continue
        if higher_is_worse is None:
            series = {m: abs(v) for m, v in series.items()}
            higher_is_worse = True
        a = assess_from_fred_history(display_name, month, series, higher_is_worse)
        if a.value is not None:
            results[data_key] = a
    return results


def _pct(a: IndicatorAssessment | None) -> float:
    """取百分位，None 时返回 50（中性）。"""
    if a is None:
        return 50.0
    return a.percentile if a.percentile is not None else 50.0


def _is_deteriorating(a: IndicatorAssessment) -> bool:
    return a.trend in (TrendTier.DETERIORATING, TrendTier.ACCELERATING_DETERIORATION)


def _is_improving(a: IndicatorAssessment) -> bool:
    return a.trend in (TrendTier.IMPROVING, TrendTier.ACCELERATING_IMPROVEMENT)


def _is_accelerating_bad(a: IndicatorAssessment) -> bool:
    return a.trend == TrendTier.ACCELERATING_DETERIORATION


# ━━ 帝国周期阶段定义 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


_EMPIRE_STAGES = {
    1: ("new_order", "新秩序", "从废墟中建立新体系，低债务低冲突"),
    2: ("building", "建设期", "教育/基建/制度投入，生产率上升"),
    3: ("prosperity", "繁荣期", "收获投入回报，成为世界领袖"),
    4: ("overextension", "过度扩张", "消费超出生产，债务开始积累"),
    5: ("conflict", "内外冲突", "贫富分化+外部挑战+债务不可持续"),
    6: ("restructuring", "重组期", "货币贬值+权力转移+内部重组"),
}

_EMPIRE_DESTINATIONS = {
    1: "建立新的货币和制度体系",
    2: "进入繁荣期，但基础设施投入终将饱和",
    3: "过度扩张不可避免 — 成功导致自满和挥霍",
    4: "内外冲突不可避免 — 过度扩张积累的矛盾必须释放",
    5: "货币贬值+权力转移+内部重组（不可避免，问题是快慢和烈度）",
    6: "新秩序的诞生 — 从废墟中重建",
}

_EMPIRE_KEY_VARIABLES = {
    1: "新制度能否赢得信任",
    2: "教育和基建投入的质量",
    3: "能否控制成功带来的自满",
    4: "F5(技术)能否延缓到达冲突期",
    5: "F5(AI/技术)能否续命 — 续命几十年 or 加速几年内到达重组",
    6: "新秩序的设计者是谁",
}


# ━━ 1. 帝国阶段检测 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def detect_empire_stage(
    fred_history: dict,
    month: str,
) -> EmpireStage:
    """
    百分位驱动的帝国阶段检测。

    用债务、不平等、外部压力、生产率的百分位组合判断：
    - 债务低+不平等低+生产率高 → 阶段2-3（建设/繁荣）
    - 债务高+不平等升+外部挑战 → 阶段4-5（过度扩张/冲突）
    - 债务极高+内部极化+货币政策困境 → 阶段5→6过渡
    """
    derived = _build_derived_series(fred_history)
    indicators = _assess_indicators(_ALL_INDICATORS, derived, month)

    debt_pct = _pct(indicators.get("total_debt_gdp"))
    gini_pct = _pct(indicators.get("gini"))
    unemp_pct = _pct(indicators.get("unemployment"))
    epu_pct = _pct(indicators.get("epu_index"))
    prod_pct = _pct(indicators.get("productivity_growth"))
    rate_pct = _pct(indicators.get("fed_funds_rate"))
    fiscal_pct = _pct(indicators.get("fiscal_deficit_gdp"))

    evidence: list[str] = []

    # 评分逻辑: 从低阶段到高阶段打分
    # 高债务 → 高阶段
    stage_score = 0.0

    if debt_pct > 70:
        stage_score += 2.0
        evidence.append(f"债务/GDP P{debt_pct:.0f} — 高位")
    elif debt_pct > 50:
        stage_score += 1.0
        evidence.append(f"债务/GDP P{debt_pct:.0f} — 中高位")
    else:
        evidence.append(f"债务/GDP P{debt_pct:.0f} — 低位")

    # 高不平等 → 高阶段
    if gini_pct > 70:
        stage_score += 1.5
        evidence.append(f"基尼系数 P{gini_pct:.0f} — 极化严重")
    elif gini_pct > 50:
        stage_score += 0.5
        evidence.append(f"基尼系数 P{gini_pct:.0f} — 中等不平等")

    # 外部压力
    if epu_pct > 70:
        stage_score += 1.0
        evidence.append(f"政策不确定性 P{epu_pct:.0f} — 外部压力大")
    elif epu_pct > 50:
        stage_score += 0.3
        evidence.append(f"政策不确定性 P{epu_pct:.0f}")

    # 生产率低 → 高阶段（创新枯竭）
    if prod_pct < 30:
        stage_score += 0.5
        evidence.append(f"生产率 P{prod_pct:.0f} — 低迷")
    elif prod_pct > 70:
        stage_score -= 0.5
        evidence.append(f"生产率 P{prod_pct:.0f} — 活跃")

    # 财政赤字高 → 高阶段
    if fiscal_pct > 70:
        stage_score += 0.5
        evidence.append(f"财政赤字 P{fiscal_pct:.0f} — 高位")

    # 利率趋近于零 = 政策空间耗尽 → 阶段5-6信号
    if rate_pct < 30 and debt_pct > 70:
        stage_score += 0.5
        evidence.append(f"利率 P{rate_pct:.0f} + 高债务 — 政策空间耗尽")

    # 映射到阶段
    if stage_score < 1.0:
        stage = 2  # 建设期（阶段1=新秩序太罕见，不做检测）
        confidence = 0.5
    elif stage_score < 2.0:
        stage = 3  # 繁荣期
        confidence = 0.6
    elif stage_score < 3.5:
        stage = 4  # 过度扩张
        confidence = 0.65
    elif stage_score < 5.0:
        stage = 5  # 内外冲突
        confidence = 0.7
    else:
        # 阶段5→6过渡：债务极高+内部极化+政策困境
        stage = 5  # 仍标记为5，但confidence更高表示接近6
        confidence = 0.8
        if debt_pct > 80 and gini_pct > 70:
            evidence.append("阶段5→6过渡信号: 债务极高+内部极化")

    stage_name = _EMPIRE_STAGES[stage][0]
    destination = _EMPIRE_DESTINATIONS[stage]
    key_variable = _EMPIRE_KEY_VARIABLES[stage]

    return EmpireStage(
        stage=stage,
        stage_name=stage_name,
        confidence=round(confidence, 2),
        evidence=evidence,
        destination=destination,
        key_variable=key_variable,
    )


# ━━ 2. 五力状态评估 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


# 每个力量在帝国周期中的角色
_FORCE_ROLES = {
    "f1a": "节拍器 — 每次短周期下行都揭开长周期烂账",
    "f1b": "终点的数学必然性 — 债务积累不可逆",
    "f2": "终点的社会表现 — 不平等终将爆发",
    "f3": "终点的地缘表现 — 挑战者必然出现",
    "f4": "催化剂 — 随机加速",
    "f5": "唯一能延缓终点的力量 — 续命还是加速",
}

_FORCE_NAMES = {
    "f1a": "短债务周期",
    "f1b": "长债务周期",
    "f2": "内部秩序",
    "f3": "外部秩序",
    "f4": "自然之力",
    "f5": "技术",
}


def _avg_percentile(indicators: dict[str, IndicatorAssessment], keys: list[str]) -> float:
    """计算一组指标的平均百分位。"""
    vals = [_pct(indicators.get(k)) for k in keys if k in indicators]
    return sum(vals) / len(vals) if vals else 50.0


def _determine_status(
    avg_pct: float,
    indicators: dict[str, IndicatorAssessment],
    keys: list[str],
) -> str:
    """根据百分位和趋势判断力量状态。"""
    # 检查趋势方向
    deteriorating_count = 0
    improving_count = 0
    accel_bad = False
    for k in keys:
        a = indicators.get(k)
        if a is None:
            continue
        if _is_deteriorating(a):
            deteriorating_count += 1
            if _is_accelerating_bad(a):
                accel_bad = True
        elif _is_improving(a):
            improving_count += 1

    if accel_bad and avg_pct > 50:
        return "accelerating_decline"
    elif deteriorating_count > improving_count and avg_pct > 50:
        return "declining"
    elif improving_count > deteriorating_count:
        return "improving"
    else:
        return "stable"


def assess_forces(
    fred_history: dict,
    month: str,
    empire_stage: EmpireStage,
) -> list[ForceStatus]:
    """评估六大力量（F1a/F1b/F2/F3/F4/F5）在帝国周期中的状态。"""
    derived = _build_derived_series(fred_history)
    indicators = _assess_indicators(_ALL_INDICATORS, derived, month)

    force_configs: list[tuple[str, list[tuple[str, str, bool | None]]]] = [
        ("f1a", FORCE1A_INDICATORS),
        ("f1b", FORCE1B_INDICATORS),
        ("f2", FORCE2_INDICATORS),
        ("f3", FORCE3_INDICATORS),
        ("f4", FORCE4_INDICATORS),
        ("f5", FORCE5_INDICATORS),
    ]

    results: list[ForceStatus] = []
    for force_id, indicator_list in force_configs:
        keys = [k for k, _, _ in indicator_list]
        avg_pct = _avg_percentile(indicators, keys)
        status = _determine_status(avg_pct, indicators, keys)

        # 构建证据
        evidence: list[str] = []
        for k in keys:
            a = indicators.get(k)
            if a is not None:
                pct = _pct(a)
                trend_cn = {
                    TrendTier.ACCELERATING_DETERIORATION: "加速恶化",
                    TrendTier.DETERIORATING: "恶化",
                    TrendTier.STABLE: "稳定",
                    TrendTier.IMPROVING: "改善",
                    TrendTier.ACCELERATING_IMPROVEMENT: "加速改善",
                }.get(a.trend, "未知")
                evidence.append(f"{a.name} P{pct:.0f} {trend_cn}")

        # 阶段5+F5活跃 → 标记为 key_variable
        if force_id == "f5" and empire_stage.stage >= 4 and status == "improving":
            status = "key_variable"

        results.append(ForceStatus(
            force_id=force_id,
            force_name=_FORCE_NAMES[force_id],
            role_in_empire=_FORCE_ROLES[force_id],
            status=status,
            percentile=round(avg_pct, 1),
            evidence=evidence,
        ))

    return results


# ━━ 3. 货币政策约束冲突 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def detect_monetary_conflict(
    fred_history: dict,
    month: str,
) -> MonetaryPolicyConflict | None:
    """
    检测货币政策的多重约束冲突。

    五个责任:
    1. 就业 → 需要低利率
    2. CPI控制 → 需要高利率
    3. 金融稳定 → 需要低利率（兜底泡沫）
    4. 国债可持续 → 需要低利率（政府还不起高息）
    5. 全球美元流动性 → 需要低利率
    """
    derived = _build_derived_series(fred_history)
    indicators = _assess_indicators(_ALL_INDICATORS, derived, month)

    cpi_pct = _pct(indicators.get("cpi_yoy"))
    unemp_pct = _pct(indicators.get("unemployment"))
    debt_pct = _pct(indicators.get("total_debt_gdp"))
    rate_pct = _pct(indicators.get("fed_funds_rate"))
    spread_pct = _pct(indicators.get("credit_spread_hy"))

    conflicts: list[str] = []

    # CPI高 → 需要加息
    if cpi_pct > 60:
        if debt_pct > 60:
            conflicts.append("CPI需要加息 vs 国债还不起高息")
        if unemp_pct > 50:
            conflicts.append("CPI需要加息 vs 就业需要降息")
        if spread_pct > 60:
            conflicts.append("CPI需要加息 vs 金融稳定需要降息")

    # 利率已高 + 债务高 = 偿债压力
    if rate_pct > 60 and debt_pct > 60:
        conflicts.append("利率高位 vs 政府偿债压力")

    # 就业弱 + 通胀高 = 滞胀困境
    if unemp_pct > 60 and cpi_pct > 60:
        conflicts.append("滞胀困境: 就业和通胀同时需要相反的利率方向")

    if not conflicts:
        return None

    # 判断最可能被牺牲的
    # 历史规律: 央行最终总是选择印钱（牺牲CPI）
    if debt_pct > 70:
        sacrifice = "CPI目标（印钱→通胀→货币贬值）"
    elif unemp_pct > 70:
        sacrifice = "CPI目标（降息保就业）"
    elif spread_pct > 70:
        sacrifice = "CPI目标（降息保金融稳定）"
    else:
        sacrifice = "就业（维持紧缩压制通胀）"

    severity = min(len(conflicts) / 4.0, 1.0)

    return MonetaryPolicyConflict(
        conflicting_mandates=conflicts,
        most_likely_sacrifice=sacrifice,
        severity=round(severity, 2),
    )


# ━━ 4. 脆弱点识别 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


# 因果传导链（帝国周期感知版本）
_CAUSAL_CHAINS = [
    # 短周期 → 长周期揭盖
    ("fed_funds_rate", "mortgage_delinquency", "利率→偿债→逾期→信用风险",
     "F1a下行揭开F1b隐藏坏账"),
    ("lending_standards", "credit_growth", "银行收紧→信贷收缩",
     "短周期信贷紧缩的传导"),
    ("credit_growth", "gdp_growth", "信贷收缩→经济放缓",
     "信贷是经济的血液"),
    ("credit_growth", "unemployment", "信贷收缩→企业裁员",
     "信贷收缩→企业失去融资→裁员"),

    # 利率传导
    ("fed_funds_rate", "credit_spread_hy", "利率升→融资成本→信用风险",
     "利率是F1b的引信"),

    # 就业-消费正反馈
    ("unemployment", "retail_sales_growth", "失业→消费下降",
     "F2社会压力的核心传导"),

    # 消费信贷恶化
    ("consumer_credit_growth", "credit_card_delinquency", "过度借贷→还不起",
     "短期信贷质量恶化"),

    # 供应链传导
    ("cass_freight_yoy", "gdp_growth", "货运中断→生产停滞→GDP下降",
     "F4自然冲击的传导"),
    ("cass_freight_yoy", "unemployment", "供应链断→企业停工→失业",
     "F4→F2的跨力量传导"),
    ("import_price_yoy", "cpi_yoy", "进口成本飙升→通胀",
     "F3外部冲突通过供应链打到F1a"),
    ("transport_cpi_yoy", "import_price_yoy", "运输成本升→进口成本升",
     "F4→F3的跨力量传导"),

    # 跨力量传导（帝国周期特有）
    # F5失败 → 坏账加到F1b
    ("productivity_growth", "total_debt_gdp", "生产率低迷→债务/GDP恶化",
     "F5失败→F1b加速"),
]

# 帝国阶段对弱点的权重加成
_EMPIRE_VULNERABILITY_WEIGHTS: dict[int, dict[str, float]] = {
    # 阶段4: 过度扩张 — 信贷质量和利率是命门
    4: {
        "fed_funds_rate": 1.8,
        "mortgage_delinquency": 1.5,
        "credit_card_delinquency": 1.5,
        "total_debt_gdp": 1.5,
        "lending_standards": 1.3,
    },
    # 阶段5: 冲突 — F1b指标权重最高（终点的核心机制）
    5: {
        "total_debt_gdp": 2.0,           # F1b: 终点的数学必然性
        "fed_funds_rate": 1.8,           # 利率是F1b的引信
        "productivity_growth": 1.5,      # F5: 关键变量
        "gini": 1.5,                     # F2: 社会爆发点
        "epu_index": 1.3,               # F3: 外部挑战
        "mortgage_delinquency": 1.5,
        "credit_spread_hy": 1.5,
        "household_debt_gdp": 1.5,
    },
}


def identify_vulnerabilities(
    fred_history: dict,
    month: str,
    empire_stage: EmpireStage,
) -> list[Vulnerability]:
    """
    两层脆弱点识别（帝国周期感知版本）。

    第一层（事前）: 结构性弱点 — 高百分位 + 帝国阶段加权 + 下游连接多
    第二层（事后）: 突变检测 — 3个月内百分位跳变>30
    """
    derived = _build_derived_series(fred_history)
    indicators = _assess_indicators(_ALL_INDICATORS, derived, month)

    # 额外补充零售
    for key, name, hiw in [("retail_sales_growth", "零售销售增速", False)]:
        series = derived.get(key)
        if series is not None:
            a = assess_from_fred_history(name, month, series, hiw)
            if a.value is not None:
                indicators[key] = a

    # 计算每个指标的下游连接数
    downstream_count: dict[str, int] = {}
    for upstream, _, _, _ in _CAUSAL_CHAINS:
        downstream_count[upstream] = downstream_count.get(upstream, 0) + 1

    # 帝国阶段权重
    empire_weights = _EMPIRE_VULNERABILITY_WEIGHTS.get(empire_stage.stage, {})

    # ── 第一层: 结构性弱点 ──
    structural_vulns: list[Vulnerability] = []
    for key, a in indicators.items():
        pct = _pct(a)
        connections = downstream_count.get(key, 0)
        if connections == 0:
            continue

        if pct > 65:
            # 找下游传导链
            chain_parts: list[str] = []
            empire_context_parts: list[str] = []
            for up, _, mech, ctx in _CAUSAL_CHAINS:
                if up == key:
                    chain_parts.append(mech)
                    empire_context_parts.append(ctx)

            base_severity = (pct / 100) * (0.5 + connections * 0.25)
            empire_boost = empire_weights.get(key, 1.0)
            severity = min(base_severity * empire_boost, 1.0)

            empire_ctx = ""
            if empire_boost > 1.0:
                empire_ctx = f"帝国阶段{empire_stage.stage}关键指标(权重x{empire_boost:.1f})"
            if empire_context_parts:
                empire_ctx += (": " if empire_ctx else "") + empire_context_parts[0]

            structural_vulns.append(Vulnerability(
                location=a.name,
                severity=round(severity, 2),
                mechanism=f"P{pct:.0f} → {' → '.join(chain_parts)}" if chain_parts else f"P{pct:.0f}",
                trigger="冲击此节点将沿因果链传导",
                empire_context=empire_ctx,
            ))

    structural_vulns.sort(key=lambda v: v.severity, reverse=True)

    # ── 第二层: 突变检测 ──
    _hiw_map = {k: hiw for k, _, hiw in _ALL_INDICATORS}
    _hiw_map["retail_sales_growth"] = False

    sudden_vuln: Vulnerability | None = None
    max_jump = 0.0
    for key, a in indicators.items():
        pct_now = _pct(a)
        series = derived.get(key)
        if series is None:
            continue

        year, mo = int(month[:4]), int(month[5:7])
        mo3 = mo - 3
        yr3 = year
        if mo3 <= 0:
            mo3 += 12
            yr3 -= 1
        month_3ago = f"{yr3}-{mo3:02d}"
        sorted_m = sorted(m for m in series if m <= month_3ago)
        if not sorted_m:
            continue
        hist_3ago = [series[m] for m in sorted_m]
        pct_3ago = compute_percentile(
            hist_3ago[-1], hist_3ago[:-1] if len(hist_3ago) > 1 else []
        )

        jump = abs(pct_now - pct_3ago)

        hiw = _hiw_map.get(key, True)
        if hiw is None:
            hiw = True
        if hiw:
            is_worsening = pct_now > pct_3ago
        else:
            is_worsening = pct_now < pct_3ago

        if not is_worsening:
            continue

        if jump > max_jump and jump > 30:
            max_jump = jump
            hit_chains = []
            hit_contexts = []
            for up, _, mech, ctx in _CAUSAL_CHAINS:
                if up == key:
                    hit_chains.append(mech)
                    hit_contexts.append(ctx)

            mechanism = f"突变(P{pct_3ago:.0f}→P{pct_now:.0f})"
            if hit_chains:
                mechanism += f" 打到传导链: {' → '.join(hit_chains)}"

            empire_ctx = hit_contexts[0] if hit_contexts else ""

            sudden_vuln = Vulnerability(
                location=a.name,
                severity=round(jump / 100, 2),
                mechanism=mechanism,
                trigger="冲击已发生，关注下游传导",
                empire_context=empire_ctx,
            )

    # 输出最多2个
    vulns: list[Vulnerability] = []
    if sudden_vuln:
        vulns.append(sudden_vuln)
    for sv in structural_vulns:
        if sv.location not in {v.location for v in vulns}:
            vulns.append(sv)
            if len(vulns) >= 2:
                break

    return vulns[:2]


# ━━ 5. 关键问题 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def generate_key_question(
    empire_stage: EmpireStage,
    forces: list[ForceStatus],
) -> str:
    """基于帝国阶段和五力状态，输出当前最关键的一个问题。"""
    f_map = {f.force_id: f for f in forces}

    f5 = f_map.get("f5")
    f1a = f_map.get("f1a")
    f1b = f_map.get("f1b")
    f3 = f_map.get("f3")
    f2 = f_map.get("f2")

    if empire_stage.stage >= 5:
        # 阶段5: 最关键的分水岭
        if f5 and f5.status in ("improving", "key_variable"):
            return "AI/技术能否带来真正的生产率提升，延缓帝国周期终点到达？"
        if f1a and f1a.status in ("declining", "accelerating_decline"):
            return "这次短周期收缩会揭开多少长周期烂账？F1a下行是否触发F1b暴露？"
        if f3 and f3.status in ("declining", "accelerating_decline"):
            return "贸易/技术冲突会不会通过供应链打到F1a的弱点？"
        if f2 and f2.status in ("declining", "accelerating_decline"):
            return "内部极化是否会导致政策瘫痪，丧失应对危机的能力？"
        if f1b and f1b.percentile > 70:
            return "国债利息占财政收入的比例何时突破临界点？"
        return "哪个力量会率先触发从阶段5到阶段6的跳跃？"

    elif empire_stage.stage == 4:
        if f5 and f5.status in ("improving", "key_variable"):
            return "技术创新能否延缓过度扩张向冲突期的转化？"
        if f1b and f1b.percentile > 60:
            return "债务积累速度是否已超过经济增长速度？临界点在哪？"
        return "过度扩张的哪个方面会最先暴露 — 债务、不平等、还是外部挑战？"

    elif empire_stage.stage == 3:
        return "繁荣期的自满信号是否已经出现？消费是否已超出生产？"

    else:
        return "建设期的制度和基建投入质量如何？是否在为未来繁荣奠基？"


# ━━ 6. 下一步推演 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _generate_next_step(
    empire_stage: EmpireStage,
    forces: list[ForceStatus],
    vulns: list[Vulnerability],
) -> str:
    """基于力量状态和脆弱点，判断下一步最可能发生什么。"""
    f_map = {f.force_id: f for f in forces}
    f1a = f_map.get("f1a")
    f1b = f_map.get("f1b")
    f5 = f_map.get("f5")

    parts: list[str] = []

    if f1a and f1a.status in ("declining", "accelerating_decline"):
        parts.append("F1a显示收缩信号")
        if f1b and f1b.percentile > 70:
            parts.append("如果继续 → 揭盖器启动，F1b隐藏坏账将暴露")
        else:
            parts.append("短周期调整，但长债务负担尚可控")
    elif f1a and f1a.status == "improving":
        parts.append("F1a显示扩张信号")
        if f1b and f1b.percentile > 70:
            parts.append("但每次扩张都在给F1b加药量（救助→更多债务）")

    if f5 and f5.status == "key_variable":
        parts.append("F5(技术)是分水岭 — 成败决定续命还是加速")

    if vulns:
        parts.append(f"最大脆弱点: {vulns[0].location}")

    return "; ".join(parts) if parts else "各力量相对平稳，等待新的催化剂"


# ━━ 7. 统一入口 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


# ━━ 7.5 新闻事件丰富 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def enrich_with_news(result: SimulationResult, events: list) -> SimulationResult:
    """用新闻事件给脆弱点添加具体解释。

    匹配逻辑:
    - 脆弱点的 location 关键词 <-> 事件的 force_id
    """
    for vuln in result.vulnerabilities:
        related = [e for e in events if _event_matches_vulnerability(e, vuln)]
        if related:
            best = max(related, key=lambda e: e.confidence)
            vuln.mechanism = f"{vuln.mechanism}\n     原因: {best.headline}"
            if best.affected_sectors:
                vuln.mechanism += f"\n     影响行业: {', '.join(best.affected_sectors)}"
            if best.transmission:
                vuln.mechanism += f"\n     传导: {best.transmission}"

    result.news_context = events
    return result


def _event_matches_vulnerability(event, vuln) -> bool:
    """判断一个新闻事件是否和一个脆弱点相关。"""
    vuln_loc = vuln.location.lower()

    # 运输/供应链相关脆弱点 <-> F3/F4事件
    if any(k in vuln_loc for k in ["运输", "货运", "进口", "供应链", "shipping"]):
        return event.force_id in ("f3", "f4")

    # 利率/信贷相关 <-> F1a事件
    if any(k in vuln_loc for k in ["利率", "信贷", "贷款", "rate", "credit"]):
        return event.force_id == "f1a"

    # 债务相关 <-> F1b事件
    if any(k in vuln_loc for k in ["债务", "国债", "赤字", "debt", "deficit"]):
        return event.force_id == "f1b"

    # 就业相关 <-> F2事件
    if any(k in vuln_loc for k in ["失业", "就业", "unemployment", "labor"]):
        return event.force_id == "f2"

    # 生产率/技术相关 <-> F5事件
    if any(k in vuln_loc for k in ["生产率", "技术", "productivity", "tech"]):
        return event.force_id == "f5"

    return False


def simulate(fred_history: dict, month: str, news_events: list | None = None) -> SimulationResult:
    """完整预演: 帝国阶段 → 五力(F1拆分) → 玩家地图 → 货币冲突 → 脆弱点 → 关键问题"""
    empire = detect_empire_stage(fred_history, month)
    forces = assess_forces(fred_history, month, empire)
    monetary = detect_monetary_conflict(fred_history, month)
    vulns = identify_vulnerabilities(fred_history, month, empire)
    question = generate_key_question(empire, forces)
    next_step = _generate_next_step(empire, forces, vulns)

    # 玩家地图
    player_map = None
    try:
        from anchor.compute.player_tracker import build_player_map
        player_map = build_player_map(fred_history, month)

        # 用玩家信息丰富脆弱点描述
        _enrich_vulns_with_players(vulns, player_map)
    except Exception:
        pass  # 玩家数据不可用时不影响核心预演

    result = SimulationResult(
        empire_stage=empire,
        forces=forces,
        monetary_conflict=monetary,
        vulnerabilities=vulns,
        next_step=next_step,
        key_question=question,
        snapshot_date=month,
        player_map=player_map,
    )

    if news_events:
        enrich_with_news(result, news_events)

    return result


def _enrich_vulns_with_players(vulns: list[Vulnerability], player_map) -> None:
    """用玩家地图丰富脆弱点——指出谁先爆、为什么、爆了怎么传导。

    不是列出所有承压玩家，而是找到最可能的引爆者和传导链。
    """
    if not player_map:
        return

    # 按承压严重度排序的玩家 (health: crisis > stressed > healthy)
    _health_rank = {"crisis": 3, "stressed": 2, "healthy": 1}
    player_list = [
        ("私募信贷", player_map.private_credit),
        ("银行", player_map.banks),
        ("外国政府", player_map.foreign_governments),
        ("散户", player_map.retail),
    ]

    for vuln in vulns:
        loc = vuln.location.lower()

        # ── 找引爆者: 在这个脆弱点的传导链上，谁最脆弱 ──

        # 信贷/利率脆弱点 → 找信贷链上最弱的玩家
        if any(k in loc for k in ["利率", "信贷", "贷款", "利差"]):
            # 信贷链上的玩家优先级: 私募信贷(最脆弱) > 银行 > 散户
            chain_players = [
                ("私募信贷", player_map.private_credit,
                 "借了最多+质量最差(CCC级)+依赖银行续贷",
                 "违约→银行商业贷款坏账→银行收紧→信贷进一步收缩"),
                ("银行", player_map.banks,
                 "商业贷款敞口+地产敞口",
                 "惜贷→信贷收缩→企业+消费者都借不到钱"),
                ("散户", player_map.retail,
                 "保证金杠杆+消费贷",
                 "违约→消费下降→企业盈利→股市"),
            ]
        # 供应链/运输脆弱点 → 外部秩序相关玩家
        elif any(k in loc for k in ["运输", "进口", "供应链", "油价"]):
            chain_players = [
                ("外国政府", player_map.foreign_governments,
                 "地缘冲突+贸易壁垒+资本回撤",
                 "供应链断→生产成本↑→CPI↑→Fed被迫行动"),
            ]
        # 其他
        else:
            chain_players = [
                (name, pg, "系统性承压", "传导到其他环节")
                for name, pg in player_list
            ]

        # 在候选中找最脆弱的
        detonator = None
        for pname, pg, why, aftermath in chain_players:
            if pg.health != "healthy":
                detonator = (pname, pg, why, aftermath)
                break

        if detonator:
            pname, pg, why, aftermath = detonator
            vuln.mechanism += (
                f"\n     引爆者: {pname} [{pg.health}] — {pg.detail}"
                f"\n     为什么先爆: {why}"
                f"\n     爆后传导: {aftermath}"
            )


# ━━ 8. 格式化输出 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


_STAGE_CN = {
    "new_order": "第一阶段-新秩序",
    "building": "第二阶段-建设期",
    "prosperity": "第三阶段-繁荣期",
    "overextension": "第四阶段-过度扩张",
    "conflict": "第五阶段-内外冲突",
    "restructuring": "第六阶段-重组期",
}

_STATUS_CN = {
    "accelerating_decline": "加速恶化",
    "declining": "恶化中",
    "stable": "稳定",
    "improving": "改善中",
    "key_variable": "关键变量",
}


def format_simulation(result: SimulationResult) -> str:
    """人可读的预演输出。"""
    lines: list[str] = []
    e = result.empire_stage

    lines.append(f"=== 预演快照: {result.snapshot_date} ===")
    lines.append("")

    # 帝国周期
    stage_cn = _STAGE_CN.get(e.stage_name, e.stage_name)
    lines.append(f"帝国周期: {stage_cn} (信心{e.confidence:.2f})")
    lines.append(f"  终点: {e.destination}")
    lines.append(f"  关键变量: {e.key_variable}")
    if e.evidence:
        for ev in e.evidence:
            lines.append(f"  - {ev}")
    lines.append("")

    # 五力状态
    lines.append("五力状态:")
    for f in result.forces:
        status_cn = _STATUS_CN.get(f.status, f.status)
        lines.append(f"  {f.force_id.upper()} {f.force_name}: P{f.percentile:.0f} {status_cn}")
        lines.append(f"     角色: {f.role_in_empire}")
    lines.append("")

    # 货币政策冲突
    if result.monetary_conflict:
        mc = result.monetary_conflict
        lines.append(f"货币政策: 约束冲突 (严重度{mc.severity:.2f})")
        for c in mc.conflicting_mandates:
            lines.append(f"  - {c}")
        lines.append(f"  最可能牺牲: {mc.most_likely_sacrifice}")
    else:
        lines.append("货币政策: 约束尚可调和")
    lines.append("")

    # 玩家地图
    if result.player_map:
        pm = result.player_map
        _health_icon = {"healthy": "🟢", "stressed": "🟡", "crisis": "🔴"}
        lines.append("玩家地图:")
        for name, pg in [
            ("银行", pm.banks),
            ("央行", pm.central_banks),
            ("私募信贷", pm.private_credit),
            ("外国政府", pm.foreign_governments),
            ("散户", pm.retail),
        ]:
            icon = _health_icon.get(pg.health, "?")
            lines.append(f"  {icon} {name}: {pg.health} ({pg.trend}) — {pg.detail}")
        lines.append(f"  资金流向: {pm.capital_flow_direction}")
        if pm.stress_signals:
            for s in pm.stress_signals:
                lines.append(f"  ⚠ {s}")
        lines.append("")

    # 脆弱点
    if result.vulnerabilities:
        lines.append("脆弱点:")
        for i, v in enumerate(result.vulnerabilities, 1):
            sev_bar = "!" * max(1, int(v.severity * 5))
            lines.append(f"  {i}. [{sev_bar}] {v.location} (严重度{v.severity:.2f})")
            lines.append(f"     传导: {v.mechanism}")
            if v.empire_context:
                lines.append(f"     帝国语境: {v.empire_context}")
    else:
        lines.append("脆弱点: 暂无显著脆弱点")
    lines.append("")

    # 新闻上下文
    if result.news_context:
        _FORCE_CN = {
            "f1a": "短债务", "f1b": "长债务", "f2": "内部",
            "f3": "外部", "f4": "自然", "f5": "技术",
        }
        lines.append("近期相关事件:")
        for event in result.news_context[:5]:
            lines.append(f"  [{_FORCE_CN.get(event.force_id, '?')}] {event.headline}")
            if event.affected_sectors:
                lines.append(f"    影响: {', '.join(event.affected_sectors)}")
        lines.append("")

    # 关键问题
    lines.append(f"关键问题: {result.key_question}")
    lines.append("")

    # 下一步
    lines.append(f"下一步: {result.next_step}")

    return "\n".join(lines)


# ━━ 自测 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


if __name__ == "__main__":
    import json
    import os

    # 定位数据文件
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
    data_path = os.path.join(project_root, "tests", "data_fred_monthly_history.json")

    with open(data_path) as f:
        fred_history = json.load(f)

    test_months = [
        ("2007-06", "危机前的晚期扩张"),
        ("2008-10", "危机中的收缩"),
        ("2010-06", "复苏期"),
        ("2020-04", "COVID冲击"),
        ("2024-06", "当前状态"),
    ]

    for month, label in test_months:
        print(f"\n{'='*70}")
        print(f"测试: {month} ({label})")
        print(f"{'='*70}")
        result = simulate(fred_history, month)
        print(format_simulation(result))
        print()

    # ━━ Mock 新闻事件测试 ━━
    from anchor.collect.news_events import EconomicEvent

    mock_events = [
        EconomicEvent(
            headline="Trump宣布对中国商品加征25%关税",
            source="reuters", date="2026-03-15",
            force_id="f3", force_impact="negative",
            affected_sectors=["XLK", "XLI"],
            affected_entities=["AAPL", "TSMC"],
            transmission="关税→进口成本↑→电子制造业盈利↓",
            asset_implications={"equity": "negative", "gold": "positive"},
            confidence=0.9,
        ),
        EconomicEvent(
            headline="美联储维持利率不变，暗示年内可能降息",
            source="bloomberg", date="2026-03-19",
            force_id="f1a", force_impact="positive",
            affected_sectors=["XLF", "XLRE"],
            affected_entities=[],
            transmission="利率维持→房贷压力不加重→地产和银行受益",
            asset_implications={"equity": "positive", "bond": "positive"},
            confidence=0.85,
        ),
        EconomicEvent(
            headline="OpenAI发布GPT-5，企业采用率低于预期",
            source="wsj", date="2026-03-18",
            force_id="f5", force_impact="negative",
            affected_sectors=["XLK"],
            affected_entities=["MSFT", "NVDA"],
            transmission="AI采用率不及预期→投资回报存疑→科技股承压",
            asset_implications={"equity": "negative"},
            confidence=0.7,
        ),
    ]

    print(f"\n{'='*70}")
    print("测试: 2024-06 + Mock新闻事件")
    print(f"{'='*70}")
    result = simulate(fred_history, "2024-06", news_events=mock_events)
    print(format_simulation(result))
