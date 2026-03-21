"""
预演模块
========

从当前状态出发：
1. 识别周期位置（短周期+长周期）— 用百分位+趋势，不用硬编码阈值
2. 一步推演 — 因果图各节点按当前趋势走一步
3. 终点列举 — 历史上从当前位置出发的可能终局
4. 脆弱点 — 什么条件会让路径从好终点跳到坏终点
"""

from __future__ import annotations

from dataclasses import dataclass, field

from anchor.compute.percentile_trend import (
    IndicatorAssessment,
    TrendTier,
    assess_from_fred_history,
)
from polaris.chains.forces_pure import _build_derived_series


# ━━ 数据结构 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@dataclass
class CyclePosition:
    """周期位置"""
    short_cycle: str       # early_expansion / mid_expansion / late_expansion / early_contraction / mid_contraction / late_contraction
    short_confidence: float
    long_cycle: str        # early_leverage / mid_leverage / late_leverage / deleveraging
    long_confidence: float
    evidence: list[str] = field(default_factory=list)


@dataclass
class Endpoint:
    """可能的终点"""
    name: str
    description: str
    historical_probability: float
    asset_implications: dict[str, str]
    key_condition: str


@dataclass
class Vulnerability:
    """脆弱点"""
    location: str
    severity: float        # 0-1
    mechanism: str
    trigger: str


@dataclass
class NextStep:
    """一步推演结果"""
    most_likely: str
    direction: str         # "improving" / "deteriorating" / "stable"
    key_changes: list[str]
    approaching_endpoint: str


@dataclass
class SimulationResult:
    """完整预演结果"""
    cycle_position: CyclePosition
    endpoints: list[Endpoint]
    vulnerabilities: list[Vulnerability]
    next_step: NextStep
    snapshot_date: str


# ━━ 辅助: 取指标评估 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# 周期定位所需指标: (derived_key, display_name, higher_is_worse)
_CYCLE_INDICATORS = [
    ("credit_growth",    "信贷增速",    False),
    ("gdp_growth",       "GDP增速",     False),
    ("unemployment",     "失业率",      True),
    ("fed_funds_rate",   "基准利率",    True),
    ("total_debt_gdp",   "总债务/GDP",  True),
    ("cpi_yoy",          "CPI同比",     True),
    ("lending_standards", "贷款标准收紧", True),
]

# 脆弱点检测所需扩展指标
# 注意: 不含 financial_leverage（杠杆是放大器不是触发器，每次都亮没有信息量）
_VULN_INDICATORS = _CYCLE_INDICATORS + [
    ("credit_spread_hy",     "高收益利差",    True),
    ("mortgage_delinquency", "房贷逾期率",    True),
    ("household_debt_gdp",   "家庭债务/GDP",  True),
    ("credit_card_delinquency", "信用卡逾期率", True),
    ("consumer_credit_growth",  "消费贷增速",   True),
    ("mortgage_debt_service",   "房贷偿付比",   True),
    # 供应链指标
    ("cass_freight_yoy",     "货运量变化",    False),  # 下降=差
    ("transport_cpi_yoy",    "运输成本",      True),
    ("import_price_yoy",     "进口价格",      True),
]


def _assess_indicators(
    indicator_list: list[tuple[str, str, bool]],
    derived: dict,
    month: str,
) -> dict[str, IndicatorAssessment]:
    """批量评估指标，返回 {derived_key: assessment}。"""
    results: dict[str, IndicatorAssessment] = {}
    for data_key, display_name, higher_is_worse in indicator_list:
        series = derived.get(data_key)
        if series is None:
            continue
        a = assess_from_fred_history(display_name, month, series, higher_is_worse)
        if a.value is not None:
            results[data_key] = a
    return results


# ━━ 趋势判断辅助 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _is_improving(a: IndicatorAssessment) -> bool:
    """指标在改善？（考虑 higher_is_worse 已在 assess 阶段处理）"""
    return a.trend in (TrendTier.IMPROVING, TrendTier.ACCELERATING_IMPROVEMENT)


def _is_deteriorating(a: IndicatorAssessment) -> bool:
    """指标在恶化？"""
    return a.trend in (TrendTier.DETERIORATING, TrendTier.ACCELERATING_DETERIORATION)


def _is_accelerating_bad(a: IndicatorAssessment) -> bool:
    return a.trend == TrendTier.ACCELERATING_DETERIORATION


def _pct(a: IndicatorAssessment) -> float:
    """取百分位，None 时返回 50（中性）。"""
    return a.percentile if a.percentile is not None else 50.0


# ━━ 1. 周期位置识别 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def identify_cycle_position(fred_history: dict, month: str) -> CyclePosition:
    """
    百分位驱动的周期定位。

    短周期: 信贷+GDP+失业+利率的百分位和趋势 → 6 个阶段
    长周期: 债务/GDP 百分位 → 4 个阶段
    """
    derived = _build_derived_series(fred_history)
    indicators = _assess_indicators(_CYCLE_INDICATORS, derived, month)

    credit = indicators.get("credit_growth")
    gdp = indicators.get("gdp_growth")
    unemp = indicators.get("unemployment")
    rate = indicators.get("fed_funds_rate")
    debt = indicators.get("total_debt_gdp")
    cpi = indicators.get("cpi_yoy")
    lending = indicators.get("lending_standards")

    evidence: list[str] = []

    # ── 短周期判断 ──
    #
    # 核心逻辑 (百分位区间: <30=低, 30-50=中低, 50-70=中高, >70=高):
    # 先判断方向（扩张 vs 收缩），再判断阶段（早/中/晚）

    # 综合扩张/收缩信号计数
    expansion_signals = 0
    contraction_signals = 0

    if credit and _is_improving(credit):
        expansion_signals += 1
        evidence.append(f"信贷在改善(P{_pct(credit):.0f})")
    elif credit and _is_deteriorating(credit):
        contraction_signals += 1
        evidence.append(f"信贷在恶化(P{_pct(credit):.0f})")

    if gdp and _is_improving(gdp):
        expansion_signals += 1
        evidence.append(f"GDP在改善(P{_pct(gdp):.0f})")
    elif gdp and _is_deteriorating(gdp):
        contraction_signals += 1
        evidence.append(f"GDP在恶化(P{_pct(gdp):.0f})")

    if unemp and _is_improving(unemp):
        expansion_signals += 1
        evidence.append(f"失业在改善(P{_pct(unemp):.0f})")
    elif unemp and _is_deteriorating(unemp):
        contraction_signals += 1
        evidence.append(f"失业在恶化(P{_pct(unemp):.0f})")

    if lending and _is_improving(lending):
        expansion_signals += 1
        evidence.append(f"贷款标准在放松(P{_pct(lending):.0f})")
    elif lending and _is_deteriorating(lending):
        contraction_signals += 1
        evidence.append(f"贷款标准在收紧(P{_pct(lending):.0f})")

    # 判断大方向
    is_expansion = expansion_signals > contraction_signals

    # 判断阶段
    if is_expansion:
        # 晚期扩张: 失业百分位<30(很低) + 利率百分位>70(很高) → 过热
        unemp_pct = _pct(unemp) if unemp else 50
        rate_pct = _pct(rate) if rate else 50
        cpi_pct = _pct(cpi) if cpi else 50

        if unemp_pct < 30 and rate_pct > 70:
            short_cycle = "late_expansion"
            evidence.append("失业极低+利率极高→过热")
        elif unemp_pct < 30 or cpi_pct > 70:
            # 经济在走热但还没到顶
            short_cycle = "mid_expansion"
            evidence.append("经济走热中")
        else:
            short_cycle = "early_expansion"
            evidence.append("经济开始扩张")
    else:
        # 收缩阶段
        rate_improving = rate and _is_improving(rate)
        credit_improving = credit and _is_improving(credit)

        if rate_improving or credit_improving:
            # 利率在下降或信贷开始改善 → 接近见底
            short_cycle = "late_contraction"
            if rate_improving:
                evidence.append("利率在下降→接近见底")
            if credit_improving:
                evidence.append("信贷开始改善→接近见底")
        elif contraction_signals >= 3:
            short_cycle = "mid_contraction"
            evidence.append("多数指标同时恶化→收缩中期")
        else:
            short_cycle = "early_contraction"
            evidence.append("收缩信号出现但未全面扩散")

    # 短周期置信度: 信号一致性
    total_signals = expansion_signals + contraction_signals
    if total_signals > 0:
        dominant = max(expansion_signals, contraction_signals)
        short_confidence = round(dominant / max(total_signals, 1), 2)
    else:
        short_confidence = 0.3  # 无信号→低置信度

    # ── 长周期判断 ──
    #
    # 债务/GDP 百分位 → 杠杆阶段
    debt_pct = _pct(debt) if debt else 50
    rate_pct_val = _pct(rate) if rate else 50

    if debt_pct < 40:
        long_cycle = "early_leverage"
        long_confidence = 0.7
        evidence.append(f"债务/GDP百分位低(P{debt_pct:.0f})→早期杠杆")
    elif debt_pct < 70:
        long_cycle = "mid_leverage"
        long_confidence = 0.6
        evidence.append(f"债务/GDP百分位中(P{debt_pct:.0f})→中期杠杆")
    elif rate_pct_val < 30:
        # 高债务 + 低利率 → 去杠杆（央行在刺激）
        long_cycle = "deleveraging"
        long_confidence = 0.8
        evidence.append(f"高债务(P{debt_pct:.0f})+低利率(P{rate_pct_val:.0f})→去杠杆")
    else:
        long_cycle = "late_leverage"
        long_confidence = 0.7
        evidence.append(f"债务/GDP百分位高(P{debt_pct:.0f})→晚期杠杆")

    return CyclePosition(
        short_cycle=short_cycle,
        short_confidence=short_confidence,
        long_cycle=long_cycle,
        long_confidence=long_confidence,
        evidence=evidence,
    )


# ━━ 2. 脆弱点识别 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


# 正反馈环定义 (金融原理/经济常识级别)
# 每个环: (指标A, 指标B, 描述)
_FEEDBACK_LOOPS = [
    # 失业↑ → 消费↓ → 企业↓ → 失业更↑
    ("unemployment", "retail_sales_growth", "失业→消费萎缩→企业裁员→失业"),
    # 利差↑ → 融资难 → 违约↑ → 利差更↑
    ("credit_spread_hy", "mortgage_delinquency", "利差→融资成本→违约→利差"),
    # 资产价↓ → 抵押品↓ → 信贷缩 → 资产更↓
    ("financial_leverage", "credit_growth", "杠杆→抵押品→信贷收缩→资产下跌"),
    # 贷款标准收紧 → 信贷收缩 → 经济恶化 → 标准更紧
    ("lending_standards", "credit_growth", "贷款标准收紧→信贷缩→经济差→更收紧"),
]

# 背离检测: (指标A, 指标B, 条件描述)
# A在改善但B在恶化，说明有隐藏风险
_DIVERGENCES = [
    # GDP好但信贷在收缩 → 信贷领先GDP
    ("gdp_growth", "credit_growth", "GDP尚好但信贷已在收缩(信贷领先GDP)"),
    # 失业低但贷款标准在收紧 → 银行看到了消费者没看到的
    ("unemployment", "lending_standards", "失业尚低但银行在收紧贷款(银行看到隐藏风险)"),
]


def identify_vulnerabilities(
    fred_history: dict,
    month: str,
    cycle_position: CyclePosition | None = None,
) -> list[Vulnerability]:
    """
    两层脆弱点识别:

    第一层（事前，常态）: 系统的结构性弱点
      "如果遭到冲击，系统会在哪里断裂？"
      = 百分位高位（张力大）+ 在当前周期位置是关键指标 + 下游连接多

      弱点随周期位置变化（大周期先验经验）:
      - 晚期扩张: 信贷质量+利率（借了太多+加息打到偿债）
      - 早期收缩: 就业+消费（失业→消费→更多失业的正反馈）
      - 晚期杠杆: 任何外部冲击都被杠杆放大→看信贷+利差
      - 去杠杆: 政策反应（印钱vs紧缩）→看利率+货币供应

    第二层（事后，冲击发生时）: 突变打到了哪个弱点
      "刚才发生了什么？打到了哪？"
      = 百分位短期跳变 → 匹配到传导链

    最多输出2个。
    """
    from anchor.compute.percentile_trend import SignalTier, compute_percentile

    derived = _build_derived_series(fred_history)
    indicators = _assess_indicators(_VULN_INDICATORS, derived, month)

    # 追加零售销售
    for key, name, hiw in [("retail_sales_growth", "零售销售增速", False)]:
        series = derived.get(key)
        if series is not None:
            a = assess_from_fred_history(name, month, series, hiw)
            if a.value is not None:
                indicators[key] = a

    # ── 因果传导链（金融原理/经济常识）──
    _CAUSAL_CHAIN = [
        ("lending_standards", "credit_growth", "银行收紧→信贷收缩"),
        ("credit_growth", "gdp_growth", "信贷收缩→经济放缓"),
        ("credit_growth", "unemployment", "信贷收缩→企业裁员"),
        ("fed_funds_rate", "mortgage_delinquency", "利率升→还款压力→逾期"),
        ("fed_funds_rate", "credit_spread_hy", "利率升→融资成本→信用风险"),
        ("unemployment", "retail_sales_growth", "失业→消费下降"),
        ("consumer_credit_growth", "credit_card_delinquency", "过度借贷→还不起"),
        # 供应链传导
        ("cass_freight_yoy", "gdp_growth", "货运中断→生产停滞→GDP下降"),
        ("cass_freight_yoy", "unemployment", "供应链断→企业停工→失业"),
        ("import_price_yoy", "cpi_yoy", "进口成本飙升→通胀"),
        ("transport_cpi_yoy", "import_price_yoy", "运输成本升→进口成本升"),
    ]

    # 计算每个指标的下游连接数（影响面）
    downstream_count = {}
    for upstream, downstream, _ in _CAUSAL_CHAIN:
        downstream_count[upstream] = downstream_count.get(upstream, 0) + 1

    # ══════════════════════════════════════════
    # 大周期先验: 不同周期位置，不同指标是关键弱点
    # 这是达利欧从几百年历史中总结的规律
    # ══════════════════════════════════════════
    # 周期位置 → 哪些指标在这个阶段特别危险（权重加成）
    _CYCLE_VULNERABILITY_WEIGHTS: dict[str, dict[str, float]] = {
        # 晚期扩张: 借了太多，加息打到偿债 — 信贷质量是命门
        "late_expansion": {
            "mortgage_delinquency": 2.0,     # 信贷质量开始恶化
            "credit_card_delinquency": 2.0,  # 消费端信贷质量
            "fed_funds_rate": 1.8,           # 加息→偿债
            "lending_standards": 1.5,        # 银行感知到风险
            "consumer_credit_growth": 1.5,   # 过度借贷
        },
        # 早期收缩: 失业→消费→更多失业的正反馈 — 就业是命门
        "early_contraction": {
            "unemployment": 2.0,             # 失业正反馈的核心
            "retail_sales_growth": 1.8,      # 消费崩塌
            "credit_growth": 1.5,            # 信贷是否还在收缩
            "cass_freight_yoy": 1.5,         # 实体活动
        },
        # 中期收缩: 看能不能见底 — 信贷和政策是关键
        "mid_contraction": {
            "credit_growth": 2.0,            # 信贷能不能企稳
            "lending_standards": 1.8,        # 银行还在收紧吗
            "fed_funds_rate": 1.5,           # 政策空间
            "credit_spread_hy": 1.5,         # 市场定价的信用风险
        },
        # 晚期杠杆: 杠杆放大一切 — 任何冲击都危险，看信贷+利差
        "late_leverage": {
            "credit_spread_hy": 2.0,         # 信用风险是炸药
            "fed_funds_rate": 1.8,           # 利率是引信
            "lending_standards": 1.8,        # 银行行为
            "mortgage_delinquency": 1.5,     # 信贷质量
        },
        # 去杠杆: 政策选择决定良性vs恶性
        "deleveraging": {
            "fed_funds_rate": 2.0,           # 央行反应
            "credit_growth": 1.8,            # 信贷能不能重启
            "credit_spread_hy": 1.5,         # 市场信心
            "unemployment": 1.5,             # 社会承受力
        },
    }

    # 获取当前周期位置对应的权重加成
    cycle_weights = {}
    if cycle_position:
        cycle_weights.update(_CYCLE_VULNERABILITY_WEIGHTS.get(cycle_position.short_cycle, {}))
        cycle_weights.update(_CYCLE_VULNERABILITY_WEIGHTS.get(cycle_position.long_cycle, {}))

    # ══════════════════════════════════════════
    # 第一层: 结构性弱点（事前）
    # 高百分位（张力大）+ 在当前周期是关键 + 下游多（断了影响大）
    # ══════════════════════════════════════════
    structural_vulns = []
    for key, a in indicators.items():
        pct = _pct(a)
        connections = downstream_count.get(key, 0)
        if connections == 0:
            continue  # 没有下游连接的不算弱点（是末端节点）

        # 高张力: 百分位>65（有压力但不一定在恶化）
        if pct > 65:
            # 找下游传导链
            chain_desc = []
            for up, down, mech in _CAUSAL_CHAIN:
                if up == key:
                    chain_desc.append(mech)

            # 基础分 = 百分位 × 连接度
            base_severity = (pct / 100) * (0.5 + connections * 0.25)
            # 周期加成: 当前周期位置下这个指标更危险
            cycle_boost = cycle_weights.get(key, 1.0)
            severity = min(base_severity * cycle_boost, 1.0)

            cycle_note = ""
            if cycle_boost > 1.0 and cycle_position:
                short_cn = _CYCLE_CN.get(cycle_position.short_cycle, cycle_position.short_cycle)
                long_cn = _CYCLE_CN.get(cycle_position.long_cycle, cycle_position.long_cycle)
                cycle_note = f" [{short_cn}/{long_cn}阶段关键指标]"

            structural_vulns.append(Vulnerability(
                location=a.name,
                severity=round(severity, 2),
                mechanism=f"结构性弱点(P{pct:.0f}){cycle_note}: 若受冲击 → {' → '.join(chain_desc)}",
                trigger="任何打到此节点的冲击将沿因果链传导",
            ))

    structural_vulns.sort(key=lambda v: v.severity, reverse=True)

    # ══════════════════════════════════════════
    # 第二层: 突变检测（事后）
    # 百分位3个月内跳变>30 = "刚发生了什么"
    # 关键: 突变打到了哪个结构性弱点？
    # ══════════════════════════════════════════
    # 构建 higher_is_worse 查找表
    _hiw_map = {k: hiw for k, _, hiw in _VULN_INDICATORS}
    _hiw_map["retail_sales_growth"] = False

    sudden_vuln = None
    max_jump = 0
    for key, a in indicators.items():
        pct_now = _pct(a)
        series = derived.get(key)
        if series is None:
            continue

        # 取3个月前的百分位
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
        pct_3ago = compute_percentile(hist_3ago[-1], hist_3ago[:-1] if len(hist_3ago) > 1 else [])

        jump = abs(pct_now - pct_3ago)

        # 只报恶化方向的突变（改善不算脆弱点）
        hiw = _hiw_map.get(key, True)
        if hiw:
            is_worsening = pct_now > pct_3ago  # higher_is_worse: 百分位升=恶化
        else:
            is_worsening = pct_now < pct_3ago  # lower_is_worse: 百分位降=恶化

        if not is_worsening:
            continue

        if jump > max_jump and jump > 30:
            max_jump = jump

            # 这个突变打到了哪些弱点？
            hit_chains = []
            for up, down, mech in _CAUSAL_CHAIN:
                if up == key:
                    hit_chains.append(mech)

            if hit_chains:
                mechanism = f"突变(P{pct_3ago:.0f}→P{pct_now:.0f}) 打到传导链: {' → '.join(hit_chains)}"
            else:
                mechanism = f"突变(P{pct_3ago:.0f}→P{pct_now:.0f})"

            sudden_vuln = Vulnerability(
                location=a.name,
                severity=round(jump / 100, 2),
                mechanism=mechanism,
                trigger="冲击已发生，关注下游传导",
            )

    # ══════════════════════════════════════════
    # 输出: 最多2个
    # ══════════════════════════════════════════
    vulns = []

    # 有突变 → 最高优先（事后立刻反应）
    if sudden_vuln:
        vulns.append(sudden_vuln)

    # 结构性弱点 → 事前预警
    for sv in structural_vulns:
        if sv.location not in {v.location for v in vulns}:
            vulns.append(sv)
            break

    return vulns[:2]


# ━━ 3. 终点列举 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


# 短周期终点映射 (基于200年经济史的历史先验)
_SHORT_CYCLE_ENDPOINTS: dict[str, list[tuple[str, str, float, dict[str, str], str]]] = {
    "early_expansion": [
        ("continued_expansion", "扩张延续，经济动能增强",
         0.7, {"equity": "overweight", "bond": "underweight", "gold": "neutral"},
         "信贷持续扩张+就业改善"),
        ("double_dip", "二次探底，复苏夭折",
         0.3, {"equity": "underweight", "bond": "overweight", "gold": "overweight"},
         "外部冲击或政策过早收紧"),
    ],
    "mid_expansion": [
        ("late_expansion", "进入晚期扩张/过热",
         0.6, {"equity": "neutral", "bond": "underweight", "gold": "neutral"},
         "信贷和资产价格继续膨胀"),
        ("soft_landing", "温和放缓，软着陆",
         0.3, {"equity": "neutral", "bond": "neutral", "gold": "neutral"},
         "央行成功引导预期+信贷有序减速"),
        ("overheating", "经济过热+通胀失控",
         0.1, {"equity": "underweight", "bond": "underweight", "gold": "overweight"},
         "通胀预期脱锚+工资-物价螺旋"),
    ],
    "late_expansion": [
        ("soft_landing", "软着陆",
         0.4, {"equity": "neutral", "bond": "neutral", "gold": "neutral"},
         "央行精准调控+信贷有序收缩"),
        ("recession", "衰退",
         0.4, {"equity": "underweight", "bond": "overweight", "gold": "overweight"},
         "紧缩过度或外部冲击"),
        ("stagflation", "滞胀",
         0.2, {"equity": "underweight", "bond": "underweight", "gold": "overweight"},
         "供给冲击+通胀顽固+经济放缓"),
    ],
    "early_contraction": [
        ("mild_recession", "温和衰退",
         0.5, {"equity": "underweight", "bond": "overweight", "gold": "overweight"},
         "政策及时响应+去杠杆有序"),
        ("deep_recession", "深度衰退",
         0.3, {"equity": "strongly_underweight", "bond": "overweight", "gold": "strongly_overweight"},
         "系统性风险暴露+信贷崩溃"),
        ("recovery", "快速复苏(V型)",
         0.2, {"equity": "overweight", "bond": "neutral", "gold": "neutral"},
         "冲击短暂+基本面健康"),
    ],
    "mid_contraction": [
        ("bottoming", "触底企稳",
         0.5, {"equity": "neutral", "bond": "overweight", "gold": "neutral"},
         "信贷企稳+政策效果显现"),
        ("depression", "萧条",
         0.2, {"equity": "strongly_underweight", "bond": "overweight", "gold": "strongly_overweight"},
         "正反馈环失控+政策无效"),
        ("policy_rescue", "政策强力救市",
         0.3, {"equity": "overweight", "bond": "neutral", "gold": "overweight"},
         "大规模财政/货币刺激"),
    ],
    "late_contraction": [
        ("recovery", "经济复苏",
         0.7, {"equity": "overweight", "bond": "underweight", "gold": "neutral"},
         "信贷重新扩张+就业回升"),
        ("stagnation", "长期停滞",
         0.3, {"equity": "underweight", "bond": "overweight", "gold": "overweight"},
         "结构性问题未解+人口/生产率拖累"),
    ],
}

# 长周期终点修正
_LONG_CYCLE_EXTRA_ENDPOINTS: dict[str, list[tuple[str, str, float, dict[str, str], str]]] = {
    "late_leverage": [
        ("deleveraging_ahead", "去杠杆即将到来",
         0.4, {"equity": "underweight", "bond": "neutral", "gold": "overweight"},
         "债务不可持续+触发事件出现"),
    ],
    "deleveraging": [
        ("beautiful_deleveraging", "良性去杠杆(印钱+通胀稀释)",
         0.5, {"equity": "overweight", "bond": "neutral", "gold": "overweight"},
         "央行印钱速度匹配债务销毁速度"),
        ("ugly_deleveraging", "恶性去杠杆(违约+紧缩)",
         0.5, {"equity": "strongly_underweight", "bond": "uncertain", "gold": "strongly_overweight"},
         "紧缩政策+违约级联"),
    ],
}


def list_endpoints(position: CyclePosition) -> list[Endpoint]:
    """从周期位置推导可能的终点。历史规律驱动，非硬编码场景。"""
    endpoints: list[Endpoint] = []

    # 短周期终点
    short_eps = _SHORT_CYCLE_ENDPOINTS.get(position.short_cycle, [])
    for name, desc, prob, assets, cond in short_eps:
        endpoints.append(Endpoint(
            name=name,
            description=desc,
            historical_probability=prob,
            asset_implications=assets,
            key_condition=cond,
        ))

    # 长周期修正
    long_eps = _LONG_CYCLE_EXTRA_ENDPOINTS.get(position.long_cycle, [])
    for name, desc, prob, assets, cond in long_eps:
        # 长周期终点的概率要乘以一个衰减因子，不与短周期概率竞争
        endpoints.append(Endpoint(
            name=name,
            description=desc,
            historical_probability=round(prob * 0.5, 2),  # 长周期权重较低
            asset_implications=assets,
            key_condition=cond,
        ))

    # 按概率降序
    endpoints.sort(key=lambda e: e.historical_probability, reverse=True)
    return endpoints


# ━━ 4. 一步推演 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def simulate_one_step(
    fred_history: dict,
    month: str,
    position: CyclePosition,
) -> NextStep:
    """
    看各关键指标的趋势方向，判断"下一步"整体在改善还是恶化。
    加速的给更大权重。
    """
    derived = _build_derived_series(fred_history)
    indicators = _assess_indicators(_CYCLE_INDICATORS, derived, month)

    improving_weight = 0.0
    deteriorating_weight = 0.0
    key_changes: list[str] = []

    for key, a in indicators.items():
        if _is_improving(a):
            w = 1.5 if a.trend == TrendTier.ACCELERATING_IMPROVEMENT else 1.0
            improving_weight += w
            key_changes.append(f"{a.name}在改善{'(加速)' if w > 1 else ''}")
        elif _is_deteriorating(a):
            w = 1.5 if a.trend == TrendTier.ACCELERATING_DETERIORATION else 1.0
            deteriorating_weight += w
            key_changes.append(f"{a.name}在恶化{'(加速)' if w > 1 else ''}")

    # 判断整体方向
    net = improving_weight - deteriorating_weight
    if net > 1.0:
        direction = "improving"
    elif net < -1.0:
        direction = "deteriorating"
    else:
        direction = "stable"

    # 推测最接近的终点
    endpoints = list_endpoints(position)
    if not endpoints:
        approaching = "unknown"
        most_likely = "数据不足以判断下一步"
    else:
        # 根据方向选择最可能接近的终点
        if direction == "deteriorating":
            # 选负面终点中概率最高的
            negative_names = {"recession", "deep_recession", "depression",
                              "stagflation", "ugly_deleveraging", "double_dip",
                              "mild_recession", "deleveraging_ahead"}
            neg_eps = [e for e in endpoints if e.name in negative_names]
            target = neg_eps[0] if neg_eps else endpoints[0]
        elif direction == "improving":
            # 选正面终点中概率最高的
            positive_names = {"continued_expansion", "recovery", "soft_landing",
                              "beautiful_deleveraging", "late_expansion",
                              "bottoming", "policy_rescue"}
            pos_eps = [e for e in endpoints if e.name in positive_names]
            target = pos_eps[0] if pos_eps else endpoints[0]
        else:
            target = endpoints[0]  # 概率最高的

        approaching = target.name
        most_likely = f"整体在{'改善' if direction == 'improving' else '恶化' if direction == 'deteriorating' else '横盘'}中，接近「{target.description}」"

    return NextStep(
        most_likely=most_likely,
        direction=direction,
        key_changes=key_changes,
        approaching_endpoint=approaching,
    )


# ━━ 5. 统一入口 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def simulate(fred_history: dict, month: str) -> SimulationResult:
    """完整预演: 周期定位 → 终点 → 脆弱点(周期感知) → 下一步"""
    position = identify_cycle_position(fred_history, month)
    endpoints = list_endpoints(position)
    vulnerabilities = identify_vulnerabilities(fred_history, month, cycle_position=position)
    next_step = simulate_one_step(fred_history, month, position)
    return SimulationResult(
        cycle_position=position,
        endpoints=endpoints,
        vulnerabilities=vulnerabilities,
        next_step=next_step,
        snapshot_date=month,
    )


# ━━ 6. 格式化输出 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


_CYCLE_CN = {
    "early_expansion": "早期扩张",
    "mid_expansion": "中期扩张",
    "late_expansion": "晚期扩张",
    "early_contraction": "早期收缩",
    "mid_contraction": "中期收缩",
    "late_contraction": "晚期收缩",
    "early_leverage": "早期杠杆",
    "mid_leverage": "中期杠杆",
    "late_leverage": "晚期杠杆",
    "deleveraging": "去杠杆",
}

_DIRECTION_ARROW = {
    "overweight": "++",
    "strongly_overweight": "+++",
    "underweight": "--",
    "strongly_underweight": "---",
    "neutral": "~",
    "uncertain": "?",
}


def format_simulation(result: SimulationResult) -> str:
    """人可读的预演输出。"""
    lines: list[str] = []
    pos = result.cycle_position

    lines.append(f"=== 预演快照: {result.snapshot_date} ===")
    lines.append("")

    # 周期位置
    short_cn = _CYCLE_CN.get(pos.short_cycle, pos.short_cycle)
    long_cn = _CYCLE_CN.get(pos.long_cycle, pos.long_cycle)
    lines.append(f"周期位置: 短周期={short_cn}(信心{pos.short_confidence:.2f}), "
                 f"长周期={long_cn}(信心{pos.long_confidence:.2f})")
    if pos.evidence:
        for e in pos.evidence:
            lines.append(f"  - {e}")
    lines.append("")

    # 可能终点
    lines.append("可能终点:")
    for i, ep in enumerate(result.endpoints, 1):
        assets_str = ", ".join(
            f"{k}{_DIRECTION_ARROW.get(v, v)}"
            for k, v in ep.asset_implications.items()
        )
        lines.append(f"  {i}. {ep.description} ({ep.historical_probability:.0%}) "
                      f"— {assets_str}")
        lines.append(f"     关键条件: {ep.key_condition}")
    lines.append("")

    # 脆弱点
    if result.vulnerabilities:
        lines.append("脆弱点:")
        for v in result.vulnerabilities:
            sev_bar = "!" * max(1, int(v.severity * 5))
            lines.append(f"  [{sev_bar}] {v.location} (严重度{v.severity:.2f})")
            lines.append(f"      机制: {v.mechanism}")
            lines.append(f"      触发: {v.trigger}")
    else:
        lines.append("脆弱点: 暂无显著脆弱点")
    lines.append("")

    # 下一步
    ns = result.next_step
    dir_cn = {"improving": "改善中", "deteriorating": "恶化中", "stable": "横盘中"}
    lines.append(f"下一步: 整体{dir_cn.get(ns.direction, ns.direction)}，"
                 f"接近「{ns.approaching_endpoint}」")
    lines.append(f"  {ns.most_likely}")
    if ns.key_changes:
        lines.append("  关键变化:")
        for c in ns.key_changes:
            lines.append(f"    - {c}")

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
