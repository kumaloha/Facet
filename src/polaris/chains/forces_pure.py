"""
五大力量 Pure 模式评估
======================

只使用三类特征:
  1. 金融原理 (利率→偿债, 信贷→经济, 杠杆→脆弱)
  2. 经济常识 (失业→消费, 通胀→紧缩)
  3. 历史规律 (收益率曲线倒挂→衰退, 债务周期)

不使用:
  - 后验补丁 (影子银行背离, 贸易战咬合, VIX自满等)
  - 硬编码阈值 (GDP<-5, 基尼>40, 逾期率>4% 等)
  - 事后复盘的具体模式

所有判断基于: 百分位(在历史中的位置) + 趋势(方向和加速度)
"""

from __future__ import annotations

from anchor.compute.percentile_trend import (
    IndicatorAssessment,
    SignalTier,
    aggregate_force_direction,
    assess_from_fred_history,
)


# ── 指标定义: (fred_history_key, display_name, higher_is_worse) ──────────

# 金融原理: 利率传导、信贷周期、杠杆脆弱性
FORCE1_INDICATORS = [
    # 金融原理: 利率水平决定偿债成本
    ("fed_funds_rate", "基准利率", True),
    # 金融原理: 信贷扩张/收缩驱动经济周期
    ("credit_growth", "信贷增速", False),     # 低增速=收缩=差
    # 历史规律: 高债务/GDP = 经济脆弱
    ("total_debt_gdp", "总债务/GDP", True),
    # 经济常识: 失业上升 = 经济恶化
    ("unemployment", "失业率", True),
    # 经济常识: 通胀偏离 = 央行要行动
    ("cpi_yoy", "CPI同比", True),
    # 经济常识: GDP增速下降 = 经济放缓
    ("gdp_growth", "GDP增速", False),
    # 金融原理: 逾期率上升 = 信贷质量恶化
    ("mortgage_delinquency", "房贷逾期率", True),
    # 金融原理: 杠杆高 = 系统脆弱
    ("financial_leverage", "金融杠杆", True),
    # 金融原理: 银行收紧 = 信贷即将收缩
    ("lending_standards", "贷款标准收紧", True),
    # 金融原理: 家庭债务过高 = 消费脆弱
    ("household_debt_gdp", "家庭债务/GDP", True),
    # 金融原理: 偿债负担重 = 消费被挤压
    ("mortgage_debt_service", "房贷偿付比", True),
    # 金融原理: 高收益利差 = 信用风险定价
    ("credit_spread_hy", "高收益利差", True),
    # 金融原理: 消费贷增速过快 = 家庭在借钱消费，不可持续
    ("consumer_credit_growth", "消费贷增速", True),
    # 金融原理: 信用卡逾期率上升 = 消费者还不起钱了
    ("credit_card_delinquency", "信用卡逾期率", True),
]

# 经济常识: 社会稳定性影响政策可预测性
FORCE2_INDICATORS = [
    # 历史规律: 不平等加剧→政治极化→政策不确定
    ("gini", "基尼系数", True),
    # 经济常识: 赤字高 = 财政空间收窄或再分配加大
    ("fiscal_deficit_gdp", "财政赤字/GDP", True),
    # 经济常识: 失业上升 = 社会压力 + 消费能力下降
    ("unemployment", "失业率", True),
    # 经济常识: 非农就业增速下降 = 经济放缓的实数据
    ("nonfarm_payrolls_growth", "非农就业增速", False),
    # 经济常识: 零售下降 = 消费者实际在花的钱减少了（行为不是态度）
    ("retail_sales_growth", "零售销售增速", False),
    # 经济常识: 生产率涨但工资不涨 = 分配不公 → 不满积累
    ("wage_prod_gap", "工资-生产率缺口", True),
]

# 历史规律/经济常识: 全球秩序变化影响贸易和资本流动
FORCE3_INDICATORS = [
    # 经济常识: 贸易差额反映竞争力和需求结构
    ("trade_balance_abs", "贸易逆差", True),   # 逆差扩大=差
    # 金融原理: 美元走势影响全球资本流动
    ("dollar_yoy", "美元指数变化", None),       # 方向不确定，用绝对值检测波动
    # 经济常识: 油价飙升 = 供给冲击或地缘风险
    ("oil_yoy_abs", "油价波动幅度", True),      # 大幅波动=不稳定
    # 经济常识: 政策不确定性抑制投资
    ("epu_index", "政策不确定性", True),
]

# 自然冲击: 通过食品/能源价格传导
FORCE4_INDICATORS = [
    # 经济常识: 食品价格飙升 = 供给冲击
    ("food_cpi_yoy", "食品价格", True),
]

# 技术变革: 生产率是终极判据
FORCE5_INDICATORS = [
    # 经济常识: 生产率是长期增长的根本驱动力
    ("productivity_growth", "生产率增速", False),  # 高=好
    # 经济常识: R&D投入反映对未来技术的信心
    ("rd_spending_growth", "R&D支出增速", False),
]


# ── 数据预处理: 从原始fred_history提取回测所需的衍生序列 ──────────


def _build_derived_series(fred: dict) -> dict[str, dict[str, float]]:
    """从原始FRED序列构建衍生序列（YoY等），供百分位计算使用。"""
    derived = {}

    # 直接序列
    for key in ["fed_funds_rate", "unemployment", "total_debt_gdp",
                "mortgage_delinquency", "financial_leverage", "lending_standards",
                "household_debt_gdp", "mortgage_debt_service", "consumer_sentiment",
                "gini", "fiscal_deficit_gdp", "epu_index",
                "credit_spread_hy", "credit_spread_ig", "vix_daily"]:
        if key in fred:
            derived[key] = fred[key]
            # fiscal_deficit_gdp 取绝对值
            if key == "fiscal_deficit_gdp":
                derived[key] = {m: abs(v) for m, v in fred[key].items()}

    # YoY 衍生序列
    def _make_yoy(raw_series: dict[str, float]) -> dict[str, float]:
        yoy = {}
        for month, val in raw_series.items():
            year = int(month[:4])
            rest = month[4:]
            prev_month = f"{year - 1}{rest}"
            prev = raw_series.get(prev_month)
            if prev is not None and prev != 0:
                yoy[month] = ((val / prev) - 1) * 100
        return yoy

    if "cpi_index" in fred:
        derived["cpi_yoy"] = _make_yoy(fred["cpi_index"])
    if "credit_total" in fred:
        derived["credit_growth"] = _make_yoy(fred["credit_total"])
    if "food_cpi" in fred:
        derived["food_cpi_yoy"] = _make_yoy(fred["food_cpi"])
    if "nonfarm_productivity" in fred:
        derived["productivity_growth"] = _make_yoy(fred["nonfarm_productivity"])
    if "rd_spending" in fred:
        derived["rd_spending_growth"] = _make_yoy(fred["rd_spending"])
    if "nasdaq" in fred:
        derived["nasdaq_yoy"] = _make_yoy(fred["nasdaq"])

    # GDP增速直接用（已经是增速）
    if "gdp_growth" in fred:
        derived["gdp_growth"] = fred["gdp_growth"]

    # 贸易差额取绝对值（逆差越大越差）
    if "trade_balance" in fred:
        derived["trade_balance_abs"] = {
            m: abs(v) for m, v in fred["trade_balance"].items()
        }

    # 美元YoY
    if "dollar_index" in fred:
        derived["dollar_yoy"] = _make_yoy(fred["dollar_index"])

    # 油价YoY取绝对值（大幅波动=不稳定）
    if "wti_oil" in fred:
        oil_yoy = _make_yoy(fred["wti_oil"])
        derived["oil_yoy_abs"] = {m: abs(v) for m, v in oil_yoy.items()}

    # 消费贷总量YoY
    if "consumer_credit_total" in fred:
        derived["consumer_credit_growth"] = _make_yoy(fred["consumer_credit_total"])

    # 信用卡逾期率（直接用）
    if "credit_card_delinquency" in fred:
        derived["credit_card_delinquency"] = fred["credit_card_delinquency"]

    # 非农就业人数YoY
    if "nonfarm_payrolls" in fred:
        derived["nonfarm_payrolls_growth"] = _make_yoy(fred["nonfarm_payrolls"])

    # 零售销售YoY
    if "retail_sales" in fred:
        derived["retail_sales_growth"] = _make_yoy(fred["retail_sales"])

    # 工资-生产率缺口: 工资涨幅 - 生产率涨幅，正值=工资追上了，负值=工资落后
    # 取反: gap越大(工资越落后)越差 → higher_is_worse
    prod_yoy = derived.get("productivity_growth", {})
    # 用CPI近似实际工资变化（没有直接工资数据时）
    # 逻辑: 如果CPI涨5%但生产率涨2%，工人实际购买力在下降
    cpi_yoy = derived.get("cpi_yoy", {})
    if prod_yoy and cpi_yoy:
        gap = {}
        for m in prod_yoy:
            if m in cpi_yoy:
                # gap = 生产率增速 - CPI增速; 负值=生产率没跑赢通胀=工人受损
                # 取反让 higher=worse: 值越大=缺口越大=越差
                gap[m] = cpi_yoy[m] - prod_yoy[m]
        if gap:
            derived["wage_prod_gap"] = gap

    return derived


# ── Pure 模式评估 ──────────────────────────────────────────────────────


def assess_forces_pure(
    fred_history: dict,
    month: str,
) -> dict:
    """Pure模式五力评估: 只用百分位+趋势+金融原理/经济常识/历史规律。

    Args:
        fred_history: 原始FRED月度数据 {"indicator": {"YYYY-MM": value}}
        month: 当前回测月份 "YYYY-MM"

    Returns:
        {
            "force1": (direction, confidence, assessments),
            "force2": ...,
            "force3": ...,
            "force4": ...,
            "force5": ...,
        }
    """
    derived = _build_derived_series(fred_history)

    results = {}
    force_configs = [
        ("force1", FORCE1_INDICATORS),
        ("force2", FORCE2_INDICATORS),
        ("force3", FORCE3_INDICATORS),
        ("force4", FORCE4_INDICATORS),
        ("force5", FORCE5_INDICATORS),
    ]

    for force_key, indicators in force_configs:
        assessments = []
        for data_key, display_name, higher_is_worse in indicators:
            series = derived.get(data_key)
            if series is None:
                continue

            # 美元方向不确定，看波动幅度
            if higher_is_worse is None:
                # 取绝对值，大波动=差
                series = {m: abs(v) for m, v in series.items()}
                higher_is_worse = True

            a = assess_from_fred_history(
                display_name, month, series, higher_is_worse
            )
            if a.value is not None:
                assessments.append(a)

        direction, confidence = aggregate_force_direction(assessments)
        results[force_key] = (direction, confidence, assessments)

    return results


def pure_forces_to_directions(results: dict) -> dict[int, str]:
    """将 pure 评估结果转为 {force_id: direction_string}，兼容现有回测。"""
    mapping = {"force1": 1, "force2": 2, "force3": 3, "force4": 4, "force5": 5}
    return {
        mapping[k]: v[0].value
        for k, v in results.items()
        if k in mapping
    }
