"""
五大力量 → 因果图推演引擎
===========================

四个核心能力:
  1. 主要矛盾判断: 变化速度 × 因果传导面 → 哪个力量在主导
  2. 场景推演: 冲击向量 + 政策响应 → 不同路径的资产结果
  3. 大周期约束: 长周期位置 → 排除不可能的路径 (先验经验)
  4. Watchlist: 每个场景的门槛事件 → 判断走了哪条路

不依赖 LLM，纯因果图数学 + 达利欧先验经验。
"""

from __future__ import annotations

import copy
import math
from dataclasses import dataclass, field
from enum import Enum

from polaris.chains.dalio import (
    ASSET_CAUSAL_MAP,
    MacroContext,
    MechanismNode,
    CausalGraphResult,
    _propagate_causal_graph,
    _compute_asset_impacts,
    _weighted_tanh,
    COUNTRY_PROFILES,
    CountryProfile,
)
from polaris.chains.dalio_forces import (
    FiveForcesView,
    ForceAssessment,
    ForceDirection,
)


# ══════════════════════════════════════════════════════════════
#  时间尺度 — 每个信号对应哪层持仓
# ══════════════════════════════════════════════════════════════


class TimeHorizon(str, Enum):
    """信号的时间尺度 → 对应 Axion 的持仓层。"""
    LONG = "long"       # 5年+ → All Weather 结构性倾斜 (大周期/巴菲特)
    MEDIUM = "medium"   # 1-2年 → Pure Alpha 主动押注 (五大力主要矛盾/索罗斯)
    SHORT = "short"     # <3月 → 不动 (仅格局改变监控)


class PortfolioLayer(str, Enum):
    """信号对接的持仓层。"""
    ALL_WEATHER = "all_weather"    # 底仓结构性倾斜, 极低频调整
    PURE_ALPHA = "pure_alpha"     # 主动押注, 跟着主要矛盾走
    BUFFETT = "buffett"           # 个股长持, 护城河框架
    SOROS = "soros"               # 反身性跟骑/狙击, 中期
    NONE = "none"                 # 不做交易 (短期噪音)


@dataclass
class TimeTaggedSignal:
    """带时间标签的信号 — 告诉 Axion 这个信号该影响哪层持仓。"""
    horizon: TimeHorizon
    layer: PortfolioLayer        # 对接哪个持仓层
    source: str                  # 来源: "big_cycle" / "force_scenario" / "soros" / "buffett"
    signal: str                  # 信号描述
    asset_implication: str       # 资产含义
    certainty: float = 0.0      # 确定性 0-1 (长期通常更高)
    rebalance_freq: str = ""    # 建议调仓频率


# 大周期位置 → 长期信号
# 这些是高确定性的长期趋势, 不随短期波动改变
LONG_CYCLE_SIGNALS: dict[str, list[dict]] = {
    "debt_late": {  # 长债务周期晚期 (债务/GDP > 100%)
        "signals": [
            {
                "signal": "政府必然选择印钱稀释债务(紧缩政治不可能, 违约不敢)",
                "asset": "All Weather 增配黄金/TIPS权重 — 对抗货币贬值",
                "layer": "all_weather",
                "rebalance": "年度审视, 大周期位置不变则不动",
                "certainty": 0.8,
            },
            {
                "signal": "实际利率长期被压低(金融抑制)",
                "asset": "All Weather 减配长期名义债权重 — 实际回报受压",
                "layer": "all_weather",
                "rebalance": "年度审视",
                "certainty": 0.7,
            },
            {
                "signal": "周期性财政刺激不可避免(每次衰退都会更大)",
                "asset": "All Weather 通胀对冲占比应高于教科书建议",
                "layer": "all_weather",
                "rebalance": "年度审视",
                "certainty": 0.7,
            },
        ],
    },
    "empire_declining": {  # 帝国衰退期 (内部秩序恶化 + 外部挑战者崛起)
        "signals": [
            {
                "signal": "储备货币地位缓慢侵蚀 — 全球央行增持黄金减持美债",
                "asset": "All Weather 分散货币敞口 — 黄金/非美资产",
                "layer": "all_weather",
                "rebalance": "年度审视",
                "certainty": 0.5,
            },
            {
                "signal": "内部再分配压力持续 — 资本利得税/企业税上行风险",
                "asset": "巴菲特选股时加权税收政策风险",
                "layer": "buffett",
                "rebalance": "政策变化时",
                "certainty": 0.4,
            },
        ],
    },
    "tech_revolution": {  # 技术革命期 (生产率趋势线改变)
        "signals": [
            {
                "signal": "生产率长期中枢上移 — 所有基于旧趋势线的估值要重校",
                "asset": "巴菲特框架: 长持生产率受益行业的护城河公司",
                "layer": "buffett",
                "rebalance": "生产率趋势确认后不动",
                "certainty": 0.6,
            },
            {
                "signal": "技术替代就业的摩擦期 — F2(内部秩序)阵痛",
                "asset": "Pure Alpha 中期: 注意民粹政策对科技的反噬风险",
                "layer": "pure_alpha",
                "rebalance": "季度审视",
                "certainty": 0.5,
            },
        ],
    },
}


def identify_long_cycle_position(macro_data: dict) -> list[str]:
    """根据宏观数据判断当前在大周期的什么位置。

    返回活跃的长周期状态列表。
    """
    positions = []
    debt_gdp = macro_data.get("total_debt_to_gdp", 0)
    gini = macro_data.get("gini_coefficient", 0)
    productivity = macro_data.get("productivity_growth", 0)

    if debt_gdp > 100:
        positions.append("debt_late")
    if gini > 40 or macro_data.get("epu_index", 0) > 200:
        positions.append("empire_declining")
    if productivity > 2.0:
        positions.append("tech_revolution")

    return positions


def generate_time_tagged_signals(
    macro_data: dict,
    analysis: "ContradictionAnalysis",
    sim: "SimulationResult | None" = None,
) -> list[TimeTaggedSignal]:
    """生成带时间标签的信号列表。

    长期: 大周期位置 → 战略底仓
    中期: 主要矛盾 + 场景推演 → 战术调仓
    短期: (几乎没有 — 系统不做短期预测)
    """
    signals: list[TimeTaggedSignal] = []

    # ── 长期信号: 大周期位置 → All Weather / Buffett ──
    positions = identify_long_cycle_position(macro_data)
    for pos in positions:
        config = LONG_CYCLE_SIGNALS.get(pos, {})
        for s in config.get("signals", []):
            layer_str = s.get("layer", "all_weather")
            layer = {
                "all_weather": PortfolioLayer.ALL_WEATHER,
                "buffett": PortfolioLayer.BUFFETT,
                "pure_alpha": PortfolioLayer.PURE_ALPHA,
            }.get(layer_str, PortfolioLayer.ALL_WEATHER)
            signals.append(TimeTaggedSignal(
                horizon=TimeHorizon.LONG,
                layer=layer,
                source="big_cycle",
                signal=s["signal"],
                asset_implication=s["asset"],
                certainty=s["certainty"],
                rebalance_freq=s.get("rebalance", "年度审视"),
            ))

    # ── 中期信号: 主要矛盾 + 场景 → Pure Alpha ──
    if analysis.principal:
        p = analysis.principal
        signals.append(TimeTaggedSignal(
            horizon=TimeHorizon.MEDIUM,
            layer=PortfolioLayer.PURE_ALPHA,
            source="force_scenario",
            signal=f"主要矛盾: F{p.force_id}({p.force_name})",
            asset_implication=f"Pure Alpha 关注 {', '.join(p.primary_channels[:3])} 传导链",
            certainty=min(p.score * 3, 0.7),
            rebalance_freq="季度审视, 主要矛盾切换时调仓",
        ))

    if analysis.tension:
        signals.append(TimeTaggedSignal(
            horizon=TimeHorizon.MEDIUM,
            layer=PortfolioLayer.PURE_ALPHA,
            source="force_scenario",
            signal=f"力量对抗: {analysis.tension}",
            asset_implication="Pure Alpha 减小头寸/分散 — 对抗未分胜负",
            certainty=0.3,
            rebalance_freq="月度监控对抗结果",
        ))

    # 场景推演 → Pure Alpha 方向性押注
    if sim:
        for sr in sim.scenarios[:3]:
            if sr.scenario.regime_changing:
                continue
            for ai in sr.asset_impacts[:2]:
                signals.append(TimeTaggedSignal(
                    horizon=TimeHorizon.MEDIUM,
                    layer=PortfolioLayer.PURE_ALPHA,
                    source="force_scenario",
                    signal=f"场景'{sr.scenario.name}': {ai.asset_type} {ai.direction}",
                    asset_implication=f"Pure Alpha 分数 {ai.raw_score:+.3f}",
                    certainty=0.3,
                    rebalance_freq="watchlist 触发时",
                ))

    # 索罗斯: 如果 F5 是主要矛盾且 NASDAQ 涨幅大 → 泡沫跟骑信号
    nasdaq = macro_data.get("nasdaq_yoy", 0)
    if analysis.principal and analysis.principal.force_id == 5 and nasdaq > 25:
        signals.append(TimeTaggedSignal(
            horizon=TimeHorizon.MEDIUM,
            layer=PortfolioLayer.SOROS,
            source="soros_reflexivity",
            signal=f"F5 主导 + NASDAQ +{nasdaq:.0f}% — 索罗斯泡沫跟骑阶段",
            asset_implication="跟骑科技趋势, 但准备狙击. 监控反身性指标",
            certainty=0.4,
            rebalance_freq="月度审视反身性阶段",
        ))

    # ── 短期: 只监控格局改变 → 不做交易 ──
    if sim:
        for sr in sim.scenarios:
            if sr.scenario.regime_changing and sr.scenario.watchlist:
                signals.append(TimeTaggedSignal(
                    horizon=TimeHorizon.SHORT,
                    layer=PortfolioLayer.NONE,
                    source="regime_change",
                    signal=f"监控格局改变: {sr.scenario.name}",
                    asset_implication="不做交易 — 若触发则所有层重新评估",
                    certainty=0.1,
                ))

    return signals


def format_time_tagged_signals(signals: list[TimeTaggedSignal]) -> str:
    """格式化带时间标签的信号。"""
    lines = [""]
    lines.append("  Polaris → Axion 信号")
    lines.append("  ════════════════════════════════════════════════")

    layer_icons = {
        PortfolioLayer.ALL_WEATHER: "AW",
        PortfolioLayer.PURE_ALPHA: "PA",
        PortfolioLayer.BUFFETT: "BF",
        PortfolioLayer.SOROS: "SR",
        PortfolioLayer.NONE: "--",
    }

    horizon_labels = {
        TimeHorizon.LONG: (
            "长期 (5年+) → All Weather 结构性倾斜",
            "大周期位置驱动. 极低频调整(年度审视, 十年可能只动1-2次)"
        ),
        TimeHorizon.MEDIUM: (
            "中期 (1-2年) → Pure Alpha 主动押注 / 索罗斯跟骑",
            "主要矛盾驱动. 季度审视, watchlist触发调仓"
        ),
        TimeHorizon.SHORT: (
            "短期 (<3月) → 不做交易",
            "系统承认短期不可预测. 仅监控格局改变事件"
        ),
    }

    for horizon in [TimeHorizon.LONG, TimeHorizon.MEDIUM, TimeHorizon.SHORT]:
        group = [s for s in signals if s.horizon == horizon]
        if not group:
            continue
        label, desc = horizon_labels[horizon]
        lines.append(f"\n  {label}")
        lines.append(f"  {desc}")

        for s in group:
            cert_bar = "●" * int(s.certainty * 5) + "○" * (5 - int(s.certainty * 5))
            icon = layer_icons.get(s.layer, "??")
            lines.append(f"    [{icon}] [{cert_bar}] {s.signal}")
            lines.append(f"                    → {s.asset_implication}")
            if s.rebalance_freq:
                lines.append(f"                    调仓: {s.rebalance_freq}")

    lines.append("")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════
#  Part 1: Force → 7 节点 冲击映射
# ══════════════════════════════════════════════════════════════

# 每个 Force 冲击哪些节点，方向和默认强度
# (node_name, sign, default_magnitude)
# sign: +1 = Force 恶化时该节点值增大, -1 = Force 恶化时该节点值减小
# magnitude: 0-1, 该 Force 在 strongly_negative 时对节点的冲击幅度

# 每个 Force 的时间常数 — 它的变化主要在什么时间尺度上发挥作用
FORCE_TIME_CONSTANTS: dict[int, dict[str, TimeHorizon]] = {
    1: {  # 债务/信贷
        "long": TimeHorizon.LONG,    # 长债务周期位置 → 战略配置
        "medium": TimeHorizon.MEDIUM,  # 短债务周期 → 战术调仓
    },
    2: {  # 内部秩序
        "long": TimeHorizon.LONG,    # 贫富分化趋势(数十年) → 税收/监管方向
        "medium": TimeHorizon.MEDIUM,  # 民粹政策(1-3年) → 财政冲击
    },
    3: {  # 外部秩序
        "long": TimeHorizon.LONG,    # 大国权力转移(数十年) → 供应链重构
        "medium": TimeHorizon.MEDIUM,  # 关税/制裁(1-2年) → 企业利润
        "short": TimeHorizon.SHORT,   # 地缘事件(天-周) → 噪音(除非升级)
    },
    4: {  # 自然之力
        "medium": TimeHorizon.MEDIUM,  # 疫情/灾害(月-年) → 政策响应
        "short": TimeHorizon.SHORT,   # 地震/天气(天) → 通常是噪音
    },
    5: {  # 技术/创造力
        "long": TimeHorizon.LONG,    # 技术革命(数十年) → 生产率趋势线
        "medium": TimeHorizon.MEDIUM,  # 哪些公司先受益(1-3年) → 索罗斯跟骑
    },
}

FORCE_TRANSMISSION: dict[int, list[tuple[str, float, float]]] = {
    # Force 1: 债务/信贷 — 已由因果引擎原生处理，这里不重复
    1: [],

    # Force 2: 内部秩序
    # 贫富分化→消费者信心崩→消费↓
    # 财政赤字膨胀→短期consumer↑但debt_service↑+inflation↑
    # 政策不可预测→信贷收缩
    2: [
        ("consumer_health", -1, 0.30),        # 基尼恶化/信心崩→消费者受损
        ("policy_response", +1, 0.25),         # 民粹→积极财政→宽松(短期)
        ("debt_service_burden", +1, 0.20),     # 大规模财政→政府债务↑→偿债压力
        ("inflation_pressure", +1, 0.15),      # 印钱填赤字→通胀
        ("credit_availability", -1, 0.10),     # 政策不确定→银行惜贷
    ],

    # Force 3: 外部秩序
    # 关税/制裁→进口成本↑→通胀
    # 供应链重构→企业成本↑
    # 贸易萎缩→企业收入↓
    # 地缘冲突→避险→信贷收缩
    3: [
        ("inflation_pressure", +1, 0.35),      # 关税/供应链→成本推动通胀
        ("corporate_health", -1, 0.30),        # 供应链成本↑+出口受限→企业受损
        ("consumer_health", -1, 0.15),         # 进口消费品涨价→实际购买力↓
        ("credit_availability", -1, 0.15),     # 地缘不确定→风险偏好↓→信贷收紧
        ("default_pressure", +1, 0.05),        # 边际企业扛不住成本上升
    ],

    # Force 4: 自然之力
    # 疫情: 劳动力冲击(底层不能出门) + 供应链断 + 政策大放水
    # 气候灾害: 供给冲击→通胀 + 财产损失→consumer↓
    # 关键: 政策响应通常是极度宽松(2020: 零利率+财政直接撒钱)
    4: [
        ("consumer_health", -1, 0.35),         # 底层收入冲击最直接
        ("corporate_health", -1, 0.30),        # 企业停工/供应链断裂
        ("policy_response", +1, 0.35),         # 政策必然大幅宽松(央行+财政)
        ("inflation_pressure", +1, 0.20),      # 供给冲击→通胀(滞后)
        ("credit_availability", -1, 0.15),     # 初期信贷冻结(后被政策淹没)
    ],

    # Force 5: 人类创造力/技术
    # AI/新技术: 生产率↑→单位成本↓→通胀↓ + 企业利润↑
    # 但也可能: 替代就业→consumer↓(短期)
    5: [
        ("corporate_health", +1, 0.35),        # 生产率跳升→利润↑
        ("inflation_pressure", -1, 0.25),      # 单位成本下降→通胀压力↓
        ("credit_availability", +1, 0.15),     # 科技信贷扩张
        ("consumer_health", -1, 0.10),         # 短期: 技术替代就业→部分消费者受损
        ("consumer_health", +1, 0.15),         # 长期: 新行业新岗位→消费者受益
        # 净效果: consumer +0.05 (长期正，短期有摩擦)
    ],
}


def _force_direction_to_severity(direction: ForceDirection) -> float:
    """将 Force 方向转为冲击严重度 (0-1)。

    neutral 不是 0——中性状态也可能在变化中，只是还没到阈值。
    给 neutral 一个小的基础值，让它能参与矛盾分析。
    """
    return {
        ForceDirection.STRONGLY_POSITIVE: 1.0,
        ForceDirection.POSITIVE: 0.5,
        ForceDirection.NEUTRAL: 0.15,  # 不是零 — "还没爆但可能在酝酿"
        ForceDirection.NEGATIVE: 0.5,
        ForceDirection.STRONGLY_NEGATIVE: 1.0,
    }[direction]


def _force_is_negative(direction: ForceDirection) -> bool:
    """这个 Force 是利空的吗。"""
    return direction in (ForceDirection.NEGATIVE, ForceDirection.STRONGLY_NEGATIVE)


def compute_force_shocks(forces_view: FiveForcesView) -> dict[str, float]:
    """将五大力量的方向+信心转为 7 节点冲击向量。

    Returns: {node_name: shock_value} — 叠加到因果引擎的基础节点上。
    """
    shocks: dict[str, float] = {}

    for force in forces_view.forces:
        fid = force.force_id
        transmission = FORCE_TRANSMISSION.get(fid, [])
        if not transmission:
            continue

        severity = _force_direction_to_severity(force.effective_direction)
        confidence = force.system_confidence
        is_neg = _force_is_negative(force.effective_direction)

        for node_name, sign, magnitude in transmission:
            # 利空 Force: sign 保持原样 (负号=节点恶化)
            # 利好 Force: sign 反转 (F5 技术利好→corporate_health 变正)
            effective_sign = sign if is_neg else -sign
            shock = effective_sign * magnitude * severity * confidence

            shocks[node_name] = shocks.get(node_name, 0.0) + shock

    return shocks


def inject_shocks_to_nodes(
    nodes: dict[str, MechanismNode],
    shocks: dict[str, float],
    label: str = "外部力量",
) -> dict[str, MechanismNode]:
    """将冲击向量注入到已计算的节点上。

    tanh 压缩确保不会溢出 [-1, 1]。
    """
    result = {}
    for name, node in nodes.items():
        shock = shocks.get(name, 0.0)
        if abs(shock) < 0.001:
            result[name] = node
            continue

        # 在 tanh 空间叠加冲击
        atanh_val = math.atanh(max(-0.999, min(0.999, node.value)))
        new_val = math.tanh(atanh_val + shock)

        result[name] = MechanismNode(
            name=name,
            value=new_val,
            confidence=node.confidence,
            inputs_used=node.inputs_used + [f"{label}({shock:+.3f})"],
            detail=node.detail + f" | {label} {shock:+.3f}",
        )
    return result


# ══════════════════════════════════════════════════════════════
#  Part 2: 主要矛盾判断
# ══════════════════════════════════════════════════════════════


@dataclass
class ContradictionScore:
    """一个 Force 的矛盾得分。"""
    force_id: int
    force_name: str
    direction: ForceDirection

    # 变化速度 (0-1): 这个 Force 的指标变化有多快
    velocity: float = 0.0
    velocity_detail: str = ""

    # 因果传导面 (0-1): 这个 Force 影响了多少个资产
    causal_reach: float = 0.0
    causal_reach_detail: str = ""

    # 综合矛盾分 = velocity × causal_reach
    score: float = 0.0

    # 这个 Force 主要通过哪些节点传导
    primary_channels: list[str] = field(default_factory=list)


@dataclass
class ContradictionAnalysis:
    """主要矛盾分析结果。"""
    forces: list[ContradictionScore] = field(default_factory=list)
    principal: ContradictionScore | None = None
    secondary: ContradictionScore | None = None
    tension: str = ""          # 力量之间的对抗关系


def _compute_causal_reach(
    force_id: int,
    severity: float,
    confidence: float,
    base_nodes: dict[str, MechanismNode],
    profile: CountryProfile | None = None,
) -> tuple[float, str, list[str]]:
    """计算一个 Force 的因果传导面。

    方法: 只注入该 Force 的冲击，看改变了多少个资产的 raw_score。
    """
    transmission = FORCE_TRANSMISSION.get(force_id, [])
    if not transmission:
        return 0.0, "无传导定义", []

    # 构造仅该 Force 的冲击
    shocks: dict[str, float] = {}
    channels = []
    for node_name, sign, magnitude in transmission:
        shock = sign * magnitude * severity * confidence
        shocks[node_name] = shocks.get(node_name, 0.0) + shock
        if abs(shock) > 0.01:
            channels.append(node_name)

    # 注入冲击
    shocked_nodes = inject_shocks_to_nodes(base_nodes, shocks, f"F{force_id}")

    # 计算资产影响差异
    base_impacts = _compute_asset_impacts(base_nodes, profile)
    shocked_impacts = _compute_asset_impacts(shocked_nodes, profile)

    base_scores = {a.asset_type: a.raw_score for a in base_impacts}
    shocked_scores = {a.asset_type: a.raw_score for a in shocked_impacts}

    # 统计方向改变的资产数
    all_assets = set(base_scores) | set(shocked_scores)
    changed = 0
    total_delta = 0.0
    for asset in all_assets:
        bs = base_scores.get(asset, 0)
        ss = shocked_scores.get(asset, 0)
        delta = abs(ss - bs)
        total_delta += delta
        if (bs > 0.02 and ss < -0.02) or (bs < -0.02 and ss > 0.02):
            changed += 1

    n = len(all_assets) if all_assets else 1
    reach = total_delta / n  # 平均每个资产的分数变化
    detail = f"影响{len(channels)}个节点, 资产平均Δ={reach:.3f}"

    return min(reach * 5, 1.0), detail, list(set(channels))  # 归一化到 0-1


def analyze_principal_contradiction(
    forces_view: FiveForcesView,
    base_nodes: dict[str, MechanismNode],
    force_deltas: dict[int, float] | None = None,
    profile: CountryProfile | None = None,
) -> ContradictionAnalysis:
    """分析五大力量中的主要矛盾。

    Args:
        forces_view: 五大力量评估结果
        base_nodes: F1 因果引擎产出的 7 节点基础值
        force_deltas: {force_id: velocity} 手动输入的变化速度
                      如果没有，用 force severity × confidence 估算
        profile: 国家特征

    Returns:
        ContradictionAnalysis: 含排序的矛盾分数和主/次矛盾
    """
    scores: list[ContradictionScore] = []

    for force in forces_view.forces:
        fid = force.force_id
        severity = _force_direction_to_severity(force.effective_direction)
        confidence = force.system_confidence

        # 变化速度
        if force_deltas and fid in force_deltas:
            velocity = min(abs(force_deltas[fid]), 1.0)
            vel_detail = f"手动输入 Δ={force_deltas[fid]:+.2f}"
        else:
            # 无历史对比时，用 severity × confidence 估算
            velocity = severity * confidence
            vel_detail = f"估算: severity={severity:.1f} × conf={confidence:.1f}"

        # 因果传导面
        reach, reach_detail, channels = _compute_causal_reach(
            fid, severity, confidence, base_nodes, profile,
        )

        # 综合矛盾分
        score = velocity * reach

        scores.append(ContradictionScore(
            force_id=fid,
            force_name=force.force_name,
            direction=force.effective_direction,
            velocity=velocity,
            velocity_detail=vel_detail,
            causal_reach=reach,
            causal_reach_detail=reach_detail,
            score=score,
            primary_channels=channels,
        ))

    # 排序
    scores.sort(key=lambda x: x.score, reverse=True)

    analysis = ContradictionAnalysis(forces=scores)
    if scores:
        analysis.principal = scores[0]
    if len(scores) > 1:
        analysis.secondary = scores[1]

    # 检测对抗关系
    if analysis.principal and analysis.secondary:
        p = analysis.principal
        s = analysis.secondary
        p_neg = _force_is_negative(p.direction)
        s_neg = _force_is_negative(s.direction)
        if p_neg != s_neg:
            # 一正一负 = 对抗
            neg_name = p.force_name if p_neg else s.force_name
            pos_name = s.force_name if p_neg else p.force_name
            analysis.tension = (
                f"{neg_name}(利空) vs {pos_name}(利好) — "
                f"这两个力量在争夺主导权"
            )
        elif p_neg and s_neg:
            analysis.tension = (
                f"{p.force_name} + {s.force_name} 双重利空 — "
                f"多重压力叠加"
            )

    return analysis


# ══════════════════════════════════════════════════════════════
#  Part 3: 场景推演
# ══════════════════════════════════════════════════════════════


@dataclass
class WatchItem:
    """监控清单中的一个条目。"""
    event: str                    # 门槛事件描述
    data_source: str              # 从哪里监控 (FRED/新闻/官方公告)
    fred_series: str | None = None  # 如果是 FRED 可自动监控的
    threshold: str = ""           # 触发条件
    confirms_scenario: str = ""   # 触发后确认走了哪条路


@dataclass
class CycleConstraint:
    """大周期约束——排除不可能的路径。"""
    rule: str                     # 约束规则
    reason: str                   # 为什么这条路走不通
    historical_lesson: str = ""   # 历史教训 (达利欧先验)
    blocks_scenario: str = ""     # 阻止哪个场景


@dataclass
class Scenario:
    """一个推演场景。"""
    name: str
    description: str
    # 对 7 节点的冲击 {node_name: shock_value}
    shocks: dict[str, float] = field(default_factory=dict)
    # 场景概率（人判断填入, 系统不估计）
    probability: float | None = None
    # Watchlist: 确认走了这条路的门槛事件
    watchlist: list[WatchItem] = field(default_factory=list)
    # 大周期约束: 为什么这条路走不通 (如果被约束排除)
    blocked_by: list[CycleConstraint] = field(default_factory=list)
    # 是否是改变格局的极端政策场景 (Volcker 1982 类)
    regime_changing: bool = False


@dataclass
class ScenarioResult:
    """一个场景的推演结果。"""
    scenario: Scenario
    nodes_before: dict[str, float] = field(default_factory=dict)
    nodes_after: dict[str, float] = field(default_factory=dict)
    node_deltas: dict[str, float] = field(default_factory=dict)
    asset_impacts: list = field(default_factory=list)
    # 对比基线: 哪些资产方向变了
    direction_changes: list[str] = field(default_factory=list)
    # 是否被大周期约束排除
    blocked: bool = False
    block_reasons: list[str] = field(default_factory=list)


@dataclass
class SimulationResult:
    """完整推演结果。"""
    baseline_nodes: dict[str, float] = field(default_factory=dict)
    baseline_assets: list = field(default_factory=list)
    scenarios: list[ScenarioResult] = field(default_factory=list)
    # 被大周期排除的场景
    blocked_scenarios: list[ScenarioResult] = field(default_factory=list)
    # 政策分析: 哪些节点可以被政策改变
    policy_levers: dict[str, str] = field(default_factory=dict)
    # 全局 watchlist (跨场景)
    regime_change_watch: list[WatchItem] = field(default_factory=list)


def build_scenarios_for_force(
    force: ForceAssessment,
    macro_data: dict | None = None,
    include_policy_response: bool = True,
) -> list[Scenario]:
    """为一个 Force 自动生成推演场景。

    每个 Force 生成 2-4 个场景:
      - 恶化场景
      - 改善场景
      - (可选) 恶化 + 政策对冲场景
      - (可选) 改变格局的极端政策场景 (Volcker 类)

    每个场景自动附带 watchlist。
    """
    fid = force.force_id
    fname = force.force_name
    transmission = FORCE_TRANSMISSION.get(fid, [])
    if not transmission:
        return []

    scenarios = []

    # FORCE_TRANSMISSION 的 sign 定义: Force 恶化时节点的变化方向
    # 但对于 F5 这种"利好力量"，"恶化"=技术退步，sign 方向要反转
    # 通用规则: FORCE_TRANSMISSION 描述的是 Force 变强时对节点的正面作用
    #   - F2/F3/F4 变强 = 利空变强 → sign 原样 = 节点恶化
    #   - F5 变强 = 利好变强 → sign 原样 = 节点改善
    # 场景"恶化" = Force 的负面影响加大:
    #   - F2/F3/F4: 负面力量加大 → 用原始 sign
    #   - F5: 正面力量消退 → 反转 sign

    is_positive_force = force.effective_direction in (
        ForceDirection.POSITIVE, ForceDirection.STRONGLY_POSITIVE,
        ForceDirection.NEUTRAL,  # neutral 视为非负面
    )

    # ── 场景 1: 恶化 ──
    worsen_shocks = {}
    for node_name, sign, magnitude in transmission:
        effective_sign = -sign if is_positive_force else sign
        worsen_shocks[node_name] = worsen_shocks.get(node_name, 0) + effective_sign * magnitude
    scenarios.append(Scenario(
        name=f"{fname}恶化",
        description=f"F{fid}({fname})进一步恶化到极端水平",
        shocks=worsen_shocks,
        watchlist=_build_watchlist(fid, "worsen"),
    ))

    # ── 场景 2: 改善 ──
    improve_shocks = {k: -v for k, v in worsen_shocks.items()}
    scenarios.append(Scenario(
        name=f"{fname}改善",
        description=f"F{fid}({fname})明显改善",
        shocks=improve_shocks,
        watchlist=_build_watchlist(fid, "improve"),
    ))

    # ── 场景 3: 恶化 + 常规政策对冲 ──
    if include_policy_response and fid != 1:
        policy_shocks = dict(worsen_shocks)
        policy_shocks["policy_response"] = policy_shocks.get("policy_response", 0) + 0.3
        policy_shocks["consumer_health"] = policy_shocks.get("consumer_health", 0) + 0.15
        policy_shocks["debt_service_burden"] = policy_shocks.get("debt_service_burden", 0) + 0.1
        scenarios.append(Scenario(
            name=f"{fname}恶化+常规政策对冲",
            description=f"F{fid}恶化, 央行降息25-50bp + 小规模财政",
            shocks=policy_shocks,
            watchlist=_build_watchlist(fid, "policy_normal"),
        ))

    # ── 场景 4: 改变格局的极端政策 (Volcker/2020类) ──
    if include_policy_response:
        extreme_shocks = dict(worsen_shocks)
        # 极端政策: 彻底改变格局
        extreme_shocks["policy_response"] = extreme_shocks.get("policy_response", 0) + 0.7
        extreme_shocks["consumer_health"] = extreme_shocks.get("consumer_health", 0) + 0.3
        extreme_shocks["debt_service_burden"] = extreme_shocks.get("debt_service_burden", 0) + 0.3
        extreme_shocks["inflation_pressure"] = extreme_shocks.get("inflation_pressure", 0) + 0.2
        scenarios.append(Scenario(
            name=f"{fname}恶化+极端政策(格局改变)",
            description=(
                f"F{fid}恶化, 极端政策应对: 利率归零/大规模QE/财政直接撒钱。"
                f"短期救火但改变长期格局(如2020年COVID应对/1982年Volcker杀通胀)"
            ),
            shocks=extreme_shocks,
            watchlist=_build_watchlist(fid, "policy_extreme"),
            regime_changing=True,
        ))

    # ── 应用大周期约束 ──
    constraints = evaluate_cycle_constraints(macro_data or {})
    for scenario in scenarios:
        for constraint in constraints:
            if _constraint_blocks_scenario(constraint, scenario):
                scenario.blocked_by.append(constraint)

    return scenarios


def simulate_scenarios(
    base_nodes: dict[str, MechanismNode],
    scenarios: list[Scenario],
    profile: CountryProfile | None = None,
    macro: MacroContext | None = None,
) -> SimulationResult:
    """对多个场景执行推演。"""
    # 基线
    baseline_assets = _compute_asset_impacts(base_nodes, profile, macro)
    baseline_scores = {a.asset_type: a.raw_score for a in baseline_assets}
    baseline_dirs = {
        a.asset_type: a.direction for a in baseline_assets
    }

    result = SimulationResult(
        baseline_nodes={k: round(v.value, 3) for k, v in base_nodes.items()},
        baseline_assets=baseline_assets,
    )

    for scenario in scenarios:
        # 注入冲击
        shocked_nodes = inject_shocks_to_nodes(
            base_nodes, scenario.shocks, scenario.name
        )

        # 计算资产影响
        scenario_assets = _compute_asset_impacts(shocked_nodes, profile, macro)
        scenario_scores = {a.asset_type: a.raw_score for a in scenario_assets}
        scenario_dirs = {a.asset_type: a.direction for a in scenario_assets}

        # 方向变化
        direction_changes = []
        for asset in baseline_dirs:
            bd = baseline_dirs.get(asset, "")
            sd = scenario_dirs.get(asset, "")
            if bd and sd and bd != sd:
                direction_changes.append(f"{asset}: {bd}→{sd}")

        # 节点变化
        nodes_before = {k: round(v.value, 3) for k, v in base_nodes.items()}
        nodes_after = {k: round(v.value, 3) for k, v in shocked_nodes.items()}
        node_deltas = {
            k: round(nodes_after[k] - nodes_before[k], 3)
            for k in nodes_before
        }

        result.scenarios.append(ScenarioResult(
            scenario=scenario,
            nodes_before=nodes_before,
            nodes_after=nodes_after,
            node_deltas=node_deltas,
            asset_impacts=scenario_assets,
            direction_changes=direction_changes,
        ))

    # 政策杠杆分析
    result.policy_levers = {
        "policy_response": "央行利率/QE — 直接可控",
        "consumer_health": "财政转移支付 — 部分可控(需立法)",
        "credit_availability": "窗口指导/准备金率 — 间接可控",
        "debt_service_burden": "不可直接控制 — 但低利率间接缓解",
        "corporate_health": "减税/补贴 — 部分可控(需立法)",
        "inflation_pressure": "不可直接控制 — 供给侧需时间",
        "default_pressure": "不可直接控制 — 是其他节点的结果",
    }

    # 分离被阻止的场景
    active = []
    blocked = []
    for sr in result.scenarios:
        if sr.scenario.blocked_by:
            sr.blocked = True
            sr.block_reasons = [c.rule for c in sr.scenario.blocked_by]
            blocked.append(sr)
        else:
            active.append(sr)
    result.scenarios = active
    result.blocked_scenarios = blocked

    # 全局 watchlist: 改变格局的事件
    result.regime_change_watch = _build_regime_change_watchlist()

    return result


# ══════════════════════════════════════════════════════════════
#  Part 4: 大周期约束 (先验经验)
# ══════════════════════════════════════════════════════════════
#
# 达利欧大周期理论的核心:
#   短债务周期: 5-8年, 由信贷扩张/收缩驱动
#   长债务周期: 75-100年, 由债务积累/出清驱动
#   帝国兴衰周期: ~250年, 由五大力量共同驱动
#
# 大周期位置决定了哪些政策路径是可能的:
#   - 长周期末期: 利率已经很低 → "大幅加息" 不现实
#   - 债务/GDP 已极高 → "紧缩财政" 政治上不可能
#   - 通胀已嵌入预期 → "温和降息" 无法解决问题
#
# 1982 年教训: Volcker 加息到 20%——当时债务/GDP 还不高 (~30%),
# 利率有巨大空间, 通胀是主要矛盾。这种极端操作在今天的环境下
# (债务/GDP >120%, 利率空间有限) 不可能重复。


def evaluate_cycle_constraints(macro_data: dict) -> list[CycleConstraint]:
    """根据当前宏观数据评估大周期约束。

    返回当前有效的约束列表——这些约束排除了不可能的路径。
    """
    constraints = []
    debt_gdp = macro_data.get("total_debt_to_gdp", 0)
    rate = macro_data.get("fed_funds_rate", 0)
    fiscal_deficit = macro_data.get("fiscal_deficit_to_gdp", 0)
    unemployment = macro_data.get("unemployment_rate", 0)
    cpi = macro_data.get("cpi_actual", 0)

    # ── 约束 1: 利率空间 ──
    # Volcker 能加到 20% 是因为起点低 + 债务/GDP 低
    # 现在利率已经不低且债务高, 大幅加息会直接引发债务危机
    if rate < 1.0:
        constraints.append(CycleConstraint(
            rule="利率已接近零下界",
            reason=f"当前利率 {rate}%, 降息空间几乎耗尽",
            historical_lesson=(
                "2008-2015/2020-2022: 零利率后只能靠QE和财政, "
                "货币政策传导效率大幅下降"
            ),
            blocks_scenario="常规降息对冲",
        ))
    if debt_gdp > 100 and rate > 3:
        constraints.append(CycleConstraint(
            rule="高债务 + 高利率 = 偿债压力快速上升",
            reason=f"债务/GDP={debt_gdp:.0f}%, 利率{rate:.1f}% — 每加息100bp, "
                   f"政府年利息支出增加约 {debt_gdp * 0.01:.0f}% GDP",
            historical_lesson=(
                "1982 Volcker 加到20%时, 债务/GDP仅~30%, 利息负担可控. "
                "今天同样操作会直接引发主权债务危机"
            ),
            blocks_scenario="大幅加息",
        ))

    # ── 约束 2: 财政空间 ──
    if fiscal_deficit > 6:
        constraints.append(CycleConstraint(
            rule="财政赤字已极高",
            reason=f"赤字/GDP={fiscal_deficit:.1f}%, 大规模财政刺激的政治阻力大",
            historical_lesson=(
                "达利欧: 长债务周期末期, 政府倾向于印钱而非真正紧缩. "
                "'紧缩财政'在民粹时代政治上不可能"
            ),
            blocks_scenario="紧缩财政",
        ))

    # ── 约束 3: 通胀嵌入 ──
    if cpi > 4:
        constraints.append(CycleConstraint(
            rule="通胀已嵌入预期",
            reason=f"CPI {cpi:.1f}% 远超目标, 温和宽松无法解决",
            historical_lesson=(
                "1970s教训: 反复的温和宽松让通胀预期固化, "
                "最终需要Volcker式休克疗法. 但前提是债务水平允许"
            ),
            blocks_scenario="温和降息解决通胀",
        ))

    # ── 约束 3b: 通胀 + 零利率 = 必然大幅加息 → 久期资产全杀 ──
    if cpi > 5 and rate < 1:
        constraints.append(CycleConstraint(
            rule="通胀失控+零利率 → 加息风暴即将来临",
            reason=(
                f"CPI {cpi:.1f}% 但利率仅 {rate:.1f}% — 实际利率极度负值({rate-cpi:.1f}%). "
                "Fed 必然大幅加息, 所有久期资产(股票/长债/TIPS)将同时承压. "
                "只有现金和短期商品(供给驱动)相对安全"
            ),
            historical_lesson=(
                "2022: Fed从0加到4.5%, 股债同跌. "
                "1980: Volcker从11%加到20%, 同样股债双杀. "
                "关键信号: 实际利率极度负值 = 加息空间巨大 = 久期风险极大"
            ),
            blocks_scenario="维持宽松",
        ))

    # ── 约束 4: 长债务周期位置 ──
    if debt_gdp > 120:
        constraints.append(CycleConstraint(
            rule="长债务周期顶部区域",
            reason=f"政府债务/GDP={debt_gdp:.0f}%, 处于长周期债务积累的晚期",
            historical_lesson=(
                "达利欧: 长周期末期的出路只有四种 — "
                "紧缩(政治不可能)、违约/重组(极端)、"
                "转移支付(加剧不平等问题)、印钱稀释(最可能). "
                "1945后美国选了通胀稀释, 最终花了20年"
            ),
        ))

    # ── 约束 5: 就业市场状态 ──
    if unemployment > 8:
        constraints.append(CycleConstraint(
            rule="大规模失业",
            reason=f"失业率 {unemployment:.1f}%, 加息/紧缩政治上不可能",
            historical_lesson=(
                "任何政府在大规模失业时都会选择宽松, "
                "不管通胀有多高 — 除非通胀已到恶性程度"
            ),
            blocks_scenario="紧缩政策",
        ))

    # ── 约束 6: 内部秩序 + 财政的必然性 ──
    # 基尼高 + 消费者信心低 → 民粹压力 → 不可能缩减福利/转移支付
    # 这不是约束某个场景, 而是约束政策选择空间
    if fiscal_deficit > 5 and debt_gdp > 100:
        constraints.append(CycleConstraint(
            rule="民粹压力下的财政不可逆",
            reason=(
                f"赤字 {fiscal_deficit:.1f}% + 债务 {debt_gdp:.0f}% — "
                "内部秩序压力(贫富分化)使得缩减财政支出政治上不可能, "
                "但继续扩张又加重债务负担"
            ),
            historical_lesson=(
                "达利欧: 这是长债务周期末期的典型困境 — "
                "'不够钱'和'必须花钱'同时存在. "
                "1930s罗斯福选了花钱(新政), 代价是债务翻倍"
            ),
        ))

    return constraints


def _constraint_blocks_scenario(constraint: CycleConstraint, scenario: Scenario) -> bool:
    """判断一个约束是否阻止一个场景。"""
    if not constraint.blocks_scenario:
        return False
    blocks = constraint.blocks_scenario.lower()
    name = scenario.name.lower()
    desc = scenario.description.lower()
    # 简单的关键词匹配
    if "加息" in blocks and ("降息" in name or "宽松" in desc):
        return False  # 加息约束不阻止降息场景
    if "紧缩" in blocks and ("紧缩" in name or "紧缩" in desc):
        return True
    if "降息" in blocks and ("降息" in name or "降息" in desc):
        return True
    if "大幅加息" in blocks and ("加息" in name or "加息" in desc):
        return True
    return False


# ══════════════════════════════════════════════════════════════
#  Part 5: Watchlist (门槛事件监控)
# ══════════════════════════════════════════════════════════════
#
# 推演出路径后, 关键问题是: 现实走了哪条？
# 每条路径有一组"门槛事件", 观察到就确认该路径。
#
# 两类事件:
#   1. 数据门槛: FRED 可自动监控 (EPU突破500, 失业率突破6%等)
#   2. 政策/事件门槛: 需要新闻源 (Fed宣布降息, 关税落地, 疫苗批准等)
#
# 特殊类: 改变格局的事件 (regime-changing)
#   - Volcker 1982: 加息到20%杀通胀 → 之后40年牛市
#   - 2008 TARP: 政府接管银行 → 避免了1930s式螺旋
#   - 2020 无限QE: Fed无上限购债 → 信贷冻结3天后解除
#   - 这类事件发生后, 所有现有推演都要重跑


# Force → 场景类型 → 门槛事件
_WATCHLIST_TEMPLATES: dict[int, dict[str, list[dict]]] = {
    2: {  # 内部秩序
        "worsen": [
            {"event": "消费者信心跌破50", "source": "FRED", "fred": "UMCSENT",
             "threshold": "<50"},
            {"event": "大规模社会抗议/罢工", "source": "新闻"},
            {"event": "极端政策法案通过(大幅加税/资本管制)", "source": "国会"},
        ],
        "improve": [
            {"event": "消费者信心回升至70+", "source": "FRED", "fred": "UMCSENT",
             "threshold": ">70"},
            {"event": "两党合作法案(基建/教育)", "source": "国会"},
        ],
        "policy_normal": [
            {"event": "Fed降息25-50bp", "source": "FRED", "fred": "FEDFUNDS",
             "threshold": "降幅>0.25"},
            {"event": "小规模财政刺激(<1%GDP)", "source": "国会"},
        ],
        "policy_extreme": [
            {"event": "直接向居民发支票(刺激法案)", "source": "国会"},
            {"event": "Fed开始YCC(收益率曲线控制)", "source": "Fed公告"},
            {"event": "大规模国有化/接管", "source": "新闻"},
        ],
    },
    3: {  # 外部秩序
        "worsen": [
            {"event": "新关税公告(涉及>$100B商品)", "source": "白宫/商务部"},
            {"event": "EPU指数突破500", "source": "FRED", "fred": "USEPUINDXD",
             "threshold": ">500"},
            {"event": "WTI原油突破$100", "source": "FRED", "fred": "DCOILWTICO",
             "threshold": ">100"},
            {"event": "制裁升级(金融/技术)", "source": "财政部/新闻"},
            {"event": "美元指数年涨>10%", "source": "FRED", "fred": "DTWEXBGS"},
        ],
        "improve": [
            {"event": "贸易协议签署", "source": "新闻/白宫"},
            {"event": "关税下调公告", "source": "白宫/商务部"},
            {"event": "EPU回落至200以下", "source": "FRED", "fred": "USEPUINDXD",
             "threshold": "<200"},
        ],
        "policy_normal": [
            {"event": "Fed降息25bp", "source": "FRED", "fred": "FEDFUNDS"},
            {"event": "临时关税豁免(部分商品)", "source": "商务部"},
        ],
        "policy_extreme": [
            {"event": "Fed紧急降息(非会议日)", "source": "Fed公告",
             "threshold": "紧急降息=格局改变信号"},
            {"event": "广场协议2.0(多国汇率协调)", "source": "G7/G20"},
            {"event": "全面贸易禁运", "source": "新闻",
             "threshold": "若发生则所有推演失效"},
        ],
    },
    4: {  # 自然之力
        "worsen": [
            {"event": "WHO宣布PHEIC(国际关注突发公共卫生事件)", "source": "WHO"},
            {"event": "NOAA十亿美元灾害>25次/年", "source": "NOAA"},
            {"event": "食品CPI同比>10%", "source": "FRED", "fred": "CPIUFDSL",
             "threshold": "YoY>10%"},
            {"event": "M7.5+地震 + 海啸", "source": "USGS"},
        ],
        "improve": [
            {"event": "WHO解除PHEIC", "source": "WHO"},
            {"event": "疫苗/特效药获批", "source": "FDA/新闻"},
            {"event": "食品CPI回落至<3%", "source": "FRED", "fred": "CPIUFDSL"},
        ],
        "policy_normal": [
            {"event": "Fed降息+小规模灾害救助", "source": "Fed/国会"},
        ],
        "policy_extreme": [
            {"event": "Fed无限QE(2020.3.23类)", "source": "Fed公告",
             "threshold": "无限制资产购买=格局改变"},
            {"event": "全国封锁令", "source": "白宫"},
            {"event": "财政直接发钱(刺激支票)", "source": "国会"},
        ],
    },
    5: {  # 技术/创造力
        "worsen": [
            {"event": "NASDAQ跌>20%(科技熊市)", "source": "市场"},
            {"event": "AI监管法案(限制部署)", "source": "国会/EU"},
            {"event": "生产率增速跌破1%", "source": "FRED", "fred": "OPHNFB"},
            {"event": "大规模科技裁员(>10万)", "source": "新闻"},
        ],
        "improve": [
            {"event": "生产率增速>3%(历史罕见)", "source": "FRED", "fred": "OPHNFB"},
            {"event": "AI应用爆发(企业采用率>50%)", "source": "行业报告"},
            {"event": "NASDAQ新高+利润增速>30%", "source": "市场"},
        ],
        "policy_normal": [],
        "policy_extreme": [
            {"event": "科技反垄断拆分(类AT&T 1984)", "source": "司法部/法院"},
            {"event": "AI军备竞赛(政府万亿级投资)", "source": "国会"},
        ],
    },
}


def _build_watchlist(force_id: int, scenario_type: str) -> list[WatchItem]:
    """为一个场景构建 watchlist。"""
    templates = _WATCHLIST_TEMPLATES.get(force_id, {}).get(scenario_type, [])
    items = []
    for t in templates:
        items.append(WatchItem(
            event=t["event"],
            data_source=t.get("source", "新闻"),
            fred_series=t.get("fred"),
            threshold=t.get("threshold", ""),
            confirms_scenario=f"F{force_id}_{scenario_type}",
        ))
    return items


def _build_regime_change_watchlist() -> list[WatchItem]:
    """构建全局的格局改变事件 watchlist。

    达利欧1982年教训: 极端政策行动可以改变周期本身。
    这类事件发生后，所有推演都要重新跑。
    """
    return [
        WatchItem(
            event="Fed紧急降息(非会议日)",
            data_source="Fed公告",
            fred_series="FEDFUNDS",
            threshold="紧急降息=过去30年仅3次(2001.1, 2008.1, 2020.3)",
            confirms_scenario="格局改变 — 所有推演需重跑",
        ),
        WatchItem(
            event="Fed宣布无限QE / YCC",
            data_source="Fed公告",
            threshold="2020.3.23 无限QE后, 信贷冻结3天解除. 资产价格规则完全改变",
            confirms_scenario="格局改变 — 流动性规则重置",
        ),
        WatchItem(
            event="政府接管/国有化金融机构",
            data_source="新闻/FDIC",
            threshold="2008 TARP / 2023 SVB 类. 避免系统性崩盘但改变规则",
            confirms_scenario="格局改变 — 信用风险重定价",
        ),
        WatchItem(
            event="大规模财政直接发钱(>1%GDP)",
            data_source="国会",
            threshold="2020-2021三轮刺激支票共$5T. 直接改变消费者行为和通胀预期",
            confirms_scenario="格局改变 — 通胀预期重置",
        ),
        WatchItem(
            event="战争爆发(涉及主要经济体)",
            data_source="新闻",
            threshold="所有经济模型假设和平。大国战争改变一切",
            confirms_scenario="格局改变 — 所有模型失效",
        ),
        WatchItem(
            event="主权债务违约(G7国家)",
            data_source="新闻/评级机构",
            threshold="历史上极罕见, 但长债务周期末期风险上升",
            confirms_scenario="格局改变 — 全球金融秩序重构",
        ),
    ]


# ══════════════════════════════════════════════════════════════
#  格式化
# ══════════════════════════════════════════════════════════════


def format_contradiction_analysis(analysis: ContradictionAnalysis) -> str:
    """格式化主要矛盾分析。"""
    lines = [""]
    lines.append("  主要矛盾分析")
    lines.append("  ════════════════════════════════════════════════")

    dir_labels = {
        ForceDirection.STRONGLY_POSITIVE: "▲▲",
        ForceDirection.POSITIVE: "▲",
        ForceDirection.NEUTRAL: "─",
        ForceDirection.NEGATIVE: "▼",
        ForceDirection.STRONGLY_NEGATIVE: "▼▼",
    }

    for cs in analysis.forces:
        bar = "█" * int(cs.score * 20)
        label = dir_labels[cs.direction]
        is_principal = " ★" if cs == analysis.principal else ""
        lines.append(
            f"\n  F{cs.force_id} {cs.force_name} {label}{is_principal}"
        )
        lines.append(
            f"    矛盾分: {cs.score:.3f}  "
            f"(变化={cs.velocity:.2f} × 传导面={cs.causal_reach:.2f})"
        )
        lines.append(f"    {bar}")
        if cs.primary_channels:
            lines.append(f"    传导: {' → '.join(cs.primary_channels)}")

    if analysis.tension:
        lines.append(f"\n  {'─' * 48}")
        lines.append(f"  对抗: {analysis.tension}")

    if analysis.principal:
        p = analysis.principal
        lines.append(f"\n  {'═' * 48}")
        lines.append(
            f"  ★ 主要矛盾: F{p.force_id}({p.force_name})"
        )
        lines.append(f"    通过 {', '.join(p.primary_channels)} 传导到资产价格")
        lines.append(f"    当前最应关注的指标: 与 {p.force_name} 相关的数据变化")

    lines.append("")
    return "\n".join(lines)


def format_simulation(sim: SimulationResult) -> str:
    """格式化场景推演结果。"""
    lines = [""]
    lines.append("  场景推演")
    lines.append("  ════════════════════════════════════════════════")

    # 基线
    lines.append("\n  基线节点:")
    for name, val in sorted(sim.baseline_nodes.items()):
        bar_len = int(abs(val) * 20)
        bar = ("+" * bar_len) if val >= 0 else ("-" * bar_len)
        lines.append(f"    {name:25s} {val:+.3f} {bar}")

    # ── 被大周期排除的场景 ──
    if sim.blocked_scenarios:
        lines.append(f"\n  {'─' * 48}")
        lines.append("  大周期约束排除的路径:")
        for sr in sim.blocked_scenarios:
            lines.append(f"    ✗ {sr.scenario.name}")
            for reason in sr.block_reasons:
                lines.append(f"      → {reason}")

    # ── 可能的场景 ──
    for sr in sim.scenarios:
        s = sr.scenario
        regime_tag = " [格局改变]" if s.regime_changing else ""
        lines.append(f"\n  ── {s.name}{regime_tag} ──")
        lines.append(f"  {s.description}")

        # 节点变化
        significant = {k: v for k, v in sr.node_deltas.items() if abs(v) > 0.005}
        if significant:
            lines.append("    节点变化:")
            for name, delta in sorted(significant.items(), key=lambda x: -abs(x[1])):
                arrow = "↑" if delta > 0 else "↓"
                before = sr.nodes_before[name]
                after = sr.nodes_after[name]
                lines.append(
                    f"      {name:25s} {before:+.3f} → {after:+.3f} ({arrow}{abs(delta):.3f})"
                )

        # 资产方向变化
        if sr.direction_changes:
            lines.append("    资产方向翻转:")
            for change in sr.direction_changes:
                lines.append(f"      ⚡ {change}")

        # 资产影响
        lines.append("    资产影响:")
        for ai in sr.asset_impacts[:6]:
            arrow = "▲" if ai.direction == "overweight" else "▼"
            lines.append(f"      {arrow} {ai.asset_type:22s} {ai.raw_score:+.3f}")

        # Watchlist
        if s.watchlist:
            lines.append("    监控清单 (确认走了这条路):")
            for w in s.watchlist:
                auto = " [自动]" if w.fred_series else ""
                lines.append(f"      👁 {w.event}{auto}")
                if w.threshold:
                    lines.append(f"         触发: {w.threshold}")

    # 政策杠杆
    lines.append(f"\n  {'─' * 48}")
    lines.append("  政策杠杆 (哪些可以被改变):")
    for node, desc in sim.policy_levers.items():
        controllable = "✓" if "可控" in desc else "✗"
        lines.append(f"    {controllable} {node:25s} {desc}")

    # 全局格局改变监控
    if sim.regime_change_watch:
        lines.append(f"\n  {'═' * 48}")
        lines.append("  格局改变事件 (发生后所有推演重跑):")
        lines.append("  达利欧1982教训: 极端政策行动改变周期本身")
        for w in sim.regime_change_watch:
            lines.append(f"    ⚡ {w.event}")
            if w.threshold:
                lines.append(f"       {w.threshold}")

    lines.append("")
    return "\n".join(lines)
