"""
索罗斯反身性链（宏观层）
========================

Pure Alpha 的第二条腿:
  第一条腿（达利欧）: 经济机器预测未来基本面
  第二条腿（索罗斯）: 市场定价 vs 达利欧预测 → 偏差 = Alpha 机会

核心问题: "市场定价与因果引擎预测之间的偏差，是否处于可利用的反身性阶段？"

输入:
  1. DalioResult — 达利欧的基本面预测
  2. MarketImplied — 市场隐含预期（breakeven inflation、利率期货、信用利差等）

输出:
  1. 偏差列表 — 达利欧认为 X 但市场定价 Y
  2. 反身性阶段 — 自我强化中 / 接近顶点 / 正在反转
  3. Alpha 机会 — 偏差大 × 可能收敛的押注
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from polaris.principles.dimensions import (
    CycleRegime,
    DalioResult,
    Tilt,
)


# ── 市场隐含预期数据 ─────────────────────────────────────────


@dataclass
class MarketImplied:
    """市场隐含预期 — 从资产价格反推市场认为会发生什么。

    所有字段可选。缺失字段导致对应偏差无法计算。
    数据源: FRED (breakeven, credit spreads), 利率期货, 反向 DCF
    """
    # 通胀预期
    breakeven_5y: float | None = None          # 5 年期 breakeven 通胀 (%)
    breakeven_10y: float | None = None         # 10 年期 breakeven 通胀 (%)

    # 利率预期
    implied_rate_12m: float | None = None      # 12 个月后市场隐含利率 (%)

    # 信用风险定价
    credit_spread_ig: float | None = None      # 投资级信用利差 (%)
    credit_spread_hy: float | None = None      # 高收益信用利差 (%)

    # 股票隐含增长
    implied_equity_growth: float | None = None  # 反向 DCF 隐含增长率 (%)

    # 波动率
    vix: float | None = None                    # 隐含波动率
    vix_term_structure: float | None = None     # VIX 期限结构斜率（正=contango=平静, 负=backwardation=恐慌）


# ── 偏差类型 ────────────────────────────────────────────────


class DivergenceType(str, Enum):
    INFLATION = "inflation"         # 达利欧预测的通胀 vs 市场定价的通胀
    GROWTH = "growth"               # 达利欧预测的增长 vs 市场定价的增长
    CREDIT_RISK = "credit_risk"     # 达利欧预测的违约 vs 市场定价的违约
    POLICY = "policy"               # 达利欧预测的政策路径 vs 市场定价的利率路径
    VOLATILITY = "volatility"       # 达利欧预测的不确定性 vs 市场定价的波动率


@dataclass
class Divergence:
    """单个偏差。"""
    type: DivergenceType
    dalio_view: float               # 达利欧因果引擎的预测值
    market_view: float              # 市场隐含定价
    gap: float                      # 偏差 = dalio - market (正=达利欧比市场更乐观/高)
    gap_magnitude: str              # "small" / "medium" / "large"
    direction: str                  # 达利欧比市场更 "hawkish" / "dovish" / "bullish" / "bearish"
    tradeable: bool                 # 是否可交易
    detail: str                     # 人可读解释


class ReflexivityPhase(str, Enum):
    """反身性阶段。"""
    EARLY_SELF_REINFORCING = "early_self_reinforcing"    # 趋势刚开始自我强化
    LATE_SELF_REINFORCING = "late_self_reinforcing"      # 趋势强化到极端
    APPROACHING_CLIMAX = "approaching_climax"             # 接近顶点/底点
    REVERSAL = "reversal"                                 # 正在反转
    NEUTRAL = "neutral"                                   # 无明显反身性
    UNKNOWN = "unknown"


@dataclass
class AlphaOpportunity:
    """Alpha 机会 = 偏差 × 反身性阶段 × 可操作性。"""
    asset_type: str                  # 哪个资产类别
    direction: str                   # "overweight" / "underweight"
    conviction: float                # 信心 0-1
    thesis: str                      # 为什么这是机会
    divergence: Divergence           # 对应的偏差
    risk: str                        # 如果错了会怎样


@dataclass
class SorosChainResult:
    """索罗斯链完整输出。"""
    divergences: list[Divergence] = field(default_factory=list)
    reflexivity_phase: ReflexivityPhase = ReflexivityPhase.UNKNOWN
    reflexivity_detail: str = ""
    alpha_opportunities: list[AlphaOpportunity] = field(default_factory=list)
    conclusion: str = ""


# ══════════════════════════════════════════════════════════════
#  偏差检测
# ══════════════════════════════════════════════════════════════


def _classify_gap(gap: float, thresholds: tuple[float, float] = (0.5, 1.5)) -> str:
    """偏差幅度分类。"""
    if abs(gap) < thresholds[0]:
        return "small"
    elif abs(gap) < thresholds[1]:
        return "medium"
    return "large"


def _detect_inflation_divergence(
    dalio: DalioResult,
    market: MarketImplied,
) -> Divergence | None:
    """达利欧预测的通胀方向 vs 市场 breakeven 通胀。"""
    if market.breakeven_5y is None:
        return None
    if dalio.regime is None:
        return None

    # 达利欧的通胀预测: 从因果引擎的 inflation_pressure 推断
    # inflation_gap > 0 = 达利欧认为通胀高于预期
    dalio_inflation_view = dalio.regime.inflation_gap  # pp above/below expected

    # 市场的通胀预测: breakeven inflation vs 央行目标(2%)
    market_inflation_view = market.breakeven_5y - 2.0  # pp above/below 2%

    gap = dalio_inflation_view - market_inflation_view

    if abs(gap) < 0.3:
        return None  # 偏差太小，不值得交易

    direction = "hawkish" if gap > 0 else "dovish"
    # 达利欧比市场更 hawkish = 达利欧认为通胀更高 = 市场低估了通胀

    return Divergence(
        type=DivergenceType.INFLATION,
        dalio_view=dalio_inflation_view,
        market_view=market_inflation_view,
        gap=round(gap, 2),
        gap_magnitude=_classify_gap(gap),
        direction=direction,
        tradeable=True,
        detail=(
            f"达利欧认为通胀偏离目标 {dalio_inflation_view:+.1f}pp，"
            f"市场定价偏离 {market_inflation_view:+.1f}pp，"
            f"偏差 {gap:+.1f}pp → 达利欧比市场更{direction}"
        ),
    )


def _detect_credit_divergence(
    dalio: DalioResult,
    market: MarketImplied,
) -> Divergence | None:
    """达利欧预测的违约压力 vs 市场信用利差。"""
    if market.credit_spread_hy is None:
        return None

    # 达利欧的违约预测: 从因果引擎的 tilts 推断
    # 如果 default_pressure 高 → 达利欧认为违约风险高
    # 用 DalioResult 的 active_tilts 间接推断
    dalio_risk_view = 0.0
    for tilt in dalio.active_tilts:
        if tilt.asset_type == "equity_cyclical" and tilt.direction == "underweight":
            dalio_risk_view += tilt.magnitude
        if tilt.asset_type == "gold" and tilt.direction == "overweight":
            dalio_risk_view += tilt.magnitude * 0.5

    # 市场的违约定价: HY 利差
    # 正常: 3-5%, 紧张: 5-7%, 恐慌: 7%+
    market_risk_view = (market.credit_spread_hy - 4.0) / 3.0  # 归一化: 4%=0, 7%=1

    gap = dalio_risk_view - market_risk_view

    if abs(gap) < 0.2:
        return None

    direction = "bearish" if gap > 0 else "bullish"

    return Divergence(
        type=DivergenceType.CREDIT_RISK,
        dalio_view=round(dalio_risk_view, 2),
        market_view=round(market_risk_view, 2),
        gap=round(gap, 2),
        gap_magnitude=_classify_gap(gap, (0.3, 0.8)),
        direction=direction,
        tradeable=True,
        detail=(
            f"达利欧违约压力信号 {dalio_risk_view:.2f}，"
            f"市场信用利差定价 {market_risk_view:.2f} (HY={market.credit_spread_hy:.1f}%)，"
            f"偏差 {gap:+.2f} → 达利欧比市场更{direction}"
        ),
    )


def _detect_policy_divergence(
    dalio: DalioResult,
    market: MarketImplied,
    current_rate: float | None,
) -> Divergence | None:
    """达利欧预测的政策路径 vs 市场利率期货。"""
    if market.implied_rate_12m is None or current_rate is None:
        return None

    # 市场隐含的 12 个月后利率变化
    market_rate_change = market.implied_rate_12m - current_rate

    # 达利欧的预测: 从 regime + tilts 推断政策方向
    # 如果 nominal_bond overweight → 达利欧预期降息
    dalio_rate_change = 0.0
    for tilt in dalio.active_tilts:
        if tilt.asset_type == "nominal_bond":
            if tilt.direction == "overweight":
                dalio_rate_change -= tilt.magnitude * 2.0  # 超配债券 → 预期降息
            else:
                dalio_rate_change += tilt.magnitude * 2.0  # 低配债券 → 预期加息

    gap = dalio_rate_change - market_rate_change

    if abs(gap) < 0.3:
        return None

    direction = "dovish" if gap < 0 else "hawkish"

    return Divergence(
        type=DivergenceType.POLICY,
        dalio_view=round(dalio_rate_change, 2),
        market_view=round(market_rate_change, 2),
        gap=round(gap, 2),
        gap_magnitude=_classify_gap(gap, (0.5, 1.5)),
        direction=direction,
        tradeable=True,
        detail=(
            f"达利欧预期利率变化 {dalio_rate_change:+.1f}pp，"
            f"市场定价 {market_rate_change:+.1f}pp，"
            f"偏差 {gap:+.1f}pp → 达利欧比市场更{direction}"
        ),
    )


def _detect_volatility_divergence(
    dalio: DalioResult,
    market: MarketImplied,
) -> Divergence | None:
    """达利欧看到的尾部风险 vs 市场定价的波动率。"""
    if market.vix is None:
        return None

    # 达利欧的风险视图: 从因果引擎的 hedge_specs 数量推断
    dalio_risk_level = 0.0
    if hasattr(dalio, "school_score"):
        # 用 tilts 的分散度间接推断不确定性
        ow = [t for t in dalio.active_tilts if t.direction == "overweight"]
        uw = [t for t in dalio.active_tilts if t.direction == "underweight"]
        # 偏移幅度大 = 达利欧认为方向性强 = 应该有对应的波动率
        avg_magnitude = sum(t.magnitude for t in dalio.active_tilts) / max(len(dalio.active_tilts), 1)
        dalio_risk_level = avg_magnitude * 30 + 15  # 粗略映射到 VIX 等效

    # 偏差: 达利欧认为的不确定性 vs VIX
    gap = dalio_risk_level - market.vix

    if abs(gap) < 5:
        return None

    direction = "bearish" if gap > 0 else "bullish"

    return Divergence(
        type=DivergenceType.VOLATILITY,
        dalio_view=round(dalio_risk_level, 1),
        market_view=market.vix,
        gap=round(gap, 1),
        gap_magnitude=_classify_gap(gap, (5, 15)),
        direction=direction,
        tradeable=True,
        detail=(
            f"达利欧隐含波动率 {dalio_risk_level:.0f}，"
            f"VIX={market.vix:.0f}，"
            f"偏差 {gap:+.0f} → {'市场太自满' if gap > 0 else '市场太恐慌'}"
        ),
    )


# ══════════════════════════════════════════════════════════════
#  反身性阶段判断
# ══════════════════════════════════════════════════════════════


def _assess_reflexivity(
    divergences: list[Divergence],
    market: MarketImplied,
) -> tuple[ReflexivityPhase, str]:
    """判断当前反身性阶段。

    索罗斯框架:
    - 趋势 + 偏差 = 开始自我强化
    - 偏差越来越大 = 趋势加速（远离均衡）
    - 偏差极端 + 外部冲击 = 接近反转
    - 偏差开始缩小 = 正在反转
    """
    if not divergences:
        return ReflexivityPhase.NEUTRAL, "无显著偏差"

    large_gaps = [d for d in divergences if d.gap_magnitude == "large"]
    medium_gaps = [d for d in divergences if d.gap_magnitude == "medium"]

    # VIX 期限结构是反身性的关键指标
    # backwardation(负斜率) = 近期恐慌 > 远期 = 市场在反转中
    # contango(正斜率) = 市场自满 = 可能在自我强化的晚期
    vix_backwardation = (
        market.vix_term_structure is not None
        and market.vix_term_structure < -0.1
    )
    vix_extreme_contango = (
        market.vix_term_structure is not None
        and market.vix_term_structure > 0.5
    )

    if vix_backwardation and large_gaps:
        return (
            ReflexivityPhase.REVERSAL,
            f"VIX 期限结构倒挂 + {len(large_gaps)} 个大偏差 → 反转进行中",
        )

    if len(large_gaps) >= 2:
        return (
            ReflexivityPhase.APPROACHING_CLIMAX,
            f"{len(large_gaps)} 个大偏差 → 接近极端，反转风险高",
        )

    if vix_extreme_contango and medium_gaps:
        return (
            ReflexivityPhase.LATE_SELF_REINFORCING,
            f"VIX 极度 contango + 偏差累积 → 自我强化晚期（市场自满）",
        )

    if medium_gaps:
        return (
            ReflexivityPhase.EARLY_SELF_REINFORCING,
            f"{len(medium_gaps)} 个中等偏差 → 趋势在强化",
        )

    return ReflexivityPhase.NEUTRAL, "偏差较小，无明显反身性"


# ══════════════════════════════════════════════════════════════
#  Alpha 机会生成
# ══════════════════════════════════════════════════════════════


def _generate_alpha_opportunities(
    divergences: list[Divergence],
    phase: ReflexivityPhase,
    dalio: DalioResult,
) -> list[AlphaOpportunity]:
    """从偏差 + 反身性阶段 → Alpha 机会。

    只在偏差 medium/large + 可交易时生成机会。
    反身性阶段影响信心:
      - approaching_climax/reversal: 高信心（偏差即将收敛）
      - late_self_reinforcing: 中等信心（还在强化但接近极端）
      - early_self_reinforcing: 低信心（趋势可能继续）
    """
    phase_confidence = {
        ReflexivityPhase.REVERSAL: 0.8,
        ReflexivityPhase.APPROACHING_CLIMAX: 0.7,
        ReflexivityPhase.LATE_SELF_REINFORCING: 0.5,
        ReflexivityPhase.EARLY_SELF_REINFORCING: 0.3,
        ReflexivityPhase.NEUTRAL: 0.2,
        ReflexivityPhase.UNKNOWN: 0.1,
    }

    opportunities: list[AlphaOpportunity] = []

    for div in divergences:
        if not div.tradeable:
            continue
        if div.gap_magnitude == "small":
            continue

        base_conviction = phase_confidence.get(phase, 0.2)
        if div.gap_magnitude == "large":
            base_conviction = min(1.0, base_conviction + 0.2)

        # 映射偏差到资产方向
        if div.type == DivergenceType.INFLATION:
            if div.gap > 0:
                # 达利欧认为通胀更高 → 市场低估通胀 → 做多通胀对冲
                opportunities.append(AlphaOpportunity(
                    asset_type="inflation_linked_bond",
                    direction="overweight",
                    conviction=base_conviction,
                    thesis=f"市场低估通胀: breakeven {div.market_view:+.1f}pp，达利欧预测 {div.dalio_view:+.1f}pp",
                    divergence=div,
                    risk="如果通胀确实回落，TIPS 会跑输名义债券",
                ))
            else:
                opportunities.append(AlphaOpportunity(
                    asset_type="nominal_bond",
                    direction="overweight",
                    conviction=base_conviction,
                    thesis=f"市场高估通胀: breakeven {div.market_view:+.1f}pp，达利欧预测 {div.dalio_view:+.1f}pp",
                    divergence=div,
                    risk="如果通胀超预期上行，名义债券会大亏",
                ))

        elif div.type == DivergenceType.CREDIT_RISK:
            if div.gap > 0:
                # 达利欧比市场更悲观 → 市场低估违约风险 → 避险
                opportunities.append(AlphaOpportunity(
                    asset_type="equity_cyclical",
                    direction="underweight",
                    conviction=base_conviction,
                    thesis=f"市场低估信用风险: 利差仅 {div.market_view:.1f}，达利欧违约信号 {div.dalio_view:.1f}",
                    divergence=div,
                    risk="如果经济软着陆，空头会被轧",
                ))
            else:
                opportunities.append(AlphaOpportunity(
                    asset_type="equity_cyclical",
                    direction="overweight",
                    conviction=base_conviction,
                    thesis=f"市场高估信用风险: 利差 {div.market_view:.1f}，达利欧违约信号仅 {div.dalio_view:.1f}",
                    divergence=div,
                    risk="如果信用事件爆发，损失会很大",
                ))

        elif div.type == DivergenceType.POLICY:
            if div.gap < 0:
                # 达利欧比市场更鸽 → 市场低估降息 → 做多债券
                opportunities.append(AlphaOpportunity(
                    asset_type="nominal_bond",
                    direction="overweight",
                    conviction=base_conviction,
                    thesis=f"市场低估降息: 市场定价 {div.market_view:+.1f}pp，达利欧预测 {div.dalio_view:+.1f}pp",
                    divergence=div,
                    risk="如果央行比预期更鹰派，债券会亏",
                ))
            else:
                opportunities.append(AlphaOpportunity(
                    asset_type="nominal_bond",
                    direction="underweight",
                    conviction=base_conviction,
                    thesis=f"市场低估加息: 市场定价 {div.market_view:+.1f}pp，达利欧预测 {div.dalio_view:+.1f}pp",
                    divergence=div,
                    risk="如果通胀缓解导致降息，会踏空",
                ))

        elif div.type == DivergenceType.VOLATILITY:
            if div.gap > 0:
                # 达利欧看到更高风险 → VIX 太低 → 买波动率
                opportunities.append(AlphaOpportunity(
                    asset_type="gold",
                    direction="overweight",
                    conviction=base_conviction,
                    thesis=f"市场太自满: VIX={div.market_view:.0f}，达利欧隐含波动率 {div.dalio_view:.0f}",
                    divergence=div,
                    risk="如果市场继续平静，持有黄金的机会成本",
                ))

    # 按信心排序
    opportunities.sort(key=lambda x: x.conviction, reverse=True)
    return opportunities


# ══════════════════════════════════════════════════════════════
#  主入口
# ══════════════════════════════════════════════════════════════


def evaluate_soros(
    dalio: DalioResult,
    market: MarketImplied,
    current_rate: float | None = None,
) -> SorosChainResult:
    """索罗斯链主入口。

    输入:
      dalio: 达利欧因果引擎的输出（第一条腿）
      market: 市场隐含预期数据
      current_rate: 当前央行基准利率

    输出:
      偏差列表 + 反身性阶段 + Alpha 机会
    """
    result = SorosChainResult()

    # ── 检测偏差 ──
    detectors = [
        _detect_inflation_divergence(dalio, market),
        _detect_credit_divergence(dalio, market),
        _detect_policy_divergence(dalio, market, current_rate),
        _detect_volatility_divergence(dalio, market),
    ]
    result.divergences = [d for d in detectors if d is not None]

    # ── 反身性阶段 ──
    result.reflexivity_phase, result.reflexivity_detail = _assess_reflexivity(
        result.divergences, market,
    )

    # ── Alpha 机会 ──
    result.alpha_opportunities = _generate_alpha_opportunities(
        result.divergences, result.reflexivity_phase, dalio,
    )

    # ── 结论 ──
    if not result.divergences:
        result.conclusion = "无显著偏差: 达利欧预测与市场定价基本一致"
    elif not result.alpha_opportunities:
        result.conclusion = f"{len(result.divergences)} 个偏差但均不可操作"
    else:
        top = result.alpha_opportunities[0]
        result.conclusion = (
            f"{len(result.divergences)} 个偏差，{len(result.alpha_opportunities)} 个 Alpha 机会。"
            f"最佳: {top.direction} {top.asset_type} (信心 {top.conviction:.0%})"
        )

    return result


# ══════════════════════════════════════════════════════════════
#  格式化
# ══════════════════════════════════════════════════════════════


def compute_complacency_signal(market: MarketImplied) -> dict[str, float]:
    """纯市场信号检测（不依赖 breakeven/credit spread）。

    用 VIX 水平 + VIX 期限结构判断市场自满/恐慌程度。
    这两个数据从 yfinance 直接获取，不需要 FRED。

    自满(VIX低+contango) → 风险资产被高估 → 减分
    恐慌(VIX高+backwardation) → 风险资产被低估 → 加分
    """
    adjustments: dict[str, float] = {}

    if market.vix is None:
        return adjustments

    vix = market.vix
    vix_ts = market.vix_term_structure  # 正=contango(平静), 负=backwardation(恐慌)

    # ── 极度自满: VIX < 14 + contango ──
    # 市场没有为风险定价 → 所有风险资产被高估
    if vix < 14 and (vix_ts is None or vix_ts > 0.2):
        strength = (14 - vix) / 10  # VIX=10 → strength=0.4
        adjustments["equity_cyclical"] = -strength * 0.5
        adjustments["commodity"] = -strength * 0.3
        adjustments["gold"] = strength * 0.3  # 自满时黄金被低估
        adjustments["cash"] = strength * 0.3

    # ── 极度恐慌: VIX > 30 + backwardation ──
    # 市场过度恐慌 → 风险资产被低估
    elif vix > 30 and (vix_ts is not None and vix_ts < -0.1):
        strength = min((vix - 30) / 20, 0.5)  # VIX=50 → strength=0.5
        adjustments["equity_cyclical"] = strength * 0.4
        adjustments["gold"] = strength * 0.3  # 恐慌时黄金仍有价值
        adjustments["nominal_bond"] = strength * 0.2
        adjustments["commodity"] = -strength * 0.2  # 恐慌=需求崩溃=大宗差

    # ── 中间状态: VIX 20-30 ──
    # 不做调整——不确定性本身是合理定价

    return adjustments


def compute_soros_adjustments(result: SorosChainResult) -> dict[str, float]:
    """把索罗斯偏差转化为资产排名调整分数。

    核心逻辑:
    - 通胀偏差: 达利欧比市场更鹰 → 市场低估了通胀 → nominal_bond 是被高估的(减分)
    - 信用偏差: 达利欧比市场更熊 → 市场低估了风险 → equity 是被高估的(减分)
    - 波动率偏差: VIX 太低(市场自满) → 所有风险资产被高估(减分)

    返回: {asset_type: adjustment} 正=加分 负=减分
    """
    adjustments: dict[str, float] = {}

    for div in result.divergences:
        if div.gap_magnitude == "small":
            continue

        strength = 0.15 if div.gap_magnitude == "medium" else 0.25

        if div.type == DivergenceType.INFLATION:
            if div.gap > 0:
                # 达利欧认为通胀更高 → 市场低估通胀 → 名义债券被高估
                adjustments["nominal_bond"] = adjustments.get("nominal_bond", 0) - strength
                adjustments["inflation_linked_bond"] = adjustments.get("inflation_linked_bond", 0) + strength * 0.5
                adjustments["commodity"] = adjustments.get("commodity", 0) + strength * 0.5
            else:
                # 达利欧认为通胀更低 → 市场高估了通胀 → 大宗/黄金被高估
                adjustments["commodity"] = adjustments.get("commodity", 0) - strength
                adjustments["gold"] = adjustments.get("gold", 0) - strength * 0.5
                adjustments["nominal_bond"] = adjustments.get("nominal_bond", 0) + strength * 0.5

        elif div.type == DivergenceType.CREDIT_RISK:
            if div.gap > 0:
                # 达利欧更悲观 → 市场低估风险 → 风险资产被高估
                adjustments["equity_cyclical"] = adjustments.get("equity_cyclical", 0) - strength
                adjustments["gold"] = adjustments.get("gold", 0) + strength * 0.5
                adjustments["cash"] = adjustments.get("cash", 0) + strength * 0.5
            else:
                # 达利欧更乐观 → 市场高估了风险 → 风险资产被低估
                adjustments["equity_cyclical"] = adjustments.get("equity_cyclical", 0) + strength
                adjustments["nominal_bond"] = adjustments.get("nominal_bond", 0) - strength * 0.3

        elif div.type == DivergenceType.VOLATILITY:
            if div.gap > 0:
                # 达利欧看到更高风险 → VIX 太低 → 所有风险资产被高估
                adjustments["equity_cyclical"] = adjustments.get("equity_cyclical", 0) - strength * 0.7
                adjustments["commodity"] = adjustments.get("commodity", 0) - strength * 0.5
                adjustments["gold"] = adjustments.get("gold", 0) + strength * 0.7
                adjustments["cash"] = adjustments.get("cash", 0) + strength * 0.5
            else:
                # VIX 太高(市场恐慌) → 风险资产被低估
                adjustments["equity_cyclical"] = adjustments.get("equity_cyclical", 0) + strength * 0.5
                adjustments["gold"] = adjustments.get("gold", 0) - strength * 0.3

        elif div.type == DivergenceType.POLICY:
            if div.gap < 0:
                # 达利欧预期更多降息 → 市场低估降息 → 债券被低估
                adjustments["nominal_bond"] = adjustments.get("nominal_bond", 0) + strength
            else:
                adjustments["nominal_bond"] = adjustments.get("nominal_bond", 0) - strength

    return adjustments


def format_soros(result: SorosChainResult) -> str:
    """格式化索罗斯链报告。"""
    lines = [""]
    lines.append("  索罗斯反身性链 (Pure Alpha 第二条腿)")
    lines.append("  ════════════════════════════════════════════════")

    # 偏差
    if result.divergences:
        lines.append("\n  偏差检测:")
        for d in result.divergences:
            mag = {"small": "·", "medium": "◆", "large": "★"}[d.gap_magnitude]
            lines.append(f"    {mag} [{d.type.value}] {d.detail}")
    else:
        lines.append("\n  偏差: 无 — 达利欧与市场一致")

    # 反身性
    phase_labels = {
        ReflexivityPhase.EARLY_SELF_REINFORCING: "早期自我强化",
        ReflexivityPhase.LATE_SELF_REINFORCING: "晚期自我强化",
        ReflexivityPhase.APPROACHING_CLIMAX: "接近极端",
        ReflexivityPhase.REVERSAL: "正在反转",
        ReflexivityPhase.NEUTRAL: "中性",
        ReflexivityPhase.UNKNOWN: "未知",
    }
    lines.append(f"\n  反身性: {phase_labels.get(result.reflexivity_phase, '?')}")
    lines.append(f"    {result.reflexivity_detail}")

    # Alpha 机会
    if result.alpha_opportunities:
        lines.append(f"\n  Alpha 机会 ({len(result.alpha_opportunities)}):")
        for opp in result.alpha_opportunities:
            arrow = "▲" if opp.direction == "overweight" else "▼"
            lines.append(f"    {arrow} {opp.asset_type} (信心 {opp.conviction:.0%})")
            lines.append(f"      论点: {opp.thesis}")
            lines.append(f"      风险: {opp.risk}")

    lines.append(f"\n  ════════════════════════════════════════════════")
    lines.append(f"  {result.conclusion}")
    lines.append("")
    return "\n".join(lines)
