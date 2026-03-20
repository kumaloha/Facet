"""
索罗斯反身性认知链
==================

独立的认知框架——不依赖达利欧的输出。

索罗斯的核心: 市场参与者的认知本身改变了现实。
他不预测经济，他观察市场行为，判断偏差何时会自我修正。

输入: 纯市场数据（价格、波动率、利差、动量）
输出:
  1. 市场叙事: 市场在讲什么故事
  2. 反身性阶段: 这个故事是在自我强化还是接近破裂
  3. 过度延伸: 每个资产被推到了多极端的位置
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


# ── 市场数据输入 ────────────────────────────────────────────


@dataclass
class MarketState:
    """纯市场数据——索罗斯认知的唯一输入。

    不含任何宏观经济数据（那是达利欧的领域）。
    只有市场价格和市场行为。
    """
    # 各资产近期动量 (过去 12 个月回报 %)
    momentum_equity: float | None = None
    momentum_long_bond: float | None = None
    momentum_gold: float | None = None
    momentum_commodity: float | None = None
    momentum_em_bond: float | None = None

    # 波动率
    vix: float | None = None
    vix_percentile: float | None = None       # VIX 在过去 5 年的百分位 (0-100)
    vix_change_1m: float | None = None        # VIX 过去 1 个月变化

    # 信用市场
    credit_spread_hy: float | None = None     # 高收益信用利差 (%)
    credit_spread_change_3m: float | None = None  # 利差 3 个月变化 (pp)

    # 收益率曲线
    yield_curve_10y_3m: float | None = None   # 10Y - 3M 利差 (%)
    yield_curve_change_3m: float | None = None

    # 估值
    equity_pe_ratio: float | None = None      # S&P 500 P/E 比率
    equity_pe_percentile: float | None = None # P/E 在历史分布的百分位 (0-100)

    # 快照时间
    snapshot_date: str = ""


# ── 叙事类型 ────────────────────────────────────────────────


class MarketNarrative(str, Enum):
    """市场在讲什么故事。"""
    RISK_ON = "risk_on"                    # 风险偏好
    RISK_OFF = "risk_off"                  # 风险厌恶
    INFLATION_FEAR = "inflation_fear"      # 通胀恐惧
    DEFLATION_FEAR = "deflation_fear"      # 通缩恐惧
    COMPLACENCY = "complacency"            # 自满
    PANIC = "panic"                        # 恐慌
    AMBIGUOUS = "ambiguous"                # 不明确


class ReflexivityPhase(str, Enum):
    """反身性阶段。"""
    EARLY_TREND = "early_trend"
    SELF_REINFORCING = "self_reinforcing"
    LATE_STAGE = "late_stage"
    APPROACHING_CLIMAX = "approaching_climax"
    REVERSAL = "reversal"
    NEUTRAL = "neutral"


@dataclass
class SorosInsight:
    """索罗斯认知链的输出。"""
    narrative: MarketNarrative = MarketNarrative.AMBIGUOUS
    narrative_detail: str = ""

    phase: ReflexivityPhase = ReflexivityPhase.NEUTRAL
    phase_detail: str = ""

    # 每个资产的过度延伸度 (-1 到 +1)
    # 正 = 市场过度乐观（被高估）→ 均值回归风险
    # 负 = 市场过度悲观（被低估）→ 反弹机会
    overextension: dict[str, float] = field(default_factory=dict)

    confidence: float = 0.0
    evidence: list[str] = field(default_factory=list)


# ── 叙事识别 ────────────────────────────────────────────────


def _identify_narrative(market: MarketState) -> tuple[MarketNarrative, str]:
    """从市场数据读出市场在讲什么故事。"""
    eq = market.momentum_equity
    bond = market.momentum_long_bond
    gold = market.momentum_gold
    comm = market.momentum_commodity
    vix = market.vix

    if eq is None:
        return MarketNarrative.AMBIGUOUS, "数据不足"

    # VIX 极端先判
    if vix is not None and vix > 35:
        return MarketNarrative.PANIC, f"VIX={vix:.0f} 恐慌水平"
    if vix is not None and vix < 13 and eq is not None and eq > 15:
        return MarketNarrative.COMPLACENCY, f"VIX={vix:.0f} + 股票涨{eq:.0f}% 市场自满"

    # 股债方向组合
    if eq is not None and bond is not None:
        if eq > 10 and bond > 5:
            return MarketNarrative.RISK_ON, f"股债双涨: 股+{eq:.0f}% 债+{bond:.0f}%"
        if eq < -5 and bond > 10:
            return MarketNarrative.RISK_OFF, f"避险: 股{eq:.0f}% 债+{bond:.0f}%"
        if eq < -10 and bond < -10:
            return MarketNarrative.PANIC, f"全面下跌: 股{eq:.0f}% 债{bond:.0f}%"

    # 通胀/通缩叙事
    if bond is not None and comm is not None:
        if bond < -5 and comm > 10:
            return MarketNarrative.INFLATION_FEAR, f"通胀叙事: 大宗+{comm:.0f}% 债{bond:.0f}%"
        if bond > 10 and comm < -10:
            return MarketNarrative.DEFLATION_FEAR, f"通缩叙事: 债+{bond:.0f}% 大宗{comm:.0f}%"

    return MarketNarrative.AMBIGUOUS, "混合信号"


# ── 反身性阶段 ──────────────────────────────────────────────


def _assess_reflexivity(
    market: MarketState,
    narrative: MarketNarrative,
) -> tuple[ReflexivityPhase, str]:
    """判断当前趋势处于反身性的哪个阶段。"""
    eq_mom = market.momentum_equity
    vix = market.vix
    vix_change = market.vix_change_1m
    spread_change = market.credit_spread_change_3m
    pe_pct = market.equity_pe_percentile

    if narrative == MarketNarrative.PANIC:
        if vix_change is not None and vix_change > 10:
            return ReflexivityPhase.APPROACHING_CLIMAX, "恐慌加速 → 可能接近底部"
        return ReflexivityPhase.SELF_REINFORCING, "恐慌中 → 抛售自我强化"

    if narrative == MarketNarrative.COMPLACENCY:
        if pe_pct is not None and pe_pct > 85:
            return ReflexivityPhase.APPROACHING_CLIMAX, f"自满 + 估值{pe_pct:.0f}百分位 → 接近顶点"
        return ReflexivityPhase.LATE_STAGE, "自满 → 趋势已延伸很久"

    if narrative == MarketNarrative.RISK_ON:
        if spread_change is not None and spread_change < -0.5:
            return ReflexivityPhase.SELF_REINFORCING, "risk on + 利差收窄 → 自我强化"
        if eq_mom is not None and eq_mom > 25:
            return ReflexivityPhase.LATE_STAGE, f"股票涨{eq_mom:.0f}% → 已延伸"
        return ReflexivityPhase.EARLY_TREND, "risk on 初期"

    if narrative == MarketNarrative.RISK_OFF:
        if spread_change is not None and spread_change > 1.0:
            return ReflexivityPhase.SELF_REINFORCING, "risk off + 利差扩大 → 自我强化"
        return ReflexivityPhase.EARLY_TREND, "避险初期"

    # VIX 方向反转
    if vix_change is not None:
        if vix_change < -5 and vix is not None and vix > 25:
            return ReflexivityPhase.REVERSAL, "VIX 从高位回落 → 恐慌在消退"
        if vix_change > 5 and vix is not None and vix < 20:
            return ReflexivityPhase.REVERSAL, "VIX 从低位急升 → 自满在瓦解"

    return ReflexivityPhase.NEUTRAL, "无明显反身性趋势"


# ── 过度延伸检测 ────────────────────────────────────────────


def _compute_overextension(market: MarketState) -> dict[str, float]:
    """每个资产被市场推到了多极端的位置。

    动量极端 + 估值极端 = 过度延伸
    输出: -1(极度悲观) 到 +1(极度乐观)
    """
    overext: dict[str, float] = {}

    if market.momentum_equity is not None:
        eq = market.momentum_equity / 40  # ±40% → ±1
        if market.equity_pe_percentile is not None:
            val = (market.equity_pe_percentile - 50) / 50
            eq = eq * 0.5 + val * 0.5
        overext["equity_cyclical"] = max(-1, min(1, eq))

    if market.momentum_long_bond is not None:
        overext["long_term_bond"] = max(-1, min(1, market.momentum_long_bond / 30))
        overext["intermediate_bond"] = overext["long_term_bond"] * 0.5

    if market.momentum_gold is not None:
        overext["gold"] = max(-1, min(1, market.momentum_gold / 30))

    if market.momentum_commodity is not None:
        overext["commodity"] = max(-1, min(1, market.momentum_commodity / 30))

    if market.momentum_em_bond is not None:
        overext["em_bond"] = max(-1, min(1, market.momentum_em_bond / 25))

    # VIX 修正: 自满时高估更危险，恐慌时低估更有机会
    if market.vix is not None:
        if market.vix < 14:
            for k, v in overext.items():
                if v > 0:
                    overext[k] = min(1.0, v * 1.3)
        elif market.vix > 30:
            for k, v in overext.items():
                if v < 0:
                    overext[k] = max(-1.0, v * 1.3)

    return overext


# ── 主入口 ──────────────────────────────────────────────────


def evaluate_soros(market: MarketState) -> SorosInsight:
    """索罗斯认知链主入口。

    纯市场数据 → 叙事 + 反身性阶段 + 过度延伸度
    不需要达利欧的输出。
    """
    result = SorosInsight()

    result.narrative, result.narrative_detail = _identify_narrative(market)
    result.evidence.append(f"叙事: {result.narrative.value} — {result.narrative_detail}")

    result.phase, result.phase_detail = _assess_reflexivity(market, result.narrative)
    result.evidence.append(f"阶段: {result.phase.value} — {result.phase_detail}")

    result.overextension = _compute_overextension(market)
    for asset, ext in sorted(result.overextension.items(), key=lambda x: -abs(x[1])):
        if abs(ext) > 0.3:
            direction = "高估" if ext > 0 else "低估"
            result.evidence.append(f"{asset}: {direction} {abs(ext):.0%}")

    data_count = sum(1 for v in [
        market.momentum_equity, market.vix, market.credit_spread_hy,
        market.equity_pe_percentile, market.momentum_long_bond,
    ] if v is not None)
    result.confidence = min(1.0, data_count / 5)

    return result


# ── 格式化 ──────────────────────────────────────────────────


def format_soros(result: SorosInsight) -> str:
    """格式化索罗斯认知报告。"""
    lines = [""]
    lines.append("  索罗斯反身性认知")
    lines.append("  ════════════════════════════════════════════════")

    narrative_labels = {
        MarketNarrative.RISK_ON: "风险偏好 (Risk On)",
        MarketNarrative.RISK_OFF: "风险厌恶 (Risk Off)",
        MarketNarrative.INFLATION_FEAR: "通胀恐惧",
        MarketNarrative.DEFLATION_FEAR: "通缩恐惧",
        MarketNarrative.COMPLACENCY: "市场自满",
        MarketNarrative.PANIC: "市场恐慌",
        MarketNarrative.AMBIGUOUS: "不明确",
    }
    phase_labels = {
        ReflexivityPhase.EARLY_TREND: "早期趋势",
        ReflexivityPhase.SELF_REINFORCING: "自我强化中",
        ReflexivityPhase.LATE_STAGE: "晚期延伸",
        ReflexivityPhase.APPROACHING_CLIMAX: "接近极端",
        ReflexivityPhase.REVERSAL: "正在反转",
        ReflexivityPhase.NEUTRAL: "中性",
    }

    lines.append(f"\n  叙事: {narrative_labels.get(result.narrative, '?')}")
    lines.append(f"    {result.narrative_detail}")
    lines.append(f"  阶段: {phase_labels.get(result.phase, '?')}")
    lines.append(f"    {result.phase_detail}")

    if result.overextension:
        lines.append(f"\n  过度延伸:")
        for asset, ext in sorted(result.overextension.items(), key=lambda x: -abs(x[1])):
            bar = "+" * int(max(ext * 10, 0)) + "-" * int(max(-ext * 10, 0))
            direction = "高估" if ext > 0 else "低估"
            lines.append(f"    {asset:25s} {ext:+.2f} ({direction}) {bar}")

    lines.append(f"\n  置信度: {result.confidence:.0%}")
    lines.append("")
    return "\n".join(lines)
