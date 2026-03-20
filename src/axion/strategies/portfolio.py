"""
组合构建: 全天候底仓 + Pure Alpha 偏移 + 索罗斯 Alpha
=================================================

最终持仓 = Layer 1 (全天候) + Layer 2 (因果引擎偏移) + Layer 3 (市场偏差 Alpha)

约束:
- 任何资产不超过 50%（集中度限制）
- 任何资产不低于 0%（不做空，只超配/低配）
- Pure Alpha 偏移不超过底仓的 50%（不过度偏离）
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class PortfolioAllocation:
    """单个资产的最终配置。"""
    asset: str
    base_weight: float         # 全天候底仓
    alpha_offset: float        # Pure Alpha 偏移
    soros_offset: float        # 索罗斯 Alpha 叠加
    final_weight: float        # 最终权重
    explanation: str = ""      # 为什么这么配


@dataclass
class PortfolioResult:
    """完整组合输出。"""
    allocations: list[PortfolioAllocation]
    total_alpha_offset: float = 0.0
    total_soros_offset: float = 0.0
    effective_risk_budget: dict[str, float] = field(default_factory=dict)


def build_portfolio(
    all_weather_weights: dict[str, float],
    dalio_tilts: list | None = None,
    soros_opportunities: list | None = None,
    leverage: float = 1.0,
    allow_short: bool = False,
    max_offset_ratio: float = 0.5,
    max_single_weight: float = 0.50,
    min_single_weight: float = 0.00,
) -> PortfolioResult:
    """构建最终组合。

    Args:
        all_weather_weights: 全天候底仓 (ERC 权重)
        dalio_tilts: Pure Alpha 的 Tilt 列表
        soros_opportunities: 索罗斯 Alpha 机会列表
        max_offset_ratio: 单个资产偏移不超过底仓的 X%
        max_single_weight: 单个资产最大权重
        min_single_weight: 单个资产最小权重
    """
    # 底仓
    weights = {a: w for a, w in all_weather_weights.items()}
    explanations: dict[str, list[str]] = {a: [f"底仓 {w:.1%}"] for a, w in weights.items()}

    # Layer 2: Pure Alpha 偏移
    alpha_offsets: dict[str, float] = {}
    if dalio_tilts:
        for tilt in dalio_tilts:
            asset = tilt.asset_type
            if asset not in weights:
                continue

            base = weights[asset]
            max_offset = base * max_offset_ratio

            if tilt.direction == "overweight":
                offset = min(tilt.magnitude * base, max_offset)
            else:
                offset = -min(tilt.magnitude * base, max_offset)

            alpha_offsets[asset] = offset
            weights[asset] = weights[asset] + offset
            explanations.setdefault(asset, []).append(
                f"Alpha {'+' if offset > 0 else ''}{offset:.1%} ({tilt.thesis[:40]})"
            )

    # Layer 3: 索罗斯 Alpha（更保守，只用高信心机会）
    soros_offsets: dict[str, float] = {}
    if soros_opportunities:
        for opp in soros_opportunities:
            if opp.conviction < 0.5:
                continue  # 只用高信心机会

            asset = opp.asset_type
            if asset not in weights:
                continue

            base = all_weather_weights.get(asset, 0.05)
            max_soros = base * 0.3  # 索罗斯偏移更保守

            if opp.direction == "overweight":
                offset = min(opp.conviction * base * 0.3, max_soros)
            else:
                offset = -min(opp.conviction * base * 0.3, max_soros)

            soros_offsets[asset] = soros_offsets.get(asset, 0) + offset
            weights[asset] = weights[asset] + offset
            explanations.setdefault(asset, []).append(
                f"Soros {'+' if offset > 0 else ''}{offset:.1%} ({opp.thesis[:40]})"
            )

    # 杠杆: 放大所有仓位
    if leverage != 1.0:
        weights = {k: v * leverage for k, v in weights.items()}

    # 约束
    min_w = -max_single_weight if allow_short else min_single_weight
    for asset in weights:
        weights[asset] = max(min_w, min(max_single_weight, weights[asset]))

    # 归一化: 多头总权重 = leverage（不是 1.0）
    long_total = sum(v for v in weights.values() if v > 0)
    if long_total > 0:
        target_long = leverage
        scale = target_long / long_total
        weights = {k: v * scale if v > 0 else v for k, v in weights.items()}

    # 构建输出
    allocations = []
    for asset in sorted(weights.keys(), key=lambda x: -weights[x]):
        allocations.append(PortfolioAllocation(
            asset=asset,
            base_weight=all_weather_weights.get(asset, 0),
            alpha_offset=alpha_offsets.get(asset, 0),
            soros_offset=soros_offsets.get(asset, 0),
            final_weight=round(weights[asset], 4),
            explanation=" → ".join(explanations.get(asset, [])),
        ))

    return PortfolioResult(
        allocations=allocations,
        total_alpha_offset=sum(abs(v) for v in alpha_offsets.values()),
        total_soros_offset=sum(abs(v) for v in soros_offsets.values()),
    )


def format_portfolio(result: PortfolioResult) -> str:
    """格式化组合报告。"""
    lines = [""]
    lines.append("  最终组合配置")
    lines.append("  ════════════════════════════════════════════════")
    lines.append(f"  Alpha 总偏移: {result.total_alpha_offset:.1%}  Soros 总偏移: {result.total_soros_offset:.1%}")
    lines.append("")
    lines.append(f"  {'资产':20s} {'底仓':>6s} {'Alpha':>7s} {'Soros':>7s} {'最终':>6s}")
    lines.append(f"  {'-'*55}")
    for a in result.allocations:
        lines.append(
            f"  {a.asset:20s} {a.base_weight:5.1%} {a.alpha_offset:+6.1%} {a.soros_offset:+6.1%} {a.final_weight:5.1%}"
        )
        if a.explanation:
            lines.append(f"    {a.explanation[:70]}")
    lines.append("")
    return "\n".join(lines)
