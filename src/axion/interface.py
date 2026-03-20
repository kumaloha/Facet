"""
Polaris → Axion 接口定义
========================

Polaris（认知层）的完整输出 → Axion（执行层）的输入。

分层结构:
  Layer 1: 候选池（巴菲特链筛选的标的）
  Layer 2: 全天候底仓（ERC 权重）+ Pure Alpha 偏移（因果引擎）+ 对冲规格
  Layer 3: Alpha 机会（索罗斯链的市场偏差）
  Layer 4: 最终组合配置（三层合并后的权重）
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class CandidateStock:
    """巴菲特链筛选的候选标的。"""
    ticker: str
    company_name: str
    intrinsic_value: float | None = None
    current_price: float | None = None
    margin_of_safety: float | None = None
    buffett_signal: str = ""                    # "值得持有" / "观望" / "不值得持有"


@dataclass
class HedgeInstruction:
    """对冲指令（从 DalioResult.HedgeSpec 转换）。"""
    protects_against: str                       # 保护什么情景
    max_acceptable_loss: float                  # 该情景下可接受最大亏损 (%)
    suggested_instruments: list[str] = field(default_factory=list)  # 建议工具
    urgency: str = "normal"                     # "urgent" / "normal" / "optional"


@dataclass
class RebalanceInstruction:
    """再平衡指令。"""
    asset: str
    current_weight: float
    target_weight: float
    action: str                                 # "buy" / "sell"
    size_pct: float                             # 交易量占组合 (%)


@dataclass
class AxionInput:
    """Polaris → Axion 的完整输出。

    Axion 接收此结构后执行:
    1. 在候选池内筛选具体买卖标的
    2. 按最终组合配置执行再平衡
    3. 按对冲指令选择工具并执行
    4. 监控 decay_triggers，触发时自动调整
    """
    # Layer 1: 候选池
    candidates: list[CandidateStock] = field(default_factory=list)

    # Layer 2: 资产配置
    all_weather_weights: dict[str, float] = field(default_factory=dict)
    final_weights: dict[str, float] = field(default_factory=dict)

    # Layer 3: 对冲
    hedge_instructions: list[HedgeInstruction] = field(default_factory=list)

    # Layer 4: 再平衡
    rebalance_instructions: list[RebalanceInstruction] = field(default_factory=list)

    # 上下文
    regime: str = ""                            # 当前周期阶段
    regime_confidence: float = 0.0
    alpha_opportunities_count: int = 0
    snapshot_date: str = ""

    # 监控触发条件
    decay_triggers: list[str] = field(default_factory=list)


def format_axion_input(inp: AxionInput) -> str:
    """格式化 Axion 输入。"""
    lines = [""]
    lines.append("  Polaris → Axion 交接")
    lines.append("  ════════════════════════════════════════════════")
    lines.append(f"  周期: {inp.regime} (置信度 {inp.regime_confidence:.0%})")
    lines.append(f"  日期: {inp.snapshot_date}")

    if inp.candidates:
        lines.append(f"\n  候选池 ({len(inp.candidates)} 只):")
        for c in inp.candidates[:5]:
            mos = f"安全边际 {c.margin_of_safety:.0%}" if c.margin_of_safety else ""
            lines.append(f"    {c.ticker:8s} {c.buffett_signal:10s} {mos}")

    lines.append(f"\n  资产配置:")
    for asset, w in sorted(inp.final_weights.items(), key=lambda x: -x[1]):
        base = inp.all_weather_weights.get(asset, 0)
        offset = w - base
        lines.append(f"    {asset:25s}: {w:.1%} (底仓{base:.1%} {offset:+.1%})")

    if inp.hedge_instructions:
        lines.append(f"\n  对冲指令 ({len(inp.hedge_instructions)}):")
        for h in inp.hedge_instructions:
            lines.append(f"    [{h.urgency}] {h.protects_against[:50]}")
            lines.append(f"      最大亏损: {h.max_acceptable_loss:.0%}")

    if inp.rebalance_instructions:
        lines.append(f"\n  再平衡 ({len(inp.rebalance_instructions)}):")
        for r in inp.rebalance_instructions:
            lines.append(f"    {r.action:4s} {r.asset:20s} {r.size_pct:.1%}")

    if inp.decay_triggers:
        lines.append(f"\n  监控触发条件:")
        for t in inp.decay_triggers[:3]:
            lines.append(f"    ⚡ {t}")

    lines.append("")
    return "\n".join(lines)
