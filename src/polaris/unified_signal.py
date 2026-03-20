"""
Polaris 统一信号输出
====================

三条评估线 + 五大力量地基 → 分层信号 → Axion 执行

架构:
  五大力量 (共享地基)
    ├→ 评估生意 (公司) → 选什么 (WHAT)
    ├→ 评估资产 (类别) → 配多少 (HOW MUCH)
    └→ 评估偏差 (人性) → 什么时候动 (WHEN)

信号分三个时间层:
  长期层 (年度审视)  → Axion 战略底仓
  中期层 (季度审视)  → Axion 战术叠加
  短期层 (不交易)    → Axion 仅监控
"""

from __future__ import annotations

from dataclasses import dataclass, field


# ══════════════════════════════════════════════════════════════
#  分层信号: 每个资产在三个时间层的指令
# ══════════════════════════════════════════════════════════════


@dataclass
class LongTermSignal:
    """长期层: 战略底仓 (年度审视, 大周期不变则不动)

    来源: 评估资产(大周期) + 评估生意(护城河)
    """
    direction: str = "neutral"    # overweight / underweight / neutral
    weight: float = 0.0           # 底仓权重 (0-1)
    reason: str = ""
    source: str = ""              # "big_cycle" / "business" / "all_weather"
    # 评估生意输出 (仅个股有)
    business_quality: float | None = None     # 0-1
    intrinsic_value_gap: float | None = None  # 正=低估%
    moat: str | None = None                   # "wide"/"narrow"/"none"


@dataclass
class MediumTermSignal:
    """中期层: 战术叠加 (季度审视, 主要矛盾/偏差驱动)

    来源: 评估资产(主要矛盾+场景) + 评估偏差(泡沫阶段+反身性)
    """
    direction: str = "neutral"    # overweight / underweight / neutral
    delta: float = 0.0            # 在底仓上 +/- 多少
    reason: str = ""
    source: str = ""              # "contradiction" / "scenario" / "belief_gap" / "reflexivity"
    # 评估资产输出
    cycle_valuation: float = 0.0  # -1(极贵) 到 +1(极便宜)
    driving_forces: list[str] = field(default_factory=list)  # ["F5+", "F3-"]
    # 评估偏差输出
    belief_gap: float = 0.0       # 市场 vs 现实
    bubble_phase: str = ""        # early/reinforcing/late/climax/reversal
    fragility: float = 0.0        # 0-1
    action_timing: str = ""       # ride / hedge / snipe / wait


@dataclass
class ShortTermSignal:
    """短期层: 不交易, 只监控 (日度检查触发条件)

    来源: 评估偏差(格局改变事件)
    """
    action: str = "hold"          # hold / alert / regime_change
    triggers: list[str] = field(default_factory=list)
    alert_reason: str = ""


@dataclass
class LayeredSignal:
    """一个资产的三层信号 — 不矛盾, 叠加执行。"""
    asset: str                    # "equity_cyclical" / "gold" / "NVDA"
    asset_type: str = "class"     # "class" (资产类) / "stock" (个股)

    long_term: LongTermSignal = field(default_factory=LongTermSignal)
    medium_term: MediumTermSignal = field(default_factory=MediumTermSignal)
    short_term: ShortTermSignal = field(default_factory=ShortTermSignal)

    @property
    def effective_weight(self) -> float:
        """最终权重 = 底仓 + 战术叠加。短期层不改权重。"""
        return max(0, self.long_term.weight + self.medium_term.delta)

    @property
    def conviction(self) -> float:
        """综合信心: 三层共振时最高。"""
        long_conf = 0.5 if self.long_term.direction != "neutral" else 0.2
        med_conf = abs(self.medium_term.delta) * 2
        # 同向共振加分
        long_dir = 1 if self.long_term.direction == "overweight" else (-1 if self.long_term.direction == "underweight" else 0)
        med_dir = 1 if self.medium_term.delta > 0 else (-1 if self.medium_term.delta < 0 else 0)
        resonance = 0.2 if long_dir == med_dir and long_dir != 0 else 0
        return min(1.0, long_conf + med_conf + resonance)


@dataclass
class UnifiedOutput:
    """Polaris → Axion 的完整输出。"""
    # 所有资产的分层信号
    signals: list[LayeredSignal] = field(default_factory=list)

    # 五大力量摘要 (给人看)
    forces_summary: str = ""
    principal_contradiction: str = ""

    # 全局状态
    big_cycle_position: list[str] = field(default_factory=list)  # ["debt_late", "tech_revolution"]
    regime_change_watch: list[str] = field(default_factory=list)
    overall_fragility: float = 0.0

    snapshot_date: str = ""

    def get_signal(self, asset: str) -> LayeredSignal | None:
        for s in self.signals:
            if s.asset == asset:
                return s
        return None


# ══════════════════════════════════════════════════════════════
#  构建统一信号
# ══════════════════════════════════════════════════════════════


def build_unified_output(
    # 五大力量 (地基)
    forces_view=None,
    contradiction_analysis=None,
    cycle_constraints=None,
    # 评估生意
    business_candidates: list[dict] | None = None,
    # 评估资产
    all_weather_weights: dict[str, float] | None = None,
    big_cycle_tilts: dict[str, float] | None = None,
    scenario_asset_impacts: list | None = None,
    # 评估偏差
    soros_insight=None,
) -> UnifiedOutput:
    """从三条评估线构建统一信号。"""
    output = UnifiedOutput()

    aw = all_weather_weights or {}
    tilts = big_cycle_tilts or {}

    # ── 五大力量摘要 ──
    if forces_view:
        from polaris.chains.dalio_forces import ForceDirection
        dirs = {
            ForceDirection.STRONGLY_POSITIVE: "▲▲",
            ForceDirection.POSITIVE: "▲",
            ForceDirection.NEUTRAL: "─",
            ForceDirection.NEGATIVE: "▼",
            ForceDirection.STRONGLY_NEGATIVE: "▼▼",
        }
        parts = []
        for f in forces_view.forces:
            parts.append(f"F{f.force_id}{dirs[f.effective_direction]}")
        output.forces_summary = " ".join(parts)

    if contradiction_analysis and contradiction_analysis.principal:
        p = contradiction_analysis.principal
        output.principal_contradiction = (
            f"F{p.force_id}({p.force_name}) score={p.score:.3f}"
        )

    # ── 大周期位置 ──
    from polaris.chains.dalio_simulation import identify_long_cycle_position
    if tilts or aw:
        # 从 tilts 的来源推断
        output.big_cycle_position = list(tilts.keys()) if isinstance(next(iter(tilts.values()), None), dict) else []

    # ── 为每个资产类构建分层信号 ──
    ALL_ASSETS = [
        "equity", "long_term_bond", "intermediate_bond",
        "commodity", "gold", "inflation_linked_bond", "em_bond",
    ]

    # 因果图资产名 → 回测资产名
    CAUSAL_TO_ASSET = {
        "equity_cyclical": "equity",
        "equity_defensive": "equity",
        "long_term_bond": "long_term_bond",
        "intermediate_bond": "intermediate_bond",
        "commodity": "commodity",
        "gold": "gold",
        "inflation_linked_bond": "inflation_linked_bond",
        "em_bond": "em_bond",
    }

    for asset in ALL_ASSETS:
        sig = LayeredSignal(asset=asset, asset_type="class")

        # ── 长期层: All Weather 底仓 + 大周期倾斜 ──
        base = aw.get(asset, 0)
        tilt = tilts.get(asset, 0)
        sig.long_term.weight = base + tilt
        if tilt > 0.01:
            sig.long_term.direction = "overweight"
            sig.long_term.reason = f"大周期倾斜 +{tilt:.1%}"
        elif tilt < -0.01:
            sig.long_term.direction = "underweight"
            sig.long_term.reason = f"大周期倾斜 {tilt:.1%}"
        else:
            sig.long_term.direction = "neutral"
            sig.long_term.reason = "All Weather 基准"
        sig.long_term.source = "big_cycle" if abs(tilt) > 0.01 else "all_weather"

        # ── 中期层: 场景推演 + 索罗斯偏差 ──

        # 来源1: 评估资产 (场景推演 → 资产影响)
        if scenario_asset_impacts:
            for ai in scenario_asset_impacts:
                mapped = CAUSAL_TO_ASSET.get(ai.asset_type, ai.asset_type)
                if mapped == asset:
                    if ai.direction == "overweight":
                        sig.medium_term.delta += ai.raw_score * 0.1
                    else:
                        sig.medium_term.delta -= abs(ai.raw_score) * 0.1
                    sig.medium_term.source = "scenario"

        # 来源2: 评估偏差 (索罗斯交易信号)
        if soros_insight and soros_insight.trade_signal:
            ts = soros_insight.trade_signal
            ride = ts.ride_assets.get(asset, 0)
            hedge = ts.hedge_assets.get(asset, 0)

            if ride > 0.01:
                sig.medium_term.delta += ride * 0.08  # 缩放到合理范围
                sig.medium_term.action_timing = ts.action.value
            if hedge > 0.01:
                # 避险资产的对冲是做多
                if asset in ("gold", "long_term_bond", "intermediate_bond"):
                    sig.medium_term.delta += hedge * 0.05
                else:
                    sig.medium_term.delta -= hedge * 0.05
                sig.medium_term.action_timing = ts.action.value

            sig.medium_term.bubble_phase = soros_insight.phase.value
            if soros_insight.reflexivity_feedback:
                sig.medium_term.fragility = soros_insight.reflexivity_feedback.fragility

            # 偏差信息
            if soros_insight.biggest_gap:
                sig.medium_term.belief_gap = soros_insight.biggest_gap.gap

        sig.medium_term.delta = round(sig.medium_term.delta, 4)
        if sig.medium_term.delta > 0.005:
            sig.medium_term.direction = "overweight"
        elif sig.medium_term.delta < -0.005:
            sig.medium_term.direction = "underweight"
        if not sig.medium_term.source:
            sig.medium_term.source = "belief_gap" if soros_insight else ""

        # 主要矛盾驱动力
        if contradiction_analysis and contradiction_analysis.principal:
            p = contradiction_analysis.principal
            sig.medium_term.driving_forces = [f"F{p.force_id}{'+' if p.score > 0 else '-'}"]

        # ── 短期层: 仅监控 ──
        sig.short_term.action = "hold"
        if soros_insight and soros_insight.trade_signal:
            sig.short_term.triggers = list(soros_insight.trade_signal.reversal_triggers)

        output.signals.append(sig)

    # ── 个股信号 (评估生意) ──
    if business_candidates:
        for cand in business_candidates:
            sig = LayeredSignal(
                asset=cand.get("ticker", ""),
                asset_type="stock",
            )
            sig.long_term.direction = "overweight" if cand.get("signal") == "值得持有" else "neutral"
            sig.long_term.weight = 0.0  # 个股权重由 Axion 分配
            sig.long_term.business_quality = cand.get("quality", 0)
            sig.long_term.intrinsic_value_gap = cand.get("margin_of_safety", 0)
            sig.long_term.moat = cand.get("moat", "none")
            sig.long_term.reason = f"护城河: {sig.long_term.moat}"
            sig.long_term.source = "business"
            output.signals.append(sig)

    # ── 全局 ──
    if soros_insight and soros_insight.reflexivity_feedback:
        output.overall_fragility = soros_insight.reflexivity_feedback.fragility
    if soros_insight and soros_insight.trade_signal:
        output.regime_change_watch = [
            t for t in soros_insight.trade_signal.reversal_triggers
        ]

    return output


# ══════════════════════════════════════════════════════════════
#  格式化
# ══════════════════════════════════════════════════════════════


def format_unified_output(output: UnifiedOutput) -> str:
    """格式化统一信号。"""
    lines = [""]
    lines.append("  Polaris → Axion 统一信号")
    lines.append("  ════════════════════════════════════════════════")

    if output.forces_summary:
        lines.append(f"  五大力量: {output.forces_summary}")
    if output.principal_contradiction:
        lines.append(f"  主要矛盾: {output.principal_contradiction}")
    if output.overall_fragility > 0:
        fbar = "█" * int(output.overall_fragility * 10) + "░" * (10 - int(output.overall_fragility * 10))
        lines.append(f"  系统脆弱性: [{fbar}] {output.overall_fragility:.0%}")

    # 资产类信号
    class_signals = [s for s in output.signals if s.asset_type == "class"]
    if class_signals:
        lines.append(f"\n  ── 资产配置 ──")
        lines.append(f"  {'资产':20s} {'底仓':>6s} {'叠加':>7s} {'最终':>6s} {'长期':8s} {'中期':10s} {'泡沫':8s}")
        lines.append(f"  {'─' * 72}")

        for s in sorted(class_signals, key=lambda x: -x.effective_weight):
            long_dir = {"overweight": "↑", "underweight": "↓", "neutral": "─"}[s.long_term.direction]
            med_dir = {"overweight": "↑", "underweight": "↓", "neutral": "─"}[s.medium_term.direction]
            phase = s.medium_term.bubble_phase[:8] if s.medium_term.bubble_phase else "─"
            lines.append(
                f"  {s.asset:20s} "
                f"{s.long_term.weight:5.1%} "
                f"{s.medium_term.delta:+6.2%} "
                f"{s.effective_weight:5.1%} "
                f"{long_dir} {s.long_term.reason[:6]:6s} "
                f"{med_dir} {s.medium_term.source[:8]:8s} "
                f"{phase}"
            )

    # 个股信号
    stock_signals = [s for s in output.signals if s.asset_type == "stock"]
    if stock_signals:
        lines.append(f"\n  ── 个股 (评估生意) ──")
        for s in stock_signals:
            moat = s.long_term.moat or "?"
            gap = s.long_term.intrinsic_value_gap
            gap_str = f"{gap:+.0f}%" if gap else "?"
            lines.append(f"  {s.asset:8s} 护城河:{moat:6s} 安全边际:{gap_str:5s} {s.long_term.direction}")

    # 监控
    if output.regime_change_watch:
        lines.append(f"\n  ── 短期: 监控(不交易) ──")
        for t in output.regime_change_watch[:4]:
            lines.append(f"  · {t}")

    lines.append("")
    return "\n".join(lines)
