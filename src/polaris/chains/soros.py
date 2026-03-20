"""
索罗斯反身性认知链
==================

独立的认知框架——不依赖达利欧的输出，但可以和达利欧对比。

索罗斯的核心: 市场参与者的认知本身改变了现实。
他不预测经济，他观察市场行为，判断偏差何时会自我修正。

v2 叙事系统:
  旧版: 6 个硬编码桶 (risk_on/off 等) — 无法表达 "AI泡沫 vs 关税"
  新版: 叙事 = 市场对五大力量的定价。从资产价格反推市场在押注哪些力量。
        与达利欧的真实力量对比 → 偏差 = 反身性机会

输入: 纯市场数据（价格、波动率、利差、动量）
输出:
  1. 市场信念: 市场在押注哪些力量 (从价格反推)
  2. 现实偏差: 市场信念 vs 达利欧真实力量 → 哪里有错误定价
  3. 反身性阶段: 这个偏差在自我强化还是接近修正
  4. 过度延伸: 每个资产被推到了多极端的位置
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

    # ── 时间维度 (反身性阶段判断用) ──
    # 趋势持续: 连续几个月主趋势维持方向
    trend_duration_months: int | None = None        # 主要趋势持续了几个月
    # 加速度: 短期动量 vs 长期动量 → 趋势在加速还是减速
    momentum_equity_3m: float | None = None          # 股票 3 个月动量 (%)
    momentum_equity_6m: float | None = None          # 股票 6 个月动量 (%)
    # 信用市场趋势
    credit_spread_hy_6m_ago: float | None = None     # 6 个月前的高收益利差
    # VIX 趋势
    vix_6m_ago: float | None = None                  # 6 个月前的 VIX

    # 快照时间
    snapshot_date: str = ""


# ── 市场信念 (从价格反推市场在押注什么) ────────────────────


@dataclass
class MarketBelief:
    """市场对一个力量的定价——从资产价格反推出来的。

    正值 = 市场认为这个力量是利好的（或者忽视了它的利空）
    负值 = 市场认为这个力量是利空的（或者过度恐惧了）
    """
    force_id: int
    force_name: str
    priced_direction: float = 0.0   # -1 到 +1: 市场定价的方向
    pricing_evidence: list[str] = field(default_factory=list)


@dataclass
class RealityGap:
    """市场信念 vs 现实的偏差——索罗斯找的就是这个。"""
    force_id: int
    force_name: str
    market_prices: float        # 市场定价 (-1 到 +1)
    reality: float              # 达利欧五力真实方向 (-1 到 +1)
    gap: float                  # market_prices - reality (正=市场过度乐观)
    gap_detail: str = ""
    trade_implication: str = "" # 如果偏差修正，该怎么做


class ReflexivityPhase(str, Enum):
    """反身性阶段。"""
    EARLY_TREND = "early_trend"
    SELF_REINFORCING = "self_reinforcing"
    LATE_STAGE = "late_stage"
    APPROACHING_CLIMAX = "approaching_climax"
    REVERSAL = "reversal"
    NEUTRAL = "neutral"


# 保留旧枚举兼容
class MarketNarrative(str, Enum):
    RISK_ON = "risk_on"
    RISK_OFF = "risk_off"
    INFLATION_FEAR = "inflation_fear"
    DEFLATION_FEAR = "deflation_fear"
    COMPLACENCY = "complacency"
    PANIC = "panic"
    AMBIGUOUS = "ambiguous"


@dataclass
class SorosInsight:
    """索罗斯认知链的输出。"""
    # v2: 市场信念 (从价格反推)
    beliefs: list[MarketBelief] = field(default_factory=list)
    narrative_summary: str = ""    # 一句话概括市场在讲什么故事

    # v2: 现实偏差 (vs 达利欧)
    reality_gaps: list[RealityGap] = field(default_factory=list)
    biggest_gap: RealityGap | None = None

    # v1 兼容
    narrative: MarketNarrative = MarketNarrative.AMBIGUOUS
    narrative_detail: str = ""

    phase: ReflexivityPhase = ReflexivityPhase.NEUTRAL
    phase_detail: str = ""

    # 每个资产的过度延伸度 (-1 到 +1)
    overextension: dict[str, float] = field(default_factory=dict)

    # v2: 反身性反馈 (市场价格→改变现实)
    reflexivity_feedback: "ReflexivityFeedback | None" = None

    # v2: 背离信号 (市场内部分裂)
    divergences: list["Divergence"] = field(default_factory=list)

    # v2: 交易信号 → Axion
    trade_signal: "SorosTradeSignal | None" = None

    confidence: float = 0.0
    evidence: list[str] = field(default_factory=list)


# ══════════════════════════════════════════════════════════════
#  v2: 从价格反推市场信念
# ══════════════════════════════════════════════════════════════
#
# 资产价格是市场对五大力量的投票:
#   股票涨+VIX低 → 市场不担心F1(债务)/F3(地缘), 押注F5(技术)
#   黄金涨+美元跌 → 市场在定价F1长周期(印钱) + F3(去美元化)
#   大宗涨+债跌 → 市场在定价通胀 (F3供给冲击 或 F4自然冲击)
#   全部跌 → 市场在定价加息风暴 (F1短周期紧缩)


def _infer_force_beliefs(market: MarketState) -> list[MarketBelief]:
    """从市场价格反推: 市场在押注哪些力量。"""
    beliefs = []
    eq = market.momentum_equity or 0
    bond = market.momentum_long_bond or 0
    gold = market.momentum_gold or 0
    comm = market.momentum_commodity or 0
    vix = market.vix or 20
    spread = market.credit_spread_hy or 4
    spread_chg = market.credit_spread_change_3m or 0
    pe_pct = market.equity_pe_percentile or 50

    # ── F1 债务/信贷: 信用利差 + 收益率曲线 ──
    # 利差收窄 = 市场认为信贷健康; 利差扩大 = 市场担心信贷
    f1_price = 0.0
    f1_evidence = []
    if spread < 3.5:
        f1_price += 0.3
        f1_evidence.append(f"利差{spread:.1f}%偏紧=市场不担心信贷")
    elif spread > 6:
        f1_price -= 0.5
        f1_evidence.append(f"利差{spread:.1f}%很宽=市场担心违约")
    if spread_chg < -0.5:
        f1_price += 0.2
        f1_evidence.append("利差在收窄")
    elif spread_chg > 0.5:
        f1_price -= 0.3
        f1_evidence.append("利差在扩大")
    # 收益率曲线
    yc = market.yield_curve_10y_3m
    if yc is not None:
        if yc < 0:
            f1_price -= 0.2
            f1_evidence.append(f"收益率曲线倒挂({yc:+.2f})=衰退信号")
        elif yc > 1:
            f1_price += 0.1
            f1_evidence.append(f"收益率曲线正常({yc:+.2f})")
    beliefs.append(MarketBelief(1, "债务/信贷", max(-1, min(1, f1_price)), f1_evidence))

    # ── F2 内部秩序: 消费股 vs 防御股 (用 VIX 近似) ──
    f2_price = 0.0
    f2_evidence = []
    if vix < 15:
        f2_price += 0.2
        f2_evidence.append(f"VIX={vix:.0f}低=市场不担心内部冲击")
    elif vix > 25:
        f2_price -= 0.2
        f2_evidence.append(f"VIX={vix:.0f}高=有不确定性")
    beliefs.append(MarketBelief(2, "内部秩序", max(-1, min(1, f2_price)), f2_evidence))

    # ── F3 外部秩序: 大宗 + 美元 + 新兴市场 ──
    f3_price = 0.0
    f3_evidence = []
    em = market.momentum_em_bond or 0
    if comm > 15:
        f3_price -= 0.3
        f3_evidence.append(f"大宗+{comm:.0f}%=可能有供给冲击/地缘")
    elif comm < -15:
        # 大宗暴跌: 需求崩塌(2008) 还是供给缓解?
        # 区分: 如果股票也在跌 → 需求崩塌 → 外部秩序也不好
        #        如果股票在涨 → 供给缓解 → 外部秩序改善
        if eq is not None and eq < -10:
            f3_price -= 0.2
            f3_evidence.append(f"大宗{comm:.0f}%+股{eq:.0f}%=需求崩塌(非供给缓解)")
        else:
            f3_price += 0.2
            f3_evidence.append(f"大宗{comm:.0f}%=供给压力缓解")
    if em < -10:
        f3_price -= 0.2
        f3_evidence.append(f"EM债{em:.0f}%=资本在逃离新兴市场")
    elif em > 10:
        f3_price += 0.2
        f3_evidence.append(f"EM债+{em:.0f}%=全球化风险偏好")
    beliefs.append(MarketBelief(3, "外部秩序", max(-1, min(1, f3_price)), f3_evidence))

    # ── F4 自然之力: 食品/能源大宗的异常波动 ──
    f4_price = 0.0
    f4_evidence = []
    if comm > 25:
        f4_price -= 0.2
        f4_evidence.append(f"大宗+{comm:.0f}%=可能有供给中断")
    else:
        f4_evidence.append("大宗正常=市场未定价自然冲击")
    beliefs.append(MarketBelief(4, "自然之力", max(-1, min(1, f4_price)), f4_evidence))

    # ── F5 技术/创造力: 股票(尤其科技)动量 + 估值 ──
    f5_price = 0.0
    f5_evidence = []
    if eq > 20:
        f5_price += 0.5
        f5_evidence.append(f"股票+{eq:.0f}%=市场押注增长/技术")
    elif eq > 10:
        f5_price += 0.3
        f5_evidence.append(f"股票+{eq:.0f}%=温和看好")
    elif eq < -10:
        f5_price -= 0.3
        f5_evidence.append(f"股票{eq:.0f}%=增长悲观")
    if pe_pct > 80:
        f5_price += 0.2
        f5_evidence.append(f"PE百分位{pe_pct:.0f}%=估值极高, 市场在赌增长持续")
    elif pe_pct < 20:
        f5_price -= 0.2
        f5_evidence.append(f"PE百分位{pe_pct:.0f}%=估值极低")
    if vix < 14 and eq > 20:
        f5_price += 0.2
        f5_evidence.append("低波动+高涨幅=自满性看多")
    beliefs.append(MarketBelief(5, "技术/创造力", max(-1, min(1, f5_price)), f5_evidence))

    return beliefs


def _compute_reality_gaps(
    beliefs: list[MarketBelief],
    force_directions: dict[int, float] | None = None,
) -> list[RealityGap]:
    """计算市场信念 vs 达利欧真实力量的偏差。

    force_directions: {force_id: -1到+1} 从达利欧五力评估转换来。
    如果没有传入，只返回市场信念，不计算偏差。
    """
    if not force_directions:
        return []

    gaps = []
    for belief in beliefs:
        reality = force_directions.get(belief.force_id, 0)
        gap = belief.priced_direction - reality

        detail = ""
        trade = ""
        if abs(gap) > 0.3:
            if gap > 0:
                detail = (
                    f"市场对F{belief.force_id}({belief.force_name})过度乐观: "
                    f"定价{belief.priced_direction:+.2f} vs 现实{reality:+.2f}"
                )
                trade = f"如果现实修正市场信念 → F{belief.force_id}相关资产将下跌"
            else:
                detail = (
                    f"市场对F{belief.force_id}({belief.force_name})过度悲观: "
                    f"定价{belief.priced_direction:+.2f} vs 现实{reality:+.2f}"
                )
                trade = f"如果市场发现现实没那么差 → F{belief.force_id}相关资产将反弹"

        gaps.append(RealityGap(
            force_id=belief.force_id,
            force_name=belief.force_name,
            market_prices=belief.priced_direction,
            reality=reality,
            gap=round(gap, 2),
            gap_detail=detail,
            trade_implication=trade,
        ))

    gaps.sort(key=lambda x: -abs(x.gap))
    return gaps


def _summarize_narrative(beliefs: list[MarketBelief]) -> str:
    """从市场信念生成一句话叙事摘要。"""
    bullish = [b for b in beliefs if b.priced_direction > 0.2]
    bearish = [b for b in beliefs if b.priced_direction < -0.2]

    parts = []
    if bullish:
        names = [b.force_name for b in sorted(bullish, key=lambda x: -x.priced_direction)]
        parts.append(f"押注 {'+'.join(names)}")
    if bearish:
        names = [b.force_name for b in sorted(bearish, key=lambda x: x.priced_direction)]
        parts.append(f"担心 {'+'.join(names)}")
    if not parts:
        return "市场无明确方向性押注"

    return "市场叙事: " + ", ".join(parts)


# ══════════════════════════════════════════════════════════════
#  v1 兼容: 旧版叙事识别
# ══════════════════════════════════════════════════════════════


def _identify_narrative(market: MarketState) -> tuple[MarketNarrative, str]:
    """从市场数据读出市场在讲什么故事 (v1 兼容)。"""
    eq = market.momentum_equity
    bond = market.momentum_long_bond
    gold = market.momentum_gold
    comm = market.momentum_commodity
    vix = market.vix

    if eq is None:
        return MarketNarrative.AMBIGUOUS, "数据不足"

    if vix is not None and vix > 35:
        return MarketNarrative.PANIC, f"VIX={vix:.0f} 恐慌水平"
    if vix is not None and vix < 13 and eq is not None and eq > 15:
        return MarketNarrative.COMPLACENCY, f"VIX={vix:.0f} + 股票涨{eq:.0f}% 市场自满"

    if eq is not None and bond is not None:
        if eq > 10 and bond > 5:
            return MarketNarrative.RISK_ON, f"股债双涨: 股+{eq:.0f}% 债+{bond:.0f}%"
        if eq < -5 and bond > 10:
            return MarketNarrative.RISK_OFF, f"避险: 股{eq:.0f}% 债+{bond:.0f}%"
        if eq < -10 and bond < -10:
            return MarketNarrative.PANIC, f"全面下跌: 股{eq:.0f}% 债{bond:.0f}%"

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
    """判断当前趋势处于反身性的哪个阶段。

    三个维度:
      1. 叙事方向: 牛/熊/恐慌/自满
      2. 时间持续: 趋势已经走了多久 (月)
      3. 加速度: 趋势在加速(自我强化中) 还是在减速(动力衰减)

    索罗斯的时间规律:
      牛市自我强化: 通常 12-36 个月
      熊市恐慌: 通常 3-12 个月 (比牛市短得多)
      接近顶部: 趋势持续很久 + 加速度变平/转负
      接近底部: 恐慌很猛 + VIX 开始从极端回落
    """
    eq_mom = market.momentum_equity
    vix = market.vix
    vix_change = market.vix_change_1m
    spread_change = market.credit_spread_change_3m
    pe_pct = market.equity_pe_percentile
    duration = market.trend_duration_months

    # ── 加速度计算 ──
    # 短期动量 vs 长期动量: 短>长=加速, 短<长=减速
    acceleration = None  # >0=加速, <0=减速
    accel_detail = ""
    mom_3m = market.momentum_equity_3m
    mom_12m = market.momentum_equity

    if mom_3m is not None and mom_12m is not None and mom_12m != 0:
        # 把 3 月动量年化后和 12 月比
        mom_3m_annualized = mom_3m * 4
        acceleration = mom_3m_annualized - mom_12m
        if acceleration > 10:
            accel_detail = f"加速中(3M年化{mom_3m_annualized:+.0f}% vs 12M{mom_12m:+.0f}%)"
        elif acceleration < -10:
            accel_detail = f"减速中(3M年化{mom_3m_annualized:+.0f}% vs 12M{mom_12m:+.0f}%)"
        else:
            accel_detail = "速度平稳"

    # VIX 趋势 (6 个月维度)
    vix_trend = None
    if vix is not None and market.vix_6m_ago is not None:
        vix_trend = vix - market.vix_6m_ago  # 正=恐惧在上升

    # 利差趋势 (6 个月维度)
    spread_trend = None
    if market.credit_spread_hy is not None and market.credit_spread_hy_6m_ago is not None:
        spread_trend = market.credit_spread_hy - market.credit_spread_hy_6m_ago

    # ── 恐慌模式 ──
    if narrative == MarketNarrative.PANIC:
        if vix_change is not None and vix_change > 10:
            return ReflexivityPhase.APPROACHING_CLIMAX, (
                f"恐慌加速(VIX月涨{vix_change:+.0f}) → 接近底部. "
                f"历史: 恐慌顶点通常VIX>40后1-3个月触底"
            )
        if duration is not None and duration > 6:
            return ReflexivityPhase.LATE_STAGE, (
                f"恐慌已持续{duration}月 → 接近尾声. "
                f"历史: 2008恐慌持续~15月, 2020仅2月"
            )
        return ReflexivityPhase.SELF_REINFORCING, "恐慌中 → 抛售自我强化"

    # ── 自满模式 ──
    if narrative == MarketNarrative.COMPLACENCY:
        time_info = f"已持续{duration}月" if duration else "持续时间未知"
        if pe_pct is not None and pe_pct > 85 and duration is not None and duration > 18:
            return ReflexivityPhase.APPROACHING_CLIMAX, (
                f"自满 + 估值{pe_pct:.0f}百分位 + {time_info} → 接近顶点. "
                f"历史: 2000年科技泡沫自满期约24月, 2007约18月"
            )
        if acceleration is not None and acceleration < -10 and duration is not None and duration > 12:
            return ReflexivityPhase.APPROACHING_CLIMAX, (
                f"自满但涨幅在收窄 + {time_info} → 动力衰减. "
                f"{accel_detail}"
            )
        if pe_pct is not None and pe_pct > 85:
            return ReflexivityPhase.LATE_STAGE, f"自满 + 估值极高 + {time_info}"
        return ReflexivityPhase.LATE_STAGE, f"自满 → {time_info}"

    # ── Risk On 模式 ──
    if narrative == MarketNarrative.RISK_ON:
        time_info = f"已持续{duration}月" if duration else ""

        # 自我强化: 涨 + 利差在收窄 + 持续<18月
        if spread_change is not None and spread_change < -0.5:
            if duration is not None and duration > 24:
                return ReflexivityPhase.LATE_STAGE, (
                    f"risk on 自我强化了{duration}月 + 利差仍收窄 → 晚期. "
                    f"历史: 2003-2007牛市自我强化约48月"
                )
            return ReflexivityPhase.SELF_REINFORCING, (
                f"risk on + 利差收窄 → 自我强化中. {time_info}"
            )

        # 加速度判断
        if acceleration is not None:
            if acceleration > 10:
                return ReflexivityPhase.SELF_REINFORCING, (
                    f"risk on + {accel_detail}. {time_info}"
                )
            if acceleration < -10 and duration is not None and duration > 12:
                return ReflexivityPhase.LATE_STAGE, (
                    f"risk on 但{accel_detail}. {time_info} → 动力在衰减"
                )

        if eq_mom is not None and eq_mom > 25:
            if duration is not None and duration > 18:
                return ReflexivityPhase.LATE_STAGE, (
                    f"股票+{eq_mom:.0f}% + {time_info} → 已充分延伸"
                )
            return ReflexivityPhase.SELF_REINFORCING, f"股票+{eq_mom:.0f}% {time_info}"

        return ReflexivityPhase.EARLY_TREND, f"risk on 初期. {time_info}"

    # ── Risk Off 模式 ──
    if narrative == MarketNarrative.RISK_OFF:
        time_info = f"已持续{duration}月" if duration else ""
        if spread_change is not None and spread_change > 1.0:
            return ReflexivityPhase.SELF_REINFORCING, f"risk off + 利差扩大 → 自我强化. {time_info}"
        if duration is not None and duration > 6:
            return ReflexivityPhase.LATE_STAGE, f"避险已{duration}月 → 可能接近尾声"
        return ReflexivityPhase.EARLY_TREND, f"避险初期. {time_info}"

    # ── 反转检测 ──
    if vix_change is not None:
        if vix_change < -5 and vix is not None and vix > 25:
            return ReflexivityPhase.REVERSAL, (
                f"VIX从高位回落(月降{abs(vix_change):.0f}) → 恐慌消退"
            )
        if vix_change > 5 and vix is not None and vix < 20:
            return ReflexivityPhase.REVERSAL, (
                f"VIX从低位急升(月涨{vix_change:.0f}) → 自满瓦解"
            )

    # 利差方向反转
    if spread_trend is not None:
        if spread_trend > 1.0 and market.credit_spread_hy_6m_ago is not None and market.credit_spread_hy_6m_ago < 4:
            return ReflexivityPhase.REVERSAL, (
                f"信用利差从{market.credit_spread_hy_6m_ago:.1f}%扩大到{market.credit_spread_hy:.1f}% → 信贷环境反转"
            )

    # ── 兜底: 用加速度+持续时间+估值综合判断 ──
    # 叙事桶没匹配上，但市场可能仍在某个趋势中
    if eq_mom is not None and duration is not None:
        time_info = f"持续{duration}月"
        # 牛市中（股票正动量）
        if eq_mom > 10:
            if acceleration is not None and acceleration < -10 and duration > 18:
                return ReflexivityPhase.APPROACHING_CLIMAX, (
                    f"牛市{time_info}但{accel_detail} → 动力衰减接近顶部"
                )
            if duration > 24 and pe_pct is not None and pe_pct > 80:
                return ReflexivityPhase.LATE_STAGE, (
                    f"牛市{time_info} + 估值{pe_pct:.0f}百分位 → 晚期"
                )
            if duration > 12:
                return ReflexivityPhase.SELF_REINFORCING, (
                    f"牛市{time_info} + 股票+{eq_mom:.0f}%. {accel_detail}"
                )
            return ReflexivityPhase.EARLY_TREND, f"牛市{time_info}"
        # 熊市中（股票负动量）
        if eq_mom < -10:
            if duration > 9:
                return ReflexivityPhase.LATE_STAGE, f"熊市{time_info} → 可能接近尾声"
            if acceleration is not None and acceleration < -10:
                return ReflexivityPhase.SELF_REINFORCING, f"熊市加速中. {accel_detail}"
            return ReflexivityPhase.EARLY_TREND, f"下跌趋势{time_info}"

    return ReflexivityPhase.NEUTRAL, "无明显反身性趋势"


# ── 过度延伸检测 ────────────────────────────────────────────


def _compute_overextension(market: MarketState) -> dict[str, float]:
    """每个资产被市场推到了多极端的位置。

    关键区分:
      风险资产 (股票/大宗/EM): 用动量+估值+贪婪度 → 正=高估,负=低估
      避险资产 (债/黄金): 逻辑不同
        - 债: 动量为主, 涨多了=高估(利率过低)
        - 黄金: 涨可能是合理的恐惧定价, 用动量vs恐惧水平判断

    输出: -1(极度悲观/低估) 到 +1(极度乐观/高估)
    """
    overext: dict[str, float] = {}

    # ── 贪婪/恐惧指标 ──
    greed_fear = 0.0
    if market.vix is not None:
        greed_fear += -(market.vix - 20) / 50
    if market.credit_spread_hy is not None:
        greed_fear += -(market.credit_spread_hy - 5) / 10
    greed_fear = max(-1, min(1, greed_fear))

    # ── 风险资产: 股票 ──
    if market.momentum_equity is not None:
        mom = market.momentum_equity / 40
        val = 0.0
        if market.equity_pe_percentile is not None:
            val = (market.equity_pe_percentile - 50) / 50
        eq = mom * 0.4 + val * 0.3 + greed_fear * 0.3
        overext["equity_cyclical"] = max(-1, min(1, eq))

    # ── 风险资产: 大宗 ──
    if market.momentum_commodity is not None:
        overext["commodity"] = max(-1, min(1, market.momentum_commodity / 30 * 0.7 + greed_fear * 0.3))

    # ── 风险资产: EM 债 ──
    if market.momentum_em_bond is not None:
        overext["em_bond"] = max(-1, min(1, market.momentum_em_bond / 25 * 0.6 + greed_fear * 0.4))

    # ── 避险资产: 长债 ──
    # 债的过度延伸 = 纯动量 (涨太多=过度定价避险,跌太多=过度定价紧缩)
    if market.momentum_long_bond is not None:
        overext["long_term_bond"] = max(-1, min(1, market.momentum_long_bond / 25))
        overext["intermediate_bond"] = overext["long_term_bond"] * 0.5

    # ── 避险资产: 黄金 ──
    # 黄金涨 + 恐惧高 = 合理的避险定价, 不算过度延伸
    # 黄金涨 + 恐惧低 = 过度乐观 (市场在追金价)
    if market.momentum_gold is not None:
        gold_mom = market.momentum_gold / 30
        # 恐惧合理度: 恐惧越高, 黄金涨越合理
        fear_justified = max(0, -greed_fear)  # 恐惧时为正
        # 过度延伸 = 涨幅 - 恐惧合理部分
        gold_excess = gold_mom - fear_justified * 0.5
        overext["gold"] = max(-1, min(1, gold_excess))

    # ── 时间修正: 只对风险资产加时间惩罚 ──
    duration = market.trend_duration_months
    if duration is not None and duration > 12:
        time_stretch = min((duration - 12) / 24, 0.3)
        for k in ["equity_cyclical", "commodity", "em_bond"]:
            if k in overext and overext[k] > 0:
                overext[k] = min(1.0, overext[k] + time_stretch)

    return overext


# ══════════════════════════════════════════════════════════════
#  动作信号: 认知 → Axion 该做什么
# ══════════════════════════════════════════════════════════════


class SorosAction(str, Enum):
    """索罗斯的操作模式。"""
    RIDE = "ride"                    # 跟骑趋势 (趋势自我强化中)
    RIDE_WITH_HEDGE = "ride_hedge"   # 跟骑但加对冲 (晚期, 可能快到头了)
    PREPARE_SNIPE = "prepare_snipe"  # 准备狙击 (接近极端, 开始建立反向头寸)
    SNIPE = "snipe"                  # 狙击 (反转确认, 全力反向)
    WAIT = "wait"                    # 等待 (没有明确信号)


@dataclass
class SorosTradeSignal:
    """索罗斯链 → Axion 的具体交易信号。

    分两层:
      主仓位 (顺势): 跟骑趋势方向的资产
      对冲仓位 (逆势): 针对偏差的保护/狙击头寸

    两层不矛盾——索罗斯同时持有顺势仓位和反向保护。
    核心理念: "跟骑泡沫赚钱, 但买保险防崩盘"
    """
    action: SorosAction = SorosAction.WAIT
    action_detail: str = ""

    # ── 主仓位: 跟骑趋势 ──
    ride_conviction: float = 0.0    # 0(不跟) 到 1(全力跟)
    ride_assets: dict[str, float] = field(default_factory=dict)  # asset: 权重调整

    # ── 对冲/狙击仓位: 针对偏差 ──
    hedge_conviction: float = 0.0   # 0(不对冲) 到 1(重仓对冲)
    hedge_assets: dict[str, float] = field(default_factory=dict)
    hedge_reason: str = ""

    # ── 时间窗口 ──
    estimated_months_to_reversal: int | None = None
    time_window_detail: str = ""
    reversal_triggers: list[str] = field(default_factory=list)  # 具体触发条件


def _compute_trade_signal(
    phase: ReflexivityPhase,
    overextension: dict[str, float],
    duration: int | None,
    biggest_gap: RealityGap | None,
    all_gaps: list[RealityGap] | None,
    acceleration: float | None,
    market: MarketState | None = None,
) -> SorosTradeSignal:
    """从认知层产出具体交易信号。

    索罗斯的操作分两层:
      主仓: 跟骑趋势方向 (做多涨的资产)
      对冲: 买保险防偏差修正 (做空被忽视风险的敏感资产)

    阶段映射:
      早期  → 主仓50%, 对冲10%
      强化  → 主仓80%, 对冲15%
      晚期  → 主仓40%, 对冲40%  ← 对冲大幅上升
      极端  → 主仓10%, 对冲70%  ← 准备反向
      反转  → 主仓0%,  对冲90%  ← 全力狙击
    """
    signal = SorosTradeSignal()

    # ── 阶段 → 主仓 vs 对冲比例 ──
    ride_hedge_map = {
        ReflexivityPhase.EARLY_TREND:        (0.50, 0.10, SorosAction.RIDE,
            "趋势初期 → 中等仓位跟骑, 小量对冲"),
        ReflexivityPhase.SELF_REINFORCING:   (0.80, 0.15, SorosAction.RIDE,
            "自我强化 → 大仓跟骑, 这是赚钱最快的阶段. 对冲保持但不加大"),
        ReflexivityPhase.LATE_STAGE:         (0.40, 0.40, SorosAction.RIDE_WITH_HEDGE,
            "晚期延伸 → 减主仓, 对冲升至同等水平. '一只脚在门口'"),
        ReflexivityPhase.APPROACHING_CLIMAX: (0.10, 0.70, SorosAction.PREPARE_SNIPE,
            "接近极端 → 大幅减主仓, 对冲占主导. 准备反向"),
        ReflexivityPhase.REVERSAL:           (0.00, 0.90, SorosAction.SNIPE,
            "反转确认 → 清空主仓, 全力狙击. 索罗斯的赚钱时刻"),
    }
    ride_conv, hedge_conv, action, detail = ride_hedge_map.get(
        phase, (0.0, 0.0, SorosAction.WAIT, "无明确信号 → 不做方向性押注")
    )
    signal.action = action
    signal.action_detail = detail
    signal.ride_conviction = ride_conv
    signal.hedge_conviction = hedge_conv

    # ── 主仓资产: 跟骑趋势方向 ──
    # 关键修正: 过度延伸越高，跟骑力度越低 (不是越高越跟)
    # 索罗斯跟骑的是趋势，不是极端。极端反而是该减仓的信号。
    RISK_ASSETS = {"equity_cyclical", "commodity", "em_bond"}
    # 获取各资产的原始动量，动量为负的不跟骑
    asset_momentum = {}
    if market:
        asset_momentum = {
            "equity_cyclical": market.momentum_equity or 0,
            "commodity": market.momentum_commodity or 0,
            "em_bond": market.momentum_em_bond or 0,
        }
    for asset, ext in overextension.items():
        if asset not in RISK_ASSETS:
            continue
        mom = asset_momentum.get(asset, 0)
        if ext <= 0 or mom <= 0:
            continue  # 不跟动量为负或不过度延伸的资产
            # 过度延伸 0.0→1.0: 跟骑力度从 100%→30% 的 ride_conviction
        # 核心: 延伸越大→跟骑比例越低（不是越高）
        overext_penalty = 1.0 - ext * 0.7  # ext=0→1.0, ext=1→0.3
        ride_weight = ride_conv * max(0.1, overext_penalty)
        signal.ride_assets[asset] = round(ride_weight, 2)

    # ── 对冲资产: 两个来源 ──
    # 来源1: 避险资产 (黄金/债) — 趋势反转时它们涨
    # 修正: 对冲也要看性价比 — 避险资产自身过度延伸 = 保险太贵
    SAFE_ASSETS = {"gold", "long_term_bond", "intermediate_bond"}
    for asset in SAFE_ASSETS:
        ext = overextension.get(asset, 0)
        # 避险资产自身高估 → 保险太贵，减少配置
        # 避险资产自身低估 → 保险便宜，正好买
        cost_adj = 1.0 - max(0, ext) * 0.6  # ext=0→1.0, ext=0.8→0.52
        hedge_weight = hedge_conv * 0.5 * max(0.2, cost_adj)
        signal.hedge_assets[asset] = round(hedge_weight, 2)

    # 来源2: 现实偏差 → 做空被忽视的风险
    significant_gaps = []
    if all_gaps:
        significant_gaps = [g for g in all_gaps if abs(g.gap) > 0.3]
        for gap in significant_gaps[:2]:  # 最多2个偏差交易
            if gap.gap > 0:
                # 市场过度乐观 → 做空该 Force 敏感资产
                # F2/F3 过度乐观 → 做空风险资产
                if gap.force_id in (2, 3):
                    for asset in RISK_ASSETS:
                        signal.hedge_assets[asset] = round(
                            signal.hedge_assets.get(asset, 0) + abs(gap.gap) * hedge_conv * 0.3,
                            2
                        )
                # F5 过度乐观 → 特别做空股票
                elif gap.force_id == 5:
                    signal.hedge_assets["equity_cyclical"] = round(
                        signal.hedge_assets.get("equity_cyclical", 0) + abs(gap.gap) * hedge_conv * 0.4,
                        2
                    )
            else:
                # 市场过度悲观 → 做多该 Force 敏感资产 (这是机会)
                if gap.force_id == 1:
                    signal.ride_assets["long_term_bond"] = round(abs(gap.gap) * 0.3, 2)

    if significant_gaps:
        top = significant_gaps[0]
        direction = "做空" if top.gap > 0 else "做多"
        signal.hedge_reason = (
            f"F{top.force_id}({top.force_name}) 偏差{top.gap:+.2f} → "
            f"市场{('忽视了风险' if top.gap > 0 else '过度恐惧')} → {direction}相关资产"
        )

    # ── 时间窗口: 基于具体条件, 不是死公式 ──
    signal.reversal_triggers = []
    if duration is not None:
        if phase in (ReflexivityPhase.SELF_REINFORCING, ReflexivityPhase.EARLY_TREND):
            # 给范围估计 + 触发条件
            # 牛市平均 24-36 月, 但方差大 (2003-2007=48月, 2020-2021=15月)
            # 用当前位置给一个宽范围
            low = max(1, 18 - duration)
            high = max(low + 3, 36 - duration)
            signal.estimated_months_to_reversal = (low + high) // 2
            signal.time_window_detail = (
                f"已走{duration}月, 趋势仍在强化. "
                f"历史范围: 还有{low}-{high}月 (牛市平均24-36月, 方差大)"
            )
            signal.reversal_triggers = [
                "VIX 从低位急升 >5 (自满瓦解)",
                "信用利差单月扩大 >1pp (信贷裂缝)",
                "3月动量年化 < 12月动量的一半 (加速度转负)",
            ]
            if acceleration is not None and acceleration > 0:
                signal.time_window_detail += f" 当前加速中({acceleration:+.0f})"
                signal.reversal_triggers.append("加速度转负 = 最早的减速信号")
        elif phase == ReflexivityPhase.LATE_STAGE:
            signal.estimated_months_to_reversal = max(1, min(6, 30 - duration))
            signal.time_window_detail = (
                f"已走{duration}月, 进入晚期 → "
                f"估计{signal.estimated_months_to_reversal}月内触发反转"
            )
            signal.reversal_triggers = [
                "VIX 突破 20 且上升趋势确立",
                "信用利差开始趋势性扩大",
                "任一偏差被事件确认 (关税落地/政策冲击)",
                "3月动量转负",
            ]
        elif phase == ReflexivityPhase.APPROACHING_CLIMAX:
            signal.estimated_months_to_reversal = 2
            signal.time_window_detail = f"已走{duration}月, 接近极端 → 1-3月内大概率反转"
            signal.reversal_triggers = [
                "任何负面催化剂 (盈利miss/政策冲击/黑天鹅)",
                "单日跌幅 >3% (恐慌开始)",
                "VIX 突破 25",
            ]

    # 加速度修正
    if acceleration is not None and signal.estimated_months_to_reversal is not None:
        if acceleration > 10:
            signal.estimated_months_to_reversal += 2
            signal.time_window_detail += " (加速中, 窗口延长)"
        elif acceleration < -10:
            signal.estimated_months_to_reversal = max(1, signal.estimated_months_to_reversal - 2)
            signal.time_window_detail += " (减速中, 窗口缩短)"

    # ── 净额化: 合并主仓和对冲中重叠的资产 ──
    all_assets = set(signal.ride_assets) | set(signal.hedge_assets)
    net_ride = {}
    net_hedge = {}
    for asset in all_assets:
        ride = signal.ride_assets.get(asset, 0)
        hedge = signal.hedge_assets.get(asset, 0)
        if asset in ("gold", "long_term_bond", "intermediate_bond"):
            # 避险资产: 对冲层是做多, 不和主仓冲突
            if ride > 0:
                net_ride[asset] = ride
            if hedge > 0:
                net_hedge[asset] = hedge
        else:
            # 风险资产: 主仓做多, 对冲做空 → 净额
            net = ride - hedge
            if net > 0.01:
                net_ride[asset] = round(net, 2)
            elif net < -0.01:
                net_hedge[asset] = round(abs(net), 2)
    signal.ride_assets = net_ride
    signal.hedge_assets = net_hedge

    return signal


# ══════════════════════════════════════════════════════════════
#  反身性反馈: 市场价格 → 改变现实 → 因果图节点修正
# ══════════════════════════════════════════════════════════════
#
# 索罗斯的核心洞见: 市场价格不只是反映现实, 它改变现实。
#
#   股票涨 → 财富效应 → 消费↑ → GDP↑ → 企业盈利↑ → 股票更涨
#   利差窄 → 融资成本低 → 企业加杠杆 → 投资↑ → 经济好 → 利差更窄
#   VIX低 → 卖期权利润高 → 杠杆堆积 → 波动率被压制 → VIX更低
#
# 关键: 正反馈在强化中累积脆弱性, 直到某个阈值触发反转。
# 系统需要同时计算: (1) 正反馈对因果节点的修正 (2) 脆弱性的积累


@dataclass
class ReflexivityFeedback:
    """反身性反馈: 市场价格对因果图节点的修正量。

    可以直接叠加到达利欧因果引擎的 7 个节点上。
    """
    # 节点修正 {node_name: delta}
    node_adjustments: dict[str, float] = field(default_factory=dict)
    # 脆弱性积累 (0-1): 正反馈累积的系统性风险
    fragility: float = 0.0
    # 人可读的反馈链
    feedback_chains: list[str] = field(default_factory=list)


def compute_reflexivity_feedback(market: MarketState) -> ReflexivityFeedback:
    """计算市场价格对现实的反馈效应。

    输出可以注入达利欧因果引擎, 让因果图反映"市场本身对经济的影响"。

    这是索罗斯和达利欧的接口:
      达利欧: 经济基本面 → 因果图 → 资产价格
      索罗斯: 资产价格 → 反馈修正 → 因果图被修改 → 新的资产价格
    """
    fb = ReflexivityFeedback()
    eq = market.momentum_equity or 0
    vix = market.vix or 20
    spread = market.credit_spread_hy or 5
    spread_chg = market.credit_spread_change_3m or 0
    duration = market.trend_duration_months or 0

    # ══ 1. 财富效应: 股票价格 → consumer_health ══
    # 美国家庭资产60%+在股市，股票涨跌直接影响消费意愿
    # 研究: 股市财富每增加$1, 消费增加$0.03-0.05 (3-5美分效应)
    if eq != 0:
        wealth_effect = eq / 100 * 0.3  # 股涨30%→consumer +0.09
        fb.node_adjustments["consumer_health"] = round(wealth_effect, 3)
        if abs(wealth_effect) > 0.03:
            direction = "↑" if wealth_effect > 0 else "↓"
            fb.feedback_chains.append(
                f"财富效应: 股票{eq:+.0f}% → 消费者{direction} ({wealth_effect:+.3f})"
            )

    # ══ 2. 融资渠道: 信用利差 → credit_availability + corporate_health ══
    # 利差窄 = 企业融资便宜 → 更多借贷、投资、回购 → 自我强化
    # 利差宽 = 融资困难 → 企业不敢借 → 经济放缓 → 利差更宽
    if spread < 4:
        credit_ease = (4 - spread) / 4 * 0.2  # 利差3%→+0.05
        fb.node_adjustments["credit_availability"] = round(
            fb.node_adjustments.get("credit_availability", 0) + credit_ease, 3)
        fb.node_adjustments["corporate_health"] = round(
            fb.node_adjustments.get("corporate_health", 0) + credit_ease * 0.5, 3)
        fb.feedback_chains.append(
            f"融资宽松: 利差{spread:.1f}%偏紧 → 信贷+{credit_ease:.3f} 企业+{credit_ease*0.5:.3f}"
        )
    elif spread > 7:
        credit_tight = -(spread - 7) / 10 * 0.3
        fb.node_adjustments["credit_availability"] = round(
            fb.node_adjustments.get("credit_availability", 0) + credit_tight, 3)
        fb.node_adjustments["corporate_health"] = round(
            fb.node_adjustments.get("corporate_health", 0) + credit_tight * 0.7, 3)
        fb.feedback_chains.append(
            f"融资冻结: 利差{spread:.1f}% → 信贷{credit_tight:.3f} 企业{credit_tight*0.7:.3f}"
        )

    # 利差变化方向 = 反馈的加速度
    if spread_chg < -0.5:
        fb.node_adjustments["credit_availability"] = round(
            fb.node_adjustments.get("credit_availability", 0) + 0.03, 3)
        fb.feedback_chains.append("利差收窄趋势 → 正反馈加速中")
    elif spread_chg > 1.0:
        fb.node_adjustments["credit_availability"] = round(
            fb.node_adjustments.get("credit_availability", 0) - 0.05, 3)
        fb.node_adjustments["default_pressure"] = round(
            fb.node_adjustments.get("default_pressure", 0) + 0.03, 3)
        fb.feedback_chains.append("利差扩大趋势 → 负反馈开始")

    # ══ 3. 波动率反馈: VIX → 杠杆/风险偏好 ══
    # VIX低 → 风险模型允许更多杠杆 → 更多买入 → VIX更低 → 更多杠杆
    # 这是"波动率悖论": 低波动率本身制造了未来的高波动率
    if vix < 14:
        vol_feedback = (14 - vix) / 14 * 0.1  # VIX=10 → +0.03
        fb.node_adjustments["credit_availability"] = round(
            fb.node_adjustments.get("credit_availability", 0) + vol_feedback, 3)
        fb.feedback_chains.append(
            f"波动率悖论: VIX={vix:.0f}低 → 杠杆堆积+{vol_feedback:.3f} (脆弱性在增加)"
        )
    elif vix > 30:
        vol_panic = -(vix - 30) / 30 * 0.15  # VIX=60 → -0.15
        fb.node_adjustments["credit_availability"] = round(
            fb.node_adjustments.get("credit_availability", 0) + vol_panic, 3)
        fb.node_adjustments["consumer_health"] = round(
            fb.node_adjustments.get("consumer_health", 0) + vol_panic * 0.5, 3)
        fb.feedback_chains.append(
            f"恐慌反馈: VIX={vix:.0f} → 信贷冻结{vol_panic:.3f} + 消费者恐惧{vol_panic*0.5:.3f}"
        )

    # ══ 4. 脆弱性积累 ══
    # 正反馈持续越久, 系统越脆弱 (杠杆越高, 仓位越拥挤)
    # 脆弱性不直接改变因果节点, 但会放大未来冲击的效果
    fragility = 0.0

    # 时间维度: 趋势越久 → 杠杆越高
    if duration > 12:
        fragility += min(0.3, (duration - 12) / 40)

    # 波动率维度: VIX越低 → 隐性杠杆越高
    if vix < 16:
        fragility += (16 - vix) / 16 * 0.2

    # 信用维度: 利差越窄 → 信用风险被低估
    if spread < 4:
        fragility += (4 - spread) / 4 * 0.2

    # 估值维度: PE越高 → 对盈利失望的敏感度越高
    pe_pct = market.equity_pe_percentile or 50
    if pe_pct > 70:
        fragility += (pe_pct - 70) / 100

    fb.fragility = min(1.0, fragility)

    if fb.fragility > 0.5:
        fb.feedback_chains.append(
            f"⚠ 脆弱性 {fb.fragility:.0%}: 正反馈累积了大量隐性风险, "
            f"小冲击可能引发大崩盘"
        )

    return fb


# ══════════════════════════════════════════════════════════════
#  背离检测 — 市场内部分裂 = 趋势松动的早期信号
# ══════════════════════════════════════════════════════════════


@dataclass
class Divergence:
    """市场背离信号。"""
    name: str
    severity: float      # 0-1, 越高越危险
    detail: str
    implication: str     # 对交易的含义


def _detect_divergences(market: MarketState) -> list[Divergence]:
    """检测市场内部的背离信号。

    背离 = 不同市场在讲不同的故事 = 趋势内部在分裂。
    索罗斯: 当背离出现, 意味着反身性循环开始松动。

    关键背离:
      1. 股涨 + 利差扩 → 信贷市场看到了股市没看到的风险
      2. 金涨 + VIX低 → 有人在买保险但整体不恐慌 (聪明钱 vs 散户)
      3. 股涨 + 短期动量减速 → 涨幅靠惯性不是新资金
      4. 股涨 + 收益率曲线倒挂 → 股市赌增长, 债市赌衰退
      5. VIX 下降趋势 + 利差上升趋势 → 波动率自满 + 信贷担忧
    """
    divs = []
    eq = market.momentum_equity
    vix = market.vix
    gold = market.momentum_gold
    spread = market.credit_spread_hy
    spread_chg = market.credit_spread_change_3m
    yc = market.yield_curve_10y_3m
    mom_3m = market.momentum_equity_3m

    # 1. 股涨 + 利差扩大
    if eq is not None and eq > 10 and spread_chg is not None and spread_chg > 0.3:
        sev = min(1.0, spread_chg / 1.5)
        divs.append(Divergence(
            name="股债背离",
            severity=sev,
            detail=f"股票+{eq:.0f}%但利差扩大{spread_chg:+.1f}pp — 信贷市场在发出警告",
            implication="信贷市场通常比股市更早反应真实风险. 密切关注利差趋势",
        ))

    # 2. 金涨 + VIX低 (聪明钱在买保险)
    if gold is not None and gold > 15 and vix is not None and vix < 18:
        sev = min(1.0, gold / 40)
        divs.append(Divergence(
            name="金VIX背离",
            severity=sev,
            detail=f"黄金+{gold:.0f}%但VIX仅{vix:.0f} — 有人在买保险但市场表面平静",
            implication="可能是央行/机构在囤金(结构性), 也可能是聪明钱嗅到了风险",
        ))

    # 3. 动量减速背离 (12月涨但3月已经不涨了)
    if eq is not None and eq > 15 and mom_3m is not None:
        annualized_3m = mom_3m * 4
        if annualized_3m < eq * 0.4:  # 3月年化不到12月的40%
            sev = min(1.0, (eq - annualized_3m) / eq)
            divs.append(Divergence(
                name="动量衰减",
                severity=sev,
                detail=f"12月+{eq:.0f}%但3月年化仅{annualized_3m:.0f}% — 涨幅靠旧动量",
                implication="新资金在减少, 趋势靠惯性维持. 最早的反转预警之一",
            ))

    # 4. 股涨 + 收益率曲线倒挂
    if eq is not None and eq > 10 and yc is not None and yc < -0.1:
        sev = min(1.0, abs(yc) / 1.0)
        divs.append(Divergence(
            name="股债期限背离",
            severity=sev,
            detail=f"股票+{eq:.0f}%但曲线倒挂{yc:+.2f} — 股市赌增长, 债市赌衰退",
            implication="收益率曲线倒挂是历史上最可靠的衰退预测指标(领先12-18月)",
        ))

    # 5. VIX 低位 + 利差绝对水平上升
    if (vix is not None and vix < 16 and spread is not None and spread > 4.5
            and market.credit_spread_hy_6m_ago is not None
            and spread > market.credit_spread_hy_6m_ago + 0.5):
        divs.append(Divergence(
            name="波动率-信用背离",
            severity=0.6,
            detail=f"VIX={vix:.0f}(平静)但利差从{market.credit_spread_hy_6m_ago:.1f}%升至{spread:.1f}% — 股市自满, 信贷收紧",
            implication="VIX 是散户情绪, 利差是机构定价. 机构在撤退",
        ))

    divs.sort(key=lambda d: -d.severity)
    return divs


# ── 主入口 ──────────────────────────────────────────────────


def evaluate_soros(
    market: MarketState,
    force_directions: dict[int, float] | None = None,
    prior_phase: ReflexivityPhase | None = None,
) -> SorosInsight:
    """索罗斯认知链主入口。

    纯市场数据 → 市场信念 + 现实偏差 + 反身性阶段 + 过度延伸度

    Args:
        market: 纯市场数据
        force_directions: 达利欧五力方向 {force_id: -1到+1}
        prior_phase: 上一期的反身性阶段 (状态延续用)
            如果上期是 CLIMAX/SNIPE, 不会轻易回退到 ride

    状态延续规则 (索罗斯不会因为一个月的假信号就放弃判断):
      CLIMAX → 只能去 REVERSAL/SNIPE 或停留, 不能回 RIDE
      LATE_STAGE → 只能去 CLIMAX/REVERSAL 或停留, 不能回 EARLY
      REVERSAL → 只能去 NEUTRAL 或停留
      恢复到 RIDE 需要: 3月动量转正 + VIX回落 + 利差收窄
    """
    result = SorosInsight()

    # v2: 从价格反推市场信念
    result.beliefs = _infer_force_beliefs(market)
    result.narrative_summary = _summarize_narrative(result.beliefs)
    result.evidence.append(result.narrative_summary)

    # v2: 现实偏差
    if force_directions:
        result.reality_gaps = _compute_reality_gaps(result.beliefs, force_directions)
        if result.reality_gaps:
            result.biggest_gap = result.reality_gaps[0]
            if abs(result.biggest_gap.gap) > 0.3:
                result.evidence.append(
                    f"最大偏差: F{result.biggest_gap.force_id}"
                    f"({result.biggest_gap.force_name}) gap={result.biggest_gap.gap:+.2f}"
                )

    # v1 兼容: 旧版叙事
    result.narrative, result.narrative_detail = _identify_narrative(market)

    # 反身性阶段
    result.phase, result.phase_detail = _assess_reflexivity(market, result.narrative)

    # 状态延续: 防止从高危状态因一个月假信号回退
    if prior_phase is not None:
        new_phase = result.phase
        # CLIMAX 不能直接回 ride/early — 必须看到趋势真正恢复
        if prior_phase == ReflexivityPhase.APPROACHING_CLIMAX:
            if new_phase in (ReflexivityPhase.EARLY_TREND, ReflexivityPhase.SELF_REINFORCING):
                # 检查恢复条件: 3月动量转正 + VIX 回落
                mom3 = market.momentum_equity_3m
                vix = market.vix
                recovered = (mom3 is not None and mom3 > 3 and
                             vix is not None and vix < 18)
                if not recovered:
                    result.phase = ReflexivityPhase.LATE_STAGE
                    result.phase_detail = (
                        f"[状态锁定] 上期CLIMAX, 当前数据说'{new_phase.value}', "
                        f"但恢复条件未满足(需3M动量>3%+VIX<18) → 保持LATE"
                    )
        # LATE_STAGE 不能回 EARLY
        elif prior_phase == ReflexivityPhase.LATE_STAGE:
            if new_phase == ReflexivityPhase.EARLY_TREND:
                mom3 = market.momentum_equity_3m
                if mom3 is None or mom3 < 5:
                    result.phase = ReflexivityPhase.LATE_STAGE
                    result.phase_detail = (
                        f"[状态锁定] 上期LATE, 恢复条件未满足 → 保持LATE"
                    )
        # REVERSAL 不能直接回 RIDE
        elif prior_phase == ReflexivityPhase.REVERSAL:
            if new_phase in (ReflexivityPhase.EARLY_TREND, ReflexivityPhase.SELF_REINFORCING):
                mom3 = market.momentum_equity_3m
                vix = market.vix
                spread_chg = market.credit_spread_change_3m
                recovered = (mom3 is not None and mom3 > 5 and
                             vix is not None and vix < 20 and
                             (spread_chg is None or spread_chg < 0))
                if not recovered:
                    result.phase = ReflexivityPhase.NEUTRAL
                    result.phase_detail = (
                        f"[状态锁定] 上期REVERSAL, 全面恢复条件未满足 → NEUTRAL"
                    )

    result.evidence.append(f"反身性: {result.phase.value} — {result.phase_detail}")

    # 过度延伸
    result.overextension = _compute_overextension(market)
    for asset, ext in sorted(result.overextension.items(), key=lambda x: -abs(x[1])):
        if abs(ext) > 0.3:
            direction = "高估" if ext > 0 else "低估"
            result.evidence.append(f"{asset}: {direction} {abs(ext):.0%}")

    # v2: 反身性反馈 (市场价格如何改变现实)
    result.reflexivity_feedback = compute_reflexivity_feedback(market)
    for chain in result.reflexivity_feedback.feedback_chains:
        result.evidence.append(f"反馈: {chain}")

    # v2: 背离检测
    result.divergences = _detect_divergences(market)
    for d in result.divergences:
        if d.severity > 0.3:
            result.evidence.append(f"背离: {d.name} (严重度{d.severity:.0%})")

    data_count = sum(1 for v in [
        market.momentum_equity, market.vix, market.credit_spread_hy,
        market.equity_pe_percentile, market.momentum_long_bond,
    ] if v is not None)
    result.confidence = min(1.0, data_count / 5)

    # ── 信念衰减: 趋势越久+背离越多 → 信心降低 ──
    # 索罗斯: 趋势持续本身不说明趋势正确, 反而增加了偏差积累
    duration = market.trend_duration_months
    if duration is not None and duration > 12:
        time_decay = min(0.3, (duration - 12) / 60)  # 每5年衰减0.1, 上限0.3
        result.confidence = max(0.2, result.confidence - time_decay)
    if result.divergences:
        div_penalty = sum(d.severity * 0.1 for d in result.divergences)
        result.confidence = max(0.2, result.confidence - div_penalty)

    # v2: 交易信号
    acceleration = None
    if market.momentum_equity_3m is not None and market.momentum_equity is not None:
        acceleration = market.momentum_equity_3m * 4 - market.momentum_equity

    result.trade_signal = _compute_trade_signal(
        phase=result.phase,
        overextension=result.overextension,
        duration=market.trend_duration_months,
        biggest_gap=result.biggest_gap,
        all_gaps=result.reality_gaps,
        acceleration=acceleration,
        market=market,
    )

    # 背离影响交易信号: 背离严重时, 重算信号
    if result.trade_signal and result.divergences:
        total_div_severity = sum(d.severity for d in result.divergences)
        if total_div_severity > 0.3:
            ts = result.trade_signal
            boost = min(0.3, total_div_severity * 0.15)
            new_hedge = min(1.0, ts.hedge_conviction + boost)
            new_ride = max(0.0, ts.ride_conviction - boost * 0.5)
            ts.action_detail += f" (背离信号: 主仓{ts.ride_conviction:.0%}→{new_ride:.0%}, 对冲{ts.hedge_conviction:.0%}→{new_hedge:.0%})"
            ts.hedge_conviction = new_hedge
            ts.ride_conviction = new_ride

            # 重算资产配置 (用新的 conviction 比例缩放)
            if ts.ride_assets:
                scale = new_ride / max(0.01, ts.ride_conviction + boost * 0.5)
                ts.ride_assets = {k: round(v * scale, 2) for k, v in ts.ride_assets.items()}
            # 对冲资产按新比例放大
            if ts.hedge_assets:
                scale = new_hedge / max(0.01, new_hedge - boost)
                ts.hedge_assets = {k: round(min(v * scale, 0.5), 2) for k, v in ts.hedge_assets.items()}

    return result


# ── 格式化 ──────────────────────────────────────────────────


def format_soros(result: SorosInsight) -> str:
    """格式化索罗斯认知报告。"""
    lines = [""]
    lines.append("  索罗斯反身性认知")
    lines.append("  ════════════════════════════════════════════════")

    phase_labels = {
        ReflexivityPhase.EARLY_TREND: "早期趋势",
        ReflexivityPhase.SELF_REINFORCING: "自我强化中",
        ReflexivityPhase.LATE_STAGE: "晚期延伸",
        ReflexivityPhase.APPROACHING_CLIMAX: "接近极端",
        ReflexivityPhase.REVERSAL: "正在反转",
        ReflexivityPhase.NEUTRAL: "中性",
    }

    # v2: 市场信念
    if result.beliefs:
        lines.append(f"\n  {result.narrative_summary}")
        lines.append(f"\n  市场信念 (从价格反推):")
        for b in result.beliefs:
            bar_len = int(abs(b.priced_direction) * 10)
            bar = ("▸" * bar_len) if b.priced_direction >= 0 else ("◂" * bar_len)
            label = "看多" if b.priced_direction > 0.1 else "看空" if b.priced_direction < -0.1 else "中性"
            lines.append(f"    F{b.force_id} {b.force_name:10s} {b.priced_direction:+.2f} {bar} ({label})")
            for ev in b.pricing_evidence[:2]:
                lines.append(f"       {ev}")

    # v2: 现实偏差
    if result.reality_gaps:
        significant = [g for g in result.reality_gaps if abs(g.gap) > 0.2]
        if significant:
            lines.append(f"\n  现实偏差 (市场信念 vs 达利欧五力):")
            for g in significant:
                arrow = "⚡" if abs(g.gap) > 0.5 else "△"
                lines.append(
                    f"    {arrow} F{g.force_id} {g.force_name:10s}  "
                    f"市场={g.market_prices:+.2f}  现实={g.reality:+.2f}  "
                    f"偏差={g.gap:+.2f}"
                )
                if g.gap_detail:
                    lines.append(f"      {g.gap_detail}")
                if g.trade_implication:
                    lines.append(f"      → {g.trade_implication}")

    # 反身性反馈
    if result.reflexivity_feedback:
        fb = result.reflexivity_feedback
        lines.append(f"\n  反身性反馈 (市场价格 → 改变现实):")
        for chain in fb.feedback_chains:
            lines.append(f"    {chain}")
        if fb.node_adjustments:
            lines.append(f"    → 因果图修正:")
            for node, delta in sorted(fb.node_adjustments.items(), key=lambda x: -abs(x[1])):
                if abs(delta) > 0.005:
                    lines.append(f"      {node:25s} {delta:+.3f}")
        frag_bar = "█" * int(fb.fragility * 10) + "░" * (10 - int(fb.fragility * 10))
        lines.append(f"    脆弱性: [{frag_bar}] {fb.fragility:.0%}")

    # 背离信号
    if result.divergences:
        lines.append(f"\n  背离信号 (市场内部分裂):")
        for d in result.divergences:
            bar = "!" * int(d.severity * 5)
            lines.append(f"    [{bar:5s}] {d.name}: {d.detail}")
            lines.append(f"           {d.implication}")

    # 反身性阶段
    lines.append(f"\n  反身性: {phase_labels.get(result.phase, '?')}")
    lines.append(f"    {result.phase_detail}")

    # 过度延伸
    if result.overextension:
        lines.append(f"\n  过度延伸:")
        for asset, ext in sorted(result.overextension.items(), key=lambda x: -abs(x[1])):
            bar = "+" * int(max(ext * 10, 0)) + "-" * int(max(-ext * 10, 0))
            direction = "高估" if ext > 0 else "低估"
            lines.append(f"    {asset:25s} {ext:+.2f} ({direction}) {bar}")

    # 交易信号
    if result.trade_signal:
        ts = result.trade_signal
        action_icons = {
            SorosAction.RIDE: "跟骑",
            SorosAction.RIDE_WITH_HEDGE: "跟骑+对冲",
            SorosAction.PREPARE_SNIPE: "准备狙击",
            SorosAction.SNIPE: "狙击!",
            SorosAction.WAIT: "等待",
        }
        lines.append(f"\n  ── Axion 交易信号 ──")
        lines.append(f"  动作: {action_icons.get(ts.action, ts.action.value)}")
        lines.append(f"    {ts.action_detail}")
        lines.append(f"  主仓(顺势): {ts.ride_conviction:.0%}    对冲(逆势): {ts.hedge_conviction:.0%}")

        if ts.ride_assets:
            lines.append(f"\n  主仓 — 跟骑趋势:")
            for asset, w in sorted(ts.ride_assets.items(), key=lambda x: -x[1]):
                if w > 0.01:
                    lines.append(f"    做多 {asset:25s} {w:+.2f}")

        if ts.hedge_assets:
            lines.append(f"\n  对冲 — 防偏差修正:")
            if ts.hedge_reason:
                lines.append(f"    原因: {ts.hedge_reason}")
            for asset, w in sorted(ts.hedge_assets.items(), key=lambda x: -x[1]):
                if w > 0.01:
                    direction = "做多(避险)" if asset in ("gold", "long_term_bond", "intermediate_bond") else "做空"
                    lines.append(f"    {direction} {asset:25s} {w:.2f}")

        if ts.time_window_detail:
            lines.append(f"\n  时间窗口: {ts.time_window_detail}")
        if ts.estimated_months_to_reversal is not None:
            lines.append(f"    估计: ~{ts.estimated_months_to_reversal}月")
        if ts.reversal_triggers:
            lines.append(f"  反转触发条件:")
            for t in ts.reversal_triggers:
                lines.append(f"    · {t}")

    lines.append(f"\n  置信度: {result.confidence:.0%}")
    lines.append("")
    return "\n".join(lines)
