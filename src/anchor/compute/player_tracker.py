"""
玩家追踪器
==========

桥水方法论: "gets down to the nitty-gritty of who is the buyer and who's the seller"

三层数据:
1. 玩家仓位 (谁持有什么)
2. 玩家资产负债表 (谁有多少杠杆)
3. 资金流动 (钱在往哪走)

五类玩家:
- banks: 银行 (H.8 贷款 + 准备金 + 贴现窗口)
- central_banks: 央行 (Fed/ECB/BOJ 资产负债表)
- private_credit: 私募信贷 (CCC收益率 + BDC股价)
- foreign_governments: 外国政府 (TIC持债 + 中国储备)
- retail: 散户 (货币基金 + 保证金)
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from anchor.compute.percentile_trend import (
    IndicatorAssessment,
    SignalTier,
    assess_from_fred_history,
)


# ━━ 数据结构 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@dataclass
class PlayerGroup:
    """单个玩家群体的状态快照。"""
    name: str            # "banks" / "central_banks" / "private_credit" / "foreign_gov" / "retail"
    health: str          # "healthy" / "stressed" / "crisis"
    key_metric: float    # 最关键的一个数字
    trend: str           # "improving" / "stable" / "deteriorating"
    detail: str          # 一句话描述
    indicators: list[IndicatorAssessment] = field(default_factory=list)


@dataclass
class PlayerMap:
    """所有玩家群体的汇总地图。"""
    month: str
    banks: PlayerGroup
    central_banks: PlayerGroup
    private_credit: PlayerGroup
    foreign_governments: PlayerGroup
    retail: PlayerGroup

    capital_flow_direction: str     # "risk_on" / "risk_off" / "mixed"
    stress_signals: list[str]       # 警报列表

    def summary(self) -> str:
        lines = [
            f"=== Player Map: {self.month} ===",
            f"  Capital Flow: {self.capital_flow_direction}",
        ]
        for group in [self.banks, self.central_banks, self.private_credit,
                       self.foreign_governments, self.retail]:
            lines.append(
                f"  {group.name:20s} | {group.health:10s} | "
                f"trend={group.trend:14s} | {group.detail}"
            )
        if self.stress_signals:
            lines.append(f"  STRESS SIGNALS: {', '.join(self.stress_signals)}")
        else:
            lines.append(f"  No active stress signals")
        return "\n".join(lines)


# ━━ 辅助函数 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _get_value(fred_history: dict, key: str, month: str) -> Optional[float]:
    """从 fred_history 取某 key 在 month 或之前最近的值。"""
    series = fred_history.get(key, {})
    if not series:
        return None
    candidates = sorted(k for k in series if k <= month)
    if not candidates:
        return None
    return series[candidates[-1]]


def _yoy_change(fred_history: dict, key: str, month: str) -> Optional[float]:
    """计算 key 在 month 相对一年前的 YoY 变化百分比。"""
    series = fred_history.get(key, {})
    if not series:
        return None
    current = _get_value(fred_history, key, month)
    if current is None:
        return None
    year = int(month[:4])
    prev_month = f"{year - 1}{month[4:]}"
    prev = _get_value(fred_history, key, prev_month)
    if prev is None or prev == 0:
        return None
    return ((current / prev) - 1) * 100


def _health_from_tier(tier: SignalTier) -> str:
    """SignalTier → health string。"""
    if tier in (SignalTier.EXTREME_DETERIORATION,):
        return "crisis"
    if tier in (SignalTier.DETERIORATING,):
        return "stressed"
    return "healthy"


def _trend_from_assessments(assessments: list[IndicatorAssessment]) -> str:
    """从多个指标评估推断趋势方向。"""
    if not assessments:
        return "stable"
    scores = []
    for a in assessments:
        if a.tier == SignalTier.EXTREME_DETERIORATION:
            scores.append(-2)
        elif a.tier == SignalTier.DETERIORATING:
            scores.append(-1)
        elif a.tier == SignalTier.IMPROVING:
            scores.append(1)
        elif a.tier == SignalTier.EXTREME_IMPROVEMENT:
            scores.append(2)
        else:
            scores.append(0)
    avg = sum(scores) / len(scores)
    if avg < -0.5:
        return "deteriorating"
    if avg > 0.5:
        return "improving"
    return "stable"


def _worst_health(assessments: list[IndicatorAssessment]) -> str:
    """取最差的健康状态 (悲观方偏向)。"""
    for a in assessments:
        if a.tier == SignalTier.EXTREME_DETERIORATION:
            return "crisis"
    for a in assessments:
        if a.tier == SignalTier.DETERIORATING:
            return "stressed"
    return "healthy"


# ━━ 各玩家群体评估 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _assess_banks(fred_history: dict, month: str) -> PlayerGroup:
    """银行: H.8 贷款量 + 贷款标准 + 准备金 + 贴现窗口。"""
    indicators = []

    # 商业贷款趋势 (增长=健康, 收缩=收紧)
    ci = assess_from_fred_history(
        "bank_ci_loans", month,
        fred_history.get("bank_ci_loans", {}),
        higher_is_worse=False,
    )
    indicators.append(ci)

    # 地产贷款
    re = assess_from_fred_history(
        "bank_re_loans", month,
        fred_history.get("bank_re_loans", {}),
        higher_is_worse=False,
    )
    indicators.append(re)

    # 贷款标准收紧 (高=差)
    lending = assess_from_fred_history(
        "lending_standards", month,
        fred_history.get("lending_standards", {}),
        higher_is_worse=True,
    )
    indicators.append(lending)

    # Fed贴现窗口 (高=有银行求救)
    primary = assess_from_fred_history(
        "fed_primary_credit", month,
        fred_history.get("fed_primary_credit", {}),
        higher_is_worse=True,
    )
    indicators.append(primary)

    # 银行准备金 (低=紧张)
    reserves = assess_from_fred_history(
        "bank_reserves", month,
        fred_history.get("bank_reserves", {}),
        higher_is_worse=False,
    )
    indicators.append(reserves)

    health = _worst_health(indicators)
    trend = _trend_from_assessments(indicators)

    # 关键数字: 商业贷款 YoY
    ci_yoy = _yoy_change(fred_history, "bank_ci_loans", month)
    key_metric = ci_yoy if ci_yoy is not None else 0.0

    # 描述
    primary_val = _get_value(fred_history, "fed_primary_credit", month)
    discount_warning = ""
    if primary_val is not None and primary_val > 1000:
        discount_warning = "; 贴现窗口活跃!"
    detail = f"商业贷款YoY={key_metric:+.1f}%{discount_warning}"

    return PlayerGroup(
        name="banks", health=health, key_metric=round(key_metric, 2),
        trend=trend, detail=detail, indicators=indicators,
    )


def _assess_central_banks(fred_history: dict, month: str) -> PlayerGroup:
    """央行: Fed/ECB/BOJ 资产负债表趋势 — 扩表=放水, 缩表=收紧。"""
    indicators = []

    # Fed 资产负债表 (QE=增长=宽松, QT=收缩=紧缩)
    # 对市场: 扩表好, 缩表差 → higher_is_worse=False
    fed_bs = assess_from_fred_history(
        "fed_balance_sheet", month,
        fred_history.get("fed_balance_sheet", {}),
        higher_is_worse=False,
    )
    indicators.append(fed_bs)

    # ECB
    ecb = assess_from_fred_history(
        "ecb_assets", month,
        fred_history.get("ecb_assets", {}),
        higher_is_worse=False,
    )
    indicators.append(ecb)

    # BOJ
    boj = assess_from_fred_history(
        "boj_assets", month,
        fred_history.get("boj_assets", {}),
        higher_is_worse=False,
    )
    indicators.append(boj)

    health = _worst_health(indicators)
    trend = _trend_from_assessments(indicators)

    fed_yoy = _yoy_change(fred_history, "fed_balance_sheet", month)
    key_metric = fed_yoy if fed_yoy is not None else 0.0

    if key_metric > 5:
        stance = "大幅扩表(放水)"
    elif key_metric > 0:
        stance = "温和扩表"
    elif key_metric > -5:
        stance = "温和缩表(QT)"
    else:
        stance = "大幅缩表"
    detail = f"Fed BS YoY={key_metric:+.1f}%, {stance}"

    return PlayerGroup(
        name="central_banks", health=health, key_metric=round(key_metric, 2),
        trend=trend, detail=detail, indicators=indicators,
    )


def _assess_private_credit(
    fred_history: dict, month: str,
    player_stock_data: Optional[dict] = None,
) -> PlayerGroup:
    """私募信贷: CCC/BB/BBB 收益率分层 + BDC 股价趋势。"""
    indicators = []

    # CCC 收益率 (高=最差信贷融资困难)
    ccc = assess_from_fred_history(
        "yield_ccc", month,
        fred_history.get("yield_ccc", {}),
        higher_is_worse=True,
    )
    indicators.append(ccc)

    # BB 收益率
    bb = assess_from_fred_history(
        "yield_bb", month,
        fred_history.get("yield_bb", {}),
        higher_is_worse=True,
    )
    indicators.append(bb)

    # BBB 收益率
    bbb = assess_from_fred_history(
        "yield_bbb", month,
        fred_history.get("yield_bbb", {}),
        higher_is_worse=True,
    )
    indicators.append(bbb)

    health = _worst_health(indicators)
    trend = _trend_from_assessments(indicators)

    ccc_val = _get_value(fred_history, "yield_ccc", month)
    bb_val = _get_value(fred_history, "yield_bb", month)
    key_metric = ccc_val if ccc_val is not None else 0.0

    # CCC-BB 利差 = 信贷质量分化
    spread_detail = ""
    if ccc_val is not None and bb_val is not None:
        spread = ccc_val - bb_val
        spread_detail = f", CCC-BB spread={spread:.1f}pp"

    # BDC 股价信号 (如果有)
    bdc_detail = ""
    if player_stock_data:
        bdc_tickers = ["ARCC", "MAIN", "FSK", "BXSL"]
        bdc_returns = []
        for t in bdc_tickers:
            ticker_data = player_stock_data.get(t, {})
            if month in ticker_data:
                ret = ticker_data[month].get("return")
                if ret is not None:
                    bdc_returns.append(ret)
        if bdc_returns:
            avg_ret = sum(bdc_returns) / len(bdc_returns)
            bdc_detail = f", BDC avg return={avg_ret:+.1f}%"

    detail = f"CCC yield={key_metric:.1f}%{spread_detail}{bdc_detail}"

    return PlayerGroup(
        name="private_credit", health=health, key_metric=round(key_metric, 2),
        trend=trend, detail=detail, indicators=indicators,
    )


def _assess_foreign_governments(fred_history: dict, month: str) -> PlayerGroup:
    """外国政府: TIC 持债 + 中国储备 — 在买还是卖美国资产?"""
    indicators = []

    # 外国持有美债 (减少=去美元化)
    foreign_treasury = assess_from_fred_history(
        "foreign_treasury_holdings", month,
        fred_history.get("foreign_treasury_holdings", {}),
        higher_is_worse=False,
    )
    indicators.append(foreign_treasury)

    # 中国外汇储备 (减少=可能抛售)
    china_res = assess_from_fred_history(
        "china_reserves", month,
        fred_history.get("china_reserves", {}),
        higher_is_worse=False,
    )
    indicators.append(china_res)

    health = _worst_health(indicators)
    trend = _trend_from_assessments(indicators)

    tic_yoy = _yoy_change(fred_history, "foreign_treasury_holdings", month)
    key_metric = tic_yoy if tic_yoy is not None else 0.0

    if key_metric > 3:
        action = "积极买入美债"
    elif key_metric > 0:
        action = "温和增持美债"
    elif key_metric > -3:
        action = "温和减持美债"
    else:
        action = "抛售美债(去美元化)"
    detail = f"外国持债YoY={key_metric:+.1f}%, {action}"

    return PlayerGroup(
        name="foreign_governments", health=health, key_metric=round(key_metric, 2),
        trend=trend, detail=detail, indicators=indicators,
    )


def _assess_retail(fred_history: dict, month: str) -> PlayerGroup:
    """散户: 货币基金规模 + 消费者信心 — 逃向安全 vs 追逐风险。"""
    indicators = []

    # 货币基金规模 (飙升=逃向安全=risk_off)
    mmf = assess_from_fred_history(
        "money_market_funds", month,
        fred_history.get("money_market_funds", {}),
        higher_is_worse=True,  # 高=资金逃离风险资产
    )
    indicators.append(mmf)

    # 消费者信心 (低=悲观)
    sentiment = assess_from_fred_history(
        "consumer_sentiment", month,
        fred_history.get("consumer_sentiment", {}),
        higher_is_worse=False,
    )
    indicators.append(sentiment)

    health = _worst_health(indicators)
    trend = _trend_from_assessments(indicators)

    mmf_val = _get_value(fred_history, "money_market_funds", month)
    mmf_yoy = _yoy_change(fred_history, "money_market_funds", month)
    key_metric = mmf_yoy if mmf_yoy is not None else 0.0

    if key_metric > 20:
        mood = "恐慌性涌入货币基金"
    elif key_metric > 5:
        mood = "资金流入货币基金"
    elif key_metric > -5:
        mood = "资金流动正常"
    else:
        mood = "资金流出货币基金(追逐风险)"
    detail = f"货币基金YoY={key_metric:+.1f}%, {mood}"

    return PlayerGroup(
        name="retail", health=health, key_metric=round(key_metric, 2),
        trend=trend, detail=detail, indicators=indicators,
    )


# ━━ 资金流向 + 压力信号 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _determine_capital_flow(
    banks: PlayerGroup,
    retail: PlayerGroup,
    fred_history: dict,
    month: str,
) -> str:
    """综合判断资金流向: risk_on / risk_off / mixed。"""
    risk_off_signals = 0
    risk_on_signals = 0

    # 银行紧缩 = risk_off
    if banks.health in ("stressed", "crisis"):
        risk_off_signals += 1
    elif banks.trend == "improving":
        risk_on_signals += 1

    # 散户逃向安全 = risk_off
    if retail.key_metric > 10:  # 货币基金 YoY > 10%
        risk_off_signals += 1
    elif retail.key_metric < -5:
        risk_on_signals += 1

    # VIX
    vix = _get_value(fred_history, "vix_daily", month)
    if vix is not None:
        if vix > 30:
            risk_off_signals += 1
        elif vix < 15:
            risk_on_signals += 1

    # 金融条件
    nfci = _get_value(fred_history, "nfci", month)
    if nfci is not None:
        if nfci > 0:  # NFCI > 0 = 条件紧缩
            risk_off_signals += 1
        elif nfci < -0.5:
            risk_on_signals += 1

    if risk_off_signals >= 2 and risk_off_signals > risk_on_signals:
        return "risk_off"
    if risk_on_signals >= 2 and risk_on_signals > risk_off_signals:
        return "risk_on"
    return "mixed"


def _detect_stress_signals(
    fred_history: dict,
    month: str,
    banks: PlayerGroup,
) -> list[str]:
    """检测活跃的压力信号。"""
    signals = []

    # Fed 贴现窗口活跃 (>$1B = 有银行求救)
    primary = _get_value(fred_history, "fed_primary_credit", month)
    if primary is not None and primary > 1000:
        signals.append(f"Fed贴现窗口活跃(${primary/1e6:.1f}T)")

    # NFCI 恶化 (>0 = 金融条件紧缩)
    nfci = _get_value(fred_history, "nfci", month)
    if nfci is not None and nfci > 0:
        signals.append(f"NFCI紧缩({nfci:+.2f})")

    # 金融压力指数
    stress = _get_value(fred_history, "financial_stress", month)
    if stress is not None and stress > 1.0:
        signals.append(f"金融压力指数升高({stress:.2f})")

    # CCC 收益率飙升 (>15% = 垃圾债融资困难)
    ccc = _get_value(fred_history, "yield_ccc", month)
    if ccc is not None and ccc > 15:
        signals.append(f"CCC收益率飙升({ccc:.1f}%)")

    # 贷款标准大幅收紧 (>30 = 显著收紧)
    lending = _get_value(fred_history, "lending_standards", month)
    if lending is not None and lending > 30:
        signals.append(f"贷款标准大幅收紧({lending:.0f}%)")

    # 银行准备金骤降 (YoY < -20%)
    reserves_yoy = _yoy_change(fred_history, "bank_reserves", month)
    if reserves_yoy is not None and reserves_yoy < -20:
        signals.append(f"银行准备金骤降({reserves_yoy:+.0f}% YoY)")

    return signals


# ━━ 主入口 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def build_player_map(
    fred_history: dict,
    month: str,
    player_stock_data: Optional[dict] = None,
) -> PlayerMap:
    """构建玩家地图 — 每个主要参与者在做什么。

    Args:
        fred_history: {"indicator_key": {"YYYY-MM": float}} 所有 FRED 数据
        month: 当前月份 "YYYY-MM"
        player_stock_data: 可选, BDC/REIT 股票数据

    Returns:
        PlayerMap — 所有玩家群体状态 + 资金流向 + 压力信号
    """
    # 评估各玩家群体
    banks = _assess_banks(fred_history, month)
    central_banks = _assess_central_banks(fred_history, month)
    private_credit = _assess_private_credit(fred_history, month, player_stock_data)
    foreign_gov = _assess_foreign_governments(fred_history, month)
    retail = _assess_retail(fred_history, month)

    # 资金流向
    capital_flow = _determine_capital_flow(banks, retail, fred_history, month)

    # 压力信号
    stress = _detect_stress_signals(fred_history, month, banks)

    return PlayerMap(
        month=month,
        banks=banks,
        central_banks=central_banks,
        private_credit=private_credit,
        foreign_governments=foreign_gov,
        retail=retail,
        capital_flow_direction=capital_flow,
        stress_signals=stress,
    )


# ━━ 自测 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


if __name__ == "__main__":
    print("=" * 70)
    print("player_tracker.py — 玩家地图自测")
    print("=" * 70)

    # 加载 FRED 缓存
    cache_path = Path(__file__).resolve().parents[3] / "tests" / "data_fred_monthly_history.json"
    with open(cache_path) as f:
        fred_history = json.load(f)
    print(f"Loaded {len(fred_history)} FRED series from cache")

    # 加载 BDC 数据 (如果有)
    stock_path = Path(__file__).resolve().parents[3] / "data" / "player_stock_data.json"
    player_stock_data = None
    if stock_path.exists():
        with open(stock_path) as f:
            player_stock_data = json.load(f)
        print(f"Loaded player stock data: {len(player_stock_data) - 1} tickers")

    # 测试四个关键月份
    test_months = [
        ("2007-06", "次贷危机前夜"),
        ("2008-10", "金融危机顶峰"),
        ("2020-04", "COVID冲击"),
        ("2026-03", "当前"),
    ]

    for month, label in test_months:
        print(f"\n{'─' * 70}")
        print(f"  {label} ({month})")
        print(f"{'─' * 70}")
        pm = build_player_map(fred_history, month, player_stock_data)
        print(pm.summary())
