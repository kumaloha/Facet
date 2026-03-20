"""
跨资产相关性监控
================

检测资产间相关性偏离历史常态 → 最强的系统性风险信号。

核心信号:
  1. 股债相关性翻正 — 分散失效, 2022 经典
  2. 全资产相关性趋向 1 — 流动性危机, 2008.9 / 2020.3
  3. 避险资产失效 — 黄金/债不再反向 = 传统对冲失灵
  4. 相关性骤变 — 突然变化本身就是信号 (regime change)

接入:
  达利欧: 影响 All Weather 的风险预算 (相关性变了→ERC 权重该调)
  索罗斯: 相关性异常 = 市场结构在变 → 背离信号
"""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class CorrelationAlert:
    """一个相关性异常信号。"""
    pair: str                   # "equity-long_term_bond"
    severity: float             # 0-1
    current: float              # 当前相关性
    historical: float           # 历史正常水平
    deviation: float            # 偏离幅度
    detail: str
    implication: str


@dataclass
class CorrelationRegime:
    """相关性体制状态。"""
    # 关键对的当前相关性
    pairs: dict[str, float] = field(default_factory=dict)
    # 异常信号
    alerts: list[CorrelationAlert] = field(default_factory=list)
    # 全局状态
    avg_correlation: float = 0.0        # 所有对的平均相关性 (趋向1=危机)
    diversification_score: float = 1.0  # 0=完全同涨同跌, 1=完美分散
    regime: str = "normal"              # normal / stressed / crisis
    regime_detail: str = ""


# ── 历史正常相关性 (基于 2007-2024 月度数据) ──
# 这些是长期均值, 实际会随周期波动

NORMAL_CORRELATIONS = {
    # (asset_a, asset_b): (mean, std) — 基于月度回报
    ("equity", "long_term_bond"): (-0.20, 0.30),      # 股债通常负相关
    ("equity", "intermediate_bond"): (-0.15, 0.25),
    ("equity", "gold"): (0.00, 0.25),                  # 股金弱相关
    ("equity", "commodity"): (0.40, 0.25),              # 股商品正相关(顺周期)
    ("equity", "em_bond"): (0.50, 0.20),                # 股EM强正相关
    ("long_term_bond", "gold"): (0.15, 0.25),           # 避险双雄弱正相关
    ("long_term_bond", "commodity"): (-0.15, 0.25),     # 债商品弱负相关
    ("gold", "commodity"): (0.25, 0.25),                # 实物资产弱正相关
}

# 哪些对的相关性变化最重要
CRITICAL_PAIRS = [
    ("equity", "long_term_bond"),    # 最重要: 分散有效性的基石
    ("equity", "gold"),              # 避险资产是否还在工作
    ("long_term_bond", "gold"),      # 避险双雄是否同向
]


def compute_rolling_correlations(
    monthly_returns: dict[str, dict[str, float]],
    window: int = 12,
    end_month: str | None = None,
) -> dict[str, float]:
    """计算滚动相关性。

    Args:
        monthly_returns: {"2024-01": {"equity": 2.5, "long_term_bond": -1.0, ...}, ...}
        window: 滚动窗口 (月)
        end_month: 截止月 (含), None=用最新月

    Returns: {pair_key: correlation}
    """
    months = sorted(monthly_returns.keys())
    if end_month:
        months = [m for m in months if m <= end_month]
    if len(months) < window:
        return {}

    recent = months[-window:]

    # 收集各资产月度回报
    assets = set()
    for m in recent:
        assets.update(monthly_returns[m].keys())

    asset_returns: dict[str, list[float]] = {}
    for asset in assets:
        rets = []
        for m in recent:
            rets.append(monthly_returns[m].get(asset, 0))
        asset_returns[asset] = rets

    # 计算两两相关性
    correlations = {}
    asset_list = sorted(assets)
    for i, a in enumerate(asset_list):
        for b in asset_list[i + 1:]:
            ra = asset_returns[a]
            rb = asset_returns[b]
            corr = _pearson(ra, rb)
            if corr is not None:
                key = f"{a}-{b}"
                correlations[key] = round(corr, 3)

    return correlations


def _pearson(x: list[float], y: list[float]) -> float | None:
    """Pearson 相关系数。"""
    n = len(x)
    if n < 3:
        return None
    mx = sum(x) / n
    my = sum(y) / n
    dx = [xi - mx for xi in x]
    dy = [yi - my for yi in y]
    sxy = sum(a * b for a, b in zip(dx, dy))
    sxx = sum(a * a for a in dx)
    syy = sum(b * b for b in dy)
    if sxx == 0 or syy == 0:
        return None
    return sxy / (sxx * syy) ** 0.5


def analyze_correlation_regime(
    monthly_returns: dict[str, dict[str, float]],
    end_month: str | None = None,
    short_window: int = 6,
    long_window: int = 36,
) -> CorrelationRegime:
    """分析相关性体制。

    用短窗口(6月) vs 长窗口(36月) 对比检测相关性骤变。
    """
    result = CorrelationRegime()

    short_corr = compute_rolling_correlations(monthly_returns, short_window, end_month)
    long_corr = compute_rolling_correlations(monthly_returns, long_window, end_month)
    result.pairs = short_corr

    if not short_corr:
        result.regime = "unknown"
        result.regime_detail = "数据不足"
        return result

    # ── 检测每一对的异常 ──
    for pair_key, current in short_corr.items():
        parts = pair_key.split("-")
        if len(parts) != 2:
            continue
        a, b = parts

        # 查历史正常值
        normal = NORMAL_CORRELATIONS.get((a, b)) or NORMAL_CORRELATIONS.get((b, a))
        hist_mean = normal[0] if normal else 0
        hist_std = normal[1] if normal else 0.3

        # 偏离度 = (当前 - 历史均值) / 历史标准差
        deviation = (current - hist_mean) / hist_std if hist_std > 0 else 0

        # 长窗口作为对比基准
        long_val = long_corr.get(pair_key, hist_mean)
        short_long_shift = current - long_val

        # 判断是否异常
        alert = None
        pair_tuple = (a, b)

        # 1. 股债相关性翻正 (最重要)
        if pair_tuple in (("equity", "long_term_bond"), ("long_term_bond", "equity")):
            if current > 0.2 and hist_mean < 0:
                alert = CorrelationAlert(
                    pair=pair_key, severity=min(1.0, (current - 0) / 0.5),
                    current=current, historical=hist_mean, deviation=deviation,
                    detail=f"股债相关性翻正: {current:+.2f} (正常{hist_mean:+.2f})",
                    implication=(
                        "分散失效 — All Weather 的核心假设(股债负相关)被打破. "
                        "2022经典: 加息同时打击股和债. "
                        "需要: 增配现金/大宗, 降低整体杠杆"
                    ),
                )
            elif current > -0.05 and hist_mean < -0.15:
                alert = CorrelationAlert(
                    pair=pair_key, severity=0.4,
                    current=current, historical=hist_mean, deviation=deviation,
                    detail=f"股债相关性接近零: {current:+.2f} (正常{hist_mean:+.2f})",
                    implication="股债分散效果在减弱, 接近失效临界点",
                )

        # 2. 避险资产失灵 (黄金不再对冲)
        if pair_tuple in (("equity", "gold"), ("gold", "equity")):
            if current > 0.5:
                alert = CorrelationAlert(
                    pair=pair_key, severity=0.6,
                    current=current, historical=hist_mean, deviation=deviation,
                    detail=f"股金同涨同跌: {current:+.2f}",
                    implication="黄金失去避险功能, 可能是美元/利率驱动而非避险需求",
                )

        # 3. 通用: 相关性骤变 (短窗口 vs 长窗口偏离 > 0.4)
        if alert is None and abs(short_long_shift) > 0.4:
            direction = "急升" if short_long_shift > 0 else "急降"
            alert = CorrelationAlert(
                pair=pair_key, severity=min(0.7, abs(short_long_shift) / 0.8),
                current=current, historical=long_val, deviation=short_long_shift,
                detail=f"相关性骤变: {a}-{b} 从{long_val:+.2f}→{current:+.2f} ({direction})",
                implication="市场结构在变化, 旧的分散假设可能失效",
            )

        if alert:
            result.alerts.append(alert)

    result.alerts.sort(key=lambda x: -x.severity)

    # ── 全局指标 ──
    all_corrs = list(short_corr.values())
    if all_corrs:
        result.avg_correlation = round(statistics.mean(all_corrs), 3)
        # 分散分 = 1 - 平均|相关性| (越分散越好)
        result.diversification_score = round(
            1.0 - statistics.mean(abs(c) for c in all_corrs), 3
        )

    # ── 体制判断 ──
    high_severity = [a for a in result.alerts if a.severity > 0.6]
    if result.avg_correlation > 0.5 or len(high_severity) >= 2:
        result.regime = "crisis"
        result.regime_detail = (
            f"相关性危机: 平均相关性{result.avg_correlation:+.2f}, "
            f"{len(high_severity)}个严重异常. 分散失效, 所有资产同向移动"
        )
    elif result.avg_correlation > 0.3 or high_severity:
        result.regime = "stressed"
        result.regime_detail = (
            f"相关性紧张: 平均{result.avg_correlation:+.2f}, "
            f"分散效果{result.diversification_score:.0%}. 部分对冲在失效"
        )
    else:
        result.regime = "normal"
        result.regime_detail = (
            f"相关性正常: 平均{result.avg_correlation:+.2f}, "
            f"分散效果{result.diversification_score:.0%}"
        )

    return result


# ══════════════════════════════════════════════════════════════
#  个股相关性: 达利欧选股约束 — "和已有的相关性低不低"
# ══════════════════════════════════════════════════════════════


@dataclass
class StockCorrelation:
    """两只股票之间的相关性。"""
    ticker_a: str
    ticker_b: str
    correlation: float
    period_months: int = 12


@dataclass
class PortfolioDiversification:
    """一组持仓的分散度评估。"""
    holdings: list[str]
    # 相关性矩阵
    correlations: dict[str, float] = field(default_factory=dict)  # "NVDA-GOOGL": 0.46
    # 平均相关性 (越低越好)
    avg_correlation: float = 0.0
    # 最高相关对 (最不分散的)
    most_correlated: StockCorrelation | None = None
    # 最低相关对 (最分散的)
    least_correlated: StockCorrelation | None = None
    # 分散评分 0-1 (1=完美分散)
    diversification_score: float = 0.0
    # 建议: 加什么能改善分散
    suggestions: list[str] = field(default_factory=list)


def evaluate_stock_diversification(
    holdings: list[str],
    candidates: list[str] | None = None,
    period: str = "1y",
) -> PortfolioDiversification:
    """评估一组持仓的分散度，建议下一步加什么。

    Args:
        holdings: 当前持仓 ["NVDA", "GOOGL", ...]
        candidates: 候选加入的股票 (巴菲特选出的好公司)
        period: 回看期

    Returns:
        PortfolioDiversification: 分散度评估 + 建议
    """
    import yfinance as yf

    result = PortfolioDiversification(holdings=list(holdings))

    all_tickers = list(holdings) + (candidates or [])
    if len(all_tickers) < 2:
        return result

    try:
        data = yf.download(all_tickers, period=period, auto_adjust=True, progress=False)["Close"]
        returns = data.pct_change().dropna()
    except Exception:
        return result

    if returns.empty or len(returns) < 20:
        return result

    corr_matrix = returns.corr()

    # 持仓间的相关性
    pair_corrs = []
    for i, a in enumerate(holdings):
        for b in holdings[i + 1:]:
            if a in corr_matrix.columns and b in corr_matrix.columns:
                c = float(corr_matrix.loc[a, b])
                key = f"{a}-{b}"
                result.correlations[key] = round(c, 3)
                pair_corrs.append(StockCorrelation(a, b, round(c, 3)))

    if pair_corrs:
        result.avg_correlation = round(
            sum(p.correlation for p in pair_corrs) / len(pair_corrs), 3
        )
        result.most_correlated = max(pair_corrs, key=lambda p: p.correlation)
        result.least_correlated = min(pair_corrs, key=lambda p: p.correlation)
        # 分散分 = 1 - 平均|相关性|
        result.diversification_score = round(
            max(0, 1.0 - sum(abs(p.correlation) for p in pair_corrs) / len(pair_corrs)),
            3,
        )

    # 候选股: 哪个加了最能改善分散
    if candidates:
        candidate_scores = []
        for cand in candidates:
            if cand in holdings or cand not in corr_matrix.columns:
                continue
            # 计算候选和所有持仓的平均相关性
            corrs_with_holdings = []
            for h in holdings:
                if h in corr_matrix.columns:
                    corrs_with_holdings.append(abs(float(corr_matrix.loc[cand, h])))
            if corrs_with_holdings:
                avg_corr = sum(corrs_with_holdings) / len(corrs_with_holdings)
                candidate_scores.append((cand, avg_corr))

        # 按与持仓的平均相关性排序 (越低越好=分散效果越好)
        candidate_scores.sort(key=lambda x: x[1])
        for ticker, avg_c in candidate_scores[:3]:
            result.suggestions.append(
                f"加 {ticker} (与现有持仓平均相关性 {avg_c:.2f}) — 分散效果{'好' if avg_c < 0.3 else '一般' if avg_c < 0.5 else '差'}"
            )

    return result


def format_stock_diversification(div: PortfolioDiversification) -> str:
    """格式化个股分散度报告。"""
    lines = [""]
    lines.append("  持仓分散度 (达利欧选股约束)")
    lines.append("  ════════════════════════════════════════════════")
    lines.append(f"  持仓: {', '.join(div.holdings)}")

    bar = "█" * int(div.diversification_score * 10) + "░" * (10 - int(div.diversification_score * 10))
    lines.append(f"  分散评分: [{bar}] {div.diversification_score:.0%}")
    lines.append(f"  平均相关性: {div.avg_correlation:+.2f} ({'好' if abs(div.avg_correlation) < 0.3 else '一般' if abs(div.avg_correlation) < 0.5 else '差，太集中'})")

    if div.most_correlated:
        mc = div.most_correlated
        lines.append(f"  最集中: {mc.ticker_a}-{mc.ticker_b} = {mc.correlation:+.2f} ← 考虑减一个")
    if div.least_correlated:
        lc = div.least_correlated
        lines.append(f"  最分散: {lc.ticker_a}-{lc.ticker_b} = {lc.correlation:+.2f}")

    if div.correlations:
        lines.append(f"\n  相关性矩阵:")
        for pair, corr in sorted(div.correlations.items(), key=lambda x: -abs(x[1])):
            bar = "+" * int(max(corr * 8, 0)) + "-" * int(max(-corr * 8, 0))
            lines.append(f"    {pair:18s} {corr:+.2f} {bar}")

    if div.suggestions:
        lines.append(f"\n  达利欧建议 (加什么改善分散):")
        for s in div.suggestions:
            lines.append(f"    → {s}")

    lines.append("")
    return "\n".join(lines)


def format_correlation_regime(regime: CorrelationRegime) -> str:
    """格式化相关性体制报告。"""
    lines = [""]
    lines.append("  跨资产相关性监控")
    lines.append("  ════════════════════════════════════════════════")

    regime_icons = {"normal": "●", "stressed": "◐", "crisis": "○"}
    icon = regime_icons.get(regime.regime, "?")
    lines.append(f"\n  体制: {icon} {regime.regime_detail}")
    lines.append(f"  分散效果: {regime.diversification_score:.0%}")

    # 关键对的相关性矩阵
    if regime.pairs:
        lines.append(f"\n  关键相关性 (6月滚动):")
        for pair_key in sorted(regime.pairs):
            corr = regime.pairs[pair_key]
            parts = pair_key.split("-")
            if len(parts) != 2:
                continue
            a, b = parts
            normal = NORMAL_CORRELATIONS.get((a, b)) or NORMAL_CORRELATIONS.get((b, a))
            hist = normal[0] if normal else 0
            # 只显示关键对
            if (a, b) in NORMAL_CORRELATIONS or (b, a) in NORMAL_CORRELATIONS:
                bar_len = int(abs(corr) * 10)
                bar = ("+" * bar_len) if corr >= 0 else ("-" * bar_len)
                drift = corr - hist
                drift_str = f"({drift:+.2f})" if abs(drift) > 0.1 else ""
                lines.append(f"    {a:20s}-{b:20s} {corr:+.2f} {bar:10s} 常态{hist:+.2f} {drift_str}")

    # 异常信号
    if regime.alerts:
        lines.append(f"\n  异常信号:")
        for alert in regime.alerts:
            bar = "!" * int(alert.severity * 5)
            lines.append(f"    [{bar:5s}] {alert.detail}")
            lines.append(f"           {alert.implication}")

    lines.append("")
    return "\n".join(lines)
