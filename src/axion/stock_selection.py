"""
选股模块: 巴菲特持仓 + 相关性过滤
==================================

用伯克希尔13F持仓作为选股来源，叠加相关性过滤减少集中度。
在回测中替代equity ETF(SPY)仓位。
"""

from __future__ import annotations

import json
import statistics
from pathlib import Path

_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
_TESTS_DIR = Path(__file__).resolve().parent.parent.parent / "tests"


def _load_json(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def load_berkshire_holdings() -> dict[str, dict[str, float]]:
    """加载伯克希尔季度持仓。返回 {"2007-Q1": {"KO": 0.18, ...}}"""
    return _load_json(_DATA_DIR / "berkshire_holdings.json")


def load_stock_returns() -> dict[str, dict[str, float]]:
    """加载个股月度回报。返回 {"AAPL": {"2007-01": 2.5, ...}}"""
    return _load_json(_DATA_DIR / "berkshire_stock_returns.json")


def _month_to_quarter(month: str) -> str:
    """'2007-03' → '2007-Q1'"""
    y, m = month.split("-")
    q = (int(m) - 1) // 3 + 1
    return f"{y}-Q{q}"


def _prev_quarter(quarter: str) -> str:
    """'2007-Q2' → '2007-Q1'"""
    y, q = quarter.split("-Q")
    q = int(q)
    if q == 1:
        return f"{int(y) - 1}-Q4"
    return f"{y}-Q{q - 1}"


def _compute_correlation(returns_a: list[float], returns_b: list[float]) -> float:
    """计算两个回报序列的相关系数。"""
    if len(returns_a) < 6 or len(returns_b) < 6:
        return 0.0
    n = min(len(returns_a), len(returns_b))
    a = returns_a[-n:]
    b = returns_b[-n:]
    mean_a = sum(a) / n
    mean_b = sum(b) / n
    cov = sum((a[i] - mean_a) * (b[i] - mean_b) for i in range(n)) / n
    std_a = (sum((x - mean_a) ** 2 for x in a) / n) ** 0.5
    std_b = (sum((x - mean_b) ** 2 for x in b) / n) ** 0.5
    if std_a == 0 or std_b == 0:
        return 0.0
    return cov / (std_a * std_b)


def filter_by_correlation(
    tickers: list[str],
    weights: dict[str, float],
    stock_returns: dict[str, dict[str, float]],
    month: str,
    max_corr: float = 0.7,
    lookback_months: int = 36,
) -> dict[str, float]:
    """相关性过滤: 去掉高相关的股票，保留权重大的。

    逻辑:
    1. 按权重从大到小排序
    2. 逐个加入，如果与已加入的任一股票相关性>max_corr，跳过
    3. 返回过滤后的持仓（重新归一化权重）
    """
    # 获取每只股票到month为止的回报序列
    def _get_returns(ticker: str) -> list[float]:
        rets = stock_returns.get(ticker, {})
        sorted_months = sorted(m for m in rets if m <= month)
        return [rets[m] for m in sorted_months[-lookback_months:]]

    sorted_tickers = sorted(tickers, key=lambda t: weights.get(t, 0), reverse=True)

    selected = []
    selected_returns = []

    for ticker in sorted_tickers:
        rets = _get_returns(ticker)
        if len(rets) < 6:
            continue

        # 检查与已选股票的相关性
        too_correlated = False
        for existing_rets in selected_returns:
            corr = _compute_correlation(rets, existing_rets)
            if abs(corr) > max_corr:
                too_correlated = True
                break

        if not too_correlated:
            selected.append(ticker)
            selected_returns.append(rets)

    # 重新归一化权重
    if not selected:
        return {}

    total = sum(weights.get(t, 0) for t in selected)
    if total == 0:
        # 等权
        w = 1.0 / len(selected)
        return {t: w for t in selected}
    return {t: weights.get(t, 0) / total for t in selected}


def get_buffett_portfolio(
    month: str,
    holdings: dict[str, dict[str, float]],
    stock_returns: dict[str, dict[str, float]],
    max_corr: float = 0.7,
) -> dict[str, float]:
    """获取某月的巴菲特选股组合（含相关性过滤）。

    用最近可用的13F持仓（季度滞后，模拟真实信息延迟）。
    """
    quarter = _month_to_quarter(month)
    # 13F有45天延迟，用上一季度的持仓
    prev_q = _prev_quarter(quarter)

    # 找最近可用的持仓
    h = holdings.get(prev_q) or holdings.get(_prev_quarter(prev_q))
    if not h:
        return {}

    # 只保留有回报数据的股票
    available = {t: w for t, w in h.items() if t in stock_returns}
    if not available:
        return {}

    # 相关性过滤
    filtered = filter_by_correlation(
        list(available.keys()), available, stock_returns, month, max_corr
    )
    return filtered


def compute_stock_portfolio_return(
    portfolio: dict[str, float],
    stock_returns: dict[str, dict[str, float]],
    month: str,
) -> float | None:
    """计算选股组合的月回报（%）。"""
    if not portfolio:
        return None

    total_ret = 0.0
    total_weight = 0.0
    for ticker, weight in portfolio.items():
        ret = stock_returns.get(ticker, {}).get(month)
        if ret is not None:
            total_ret += weight * ret
            total_weight += weight

    if total_weight == 0:
        return None

    # 归一化（处理某些股票当月无数据的情况）
    return total_ret / total_weight * (total_weight / sum(portfolio.values()))


if __name__ == "__main__":
    print("=" * 60)
    print("选股模块自测")
    print("=" * 60)

    holdings = load_berkshire_holdings()
    stock_returns = load_stock_returns()

    print(f"Holdings: {len(holdings)} quarters")
    print(f"Stock returns: {len(stock_returns)} tickers")

    # 测试几个季度
    for month in ["2007-06", "2012-06", "2018-06", "2023-06"]:
        portfolio = get_buffett_portfolio(month, holdings, stock_returns)
        ret = compute_stock_portfolio_return(portfolio, stock_returns, month)
        n = len(portfolio)
        top3 = sorted(portfolio.items(), key=lambda x: -x[1])[:3]
        print(f"\n{month}: {n} stocks, return={ret:.1f}%" if ret else f"\n{month}: {n} stocks, no return data")
        for t, w in top3:
            print(f"  {t:6s} {w:.1%}")
