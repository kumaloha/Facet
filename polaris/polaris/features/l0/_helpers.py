"""
特征计算辅助函数
================
所有 L0 特征共用的数据读取和计算工具。
"""

import pandas as pd

from polaris.features.types import ComputeContext


# 别名映射：特征代码期望的 key → yfinance 实际的 key
# 让特征代码不需要关心数据源的命名差异
_ITEM_KEY_ALIASES: dict[str, list[str]] = {
    "revenue": ["total_revenue", "operating_revenue"],
    "depreciation_amortization": [
        "reconciled_depreciation",
        "depreciation_and_amortization",
        "depreciation_amortization_depletion",
    ],
    "capital_expenditures": ["capital_expenditure", "capital_expenditure_reported"],
    "operating_cash_flow": [
        "cash_flow_from_continuing_operating_activities",
        "operating_cash_flow",
    ],
    "total_equity": ["stockholders_equity", "common_stock_equity", "total_equity_gross_minority_interest"],
    "total_liabilities": ["total_liabilities_net_minority_interest"],
    "total_debt": ["total_debt", "long_term_debt_and_capital_lease_obligation"],
    "interest_expense": ["interest_expense", "interest_expense_non_operating"],
    "current_assets": ["current_assets"],
    "current_liabilities": ["current_liabilities"],
    "accounts_receivable": ["accounts_receivable", "receivables"],
    "inventory": ["inventory"],
    "goodwill": ["goodwill"],
    "share_repurchase": ["repurchase_of_capital_stock"],
    "dividends_paid": ["cash_dividends_paid", "common_stock_dividend_paid"],
    "shares_outstanding": ["ordinary_shares_number", "share_issued", "diluted_average_shares"],
}


def get_item(ctx: ComputeContext, item_key: str) -> float | None:
    """从 financial_line_items 中查找某个科目的值（当期）。

    先精确匹配 item_key，找不到则尝试别名。
    """
    df = ctx.get_financial_line_items()
    if df.empty:
        return None

    # 精确匹配
    matches = df[df["item_key"] == item_key]
    if not matches.empty:
        return float(matches.iloc[0]["value"])

    # 别名匹配
    for alias in _ITEM_KEY_ALIASES.get(item_key, []):
        matches = df[df["item_key"] == alias]
        if not matches.empty:
            return float(matches.iloc[0]["value"])

    return None


def get_item_series(
    ctx: ComputeContext, item_key: str, n_periods: int | None = None
) -> pd.Series:
    """获取某科目跨期的时间序列。

    返回按 period 排序的 Series，index=period, values=float。
    先精确匹配 item_key，找不到则尝试别名。
    """
    df = ctx.get_financial_line_items_history(n_periods=n_periods)
    if df.empty:
        return pd.Series(dtype=float)

    # 尝试的 key 列表：原始 key + 别名
    keys_to_try = [item_key] + _ITEM_KEY_ALIASES.get(item_key, [])

    for key in keys_to_try:
        items = df[df["item_key"] == key][["period", "value"]].drop_duplicates(
            subset=["period"]
        )
        if not items.empty:
            return items.set_index("period")["value"].sort_index().astype(float)

    return pd.Series(dtype=float)


def safe_div(numerator: float | None, denominator: float | None) -> float | None:
    """安全除法：任一操作数为 None 或除数为 0 时返回 None。"""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def ratio(ctx: ComputeContext, num_key: str, den_key: str) -> float | None:
    """计算两个财务科目的比率。"""
    return safe_div(get_item(ctx, num_key), get_item(ctx, den_key))


def yoy_growth(series: pd.Series) -> float | None:
    """计算同比增速。

    季度数据（≥5 期）：当期 vs 4 期前（同季度同比）。
    年度数据（<5 期）：当期 vs 上一期。
    """
    if len(series) < 2:
        return None
    current = series.iloc[-1]
    prior = series.iloc[-5] if len(series) >= 5 else series.iloc[0]
    if prior == 0:
        return None
    return (current - prior) / abs(prior)


def stability(series: pd.Series, min_periods: int = 4) -> float | None:
    """计算标准差（越低越稳定）。"""
    if len(series) < min_periods:
        return None
    return float(series.std())


def consecutive_positive(series: pd.Series) -> int:
    """从序列末尾向前计数连续正值。"""
    count = 0
    for val in reversed(series.values):
        if val > 0:
            count += 1
        else:
            break
    return count


def consecutive_growth(series: pd.Series) -> int:
    """从序列末尾向前计数连续增长期数。"""
    if len(series) < 2:
        return 0
    diffs = series.diff().dropna()
    count = 0
    for val in reversed(diffs.values):
        if val > 0:
            count += 1
        else:
            break
    return count
