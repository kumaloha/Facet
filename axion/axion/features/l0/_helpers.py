"""
特征计算辅助函数
================
所有 L0 特征共用的数据读取和计算工具。
"""

import pandas as pd

from axion.features.types import ComputeContext


def get_item(ctx: ComputeContext, item_key: str) -> float | None:
    """从 financial_line_items 中查找某个科目的值（当期）。"""
    df = ctx.get_financial_line_items()
    if df.empty:
        return None
    matches = df[df["item_key"] == item_key]
    if matches.empty:
        return None
    return float(matches.iloc[0]["value"])


def get_item_series(
    ctx: ComputeContext, item_key: str, n_periods: int | None = None
) -> pd.Series:
    """获取某科目跨期的时间序列。

    返回按 period 排序的 Series，index=period, values=float。
    """
    df = ctx.get_financial_line_items_history(n_periods=n_periods)
    if df.empty:
        return pd.Series(dtype=float)
    items = df[df["item_key"] == item_key][["period", "value"]].drop_duplicates(
        subset=["period"]
    )
    if items.empty:
        return pd.Series(dtype=float)
    return items.set_index("period")["value"].sort_index().astype(float)


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
