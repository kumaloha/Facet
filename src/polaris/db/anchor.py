"""
Anchor DB 只读连接
==================
本地开发：SQLite（只读）
云上部署：PostgreSQL / MySQL via SQLAlchemy

所有查询用 SQLAlchemy text() + 命名参数，兼容 SQLite 和 PostgreSQL。
"""

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from polaris.config import settings

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        url = settings.anchor_db_url
        connect_args = {}
        if url.startswith("sqlite"):
            # SQLite 只读模式
            url = url.replace("sqlite:///", "sqlite:///file:", 1)
            if "?" not in url:
                url += "?mode=ro&uri=true"
            connect_args = {"check_same_thread": False}
        _engine = create_engine(url, connect_args=connect_args, pool_pre_ping=True)
    return _engine


def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})


def query_df_safe(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Query that returns empty DataFrame if table doesn't exist yet."""
    try:
        return query_df(sql, params)
    except OperationalError as e:
        if "no such table" in str(e) or "does not exist" in str(e):
            return pd.DataFrame()
        raise


# ── 公司查询 ──────────────────────────────────────────────────────────


def resolve_company_id(ticker: str) -> int | None:
    df = query_df(
        "SELECT id FROM company_profiles WHERE ticker = :ticker",
        {"ticker": ticker},
    )
    return int(df.iloc[0]["id"]) if not df.empty else None


def get_company_profile(company_id: int) -> dict | None:
    df = query_df(
        "SELECT * FROM company_profiles WHERE id = :id",
        {"id": company_id},
    )
    return df.iloc[0].to_dict() if not df.empty else None


def get_periods(company_id: int) -> list[str]:
    """获取某公司所有可用 period（优先从 financial_statements 获取）。"""
    df = query_df(
        "SELECT DISTINCT period FROM financial_statements "
        "WHERE company_id = :cid ORDER BY period",
        {"cid": company_id},
    )
    if df.empty:
        df = query_df(
            "SELECT DISTINCT period FROM operational_issues "
            "WHERE company_id = :cid ORDER BY period",
            {"cid": company_id},
        )
    return df["period"].tolist()


# ── 单期查询（按 company_id + period）──────────────────────────────


def get_financial_line_items(company_id: int, period: str) -> pd.DataFrame:
    return query_df(
        "SELECT fli.* FROM financial_line_items fli "
        "JOIN financial_statements fs ON fli.statement_id = fs.id "
        "WHERE fs.company_id = :cid AND fs.period = :p",
        {"cid": company_id, "p": period},
    )


def get_financial_statements(company_id: int, period: str) -> pd.DataFrame:
    return query_df(
        "SELECT * FROM financial_statements "
        "WHERE company_id = :cid AND period = :p",
        {"cid": company_id, "p": period},
    )


def get_operational_issues(company_id: int, period: str) -> pd.DataFrame:
    return query_df(
        "SELECT * FROM operational_issues "
        "WHERE company_id = :cid AND period = :p",
        {"cid": company_id, "p": period},
    )


def get_downstream_segments(company_id: int, period: str) -> pd.DataFrame:
    return query_df(
        "SELECT * FROM downstream_segments "
        "WHERE company_id = :cid AND period = :p",
        {"cid": company_id, "p": period},
    )


def get_upstream_segments(company_id: int, period: str) -> pd.DataFrame:
    return query_df(
        "SELECT * FROM upstream_segments "
        "WHERE company_id = :cid AND period = :p",
        {"cid": company_id, "p": period},
    )


def get_geographic_revenues(company_id: int, period: str) -> pd.DataFrame:
    return query_df(
        "SELECT * FROM geographic_revenues "
        "WHERE company_id = :cid AND period = :p",
        {"cid": company_id, "p": period},
    )


def get_non_financial_kpis(company_id: int, period: str) -> pd.DataFrame:
    return query_df(
        "SELECT * FROM non_financial_kpis "
        "WHERE company_id = :cid AND period = :p",
        {"cid": company_id, "p": period},
    )


def get_debt_obligations(company_id: int, period: str) -> pd.DataFrame:
    return query_df(
        "SELECT * FROM debt_obligations "
        "WHERE company_id = :cid AND period = :p",
        {"cid": company_id, "p": period},
    )


def get_litigations(company_id: int) -> pd.DataFrame:
    return query_df(
        "SELECT * FROM litigations WHERE company_id = :cid",
        {"cid": company_id},
    )


def get_executive_compensations(company_id: int, period: str) -> pd.DataFrame:
    return query_df(
        "SELECT * FROM executive_compensations "
        "WHERE company_id = :cid AND period = :p",
        {"cid": company_id, "p": period},
    )


def get_stock_ownership(company_id: int, period: str) -> pd.DataFrame:
    return query_df(
        "SELECT * FROM stock_ownership "
        "WHERE company_id = :cid AND period = :p",
        {"cid": company_id, "p": period},
    )


def get_company_narratives(company_id: int) -> pd.DataFrame:
    return query_df(
        "SELECT * FROM company_narratives WHERE company_id = :cid",
        {"cid": company_id},
    )


def get_related_party_transactions(company_id: int, period: str) -> pd.DataFrame:
    return query_df(
        "SELECT * FROM related_party_transactions "
        "WHERE company_id = :cid AND period = :p",
        {"cid": company_id, "p": period},
    )


# ── 多期查询（跨期特征用）──────────────────────────────────────────


def get_financial_line_items_all(company_id: int) -> pd.DataFrame:
    """获取某公司全部期间的财务科目（含 period 列）。"""
    return query_df(
        "SELECT fli.*, fs.period FROM financial_line_items fli "
        "JOIN financial_statements fs ON fli.statement_id = fs.id "
        "WHERE fs.company_id = :cid ORDER BY fs.period",
        {"cid": company_id},
    )


def get_debt_obligations_all(company_id: int) -> pd.DataFrame:
    return query_df(
        "SELECT * FROM debt_obligations "
        "WHERE company_id = :cid ORDER BY period",
        {"cid": company_id},
    )


# ── 新增表查询（安全模式，表不存在时返回空 DataFrame）─────────────


def get_pricing_actions(company_id: int) -> pd.DataFrame:
    return query_df_safe(
        "SELECT * FROM pricing_actions WHERE company_id = :cid",
        {"cid": company_id},
    )


def get_competitor_relations(company_id: int) -> pd.DataFrame:
    return query_df_safe(
        "SELECT * FROM competitor_relations WHERE company_id = :cid",
        {"cid": company_id},
    )


def get_market_share_data(company_id: int) -> pd.DataFrame:
    return query_df_safe(
        "SELECT * FROM market_share_data "
        "WHERE company_id = :cid ORDER BY period",
        {"cid": company_id},
    )


def get_known_issues(company_id: int, period: str) -> pd.DataFrame:
    return query_df_safe(
        "SELECT * FROM known_issues "
        "WHERE company_id = :cid AND period = :p",
        {"cid": company_id, "p": period},
    )


def get_management_acknowledgments(company_id: int, period: str) -> pd.DataFrame:
    return query_df_safe(
        "SELECT * FROM management_acknowledgments "
        "WHERE company_id = :cid AND period = :p",
        {"cid": company_id, "p": period},
    )


def get_insider_transactions(company_id: int) -> pd.DataFrame:
    return query_df_safe(
        "SELECT * FROM insider_transactions "
        "WHERE company_id = :cid ORDER BY transaction_date",
        {"cid": company_id},
    )


def get_executive_changes(company_id: int) -> pd.DataFrame:
    return query_df_safe(
        "SELECT * FROM executive_changes "
        "WHERE company_id = :cid ORDER BY date",
        {"cid": company_id},
    )


def get_audit_opinions(company_id: int, period: str) -> pd.DataFrame:
    return query_df_safe(
        "SELECT * FROM audit_opinions "
        "WHERE company_id = :cid AND period = :p",
        {"cid": company_id, "p": period},
    )


def get_analyst_estimates(company_id: int) -> pd.DataFrame:
    return query_df_safe(
        "SELECT * FROM analyst_estimates "
        "WHERE company_id = :cid ORDER BY period",
        {"cid": company_id},
    )


def get_equity_offerings(company_id: int) -> pd.DataFrame:
    return query_df_safe(
        "SELECT * FROM equity_offerings "
        "WHERE company_id = :cid ORDER BY date",
        {"cid": company_id},
    )


def get_management_guidance(company_id: int) -> pd.DataFrame:
    return query_df_safe(
        "SELECT * FROM management_guidance "
        "WHERE company_id = :cid ORDER BY source_period",
        {"cid": company_id},
    )


# ── 市场数据查询（stock_quotes + macro_indicators）────────────────


def get_latest_stock_quote(ticker: str) -> dict | None:
    """获取某 ticker 最新一条行情。返回 dict 或 None。"""
    df = query_df_safe(
        "SELECT * FROM stock_quotes "
        "WHERE ticker = :ticker ORDER BY trade_date DESC LIMIT 1",
        {"ticker": ticker},
    )
    return df.iloc[0].to_dict() if not df.empty else None


def get_latest_macro(indicator: str) -> float | None:
    """获取某宏观指标最新值。"""
    df = query_df_safe(
        "SELECT value FROM macro_indicators "
        "WHERE indicator = :ind ORDER BY trade_date DESC LIMIT 1",
        {"ind": indicator},
    )
    return float(df.iloc[0]["value"]) if not df.empty else None


def get_macro_series(indicator: str, n: int = 6) -> list[tuple[str, float]]:
    """获取某宏观指标最近 n 条记录（按日期去重）。返回 [(date, value), ...] 按日期升序。"""
    df = query_df_safe(
        "SELECT trade_date, value FROM macro_indicators "
        "WHERE indicator = :ind ORDER BY trade_date DESC LIMIT :n",
        {"ind": indicator, "n": n},
    )
    if df.empty:
        return []
    return [(str(row["trade_date"]), float(row["value"])) for _, row in df.iloc[::-1].iterrows()]


def build_macro_context() -> "MacroContext":
    """从 macro_indicators 表构建达利欧链的 MacroContext。

    每个指标取最新一条。缺失字段保持 None。
    """
    from polaris.chains.dalio import MacroContext
    from datetime import date as date_type

    ctx = MacroContext()

    # 直接可用的指标（值即是链需要的格式）
    direct_mapping = {
        "fed_funds_rate": "fed_funds_rate",
        "treasury_10y": "treasury_10y",       # yfinance 的 ^TNX（百分比）
        "treasury_2y": "treasury_2y",
        "vix": "vix",
        "sp500_earnings_yield": "sp500_earnings_yield",
        "unemployment": "unemployment_rate",
        "gdp_growth": "gdp_growth_actual",
        "cpi_yoy": "cpi_actual",
        "credit_growth": "credit_growth",
        "total_debt_to_gdp": "total_debt_to_gdp",
        "fiscal_deficit_to_gdp": "fiscal_deficit_to_gdp",
    }

    for db_indicator, ctx_field in direct_mapping.items():
        val = get_latest_macro(db_indicator)
        if val is not None:
            # treasury_10y 从 yfinance 来的已经是百分比（如 4.3 表示 4.3%）
            setattr(ctx, ctx_field, val)

    # sp500_earnings_yield 需要转为百分比
    if ctx.sp500_earnings_yield is not None:
        ctx.sp500_earnings_yield = ctx.sp500_earnings_yield * 100

    # 快照日期
    df = query_df_safe(
        "SELECT MAX(trade_date) as latest FROM macro_indicators",
        {},
    )
    if not df.empty and df.iloc[0]["latest"]:
        ctx.snapshot_date = str(df.iloc[0]["latest"])
    else:
        ctx.snapshot_date = str(date_type.today())


    # ── 轨迹信号（momentum / impulse）──────────────────────────────
    trajectory_specs: list[tuple[str, str, int, bool]] = [
        # (db_indicator, ctx_field, n_periods, is_impulse)
        # is_impulse=True → 二阶导数(需要>=3期), False → 一阶导数(需要>=2期)
        ("gdp_growth",    "gdp_momentum",           3, False),
        ("credit_growth", "credit_impulse",         3, True),
        ("cpi_yoy",       "inflation_momentum",     3, False),
        ("fed_funds_rate", "rate_direction",         2, False),
        ("unemployment",  "unemployment_direction",  2, False),
    ]

    for db_ind, ctx_field, n_periods, is_impulse in trajectory_specs:
        series = get_macro_series(db_ind, n=n_periods)
        if is_impulse and len(series) >= 3:
            # 二阶导数: (v[-1] - v[-2]) - (v[-2] - v[-3])
            vals = [v for _, v in series]
            delta_recent = vals[-1] - vals[-2]
            delta_prior = vals[-2] - vals[-3]
            setattr(ctx, ctx_field, round(delta_recent - delta_prior, 4))
        elif not is_impulse and len(series) >= 2:
            # 一阶导数: v[-1] - v[-2]
            vals = [v for _, v in series]
            setattr(ctx, ctx_field, round(vals[-1] - vals[-2], 4))
        # else: 数据不足，保持 None（向后兼容）

    # ── 债务结构拆解 ─────────────────────────────────────────────
    debt_mapping = {
        "household_debt_to_income": "household_debt_to_income",
        "government_debt_to_gdp": "government_debt_to_gdp",
        # corporate_debt_to_gdp 需要特殊处理（FRED 是绝对值，需除以 GDP）
    }
    for db_ind, ctx_field in debt_mapping.items():
        val = get_latest_macro(db_ind)
        if val is not None:
            setattr(ctx, ctx_field, val)

    # ── 历史百分位（用于自适应归一化）──────────────────────────────
    # 取最近 20 年的数据计算百分位
    def _compute_percentiles(indicator: str, n: int = 240) -> tuple[float | None, float | None, float | None]:
        series = get_macro_series(indicator, n=n)
        if len(series) < 20:
            return None, None, None
        vals = sorted([v for _, v in series])
        n_vals = len(vals)
        p25 = vals[int(n_vals * 0.25)]
        p50 = vals[int(n_vals * 0.50)]
        p75 = vals[int(n_vals * 0.75)]
        return p50, p25, p75

    median, p25, p75 = _compute_percentiles("fed_funds_rate")
    ctx.hist_rate_median = median
    ctx.hist_rate_p25 = p25
    ctx.hist_rate_p75 = p75

    median, _, _ = _compute_percentiles("unemployment")
    ctx.hist_unemployment_median = median

    median, _, _ = _compute_percentiles("gdp_growth")
    ctx.hist_gdp_median = median

    return ctx


def get_guidance_dict(company_id: int) -> dict[str, float | None]:
    """将 management_guidance 表转换为 DCF 引擎需要的 dict 格式。

    取每个 metric 最新一条 guidance 的 value_low/value_high 中值。
    """
    df = get_management_guidance(company_id)
    if df.empty:
        return {}

    result: dict[str, float | None] = {}
    # 按 metric 分组，取最新 source_period
    for metric in df["metric"].unique():
        rows = df[df["metric"] == metric].sort_values("source_period", ascending=False)
        row = rows.iloc[0]
        low = row.get("value_low")
        high = row.get("value_high")
        if low is not None and high is not None:
            result[metric] = (float(low) + float(high)) / 2
        elif low is not None:
            result[metric] = float(low)
        elif high is not None:
            result[metric] = float(high)

    return result
