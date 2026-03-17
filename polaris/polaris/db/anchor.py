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
