"""
市场数据采集
============
从 yfinance 拉取个股行情和宏观指标，写入 stock_quotes / macro_indicators。

用法：
    anchor market-update                    # 更新所有已跟踪公司 + 宏观指标
    anchor market-update --ticker NVDA      # 只更新一只
    anchor market-update --macro-only       # 只更新宏观指标
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Optional

import yfinance as yf
from loguru import logger
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from anchor.database.session import AsyncSessionLocal, create_tables
from anchor.models import (
    CompanyProfile,
    FinancialLineItem,
    FinancialStatement,
    MacroIndicator,
    StockQuote,
    _utcnow,
)

# ── 宏观指标 Ticker 映射 ─────────────────────────────────────────────

MACRO_TICKERS = {
    "treasury_10y": "^TNX",           # 10 年期美国国债收益率
    "vix": "^VIX",                    # CBOE 波动率指数
    "sp500": "^GSPC",                 # S&P 500 指数
}


# ── 个股数据 ─────────────────────────────────────────────────────────


async def update_stock_quotes(
    session: AsyncSession,
    ticker: str,
    days: int = 30,
    company_id: int | None = None,
) -> int:
    """拉取个股日行情，写入 stock_quotes。返回新增行数。"""
    end = date.today()
    start = end - timedelta(days=days)

    try:
        yf_ticker = yf.Ticker(ticker)
        hist = yf_ticker.history(start=start.isoformat(), end=end.isoformat())
    except Exception as e:
        logger.warning(f"yfinance failed for {ticker}: {e}")
        return 0

    if hist.empty:
        logger.info(f"No data for {ticker}")
        return 0

    # 获取 shares_outstanding 和 market_cap
    info = {}
    try:
        info = yf_ticker.info or {}
    except Exception:
        pass

    shares = info.get("sharesOutstanding")
    mcap = info.get("marketCap")

    # 查已有日期，避免重复
    existing = await _get_existing_dates(session, "stock_quotes", ticker)

    count = 0
    now = _utcnow()
    for idx, row in hist.iterrows():
        d = idx.date() if hasattr(idx, "date") else idx
        if d in existing:
            continue

        session.add(StockQuote(
            company_id=company_id,
            ticker=ticker,
            trade_date=d,
            price_open=_safe(row.get("Open")),
            price_high=_safe(row.get("High")),
            price_low=_safe(row.get("Low")),
            price_close=_safe(row.get("Close")),
            volume=int(row["Volume"]) if row.get("Volume") else None,
            shares_outstanding=shares,
            market_cap=mcap,
            created_at=now,
        ))
        count += 1

    if count > 0:
        await session.flush()
        logger.info(f"Stock quotes: {ticker} +{count} rows")

    return count


# ── 宏观指标 ─────────────────────────────────────────────────────────


async def update_macro_indicators(
    session: AsyncSession,
    days: int = 30,
) -> int:
    """拉取宏观指标，写入 macro_indicators。返回新增行数。"""
    end = date.today()
    start = end - timedelta(days=days)
    total = 0
    now = _utcnow()

    for indicator_name, yf_ticker in MACRO_TICKERS.items():
        try:
            hist = yf.download(
                yf_ticker,
                start=start.isoformat(),
                end=end.isoformat(),
                progress=False,
            )
        except Exception as e:
            logger.warning(f"yfinance macro failed for {indicator_name}: {e}")
            continue

        if hist.empty:
            continue

        existing = await _get_existing_macro_dates(session, indicator_name)

        count = 0
        for idx, row in hist.iterrows():
            d = idx.date() if hasattr(idx, "date") else idx
            if d in existing:
                continue

            close = row.get("Close")
            if close is None:
                continue
            # yfinance download 返回的 Close 可能是 Series
            val = float(close.iloc[0]) if hasattr(close, "iloc") else float(close)

            session.add(MacroIndicator(
                trade_date=d,
                indicator=indicator_name,
                value=val,
                source="yfinance",
                created_at=now,
            ))
            count += 1

        if count > 0:
            await session.flush()
            total += count
            logger.info(f"Macro: {indicator_name} +{count} rows")

    # S&P 500 earnings yield（从 PE ratio 倒推）
    total += await _update_sp500_earnings_yield(session, days)

    return total


async def _update_sp500_earnings_yield(
    session: AsyncSession,
    days: int,
) -> int:
    """从 S&P 500 的 trailing PE 计算 earnings yield。"""
    try:
        sp = yf.Ticker("^GSPC")
        info = sp.info or {}
    except Exception:
        return 0

    pe = info.get("trailingPE")
    if pe is None or pe <= 0:
        return 0

    earnings_yield = 1.0 / pe
    today = date.today()

    existing = await _get_existing_macro_dates(session, "sp500_earnings_yield")
    if today in existing:
        return 0

    session.add(MacroIndicator(
        date=today,
        indicator="sp500_earnings_yield",
        value=earnings_yield,
        source="yfinance",
        created_at=_utcnow(),
    ))
    await session.flush()
    logger.info(f"Macro: sp500_earnings_yield = {earnings_yield:.4f}")
    return 1


# ── 统一入口 ─────────────────────────────────────────────────────────


async def market_update(
    ticker: str | None = None,
    macro_only: bool = False,
    days: int = 30,
) -> dict:
    """统一入口：更新市场数据。

    Returns: {"stocks": int, "macro": int}
    """
    await create_tables()

    async with AsyncSessionLocal() as session:
        result = {"stocks": 0, "macro": 0}

        # 宏观指标（独立事务，失败不影响个股）
        try:
            result["macro"] = await update_macro_indicators(session, days=days)
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.warning(f"Macro update failed: {e}")

        if not macro_only:
            if ticker:
                # 单只
                company_id = await _resolve_company_id(session, ticker)
                result["stocks"] = await update_stock_quotes(
                    session, ticker, days=days, company_id=company_id
                )
            else:
                # 全部已跟踪公司
                tickers = await _get_tracked_tickers(session)
                for t, cid in tickers:
                    n = await update_stock_quotes(session, t, days=days, company_id=cid)
                    result["stocks"] += n

            await session.commit()

    return result


# ── 工具函数 ─────────────────────────────────────────────────────────


def _safe(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


async def _get_existing_dates(session: AsyncSession, table: str, ticker: str) -> set[date]:
    """获取已有日期，避免重复插入。"""
    stmt = select(StockQuote.trade_date).where(StockQuote.ticker == ticker)
    result = await session.exec(stmt)
    return {row for row in result.all()}


async def _get_existing_macro_dates(session: AsyncSession, indicator: str) -> set[date]:
    stmt = select(MacroIndicator.trade_date).where(MacroIndicator.indicator == indicator)
    result = await session.exec(stmt)
    return {row for row in result.all()}


async def _resolve_company_id(session: AsyncSession, ticker: str) -> int | None:
    stmt = select(CompanyProfile.id).where(CompanyProfile.ticker == ticker)
    result = await session.exec(stmt)
    return result.first()


async def _get_tracked_tickers(session: AsyncSession) -> list[tuple[str, int]]:
    """获取所有已有公司的 ticker 和 id。"""
    stmt = select(CompanyProfile.ticker, CompanyProfile.id)
    result = await session.exec(stmt)
    return list(result.all())


# ── 三表数据采集（yfinance → financial_statements + financial_line_items）──


def _item_key(label: str) -> str:
    """将 yfinance 的 line item label 转为 snake_case item_key。"""
    return label.strip().lower().replace(" ", "_").replace("-", "_")


def _period_label(ts) -> str:
    """将 yfinance 的 Timestamp 列名转为 period 字符串。

    yfinance 返回的是财年结束日期，如 2026-01-31 → FY2026。
    """
    if hasattr(ts, "year"):
        return f"FY{ts.year}"
    return str(ts)


async def _get_or_create_company(
    session: AsyncSession, ticker: str, yf_info: dict,
) -> int:
    """获取或创建公司档案，返回 company_id。"""
    cid = await _resolve_company_id(session, ticker)
    if cid is not None:
        return cid

    company = CompanyProfile(
        name=yf_info.get("longName") or yf_info.get("shortName") or ticker,
        ticker=ticker,
        market="us",
        industry=yf_info.get("industry") or "",
        summary=yf_info.get("longBusinessSummary", "")[:500] if yf_info.get("longBusinessSummary") else "",
    )
    session.add(company)
    await session.flush()
    return company.id


async def _get_existing_periods(
    session: AsyncSession, company_id: int, stmt_type: str,
) -> set[str]:
    """获取已有的 period，避免重复。"""
    stmt = select(FinancialStatement.period).where(
        FinancialStatement.company_id == company_id,
        FinancialStatement.statement_type == stmt_type,
    )
    result = await session.exec(stmt)
    return {row for row in result.all()}


async def update_financials(
    session: AsyncSession,
    ticker: str,
) -> dict[str, int]:
    """从 yfinance 拉取三表数据，写入 financial_statements + financial_line_items。

    Returns: {"statements": int, "line_items": int}
    """
    try:
        yf_ticker = yf.Ticker(ticker)
        info = yf_ticker.info or {}
    except Exception as e:
        logger.warning(f"yfinance info failed for {ticker}: {e}")
        return {"statements": 0, "line_items": 0}

    company_id = await _get_or_create_company(session, ticker, info)
    now = _utcnow()
    currency = info.get("currency", "USD")

    stmt_count = 0
    item_count = 0

    tables = [
        ("income", yf_ticker.financials),
        ("balance_sheet", yf_ticker.balance_sheet),
        ("cashflow", yf_ticker.cashflow),
    ]

    for stmt_type, df in tables:
        if df is None or df.empty:
            logger.info(f"{ticker}/{stmt_type}: no data")
            continue

        existing_periods = await _get_existing_periods(session, company_id, stmt_type)

        for col in df.columns:
            period = _period_label(col)
            if period in existing_periods:
                continue

            reported_at = col.date() if hasattr(col, "date") else None

            fs = FinancialStatement(
                company_id=company_id,
                period=period,
                period_type="annual",
                statement_type=stmt_type,
                currency=currency,
                reported_at=reported_at,
                created_at=now,
            )
            session.add(fs)
            await session.flush()

            ordinal = 0
            for item_label in df.index:
                val = df.loc[item_label, col]
                if val is None or (hasattr(val, "__float__") and str(val) == "nan"):
                    continue
                try:
                    float_val = float(val)
                except (ValueError, TypeError):
                    continue

                session.add(FinancialLineItem(
                    statement_id=fs.id,
                    item_key=_item_key(item_label),
                    item_label=item_label,
                    value=float_val,
                    ordinal=ordinal,
                ))
                ordinal += 1
                item_count += 1

            stmt_count += 1

    if stmt_count > 0:
        await session.flush()
        logger.info(f"Financials: {ticker} +{stmt_count} statements, +{item_count} line items")

    return {"statements": stmt_count, "line_items": item_count}


async def financials_update(
    ticker: str | None = None,
) -> dict:
    """统一入口：更新三表数据。

    Returns: {"companies": int, "statements": int, "line_items": int}
    """
    await create_tables()

    async with AsyncSessionLocal() as session:
        result = {"companies": 0, "statements": 0, "line_items": 0}

        if ticker:
            r = await update_financials(session, ticker)
            result["companies"] = 1
            result["statements"] = r["statements"]
            result["line_items"] = r["line_items"]
        else:
            tickers = await _get_tracked_tickers(session)
            for t, _ in tickers:
                r = await update_financials(session, t)
                result["companies"] += 1
                result["statements"] += r["statements"]
                result["line_items"] += r["line_items"]

        await session.commit()

    return result
