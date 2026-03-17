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
from anchor.models import CompanyProfile, MacroIndicator, StockQuote, _utcnow

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

        # 宏观指标
        result["macro"] = await update_macro_indicators(session, days=days)

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
