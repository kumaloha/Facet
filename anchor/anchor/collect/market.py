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

# 风险平价代理 ETF — market-update 时自动拉取行情
RISK_PARITY_ETFS = ["SPY", "TLT", "IEF", "DBC", "GLD"]


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


# ── FRED 宏观指标 ─────────────────────────────────────────────────────

# 达利欧链需要的 FRED 序列
FRED_SERIES: dict[str, str] = {
    # 基础宏观
    "fed_funds_rate": "FEDFUNDS",          # 联邦基金有效利率 (月度, %)
    "treasury_2y": "DGS2",                 # 2 年期国债收益率 (日度, %)
    "cpi_yoy": "CPIAUCSL",                # CPI 全项 (月度, 指数 → 需算 YoY)
    "gdp_growth": "A191RL1Q225SBEA",      # 实际 GDP 同比增速 (季度, %)
    "unemployment": "UNRATE",              # 失业率 (月度, %)
    "credit_growth": "TOTBKCR",            # 银行信贷总量 (周度, 十亿$ → 需算 YoY)
    "total_debt_to_gdp": "GFDEGDQ188S",   # 联邦债务/GDP (季度, %)
    "fiscal_deficit_to_gdp": "FYFSGDA188S",  # 联邦盈余或赤字/GDP (年度, %)
    # 债务结构拆解
    "household_debt_to_income": "TDSP",    # 家庭债务偿付比/可支配收入 (季度, %)
    "corporate_debt_to_gdp": "BCNSDODNS",  # 非金融企业债务 (季度, 十亿$ → 需除以 GDP)
    "government_debt_to_gdp": "GFDEGDQ188S",  # 同 total（联邦债务/GDP）
    # 市场隐含预期（索罗斯链需要）
    "breakeven_inflation_5y": "T5YIE",     # 5 年期 breakeven 通胀率 (日度, %)
    "breakeven_inflation_10y": "T10YIE",   # 10 年期 breakeven 通胀率 (日度, %)
    "credit_spread_ig": "BAMLC0A0CM",      # 投资级信用利差 (日度, %)
    "credit_spread_hy": "BAMLH0A0HYM2",   # 高收益信用利差 (日度, %)
}


async def update_fred_indicators(
    session: AsyncSession,
    days: int = 90,
) -> int:
    """从 FRED 拉取达利欧链需要的宏观指标，写入 macro_indicators。

    对于 CPI 和信贷总量等原始指标，自动计算同比增速。
    """
    import asyncio
    from anchor.config import settings

    api_key = settings.fred_api_key
    if not api_key:
        logger.warning("FRED API key not configured (settings.fred_api_key), skipping FRED update")
        return 0

    def _fetch_all():
        from fredapi import Fred
        fred = Fred(api_key=api_key)
        results = {}
        for indicator, series_id in FRED_SERIES.items():
            try:
                s = fred.get_series(series_id, observation_start=(date.today() - timedelta(days=days + 400)).isoformat())
                if not s.empty:
                    results[indicator] = s
            except Exception as e:
                logger.warning(f"[FRED] Failed to fetch {series_id}: {e}")
        return results

    try:
        raw = await asyncio.to_thread(_fetch_all)
    except Exception as e:
        logger.warning(f"[FRED] Batch fetch failed: {e}")
        return 0

    now = _utcnow()
    total = 0

    for indicator, series in raw.items():
        if series.empty:
            continue

        # CPI: 原始是指数，需要算同比增速
        if indicator == "cpi_yoy":
            series = series.pct_change(periods=12) * 100  # 12 个月前的同比 (%)
            series = series.dropna()

        # 银行信贷: 原始是总量，需要算同比增速
        if indicator == "credit_growth":
            series = series.pct_change(periods=52) * 100  # 52 周前的同比 (%)
            series = series.dropna()

        # 财政赤字: 原始是负数表示赤字，取绝对值
        if indicator == "fiscal_deficit_to_gdp":
            series = series.abs()

        # 只取最近 days 天
        cutoff = date.today() - timedelta(days=days)
        series = series[series.index >= str(cutoff)]

        existing = await _get_existing_macro_dates(session, indicator)

        count = 0
        for idx, val in series.items():
            d = idx.date() if hasattr(idx, "date") else idx
            if d in existing:
                continue
            if val != val:  # NaN check
                continue

            session.add(MacroIndicator(
                trade_date=d,
                indicator=indicator,
                value=float(val),
                source="fred",
                created_at=now,
            ))
            count += 1

        if count > 0:
            await session.flush()
            total += count
            logger.info(f"FRED: {indicator} +{count} rows (latest: {float(series.iloc[-1]):.2f})")

    return total


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
            # FRED 宏观序列（达利欧链需要）
            result["macro"] += await update_fred_indicators(session, days=days)
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.warning(f"Macro update failed: {e}")

        if not macro_only:
            # 风险平价代理 ETF（达利欧链波动率计算需要）
            for etf in RISK_PARITY_ETFS:
                n = await update_stock_quotes(session, etf, days=days)
                result["stocks"] += n

            if ticker:
                # 单只
                company_id = await _resolve_company_id(session, ticker)
                result["stocks"] += await update_stock_quotes(
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
