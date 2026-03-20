"""
股票行情批量导入（带退避重试）
==============================
逐个拉取 S&P 500 行情，遇到 rate limit 自动等待重试。
比 market_update 更适合一次性批量导入历史数据。

用法:
    python scripts/ingest_stock_quotes.py                    # 全量 365 天
    python scripts/ingest_stock_quotes.py --days 30          # 近 30 天
    python scripts/ingest_stock_quotes.py --ticker AAPL      # 单家
"""

from __future__ import annotations

import asyncio
import csv
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import yfinance as yf
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

SP500_CSV = Path(__file__).parent.parent / "data" / "sp500.csv"

# 风险平价 ETF（达利欧链需要）
RISK_PARITY_ETFS = ["SPY", "TLT", "IEF", "DBC", "GLD"]


def load_sp500() -> list[str]:
    with open(SP500_CSV) as f:
        return [row["Symbol"].upper() for row in csv.DictReader(f)]


def fetch_history(ticker: str, start: str, end: str, max_retries: int = 3) -> tuple:
    """带退避重试的 yfinance 行情拉取。返回 (hist_df, info_dict)。"""
    for attempt in range(max_retries):
        try:
            t = yf.Ticker(ticker)
            hist = t.history(start=start, end=end)
            info = {}
            try:
                info = t.info or {}
            except Exception:
                pass
            return hist, info
        except Exception as e:
            err_str = str(e)
            if "Rate" in err_str or "Too Many" in err_str or "429" in err_str:
                wait = 10 * (attempt + 1)
                logger.warning(f"  {ticker}: rate limited, 等待 {wait}s (attempt {attempt+1})")
                time.sleep(wait)
            else:
                logger.warning(f"  {ticker}: {e}")
                return None, {}
    return None, {}


async def main():
    from anchor.database.session import AsyncSessionLocal, create_tables
    from anchor.models import CompanyProfile, StockQuote, _utcnow
    from sqlmodel import select

    args = sys.argv[1:]
    days = 365
    specific_ticker = None
    for i, arg in enumerate(args):
        if arg == "--days" and i + 1 < len(args):
            days = int(args[i + 1])
        if arg == "--ticker" and i + 1 < len(args):
            specific_ticker = args[i + 1].upper()

    await create_tables()

    if specific_ticker:
        tickers = [specific_ticker]
    else:
        tickers = RISK_PARITY_ETFS + load_sp500()

    end = date.today()
    start = end - timedelta(days=days)

    async with AsyncSessionLocal() as session:
        # ticker → company_id
        result = await session.execute(
            select(CompanyProfile.id, CompanyProfile.ticker)
        )
        ticker_to_id = {r[1]: r[0] for r in result.all()}

        # 查已有 (ticker, date) 对
        result = await session.execute(
            select(StockQuote.ticker, StockQuote.trade_date)
        )
        existing = {(r[0], r[1]) for r in result.all()}

        logger.info(f"[Quotes] {len(tickers)} tickers, {days} 天, 已有 {len(existing):,} 条")

        # 找出需要补数据的 tickers
        tickers_needing_data = []
        for t in tickers:
            # 粗略检查：如果该 ticker 已有大量数据，可能不需要补
            t_count = sum(1 for k in existing if k[0] == t)
            if t_count < days * 0.5:  # 少于预期的一半，需要补
                tickers_needing_data.append(t)

        logger.info(f"[Quotes] {len(tickers_needing_data)} tickers 需要补数据")

        total = 0
        now = _utcnow()
        batch_count = 0

        for i, ticker in enumerate(tickers_needing_data):
            if i > 0 and i % 50 == 0:
                await session.flush()
                logger.info(f"[Quotes] 进度: {i}/{len(tickers_needing_data)}, 已写入 {total:,}")
                # 每 50 个 ticker 暂停 2 秒，避免 rate limit
                time.sleep(2)

            hist, info = await asyncio.to_thread(
                fetch_history, ticker, start.isoformat(), end.isoformat()
            )

            if hist is None or hist.empty:
                continue

            shares = info.get("sharesOutstanding")
            mcap = info.get("marketCap")
            company_id = ticker_to_id.get(ticker)

            count = 0
            for idx, row in hist.iterrows():
                d = idx.date() if hasattr(idx, "date") else idx
                if (ticker, d) in existing:
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
                existing.add((ticker, d))
                count += 1

            if count > 0:
                total += count
                batch_count += count

            # 每 2000 行 flush
            if batch_count >= 2000:
                await session.flush()
                batch_count = 0

        await session.commit()

    logger.info(f"[Quotes] 完成: {total:,} 条新增")


def _safe(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    asyncio.run(main())
