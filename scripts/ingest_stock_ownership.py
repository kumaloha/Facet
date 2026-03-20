"""
机构持股数据批量导入
====================
从 yfinance 提取 S&P 500 的机构持股 + 内部人持股汇总，
写入 stock_ownership 表。

用法:
    python scripts/ingest_stock_ownership.py                    # 全量 S&P 500
    python scripts/ingest_stock_ownership.py --ticker AAPL      # 单家
    python scripts/ingest_stock_ownership.py --concurrency 5    # 并发
"""

from __future__ import annotations

import asyncio
import csv
import sys
import time
from datetime import date, datetime
from pathlib import Path

import yfinance as yf
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

SP500_CSV = Path(__file__).parent.parent / "data" / "sp500.csv"


def load_sp500() -> list[str]:
    with open(SP500_CSV) as f:
        return [row["Symbol"].upper() for row in csv.DictReader(f)]


def extract_ownership(ticker: str) -> list[dict]:
    """从 yfinance 提取持股数据。"""
    try:
        t = yf.Ticker(ticker)
    except Exception as e:
        logger.warning(f"  {ticker}: yfinance 初始化失败: {e}")
        return []

    results = []
    period = f"FY{datetime.now().year}"

    # --- Major Holders (aggregate) ---
    try:
        major = t.major_holders
        if major is not None and not major.empty:
            # major_holders 是一个 DataFrame: index=Breakdown, columns=[Value]
            for idx, row in major.iterrows():
                breakdown = str(idx)
                val = row.iloc[0] if hasattr(row, "iloc") else row
                if "insider" in breakdown.lower():
                    results.append({
                        "ticker": ticker,
                        "period": period,
                        "name": "All Insiders (Aggregate)",
                        "title": "Insiders",
                        "shares_beneficially_owned": None,
                        "percent_of_class": round(float(val) * 100, 2) if float(val) < 1 else round(float(val), 2),
                    })
                elif "institution" in breakdown.lower() and "float" not in breakdown.lower():
                    results.append({
                        "ticker": ticker,
                        "period": period,
                        "name": "All Institutions (Aggregate)",
                        "title": "Institutions",
                        "shares_beneficially_owned": None,
                        "percent_of_class": round(float(val) * 100, 2) if float(val) < 1 else round(float(val), 2),
                    })
    except Exception as e:
        logger.debug(f"  {ticker}: major_holders 失败: {e}")

    # --- Top Institutional Holders ---
    try:
        inst = t.institutional_holders
        if inst is not None and not inst.empty:
            for _, row in inst.head(10).iterrows():  # Top 10
                holder = str(row.get("Holder", "")).strip()
                if not holder:
                    continue
                shares = row.get("Shares")
                pct = row.get("pctHeld")
                results.append({
                    "ticker": ticker,
                    "period": period,
                    "name": holder,
                    "title": "Institutional Holder",
                    "shares_beneficially_owned": int(shares) if shares and shares == shares else None,
                    "percent_of_class": round(float(pct) * 100, 2) if pct and pct == pct and float(pct) < 1 else (round(float(pct), 2) if pct and pct == pct else None),
                })
    except Exception as e:
        logger.debug(f"  {ticker}: institutional_holders 失败: {e}")

    return results


async def write_ownership(rows: list[dict]):
    """写入 DB。"""
    from anchor.database.session import AsyncSessionLocal, create_tables
    from anchor.models import CompanyProfile, StockOwnership
    from sqlmodel import select

    await create_tables()

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(CompanyProfile.id, CompanyProfile.ticker)
        )
        ticker_to_id = {r[1]: r[0] for r in result.all()}

        # 去重 key: company_id + period + name
        result = await session.execute(
            select(
                StockOwnership.company_id,
                StockOwnership.period,
                StockOwnership.name,
            )
        )
        existing = {(r[0], r[1], r[2]) for r in result.all()}

        written = 0
        skipped = 0
        no_company = set()

        for row in rows:
            ticker = row.pop("ticker")
            company_id = ticker_to_id.get(ticker)
            if not company_id:
                no_company.add(ticker)
                continue

            key = (company_id, row["period"], row["name"])
            if key in existing:
                skipped += 1
                continue
            existing.add(key)

            own = StockOwnership(company_id=company_id, **row)
            session.add(own)
            written += 1

            if written % 1000 == 0:
                await session.flush()

        await session.commit()

    logger.info(
        f"[Ownership] 写入 {written:,} 条, 跳过 {skipped:,}, "
        f"无公司档案 {len(no_company)}"
    )
    return written


async def process_one(ticker: str, sem: asyncio.Semaphore) -> list[dict]:
    async with sem:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_one, ticker)


def _sync_one(ticker: str) -> list[dict]:
    try:
        rows = extract_ownership(ticker)
        if rows:
            logger.info(f"  {ticker}: {len(rows)} 条持股记录")
        return rows
    except Exception as e:
        logger.error(f"  {ticker}: 失败 — {e}")
        return []


async def main():
    args = sys.argv[1:]
    specific_ticker = None
    concurrency = 5

    for i, arg in enumerate(args):
        if arg == "--ticker" and i + 1 < len(args):
            specific_ticker = args[i + 1].upper()
        if arg == "--concurrency" and i + 1 < len(args):
            concurrency = int(args[i + 1])

    if specific_ticker:
        tickers = [specific_ticker]
    else:
        tickers = load_sp500()

    logger.info(f"[Ownership] 处理 {len(tickers)} 家, 并发={concurrency}")

    sem = asyncio.Semaphore(concurrency)
    t0 = time.time()

    tasks = [process_one(t, sem) for t in tickers]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_rows = []
    for ticker, result in zip(tickers, results):
        if isinstance(result, Exception):
            logger.error(f"  {ticker}: 异常 — {result}")
        elif result:
            all_rows.extend(result)

    elapsed = time.time() - t0
    logger.info(f"[Ownership] 提取完成: {elapsed:.0f}s, {len(all_rows):,} 条")

    if all_rows:
        await write_ownership(all_rows)


if __name__ == "__main__":
    asyncio.run(main())
