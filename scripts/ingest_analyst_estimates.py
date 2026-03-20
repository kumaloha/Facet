"""
分析师预期数据批量导入
======================
从 yfinance 提取 S&P 500 的 EPS/Revenue 共识预期及历史惊喜，
写入 analyst_estimates 表。

用法:
    python scripts/ingest_analyst_estimates.py                    # 全量 S&P 500
    python scripts/ingest_analyst_estimates.py --ticker AAPL      # 单家
    python scripts/ingest_analyst_estimates.py --concurrency 5    # 并发
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


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        f = float(val)
        return f if f == f else None  # NaN check
    except (ValueError, TypeError):
        return None


def extract_estimates(ticker: str) -> list[dict]:
    """从 yfinance 提取分析师预期数据。"""
    try:
        t = yf.Ticker(ticker)
    except Exception as e:
        logger.warning(f"  {ticker}: yfinance 初始化失败: {e}")
        return []

    results = []

    # --- EPS Estimates (forward) ---
    try:
        eps_est = t.earnings_estimate
        if eps_est is not None and not eps_est.empty:
            for period_label, row in eps_est.iterrows():
                # period_label: '0q', '+1q', '0y', '+1y'
                period = _map_period(period_label, ticker)
                if not period:
                    continue
                results.append({
                    "ticker": ticker,
                    "period": period,
                    "metric": "eps",
                    "consensus_estimate": _safe_float(row.get("avg")),
                    "actual": _safe_float(row.get("yearAgoEps")),  # 去年同期实际值
                    "surprise_pct": None,
                    "estimate_date": date.today(),
                })
    except Exception as e:
        logger.debug(f"  {ticker}: earnings_estimate 失败: {e}")

    # --- Revenue Estimates (forward) ---
    try:
        rev_est = t.revenue_estimate
        if rev_est is not None and not rev_est.empty:
            for period_label, row in rev_est.iterrows():
                period = _map_period(period_label, ticker)
                if not period:
                    continue
                results.append({
                    "ticker": ticker,
                    "period": period,
                    "metric": "revenue",
                    "consensus_estimate": _safe_float(row.get("avg")),
                    "actual": _safe_float(row.get("yearAgoRevenue")),
                    "surprise_pct": None,
                    "estimate_date": date.today(),
                })
    except Exception as e:
        logger.debug(f"  {ticker}: revenue_estimate 失败: {e}")

    # --- Earnings History (actuals + surprises) ---
    try:
        hist = t.earnings_history
        if hist is not None and not hist.empty:
            for quarter_date, row in hist.iterrows():
                try:
                    if hasattr(quarter_date, "date"):
                        qd = quarter_date.date()
                    elif hasattr(quarter_date, "year"):
                        qd = quarter_date
                    else:
                        qd = datetime.strptime(str(quarter_date)[:10], "%Y-%m-%d").date()
                    period = f"Q{(qd.month - 1) // 3 + 1}FY{qd.year}"
                except Exception:
                    continue

                results.append({
                    "ticker": ticker,
                    "period": period,
                    "metric": "eps",
                    "consensus_estimate": _safe_float(row.get("epsEstimate")),
                    "actual": _safe_float(row.get("epsActual")),
                    "surprise_pct": _safe_float(row.get("surprisePercent")),
                    "estimate_date": qd,
                })
    except Exception as e:
        logger.debug(f"  {ticker}: earnings_history 失败: {e}")

    return results


def _map_period(label: str, ticker: str) -> str | None:
    """将 yfinance 期间标签转为 period 字符串。"""
    now = datetime.now()
    year = now.year
    quarter = (now.month - 1) // 3 + 1

    if label == "0q":
        return f"Q{quarter}FY{year}"
    elif label == "+1q":
        nq = quarter + 1
        ny = year
        if nq > 4:
            nq = 1
            ny += 1
        return f"Q{nq}FY{ny}"
    elif label == "0y":
        return f"FY{year}"
    elif label == "+1y":
        return f"FY{year + 1}"
    return None


async def write_estimates(rows: list[dict]):
    """写入 DB。"""
    from anchor.database.session import AsyncSessionLocal, create_tables
    from anchor.models import AnalystEstimate, CompanyProfile
    from sqlmodel import select

    await create_tables()

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(CompanyProfile.id, CompanyProfile.ticker)
        )
        ticker_to_id = {r[1]: r[0] for r in result.all()}

        # 去重 key: company_id + period + metric
        result = await session.execute(
            select(
                AnalystEstimate.company_id,
                AnalystEstimate.period,
                AnalystEstimate.metric,
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

            key = (company_id, row["period"], row["metric"])
            if key in existing:
                skipped += 1
                continue
            existing.add(key)

            est = AnalystEstimate(company_id=company_id, **row)
            session.add(est)
            written += 1

            if written % 1000 == 0:
                await session.flush()

        await session.commit()

    logger.info(
        f"[Estimates] 写入 {written:,} 条, 跳过 {skipped:,}, "
        f"无公司档案 {len(no_company)}"
    )
    return written


async def process_one(ticker: str, sem: asyncio.Semaphore) -> list[dict]:
    async with sem:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_one, ticker)


def _sync_one(ticker: str) -> list[dict]:
    try:
        rows = extract_estimates(ticker)
        if rows:
            logger.info(f"  {ticker}: {len(rows)} 条预期")
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

    logger.info(f"[Estimates] 处理 {len(tickers)} 家, 并发={concurrency}")

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
    logger.info(f"[Estimates] 提取完成: {elapsed:.0f}s, {len(all_rows):,} 条")

    if all_rows:
        await write_estimates(all_rows)


if __name__ == "__main__":
    asyncio.run(main())
