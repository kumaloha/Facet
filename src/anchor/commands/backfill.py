"""
anchor.commands.backfill — 自动拉取历年 SEC 财报并提取
======================================================
anchor backfill NVDA --years 5
anchor backfill NVDA --years 5 --fill-gaps
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime

import httpx
from loguru import logger

# SEC EDGAR API 需要合规 User-Agent（含邮箱）
_SEC_HEADERS = {
    "User-Agent": "Anchor/1.0 (anchor@example.com)",
    "Accept": "application/json",
}
_SEC_SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik}.json"


def _sec_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=30, headers=_SEC_HEADERS)


async def _resolve_cik(ticker: str) -> str | None:
    """从 SEC EDGAR 根据 ticker 查找 CIK。"""
    async with _sec_client() as client:
        r = await client.get("https://www.sec.gov/files/company_tickers.json")
        r.raise_for_status()
        tickers = r.json()
        for entry in tickers.values():
            if entry.get("ticker", "").upper() == ticker.upper():
                return str(entry["cik_str"]).zfill(10)
    return None


async def _get_10k_urls(cik: str, years: int) -> list[dict]:
    """从 EDGAR submissions API 获取最近 N 年的 10-K filing URL。"""
    url = _SEC_SUBMISSIONS.format(cik=cik)
    async with _sec_client() as client:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()

    company_name = data.get("name", "")
    recent = data["filings"]["recent"]
    forms = recent["form"]
    dates = recent["filingDate"]
    accessions = recent["accessionNumber"]
    docs = recent["primaryDocument"]

    results = []
    for i in range(len(forms)):
        if forms[i] != "10-K":
            continue
        acc = accessions[i].replace("-", "")
        filing_url = (
            f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{docs[i]}"
        )
        results.append({
            "filing_date": dates[i],
            "document": docs[i],
            "url": filing_url,
        })
        if len(results) >= years:
            break

    return results


async def _run(ticker: str, years: int, fill_gaps: bool):
    print(f"\n[1/3] 查找 {ticker} 的 SEC CIK...")
    cik = await _resolve_cik(ticker)
    if not cik:
        print(f"  ERROR: 未找到 ticker '{ticker}' 对应的 CIK")
        return
    print(f"  CIK: {cik}")

    print(f"\n[2/3] 获取最近 {years} 年 10-K filing URL...")
    filings = await _get_10k_urls(cik, years)
    if not filings:
        print(f"  ERROR: 未找到 10-K filing")
        return
    for f in filings:
        print(f"  {f['filing_date']}  {f['document']}")
        print(f"    {f['url']}")

    print(f"\n[3/3] 逐年提取（{'增量模式' if fill_gaps else '全量模式'}）...")
    print(f"{'='*60}")

    from anchor.commands.run_url import _main_url

    for i, f in enumerate(filings, 1):
        print(f"\n{'─'*60}")
        print(f"[{i}/{len(filings)}] {f['filing_date']}  {f['document']}")
        print(f"{'─'*60}")
        try:
            await _main_url(f["url"], _fill_gaps=fill_gaps)
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()
            continue

    # 最终覆盖检查
    print(f"\n{'='*60}")
    print(f"最终覆盖检查")
    print(f"{'='*60}")
    try:
        from anchor.database.session import AsyncSessionLocal
        from anchor.commands.company_sources import company_sources_command
        company_sources_command(ticker=ticker, name=None, years=years)
    except Exception as e:
        print(f"  覆盖检查失败: {e}")


def backfill_command(ticker: str, years: int, fill_gaps: bool):
    asyncio.run(_run(ticker, years, fill_gaps))
