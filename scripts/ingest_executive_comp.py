"""
DEF 14A ECD XBRL 高管薪酬提取
================================
从 SEC EDGAR DEF 14A 的 ECD (Executive Compensation Disclosure)
XBRL 标签中提取 CEO/NEO 薪酬数据，零 LLM 成本。

数据范围：Pay vs Performance 表 — CEO 总薪酬 + NEO 平均薪酬（5年历史）。
SCT 分项（base/bonus/stock）需 LLM 提取，此脚本不做。

用法:
    python scripts/ingest_executive_comp.py                    # 全量 S&P 500
    python scripts/ingest_executive_comp.py --ticker AAPL      # 单家
    python scripts/ingest_executive_comp.py --concurrency 3    # 并发数
"""

from __future__ import annotations

import asyncio
import csv
import sys
import time
from datetime import date, datetime
from pathlib import Path

from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

SP500_CSV = Path(__file__).parent.parent / "data" / "sp500.csv"


def load_sp500() -> list[dict]:
    with open(SP500_CSV) as f:
        return list(csv.DictReader(f))


def extract_ecd_from_filing(filing, ticker: str) -> list[dict]:
    """从一个 DEF 14A filing 中提取 ECD 薪酬数据。返回 executive_compensations 行。"""
    try:
        xbrl = filing.xbrl()
        if xbrl is None:
            return []
        df = xbrl.facts.to_dataframe()
    except Exception as e:
        logger.warning(f"  {ticker}: XBRL 解析失败: {e}")
        return []

    ecd = df[df["concept"].str.startswith("ecd:", na=False)].copy()
    if len(ecd) == 0:
        return []

    results = []

    # --- CEO (PEO) 数据 ---
    # 取 CEO 姓名（最新年度的 PeoName where ExecutiveCategoryAxis = PeoMember）
    peo_names = ecd[ecd["concept"] == "ecd:PeoName"]
    ceo_name = ""
    if len(peo_names) > 0:
        # 优先取 PeoMember 分类的名字
        peo_member = peo_names[
            peo_names.get("dim_ecd_ExecutiveCategoryAxis", "").astype(str).str.contains("PeoMember", na=False)
        ] if "dim_ecd_ExecutiveCategoryAxis" in peo_names.columns else peo_names
        if len(peo_member) > 0:
            ceo_name = str(peo_member.iloc[0].get("value", "")).strip()
        else:
            ceo_name = str(peo_names.iloc[0].get("value", "")).strip()

    # CEO 总薪酬（多年）
    peo_total = ecd[ecd["concept"] == "ecd:PeoTotalCompAmt"].copy()
    peo_actual = ecd[ecd["concept"] == "ecd:PeoActuallyPaidCompAmt"].copy()

    # 按 period_end 索引 CEO actually paid
    actual_by_period = {}
    for _, row in peo_actual.iterrows():
        pe = str(row.get("period_end", ""))[:10]
        val = row.get("numeric_value")
        if pe and val is not None:
            actual_by_period[pe] = float(val)

    seen_periods = set()
    for _, row in peo_total.iterrows():
        pe = str(row.get("period_end", ""))[:10]
        val = row.get("numeric_value")
        if not pe or val is None:
            continue
        # 去重（有些公司同一年报两次）
        if pe in seen_periods:
            continue
        seen_periods.add(pe)

        try:
            year = datetime.strptime(pe, "%Y-%m-%d").year
        except ValueError:
            continue

        period = f"FY{year}"
        results.append({
            "ticker": ticker,
            "period": period,
            "role_type": "executive",
            "name": ceo_name,
            "title": "CEO",
            "total_comp": float(val),
            "_period_end": pe,
        })

    # --- NEO 平均数据 ---
    neo_total = ecd[ecd["concept"] == "ecd:NonPeoNeoAvgTotalCompAmt"].copy()
    neo_actual = ecd[ecd["concept"] == "ecd:NonPeoNeoAvgCompActuallyPaidAmt"].copy()

    seen_periods = set()
    for _, row in neo_total.iterrows():
        pe = str(row.get("period_end", ""))[:10]
        val = row.get("numeric_value")
        if not pe or val is None:
            continue
        if pe in seen_periods:
            continue
        seen_periods.add(pe)

        try:
            year = datetime.strptime(pe, "%Y-%m-%d").year
        except ValueError:
            continue

        period = f"FY{year}"
        results.append({
            "ticker": ticker,
            "period": period,
            "role_type": "executive",
            "name": "NEO Average",
            "title": "Named Executive Officers (Average)",
            "total_comp": float(val),
            "_period_end": pe,
        })

    # --- 个别 NEO 姓名提取（如果有维度数据）---
    if "dim_ecd_IndividualAxis" in peo_names.columns and "dim_ecd_ExecutiveCategoryAxis" in peo_names.columns:
        neo_names = peo_names[
            peo_names["dim_ecd_ExecutiveCategoryAxis"].astype(str).str.contains("NonPeoNeo", na=False)
        ]
        # 取最新年度的 NEO 名单
        if len(neo_names) > 0:
            latest_pe = neo_names["period_end"].max()
            latest_neos = neo_names[neo_names["period_end"] == latest_pe]
            try:
                year = datetime.strptime(str(latest_pe)[:10], "%Y-%m-%d").year
            except (ValueError, TypeError):
                year = None

            if year:
                period = f"FY{year}"
                for _, row in latest_neos.iterrows():
                    neo_name = str(row.get("value", "")).strip()
                    if neo_name and neo_name != "nan":
                        # 这些个人没有单独的薪酬数据（ECD 只给平均值）
                        # 但记录姓名和身份有助于后续 LLM 提取时匹配
                        pass  # 不写入 DB，避免 total_comp=NULL 的空行

    return results


async def write_compensations(rows: list[dict]):
    """写入 DB。"""
    from anchor.database.session import AsyncSessionLocal, create_tables
    from anchor.models import CompanyProfile, ExecutiveCompensation
    from sqlmodel import select

    await create_tables()

    async with AsyncSessionLocal() as session:
        # ticker → company_id
        result = await session.execute(
            select(CompanyProfile.id, CompanyProfile.ticker)
        )
        ticker_to_id = {r[1]: r[0] for r in result.all()}

        # 查已有记录去重（company_id + period + name）
        result = await session.execute(
            select(
                ExecutiveCompensation.company_id,
                ExecutiveCompensation.period,
                ExecutiveCompensation.name,
            )
        )
        existing = {(r[0], r[1], r[2]) for r in result.all()}

        written = 0
        skipped = 0
        no_company = set()

        for row in rows:
            ticker = row.pop("ticker")
            row.pop("_period_end", None)
            company_id = ticker_to_id.get(ticker)
            if not company_id:
                no_company.add(ticker)
                continue

            key = (company_id, row["period"], row["name"])
            if key in existing:
                skipped += 1
                continue
            existing.add(key)

            comp = ExecutiveCompensation(company_id=company_id, **row)
            session.add(comp)
            written += 1

            if written % 1000 == 0:
                await session.flush()

        await session.commit()

    logger.info(
        f"[ECD] 写入 {written:,} 条, 跳过重复 {skipped:,}, "
        f"无公司档案 {len(no_company)} tickers"
    )
    return written


async def process_one(ticker: str, sem: asyncio.Semaphore) -> list[dict]:
    """处理单个公司的 DEF 14A。"""
    async with sem:
        # edgartools 是同步的，在 executor 中运行
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_process_one, ticker)


def _sync_process_one(ticker: str) -> list[dict]:
    """同步提取单个公司。"""
    from edgar import Company

    try:
        c = Company(ticker)
        filings = c.get_filings(form="DEF 14A", amendments=False)
        if not filings or len(filings) == 0:
            filings = c.get_filings(form="DEF 14A")
        if not filings or len(filings) == 0:
            logger.debug(f"  {ticker}: 无 DEF 14A")
            return []

        latest = filings.latest(1)
        rows = extract_ecd_from_filing(latest, ticker)

        if rows:
            logger.info(f"  {ticker}: {len(rows)} 条薪酬记录 (DEF 14A {latest.filing_date})")
        else:
            logger.debug(f"  {ticker}: DEF 14A 无 ECD XBRL")

        return rows
    except Exception as e:
        logger.error(f"  {ticker}: 失败 — {e}")
        return []


async def main():
    from edgar import set_identity
    set_identity("Facet Research facet@example.com")

    args = sys.argv[1:]
    specific_ticker = None
    concurrency = 3

    for i, arg in enumerate(args):
        if arg == "--ticker" and i + 1 < len(args):
            specific_ticker = args[i + 1].upper()
        if arg == "--concurrency" and i + 1 < len(args):
            concurrency = int(args[i + 1])

    if specific_ticker:
        tickers = [specific_ticker]
    else:
        sp500 = load_sp500()
        tickers = [c["Symbol"].upper() for c in sp500]

    logger.info(f"[ECD] 处理 {len(tickers)} 家公司, 并发={concurrency}")

    sem = asyncio.Semaphore(concurrency)
    t0 = time.time()

    tasks = [process_one(t, sem) for t in tickers]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_rows = []
    success = 0
    failed = 0
    for ticker, result in zip(tickers, results):
        if isinstance(result, Exception):
            logger.error(f"  {ticker}: 异常 — {result}")
            failed += 1
        elif result:
            all_rows.extend(result)
            success += 1
        else:
            success += 1  # 无数据也算成功

    elapsed = time.time() - t0
    logger.info(f"[ECD] 提取完成: {elapsed:.0f}s, {success} 成功, {failed} 失败, {len(all_rows):,} 条记录")

    if all_rows:
        written = await write_compensations(all_rows)
        logger.info(f"[ECD] 完成: {written:,} 条新增")
    else:
        logger.warning("[ECD] 无数据")


if __name__ == "__main__":
    asyncio.run(main())
