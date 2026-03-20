"""
公司数据入库脚本（XBRL-first）
==============================
从 SEC EDGAR 下载文件 → XBRL 提取 → LLM 补充叙述 → 写入 Anchor DB。

用法:
    python scripts/ingest_company.py AAPL
    python scripts/ingest_company.py AAPL --forms 10-K,DEF-14A
    python scripts/ingest_company.py DASH --forms S-1
"""

import asyncio
import sys
import time

from loguru import logger


async def ingest_company(ticker: str, forms: list[str] | None = None):
    """完整入库流程。"""
    from edgar import Company, set_identity
    set_identity("Facet Research facet@example.com")

    from anchor.config import settings
    settings.llm_mode = "cloud"

    from anchor.database.session import create_tables, get_session
    from anchor.extract.pipelines.doc_router import extract_document
    from anchor.extract.pipelines._writer import write_extraction_result

    # 创建表
    await create_tables()

    forms = forms or ["10-K", "DEF 14A"]

    # 文件类型映射
    form_to_doc_type = {
        "10-K": "annual_report",
        "DEF 14A": "proxy",
        "S-1": "prospectus",
        "F-1": "prospectus",
        "8-K": "event_filing",
    }

    c = Company(ticker)
    total_rows = 0

    for form in forms:
        doc_type = form_to_doc_type.get(form)
        if not doc_type:
            logger.warning(f"未知表单类型: {form}")
            continue

        logger.info(f"{'='*60}")
        logger.info(f"  {ticker} | {form} → {doc_type}")
        logger.info(f"{'='*60}")

        # 下载
        filings = c.get_filings(form=form)
        if not filings:
            logger.warning(f"  无 {form} 文件")
            continue

        latest = filings[0]
        logger.info(f"  Filing: {latest.filing_date} | {latest.accession_no}")

        doc = latest.document
        text = doc.text() if hasattr(doc, "text") else str(doc)
        logger.info(f"  文档长度: {len(text):,} chars")

        # 确定报告期间（修复：用 period_of_report 而非 filing_date）
        period_date = getattr(latest, "period_of_report", None) or latest.filing_date
        # period_of_report 可能返回 str（如 "2024-09-28"）
        if isinstance(period_date, str):
            period = f"FY{period_date[:4]}"
        else:
            period = f"FY{period_date.year}"

        # 提取
        t0 = time.time()
        metadata = {
            "ticker": ticker,
            "period": period,
            "company_name": c.name,
            "filing_date": str(latest.filing_date),
            "period_of_report": str(period_date),
            "form": form,
        }

        # 对 10-K 传入 filing 对象以启用 XBRL-first
        filing_obj = latest if doc_type == "annual_report" else None
        result = await extract_document(text, doc_type, metadata, filing=filing_obj)
        extract_time = time.time() - t0

        table_summary = {k: len(v) for k, v in result.tables.items()}
        rows = sum(table_summary.values())
        logger.info(f"  提取: {extract_time:.0f}s | {rows} 行 | {table_summary}")

        # 写入 DB（即使 0 行也要建 CompanyProfile，保证断点续传能识别已处理的公司）
        from anchor.database.session import AsyncSessionLocal
        from anchor.extract.pipelines._writer import get_or_create_company
        async with AsyncSessionLocal() as session:
            if rows == 0:
                await get_or_create_company(session, ticker, name=c.name, market="us")
                await session.commit()
                logger.warning(f"  {ticker} 提取 0 行，仅建档")
                continue

            stats = await write_extraction_result(
                session, result,
                market="us",
            )

        total_rows += sum(stats.values())

    logger.info(f"\n{'='*60}")
    logger.info(f"  {ticker} 入库完成: 共 {total_rows} 行")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    ticker = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    forms = None
    if len(sys.argv) > 3 and sys.argv[2] == "--forms":
        forms = sys.argv[3].split(",")

    asyncio.run(ingest_company(ticker, forms))
