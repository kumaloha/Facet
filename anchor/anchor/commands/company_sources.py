"""
anchor.commands.company_sources — 查询指定公司的全部输入源 URL
================================================================
anchor company-sources NVDA
anchor company-sources --name 台积电
"""
from __future__ import annotations

import asyncio
from datetime import datetime


# 所有带 raw_post_id 的公司表
_COMPANY_TABLES = [
    "company_narratives",
    "operational_issues",
    "financial_statements",
    "downstream_segments",
    "upstream_segments",
    "geographic_revenues",
    "non_financial_kpis",
    "debt_obligations",
    "litigations",
    "executive_compensations",
    "stock_ownership",
    "related_party_transactions",
]

# 带 period 字段的表（用于年份覆盖分析）
_TABLES_WITH_PERIOD = [
    "operational_issues",
    "financial_statements",
    "downstream_segments",
    "upstream_segments",
    "geographic_revenues",
    "non_financial_kpis",
    "debt_obligations",
    "executive_compensations",
    "stock_ownership",
    "related_party_transactions",
]


async def _find_company(session, ticker: str | None, name: str | None):
    """按 ticker 或 name 模糊查找公司，返回 (id, name, ticker)。"""
    from sqlalchemy import text

    if ticker:
        row = (await session.execute(
            text("SELECT id, name, ticker FROM company_profiles WHERE ticker = :t"),
            {"t": ticker.upper()},
        )).first()
        if row:
            return row
        # 尝试模糊匹配
        row = (await session.execute(
            text("SELECT id, name, ticker FROM company_profiles WHERE UPPER(ticker) LIKE :t ORDER BY id LIMIT 1"),
            {"t": f"%{ticker.upper()}%"},
        )).first()
        return row

    if name:
        row = (await session.execute(
            text("SELECT id, name, ticker FROM company_profiles WHERE name = :n"),
            {"n": name},
        )).first()
        if row:
            return row
        row = (await session.execute(
            text("SELECT id, name, ticker FROM company_profiles WHERE name LIKE :n ORDER BY id LIMIT 1"),
            {"n": f"%{name}%"},
        )).first()
        return row

    return None


async def _query_sources(session, company_id: int):
    """查询该公司关联的全部 raw_post URL（去重）。"""
    from sqlalchemy import text

    unions = " UNION ".join(
        f"SELECT DISTINCT raw_post_id FROM {tbl} WHERE company_id = :cid AND raw_post_id IS NOT NULL"
        for tbl in _COMPANY_TABLES
    )
    sql = f"""
        SELECT rp.id, rp.url, rp.source, rp.author_name, rp.posted_at, rp.content_summary
        FROM raw_posts rp
        WHERE rp.id IN ({unions})
        ORDER BY rp.posted_at DESC
    """
    rows = (await session.execute(text(sql), {"cid": company_id})).all()
    return rows


async def _query_period_coverage(session, company_id: int):
    """查询该公司各表的 period 覆盖情况。"""
    from sqlalchemy import text

    periods: set[str] = set()
    table_periods: dict[str, set[str]] = {}
    for tbl in _TABLES_WITH_PERIOD:
        sql = f"SELECT DISTINCT period FROM {tbl} WHERE company_id = :cid"
        rows = (await session.execute(text(sql), {"cid": company_id})).all()
        ps = {r[0] for r in rows}
        table_periods[tbl] = ps
        periods |= ps
    return periods, table_periods


def _extract_year(period: str) -> int | None:
    """从 period 字符串中提取年份，如 'FY2025' -> 2025, '2024Q4' -> 2024。"""
    import re
    m = re.search(r"(\d{4})", period)
    return int(m.group(1)) if m else None


async def _run(ticker: str | None, name: str | None, years: int):
    from anchor.database.session import AsyncSessionLocal

    async with AsyncSessionLocal() as s:
        company = await _find_company(s, ticker, name)
        if not company:
            query = ticker or name
            print(f"  未找到公司: {query!r}")
            # 列出已有公司
            from sqlalchemy import text
            rows = (await s.execute(text("SELECT ticker, name FROM company_profiles ORDER BY name"))).all()
            if rows:
                print(f"\n  已有 {len(rows)} 家公司:")
                for r in rows:
                    print(f"    {r[0]:12s}  {r[1]}")
            return

        cid, cname, cticker = company
        print(f"\n{'='*60}")
        print(f"  公司: {cname} ({cticker})   id={cid}")
        print(f"{'='*60}")

        # 1. 源 URL 列表
        sources = await _query_sources(s, cid)
        print(f"\n  输入源 URL ({len(sources)} 条):")
        print(f"  {'─'*56}")
        for row in sources:
            post_id, url, source, author, posted_at, summary = row
            ts = str(posted_at)[:10] if posted_at else "?"
            print(f"  [{ts}] {source:8s} {author}")
            print(f"         {url}")
            if summary:
                print(f"         {summary}")
            print()

        # 2. 年份覆盖分析
        periods, table_periods = await _query_period_coverage(s, cid)
        covered_years = sorted({y for p in periods if (y := _extract_year(p)) is not None}, reverse=True)
        current_year = datetime.now().year
        expected_years = list(range(current_year, current_year - years, -1))

        print(f"  期间覆盖 (期望最近 {years} 年: {expected_years[0]}–{expected_years[-1]}):")
        print(f"  {'─'*56}")

        if covered_years:
            print(f"  已有数据年份: {', '.join(str(y) for y in covered_years)}")
        else:
            print(f"  暂无数据")

        missing_years = [y for y in expected_years if y not in covered_years]
        if missing_years:
            print(f"  ⚠ 缺失年份: {', '.join(str(y) for y in missing_years)}")
        else:
            print(f"  ✓ 最近 {years} 年数据完整")

        # 3. 各表详情
        print(f"\n  各表 period 详情:")
        print(f"  {'─'*56}")
        for tbl in _TABLES_WITH_PERIOD:
            ps = table_periods.get(tbl, set())
            if ps:
                sorted_ps = sorted(ps)
                print(f"  {tbl:30s}  {', '.join(sorted_ps)}")
            else:
                print(f"  {tbl:30s}  (无数据)")


def company_sources_command(ticker: str | None, name: str | None, years: int):
    asyncio.run(_run(ticker, name, years))


# ── 自动检测（供 pipeline 调用）──────────────────────────────────────────


async def check_company_coverage(
    session,
    company_id: int,
    company_name: str,
    ticker: str,
    years: int = 5,
) -> dict:
    """检查公司年份覆盖，返回 {covered_years, missing_years, message}。

    供 extract_company_write 在写入完成后自动调用。
    """
    from sqlalchemy import text

    current_year = datetime.now().year
    expected_years = set(range(current_year, current_year - years, -1))

    # 汇总所有带 period 表的年份
    covered_years: set[int] = set()
    for tbl in _TABLES_WITH_PERIOD:
        sql = f"SELECT DISTINCT period FROM {tbl} WHERE company_id = :cid"
        rows = (await session.execute(text(sql), {"cid": company_id})).all()
        for r in rows:
            y = _extract_year(r[0])
            if y is not None:
                covered_years.add(y)

    missing = sorted(expected_years - covered_years, reverse=True)
    covered = sorted(covered_years & expected_years, reverse=True)

    if missing:
        msg = (
            f"⚠ {company_name} ({ticker}) 最近 {years} 年数据不完整 — "
            f"缺失: {', '.join(str(y) for y in missing)}  "
            f"已有: {', '.join(str(y) for y in covered) or '无'}"
        )
    else:
        msg = f"✓ {company_name} ({ticker}) 最近 {years} 年数据完整"

    return {
        "covered_years": sorted(covered_years, reverse=True),
        "missing_years": missing,
        "message": msg,
    }
