"""
竞对数据入库脚本
================
1. 读取目标公司的 downstream_segments
2. LLM 识别每条业务线的竞对（含 ticker）
3. 从 SEC EDGAR 下载竞对的 10-K
4. 提取竞对的分业务线财务数据
5. 写入 peer_financials（带 segment 标签）

用法:
    python scripts/ingest_peers.py AAPL
"""

import asyncio
import sys
import time

from loguru import logger


async def identify_competitors(ticker: str, segments: list[dict]) -> list[dict]:
    """用 LLM 识别每条业务线的竞对。"""
    from anchor.llm_client import chat_completion

    seg_desc = "\n".join(
        f"- {s['name']} ({s['pct']:.0%}): {s.get('category', '')}"
        for s in segments
    )

    system = """你是资深行业分析师。根据公司的业务线，识别每条业务线的主要竞对。
只输出 JSON，格式:
```json
[
  {
    "segment": "业务线名称",
    "competitors": [
      {"name": "竞对公司名", "ticker": "美股ticker或null", "why": "为什么是竞对"}
    ]
  }
]
```
规则:
- 每条业务线列出 2-3 个最强竞对
- ticker 只填美股上市公司的 ticker（如 GOOGL, MSFT），非美股填 null
- 竞对应该是该业务线里最强的对手，不是最弱的"""

    user = f"""公司: {ticker}
业务线:
{seg_desc}

请识别每条业务线的 2-3 个最强竞对。"""

    resp = await chat_completion(system, user, max_tokens=2000)
    if not resp:
        return []

    from anchor.extract.pipelines._mapreduce import _parse_json_raw
    import json

    text = resp.content.strip()
    # 尝试解析 JSON array
    if text.startswith("```"):
        import re
        text = re.sub(r"^```\w*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)

    try:
        data = json.loads(text)
        if isinstance(data, list):
            return data
    except json.JSONDecodeError:
        pass

    # fallback: 尝试提取 [ ... ]
    start = text.find("[")
    end = text.rfind("]")
    if start >= 0 and end > start:
        try:
            return json.loads(text[start:end + 1])
        except json.JSONDecodeError:
            pass

    logger.error(f"[Peers] 无法解析竞对列表")
    return []


async def extract_peer_financials_from_10k(
    peer_ticker: str, peer_name: str, segment: str
) -> list[dict]:
    """从竞对的 10-K 提取关键财务指标。"""
    from edgar import Company, set_identity
    set_identity("Facet Research facet@example.com")
    from anchor.llm_client import chat_completion

    try:
        c = Company(peer_ticker)
        filings = c.get_filings(form="10-K")
        if not filings:
            logger.warning(f"  [{peer_ticker}] 无 10-K")
            return []

        latest = filings[0]
        doc = latest.document
        text = doc.text() if hasattr(doc, "text") else str(doc)
        logger.info(f"  [{peer_ticker}] 10-K: {len(text):,} chars | {latest.filing_date}")

        # 找到真实的财务数据（跳过目录页）
        # 搜索包含实际数字的收入行（如 "Revenue" + 大数字）
        import re
        # 策略：找 "Revenue" 或 "Net sales" 后面跟着大数字（>1000）的位置
        fin_text = None
        for pattern in [
            r"(?i)(total\s+)?revenue[s]?\s*[\$]?\s*[\d,]{4,}",
            r"(?i)net\s+sales\s*[\$]?\s*[\d,]{4,}",
            r"(?i)total\s+net\s+revenue[s]?\s*[\$]?\s*[\d,]{4,}",
        ]:
            m = re.search(pattern, text)
            if m:
                # 往前回退 500 字抓到表头
                start = max(0, m.start() - 500)
                fin_text = text[start:start + 20000]
                break

        if not fin_text:
            # fallback: Item 7 MD&A 通常有关键数字摘要
            m = re.search(r"(?i)item\s*7[.\s]*management", text)
            if m:
                fin_text = text[m.start():m.start() + 20000]
            else:
                fin_text = text[:20000]

    except Exception as e:
        logger.warning(f"  [{peer_ticker}] 下载失败: {e}")
        return []

    # 用 LLM 提取关键指标
    system = """你是财务分析师。从 10-K 财务报表中提取关键利润率指标。
只输出 JSON:
```json
{
  "gross_margin": 0.45,
  "operating_margin": 0.20,
  "net_margin": 0.15,
  "revenue": 50000
}
```
- 比率用小数（45% → 0.45）
- revenue 用百万美元
- 取最新完整年度数据
- 没有的填 null"""

    user = f"公司: {peer_name} ({peer_ticker})\n\n{fin_text}"

    resp = await chat_completion(system, user, max_tokens=500)
    if not resp:
        return []

    from anchor.extract.pipelines._mapreduce import _parse_json_raw
    data = _parse_json_raw(resp.content)
    if not data:
        return []

    results = []
    report_end = getattr(latest, "period_of_report", None) or latest.filing_date
    period = f"FY{report_end.year}"
    for metric in ("gross_margin", "operating_margin", "net_margin", "revenue"):
        val = data.get(metric)
        if val is not None:
            results.append({
                "peer_name": peer_name,
                "metric": metric,
                "value": float(val),
                "period": period,
                "segment": segment,
                "source": f"{peer_ticker} 10-K ({latest.filing_date})",
            })

    return results


async def ingest_peers(ticker: str):
    """完整竞对入库流程。"""
    from anchor.config import settings
    settings.llm_mode = "cloud"
    from anchor.database.session import AsyncSessionLocal, create_tables
    from anchor.extract.pipelines._writer import write_extraction_result, get_or_create_company
    from anchor.extract.pipelines._mapreduce import ExtractionResult
    from anchor.models import PeerFinancial, DownstreamSegment, CompanyProfile
    from sqlmodel import select

    await create_tables()

    # 1. 读取目标公司的 downstream_segments
    async with AsyncSessionLocal() as session:
        stmt = (
            select(
                DownstreamSegment.customer_name,
                DownstreamSegment.revenue_pct,
                DownstreamSegment.product_category,
            )
            .join(CompanyProfile, DownstreamSegment.company_id == CompanyProfile.id)
            .where(CompanyProfile.ticker == ticker)
            .order_by(DownstreamSegment.revenue_pct.desc())
        )
        rows = (await session.execute(stmt)).all()

    if not rows:
        logger.error(f"[Peers] {ticker} 无 downstream_segments，请先 ingest_company")
        return

    segments = [{"name": r[0], "pct": r[1] or 0, "category": r[2] or ""} for r in rows]
    logger.info(f"[Peers] {ticker} 有 {len(segments)} 条业务线")
    for s in segments:
        logger.info(f"  {s['name']:30s} {s['pct']:.0%} ({s['category']})")

    # 2. LLM 识别竞对
    logger.info(f"\n[Peers] 识别竞对...")
    competitor_map = await identify_competitors(ticker, segments)

    if not competitor_map:
        logger.error("[Peers] 竞对识别失败")
        return

    # 收集需要下载 10-K 的竞对
    peers_to_fetch = {}  # ticker → {name, segments}
    for seg_info in competitor_map:
        seg_name = seg_info.get("segment", "")
        for comp in seg_info.get("competitors", []):
            t = comp.get("ticker")
            name = comp.get("name", "")
            if t and t != ticker:
                if t not in peers_to_fetch:
                    peers_to_fetch[t] = {"name": name, "segments": []}
                peers_to_fetch[t]["segments"].append(seg_name)

    logger.info(f"\n[Peers] 需要下载 {len(peers_to_fetch)} 家竞对的 10-K:")
    for t, info in peers_to_fetch.items():
        logger.info(f"  {t:8s} {info['name']:20s} → {info['segments']}")

    # 3. 并行下载+提取竞对财务数据
    all_peer_rows = []
    t0 = time.time()

    async def fetch_one(peer_ticker, peer_info):
        results = []
        for seg in peer_info["segments"]:
            rows = await extract_peer_financials_from_10k(
                peer_ticker, peer_info["name"], seg
            )
            results.extend(rows)
        return results

    tasks = [fetch_one(t, info) for t, info in peers_to_fetch.items()]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, (t, _) in enumerate(peers_to_fetch.items()):
        if isinstance(results[i], Exception):
            logger.error(f"  [{t}] 失败: {results[i]}")
        else:
            all_peer_rows.extend(results[i])

    elapsed = time.time() - t0
    logger.info(f"\n[Peers] 提取完成: {len(all_peer_rows)} 行 | {elapsed:.0f}s")

    # 4. 写入 DB
    if all_peer_rows:
        result = ExtractionResult(
            company_ticker=ticker,
            period="FY2025",
            tables={"peer_financials": all_peer_rows},
        )
        async with AsyncSessionLocal() as session:
            stats = await write_extraction_result(session, result, market="us")
        logger.info(f"[Peers] 写入 DB: {stats}")

    # 打印结果
    logger.info(f"\n{'='*60}")
    logger.info(f"  {ticker} 竞对数据入库完成")
    logger.info(f"{'='*60}")
    for row in all_peer_rows:
        logger.info(f"  [{row['segment']:20s}] {row['peer_name']:20s} "
                     f"{row['metric']:20s} = {row['value']}")


if __name__ == "__main__":
    ticker = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    asyncio.run(ingest_peers(ticker))
