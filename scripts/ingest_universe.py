"""
行业底表构建脚本
================
1. 从 data/sp500.csv 读取标普 500 名单
2. 批量入库 10-K（断点续传，跳过已入库的）
3. 从 downstream_segments 按 GICS sector 分批聚类行业标签
4. 交叉验证：同行业内公司互相提及时检查一致性
5. 写入 industry_members(status=suggested)，不确定的标记原因

用法:
    python scripts/ingest_universe.py                   # 完整流程
    python scripts/ingest_universe.py --skip-ingest     # 跳过下载，只生成行业映射
    python scripts/ingest_universe.py --sector "Energy"  # 只跑某个 GICS sector
    python scripts/ingest_universe.py --concurrency 3   # 并发数（默认 3）
"""

import asyncio
import csv
import sys
import time
from pathlib import Path

# 确保 scripts/ 可以互相 import
sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger

SP500_CSV = Path(__file__).parent.parent / "data" / "sp500.csv"


def load_sp500(sector_filter: str | None = None) -> list[dict]:
    """从 CSV 读取标普 500 名单。"""
    companies = []
    with open(SP500_CSV) as f:
        for row in csv.DictReader(f):
            if sector_filter and row["GICS Sector"] != sector_filter:
                continue
            companies.append({
                "ticker": row["Symbol"],
                "name": row["Security"],
                "sector": row["GICS Sector"],
                "sub_industry": row["GICS Sub-Industry"],
            })
    return companies


# ══════════════════════════════════════════════════════════════
#  第一步：批量入库 10-K
# ══════════════════════════════════════════════════════════════

async def batch_ingest(companies: list[dict], concurrency: int = 3):
    """批量入库 10-K，断点续传。"""
    from anchor.config import settings
    settings.llm_mode = "cloud"
    from anchor.database.session import AsyncSessionLocal, create_tables
    from anchor.models import CompanyProfile
    from sqlmodel import select

    await create_tables()

    # 找出已入库的
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(CompanyProfile.ticker))
        existing = {r[0] for r in result.all()}

    todo = [c for c in companies if c["ticker"] not in existing]
    logger.info(
        f"[Universe] 共 {len(companies)} 家，已入库 {len(existing)}，"
        f"待入库 {len(todo)}，并发 {concurrency}"
    )

    if not todo:
        logger.info("[Universe] 全部已入库")
        return

    from scripts.ingest_company import ingest_company

    sem = asyncio.Semaphore(concurrency)
    success = 0
    failed = []
    lock = asyncio.Lock()
    counter = 0

    async def _run_one(company: dict):
        nonlocal success, counter
        async with sem:
            async with lock:
                counter += 1
                idx = counter
            ticker = company["ticker"]
            logger.info(f"  [{idx}/{len(todo)}] {ticker} — {company['name']}")
            try:
                await ingest_company(ticker, forms=["10-K"])
                async with lock:
                    success += 1
            except Exception as e:
                logger.error(f"  [{ticker}] 失败: {e}")
                async with lock:
                    failed.append(ticker)

    await asyncio.gather(*[_run_one(c) for c in todo])

    logger.info(
        f"\n[Universe] 入库完成: 成功 {success}，失败 {len(failed)}"
    )
    if failed:
        logger.warning(f"[Universe] 失败列表: {failed}")


# ══════════════════════════════════════════════════════════════
#  第二步：生成行业映射建议
# ══════════════════════════════════════════════════════════════

async def generate_industry_suggestions():
    """从 downstream_segments 生成 industry_members 建议。按 GICS sector 分批。"""
    from anchor.config import settings
    settings.llm_mode = "cloud"
    from anchor.database.session import AsyncSessionLocal, create_tables
    from anchor.models import (
        CompanyProfile, DownstreamSegment, IndustryMember,
    )
    from anchor.llm_client import chat_completion
    from sqlmodel import select, delete
    import json
    import re

    await create_tables()

    # 读取所有公司的 downstream_segments
    async with AsyncSessionLocal() as session:
        stmt = (
            select(
                CompanyProfile.id,
                CompanyProfile.ticker,
                CompanyProfile.name,
                DownstreamSegment.customer_name,
                DownstreamSegment.revenue_pct,
                DownstreamSegment.product_category,
            )
            .join(DownstreamSegment, DownstreamSegment.company_id == CompanyProfile.id)
            .order_by(CompanyProfile.ticker, DownstreamSegment.revenue_pct.desc())
        )
        rows = (await session.execute(stmt)).all()

    if not rows:
        logger.error("[Industry] 无 downstream_segments 数据，请先 batch_ingest")
        return

    # 按公司分组
    companies = {}
    for company_id, ticker, name, seg_name, rev_pct, prod_cat in rows:
        if ticker not in companies:
            companies[ticker] = {
                "id": company_id, "name": name, "segments": []
            }
        companies[ticker]["segments"].append({
            "name": seg_name,
            "revenue_pct": rev_pct or 0,
            "product_category": prod_cat or "",
        })

    logger.info(f"[Industry] {len(companies)} 家公司，共 {len(rows)} 条业务线")

    # 用 sp500.csv 的 GICS sector 分批
    sp500 = {c["ticker"]: c for c in load_sp500()}

    # 按 sector 分组
    by_sector: dict[str, dict] = {}
    for ticker, info in companies.items():
        sector = sp500.get(ticker, {}).get("sector", "Other")
        if sector not in by_sector:
            by_sector[sector] = {}
        by_sector[sector][ticker] = info

    logger.info(f"[Industry] 按 GICS Sector 分 {len(by_sector)} 批:")
    for sector, members in sorted(by_sector.items()):
        seg_count = sum(len(m["segments"]) for m in members.values())
        logger.info(f"  {sector}: {len(members)} 家, {seg_count} 条业务线")

    # 逐批调 LLM
    all_suggestions = []
    for sector, sector_companies in sorted(by_sector.items()):
        logger.info(f"\n[Industry] 处理 sector: {sector} ({len(sector_companies)} 家)...")

        company_summaries = []
        for ticker, info in sector_companies.items():
            segs = "\n".join(
                f"    - {s['name']} ({s['revenue_pct']:.0%}) [{s['product_category']}]"
                for s in info["segments"]
            )
            company_summaries.append(f"  {ticker} ({info['name']}):\n{segs}")

        all_text = "\n".join(company_summaries)
        seg_count = sum(len(m["segments"]) for m in sector_companies.values())

        system = """你是行业分析师。根据公司的业务线，给每条业务线分配一个行业标签。

规则:
1. 行业颗粒度: 同一行业内的公司应该在抢同一批客户
   - 正确: "汽车"（新能源和燃油车抢同一批买家）
   - 正确: "半导体设计"（AMD/NVIDIA/Broadcom 抢同一批客户）
   - 错误: "新能源汽车"（太细，燃油车也是竞对）
   - 错误: "科技"（太粗，芯片和社交媒体不抢同一批客户）
2. 一家公司的不同业务线可以属于不同行业
   - 例: Amazon 的 AWS = "云计算"，电商 = "电商零售"
3. 同一个行业标签在不同公司间必须完全一致（比如不能一个叫"云计算"一个叫"云服务"）
4. 行业标签用中文，简洁统一
5. 如果一条业务线的行业归属不确定，在 uncertain 字段标 true 并说明原因
6. 如果一条业务线横跨多个行业（如综合集团），拆成多条，每条对应一个行业

只输出 JSON:
```json
[
  {"ticker": "AAPL", "segment": "iPhone", "industry": "消费电子", "uncertain": false, "reason": ""}
]
```"""

        user = (
            f"GICS Sector: {sector}\n"
            f"以下 {len(sector_companies)} 家公司，共 {seg_count} 条业务线，请分配行业标签:\n\n"
            f"{all_text}"
        )

        resp = await chat_completion(system, user, max_tokens=8000)
        if not resp:
            logger.error(f"[Industry] {sector} LLM 调用失败")
            continue

        # 解析 JSON
        text = resp.content.strip()
        if text.startswith("```"):
            text = re.sub(r"^```\w*\n?", "", text)
            text = re.sub(r"\n?```\s*$", "", text)

        try:
            batch = json.loads(text)
        except json.JSONDecodeError:
            start = text.find("[")
            end = text.rfind("]")
            if start >= 0 and end > start:
                try:
                    batch = json.loads(text[start:end + 1])
                except json.JSONDecodeError:
                    logger.error(f"[Industry] {sector} JSON 解析失败")
                    continue
            else:
                logger.error(f"[Industry] {sector} 无 JSON 数组")
                continue

        all_suggestions.extend(batch)
        logger.info(f"[Industry] {sector}: {len(batch)} 条建议")

    # ── 归一化：LLM 合并同义行业标签 ──
    industry_members_map: dict[str, list[str]] = {}  # industry → [ticker:segment, ...]
    for item in all_suggestions:
        ind = item.get("industry", "")
        key = f"{item.get('ticker')}:{item.get('segment')}"
        if ind not in industry_members_map:
            industry_members_map[ind] = []
        industry_members_map[ind].append(key)

    industry_names = list(industry_members_map.keys())
    logger.info(f"\n[Industry] 归一化前: {len(industry_names)} 个行业标签")

    if len(industry_names) > 1:
        # 构造归一化 prompt：给 LLM 看所有标签 + 每个标签下的公司，让它合并同义词
        label_summary = "\n".join(
            f"  {name} ({len(members)} 条): {', '.join(members[:5])}"
            + (f" ... +{len(members)-5}" if len(members) > 5 else "")
            for name, members in sorted(industry_members_map.items())
        )

        norm_system = """你是行业分类专家。下面是一组行业标签和它们包含的公司业务线。
你的任务:
1. 合并同义标签（如 "oilfield_services" 和 "oil_equipment" 应合并）
2. 确保每个最终标签代表"抢同一批客户"的竞争群体
3. 如果某个标签内的公司其实不是竞对（不抢同一批客户），拆开
4. 如果某个公司的业务线明显归错了行业，标记出来

输出 JSON:
```json
{
  "merges": {
    "旧标签A": "统一后的标签",
    "旧标签B": "统一后的标签"
  },
  "reclassify": [
    {"ticker": "XXX", "segment": "YYY", "from": "旧行业", "to": "新行业", "reason": "原因"}
  ],
  "uncertain": [
    {"ticker": "XXX", "segment": "YYY", "reason": "为什么不确定"}
  ]
}
```
- merges: 需要合并的标签映射（没变化的不用列）
- reclassify: 需要改行业的条目
- uncertain: 无法确定归属的条目"""

        norm_user = f"当前 {len(industry_names)} 个行业标签:\n\n{label_summary}"

        logger.info("[Industry] 调用 LLM 归一化行业标签...")
        norm_resp = await chat_completion(norm_system, norm_user, max_tokens=4000)

        if norm_resp:
            norm_text = norm_resp.content.strip()
            if norm_text.startswith("```"):
                norm_text = re.sub(r"^```\w*\n?", "", norm_text)
                norm_text = re.sub(r"\n?```\s*$", "", norm_text)

            try:
                # 尝试解析 JSON object
                start = norm_text.find("{")
                end = norm_text.rfind("}")
                if start >= 0 and end > start:
                    norm_data = json.loads(norm_text[start:end + 1])
                else:
                    norm_data = json.loads(norm_text)

                # 应用合并
                merges = norm_data.get("merges", {})
                if merges:
                    logger.info(f"[Industry] 合并 {len(merges)} 个标签:")
                    for old, new in merges.items():
                        logger.info(f"  {old} → {new}")
                    for item in all_suggestions:
                        old_ind = item.get("industry", "")
                        if old_ind in merges:
                            item["industry"] = merges[old_ind]

                # 应用重分类
                for rc in norm_data.get("reclassify", []):
                    for item in all_suggestions:
                        if (item.get("ticker") == rc.get("ticker")
                                and item.get("segment") == rc.get("segment")):
                            logger.info(
                                f"[Industry] 重分类: {rc['ticker']}:{rc['segment']} "
                                f"{rc['from']} → {rc['to']} ({rc.get('reason', '')})"
                            )
                            item["industry"] = rc["to"]

                # 标记不确定
                for unc in norm_data.get("uncertain", []):
                    for item in all_suggestions:
                        if (item.get("ticker") == unc.get("ticker")
                                and item.get("segment") == unc.get("segment")):
                            item["uncertain"] = True
                            item["reason"] = unc.get("reason", "归一化阶段标记")

            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"[Industry] 归一化 JSON 解析失败: {e}，跳过归一化")

        # 重建 industry_members_map
        industry_members_map = {}
        for item in all_suggestions:
            ind = item.get("industry", "")
            key = f"{item.get('ticker')}:{item.get('segment')}"
            if ind not in industry_members_map:
                industry_members_map[ind] = []
            industry_members_map[ind].append(key)

        logger.info(f"[Industry] 归一化后: {len(industry_members_map)} 个行业标签")

    # ── 写入 DB ──
    written = 0
    uncertain_count = 0
    async with AsyncSessionLocal() as session:
        # 清理旧的 suggested（保留 confirmed）
        await session.execute(
            delete(IndustryMember).where(IndustryMember.status == "suggested")
        )

        for item in all_suggestions:
            ticker = item.get("ticker", "")
            if ticker not in companies:
                continue

            is_uncertain = item.get("uncertain", False)
            reason = item.get("reason", "")
            seg_name = item.get("segment", "")

            rev_pct = None
            for s in companies[ticker]["segments"]:
                if s["name"] == seg_name:
                    rev_pct = s["revenue_pct"]
                    break

            member = IndustryMember(
                company_id=companies[ticker]["id"],
                segment=seg_name,
                industry=item.get("industry", ""),
                revenue_pct=rev_pct,
                status="suggested",
                source=f"LLM auto-suggest | {'⚠ ' + reason if is_uncertain else 'confident'}",
            )
            session.add(member)
            written += 1
            if is_uncertain:
                uncertain_count += 1

        await session.commit()

    logger.info(f"\n[Industry] 写入 {written} 条 industry_members (suggested)")
    logger.info(f"[Industry] 其中 {uncertain_count} 条需要人工确认")

    # 打印结果
    logger.info(f"\n{'='*70}")
    logger.info(f"  行业映射建议 — {len(industry_members_map)} 个行业")
    logger.info(f"{'='*70}")

    for ind, members in sorted(industry_members_map.items(), key=lambda x: -len(x[1])):
        logger.info(f"\n  【{ind}】({len(members)} 条)")
        for m in members:
            flag = ""
            for item in all_suggestions:
                if f"{item.get('ticker')}:{item.get('segment')}" == m and item.get("uncertain"):
                    flag = " ⚠"
                    break
            logger.info(f"    {m}{flag}")

    if uncertain_count:
        logger.info(f"\n  ⚠ = 需要人工确认行业归属")


# ══════════════════════════════════════════════════════════════
#  主入口
# ══════════════════════════════════════════════════════════════

async def main():
    args = sys.argv[1:]
    skip_ingest = "--skip-ingest" in args
    concurrency = 3
    sector_filter = None

    for i, arg in enumerate(args):
        if arg == "--concurrency" and i + 1 < len(args):
            concurrency = int(args[i + 1])
        if arg == "--sector" and i + 1 < len(args):
            sector_filter = args[i + 1]

    companies = load_sp500(sector_filter)
    logger.info(f"[Universe] 标普 500: {len(companies)} 家" +
                (f" (sector={sector_filter})" if sector_filter else ""))

    if not skip_ingest:
        await batch_ingest(companies, concurrency=concurrency)

    await generate_industry_suggestions()


if __name__ == "__main__":
    asyncio.run(main())
