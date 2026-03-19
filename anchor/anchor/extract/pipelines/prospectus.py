"""
招股书 (S-1 / F-1 / 424B) 提取管线
=====================================
新公司建档的首选数据源——行业结构描述最完整。

提取目标 (7 张表):
  - downstream_segments     ← Business 章节
  - upstream_segments       ← Business 章节
  - competitive_dynamics    ← Competition / Risk Factors
  - peer_financials         ← Industry / Competition
  - market_share_data       ← Industry Overview
  - known_issues            ← Risk Factors
  - company_narratives      ← Business / Use of Proceeds

MapReduce 策略：按章节拆分，招股书通常很长（100-300 页）。
"""

from __future__ import annotations

import re

from loguru import logger

from anchor.extract.pipelines._mapreduce import (
    ChunkMeta,
    ExtractionResult,
    map_reduce_extract,
)


_S1_SECTION_PATTERNS = [
    (r"(?i)\bbusiness\b", "business"),
    (r"(?i)\brisk\s*factors\b", "risk_factors"),
    (r"(?i)\bcompetition\b", "competition"),
    (r"(?i)\bindustry\s*(overview|background)\b", "industry"),
    (r"(?i)\buse\s*of\s*proceeds\b", "use_of_proceeds"),
    (r"(?i)\bmanagement.{0,20}discussion\b", "mda"),
    (r"(?i)\bfinancial\s*statements\b", "financials"),
]


def chunk_prospectus(content: str) -> list[ChunkMeta]:
    """按招股书章节拆分。"""
    positions = []
    for pattern, name in _S1_SECTION_PATTERNS:
        for m in re.finditer(pattern, content):
            positions.append((m.start(), name))

    if len(positions) < 2:
        logger.warning("[Prospectus] 无法识别章节，跳过 boilerplate 按长度切分")
        # 跳过前面的 cover page / TOC / legal boilerplate
        m = re.search(r'(?i)(our\s+business|business\s+overview|company\s+overview|our\s+mission)', content)
        start = max(0, m.start() - 500) if m else 0
        useful = content[start:]
        chunks = []
        for i in range(0, min(len(useful), 180000), 30000):
            chunks.append(ChunkMeta(f"part_{i//30000+1}",
                                     useful[i:i+30000], "business"))
        return chunks or [ChunkMeta("full", content[:30000], "business")]

    positions.sort(key=lambda x: x[0])
    # 去重同名段（取第一个出现的）
    seen = set()
    unique = []
    for pos, name in positions:
        if name not in seen:
            seen.add(name)
            unique.append((pos, name))
    positions = unique

    chunks = []
    for i, (start, name) in enumerate(positions):
        end = positions[i+1][0] if i+1 < len(positions) else len(content)
        text = content[start:end].strip()
        if len(text) > 200:
            if len(text) > 80000:
                for j in range(0, len(text), 60000):
                    chunks.append(ChunkMeta(f"{name}_{j//60000+1}",
                                             text[j:j+60000], name))
            else:
                chunks.append(ChunkMeta(name, text, name))

    logger.info(f"[Prospectus] 拆分为 {len(chunks)} 段: {[c.section_name for c in chunks]}")
    return chunks or [ChunkMeta("full", content[:60000], "business")]


_BASE = "你是资深基本面分析师。从招股书中提取结构化数据。只输出 JSON，无数据返回空数组 []。\n"

SECTION_PROMPTS = {
    "business": _BASE + """
## 任务：从招股书 Business 章节提取业务结构

输出 JSON:
```json
{
  "downstream_segments": [
    {
      "customer_name": "业务线名称",
      "revenue_pct": 0.40,
      "product_category": "beverage|commodity|cloud_infrastructure|insurance|banking|payment|healthcare|pharma|gaming|social_media|consumer_electronics|industrial_equipment|food|grocery|liquor|tobacco|operating_system|pipeline|utility",
      "revenue_type": "product_sale|subscription|license|transaction_fee|ad_revenue|recurring|saas",
      "switching_cost_level": "high|medium|low|null",
      "contract_duration": "one-time|1-year|multi-year|5-year|null",
      "product_criticality": "high|medium|low|null",
      "segment_gross_margin": null,
      "description": "业务线描述 ≤200字"
    }
  ],
  "upstream_segments": [
    {
      "supplier_name": "供应商/供应类型",
      "supply_type": "foundry|memory|assembly|component|raw_material|logistics|software",
      "geographic_location": "所在地",
      "is_sole_source": false,
      "concentration_risk": "风险描述"
    }
  ],
  "company_narratives": [
    {
      "narrative": "公司的战略定位/使命/愿景 ≤300字",
      "status": "announced"
    }
  ]
}
```
""",

    "competition": _BASE + """
## 任务：从招股书 Competition 章节提取竞争格局

输出 JSON:
```json
{
  "competitive_dynamics": [
    {
      "competitor_name": "竞争对手",
      "event_type": "direct_competitor|new_entry|price_war|product_launch",
      "event_description": "竞争关系描述 ≤200字",
      "outcome_description": "公司的应对/结果 ≤200字"
    }
  ],
  "peer_financials": [
    {
      "peer_name": "同行公司名",
      "metric": "gross_margin|operating_margin|net_margin|revenue",
      "value": 0.35,
      "segment": "对应本公司哪条业务线（可选）",
      "source": "招股书引用"
    }
  ],
  "market_share_data": [
    {
      "company_or_competitor": "公司名",
      "market_segment": "细分市场",
      "share_pct": 0.25,
      "source_description": "数据来源"
    }
  ]
}
```

## 提取规则
- 招股书通常会列出完整的竞争对手列表和各自的优劣势，全部提取
- 市占率数据如果有引用第三方报告（IDC/Gartner），标注 source
""",

    "industry": _BASE + """
## 任务：从招股书 Industry Overview 提取行业数据

输出 JSON:
```json
{
  "market_share_data": [
    {
      "company_or_competitor": "公司名",
      "market_segment": "细分市场",
      "share_pct": 0.25,
      "source_description": "数据来源"
    }
  ],
  "peer_financials": [
    {
      "peer_name": "同行",
      "metric": "revenue|gross_margin",
      "value": null,
      "source": "招股书引用"
    }
  ]
}
```
""",

    "risk_factors": _BASE + """
## 任务：从招股书 Risk Factors 提取风险信息

输出 JSON:
```json
{
  "known_issues": [
    {
      "issue_description": "风险描述 ≤200字",
      "issue_category": "financial|operational|legal|regulatory|reputational|geopolitical",
      "severity": "critical|major|minor",
      "source_type": "prospectus"
    }
  ],
  "competitive_dynamics": [
    {
      "competitor_name": "竞争对手",
      "event_type": "new_entry|regulatory_change|patent_challenge|patent_expiration",
      "event_description": "风险描述 ≤200字"
    }
  ]
}
```
""",

    # mda, use_of_proceeds, financials 用 business prompt fallback
    "mda": None,  # 跳过
    "use_of_proceeds": None,
    "financials": None,
}

DEDUP_KEYS = {
    "downstream_segments": "customer_name",
    "upstream_segments": "supplier_name",
    "competitive_dynamics": "competitor_name",
    "peer_financials": "peer_name",
    "market_share_data": "company_or_competitor",
    "known_issues": "issue_description",
    "company_narratives": "narrative",
}


async def extract_prospectus(
    content: str,
    metadata: dict | None = None,
) -> ExtractionResult:
    """从招股书提取行业结构和竞争格局。"""
    metadata = metadata or {}

    # 过滤掉 None prompt 的段
    chunks = [c for c in chunk_prospectus(content)
              if SECTION_PROMPTS.get(c.prompt_key) is not None]

    if not chunks:
        chunks = [ChunkMeta("full", content[:60000], "business")]

    tables = await map_reduce_extract(
        chunks=chunks,
        section_prompts={k: v for k, v in SECTION_PROMPTS.items() if v is not None},
        dedup_keys=DEDUP_KEYS,
        max_tokens=10000,
    )

    result = ExtractionResult(
        company_ticker=metadata.get("ticker", ""),
        period=metadata.get("period", ""),
        tables=tables,
        metadata=metadata,
    )
    logger.info(f"[Prospectus] 提取完成: {sum(len(v) for v in tables.values())} 行")
    return result
