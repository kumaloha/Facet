"""
竞争情报提取管线
================
从行业报告、分析师研究中提取竞争格局数据。

提取目标 (4 张表):
  - competitive_dynamics
  - peer_financials
  - market_share_data
  - known_issues
"""

from __future__ import annotations

from loguru import logger

from anchor.extract.pipelines._mapreduce import (
    ChunkMeta,
    ExtractionResult,
    map_reduce_extract,
)

_BASE = "你是资深基本面分析师。从行业报告/分析师研究中提取竞争格局数据。只输出 JSON。\n"

SECTION_PROMPTS = {
    "default": _BASE + """
## 任务：提取竞争格局、同行财务、市占率

输出 JSON:
```json
{
  "competitive_dynamics": [
    {
      "competitor_name": "竞争对手",
      "event_type": "price_war|new_entry|exit|product_launch|patent_challenge|patent_expiration|regulatory_change|industry_downturn|migration_tool",
      "event_description": "事件描述 ≤200字",
      "outcome_description": "结果/影响 ≤200字",
      "outcome_market_share_change": null,
      "event_date": "YYYY-MM 或 null"
    }
  ],
  "peer_financials": [
    {
      "peer_name": "同行公司名",
      "metric": "gross_margin|operating_margin|net_margin|revenue",
      "value": 0.35,
      "period": "FY2025",
      "segment": "对应哪条业务线（可选）",
      "source": "数据来源"
    }
  ],
  "market_share_data": [
    {
      "company_or_competitor": "公司名",
      "market_segment": "细分市场",
      "share_pct": 0.25,
      "source_description": "数据来源（如 IDC/Gartner）"
    }
  ],
  "known_issues": [
    {
      "issue_description": "第三方发现的问题 ≤200字",
      "issue_category": "financial|operational|legal|regulatory",
      "severity": "critical|major|minor",
      "source_type": "analyst"
    }
  ]
}
```

## 提取规则
- peer_financials: 只提有具体数字的财务指标，模糊描述不提
- competitive_dynamics: 只提有明确事件的竞争动态，不提泛泛的竞争描述
- market_share_data: 标注数据来源和时期
""",
}

DEDUP_KEYS = {
    "competitive_dynamics": "competitor_name",
    "peer_financials": "peer_name",
    "market_share_data": "company_or_competitor",
    "known_issues": "issue_description",
}


async def extract_competitive_intel(
    content: str,
    metadata: dict | None = None,
) -> ExtractionResult:
    """从行业报告/分析师研究提取竞争情报。"""
    metadata = metadata or {}

    chunks = [ChunkMeta("intel", content[:80000], "default")]

    tables = await map_reduce_extract(
        chunks=chunks,
        section_prompts=SECTION_PROMPTS,
        dedup_keys=DEDUP_KEYS,
        max_tokens=8000,
    )

    return ExtractionResult(
        company_ticker=metadata.get("ticker", ""),
        period=metadata.get("period", ""),
        tables=tables,
        metadata=metadata,
    )
