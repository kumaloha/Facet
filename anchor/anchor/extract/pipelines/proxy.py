"""
Proxy / DEF 14A 提取管线
========================
提取治理数据：高管薪酬、持股、关联交易、CEO Pay Ratio。
Proxy 通常不需要 MapReduce（篇幅相对短），单次提取。

提取目标 (4 张表):
  - executive_compensations
  - stock_ownership
  - related_party_transactions
  - executive_changes
"""

from __future__ import annotations

from loguru import logger

from anchor.extract.pipelines._mapreduce import (
    ChunkMeta,
    ExtractionResult,
    map_reduce_extract,
)

_BASE = "你是资深基本面分析师。只输出 JSON，无数据返回空数组 []。\n"

SECTION_PROMPTS = {
    "default": _BASE + """
## 任务：从 Proxy Statement (DEF 14A) 提取治理信息

输出 JSON:
```json
{
  "executive_compensations": [
    {
      "name": "姓名",
      "title": "职位",
      "role_type": "CEO|CFO|executive|director",
      "base_salary": null,
      "bonus": null,
      "stock_awards": null,
      "option_awards": null,
      "non_equity_incentive": null,
      "other_comp": null,
      "total_comp": null,
      "pay_ratio": null,
      "median_employee_comp": null
    }
  ],
  "stock_ownership": [
    {
      "name": "持有人",
      "title": "职位",
      "shares_beneficially_owned": null,
      "percent_of_class": 0.05
    }
  ],
  "related_party_transactions": [
    {
      "related_party": "关联方",
      "relationship": "director|officer|major_shareholder|subsidiary",
      "transaction_type": "sale|purchase|lease|loan|service",
      "amount": null,
      "is_ongoing": false,
      "description": "交易说明 ≤200字"
    }
  ],
  "executive_changes": [
    {
      "person_name": "姓名",
      "title": "职位",
      "change_type": "joined|departed|promoted",
      "change_date": "YYYY-MM-DD"
    }
  ]
}
```

## 提取规则
- 金额百万美元
- pay_ratio: CEO 总薪酬 / 员工中位数薪酬（如 CEO Pay Ratio = 200:1 → 填 200）
- percent_of_class: 用小数（5% → 0.05）
- 只提最高管理层（CEO/CFO/高管/董事），普通员工不提
""",
}

DEDUP_KEYS = {
    "executive_compensations": "name",
    "stock_ownership": "name",
    "related_party_transactions": "related_party",
    "executive_changes": "person_name",
}


def _chunk_proxy(content: str) -> list[ChunkMeta]:
    """将 Proxy 按 30K 段切分，跳过前面的 cover page。"""
    import re
    # 跳到实质内容（找 "compensation" 或 "director" 等关键词）
    m = re.search(r'(?i)(executive\s+compensation|compensation\s+discussion|director\s+nominees)', content)
    start = max(0, m.start() - 500) if m else 0

    # 按 30K 切段
    useful = content[start:]
    chunk_size = 30000
    chunks = []
    for i in range(0, len(useful), chunk_size):
        chunk = useful[i:i + chunk_size]
        if len(chunk) > 200:
            chunks.append(ChunkMeta(f"proxy_{i // chunk_size + 1}", chunk, "default"))
    return chunks or [ChunkMeta("proxy", content[:30000], "default")]


async def extract_proxy(
    content: str,
    metadata: dict | None = None,
) -> ExtractionResult:
    """从 Proxy Statement 提取治理数据。"""
    metadata = metadata or {}

    chunks = _chunk_proxy(content)

    tables = await map_reduce_extract(
        chunks=chunks,
        section_prompts=SECTION_PROMPTS,
        dedup_keys=DEDUP_KEYS,
        max_tokens=8000,
    )

    result = ExtractionResult(
        company_ticker=metadata.get("ticker", ""),
        period=metadata.get("period", ""),
        tables=tables,
        metadata=metadata,
    )
    logger.info(f"[Proxy] 提取完成: {sum(len(v) for v in tables.values())} 行")
    return result
