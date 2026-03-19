"""
财报电话会提取管线
==================
拆为 prepared_remarks + Q&A，分别提取。

提取目标 (5 张表):
  - management_guidance    ← prepared remarks
  - company_narratives     ← prepared remarks + Q&A
  - management_acknowledgments ← Q&A（分析师追问后的回应）
  - known_issues           ← Q&A（分析师提出的担忧）
  - pricing_actions        ← prepared remarks
"""

from __future__ import annotations

import re

from loguru import logger

from anchor.extract.pipelines._mapreduce import (
    ChunkMeta,
    ExtractionResult,
    map_reduce_extract,
)


def chunk_earnings_call(content: str) -> list[ChunkMeta]:
    """拆分电话会为 prepared_remarks + Q&A。"""

    # 常见的 Q&A 分隔标记
    qa_patterns = [
        r"(?i)question.{0,10}answer",
        r"(?i)Q\s*&\s*A\s*session",
        r"(?i)operator.*first question",
        r"(?i)we.{0,20}now open.{0,30}questions",
    ]

    split_pos = None
    for p in qa_patterns:
        m = re.search(p, content)
        if m:
            split_pos = m.start()
            break

    if split_pos and split_pos > len(content) * 0.15:
        remarks = content[:split_pos].strip()
        qa = content[split_pos:].strip()
        chunks = []
        if remarks:
            chunks.append(ChunkMeta("prepared_remarks", remarks[:80000], "remarks"))
        if qa:
            chunks.append(ChunkMeta("qa_session", qa[:80000], "qa"))
        if chunks:
            return chunks

    # 找不到分隔，整体处理
    return [ChunkMeta("full_call", content[:80000], "remarks")]


_BASE = "你是资深基本面分析师。只输出 JSON，无数据返回空数组 []。\n"

SECTION_PROMPTS = {
    "remarks": _BASE + """
## 任务：从财报电话会 Prepared Remarks 提取管理层指引和战略叙事

输出 JSON:
```json
{
  "management_guidance": [
    {
      "metric": "revenue_growth|operating_margin|eps|capex|roic_target|free_cash_flow|gross_margin|net_margin|tax_rate",
      "value_low": null,
      "value_high": null,
      "unit": "pct|absolute|per_share",
      "confidence_language": "expect|target|aspire|plan",
      "verbatim": "原文引用 ≤100字"
    }
  ],
  "company_narratives": [
    {
      "narrative": "管理层的战略承诺/愿景 ≤300字",
      "capital_required": null,
      "capital_unit": null,
      "promised_outcome": "承诺的结果 ≤200字",
      "deadline": "YYYY-MM-DD 或 null",
      "status": "announced|in_progress|delivered|missed"
    }
  ],
  "pricing_actions": [
    {
      "product_or_segment": "产品/业务线",
      "price_change_pct": 0.05,
      "volume_impact_pct": null,
      "effective_date": "YYYY-MM-DD"
    }
  ]
}
```

## 提取规则
- management_guidance: 只提数字化的前瞻指引（有具体数字或范围的），不提模糊表态
- company_narratives: 管理层强调的战略方向，不是财务数据复述
- status: 根据上下文判断这个承诺是新宣布(announced)、进行中(in_progress)、已兑现(delivered)还是失败(missed)
""",

    "qa": _BASE + """
## 任务：从财报电话会 Q&A 环节提取问题回应和已知风险

输出 JSON:
```json
{
  "management_acknowledgments": [
    {
      "issue_description": "管理层回应的问题 ≤200字",
      "response_quality": "strong|adequate|defensive|evasive",
      "has_action_plan": true
    }
  ],
  "known_issues": [
    {
      "issue_description": "分析师提出的担忧/公司承认的问题 ≤200字",
      "issue_category": "financial|operational|legal|regulatory|reputational",
      "severity": "critical|major|minor",
      "source_type": "earnings_call"
    }
  ],
  "company_narratives": [
    {
      "narrative": "Q&A 中补充的战略承诺 ≤300字",
      "status": "announced|in_progress|delivered|missed"
    }
  ]
}
```

## 提取规则
- response_quality 判断标准:
  - strong: 正面回应 + 有具体改进计划 + 给了时间线
  - adequate: 承认问题 + 有一般性回应
  - defensive: 承认但辩解/推卸（"这是行业共性问题"）
  - evasive: 回避问题/转移话题
- has_action_plan: 管理层是否给出了具体的改善方案（不是空话）
- known_issues: 只提分析师追问的重大问题，不是每个小问题都记
""",
}

DEDUP_KEYS = {
    "management_guidance": "metric",
    "company_narratives": "narrative",
    "management_acknowledgments": "issue_description",
    "known_issues": "issue_description",
}


async def extract_earnings_call(
    content: str,
    metadata: dict | None = None,
) -> ExtractionResult:
    """从财报电话会提取指引、叙事和问题回应。"""
    metadata = metadata or {}

    chunks = chunk_earnings_call(content)
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
    logger.info(f"[EarningsCall] 提取完成: {sum(len(v) for v in tables.values())} 行")
    return result
