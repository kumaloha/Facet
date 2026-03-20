"""
10-K / 年报提取管线
====================
按章节拆分，每章 focused prompt，MapReduce 合并。

10-K 标准章节:
  Part I:  Item 1 (Business), Item 1A (Risk Factors), Item 1B (Unresolved Staff Comments)
  Part II: Item 5 (Market), Item 6 (Selected Financial), Item 7 (MD&A),
           Item 8 (Financial Statements), Item 9A (Controls)
  Part III: Item 10-14 (Directors, Compensation, Ownership, Relationships, Fees)
  Part IV:  Item 15 (Exhibits)

提取目标 (8 张表):
  - financial_line_items  ← Item 8
  - downstream_segments   ← Item 1
  - upstream_segments     ← Item 1
  - geographic_revenues   ← Item 1 / Item 8 notes
  - debt_obligations      ← Item 8 notes
  - litigations           ← Item 1A / Item 8 notes
  - audit_opinions        ← Item 8
  - operational_issues    ← Item 7 (MD&A)
"""

from __future__ import annotations

import re

from loguru import logger

from anchor.extract.pipelines._mapreduce import (
    ChunkMeta,
    ExtractionResult,
    map_reduce_extract,
    merge_table_results,
)

# ── 章节识别 ──────────────────────────────────────────────────────────

# 10-K 章节分隔标记（宽松匹配）
_SECTION_PATTERNS = [
    # "Item 1. Business" 或 "Items 1 and 2. Business and Properties"（油企常见写法）
    # 用 [\s\S]{0,50} 跨行匹配（有些 10-K 在 Item 号和标题之间插入换行/bullet）
    (r"(?i)\bitems?\s*1\s*(?:and\s*2)?[\s\S]{0,50}?business\b", "business"),
    (r"(?i)\bitem\s*1a[\s\S]{0,30}?risk\s*factors\b", "risk_factors"),
    (r"(?i)\bitem\s*7[\s\S]{0,30}?management[\s\S]{0,20}?discussion\b", "mda"),
    (r"(?i)\bitem\s*8[\s\S]{0,30}?financial\s*statements\b", "financials"),
    (r"(?i)\bitem\s*1[0-4]\b", "governance"),  # Part III 合并
]


def chunk_10k(content: str) -> list[ChunkMeta]:
    """按 10-K 章节拆分文档。找不到章节标记则按长度切。"""

    # 找到所有章节起始位置
    positions = []
    for pattern, section_name in _SECTION_PATTERNS:
        for m in re.finditer(pattern, content):
            positions.append((m.start(), section_name))

    # 过滤噪声匹配（TOC 条目、交叉引用）
    # 策略：对每种 section type，选"后续内容最长"的匹配（真正的 section header
    # 后面跟大段正文；TOC 条目后面紧接着下一个条目；交叉引用在段落中间）
    if len(positions) > 2:
        positions.sort(key=lambda x: x[0])

        # 先过滤明显的交叉引用
        non_xref = []
        for i, (start, name) in enumerate(positions):
            after = content[start:start + 200]
            # "of this report" / "of this form" = 交叉引用
            if re.search(r"of\s+this\s+(report|form)", after, re.IGNORECASE):
                continue
            # 行内引用：marker 前面同一行有 see/in/under 等引导词
            line_start = content.rfind("\n", max(0, start - 200), start)
            if line_start == -1:
                line_start = 0
            prefix = content[line_start:start].strip()
            if re.search(
                r"(?i)\b(see|under|per|described\s+in|set\s+forth\s+in|"
                r"refer\s+to|discussed\s+in|included\s+in)\s*$",
                prefix,
            ):
                continue
            non_xref.append((start, name))

        # 对每种 section type，选内容跨度最大的匹配
        if len(non_xref) >= 2:
            # 计算每个匹配到下一个匹配的距离（= 该 section 的内容长度）
            by_section: dict[str, list[tuple[int, int]]] = {}  # name → [(start, span)]
            for i, (start, name) in enumerate(non_xref):
                next_start = (non_xref[i + 1][0]
                              if i + 1 < len(non_xref) else len(content))
                span = next_start - start
                if name not in by_section:
                    by_section[name] = []
                by_section[name].append((start, span))

            # 选每种 type 中 span 最大的那个
            best = []
            for name, candidates in by_section.items():
                winner = max(candidates, key=lambda x: x[1])
                best.append((winner[0], name))
            best.sort(key=lambda x: x[0])

            if len(best) >= 2:
                positions = best

    if len(positions) < 2:
        # 无法识别章节，按长度切
        logger.warning("[10-K] 无法识别章节标记，按长度切分")
        max_chunk = 40000
        if len(content) <= max_chunk:
            return [ChunkMeta("full", content, "business")]
        chunks = []
        for i in range(0, len(content), max_chunk):
            chunks.append(ChunkMeta(f"part_{i // max_chunk + 1}",
                                     content[i:i + max_chunk], "business"))
        return chunks

    # 按位置排序
    positions.sort(key=lambda x: x[0])

    # 切分（超长段递归拆分，保证每段 ≤ max_chunk_chars）
    max_chunk_chars = 40000  # 清理装饰字符后 40K chars ≈ 10-15K token
    chunks = []
    for i, (start, name) in enumerate(positions):
        end = positions[i + 1][0] if i + 1 < len(positions) else len(content)
        text = content[start:end].strip()
        if len(text) <= 200:  # 太短的段跳过
            continue
        if len(text) <= max_chunk_chars:
            chunks.append(ChunkMeta(name, text, name))
        else:
            # 递归拆分成 ≤ max_chunk_chars 的段
            n_parts = (len(text) + max_chunk_chars - 1) // max_chunk_chars
            part_size = len(text) // n_parts
            for j in range(n_parts):
                s = j * part_size
                e = (j + 1) * part_size if j < n_parts - 1 else len(text)
                suffix = f"_{j+1}" if n_parts > 1 else ""
                chunks.append(ChunkMeta(f"{name}{suffix}", text[s:e], name))

    if not chunks:
        chunks = [ChunkMeta("full", content[:80000], "business")]

    logger.info(f"[10-K] 拆分为 {len(chunks)} 段: {[c.section_name for c in chunks]}")
    return chunks


# ── Section Prompts ──────────────────────────────────────────────────

_BASE = "你是资深基本面分析师。只输出 JSON，无数据返回空数组 []。金额单位百万美元，比率用小数。\n"

SECTION_PROMPTS = {
    "business": _BASE + """
## 任务：从 Item 1 (Business) 提取业务结构

输出 JSON:
```json
{
  "downstream_segments": [
    {
      "customer_name": "业务线或客户名称",
      "revenue_pct": 0.40,
      "product_category": "beverage|commodity|cloud_infrastructure|insurance|banking|payment|healthcare|pharma|gaming|social_media|consumer_electronics|industrial_equipment|food|grocery|liquor|tobacco|operating_system|pipeline|utility",
      "revenue_type": "product_sale|subscription|license|transaction_fee|ad_revenue|recurring|saas",
      "is_recurring": true,
      "switching_cost_level": "high|medium|low|null",
      "contract_duration": "one-time|1-year|multi-year|5-year|null",
      "product_criticality": "high|medium|low|null",
      "segment_gross_margin": 0.25,
      "description": "简要说明"
    }
  ],
  "upstream_segments": [
    {
      "supplier_name": "供应商名",
      "supply_type": "foundry|memory|assembly|component|raw_material|logistics|software",
      "geographic_location": "所在地",
      "is_sole_source": false,
      "concentration_risk": "描述集中风险",
      "description": "说明"
    }
  ],
  "geographic_revenues": [
    {"region": "地域名", "revenue_share": 0.45}
  ]
}
```

## 提取规则
- customer_name: 优先用业务线名称（如 "Cloud Services"），不是具体公司名
- 如果文档没有明确的 segment reporting，从业务描述中推断主要业务线。宁可粗略也不要返回空。常见推断策略：
  - 综合油企：按价值链拆（Upstream / Downstream / Chemicals / Midstream）
  - 纯上游 E&P：按地理区域拆（如 "U.S. Operations"、"Egypt Operations"、"North Sea"）
  - 多元化集团：按业务类型拆
- product_category: 必须从给定列表中选择，这是生意画像的关键输入
- segment_gross_margin: 如果文档提到该业务线的毛利率则填，否则 null
- revenue_pct: 估算各业务线占总收入百分比，用小数（0.40 = 40%）。没有明确数据时可填 null，但必须输出业务线
""",

    "risk_factors": _BASE + """
## 任务：从 Item 1A (Risk Factors) 提取竞争和风险信息

输出 JSON:
```json
{
  "competitive_dynamics": [
    {
      "competitor_name": "竞争对手名",
      "event_type": "price_war|new_entry|exit|product_launch|patent_challenge|patent_expiration|regulatory_change|industry_downturn",
      "event_description": "事件描述 ≤200字",
      "outcome_description": "结果/影响 ≤200字",
      "outcome_market_share_change": null
    }
  ],
  "known_issues": [
    {
      "issue_description": "问题描述",
      "issue_category": "financial|operational|legal|regulatory|reputational",
      "severity": "critical|major|minor",
      "source_type": "annual_report"
    }
  ],
  "litigations": [
    {
      "case_name": "案件名",
      "case_type": "lawsuit|regulatory|patent|antitrust",
      "status": "pending|settled|dismissed|ongoing",
      "counterparty": "对方",
      "claimed_amount": null,
      "description": "案情摘要"
    }
  ]
}
```

## 提取规则
- competitive_dynamics: 从风险因素中提取明确提到的竞争威胁和行业事件
- event_type: 严格从给定列表中选
- known_issues: 公司自己披露的重大风险，severity 根据公司描述的严重程度判断
""",

    "mda": _BASE + """
## 任务：从 Item 7 (MD&A) 提取经营议题

输出 JSON:
```json
{
  "operational_issues": [
    {
      "topic": "议题名 ≤30字",
      "performance": "本期表现 ≤200字（不含财务数字）",
      "attribution": "归因分析 ≤200字",
      "risk": "风险展望 ≤200字",
      "guidance": "管理层指引 ≤200字"
    }
  ],
  "management_guidance": [
    {
      "metric": "revenue_growth|operating_margin|eps|capex|roic_target|free_cash_flow|gross_margin|net_margin|tax_rate",
      "value_low": null,
      "value_high": null,
      "unit": "pct|absolute|per_share",
      "confidence_language": "expect|target|aspire",
      "verbatim": "原文引用 ≤100字"
    }
  ]
}
```
""",

    "financials": _BASE + """
## 任务：从 Item 8 (Financial Statements) 提取三表数据和审计意见

输出 JSON:
```json
{
  "financial_line_items": [
    {
      "statement_type": "income|balance_sheet|cashflow",
      "item_key": "revenue|cost_of_revenue|operating_income|net_income|operating_cash_flow|capital_expenditures|depreciation_amortization|shareholders_equity|total_assets|interest_expense|goodwill|accounts_receivable|inventory|cash_and_equivalents|total_debt|current_assets|current_liabilities|dividends_paid|share_repurchase|sga_expense|rnd_expense|basic_weighted_average_shares|income_tax_expense_total|income_before_tax_total|proceeds_from_stock_issuance|proceeds_from_debt_issuance",
      "item_label": "原始标签",
      "value": 12345.0
    }
  ],
  "debt_obligations": [
    {
      "instrument_name": "债务名称",
      "debt_type": "bond|loan|lease|convertible|credit_facility",
      "principal": null,
      "interest_rate": null,
      "maturity_date": "YYYY-MM-DD"
    }
  ],
  "audit_opinion": {
    "opinion_type": "unqualified|qualified|adverse|disclaimer",
    "auditor_name": "事务所名",
    "emphasis_matters": "强调事项，无则 null"
  }
}
```

## 提取规则
- item_key 必须从给定列表中选择（标准化键）
- 金额统一百万美元（或百万人民币，按原文）
- dividends_paid 和 share_repurchase 通常为负数
- 只提最新一期完整年度数据
""",

    "governance": _BASE + """
## 任务：从 Part III 提取治理信息

输出 JSON:
```json
{
  "executive_compensations": [
    {
      "name": "姓名",
      "title": "职位",
      "role_type": "CEO|CFO|executive|director",
      "base_salary": null,
      "stock_awards": null,
      "total_comp": null,
      "pay_ratio": null
    }
  ],
  "stock_ownership": [
    {"name": "持有人", "title": "职位", "percent_of_class": 0.05}
  ],
  "related_party_transactions": [
    {
      "related_party": "关联方",
      "relationship": "director|officer|major_shareholder|subsidiary",
      "amount": null,
      "description": "交易说明"
    }
  ],
  "executive_changes": [
    {
      "person_name": "姓名",
      "title": "职位",
      "change_type": "joined|departed",
      "change_date": "YYYY-MM-DD"
    }
  ]
}
```
""",
}

# 去重规则
DEDUP_KEYS = {
    "downstream_segments": "customer_name",
    "upstream_segments": "supplier_name",
    "geographic_revenues": "region",
    "competitive_dynamics": "competitor_name",
    "known_issues": "issue_description",
    "litigations": "case_name",
    "operational_issues": "topic",
    "financial_line_items": "item_key",
    "debt_obligations": "instrument_name",
    "executive_compensations": "name",
    "stock_ownership": "name",
    "related_party_transactions": "related_party",
    "executive_changes": "person_name",
    "management_guidance": "metric",
}


# ── 主入口 ────────────────────────────────────────────────────────────

async def extract_annual_report(
    content: str,
    metadata: dict | None = None,
) -> ExtractionResult:
    """从 10-K / 年报中提取结构化数据。

    Args:
        content: 10-K 全文
        metadata: 可选元数据（ticker, period 等）

    Returns:
        ExtractionResult
    """
    metadata = metadata or {}

    # 1. 拆段
    chunks = chunk_10k(content)

    # 2. MapReduce
    tables = await map_reduce_extract(
        chunks=chunks,
        section_prompts=SECTION_PROMPTS,
        dedup_keys=DEDUP_KEYS,
        max_tokens=12000,
    )

    # 3. 包装结果
    result = ExtractionResult(
        company_ticker=metadata.get("ticker", ""),
        company_name=metadata.get("company_name", ""),
        period=metadata.get("period", ""),
        tables=tables,
        metadata=metadata,
    )

    logger.info(f"[10-K] 提取完成: {metadata.get('ticker', '?')} "
                f"| {sum(len(v) for v in tables.values())} 行 "
                f"| {list(tables.keys())}")
    return result
