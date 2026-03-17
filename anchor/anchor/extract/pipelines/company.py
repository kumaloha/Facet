"""
pipelines/company.py — Company 专用提取管线
============================================
直接提取 13 张 company 表数据，绕过 Node/Edge 架构。

架构：
  extract_company_compute(content, platform, author, today) → CompanyComputeResult
  extract_company_write(raw_post, session, compute_result) → dict
  extract_company() — 串行包装
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date as _date

from loguru import logger
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from anchor.extract.pipelines._base import call_llm, parse_json, safe_float, safe_str
from anchor.extract.schemas.company import CompanyExtractionResult
from anchor.models import (
    ASPTrend,
    AuditOpinion,
    CompanyNarrative,
    CompanyProfile,
    CompetitorRelation,
    DebtObligation,
    DeferredRevenue,
    DownstreamSegment,
    ExecutiveChange,
    ExecutiveCompensation,
    FinancialLineItem,
    FinancialStatement,
    GeographicRevenue,
    InventoryProvision,
    KnownIssue,
    Litigation,
    ManagementAcknowledgment,
    ManagementGuidance,
    MarketShareData,
    NonFinancialKPI,
    OperationalIssue,
    PricingAction,
    PurchaseObligationSummary,
    RawPost,
    RecurringRevenueBreakdown,
    RelatedPartyTransaction,
    RevenueRecognitionPolicy,
    StockOwnership,
    UpstreamSegment,
    _utcnow,
)

_MAX_TOKENS = 16384

# ── LLM 提示词 ──────────────────────────────────────────────────────────

SYSTEM_COMPANY = """\
你是一位资深基本面分析师。从公司财报/年报/Proxy Statement 中提取**非财务结构化信息**。

★ 重要：财务三表数据（利润表/资产负债表/现金流量表）由其他数据源提供，你**不需要提取**。
★ 你的任务是提取三表以外的所有定性和半定量信息。

## 输出格式
输出一个 JSON 对象。如果原文中没有某类信息，对应字段返回空数组 [] 或 null。

```json
{
  "is_relevant_content": true,
  "skip_reason": null,
  "company": {
    "name": "公司全名",
    "ticker": "股票代码（如 NVDA / 600519.SH）",
    "market": "us|cn_a|cn_h|hk|jp",
    "industry": "所属行业",
    "summary": "一句话商业模式"
  },
  "period": "FY2025 或 2025Q4",
  "operational_issues": [
    {
      "topic": "议题名 ≤30字",
      "performance": "表现（定性描述，不含财务数字）≤200字",
      "attribution": "归因 ≤200字",
      "risk": "风险 ≤200字",
      "guidance": "指引 ≤200字"
    }
  ],
  "narratives": [
    {
      "narrative": "管理层讲的故事/战略承诺 ≤300字",
      "capital_required": null,
      "capital_unit": null,
      "promised_outcome": "承诺的结果 ≤200字",
      "deadline": null
    }
  ],
  "downstream_segments": [
    {
      "segment": "业务分部名称（无则 null）",
      "customer_name": "客户名或收入流名",
      "customer_type": "direct|indirect|channel|OEM|distributor",
      "products": "产品/服务",
      "channels": "销售渠道",
      "revenue": null,
      "revenue_pct": null,
      "growth_yoy": "同比增速",
      "backlog": null,
      "backlog_note": null,
      "pricing_model": "per-unit|per-user/month|usage-based|混合",
      "contract_duration": "one-time|1-year|multi-year",
      "revenue_type": "product_sale|subscription|license|royalty|service|NRE|cloud_service",
      "is_recurring": null,
      "recognition_method": "point_in_time|over_time",
      "contract_duration_months": null,
      "switching_cost_level": "high|medium|low",
      "description": "补充说明"
    }
  ],
  "upstream_segments": [
    {
      "segment": "业务分部名称（无则 null）",
      "supplier_name": "供应商名称",
      "supply_type": "foundry|memory|assembly_test|substrate|component|contract_mfg|software|logistics",
      "material_or_service": "供应内容",
      "process_node": "制程节点（如适用）",
      "geographic_location": "所在地",
      "is_sole_source": false,
      "purchase_obligation": null,
      "lead_time": null,
      "contract_type": null,
      "prepaid_amount": null,
      "concentration_risk": "集中度风险",
      "description": "补充说明"
    }
  ],
  "geographic_revenues": [
    {
      "region": "地域名称",
      "revenue": null,
      "revenue_share": null,
      "growth_yoy": "增速",
      "note": null
    }
  ],
  "non_financial_kpis": [
    {
      "kpi_name": "指标名称",
      "kpi_value": "值",
      "kpi_unit": "单位",
      "yoy_change": "变化",
      "category": "workforce|customer|product|esg|operational",
      "note": null
    }
  ],
  "debt_obligations": [
    {
      "instrument_name": "债务工具名称",
      "debt_type": "bond|loan|lease|convertible|credit_facility",
      "principal": null,
      "currency": "USD",
      "interest_rate": null,
      "maturity_date": null,
      "is_secured": false,
      "is_current": false,
      "is_floating_rate": false,
      "note": null
    }
  ],
  "litigations": [
    {
      "case_name": "案件名称",
      "case_type": "lawsuit|regulatory|patent|antitrust|environmental|tax|other",
      "status": "pending|settled|dismissed|ongoing|appealed",
      "counterparty": null,
      "filed_at": null,
      "claimed_amount": null,
      "accrued_amount": null,
      "currency": "USD",
      "description": "案情摘要"
    }
  ],
  "executive_compensations": [
    {
      "name": "姓名",
      "title": "职位",
      "role_type": "executive|director",
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
      "percent_of_class": null
    }
  ],
  "related_party_transactions": [
    {
      "related_party": "关联方名称",
      "relationship": "director|officer|major_shareholder|subsidiary|affiliate|family",
      "transaction_type": "sale|purchase|lease|loan|guarantee|service|license|other",
      "amount": null,
      "currency": "USD",
      "terms": "交易条件",
      "is_ongoing": false,
      "description": "交易说明"
    }
  ],
  "pricing_actions": [
    {
      "product_or_segment": "涉及的产品/业务线",
      "price_change_pct": null,
      "volume_impact_pct": null,
      "effective_date": "YYYY-MM-DD"
    }
  ],
  "competitor_relations": [
    {
      "competitor_name": "竞对公司名",
      "market_segment": "竞争所在细分市场",
      "relationship_type": "direct_competitor|indirect_competitor|potential_entrant"
    }
  ],
  "market_share_data": [
    {
      "company_or_competitor": "公司或竞对名",
      "market_segment": "细分市场",
      "share_pct": null,
      "source_description": "数据来源（如 IDC Q3 2025）"
    }
  ],
  "known_issues": [
    {
      "issue_description": "已知问题描述",
      "issue_category": "financial|operational|legal|reputational|regulatory",
      "severity": "critical|major|minor",
      "source_type": "analyst_report|news|litigation|financial_anomaly"
    }
  ],
  "management_acknowledgments": [
    {
      "issue_description": "管理层提及的问题",
      "response_quality": "forthright|downplay|deflect|deny",
      "has_action_plan": false
    }
  ],
  "executive_changes": [
    {
      "person_name": "姓名",
      "title": "职位",
      "change_type": "joined|departed|promoted|demoted",
      "change_date": "YYYY-MM-DD",
      "reason": "原因（可空）"
    }
  ],
  "audit_opinion": {
    "opinion_type": "unqualified|qualified|adverse|disclaimer",
    "auditor_name": "审计师事务所",
    "emphasis_matters": "强调事项（可空）"
  },
  "management_guidance": [
    {
      "target_period": "FY2026",
      "metric": "revenue_growth|operating_margin|eps|capex|roic_target|free_cash_flow|gross_margin|net_margin|tax_rate|share_repurchase|dividend|other",
      "value_low": null,
      "value_high": null,
      "unit": "pct|absolute|per_share",
      "confidence_language": "expect|target|aspire|preliminary",
      "verbatim": "原文引用"
    }
  ],
  "inventory_provisions": [
    {
      "provision_amount": null,
      "provision_release": null,
      "net_margin_impact_pct": null,
      "note": null
    }
  ],
  "deferred_revenues": [
    {
      "total_deferred": null,
      "short_term": null,
      "long_term": null,
      "recognized_in_period": null,
      "note": null
    }
  ],
  "revenue_recognition_policies": [
    {
      "category": "product|software_license|subscription|service|NRE",
      "policy": "确认方式（时点/时段、交付条件）≤200字",
      "key_judgments": "关键判断（SSP估计、合约组合、可变对价）≤150字"
    }
  ],
  "purchase_obligation_summaries": [
    {
      "total_outstanding": null,
      "inventory_purchase_obligations": null,
      "non_inventory_obligations": null,
      "cloud_service_agreements": null,
      "breakdown_by_year": [{"year": "FY20XX", "amount": null}],
      "note": null
    }
  ],
  "asp_trends": [
    {
      "product_category": "产品类别",
      "trend": "ASP变化趋势 ≤120字",
      "driver": "驱动因素 ≤120字",
      "note": null
    }
  ],
  "recurring_revenue_breakdowns": [
    {
      "recurring_revenue": null,
      "recurring_pct": null,
      "nonrecurring_revenue": null,
      "nonrecurring_pct": null,
      "note": "如原文无明确拆分，说明估算依据"
    }
  ],
  "summary": "≤200字叙事摘要",
  "one_liner": "≤50字一句话总结"
}
```

## 经营议题表说明（最重要）
operational_issues 提取自 CEO致股东信、MD&A 等定性讨论段落。
- 每行 = 一个经营议题（如"数据中心需求"、"供应链管理"、"中国市场出口管制"）
- performance: 管理层对该议题的定性描述（不要放财务数字，财务数字在三表里）
- attribution: 为什么出现这个表现
- risk: 该议题面临什么风险
- guidance: 管理层对未来的展望/指引
- 四个字段都是 Optional，没提到就留 null

## 新增提取类型说明
- inventory_provisions: 库存减值/拨备，从 inventory note 或 COGS 明细提取
- deferred_revenues: 递延收入余额，从资产负债表负债科目和收入确认附注提取
- revenue_recognition_policies: 每类收入的确认政策，从 Note 1 Revenue Recognition 提取
- purchase_obligation_summaries: 采购义务汇总，从 Commitments and Contingencies 附注提取
- asp_trends: 产品级 ASP 变化趋势，从 MD&A 定价讨论提取
- recurring_revenue_breakdowns: 经常性 vs 非经常性收入拆分，从分部披露估算

## 规则
1. 每个独立事实单独成条目
2. 数字保留原始值和单位，金额统一百万美元
3. revenue_pct/revenue_share 为 0-1 比例
4. 无数据则返回空数组
5. 只输出 JSON
6. 如果文章与公司财报/年报无关，设 is_relevant_content=false 并说明原因
★ 无论原文是什么语言，summary 和 one_liner 必须使用中文。\
"""


def _build_user_message(content: str, platform: str, author: str, today: str) -> str:
    return f"""\
## 文章信息
平台：{platform}
作者：{author}
日期：{today}

## 文章内容

{content[:50000]}{"..." if len(content) > 50000 else ""}

## 提取任务

请从上述文章中提取公司财报/年报的全部结构化信息。\
"""


# ── Helper ───────────────────────────────────────────────────────────────


def _parse_date(s: str | None) -> _date | None:
    if not s or s == "null":
        return None
    s = str(s).strip()
    try:
        if len(s) == 4:
            return _date(int(s), 1, 1)
        return _date.fromisoformat(s[:10])
    except (ValueError, TypeError):
        return None


async def get_or_create_company(
    session: AsyncSession,
    company_data: dict | None,
) -> CompanyProfile | None:
    """根据 LLM 输出的公司信息，获取或创建 CompanyProfile。"""
    if not company_data:
        return None

    ticker = (company_data.get("ticker") or "").strip()
    if not ticker:
        return None

    result = await session.exec(
        select(CompanyProfile).where(CompanyProfile.ticker == ticker)
    )
    existing = result.first()
    if existing:
        return existing

    company = CompanyProfile(
        name=company_data.get("name", ticker),
        ticker=ticker,
        market=company_data.get("market", "us"),
        industry=company_data.get("industry"),
        summary=company_data.get("summary"),
    )
    session.add(company)
    await session.flush()
    return company


# ── Compute 阶段（纯 LLM，无 DB）────────────────────────────────────────


@dataclass
class CompanyComputeResult:
    """Company 域 LLM 提取中间结果。"""
    is_relevant: bool = False
    skip_reason: str | None = None
    data: CompanyExtractionResult | None = None


def _dedup_by_key(existing: list, new_items: list, key_fn) -> list:
    """按 key 函数去重，返回 new_items 中不重复的条目。"""
    seen = {key_fn(item) for item in existing}
    return [item for item in new_items if key_fn(item) not in seen]


def _merge_extraction_results(
    base: CompanyExtractionResult,
    other: CompanyExtractionResult,
) -> CompanyExtractionResult:
    """将 other 的提取结果合并到 base 中（去重后追加）。"""
    # 按主键去重的表
    base.operational_issues.extend(
        _dedup_by_key(base.operational_issues, other.operational_issues, lambda x: x.topic))
    base.narratives.extend(
        _dedup_by_key(base.narratives, other.narratives, lambda x: x.narrative[:80]))
    base.downstream_segments.extend(
        _dedup_by_key(base.downstream_segments, other.downstream_segments, lambda x: x.customer_name))
    base.upstream_segments.extend(
        _dedup_by_key(base.upstream_segments, other.upstream_segments, lambda x: x.supplier_name))
    base.geographic_revenues.extend(
        _dedup_by_key(base.geographic_revenues, other.geographic_revenues, lambda x: x.region))
    base.non_financial_kpis.extend(
        _dedup_by_key(base.non_financial_kpis, other.non_financial_kpis, lambda x: x.kpi_name))
    base.debt_obligations.extend(
        _dedup_by_key(base.debt_obligations, other.debt_obligations, lambda x: x.instrument_name))
    base.litigations.extend(
        _dedup_by_key(base.litigations, other.litigations, lambda x: x.case_name))
    base.executive_compensations.extend(
        _dedup_by_key(base.executive_compensations, other.executive_compensations, lambda x: x.name))
    base.stock_ownership.extend(
        _dedup_by_key(base.stock_ownership, other.stock_ownership, lambda x: x.name))
    base.related_party_transactions.extend(
        _dedup_by_key(base.related_party_transactions, other.related_party_transactions, lambda x: x.related_party))
    base.competitor_relations.extend(
        _dedup_by_key(base.competitor_relations, other.competitor_relations, lambda x: x.competitor_name))
    base.market_share_data.extend(
        _dedup_by_key(base.market_share_data, other.market_share_data,
                      lambda x: f"{x.company_or_competitor}_{x.market_segment}"))
    base.executive_changes.extend(
        _dedup_by_key(base.executive_changes, other.executive_changes, lambda x: x.person_name))
    base.management_guidance.extend(
        _dedup_by_key(base.management_guidance, other.management_guidance, lambda x: x.metric))

    # 无天然主键的表，直接追加
    base.pricing_actions.extend(other.pricing_actions)
    base.known_issues.extend(other.known_issues)
    base.management_acknowledgments.extend(other.management_acknowledgments)
    base.inventory_provisions.extend(other.inventory_provisions)
    base.deferred_revenues.extend(other.deferred_revenues)
    base.revenue_recognition_policies.extend(other.revenue_recognition_policies)
    base.purchase_obligation_summaries.extend(other.purchase_obligation_summaries)
    base.asp_trends.extend(other.asp_trends)
    base.recurring_revenue_breakdowns.extend(other.recurring_revenue_breakdowns)

    if other.audit_opinion and not base.audit_opinion:
        base.audit_opinion = other.audit_opinion

    # 保留更长的摘要
    if other.summary and (not base.summary or len(other.summary) > len(base.summary)):
        base.summary = other.summary
    if other.one_liner and not base.one_liner:
        base.one_liner = other.one_liner
    return base


# 长文档阈值（超过此长度尝试分段提取）
_CHUNK_THRESHOLD = 100_000

# Topic → 主要产出表的映射（用于增量提取：跳过已有数据的 topic）
TOPIC_TO_TABLES: dict[str, list[str]] = {
    "business": [
        "operational_issues", "company_narratives",
        "downstream_segments", "upstream_segments",
        "competitor_relations", "market_share_data",
        "litigations", "non_financial_kpis",
        "management_guidance", "known_issues",
    ],
    "governance": [
        "debt_obligations", "geographic_revenues",
        "executive_compensations", "stock_ownership",
        "related_party_transactions",
        "revenue_recognition_policies", "deferred_revenues",
        "purchase_obligation_summaries",
    ],
}


async def _get_populated_tables(session, company_id: int) -> set[str]:
    """查询该公司已有数据的表名集合。"""
    from sqlalchemy import text

    populated = set()
    check_tables = [
        "operational_issues", "company_narratives", "downstream_segments",
        "upstream_segments", "geographic_revenues", "non_financial_kpis",
        "debt_obligations", "litigations", "executive_compensations",
        "stock_ownership", "related_party_transactions",
        "financial_statements", "financial_line_items",
        "inventory_provisions", "deferred_revenues",
        "revenue_recognition_policies", "purchase_obligation_summaries",
        "asp_trends", "recurring_revenue_breakdowns",
    ]
    for tbl in check_tables:
        try:
            row = (await session.execute(
                text(f"SELECT COUNT(*) FROM {tbl} WHERE company_id = :cid"),
                {"cid": company_id},
            )).scalar()
            if row and row > 0:
                populated.add(tbl)
        except Exception:
            pass
    # financial_line_items 通过 financial_statements 间接关联
    if "financial_statements" in populated:
        populated.add("financial_line_items")
    return populated


def _topics_to_skip(populated_tables: set[str]) -> set[str]:
    """根据已有数据的表，判断哪些 topic 可以跳过。

    只有当某 topic 对应的所有表都已有数据时才跳过该 topic。
    """
    skip = set()
    for topic, tables in TOPIC_TO_TABLES.items():
        if all(t in populated_tables for t in tables):
            skip.add(topic)
    return skip


async def _extract_single_chunk(
    content: str,
    platform: str,
    author: str,
    today: str,
    chunk_label: str = "",
) -> CompanyExtractionResult | None:
    """对单个文本块调用 LLM 提取，返回解析结果或 None。"""
    user_msg = _build_user_message(content, platform, author, today)
    raw = await call_llm(SYSTEM_COMPANY, user_msg, _MAX_TOKENS)
    if raw is None:
        logger.warning(f"[Company] LLM returned None {chunk_label}")
        return None

    parsed = parse_json(raw, CompanyExtractionResult, f"company_extract{chunk_label}")
    if parsed is None:
        logger.warning(f"[Company] Parse failed {chunk_label}")
        return None

    if not parsed.is_relevant_content:
        logger.debug(f"[Company] Not relevant {chunk_label}: {parsed.skip_reason}")
        return None

    return parsed


async def extract_company_compute(
    content: str,
    platform: str,
    author: str,
    today: str,
    skip_topics: set[str] | None = None,
) -> CompanyComputeResult:
    """纯 LLM 计算阶段：提取 company 域全量结构化数据。

    长文档（>50K 字符）自动按 10-K Item/Note 分段，
    每个主题独立调用 LLM，最后合并结果。

    Args:
        skip_topics: 跳过的 topic 集合（增量提取时使用）
    """
    result = CompanyComputeResult()

    if len(content) <= _CHUNK_THRESHOLD:
        # 短文档：单次提取
        parsed = await _extract_single_chunk(content, platform, author, today)
        if parsed is None:
            result.skip_reason = "not company content"
            return result
        result.is_relevant = True
        result.data = parsed
    else:
        # 长文档：尝试 10-K 分段
        from anchor.extract.sec_10k_splitter import split_10k, get_sections_for_topic, TOPIC_SECTIONS

        sections = split_10k(content)
        if len(sections) >= 3:
            # 成功识别 10-K 结构，按主题分段提取
            logger.info(
                f"[Company] 10-K detected: {len(sections)} sections, "
                f"splitting into {len(TOPIC_SECTIONS)} topic chunks"
            )

            async def _extract_topic(topic: str) -> tuple[str, CompanyExtractionResult | None]:
                topic_text = get_sections_for_topic(topic, sections)
                if not topic_text.strip():
                    logger.debug(f"[Company] Topic '{topic}' has no matching sections, skip")
                    return topic, None
                # 截断过长的主题文本，保留前 50K 字符
                if len(topic_text) > _CHUNK_THRESHOLD:
                    logger.info(
                        f"[Company] Topic '{topic}' too long ({len(topic_text):,} chars), "
                        f"truncating to {_CHUNK_THRESHOLD:,}"
                    )
                    topic_text = topic_text[:_CHUNK_THRESHOLD]
                logger.info(f"[Company] Extracting topic '{topic}' ({len(topic_text):,} chars)")
                parsed = await _extract_single_chunk(
                    topic_text, platform, author, today,
                    chunk_label=f" [{topic}]",
                )
                return topic, parsed

            # 2 路并发提取（M 系列 Mac + 12B 模型可撑住）
            import asyncio as _aio
            _skip = skip_topics or set()
            topics_to_run = [t for t in TOPIC_SECTIONS if t not in _skip]
            for t in _skip:
                logger.info(f"[Company] Skipping topic '{t}' (tables already populated)")

            # 按 2 个一组并发
            _CONCURRENCY = 2
            merged: CompanyExtractionResult | None = None
            for batch_start in range(0, len(topics_to_run), _CONCURRENCY):
                batch = topics_to_run[batch_start:batch_start + _CONCURRENCY]
                batch_results = await _aio.gather(*[_extract_topic(t) for t in batch])
                for topic, parsed in batch_results:
                    if parsed is None:
                        continue
                    if merged is None:
                        merged = parsed
                    else:
                        _merge_extraction_results(merged, parsed)
                    logger.info(
                        f"[Company] Topic '{topic}' extracted: "
                        f"issues={len(parsed.operational_issues)} "
                        f"downstream={len(parsed.downstream_segments)} "
                        f"debt={len(parsed.debt_obligations)} "
                        f"exec_comp={len(parsed.executive_compensations)}"
                    )

            if merged is None:
                result.skip_reason = "no relevant content in any section"
                return result

            result.is_relevant = True
            result.data = merged
        else:
            # 无法识别 10-K 结构，按固定大小分块
            logger.info(f"[Company] No 10-K structure detected, chunking by size")
            chunk_size = _CHUNK_THRESHOLD
            chunks = [content[i:i + chunk_size] for i in range(0, len(content), chunk_size)]
            logger.info(f"[Company] Split into {len(chunks)} chunks")

            import asyncio as _aio
            merged = None
            _CONCURRENCY = 2
            for batch_start in range(0, len(chunks), _CONCURRENCY):
                batch = chunks[batch_start:batch_start + _CONCURRENCY]
                batch_tasks = [
                    _extract_single_chunk(
                        c, platform, author, today,
                        chunk_label=f" [chunk {batch_start+j+1}/{len(chunks)}]",
                    )
                    for j, c in enumerate(batch)
                ]
                batch_results = await _aio.gather(*batch_tasks)
                for parsed in batch_results:
                    if parsed is None:
                        continue
                    if merged is None:
                        merged = parsed
                    else:
                        _merge_extraction_results(merged, parsed)

            if merged is None:
                result.skip_reason = "no relevant content in any chunk"
                return result

            result.is_relevant = True
            result.data = merged

    data = result.data
    logger.info(
        f"[Company] Compute done: "
        f"issues={len(data.operational_issues)} "
        f"narratives={len(data.narratives)} "
        f"downstream={len(data.downstream_segments)} "
        f"upstream={len(data.upstream_segments)} "
        f"debt={len(data.debt_obligations)} "
        f"exec_comp={len(data.executive_compensations)} "
        f"litigation={len(data.litigations)} "
        f"fin_items={len(data.financial_line_items)} "
        f"inv_prov={len(data.inventory_provisions)} "
        f"deferred_rev={len(data.deferred_revenues)}"
    )
    return result


# ── Write 阶段（纯 DB，无 LLM）─────────────────────────────────────────


async def extract_company_write(
    raw_post: RawPost,
    session: AsyncSession,
    compute_result: CompanyComputeResult,
) -> dict:
    """DB 写入阶段：将 compute 结果写入 13 张 company 表。"""
    counts: dict[str, int] = {}

    if not compute_result.is_relevant or compute_result.data is None:
        raw_post.is_processed = True
        raw_post.processed_at = _utcnow()
        session.add(raw_post)
        await session.flush()
        return {
            "is_relevant_content": False,
            "skip_reason": compute_result.skip_reason or "not company content",
            "table_counts": {},
            "summary": None,
        }

    data = compute_result.data

    # ── 获取或创建公司 ─────────────────────────────────────────────────
    company_dict = data.company.model_dump() if data.company else None
    company = await get_or_create_company(session, company_dict)
    if not company:
        logger.warning("[Company] Cannot identify company, skipping DB write")
        raw_post.is_processed = True
        raw_post.processed_at = _utcnow()
        session.add(raw_post)
        await session.flush()
        return {
            "is_relevant_content": False,
            "skip_reason": "cannot identify company (no ticker)",
            "table_counts": {},
            "summary": data.summary,
        }

    company_id = company.id
    period = data.period or ""

    # ── Financial Statements ─────────────────────────────────────────────
    fin_item_count = 0
    if data.financial_statements:
        fs = data.financial_statements
        currency = fs.currency or "USD"
        period_type = "quarterly" if "Q" in period else "annual"

        for stmt_type, items in [
            ("income", fs.income),
            ("balance_sheet", fs.balance_sheet),
            ("cashflow", fs.cashflow),
        ]:
            if not items:
                continue
            stmt = FinancialStatement(
                company_id=company_id,
                period=period,
                period_type=period_type,
                statement_type=stmt_type,
                currency=currency,
                reported_at=raw_post.posted_at.date() if raw_post.posted_at else None,
                raw_post_id=raw_post.id,
            )
            session.add(stmt)
            await session.flush()

            for ordinal, item in enumerate(items, 1):
                val = safe_float(item.value)
                if val is None:
                    continue
                session.add(FinancialLineItem(
                    statement_id=stmt.id,
                    item_key=item.item_key,
                    item_label=item.item_label,
                    value=val,
                    ordinal=ordinal,
                    note=item.note,
                ))
                fin_item_count += 1
    counts["financial_line_items"] = fin_item_count

    # ── Operational Issues ──────────────────────────────────────────────
    for item in data.operational_issues:
        session.add(OperationalIssue(
            company_id=company_id,
            period=period,
            raw_post_id=raw_post.id,
            topic=item.topic,
            performance=item.performance,
            attribution=item.attribution,
            risk=item.risk,
            guidance=item.guidance,
        ))
    counts["operational_issues"] = len(data.operational_issues)

    # ── Narratives ──────────────────────────────────────────────────────
    for item in data.narratives:
        session.add(CompanyNarrative(
            company_id=company_id,
            raw_post_id=raw_post.id,
            narrative=item.narrative,
            capital_required=safe_float(item.capital_required),
            capital_unit=item.capital_unit,
            promised_outcome=item.promised_outcome,
            deadline=_parse_date(item.deadline),
            reported_at=raw_post.posted_at.date() if raw_post.posted_at else None,
        ))
    counts["narratives"] = len(data.narratives)

    # ── Downstream Segments ─────────────────────────────────────────────
    for item in data.downstream_segments:
        session.add(DownstreamSegment(
            company_id=company_id,
            period=period,
            raw_post_id=raw_post.id,
            segment=item.segment,
            customer_name=item.customer_name,
            customer_type=item.customer_type,
            products=item.products,
            channels=item.channels,
            revenue=safe_float(item.revenue),
            revenue_pct=safe_float(item.revenue_pct),
            growth_yoy=safe_str(item.growth_yoy),
            backlog=safe_float(item.backlog),
            backlog_note=item.backlog_note,
            pricing_model=item.pricing_model,
            contract_duration=item.contract_duration,
            revenue_type=item.revenue_type,
            is_recurring=item.is_recurring,
            recognition_method=item.recognition_method,
            contract_duration_months=item.contract_duration_months,
            switching_cost_level=item.switching_cost_level,
            description=item.description,
        ))
    counts["downstream_segments"] = len(data.downstream_segments)

    # ── Upstream Segments ───────────────────────────────────────────────
    for item in data.upstream_segments:
        session.add(UpstreamSegment(
            company_id=company_id,
            period=period,
            raw_post_id=raw_post.id,
            segment=item.segment,
            supplier_name=item.supplier_name,
            supply_type=item.supply_type,
            material_or_service=item.material_or_service,
            process_node=item.process_node,
            geographic_location=item.geographic_location,
            is_sole_source=item.is_sole_source,
            purchase_obligation=safe_float(item.purchase_obligation),
            lead_time=item.lead_time,
            contract_type=item.contract_type,
            prepaid_amount=safe_float(item.prepaid_amount),
            concentration_risk=item.concentration_risk,
            description=item.description,
        ))
    counts["upstream_segments"] = len(data.upstream_segments)

    # ── Geographic Revenues ─────────────────────────────────────────────
    for item in data.geographic_revenues:
        session.add(GeographicRevenue(
            company_id=company_id,
            period=period,
            raw_post_id=raw_post.id,
            region=item.region,
            revenue=safe_float(item.revenue),
            revenue_share=safe_float(item.revenue_share),
            growth_yoy=safe_str(item.growth_yoy),
            note=item.note,
        ))
    counts["geographic_revenues"] = len(data.geographic_revenues)

    # ── Non-Financial KPIs ──────────────────────────────────────────────
    for item in data.non_financial_kpis:
        session.add(NonFinancialKPI(
            company_id=company_id,
            period=period,
            raw_post_id=raw_post.id,
            kpi_name=item.kpi_name,
            kpi_value=str(item.kpi_value),
            kpi_unit=item.kpi_unit,
            yoy_change=item.yoy_change,
            category=item.category,
            note=item.note,
        ))
    counts["non_financial_kpis"] = len(data.non_financial_kpis)

    # ── Debt Obligations ────────────────────────────────────────────────
    for item in data.debt_obligations:
        session.add(DebtObligation(
            company_id=company_id,
            period=period,
            raw_post_id=raw_post.id,
            instrument_name=item.instrument_name,
            debt_type=item.debt_type,
            principal=safe_float(item.principal),
            currency=item.currency,
            interest_rate=safe_float(item.interest_rate),
            maturity_date=_parse_date(item.maturity_date),
            is_secured=item.is_secured,
            is_current=item.is_current,
            is_floating_rate=item.is_floating_rate,
            note=item.note,
        ))
    counts["debt_obligations"] = len(data.debt_obligations)

    # ── Litigations ─────────────────────────────────────────────────────
    for item in data.litigations:
        session.add(Litigation(
            company_id=company_id,
            raw_post_id=raw_post.id,
            case_name=item.case_name,
            case_type=item.case_type,
            status=item.status,
            counterparty=item.counterparty,
            filed_at=_parse_date(item.filed_at),
            claimed_amount=safe_float(item.claimed_amount),
            accrued_amount=safe_float(item.accrued_amount),
            currency=item.currency,
            description=item.description,
        ))
    counts["litigations"] = len(data.litigations)

    # ── Executive Compensations ─────────────────────────────────────────
    for item in data.executive_compensations:
        session.add(ExecutiveCompensation(
            company_id=company_id,
            period=period,
            raw_post_id=raw_post.id,
            role_type=item.role_type,
            name=item.name,
            title=item.title,
            base_salary=safe_float(item.base_salary),
            bonus=safe_float(item.bonus),
            stock_awards=safe_float(item.stock_awards),
            option_awards=safe_float(item.option_awards),
            non_equity_incentive=safe_float(item.non_equity_incentive),
            other_comp=safe_float(item.other_comp),
            total_comp=safe_float(item.total_comp),
            pay_ratio=safe_float(item.pay_ratio),
            median_employee_comp=safe_float(item.median_employee_comp),
        ))
    counts["executive_compensations"] = len(data.executive_compensations)

    # ── Stock Ownership ─────────────────────────────────────────────────
    for item in data.stock_ownership:
        session.add(StockOwnership(
            company_id=company_id,
            period=period,
            raw_post_id=raw_post.id,
            name=item.name,
            title=item.title,
            shares_beneficially_owned=item.shares_beneficially_owned,
            percent_of_class=safe_float(item.percent_of_class),
        ))
    counts["stock_ownership"] = len(data.stock_ownership)

    # ── Related Party Transactions ──────────────────────────────────────
    for item in data.related_party_transactions:
        session.add(RelatedPartyTransaction(
            company_id=company_id,
            period=period,
            raw_post_id=raw_post.id,
            related_party=item.related_party,
            relationship=item.relationship,
            transaction_type=item.transaction_type,
            amount=safe_float(item.amount),
            currency=item.currency,
            terms=item.terms,
            is_ongoing=item.is_ongoing,
            description=item.description,
        ))
    counts["related_party_transactions"] = len(data.related_party_transactions)

    # ── Pricing Actions ──────────────────────────────────────────────
    for item in data.pricing_actions:
        session.add(PricingAction(
            company_id=company_id,
            period=period,
            product_or_segment=item.product_or_segment,
            price_change_pct=safe_float(item.price_change_pct),
            volume_impact_pct=safe_float(item.volume_impact_pct),
            effective_date=_parse_date(item.effective_date),
            raw_post_id=raw_post.id,
        ))
    counts["pricing_actions"] = len(data.pricing_actions)

    # ── Competitor Relations ─────────────────────────────────────────
    for item in data.competitor_relations:
        session.add(CompetitorRelation(
            company_id=company_id,
            competitor_name=item.competitor_name,
            market_segment=item.market_segment,
            relationship_type=item.relationship_type,
            raw_post_id=raw_post.id,
        ))
    counts["competitor_relations"] = len(data.competitor_relations)

    # ── Market Share Data ────────────────────────────────────────────
    for item in data.market_share_data:
        session.add(MarketShareData(
            company_id=company_id,
            market_segment=item.market_segment,
            period=period,
            share_pct=safe_float(item.share_pct),
            source_description=item.source_description,
            raw_post_id=raw_post.id,
        ))
    counts["market_share_data"] = len(data.market_share_data)

    # ── Known Issues ─────────────────────────────────────────────────
    for item in data.known_issues:
        session.add(KnownIssue(
            company_id=company_id,
            period=period,
            issue_description=item.issue_description,
            issue_category=item.issue_category,
            severity=item.severity,
            source_type=item.source_type,
            raw_post_id=raw_post.id,
        ))
    counts["known_issues"] = len(data.known_issues)

    # ── Management Acknowledgments ───────────────────────────────────
    for item in data.management_acknowledgments:
        session.add(ManagementAcknowledgment(
            company_id=company_id,
            period=period,
            issue_description=item.issue_description,
            response_quality=item.response_quality,
            has_action_plan=item.has_action_plan,
            raw_post_id=raw_post.id,
        ))
    counts["management_acknowledgments"] = len(data.management_acknowledgments)

    # ── Executive Changes ────────────────────────────────────────────
    for item in data.executive_changes:
        session.add(ExecutiveChange(
            company_id=company_id,
            person_name=item.person_name,
            title=item.title,
            change_type=item.change_type,
            change_date=_parse_date(item.change_date),
            reason=item.reason,
            raw_post_id=raw_post.id,
        ))
    counts["executive_changes"] = len(data.executive_changes)

    # ── Audit Opinion ────────────────────────────────────────────────
    if data.audit_opinion:
        session.add(AuditOpinion(
            company_id=company_id,
            period=period,
            opinion_type=data.audit_opinion.opinion_type,
            auditor_name=data.audit_opinion.auditor_name,
            emphasis_matters=data.audit_opinion.emphasis_matters,
            raw_post_id=raw_post.id,
        ))
        counts["audit_opinions"] = 1
    else:
        counts["audit_opinions"] = 0

    # ── Management Guidance ──────────────────────────────────────────
    for item in data.management_guidance:
        session.add(ManagementGuidance(
            company_id=company_id,
            source_period=period,
            target_period=item.target_period,
            metric=item.metric,
            value_low=safe_float(item.value_low),
            value_high=safe_float(item.value_high),
            unit=item.unit,
            confidence_language=item.confidence_language,
            verbatim=item.verbatim,
            raw_post_id=raw_post.id,
        ))
    counts["management_guidance"] = len(data.management_guidance)

    # ── Financial Line Items ─────────────────────────────────────────
    # Group by statement_type, create one FinancialStatement per type, then add line items
    from collections import defaultdict
    stmt_groups = defaultdict(list)
    for item in data.financial_line_items:
        stmt_groups[item.statement_type or "income"].append(item)

    fi_count = 0
    for stmt_type, items in stmt_groups.items():
        stmt = FinancialStatement(
            company_id=company_id,
            period=period,
            period_type="annual" if "FY" in period or "fy" in period.lower() else "quarterly",
            statement_type=stmt_type,
            currency="USD",
            reported_at=raw_post.posted_at.date() if raw_post.posted_at else None,
            raw_post_id=raw_post.id,
        )
        session.add(stmt)
        await session.flush()
        for item in items:
            if item.item_key:
                session.add(FinancialLineItem(
                    statement_id=stmt.id,
                    item_key=item.item_key,
                    item_label=item.item_label or item.item_key,
                    value=safe_float(item.value) or 0,
                    parent_key=item.parent_key,
                    ordinal=item.ordinal or 0,
                    note=item.note,
                ))
                fi_count += 1
    counts["financial_line_items_flat"] = fi_count

    # ── Inventory Provisions ─────────────────────────────────────────
    for item in data.inventory_provisions:
        session.add(InventoryProvision(
            company_id=company_id,
            period=period,
            raw_post_id=raw_post.id,
            provision_amount=safe_float(item.provision_amount),
            provision_release=safe_float(item.provision_release),
            net_margin_impact_pct=safe_float(item.net_margin_impact_pct),
            note=item.note,
        ))
    counts["inventory_provisions"] = len(data.inventory_provisions)

    # ── Deferred Revenues ────────────────────────────────────────────
    for item in data.deferred_revenues:
        session.add(DeferredRevenue(
            company_id=company_id,
            period=period,
            raw_post_id=raw_post.id,
            total_deferred=safe_float(item.total_deferred),
            short_term=safe_float(item.short_term),
            long_term=safe_float(item.long_term),
            recognized_in_period=safe_float(item.recognized_in_period),
            note=item.note,
        ))
    counts["deferred_revenues"] = len(data.deferred_revenues)

    # ── Revenue Recognition Policies ─────────────────────────────────
    for item in data.revenue_recognition_policies:
        if item.category:
            session.add(RevenueRecognitionPolicy(
                company_id=company_id,
                period=period,
                raw_post_id=raw_post.id,
                category=item.category,
                policy=item.policy,
                key_judgments=item.key_judgments,
            ))
    counts["revenue_recognition_policies"] = len(data.revenue_recognition_policies)

    # ── Purchase Obligation Summaries ────────────────────────────────
    import json as _json
    for item in data.purchase_obligation_summaries:
        session.add(PurchaseObligationSummary(
            company_id=company_id,
            period=period,
            raw_post_id=raw_post.id,
            total_outstanding=safe_float(item.total_outstanding),
            inventory_purchase_obligations=safe_float(item.inventory_purchase_obligations),
            non_inventory_obligations=safe_float(item.non_inventory_obligations),
            cloud_service_agreements=safe_float(item.cloud_service_agreements),
            breakdown_by_year_json=_json.dumps(item.breakdown_by_year, ensure_ascii=False) if item.breakdown_by_year else None,
            note=item.note,
        ))
    counts["purchase_obligation_summaries"] = len(data.purchase_obligation_summaries)

    # ── ASP Trends ───────────────────────────────────────────────────
    for item in data.asp_trends:
        if item.product_category:
            session.add(ASPTrend(
                company_id=company_id,
                period=period,
                raw_post_id=raw_post.id,
                product_category=item.product_category,
                trend=item.trend,
                driver=item.driver,
                note=item.note,
            ))
    counts["asp_trends"] = len(data.asp_trends)

    # ── Recurring Revenue Breakdowns ─────────────────────────────────
    for item in data.recurring_revenue_breakdowns:
        session.add(RecurringRevenueBreakdown(
            company_id=company_id,
            period=period,
            raw_post_id=raw_post.id,
            recurring_revenue=safe_float(item.recurring_revenue),
            recurring_pct=safe_float(item.recurring_pct),
            nonrecurring_revenue=safe_float(item.nonrecurring_revenue),
            nonrecurring_pct=safe_float(item.nonrecurring_pct),
            note=item.note,
        ))
    counts["recurring_revenue_breakdowns"] = len(data.recurring_revenue_breakdowns)

    # ── 更新 RawPost ──────────────────────────────────────────────────
    raw_post.is_processed = True
    raw_post.processed_at = _utcnow()
    if data.summary:
        raw_post.content_summary = data.summary
    session.add(raw_post)
    await session.commit()

    total = sum(counts.values())
    logger.info(f"[Company] Write done: {total} rows across {len([v for v in counts.values() if v])} tables")

    # ── 自动检测年份覆盖 ──────────────────────────────────────────────
    coverage = None
    try:
        from anchor.commands.company_sources import check_company_coverage
        coverage = await check_company_coverage(
            session, company_id, company.name, company.ticker,
        )
        if coverage["missing_years"]:
            logger.warning(coverage["message"])
        else:
            logger.info(coverage["message"])
    except Exception as e:
        logger.debug(f"[Company] Coverage check skipped: {e}")

    return {
        "is_relevant_content": True,
        "skip_reason": None,
        "table_counts": counts,
        "summary": data.summary,
        "one_liner": data.one_liner,
        "company_name": company.name,
        "company_ticker": company.ticker,
        "coverage": coverage,
    }


# ── 主入口（串行 compute + write）──────────────────────────────────────


async def extract_company(
    raw_post: RawPost,
    session: AsyncSession,
    content: str,
    platform: str,
    author: str,
    today: str,
    fill_gaps: bool = False,
) -> dict | None:
    """Company 域提取入口：LLM 提取 → 全量表写入 DB。

    Args:
        fill_gaps: 增量模式 — 只提取缺失表对应的 topic，跳过已有数据的 topic。
    """
    skip_topics: set[str] | None = None

    if fill_gaps:
        # 查找公司 ticker（从内容前 5000 字符快速识别）
        from anchor.extract.pipelines._base import call_llm as _cl
        from sqlmodel import select as _sel
        # 尝试从已有公司档案查找
        existing = (await session.exec(
            _sel(CompanyProfile)
        )).all()
        if existing:
            # 检查每个公司的已有数据
            for cp in existing:
                populated = await _get_populated_tables(session, cp.id)
                skip = _topics_to_skip(populated)
                if skip:
                    skip_topics = skip
                    skipped_tables = []
                    for t in skip:
                        skipped_tables.extend(TOPIC_TO_TABLES[t])
                    logger.info(
                        f"[Company] Fill-gaps mode: {cp.name} ({cp.ticker}) — "
                        f"skipping topics {skip} (tables already populated: {skipped_tables})"
                    )
                    break

    compute_result = await extract_company_compute(
        content, platform, author, today, skip_topics=skip_topics,
    )
    return await extract_company_write(raw_post, session, compute_result)
