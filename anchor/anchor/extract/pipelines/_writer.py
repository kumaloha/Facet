"""
提取结果 → Anchor DB 写入
===========================
将 ExtractionResult 的 tables dict 写入对应的 SQLModel 表。
自动创建/查找 CompanyProfile，按 natural key 去重。
"""

from __future__ import annotations

from datetime import date as _date
from typing import Optional

from loguru import logger
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from anchor.extract.pipelines._mapreduce import ExtractionResult
from anchor.models import (
    AuditOpinion,
    CompanyNarrative,
    CompanyProfile,
    CompetitiveDynamic,
    DebtObligation,
    DownstreamSegment,
    ExecutiveChange,
    ExecutiveCompensation,
    FinancialLineItem,
    FinancialStatement,
    GeographicRevenue,
    KnownIssue,
    Litigation,
    ManagementAcknowledgment,
    ManagementGuidance,
    MarketShareData,
    NonFinancialKPI,
    OperationalIssue,
    PeerFinancial,
    PricingAction,
    RelatedPartyTransaction,
    StockOwnership,
    UpstreamSegment,
    _utcnow,
)


def _parse_date(s) -> Optional[_date]:
    if not s or s == "null":
        return None
    try:
        return _date.fromisoformat(str(s)[:10])
    except (ValueError, TypeError):
        return None


async def get_or_create_company(
    session: AsyncSession,
    ticker: str,
    name: str = "",
    market: str = "us",
    industry: str | None = None,
) -> CompanyProfile:
    """获取或创建公司档案。"""
    result = await session.exec(
        select(CompanyProfile).where(CompanyProfile.ticker == ticker)
    )
    existing = result.first()
    if existing:
        return existing

    company = CompanyProfile(
        name=name or ticker,
        ticker=ticker,
        market=market,
        industry=industry,
    )
    session.add(company)
    await session.flush()
    logger.info(f"[Writer] 新建公司: {ticker} ({name})")
    return company


# ── 表写入映射 ──────────────────────────────────────────────────────

async def _write_financial_line_items(
    session: AsyncSession, company_id: int, period: str, rows: list[dict],
    raw_post_id: int | None = None,
):
    """写入财务科目。需要先创建 FinancialStatement。"""
    if not rows:
        return 0

    # 按 statement_type 分组
    by_type: dict[str, list[dict]] = {}
    for row in rows:
        st = row.get("statement_type", "income")
        by_type.setdefault(st, []).append(row)

    count = 0
    for stmt_type, items in by_type.items():
        # 创建 statement
        stmt = FinancialStatement(
            company_id=company_id,
            period=period,
            period_type="annual",
            statement_type=stmt_type,
            raw_post_id=raw_post_id,
        )
        session.add(stmt)
        await session.flush()

        for i, item in enumerate(items):
            val = item.get("value")
            if val is None:
                continue
            li = FinancialLineItem(
                statement_id=stmt.id,
                item_key=item.get("item_key", ""),
                item_label=item.get("item_label", ""),
                value=float(val),
                parent_key=item.get("parent_key"),
                ordinal=i,
                note=item.get("note"),
            )
            session.add(li)
            count += 1

    return count


async def _write_simple_rows(
    session: AsyncSession,
    model_cls,
    company_id: int,
    period: str,
    rows: list[dict],
    field_mapping: dict[str, str] | None = None,
    raw_post_id: int | None = None,
    has_period: bool = True,
):
    """通用写入：将 dict 列表写入指定 SQLModel 表。"""
    if not rows:
        return 0

    field_mapping = field_mapping or {}
    count = 0
    for row in rows:
        kwargs = {"company_id": company_id}
        if has_period:
            kwargs["period"] = period
        if raw_post_id and hasattr(model_cls, "raw_post_id"):
            kwargs["raw_post_id"] = raw_post_id

        # 映射字段
        for src_key, val in row.items():
            dst_key = field_mapping.get(src_key, src_key)
            if hasattr(model_cls, dst_key) and dst_key not in ("id", "company_id", "period"):
                # 日期字段特殊处理
                if dst_key in ("effective_date", "filed_at", "change_date",
                               "maturity_date", "reported_at", "deadline",
                               "event_date"):
                    kwargs[dst_key] = _parse_date(val)
                else:
                    kwargs[dst_key] = val

        # 补充必填字段的默认值 — 覆盖所有 NOT NULL str 字段
        _NOT_NULL_DEFAULTS = {
            "source_period": period,
            "market_segment": row.get("market_segment", ""),
            "case_name": row.get("case_name", row.get("description", "")[:200] if row.get("description") else ""),
            "topic": row.get("topic", ""),
            "metric": row.get("metric", "other"),
            "narrative": row.get("narrative", ""),
            "competitor_name": row.get("competitor_name", ""),
            "event_type": row.get("event_type", "other"),
            "event_description": row.get("event_description", ""),
            "related_party": row.get("related_party", ""),
            "relationship": row.get("relationship", "other"),
            "transaction_type": row.get("transaction_type", "other"),
            "issue_description": row.get("issue_description", ""),
            "product_or_segment": row.get("product_or_segment", ""),
        }
        for field, default in _NOT_NULL_DEFAULTS.items():
            if hasattr(model_cls, field) and field not in kwargs:
                kwargs[field] = default

        if hasattr(model_cls, "name") and "name" not in kwargs and model_cls not in (CompanyProfile, ExecutiveChange):
            kwargs["name"] = row.get("name", row.get("person_name", ""))
        if hasattr(model_cls, "person_name") and "person_name" not in kwargs:
            kwargs["person_name"] = row.get("person_name", row.get("name", ""))

        # 跳过关键字段为空的行（LLM 输出缺失）
        skip = False
        for field in ("competitor_name", "related_party", "person_name"):
            if hasattr(model_cls, field) and not kwargs.get(field):
                logger.warning(
                    f"[Writer] 跳过 {model_cls.__tablename__} 行: {field} 为空"
                )
                skip = True
                break
        if skip:
            continue

        try:
            obj = model_cls(**kwargs)
            async with session.begin_nested():
                session.add(obj)
                await session.flush()
            count += 1
        except Exception as e:
            logger.warning(f"[Writer] 写入 {model_cls.__tablename__} 失败: {e}")

    return count


# ── 表名 → 写入逻辑映射 ──────────────────────────────────────────

TABLE_WRITERS = {
    "financial_line_items": ("_fli", None),
    "downstream_segments": (DownstreamSegment, None),
    "upstream_segments": (UpstreamSegment, None),
    "geographic_revenues": (GeographicRevenue, None),
    "competitive_dynamics": (CompetitiveDynamic, {"event_date": "event_date"}),
    "peer_financials": (PeerFinancial, None),
    "market_share_data": (MarketShareData, {
        "company_or_competitor": "market_segment",
        "share_pct": "share_pct",
    }),
    "known_issues": (KnownIssue, None),
    "management_acknowledgments": (ManagementAcknowledgment, None),
    "company_narratives": (CompanyNarrative, {
        "deadline": "deadline",
    }),
    "executive_compensations": (ExecutiveCompensation, None),
    "stock_ownership": (StockOwnership, None),
    "related_party_transactions": (RelatedPartyTransaction, None),
    "executive_changes": (ExecutiveChange, None),
    "litigations": (Litigation, {
        "case_name": "case_name",
        "filed_at": "filed_at",
    }),
    "debt_obligations": (DebtObligation, None),
    "operational_issues": (OperationalIssue, None),
    "management_guidance": (ManagementGuidance, None),
    "pricing_actions": (PricingAction, None),
    "non_financial_kpis": (NonFinancialKPI, None),
    "audit_opinion": (AuditOpinion, None),
}


async def write_extraction_result(
    session: AsyncSession,
    result: ExtractionResult,
    market: str = "us",
    raw_post_id: int | None = None,
) -> dict[str, int]:
    """将提取结果写入 Anchor DB。

    Returns:
        {table_name: rows_written}
    """
    ticker = result.company_ticker
    if not ticker:
        logger.warning("[Writer] 无 ticker，跳过写入")
        return {}

    company = await get_or_create_company(
        session, ticker,
        name=result.company_name,
        market=market,
    )
    company_id = company.id
    period = result.period or "FY2025"

    stats: dict[str, int] = {}

    for table_name, rows in result.tables.items():
        if not rows:
            continue

        if table_name == "financial_line_items":
            n = await _write_financial_line_items(
                session, company_id, period, rows, raw_post_id)
            stats[table_name] = n
            continue

        # 查表配置
        config = TABLE_WRITERS.get(table_name)
        if not config:
            logger.warning(f"[Writer] 未知表 '{table_name}'，跳过 {len(rows)} 行")
            continue

        model_cls, field_mapping = config

        # audit_opinion 是单个 dict 不是 list
        if table_name == "audit_opinion":
            if isinstance(rows, list):
                pass  # 已经是 list
            else:
                rows = [rows]

        # 判断是否需要 period 字段
        has_period = hasattr(model_cls, "period") if model_cls != "_fli" else True

        n = await _write_simple_rows(
            session, model_cls, company_id, period, rows,
            field_mapping=field_mapping,
            raw_post_id=raw_post_id,
            has_period=has_period,
        )
        stats[table_name] = n

    await session.commit()

    total = sum(stats.values())
    logger.info(f"[Writer] {ticker}/{period}: 写入 {total} 行 → {stats}")
    return stats
