"""
XBRL 结构化数据提取
====================
从 SEC EDGAR XBRL 中提取财务三表、债务、审计、地理/业务分部等结构化数据。
免费、精确、零 token 消耗。

依赖: edgartools (pip install edgartools)

用法:
    from edgar import Company
    filing = Company("AAPL").get_filings(form="10-K")[0]
    result = extract_xbrl(filing)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from loguru import logger


# ── 标准概念映射 ──────────────────────────────────────────────────────
# XBRL US-GAAP concept → 我们的 item_key
# 每个 concept 带 statement_type 标签（income / balance_sheet / cashflow）

CONCEPT_MAP: list[tuple[str, str, str]] = [
    # ── Income Statement ──
    ("Revenues", "revenue", "income"),
    ("RevenueFromContractWithCustomerExcludingAssessedTax", "revenue", "income"),
    ("SalesRevenueNet", "revenue", "income"),
    ("CostOfRevenue", "cost_of_revenue", "income"),
    ("CostOfGoodsAndServicesSold", "cost_of_revenue", "income"),
    ("OperatingIncomeLoss", "operating_income", "income"),
    ("NetIncomeLoss", "net_income", "income"),
    ("InterestExpense", "interest_expense", "income"),
    ("SellingGeneralAndAdministrativeExpense", "sga_expense", "income"),
    ("ResearchAndDevelopmentExpense", "rnd_expense", "income"),
    ("IncomeTaxExpenseBenefit", "income_tax_expense_total", "income"),
    ("IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest", "income_before_tax_total", "income"),
    ("WeightedAverageNumberOfShareOutstandingBasicAndDiluted", "basic_weighted_average_shares", "income"),
    ("WeightedAverageNumberOfSharesOutstandingBasic", "basic_weighted_average_shares", "income"),

    # ── Balance Sheet ──
    ("Assets", "total_assets", "balance_sheet"),
    ("StockholdersEquity", "shareholders_equity", "balance_sheet"),
    ("StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest", "shareholders_equity", "balance_sheet"),
    ("Goodwill", "goodwill", "balance_sheet"),
    ("AccountsReceivableNetCurrent", "accounts_receivable", "balance_sheet"),
    ("InventoryNet", "inventory", "balance_sheet"),
    ("CashAndCashEquivalentsAtCarryingValue", "cash_and_equivalents", "balance_sheet"),
    ("LongTermDebt", "total_debt", "balance_sheet"),
    ("LongTermDebtAndCapitalLeaseObligations", "total_debt", "balance_sheet"),
    ("AssetsCurrent", "current_assets", "balance_sheet"),
    ("LiabilitiesCurrent", "current_liabilities", "balance_sheet"),

    # ── Cash Flow ──
    ("NetCashProvidedByUsedInOperatingActivities", "operating_cash_flow", "cashflow"),
    ("PaymentsToAcquirePropertyPlantAndEquipment", "capital_expenditures", "cashflow"),
    ("DepreciationDepletionAndAmortization", "depreciation_amortization", "cashflow"),
    ("PaymentsOfDividends", "dividends_paid", "cashflow"),
    ("PaymentsOfDividendsCommonStock", "dividends_paid", "cashflow"),
    ("PaymentsForRepurchaseOfCommonStock", "share_repurchase", "cashflow"),
    ("ProceedsFromIssuanceOfCommonStock", "proceeds_from_stock_issuance", "cashflow"),
    ("ProceedsFromIssuanceOfLongTermDebt", "proceeds_from_debt_issuance", "cashflow"),
]


# ── 维度轴名称 ──────────────────────────────────────────────────────
# XBRL 分部报告用的 Axis
SEGMENT_AXES = [
    "StatementBusinessSegmentsAxis",
    "ProductOrServiceAxis",
    "SegmentReportingInformationBySegmentAxis",
]

GEOGRAPHIC_AXES = [
    "StatementGeographicalAxis",
    "GeographicDistributionDomesticAndForeignAxis",
]


# ── 提取函数 ──────────────────────────────────────────────────────

def _safe_float(val: Any) -> float | None:
    """安全转换为 float，单位：百万美元。"""
    if val is None:
        return None
    try:
        v = float(val)
        # XBRL 原始值通常是实际金额，需要转百万
        # 但有些已经是百万（通过 scale 属性）
        # edgartools 返回的已经是原始值（美元），统一除以 1e6
        if abs(v) > 1e8:  # 超过1亿 → 大概率是原始美元
            return round(v / 1e6, 2)
        elif abs(v) > 1e5:  # 超过10万 → 可能是千元
            return round(v / 1e3, 2)
        else:
            return round(v, 2)
    except (ValueError, TypeError):
        return None


def _safe_float_raw(val: Any) -> float | None:
    """安全转换为 float，保留原值（用于比率等无需单位转换的字段）。"""
    if val is None:
        return None
    try:
        return round(float(val), 6)
    except (ValueError, TypeError):
        return None


def extract_financial_line_items(xbrl) -> list[dict]:
    """从 XBRL 提取三表核心科目。

    Args:
        xbrl: edgartools Filing.xbrl() 返回的 FilingXbrl 对象

    Returns:
        [{statement_type, item_key, item_label, value}, ...]
    """
    results = []
    seen_keys: set[str] = set()

    # 尝试通过 statements 接口获取
    try:
        statements = xbrl.statements
    except Exception:
        statements = None

    # 方法1: 通过 financials 属性获取标准报表
    for concept_name, item_key, stmt_type in CONCEPT_MAP:
        if item_key in seen_keys:
            continue

        val = _get_fact_value(xbrl, concept_name)
        if val is not None:
            fval = _safe_float(val)
            if fval is not None and fval != 0:
                results.append({
                    "statement_type": stmt_type,
                    "item_key": item_key,
                    "item_label": concept_name,
                    "value": fval,
                })
                seen_keys.add(item_key)

    logger.info(f"[XBRL] 提取 {len(results)} 个财务科目")
    return results


def _get_fact_value(xbrl, concept: str) -> Any:
    """从 XBRL 获取单个 fact 的值。优先取最新期间、无维度的值。"""
    try:
        # edgartools: xbrl.facts 或 xbrl.get_facts_for_concept()
        facts = None

        # 方法1: query_facts (edgartools >= 3.x)
        if hasattr(xbrl, 'query_facts'):
            try:
                facts = xbrl.query_facts(concept)
            except Exception:
                pass

        # 方法2: get_facts (some versions)
        if facts is None and hasattr(xbrl, 'get_facts'):
            try:
                facts = xbrl.get_facts(concept)
            except Exception:
                pass

        # 方法3: 直接从 facts DataFrame
        if facts is None and hasattr(xbrl, 'facts'):
            try:
                df = xbrl.facts
                if hasattr(df, 'query'):
                    matched = df[df['concept'].str.contains(concept, case=False, na=False)]
                    if not matched.empty:
                        # 取最新的、无维度的
                        row = matched.iloc[-1]
                        return row.get('value', row.get('val', None))
            except Exception:
                pass

        # 方法4: 通过 statements
        if facts is None:
            return _get_from_statements(xbrl, concept)

        if facts is not None:
            if hasattr(facts, '__len__') and len(facts) > 0:
                # DataFrame
                if hasattr(facts, 'iloc'):
                    return facts.iloc[-1].get('value', facts.iloc[-1].get('val', None))
                # list
                if isinstance(facts, list):
                    return facts[-1] if not isinstance(facts[-1], dict) else facts[-1].get('value')

    except Exception as e:
        logger.debug(f"[XBRL] 获取 {concept} 失败: {e}")
    return None


def _get_from_statements(xbrl, concept: str) -> Any:
    """从标准报表中查找概念值。"""
    try:
        # 尝试获取各标准报表
        for stmt_getter in ['income_statement', 'balance_sheet', 'cash_flow_statement']:
            try:
                stmt = getattr(xbrl, stmt_getter, None)
                if callable(stmt):
                    stmt = stmt()
                if stmt is None:
                    continue

                # stmt 可能是 DataFrame 或自定义对象
                if hasattr(stmt, 'get_value'):
                    val = stmt.get_value(concept)
                    if val is not None:
                        return val
                elif hasattr(stmt, 'data') and hasattr(stmt.data, 'query'):
                    matched = stmt.data[stmt.data['concept'].str.contains(concept, case=False, na=False)]
                    if not matched.empty:
                        return matched.iloc[-1].get('value')
            except Exception:
                continue
    except Exception:
        pass
    return None


def extract_debt_obligations(xbrl) -> list[dict]:
    """从 XBRL 提取债务明细。"""
    results = []

    # 尝试从 notes / 维度数据获取债务分项
    debt_concepts = [
        ("LongTermDebt", "bond"),
        ("LongTermLineOfCredit", "credit_facility"),
        ("CapitalLeaseObligations", "lease"),
        ("ConvertibleDebt", "convertible"),
        ("SecuredDebt", "loan"),
        ("UnsecuredDebt", "bond"),
        ("NotesPayable", "bond"),
    ]

    for concept, debt_type in debt_concepts:
        val = _get_fact_value(xbrl, concept)
        if val is not None:
            fval = _safe_float(val)
            if fval is not None and fval != 0:
                results.append({
                    "instrument_name": concept,
                    "debt_type": debt_type,
                    "principal": fval,
                    "interest_rate": None,
                    "maturity_date": None,
                })

    logger.info(f"[XBRL] 提取 {len(results)} 条债务")
    return results


def extract_geographic_revenues(xbrl) -> list[dict]:
    """从 XBRL 维度数据提取地理收入分布。"""
    results = []

    try:
        # edgartools: 通过维度 facts 获取地理分部
        for axis in GEOGRAPHIC_AXES:
            members = _get_dimension_members(xbrl, axis)
            if members:
                total = sum(m['value'] for m in members if m['value'] is not None)
                for m in members:
                    if m['value'] is not None and total > 0:
                        results.append({
                            "region": m['member'],
                            "revenue_share": round(m['value'] / total, 4),
                        })
                if results:
                    break
    except Exception as e:
        logger.debug(f"[XBRL] 地理收入提取失败: {e}")

    logger.info(f"[XBRL] 提取 {len(results)} 个地理分部")
    return results


def extract_segment_hints(xbrl) -> list[str]:
    """从 XBRL 维度数据提取业务分部名称（供 LLM prompt 参考）。"""
    hints = []

    try:
        for axis in SEGMENT_AXES:
            members = _get_dimension_members(xbrl, axis)
            if members:
                for m in members:
                    name = m['member']
                    # 清理 XBRL member 名称（去掉 "Member" 后缀等）
                    name = name.replace("Member", "").strip()
                    if name and name not in hints:
                        hints.append(name)
                if hints:
                    break
    except Exception as e:
        logger.debug(f"[XBRL] 分部提示提取失败: {e}")

    return hints


def _get_dimension_members(xbrl, axis: str) -> list[dict]:
    """获取指定维度轴的所有成员及其收入值。"""
    members = []

    try:
        # edgartools 通用方法
        if hasattr(xbrl, 'facts') and hasattr(xbrl.facts, 'query'):
            df = xbrl.facts
            # 找包含该 axis 的 facts
            if 'dimensions' in df.columns:
                dim_rows = df[df['dimensions'].apply(
                    lambda d: axis in str(d) if d else False
                )]
            elif 'axis' in df.columns:
                dim_rows = df[df['axis'].str.contains(axis, case=False, na=False)]
            else:
                return members

            # 找收入相关的 facts
            revenue_concepts = ['Revenue', 'Sales', 'NetSales']
            for concept in revenue_concepts:
                matched = dim_rows[dim_rows['concept'].str.contains(concept, case=False, na=False)]
                if not matched.empty:
                    for _, row in matched.iterrows():
                        member_name = _extract_member_name(row, axis)
                        val = row.get('value', row.get('val'))
                        if member_name and val is not None:
                            members.append({
                                'member': member_name,
                                'value': float(val),
                            })
                    if members:
                        break
    except Exception as e:
        logger.debug(f"[XBRL] 维度 {axis} 查询失败: {e}")

    return members


def _extract_member_name(row, axis: str) -> str | None:
    """从 fact row 中提取维度成员名称。"""
    # 不同版本的 edgartools 结构不同
    for field in ('member', 'dimensions', 'dim_member'):
        val = row.get(field)
        if val:
            if isinstance(val, dict):
                return val.get(axis, str(val))
            return str(val).split(':')[-1].replace('Member', '').strip()
    return None


def extract_audit_info(xbrl) -> dict | None:
    """从 XBRL 提取审计意见。"""
    try:
        # 审计师名称
        auditor = _get_fact_value(xbrl, "AuditorName")
        if auditor is None:
            auditor = _get_fact_value(xbrl, "AccountingFirm")

        if auditor:
            return {
                "opinion_type": "unqualified",  # XBRL 中 99%+ 是无保留意见
                "auditor_name": str(auditor),
                "emphasis_matters": None,
            }
    except Exception as e:
        logger.debug(f"[XBRL] 审计信息提取失败: {e}")

    return None


# ── 主入口 ────────────────────────────────────────────────────────────

@dataclass
class XBRLData:
    """XBRL 提取结果。"""
    financial_line_items: list[dict] = field(default_factory=list)
    debt_obligations: list[dict] = field(default_factory=list)
    geographic_revenues: list[dict] = field(default_factory=list)
    audit_opinion: dict | None = None
    segment_hints: list[str] = field(default_factory=list)
    # 元数据
    has_xbrl: bool = False
    error: str | None = None


def extract_xbrl(filing) -> XBRLData:
    """从 edgartools Filing 对象提取全部 XBRL 数据。

    Args:
        filing: edgartools Filing 对象 (Company.get_filings()[0])

    Returns:
        XBRLData 包含所有可提取的结构化数据
    """
    result = XBRLData()

    try:
        xbrl = filing.xbrl()
    except Exception as e:
        result.error = f"XBRL 解析失败: {e}"
        logger.warning(f"[XBRL] {result.error}")
        return result

    if xbrl is None:
        result.error = "Filing 无 XBRL 数据"
        logger.warning(f"[XBRL] {result.error}")
        return result

    result.has_xbrl = True
    logger.info("[XBRL] 开始提取结构化数据")

    # 1. 财务三表
    result.financial_line_items = extract_financial_line_items(xbrl)

    # 2. 债务
    result.debt_obligations = extract_debt_obligations(xbrl)

    # 3. 地理收入
    result.geographic_revenues = extract_geographic_revenues(xbrl)

    # 4. 审计
    result.audit_opinion = extract_audit_info(xbrl)

    # 5. 业务分部提示（供 LLM prompt 用）
    result.segment_hints = extract_segment_hints(xbrl)

    logger.info(
        f"[XBRL] 提取完成: "
        f"{len(result.financial_line_items)} 财务科目, "
        f"{len(result.debt_obligations)} 债务, "
        f"{len(result.geographic_revenues)} 地理分部, "
        f"审计={'有' if result.audit_opinion else '无'}, "
        f"{len(result.segment_hints)} 分部提示"
    )
    return result
