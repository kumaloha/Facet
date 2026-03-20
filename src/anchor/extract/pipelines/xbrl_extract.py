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

import pandas as pd
from loguru import logger


# ── 标准概念映射 ──────────────────────────────────────────────────────
# (XBRL concept 精确后缀, 我们的 item_key, statement_type, is_shares)
# 按优先级排序：同一个 item_key 只取第一个匹配
# concept 匹配规则: df['concept'].str.endswith(':' + suffix) — 精确匹配

CONCEPT_MAP: list[tuple[str, str, str, bool]] = [
    # ── Income Statement (duration) ──
    # Revenue: 按优先级，覆盖科技/零售/银行/公用事业/保险
    ("RevenueFromContractWithCustomerExcludingAssessedTax", "revenue", "income", False),
    ("RevenueFromContractWithCustomerIncludingAssessedTax", "revenue", "income", False),
    ("Revenues", "revenue", "income", False),
    ("SalesRevenueNet", "revenue", "income", False),
    ("InterestAndDividendIncomeOperating", "revenue", "income", False),  # 银行
    ("InterestIncomeExpenseNet", "revenue", "income", False),            # 银行 (net interest)
    ("RegulatedAndUnregulatedOperatingRevenue", "revenue", "income", False),  # 公用事业
    ("RegulatedOperatingRevenue", "revenue", "income", False),                # 公用事业
    ("OperatingLeasesIncomeStatementLeaseRevenue", "revenue", "income", False),  # REIT
    ("PremiumsEarnedNet", "revenue", "income", False),                   # 保险
    ("RevenuesAndOther", "revenue", "income", False),                     # 石油 (APA)
    ("OperatingRevenue", "revenue", "income", False),                    # 通用 fallback
    ("CostOfGoodsAndServicesSold", "cost_of_revenue", "income", False),
    ("CostOfRevenue", "cost_of_revenue", "income", False),
    ("OperatingIncomeLoss", "operating_income", "income", False),
    ("NetIncomeLoss", "net_income", "income", False),
    ("ProfitLoss", "net_income", "income", False),                       # AVGO/MA/CAT 等
    ("NetIncomeLossAvailableToCommonStockholdersBasic", "net_income", "income", False),  # BKNG
    ("InterestExpense", "interest_expense", "income", False),
    ("InterestExpenseNonoperating", "interest_expense", "income", False),
    ("SellingGeneralAndAdministrativeExpense", "sga_expense", "income", False),
    ("ResearchAndDevelopmentExpense", "rnd_expense", "income", False),
    ("IncomeTaxExpenseBenefit", "income_tax_expense_total", "income", False),
    ("IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest", "income_before_tax_total", "income", False),
    ("WeightedAverageNumberOfSharesOutstandingBasic", "basic_weighted_average_shares", "income", True),

    # ── Balance Sheet (instant) ──
    ("Assets", "total_assets", "balance_sheet", False),
    ("Liabilities", "total_liabilities", "balance_sheet", False),
    ("LiabilitiesAndStockholdersEquity", "total_liabilities_and_equity", "balance_sheet", False),
    ("StockholdersEquity", "shareholders_equity", "balance_sheet", False),
    ("StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest", "shareholders_equity", "balance_sheet", False),
    ("Goodwill", "goodwill", "balance_sheet", False),
    ("AccountsReceivableNetCurrent", "accounts_receivable", "balance_sheet", False),
    ("InventoryNet", "inventory", "balance_sheet", False),
    ("CashAndCashEquivalentsAtCarryingValue", "cash_and_equivalents", "balance_sheet", False),
    ("LongTermDebt", "total_debt", "balance_sheet", False),
    ("LongTermDebtAndCapitalLeaseObligations", "total_debt", "balance_sheet", False),
    ("AssetsCurrent", "current_assets", "balance_sheet", False),
    ("LiabilitiesCurrent", "current_liabilities", "balance_sheet", False),

    # ── Cash Flow (duration) ──
    ("NetCashProvidedByUsedInOperatingActivities", "operating_cash_flow", "cashflow", False),
    ("PaymentsToAcquirePropertyPlantAndEquipment", "capital_expenditures", "cashflow", False),
    ("DepreciationDepletionAndAmortization", "depreciation_amortization", "cashflow", False),
    ("PaymentsOfDividends", "dividends_paid", "cashflow", False),
    ("PaymentsOfDividendsCommonStock", "dividends_paid", "cashflow", False),
    ("PaymentsForRepurchaseOfCommonStock", "share_repurchase", "cashflow", False),
    ("ProceedsFromIssuanceOfCommonStock", "proceeds_from_stock_issuance", "cashflow", False),
    ("ProceedsFromIssuanceOfLongTermDebt", "proceeds_from_debt_issuance", "cashflow", False),
]

# 用于匹配债务面值的 XBRL concept 关键词（按优先级）
_DEBT_PRINCIPAL_CONCEPTS = [
    "DebtInstrumentFaceAmount",
    "LongTermDebt",
    "DebtInstrumentCarryingAmount",
    "LongTermDebtNoncurrent",
]

_DEBT_RATE_CONCEPTS = [
    "DebtInstrumentInterestRateStatedPercentage",
    "DebtInstrumentInterestRateEffectivePercentage",
]


# ── 提取函数 ──────────────────────────────────────────────────────

def _to_millions(val: float) -> float:
    """将原始美元值转为百万美元。edgartools 已处理 scale，返回美元整数。"""
    return round(val / 1e6, 2)


def _to_millions_shares(val: float) -> float:
    """将原始股数转为百万股。"""
    return round(val / 1e6, 2)


def _safe_col(df: pd.DataFrame, col: str) -> bool:
    """检查列是否存在。"""
    return col in df.columns


def _detect_fy_end(df: pd.DataFrame) -> str | None:
    """检测最新财年结束日期。

    策略：
    1. 优先用 fiscal_period=='FY' 的 duration facts
    2. fallback: 取 duration facts 中最新的 period_end
    """
    non_dim = df[~df['is_dimensioned'] & df['numeric_value'].notna()]
    dur = non_dim[non_dim['period_type'] == 'duration']
    if dur.empty:
        return None

    # 优先: fiscal_period == 'FY'
    if _safe_col(dur, 'fiscal_period'):
        fy = dur[dur['fiscal_period'] == 'FY']
        if not fy.empty:
            return fy['period_end'].max()

    # fallback: 取最长 duration 的 period_end（最可能是 FY）
    if _safe_col(dur, 'period_start') and _safe_col(dur, 'period_end'):
        dur = dur.copy()
        dur['_duration_days'] = (
            pd.to_datetime(dur['period_end']) - pd.to_datetime(dur['period_start'])
        ).dt.days
        long = dur[dur['_duration_days'] > 300]  # > 300 天 → 年报
        if not long.empty:
            return long['period_end'].max()

    return dur['period_end'].max()


def extract_financial_line_items(df: pd.DataFrame) -> list[dict]:
    """从 XBRL DataFrame 提取三表核心科目。

    策略：
      - duration 类（损益/现金流）：取最新 FY 的 period_end
      - instant 类（资产负债表）：取与 FY 结束日匹配的 period_instant
      - 不要求 fiscal_period=='FY'（某些公司 instant facts 没有此标签）
      - val==0 不跳过（合法值，如零库存）
    """
    non_dim = df[~df['is_dimensioned'] & df['numeric_value'].notna()].copy()
    if non_dim.empty:
        return []

    fy_end = _detect_fy_end(df)

    # Duration facts: 取最新 FY 的 period_end
    dur = non_dim[non_dim['period_type'] == 'duration']
    if not dur.empty and fy_end is not None:
        dur = dur[dur['period_end'] == fy_end]

    # Instant facts: 取与 FY 结束日最接近的 period_instant
    inst = non_dim[non_dim['period_type'] == 'instant']
    if not inst.empty and _safe_col(inst, 'period_instant'):
        if fy_end is not None:
            # 优先精确匹配
            matched = inst[inst['period_instant'] == fy_end]
            if not matched.empty:
                inst = matched
            else:
                # fallback: 取 facts 数量最多的 instant 日期
                counts = inst['period_instant'].value_counts()
                best_date = counts.index[0]
                inst = inst[inst['period_instant'] == best_date]
                logger.debug(
                    f"[XBRL] instant facts 无法匹配 FY end {fy_end}，"
                    f"fallback 到 {best_date} ({counts.iloc[0]} facts)"
                )
        else:
            counts = inst['period_instant'].value_counts()
            best_date = counts.index[0]
            inst = inst[inst['period_instant'] == best_date]

    # 合并
    all_facts = pd.concat([dur, inst], ignore_index=True)
    if all_facts.empty:
        return []

    # 去重：同一 concept 只取第一条
    all_facts = all_facts.drop_duplicates(subset='concept', keep='first')

    # 按 CONCEPT_MAP 精确匹配
    results = []
    seen_keys: set[str] = set()

    for concept_suffix, item_key, stmt_type, is_shares in CONCEPT_MAP:
        if item_key in seen_keys:
            continue

        match_str = ':' + concept_suffix
        matched = all_facts[all_facts['concept'].str.endswith(match_str)]
        if matched.empty:
            continue

        val = matched.iloc[0]['numeric_value']

        # 单位转换
        if is_shares:
            converted = _to_millions_shares(val)
        else:
            converted = _to_millions(val)

        results.append({
            "statement_type": stmt_type,
            "item_key": item_key,
            "item_label": matched.iloc[0]['concept'],
            "value": converted,
        })
        seen_keys.add(item_key)

    logger.info(f"[XBRL] 提取 {len(results)} 个财务科目")
    return results


def extract_debt_obligations(df: pd.DataFrame) -> list[dict]:
    """从 XBRL 维度数据提取债务明细。

    策略：
      - 只从 DebtInstrumentAxis 维度提取
      - 面值：优先 DebtInstrumentFaceAmount，fallback 到 LongTermDebt 等
      - 利率：从 InterestRateStatedPercentage 取
      - 过滤掉 principal=0 的到期债务
    """
    results = []

    debt_col = 'dim_us-gaap_DebtInstrumentAxis'
    if debt_col not in df.columns:
        return results

    debt_facts = df[df[debt_col].notna() & df['numeric_value'].notna()].copy()
    if debt_facts.empty:
        return results

    # 找最新报告期的 instant 日期
    latest_instant = None
    if _safe_col(debt_facts, 'period_instant'):
        instants = debt_facts['period_instant'].dropna()
        if not instants.empty:
            latest_instant = instants.max()

    # 收集每个 instrument 的数据
    instruments = debt_facts[debt_col].unique()
    for instrument in instruments:
        inst_facts = debt_facts[debt_facts[debt_col] == instrument]
        name = str(instrument).split(':')[-1].replace('Member', '').strip()

        # ── 取面值：在最新期间找 FaceAmount ──
        principal = None
        for concept_kw in _DEBT_PRINCIPAL_CONCEPTS:
            mask = inst_facts['concept'].str.contains(concept_kw, case=False, na=False)
            candidates = inst_facts[mask]
            if candidates.empty:
                continue
            # 优先取最新 instant
            if latest_instant is not None and _safe_col(candidates, 'period_instant'):
                latest_cands = candidates[candidates['period_instant'] == latest_instant]
                if not latest_cands.empty:
                    candidates = latest_cands
            val = candidates.iloc[0]['numeric_value']
            if val != 0:  # 跳过已到期（面值清零）的债务
                principal = _to_millions(val)
                break

        if principal is None or principal <= 0:
            continue  # 无有效面值 → 跳过

        # ── 取利率 ──
        interest_rate = None
        for rate_kw in _DEBT_RATE_CONCEPTS:
            mask = inst_facts['concept'].str.contains(rate_kw, case=False, na=False)
            candidates = inst_facts[mask]
            if candidates.empty:
                continue
            if latest_instant is not None and _safe_col(candidates, 'period_instant'):
                latest_cands = candidates[candidates['period_instant'] == latest_instant]
                if not latest_cands.empty:
                    candidates = latest_cands
            val = candidates.iloc[0]['numeric_value']
            if val > 0 and val < 1:  # XBRL 利率是小数 (0.045 = 4.5%)
                interest_rate = val
                break
            elif val >= 1 and val < 100:  # 某些公司用百分数 (4.5 = 4.5%)
                interest_rate = val / 100
                break

        # ── 推断 debt_type ──
        all_concepts = ' '.join(inst_facts['concept'].str.lower().tolist())
        name_lower = name.lower()
        combined = all_concepts + ' ' + name_lower
        if 'convertible' in combined:
            debt_type = 'convertible'
        elif 'lease' in combined:
            debt_type = 'lease'
        elif 'credit' in combined or 'revolv' in combined:
            debt_type = 'credit_facility'
        elif 'loan' in combined or 'termloan' in combined:
            debt_type = 'loan'
        else:
            debt_type = 'bond'

        results.append({
            "instrument_name": name,
            "debt_type": debt_type,
            "principal": principal,
            "interest_rate": interest_rate,
            "maturity_date": None,  # XBRL 通常不包含到期日
        })

    logger.info(f"[XBRL] 提取 {len(results)} 条债务")
    return results


def extract_geographic_revenues(df: pd.DataFrame) -> list[dict]:
    """从 XBRL 维度数据提取地理收入分布。"""
    results = []

    geo_col = 'dim_srt_StatementGeographicalAxis'
    if geo_col not in df.columns:
        return results

    geo = df[df[geo_col].notna() & df['numeric_value'].notna()].copy()
    if geo.empty:
        return results

    # 只要收入概念
    rev_mask = geo['concept'].str.contains('Revenue|Sales', case=False, na=False)
    geo_rev = geo[rev_mask]
    if geo_rev.empty:
        return results

    # 取最新 FY（兼容 fiscal_period 缺失的情况）
    if _safe_col(geo_rev, 'fiscal_period'):
        fy_mask = geo_rev['fiscal_period'] == 'FY'
        if fy_mask.any():
            geo_rev = geo_rev[fy_mask]

    if _safe_col(geo_rev, 'period_end'):
        latest_end = geo_rev['period_end'].max()
        geo_rev = geo_rev[geo_rev['period_end'] == latest_end]

    # 去重
    geo_rev = geo_rev.drop_duplicates(subset=geo_col, keep='first')

    # 只保留正值（负值可能是退货/调整）
    geo_rev = geo_rev[geo_rev['numeric_value'] > 0]
    total = geo_rev['numeric_value'].sum()
    if total <= 0:
        return results

    for _, row in geo_rev.iterrows():
        region = str(row[geo_col]).split(':')[-1].replace('Member', '').strip()
        results.append({
            "region": region,
            "revenue_share": round(row['numeric_value'] / total, 4),
        })

    logger.info(f"[XBRL] 提取 {len(results)} 个地理分部")
    return results


def extract_segment_hints(df: pd.DataFrame) -> list[str]:
    """从 XBRL 维度数据提取业务分部名称（供 LLM prompt 参考）。"""
    hints = []

    for col in ['dim_us-gaap_StatementBusinessSegmentsAxis', 'dim_srt_ProductOrServiceAxis']:
        if col not in df.columns:
            continue

        members = df[col].dropna().unique()
        for m in members:
            name = str(m).split(':')[-1].replace('Member', '').strip()
            if name and name not in hints and name.lower() not in ('product', 'service', 'segment'):
                hints.append(name)

        if hints:
            break

    return hints


def extract_audit_info(df: pd.DataFrame) -> dict | None:
    """从 XBRL 提取审计意见。

    AuditorName 是字符串类型的 fact，用 'value' 列（edgartools 统一存储）。
    """
    auditor_rows = df[df['concept'].str.endswith(':AuditorName')]
    if auditor_rows.empty:
        return None

    # edgartools 字符串 facts 存在 'value' 列
    row = auditor_rows.iloc[0]
    auditor_name = None
    for col in ['value', 'string_value']:
        if _safe_col(auditor_rows, col):
            val = str(row[col])
            if val and val != 'nan' and val != 'None':
                auditor_name = val
                break

    if not auditor_name:
        return None

    # 审计意见类型
    opinion_type = "unqualified"
    opinion_rows = df[df['concept'].str.contains('AuditorOpinion|AuditorsReportType', case=False, na=False)]
    if not opinion_rows.empty:
        op_row = opinion_rows.iloc[0]
        for col in ['value', 'string_value']:
            if _safe_col(opinion_rows, col):
                op_val = str(op_row[col]).lower()
                if 'adverse' in op_val:
                    opinion_type = 'adverse'
                elif 'disclaim' in op_val:
                    opinion_type = 'disclaimer'
                elif 'qualified' in op_val and 'unqualified' not in op_val:
                    opinion_type = 'qualified'
                break

    return {
        "opinion_type": opinion_type,
        "auditor_name": auditor_name,
        "emphasis_matters": None,
    }


# ── 主入口 ────────────────────────────────────────────────────────────

@dataclass
class XBRLData:
    """XBRL 提取结果。"""
    financial_line_items: list[dict] = field(default_factory=list)
    debt_obligations: list[dict] = field(default_factory=list)
    geographic_revenues: list[dict] = field(default_factory=list)
    audit_opinion: dict | None = None
    segment_hints: list[str] = field(default_factory=list)
    has_xbrl: bool = False
    error: str | None = None


def extract_xbrl(filing) -> XBRLData:
    """从 edgartools Filing 对象提取全部 XBRL 数据。

    Args:
        filing: edgartools Filing 对象

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

    try:
        df = xbrl.facts.to_dataframe()
    except Exception as e:
        result.error = f"XBRL facts 转 DataFrame 失败: {e}"
        logger.warning(f"[XBRL] {result.error}")
        return result

    if df.empty:
        result.error = "XBRL DataFrame 为空"
        logger.warning(f"[XBRL] {result.error}")
        return result

    # 验证必需列
    required_cols = ['concept', 'is_dimensioned', 'numeric_value', 'period_type']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        result.error = f"XBRL DataFrame 缺少列: {missing}"
        logger.warning(f"[XBRL] {result.error}, 可用列: {df.columns.tolist()}")
        return result

    result.has_xbrl = True
    logger.info(f"[XBRL] 开始提取结构化数据 ({len(df)} facts)")

    result.financial_line_items = extract_financial_line_items(df)
    result.debt_obligations = extract_debt_obligations(df)
    result.geographic_revenues = extract_geographic_revenues(df)
    result.audit_opinion = extract_audit_info(df)
    result.segment_hints = extract_segment_hints(df)

    logger.info(
        f"[XBRL] 提取完成: "
        f"{len(result.financial_line_items)} 财务科目, "
        f"{len(result.debt_obligations)} 债务, "
        f"{len(result.geographic_revenues)} 地理分部, "
        f"审计={'有' if result.audit_opinion else '无'}, "
        f"{len(result.segment_hints)} 分部提示"
    )
    return result
