"""
Company 域 Pydantic Schemas — LLM 输出校验
==========================================
"""

from __future__ import annotations

from typing import Annotated, Optional

from pydantic import BaseModel, BeforeValidator


def _coerce_float(v):
    """LLM 有时会把文本塞进数字字段，宽容处理为 None。"""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().rstrip("%").replace(",", "")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _coerce_int(v):
    """宽容 int 解析。"""
    if v is None:
        return None
    if isinstance(v, int):
        return v
    s = str(v).strip().replace(",", "")
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


LenientFloat = Annotated[Optional[float], BeforeValidator(_coerce_float)]
LenientInt = Annotated[Optional[int], BeforeValidator(_coerce_int)]


def _coerce_str(v):
    """LLM 有时会把数字/null 塞进字符串字段，宽容处理。"""
    if v is None:
        return ""
    return str(v)


LenientStr = Annotated[str, BeforeValidator(_coerce_str)]


# ── LLM 输出子模型 ─────────────────────────────────────────────────────


class ExtractedOperationalIssue(BaseModel):
    topic: str
    performance: Optional[str] = None
    attribution: Optional[str] = None
    risk: Optional[str] = None
    guidance: Optional[str] = None


class ExtractedNarrative(BaseModel):
    narrative: str
    capital_required: LenientFloat = None
    capital_unit: Optional[str] = None
    promised_outcome: Optional[str] = None
    deadline: Optional[str] = None  # YYYY-MM-DD or null


class ExtractedDownstream(BaseModel):
    segment: Optional[str] = None
    customer_name: str
    customer_type: Optional[str] = None
    products: Optional[str] = None
    channels: Optional[str] = None
    revenue: LenientFloat = None
    revenue_pct: LenientFloat = None
    growth_yoy: Optional[str] = None
    backlog: LenientFloat = None
    backlog_note: Optional[str] = None
    pricing_model: Optional[str] = None
    contract_duration: Optional[str] = None
    revenue_type: Optional[str] = None
    is_recurring: Optional[bool] = None
    recognition_method: Optional[str] = None
    contract_duration_months: Optional[int] = None
    switching_cost_level: Optional[str] = None
    product_category: Optional[str] = None         # beverage|commodity|cloud_infrastructure|insurance|...
    product_criticality: Optional[str] = None      # high|medium|low
    segment_gross_margin: LenientFloat = None      # 该业务线毛利率
    description: Optional[str] = None


class ExtractedUpstream(BaseModel):
    segment: Optional[str] = None
    supplier_name: str
    supply_type: str
    material_or_service: Optional[str] = None
    process_node: Optional[str] = None
    geographic_location: Optional[str] = None
    is_sole_source: bool = False
    purchase_obligation: LenientFloat = None
    lead_time: Optional[str] = None
    contract_type: Optional[str] = None
    prepaid_amount: LenientFloat = None
    concentration_risk: Optional[str] = None
    description: Optional[str] = None


class ExtractedGeographicRevenue(BaseModel):
    region: str
    revenue: LenientFloat = None
    revenue_share: LenientFloat = None
    growth_yoy: Optional[str] = None
    note: Optional[str] = None


class ExtractedNonFinancialKPI(BaseModel):
    kpi_name: str
    kpi_value: LenientStr = ""
    kpi_unit: Optional[str] = None
    yoy_change: Optional[str] = None
    category: Optional[str] = None
    note: Optional[str] = None


class ExtractedDebtObligation(BaseModel):
    instrument_name: str
    debt_type: str = "bond"
    principal: LenientFloat = None
    currency: str = "USD"
    interest_rate: LenientFloat = None
    maturity_date: Optional[str] = None
    is_secured: bool = False
    is_current: bool = False
    is_floating_rate: bool = False
    note: Optional[str] = None


class ExtractedLitigation(BaseModel):
    case_name: str
    case_type: str = "other"
    status: str = "pending"
    counterparty: Optional[str] = None
    filed_at: Optional[str] = None
    claimed_amount: LenientFloat = None
    accrued_amount: LenientFloat = None
    currency: str = "USD"
    description: Optional[str] = None


class ExtractedExecutiveCompensation(BaseModel):
    name: str
    title: str = ""
    role_type: str = "executive"
    base_salary: LenientFloat = None
    bonus: LenientFloat = None
    stock_awards: LenientFloat = None
    option_awards: LenientFloat = None
    non_equity_incentive: LenientFloat = None
    other_comp: LenientFloat = None
    total_comp: LenientFloat = None
    pay_ratio: LenientFloat = None
    median_employee_comp: LenientFloat = None


class ExtractedStockOwnership(BaseModel):
    name: str
    title: Optional[str] = None
    shares_beneficially_owned: LenientInt = None
    percent_of_class: LenientFloat = None


class ExtractedRelatedPartyTransaction(BaseModel):
    related_party: str
    relationship: str = "other"
    transaction_type: str = "other"
    amount: LenientFloat = None
    currency: str = "USD"
    terms: Optional[str] = None
    is_ongoing: bool = False
    description: Optional[str] = None


# ── Axion 新增表 schemas ──────────────────────────────────────────────


class ExtractedPricingAction(BaseModel):
    product_or_segment: str
    price_change_pct: LenientFloat = None
    volume_impact_pct: LenientFloat = None
    effective_date: Optional[str] = None


class ExtractedCompetitorRelation(BaseModel):
    competitor_name: str
    market_segment: Optional[str] = None
    relationship_type: str = "direct_competitor"


class ExtractedCompetitiveDynamic(BaseModel):
    """竞争动态事件 — 护城河检测核心数据"""
    competitor_name: str
    event_type: str = "product_launch"  # price_war|new_entry|exit|product_launch|patent_challenge|patent_expiration|regulatory_change|industry_downturn|migration_tool
    event_description: str = ""
    outcome_description: Optional[str] = None
    outcome_market_share_change: LenientFloat = None
    estimated_investment: LenientFloat = None
    event_date: Optional[str] = None


class ExtractedPeerFinancial(BaseModel):
    """同行财务指标 — 护城河同行对比"""
    peer_name: str
    metric: str = "gross_margin"  # gross_margin|operating_margin|net_margin|revenue
    value: LenientFloat = None
    period: Optional[str] = None
    segment: Optional[str] = None  # 对应本公司哪条业务线
    source: Optional[str] = None


class ExtractedMarketShareData(BaseModel):
    company_or_competitor: str
    market_segment: str
    share_pct: LenientFloat = None
    source_description: Optional[str] = None


class ExtractedKnownIssue(BaseModel):
    issue_description: str
    issue_category: str = "operational"
    severity: str = "major"
    source_type: str = "news"


class ExtractedManagementAcknowledgment(BaseModel):
    issue_description: str
    response_quality: str = "forthright"
    has_action_plan: bool = False


class ExtractedExecutiveChange(BaseModel):
    person_name: str
    title: Optional[str] = None
    change_type: str = "joined"
    change_date: Optional[str] = None
    reason: Optional[str] = None


class ExtractedAuditOpinion(BaseModel):
    opinion_type: str = "unqualified"
    auditor_name: Optional[str] = None
    emphasis_matters: Optional[str] = None


class ExtractedManagementGuidance(BaseModel):
    target_period: Optional[str] = None
    metric: str
    value_low: LenientFloat = None
    value_high: LenientFloat = None
    unit: str = "absolute"
    confidence_language: Optional[str] = None
    verbatim: Optional[str] = None


class ExtractedFinancialLineItem(BaseModel):
    statement_type: str = "income"  # income|balance_sheet|cashflow
    item_key: str
    item_label: str = ""
    value: LenientFloat = None
    parent_key: Optional[str] = None
    ordinal: LenientInt = 0
    note: Optional[str] = None


class ExtractedFinancialStatements(BaseModel):
    """三表财务数据（利润表、资产负债表、现金流量表）"""
    currency: str = "USD"
    income: list[ExtractedFinancialLineItem] = []
    balance_sheet: list[ExtractedFinancialLineItem] = []
    cashflow: list[ExtractedFinancialLineItem] = []


class ExtractedInventoryProvision(BaseModel):
    provision_amount: LenientFloat = None
    provision_release: LenientFloat = None
    net_margin_impact_pct: LenientFloat = None
    note: Optional[str] = None


class ExtractedDeferredRevenue(BaseModel):
    total_deferred: LenientFloat = None
    short_term: LenientFloat = None
    long_term: LenientFloat = None
    recognized_in_period: LenientFloat = None
    note: Optional[str] = None


class ExtractedRevenueRecognitionPolicy(BaseModel):
    category: str = ""  # product|software_license|subscription|service|NRE
    policy: Optional[str] = None
    key_judgments: Optional[str] = None


class ExtractedPurchaseObligationSummary(BaseModel):
    total_outstanding: LenientFloat = None
    inventory_purchase_obligations: LenientFloat = None
    non_inventory_obligations: LenientFloat = None
    cloud_service_agreements: LenientFloat = None
    breakdown_by_year: list[dict] = []  # [{"year":"FY2027","amount":1234}]
    note: Optional[str] = None


class ExtractedASPTrend(BaseModel):
    product_category: str = ""
    trend: Optional[str] = None
    driver: Optional[str] = None
    note: Optional[str] = None


class ExtractedRecurringRevenueBreakdown(BaseModel):
    recurring_revenue: LenientFloat = None
    recurring_pct: LenientFloat = None
    nonrecurring_revenue: LenientFloat = None
    nonrecurring_pct: LenientFloat = None
    note: Optional[str] = None


# ── 顶层 LLM 输出模型 ──────────────────────────────────────────────────


class CompanyProfile(BaseModel):
    """公司基本信息（LLM 输出）"""
    name: str = ""
    ticker: str = ""
    market: str = "us"
    industry: Optional[str] = None
    summary: Optional[str] = None


class CompanyExtractionResult(BaseModel):
    """Company 域 LLM 提取结果（全量）"""
    is_relevant_content: bool = True
    skip_reason: Optional[str] = None

    # 公司识别
    company: Optional[CompanyProfile] = None
    period: Optional[str] = ""  # "FY2025" / "2025Q4"

    # 财务三表
    financial_statements: Optional[ExtractedFinancialStatements] = None

    # Axion 新增表
    pricing_actions: list[ExtractedPricingAction] = []
    competitor_relations: list[ExtractedCompetitorRelation] = []
    competitive_dynamics: list[ExtractedCompetitiveDynamic] = []
    peer_financials: list[ExtractedPeerFinancial] = []
    market_share_data: list[ExtractedMarketShareData] = []
    known_issues: list[ExtractedKnownIssue] = []
    management_acknowledgments: list[ExtractedManagementAcknowledgment] = []
    executive_changes: list[ExtractedExecutiveChange] = []
    audit_opinion: Optional[ExtractedAuditOpinion] = None
    management_guidance: list[ExtractedManagementGuidance] = []

    # 业务表
    operational_issues: list[ExtractedOperationalIssue] = []
    narratives: list[ExtractedNarrative] = []
    downstream_segments: list[ExtractedDownstream] = []
    upstream_segments: list[ExtractedUpstream] = []
    geographic_revenues: list[ExtractedGeographicRevenue] = []
    non_financial_kpis: list[ExtractedNonFinancialKPI] = []
    debt_obligations: list[ExtractedDebtObligation] = []
    litigations: list[ExtractedLitigation] = []
    executive_compensations: list[ExtractedExecutiveCompensation] = []
    stock_ownership: list[ExtractedStockOwnership] = []
    related_party_transactions: list[ExtractedRelatedPartyTransaction] = []

    # 财务三表明细
    financial_line_items: list[ExtractedFinancialLineItem] = []

    # 新增 6 类
    inventory_provisions: list[ExtractedInventoryProvision] = []
    deferred_revenues: list[ExtractedDeferredRevenue] = []
    revenue_recognition_policies: list[ExtractedRevenueRecognitionPolicy] = []
    purchase_obligation_summaries: list[ExtractedPurchaseObligationSummary] = []
    asp_trends: list[ExtractedASPTrend] = []
    recurring_revenue_breakdowns: list[ExtractedRecurringRevenueBreakdown] = []

    # 摘要
    summary: Optional[str] = None
    one_liner: Optional[str] = None
