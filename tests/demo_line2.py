"""
线 2 (人和环境) · 全场景验证
============================
诚信 × 3 + 管理层人格 × 3 + 风险 × 3 = 9 case
"""

import pandas as pd

import polaris.features.l0.company  # noqa: F401
import polaris.features.l0.cross_period  # noqa: F401

from polaris.features.types import ComputeContext, FeatureLevel
from polaris.features.registry import get_features
from polaris.chains.integrity import assess_integrity, format_integrity
from polaris.chains.character import assess_character, format_character
from polaris.chains.risk import assess_risk, format_risk

EMPTY = pd.DataFrame()

EMPTY_TABLES = {k: EMPTY for k in [
    "downstream_segments", "upstream_segments", "geographic_revenues",
    "debt_obligations", "debt_obligations_all", "executive_compensations",
    "stock_ownership", "company_narratives", "litigations",
    "operational_issues", "related_party_transactions", "non_financial_kpis",
    "pricing_actions", "market_share_data", "audit_opinions", "known_issues",
    "insider_transactions", "executive_changes", "equity_offerings",
    "analyst_estimates", "management_guidance", "management_acknowledgments",
    "brand_signals", "competitive_dynamics", "peer_financials",
]}


def _fli(items, period="FY2025"):
    return pd.DataFrame([{"id": i, "statement_id": 1, "item_key": k, "item_label": k,
         "value": v, "parent_key": None, "ordinal": i, "note": None, "period": period}
        for i, (k, v) in enumerate(items.items())])

def _df(rows, defaults):
    if not rows: return EMPTY
    return pd.DataFrame([{**defaults, "id": i, **r} for i, r in enumerate(rows)])

GOOD_FLI = {"revenue": 50000, "cost_of_revenue": 20000, "operating_income": 18000,
    "net_income": 14000, "operating_cash_flow": 17000, "capital_expenditures": 2000,
    "depreciation_amortization": 1500, "shareholders_equity": 60000, "total_assets": 80000,
    "interest_expense": 200, "current_assets": 30000, "current_liabilities": 10000,
    "goodwill": 1000, "accounts_receivable": 3000, "inventory": 2000,
    "cash_and_equivalents": 20000, "total_debt": 3000, "dividends_paid": -5000,
    "share_repurchase": -3000, "sga_expense": 4000, "rnd_expense": 3000,
    "basic_weighted_average_shares": 1000, "income_tax_expense_total": 4000,
    "income_before_tax_total": 18000, "proceeds_from_stock_issuance": 0,
    "proceeds_from_debt_issuance": 0}


def make_ctx(fli=None, **extra):
    ctx = ComputeContext(company_id=1, period="FY2025")
    fli_data = fli or GOOD_FLI
    ctx._cache = {"financial_line_items": _fli(fli_data),
                  "financial_line_items_all": _fli(fli_data), **EMPTY_TABLES, **extra}
    for level in (FeatureLevel.L0, FeatureLevel.L1, FeatureLevel.L2):
        for feat in get_features(level=level):
            try:
                r = feat.compute_fn(ctx)
            except: continue
            if r: ctx.features[feat.name] = r.value
    return ctx


def banner(title, desc):
    print(f"\n{'─' * 65}")
    print(f"  {title}")
    print(f"  {desc}")
    print(f"{'─' * 65}")


# ══════════════════════════════════════════════════════════════
#  诚信
# ══════════════════════════════════════════════════════════════

def integrity_clean():
    banner("诚信 Case 1: 干净公司", "无问题，财务健康")
    ctx = make_ctx()
    print(format_integrity(assess_integrity(ctx)))

def integrity_hiding():
    banner("诚信 Case 2: 有问题且在藏", "第三方发现 3 个问题，管理层只承认 1 个")
    ctx = make_ctx(
        known_issues=_df([
            {"issue_description": "供应链瓶颈导致延迟交货", "severity": "major",
             "source_type": "news", "issue_category": "operational", "period": "FY2025"},
            {"issue_description": "欧盟反垄断调查", "severity": "critical",
             "source_type": "news", "issue_category": "regulatory", "period": "FY2025"},
            {"issue_description": "核心工程师团队离职", "severity": "major",
             "source_type": "analyst_report", "issue_category": "operational", "period": "FY2025"},
        ], {"company_id": 1, "period": "FY2025", "raw_post_id": None, "created_at": "2025-01-01"}),
        management_acknowledgments=_df([
            {"issue_description": "供应链有一些挑战正在解决", "response_quality": "downplay",
             "has_action_plan": True, "known_issue_id": None, "period": "FY2025"},
        ], {"company_id": 1, "period": "FY2025", "raw_post_id": None, "created_at": "2025-01-01"}),
    )
    print(format_integrity(assess_integrity(ctx)))

def integrity_audit_fail():
    banner("诚信 Case 3: 审计非标", "保留意见 → 一票否决")
    ctx = make_ctx(
        audit_opinions=_df([
            {"opinion_type": "qualified", "auditor_name": "普华永道",
             "emphasis_matters": "对持续经营能力存在重大疑虑", "period": "FY2025"},
        ], {"company_id": 1, "period": "FY2025", "raw_post_id": None, "created_at": "2025-01-01"}),
    )
    print(format_integrity(assess_integrity(ctx)))


# ══════════════════════════════════════════════════════════════
#  管理层人格
# ══════════════════════════════════════════════════════════════

def character_strong():
    banner("人格 Case 1: 坚定型管理层", "高兑现率 + 理性配置 + 高持股")
    ctx = make_ctx(
        company_narratives=_df([
            {"narrative": "成为行业标准", "status": "delivered"},
            {"narrative": "拓展海外市场", "status": "delivered"},
            {"narrative": "研发下一代产品", "status": "delivered"},
            {"narrative": "提升客户满意度", "status": "delivered"},
            {"narrative": "降本增效", "status": "missed"},
        ], {"company_id": 1, "raw_post_id": None, "capital_required": None,
            "capital_unit": None, "promised_outcome": None, "deadline": None,
            "reported_at": None, "created_at": "2025-01-01"}),
        stock_ownership=_df([
            {"name": "CEO", "title": "CEO", "percent_of_class": 12.0},
            {"name": "CFO", "title": "CFO", "percent_of_class": 3.0},
        ], {"company_id": 1, "period": "FY2025", "shares_beneficially_owned": None,
            "raw_post_id": None, "created_at": "2025-01-01"}),
        executive_compensations=_df([
            {"name": "CEO", "title": "CEO", "role_type": "CEO", "pay_ratio": 80.0,
             "stock_awards": 5000, "total_comp": 8000},
        ], {"company_id": 1, "period": "FY2025", "base_salary": None, "bonus": None,
            "option_awards": None, "non_equity_incentive": None, "other_comp": None,
            "currency": "USD", "median_employee_comp": None, "raw_post_id": None,
            "created_at": "2025-01-01"}),
    )
    print(format_character(assess_character(ctx)))

def character_weak():
    banner("人格 Case 2: 存疑型管理层", "低兑现率 + 薪酬失控 + 关联交易")
    ctx = make_ctx(
        company_narratives=_df([
            {"narrative": "承诺 A", "status": "missed"},
            {"narrative": "承诺 B", "status": "missed"},
            {"narrative": "承诺 C", "status": "abandoned"},
            {"narrative": "承诺 D", "status": "missed"},
            {"narrative": "承诺 E", "status": "delivered"},
        ], {"company_id": 1, "raw_post_id": None, "capital_required": None,
            "capital_unit": None, "promised_outcome": None, "deadline": None,
            "reported_at": None, "created_at": "2025-01-01"}),
        executive_compensations=_df([
            {"name": "CEO", "title": "CEO", "role_type": "CEO", "pay_ratio": 600.0,
             "stock_awards": 10000, "total_comp": 15000},
        ], {"company_id": 1, "period": "FY2025", "base_salary": None, "bonus": None,
            "option_awards": None, "non_equity_incentive": None, "other_comp": None,
            "currency": "USD", "median_employee_comp": None, "raw_post_id": None,
            "created_at": "2025-01-01"}),
        related_party_transactions=_df([
            {"related_party": "CEO 关联方", "relationship": "officer",
             "transaction_type": "lease", "amount": 4000, "is_ongoing": True},
        ], {"company_id": 1, "period": "FY2025", "currency": "USD", "terms": None,
            "description": None, "raw_post_id": None, "created_at": "2025-01-01"}),
    )
    print(format_character(assess_character(ctx)))

def character_no_data():
    banner("人格 Case 3: 无数据", "没有任何管理层数据")
    ctx = make_ctx()
    print(format_character(assess_character(ctx)))


# ══════════════════════════════════════════════════════════════
#  风险
# ══════════════════════════════════════════════════════════════

def risk_tsmc():
    banner("风险 Case 1: 类台积电", "台湾地缘风险 + 客户集中")
    ctx = make_ctx(
        geographic_revenues=_df([
            {"region": "Taiwan", "revenue_share": 0.10},
            {"region": "China", "revenue_share": 0.25},
            {"region": "United States", "revenue_share": 0.35},
            {"region": "Other", "revenue_share": 0.30},
        ], {"company_id": 1, "period": "FY2025", "revenue": None, "growth_yoy": None,
            "note": None, "raw_post_id": None, "created_at": "2025-01-01"}),
        downstream_segments=_df([
            {"customer_name": "Apple", "revenue_pct": 0.25, "is_recurring": True, "revenue_type": "license"},
            {"customer_name": "NVIDIA", "revenue_pct": 0.12, "is_recurring": True, "revenue_type": "license"},
            {"customer_name": "Others", "revenue_pct": 0.63, "is_recurring": True, "revenue_type": "license"},
        ], {"company_id": 1, "period": "FY2025", "segment": None, "customer_type": None,
            "products": None, "channels": None, "revenue": None, "growth_yoy": None,
            "backlog": None, "backlog_note": None, "pricing_model": None,
            "contract_duration": None, "recognition_method": None, "description": None,
            "raw_post_id": None, "created_at": "2025-01-01"}),
    )
    print(format_risk(assess_risk(ctx)))

def risk_safe():
    banner("风险 Case 2: 低风险公司", "美国本土，客户分散，无债务")
    ctx = make_ctx(
        geographic_revenues=_df([
            {"region": "United States", "revenue_share": 0.80},
            {"region": "Europe", "revenue_share": 0.20},
        ], {"company_id": 1, "period": "FY2025", "revenue": None, "growth_yoy": None,
            "note": None, "raw_post_id": None, "created_at": "2025-01-01"}),
    )
    print(format_risk(assess_risk(ctx)))

def risk_catastrophic():
    banner("风险 Case 3: 多重灾难性风险", "高风险地区+极度客户集中+高杠杆")
    sick_fli = dict(GOOD_FLI)
    sick_fli["total_debt"] = 350000
    sick_fli["interest_expense"] = 20000
    ctx = make_ctx(fli=sick_fli,
        geographic_revenues=_df([
            {"region": "Russia", "revenue_share": 0.60},
            {"region": "Other", "revenue_share": 0.40},
        ], {"company_id": 1, "period": "FY2025", "revenue": None, "growth_yoy": None,
            "note": None, "raw_post_id": None, "created_at": "2025-01-01"}),
        downstream_segments=_df([
            {"customer_name": "单一大客户", "revenue_pct": 0.70, "is_recurring": False, "revenue_type": "project"},
            {"customer_name": "Others", "revenue_pct": 0.30, "is_recurring": False, "revenue_type": "project"},
        ], {"company_id": 1, "period": "FY2025", "segment": None, "customer_type": None,
            "products": None, "channels": None, "revenue": None, "growth_yoy": None,
            "backlog": None, "backlog_note": None, "pricing_model": None,
            "contract_duration": None, "recognition_method": None, "description": None,
            "raw_post_id": None, "created_at": "2025-01-01"}),
    )
    print(format_risk(assess_risk(ctx)))


def risk_litigation():
    banner("风险 Case 4: 类强生", "巨额诉讼，索赔/权益 > 50%")
    ctx = make_ctx(
        litigations=_df([
            {"status": "pending", "accrued_amount": 5000, "claimed_amount": 20000,
             "case_name": "产品责任诉讼 A", "case_type": "product_liability"},
            {"status": "ongoing", "accrued_amount": 3000, "claimed_amount": 15000,
             "case_name": "集体诉讼 B", "case_type": "class_action"},
            {"status": "pending", "accrued_amount": 1000, "claimed_amount": 8000,
             "case_name": "环境诉讼 C", "case_type": "environmental"},
        ], {"company_id": 1, "counterparty": None, "filed_at": None, "currency": "USD",
            "description": None, "resolution": None, "resolved_at": None,
            "raw_post_id": None, "created_at": "2025-01-01"}),
    )
    print(format_risk(assess_risk(ctx)))


def risk_airline():
    banner("风险 Case 5: 航空公司", "黑天鹅高暴露行业")
    ctx = make_ctx(
        downstream_segments=_df([
            {"customer_name": "国内航线", "revenue_pct": 0.60, "is_recurring": False,
             "revenue_type": "ticket", "product_category": "airline"},
            {"customer_name": "国际航线", "revenue_pct": 0.40, "is_recurring": False,
             "revenue_type": "ticket", "product_category": "airline"},
        ], {"company_id": 1, "period": "FY2025", "segment": None, "customer_type": None,
            "products": None, "channels": None, "revenue": None, "growth_yoy": None,
            "backlog": None, "backlog_note": None, "pricing_model": None,
            "contract_duration": None, "recognition_method": None, "description": None,
            "raw_post_id": None, "created_at": "2025-01-01"}),
    )
    print(format_risk(assess_risk(ctx)))


def risk_education_ban():
    banner("风险 Case 6: 类好未来", "双减政策灭顶")
    ctx = make_ctx(
        competitive_dynamics=_df([
            {"competitor_name": "行业", "event_type": "regulatory_change",
             "event_description": "双减政策: 禁止学科类培训机构上市融资",
             "outcome_description": "行业收入归零",
             "outcome_market_share_change": None},
        ], {"company_id": 1, "estimated_investment": None,
            "outcome_market_share_change": None, "event_date": "2021-07",
            "raw_post_id": None, "created_at": "2025-01-01"}),
    )
    print(format_risk(assess_risk(ctx)))


def risk_currency():
    banner("风险 Case 7: 新兴市场货币", "收入依赖货币高波动地区")
    ctx = make_ctx(
        geographic_revenues=_df([
            {"region": "Turkey", "revenue_share": 0.30},
            {"region": "Argentina", "revenue_share": 0.25},
            {"region": "Brazil", "revenue_share": 0.20},
            {"region": "Other", "revenue_share": 0.25},
        ], {"company_id": 1, "period": "FY2025", "revenue": None, "growth_yoy": None,
            "note": None, "raw_post_id": None, "created_at": "2025-01-01"}),
    )
    print(format_risk(assess_risk(ctx)))


if __name__ == "__main__":
    print("=" * 65)
    print("  线 2 (人和环境) · 全场景验证")
    print("=" * 65)

    integrity_clean()
    integrity_hiding()
    integrity_audit_fail()

    character_strong()
    character_weak()
    character_no_data()

    risk_tsmc()
    risk_safe()
    risk_catastrophic()
    risk_litigation()
    risk_airline()
    risk_education_ban()
    risk_currency()

    print("\n" + "=" * 65)
    print("  验证完成")
    print("=" * 65)
