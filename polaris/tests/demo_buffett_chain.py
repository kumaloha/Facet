"""
巴菲特因果链 Demo
=================
用 mock 数据跑因果链，展示每一步的探针和判定。
"""

import pandas as pd

import polaris.features.l0.company  # noqa: F401
import polaris.features.l0.cross_period  # noqa: F401

from polaris.features.types import ComputeContext, FeatureLevel, FeatureResult
from polaris.features.registry import get_features
from polaris.principles.v1.buffett_chain import evaluate_buffett_chain, format_buffett_chain


def _fli(items, period="FY2025"):
    return pd.DataFrame([
        {"id": i, "statement_id": 1, "item_key": k, "item_label": k,
         "value": v, "parent_key": None, "ordinal": i, "note": None, "period": period}
        for i, (k, v) in enumerate(items.items())
    ])

def _df(rows, defaults):
    return pd.DataFrame([{**defaults, "id": i, **r} for i, r in enumerate(rows)])

EMPTY = pd.DataFrame()

FLI = {
    "FY2022": {"revenue": 28000, "cost_of_revenue": 9800, "operating_income": 12000,
        "net_income": 9500, "operating_cash_flow": 11000, "capital_expenditures": 1200,
        "depreciation_amortization": 1000, "shareholders_equity": 40000,
        "total_assets": 55000, "interest_expense": 300, "current_assets": 25000,
        "current_liabilities": 10000, "goodwill": 3000, "accounts_receivable": 4000,
        "inventory": 2000, "cash_and_equivalents": 12000, "total_debt": 5000,
        "dividends_paid": -2000, "share_repurchase": -3000, "sga_expense": 3000,
        "rnd_expense": 4000, "basic_weighted_average_shares": 1000,
        "income_tax_expense_total": 2500, "income_before_tax_total": 12000,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
    "FY2023": {"revenue": 32000, "cost_of_revenue": 11200, "operating_income": 14000,
        "net_income": 11000, "operating_cash_flow": 13000, "capital_expenditures": 1400,
        "depreciation_amortization": 1100, "shareholders_equity": 45000,
        "total_assets": 60000, "interest_expense": 280, "current_assets": 28000,
        "current_liabilities": 11000, "goodwill": 3200, "accounts_receivable": 4500,
        "inventory": 2100, "cash_and_equivalents": 14000, "total_debt": 4500,
        "dividends_paid": -2500, "share_repurchase": -3500, "sga_expense": 3200,
        "rnd_expense": 4500, "basic_weighted_average_shares": 980,
        "income_tax_expense_total": 3000, "income_before_tax_total": 14000,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
    "FY2024": {"revenue": 37000, "cost_of_revenue": 12600, "operating_income": 17000,
        "net_income": 13500, "operating_cash_flow": 16000, "capital_expenditures": 1600,
        "depreciation_amortization": 1200, "shareholders_equity": 52000,
        "total_assets": 68000, "interest_expense": 250, "current_assets": 32000,
        "current_liabilities": 12000, "goodwill": 3300, "accounts_receivable": 5000,
        "inventory": 2200, "cash_and_equivalents": 17000, "total_debt": 4000,
        "dividends_paid": -3000, "share_repurchase": -4000, "sga_expense": 3400,
        "rnd_expense": 5000, "basic_weighted_average_shares": 960,
        "income_tax_expense_total": 3500, "income_before_tax_total": 17000,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
    "FY2025": {"revenue": 43000, "cost_of_revenue": 14200, "operating_income": 20000,
        "net_income": 16000, "operating_cash_flow": 19000, "capital_expenditures": 1800,
        "depreciation_amortization": 1400, "shareholders_equity": 60000,
        "total_assets": 78000, "interest_expense": 220, "current_assets": 38000,
        "current_liabilities": 13000, "goodwill": 3500, "accounts_receivable": 5500,
        "inventory": 2400, "cash_and_equivalents": 20000, "total_debt": 3500,
        "dividends_paid": -3500, "share_repurchase": -5000, "sga_expense": 3600,
        "rnd_expense": 5500, "basic_weighted_average_shares": 940,
        "income_tax_expense_total": 4000, "income_before_tax_total": 20000,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
}


def build(fli_map, **overrides):
    all_fli = pd.concat([_fli(v, k) for k, v in fli_map.items()], ignore_index=True)
    ds_defaults = {"company_id": 1, "period": "FY2025", "segment": None,
        "customer_type": None, "products": None, "channels": None,
        "revenue": None, "growth_yoy": None, "backlog": None,
        "backlog_note": None, "pricing_model": None, "contract_duration": None,
        "recognition_method": None, "description": None,
        "raw_post_id": None, "created_at": "2025-01-01"}

    ctx = ComputeContext(company_id=1, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli_map["FY2025"]),
        "financial_line_items_all": all_fli,
        "downstream_segments": overrides.get("downstream", _df([
            {"customer_name": "A", "revenue_pct": 0.15, "is_recurring": True, "revenue_type": "subscription", "backlog": 5000},
            {"customer_name": "B", "revenue_pct": 0.12, "is_recurring": True, "revenue_type": "license", "backlog": 3000},
            {"customer_name": "C", "revenue_pct": 0.08, "is_recurring": True, "revenue_type": "subscription"},
            {"customer_name": "D", "revenue_pct": 0.06, "is_recurring": False, "revenue_type": "project"},
            {"customer_name": "Others", "revenue_pct": 0.59, "is_recurring": True, "revenue_type": "license"},
        ], ds_defaults)),
        "upstream_segments": _df([
            {"supplier_name": "TSMC", "is_sole_source": True, "geographic_location": "Taiwan"},
            {"supplier_name": "Samsung", "is_sole_source": False, "geographic_location": "South Korea"},
            {"supplier_name": "Intel", "is_sole_source": False, "geographic_location": "US"},
            {"supplier_name": "ASML", "is_sole_source": True, "geographic_location": "Netherlands"},
        ], {"company_id": 1, "period": "FY2025", "segment": None, "supply_type": "component",
            "material_or_service": None, "process_node": None, "purchase_obligation": None,
            "contract_type": None, "prepaid_amount": None, "concentration_risk": None,
            "description": None, "raw_post_id": None, "created_at": "2025-01-01"}),
        "geographic_revenues": EMPTY,
        "debt_obligations": EMPTY,
        "debt_obligations_all": EMPTY,
        "executive_compensations": _df([
            {"name": "CEO", "title": "CEO", "role_type": "CEO", "pay_ratio": 120.0, "stock_awards": 8000, "total_comp": 10000},
        ], {"company_id": 1, "period": "FY2025", "base_salary": None, "bonus": None,
            "option_awards": None, "non_equity_incentive": None, "other_comp": None,
            "currency": "USD", "median_employee_comp": None, "raw_post_id": None, "created_at": "2025-01-01"}),
        "stock_ownership": _df([
            {"name": "CEO", "title": "CEO", "percent_of_class": 8.0},
            {"name": "CFO", "title": "CFO", "percent_of_class": 2.0},
        ], {"company_id": 1, "period": "FY2025", "shares_beneficially_owned": None,
            "raw_post_id": None, "created_at": "2025-01-01"}),
        "company_narratives": _df([
            {"narrative": "Expand cloud", "status": "delivered"},
            {"narrative": "Enter new market", "status": "delivered"},
            {"narrative": "Reduce costs", "status": "delivered"},
            {"narrative": "Hire engineers", "status": "missed"},
            {"narrative": "Launch X", "status": "delivered"},
        ], {"company_id": 1, "raw_post_id": None, "capital_required": None,
            "capital_unit": None, "promised_outcome": None, "deadline": None,
            "reported_at": None, "created_at": "2025-01-01"}),
        "litigations": _df([
            {"status": "resolved", "accrued_amount": 50, "claimed_amount": 100,
             "case_name": "Case", "case_type": "civil"},
        ], {"company_id": 1, "counterparty": None, "filed_at": None, "currency": "USD",
            "description": None, "resolution": None, "resolved_at": None,
            "raw_post_id": None, "created_at": "2025-01-01"}),
        "operational_issues": EMPTY, "related_party_transactions": EMPTY,
        "non_financial_kpis": EMPTY, "pricing_actions": EMPTY,
        "market_share_data": EMPTY, "audit_opinions": EMPTY,
        "known_issues": EMPTY, "insider_transactions": EMPTY,
        "executive_changes": EMPTY, "equity_offerings": EMPTY,
        "analyst_estimates": EMPTY, "management_guidance": EMPTY,
        "management_acknowledgments": EMPTY,
    }
    return ctx


def compute_features(ctx):
    results = {}
    for level in (FeatureLevel.L0, FeatureLevel.L1, FeatureLevel.L2):
        for feat in get_features(level=level):
            try:
                r = feat.compute_fn(ctx)
            except Exception:
                continue
            if r is not None:
                ctx.features[feat.name] = r.value
                results[feat.name] = r
    return results


if __name__ == "__main__":
    # ── Case 1: GoodCorp ──
    print("\n" + "▓" * 60)
    print("  Case 1: GoodCorp (高毛利、轻资本、稳定增长)")
    print("▓" * 60)

    ctx = build(FLI)
    compute_features(ctx)
    result = evaluate_buffett_chain(ctx, market_context={
        "price": 135.0,
        "shares_outstanding": 940.0,
        "discount_rate": 0.045,
        "guidance": {"revenue_growth": 0.15},
    })
    print(format_buffett_chain(result))

    # ── Case 2: BadCorp (低毛利) ──
    print("▓" * 60)
    print("  Case 2: BadCorp (低毛利，链应断在第一环)")
    print("▓" * 60)

    bad_fli = {"FY2025": {
        "revenue": 10000, "cost_of_revenue": 8500, "operating_income": 500,
        "net_income": -200, "operating_cash_flow": 300, "capital_expenditures": 2000,
        "depreciation_amortization": 800, "shareholders_equity": 5000,
        "total_assets": 25000, "interest_expense": 1200, "current_assets": 6000,
        "current_liabilities": 8000, "goodwill": 8000, "accounts_receivable": 3000,
        "inventory": 4000, "cash_and_equivalents": 1500, "total_debt": 15000,
        "dividends_paid": 0, "share_repurchase": 0, "sga_expense": 2500,
        "rnd_expense": 500, "basic_weighted_average_shares": 500,
        "income_tax_expense_total": 100, "income_before_tax_total": -100,
        "proceeds_from_stock_issuance": 3000, "proceeds_from_debt_issuance": 5000,
    }}
    bad_ds = _df([
        {"customer_name": "MegaCorp", "revenue_pct": 0.55, "is_recurring": False, "revenue_type": "project"},
        {"customer_name": "Others", "revenue_pct": 0.45, "is_recurring": False, "revenue_type": "project"},
    ], {"company_id": 2, "period": "FY2025", "segment": None,
        "customer_type": None, "products": None, "channels": None,
        "revenue": None, "growth_yoy": None, "backlog": None,
        "backlog_note": None, "pricing_model": None, "contract_duration": None,
        "recognition_method": None, "description": None,
        "raw_post_id": None, "created_at": "2025-01-01"})

    ctx2 = build(bad_fli, downstream=bad_ds)
    compute_features(ctx2)
    result2 = evaluate_buffett_chain(ctx2)
    print(format_buffett_chain(result2))

    # ── Case 3: 高毛利但管理层烂 ──
    print("▓" * 60)
    print("  Case 3: ShadyCorp (高毛利但管理层烂，链应断在管理层)")
    print("▓" * 60)

    ctx3 = build(FLI)
    # 替换 narratives 为全失败
    ctx3._cache["company_narratives"] = _df([
        {"narrative": "Promise 1", "status": "missed"},
        {"narrative": "Promise 2", "status": "missed"},
        {"narrative": "Promise 3", "status": "abandoned"},
        {"narrative": "Promise 4", "status": "missed"},
        {"narrative": "Promise 5", "status": "missed"},
    ], {"company_id": 1, "raw_post_id": None, "capital_required": None,
        "capital_unit": None, "promised_outcome": None, "deadline": None,
        "reported_at": None, "created_at": "2025-01-01"})
    # 加大量关联交易
    ctx3._cache["related_party_transactions"] = _df([
        {"related_party": "CEO's company", "relationship": "officer",
         "transaction_type": "lease", "amount": 5000, "is_ongoing": True},
    ], {"company_id": 1, "period": "FY2025", "currency": "USD", "terms": None,
        "description": None, "raw_post_id": None, "created_at": "2025-01-01"})
    # 加诉讼
    ctx3._cache["litigations"] = _df([
        {"status": "pending", "accrued_amount": 500, "claimed_amount": 2000, "case_name": "Case 1", "case_type": "fraud"},
        {"status": "ongoing", "accrued_amount": 300, "claimed_amount": 1000, "case_name": "Case 2", "case_type": "civil"},
    ], {"company_id": 1, "counterparty": None, "filed_at": None, "currency": "USD",
        "description": None, "resolution": None, "resolved_at": None,
        "raw_post_id": None, "created_at": "2025-01-01"})

    compute_features(ctx3)
    result3 = evaluate_buffett_chain(ctx3, market_context={
        "price": 135.0, "shares_outstanding": 940.0,
        "discount_rate": 0.045, "guidance": {"revenue_growth": 0.15},
    })
    print(format_buffett_chain(result3))
