"""
护城河检测 Demo
===============
4 个 mock 公司，展示不同护城河类型的检测结果。

Case 1: BrandCorp — 有提价记录+份额稳定 → 无形资产(品牌)
Case 2: StickyCorpSaaS — 订阅模式+长合同+高留存 → 转换成本
Case 3: CommodityCorp — 低毛利+亏损 → 伪护城河/无护城河
Case 4: GoodCorp — 高毛利稳定+经常性收入，但无行为数据 → 间接信号
"""

import pandas as pd

import polaris.features.l0.company  # noqa: F401
import polaris.features.l0.cross_period  # noqa: F401

from polaris.features.types import ComputeContext, FeatureLevel
from polaris.features.registry import get_features
from polaris.chains.moat import assess_moat, format_moat

EMPTY = pd.DataFrame()


def _fli(items, period="FY2025"):
    return pd.DataFrame([
        {"id": i, "statement_id": 1, "item_key": k, "item_label": k,
         "value": v, "parent_key": None, "ordinal": i, "note": None, "period": period}
        for i, (k, v) in enumerate(items.items())
    ])


def _df(rows, defaults):
    if not rows:
        return EMPTY
    return pd.DataFrame([{**defaults, "id": i, **r} for i, r in enumerate(rows)])


DS_DEFAULTS = {
    "company_id": 1, "period": "FY2025", "segment": None,
    "customer_type": None, "products": None, "channels": None,
    "revenue": None, "growth_yoy": None, "backlog": None,
    "backlog_note": None, "pricing_model": None, "contract_duration": None,
    "recognition_method": None, "description": None,
    "raw_post_id": None, "created_at": "2025-01-01",
}

PA_DEFAULTS = {
    "company_id": 1, "raw_post_id": None, "created_at": "2025-01-01",
}

MS_DEFAULTS = {
    "company_id": 1, "raw_post_id": None, "created_at": "2025-01-01",
}

EMPTY_TABLES = {
    "upstream_segments": EMPTY, "geographic_revenues": EMPTY,
    "debt_obligations": EMPTY, "debt_obligations_all": EMPTY,
    "executive_compensations": EMPTY, "stock_ownership": EMPTY,
    "company_narratives": EMPTY, "litigations": EMPTY,
    "operational_issues": EMPTY, "related_party_transactions": EMPTY,
    "non_financial_kpis": EMPTY, "audit_opinions": EMPTY,
    "known_issues": EMPTY, "insider_transactions": EMPTY,
    "executive_changes": EMPTY, "equity_offerings": EMPTY,
    "analyst_estimates": EMPTY, "management_guidance": EMPTY,
    "management_acknowledgments": EMPTY,
}


def compute_features(ctx):
    for level in (FeatureLevel.L0, FeatureLevel.L1, FeatureLevel.L2):
        for feat in get_features(level=level):
            try:
                r = feat.compute_fn(ctx)
            except Exception:
                continue
            if r is not None:
                ctx.features[feat.name] = r.value


# ══════════════════════════════════════════════════════════════
#  Case 1: BrandCorp — 品牌护城河（涨价+份额涨）
# ══════════════════════════════════════════════════════════════

def case_brand():
    print("\n" + "▓" * 60)
    print("  Case 1: BrandCorp (类茅台)")
    print("  有提价记录 + 提价后市场份额上升")
    print("▓" * 60)

    fli = {
        "revenue": 50_000, "cost_of_revenue": 10_000,
        "operating_income": 25_000, "net_income": 20_000,
        "operating_cash_flow": 23_000, "capital_expenditures": 2_000,
        "depreciation_amortization": 1_500,
        "shareholders_equity": 80_000, "total_assets": 100_000,
        "interest_expense": 100, "current_assets": 40_000,
        "current_liabilities": 10_000, "goodwill": 1_000,
        "accounts_receivable": 3_000, "inventory": 5_000,
        "cash_and_equivalents": 25_000, "total_debt": 2_000,
        "dividends_paid": -10_000, "share_repurchase": -5_000,
        "sga_expense": 5_000, "rnd_expense": 1_000,
        "basic_weighted_average_shares": 1_000,
        "income_tax_expense_total": 5_000, "income_before_tax_total": 25_000,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
    }

    ctx = ComputeContext(company_id=1, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli),
        "financial_line_items_all": _fli(fli),
        "downstream_segments": _df([
            {"customer_name": "Retail", "revenue_pct": 0.60, "is_recurring": True, "revenue_type": "recurring"},
            {"customer_name": "Wholesale", "revenue_pct": 0.40, "is_recurring": True, "revenue_type": "recurring"},
        ], DS_DEFAULTS),
        # 关键: 有提价记录
        "pricing_actions": _df([
            {"action": "提价 8%", "date": "2023-06", "product": "核心产品", "result": "销量未受影响"},
            {"action": "提价 12%", "date": "2024-01", "product": "核心产品", "result": "销量增长 5%"},
        ], PA_DEFAULTS),
        # 关键: 提价后市场份额上升
        "market_share_data": _df([
            {"period": "FY2023", "share": 0.32, "source": "行业报告"},
            {"period": "FY2024", "share": 0.35, "source": "行业报告"},
            {"period": "FY2025", "share": 0.37, "source": "行业报告"},
        ], MS_DEFAULTS),
        **EMPTY_TABLES,
    }

    compute_features(ctx)
    result = assess_moat(ctx)
    print(format_moat(result))


# ══════════════════════════════════════════════════════════════
#  Case 2: StickyCorp — 转换成本护城河
# ══════════════════════════════════════════════════════════════

def case_switching_cost():
    print("▓" * 60)
    print("  Case 2: StickyCorp (类 Salesforce/Oracle)")
    print("  订阅模式 + 长合同 + 高经常性收入")
    print("▓" * 60)

    fli = {
        "revenue": 30_000, "cost_of_revenue": 9_000,
        "operating_income": 10_000, "net_income": 8_000,
        "operating_cash_flow": 11_000, "capital_expenditures": 2_000,
        "depreciation_amortization": 1_500,
        "shareholders_equity": 50_000, "total_assets": 70_000,
        "interest_expense": 200, "current_assets": 25_000,
        "current_liabilities": 10_000, "goodwill": 5_000,
        "accounts_receivable": 4_000, "inventory": 500,
        "cash_and_equivalents": 15_000, "total_debt": 5_000,
        "dividends_paid": -2_000, "share_repurchase": -3_000,
        "sga_expense": 5_000, "rnd_expense": 6_000,
        "basic_weighted_average_shares": 800,
        "income_tax_expense_total": 2_000, "income_before_tax_total": 10_000,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
    }

    ctx = ComputeContext(company_id=2, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli),
        "financial_line_items_all": _fli(fli),
        "downstream_segments": _df([
            {"customer_name": "Enterprise A", "revenue_pct": 0.10,
             "is_recurring": True, "revenue_type": "subscription",
             "contract_duration": "3 years"},
            {"customer_name": "Enterprise B", "revenue_pct": 0.08,
             "is_recurring": True, "revenue_type": "saas",
             "contract_duration": "5 years"},
            {"customer_name": "Enterprise C", "revenue_pct": 0.07,
             "is_recurring": True, "revenue_type": "subscription",
             "contract_duration": "2 years"},
            {"customer_name": "SMB", "revenue_pct": 0.75,
             "is_recurring": True, "revenue_type": "saas"},
        ], DS_DEFAULTS),
        "pricing_actions": EMPTY,
        "market_share_data": EMPTY,
        **EMPTY_TABLES,
    }

    compute_features(ctx)
    result = assess_moat(ctx)
    print(format_moat(result))


# ══════════════════════════════════════════════════════════════
#  Case 3: CommodityCorp — 无护城河 + 伪护城河
# ══════════════════════════════════════════════════════════════

def case_no_moat():
    print("▓" * 60)
    print("  Case 3: CommodityCorp (烧钱补贴型)")
    print("  低毛利 + 亏损 + 大量融资 → 伪护城河")
    print("▓" * 60)

    fli = {
        "revenue": 20_000, "cost_of_revenue": 18_000,
        "operating_income": -500, "net_income": -1_500,
        "operating_cash_flow": -800, "capital_expenditures": 3_000,
        "depreciation_amortization": 1_000,
        "shareholders_equity": 8_000, "total_assets": 30_000,
        "interest_expense": 800, "current_assets": 8_000,
        "current_liabilities": 12_000, "goodwill": 2_000,
        "accounts_receivable": 5_000, "inventory": 6_000,
        "cash_and_equivalents": 3_000, "total_debt": 15_000,
        "dividends_paid": 0, "share_repurchase": 0,
        "sga_expense": 3_000, "rnd_expense": 500,
        "basic_weighted_average_shares": 500,
        "income_tax_expense_total": 0, "income_before_tax_total": -1_500,
        "proceeds_from_stock_issuance": 5_000, "proceeds_from_debt_issuance": 8_000,
    }

    ctx = ComputeContext(company_id=3, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli),
        "financial_line_items_all": _fli(fli),
        "downstream_segments": _df([
            {"customer_name": "Mass Market", "revenue_pct": 1.0,
             "is_recurring": False, "revenue_type": "project"},
        ], DS_DEFAULTS),
        "pricing_actions": EMPTY,
        "market_share_data": EMPTY,
        **EMPTY_TABLES,
    }

    compute_features(ctx)
    result = assess_moat(ctx)
    print(format_moat(result))


# ══════════════════════════════════════════════════════════════
#  Case 4: GoodCorp — 有间接信号但无行为数据
# ══════════════════════════════════════════════════════════════

def case_indirect_only():
    print("▓" * 60)
    print("  Case 4: GoodCorp (高毛利稳定，但无行为数据)")
    print("  毛利率 67% + 稳定 + 经常性收入 80%")
    print("  但没有提价记录、没有市场份额数据")
    print("▓" * 60)

    fli_map = {
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

    all_fli = pd.concat([_fli(v, k) for k, v in fli_map.items()], ignore_index=True)

    ctx = ComputeContext(company_id=4, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli_map["FY2025"]),
        "financial_line_items_all": all_fli,
        "downstream_segments": _df([
            {"customer_name": "A", "revenue_pct": 0.15, "is_recurring": True, "revenue_type": "subscription"},
            {"customer_name": "B", "revenue_pct": 0.12, "is_recurring": True, "revenue_type": "license"},
            {"customer_name": "C", "revenue_pct": 0.08, "is_recurring": True, "revenue_type": "subscription"},
            {"customer_name": "D", "revenue_pct": 0.06, "is_recurring": False, "revenue_type": "project"},
            {"customer_name": "Others", "revenue_pct": 0.59, "is_recurring": True, "revenue_type": "license"},
        ], DS_DEFAULTS),
        # 关键: 没有行为数据
        "pricing_actions": EMPTY,
        "market_share_data": EMPTY,
        **EMPTY_TABLES,
    }

    compute_features(ctx)
    result = assess_moat(ctx)
    print(format_moat(result))


if __name__ == "__main__":
    case_brand()
    case_switching_cost()
    case_no_moat()
    case_indirect_only()
