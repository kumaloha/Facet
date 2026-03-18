"""
利润分配检测 · 全场景验证
"""

import pandas as pd

import polaris.features.l0.company  # noqa: F401
import polaris.features.l0.cross_period  # noqa: F401

from polaris.features.types import ComputeContext, FeatureLevel
from polaris.features.registry import get_features
from polaris.chains.distribution import assess_distribution, format_distribution

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
    return pd.DataFrame([
        {"id": i, "statement_id": 1, "item_key": k, "item_label": k,
         "value": v, "parent_key": None, "ordinal": i, "note": None, "period": period}
        for i, (k, v) in enumerate(items.items())
    ])


def run(name, desc, fli_map, **extra_tables):
    print(f"\n{'─' * 65}")
    print(f"  {name}")
    print(f"  {desc}")
    print(f"{'─' * 65}")
    all_fli = pd.concat([_fli(v, k) for k, v in fli_map.items()], ignore_index=True)
    last_period = list(fli_map.keys())[-1]
    ctx = ComputeContext(company_id=1, period=last_period)
    ctx._cache = {
        "financial_line_items": _fli(fli_map[last_period]),
        "financial_line_items_all": all_fli,
        **EMPTY_TABLES,
        **extra_tables,
    }
    for level in (FeatureLevel.L0, FeatureLevel.L1, FeatureLevel.L2):
        for feat in get_features(level=level):
            try:
                r = feat.compute_fn(ctx)
            except Exception:
                continue
            if r is not None:
                ctx.features[feat.name] = r.value
    result = assess_distribution(ctx)
    print(format_distribution(result))


# ══════════════════════════════════════════════════════════════

def case_generous():
    """类茅台: 大方分红 + 留存回报高"""
    run("Case 1: 类茅台 (慷慨分红)", "大比例分红 + 留存 ROIC 高", {
        "FY2024": {"revenue": 1650, "cost_of_revenue": 230, "operating_income": 1020, "net_income": 850,
            "operating_cash_flow": 920, "capital_expenditures": 70, "depreciation_amortization": 50,
            "shareholders_equity": 2200, "total_assets": 3100, "interest_expense": 2,
            "current_assets": 1500, "current_liabilities": 500, "goodwill": 0,
            "accounts_receivable": 40, "inventory": 240, "cash_and_equivalents": 950,
            "total_debt": 0, "dividends_paid": -420, "share_repurchase": 0,
            "sga_expense": 120, "rnd_expense": 30, "basic_weighted_average_shares": 1256,
            "income_tax_expense_total": 250, "income_before_tax_total": 1100,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
        "FY2025": {"revenue": 1900, "cost_of_revenue": 260, "operating_income": 1200, "net_income": 1000,
            "operating_cash_flow": 1080, "capital_expenditures": 80, "depreciation_amortization": 55,
            "shareholders_equity": 2700, "total_assets": 3700, "interest_expense": 1,
            "current_assets": 1800, "current_liabilities": 550, "goodwill": 0,
            "accounts_receivable": 45, "inventory": 260, "cash_and_equivalents": 1200,
            "total_debt": 0, "dividends_paid": -500, "share_repurchase": 0,
            "sga_expense": 130, "rnd_expense": 35, "basic_weighted_average_shares": 1256,
            "income_tax_expense_total": 300, "income_before_tax_total": 1300,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
    })


def case_buyback():
    """类苹果: 大量回购 + 少量分红"""
    run("Case 2: 类苹果 (回购为主)", "大量回购缩股 + 适度分红", {
        "FY2024": {"revenue": 380000, "cost_of_revenue": 210000, "operating_income": 120000, "net_income": 95000,
            "operating_cash_flow": 110000, "capital_expenditures": 10000, "depreciation_amortization": 12000,
            "shareholders_equity": 60000, "total_assets": 350000, "interest_expense": 3500,
            "current_assets": 140000, "current_liabilities": 150000, "goodwill": 0,
            "accounts_receivable": 30000, "inventory": 6000, "cash_and_equivalents": 60000,
            "total_debt": 100000, "dividends_paid": -15000, "share_repurchase": -80000,
            "sga_expense": 25000, "rnd_expense": 27000, "basic_weighted_average_shares": 15500,
            "income_tax_expense_total": 18000, "income_before_tax_total": 113000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
        "FY2025": {"revenue": 400000, "cost_of_revenue": 220000, "operating_income": 125000, "net_income": 100000,
            "operating_cash_flow": 115000, "capital_expenditures": 11000, "depreciation_amortization": 13000,
            "shareholders_equity": 55000, "total_assets": 360000, "interest_expense": 3800,
            "current_assets": 145000, "current_liabilities": 155000, "goodwill": 0,
            "accounts_receivable": 32000, "inventory": 6500, "cash_and_equivalents": 62000,
            "total_debt": 105000, "dividends_paid": -16000, "share_repurchase": -90000,
            "sga_expense": 26000, "rnd_expense": 29000, "basic_weighted_average_shares": 15000,
            "income_tax_expense_total": 19000, "income_before_tax_total": 119000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
    })


def case_hoarder():
    """囤现金型: 赚钱但不分不投"""
    run("Case 3: 囤现金 (不分不投)", "利润好但现金堆积，不分红不回购", {
        "FY2025": {"revenue": 5000, "cost_of_revenue": 2500, "operating_income": 1500, "net_income": 1200,
            "operating_cash_flow": 1400, "capital_expenditures": 200, "depreciation_amortization": 150,
            "shareholders_equity": 15000, "total_assets": 20000, "interest_expense": 10,
            "current_assets": 12000, "current_liabilities": 2000, "goodwill": 0,
            "accounts_receivable": 500, "inventory": 300, "cash_and_equivalents": 10000,
            "total_debt": 0, "dividends_paid": 0, "share_repurchase": 0,
            "sga_expense": 500, "rnd_expense": 200, "basic_weighted_average_shares": 500,
            "income_tax_expense_total": 300, "income_before_tax_total": 1500,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
    })


def case_empire():
    """帝国建设: 利润全砸收购，商誉暴涨"""
    run("Case 4: 帝国建设 (疯狂收购)", "利润不错但全砸去收购", {
        "FY2024": {"revenue": 15000, "cost_of_revenue": 9000, "operating_income": 3500, "net_income": 2800,
            "operating_cash_flow": 3200, "capital_expenditures": 500, "depreciation_amortization": 400,
            "shareholders_equity": 12000, "total_assets": 25000, "interest_expense": 300,
            "current_assets": 8000, "current_liabilities": 5000, "goodwill": 5000,
            "accounts_receivable": 2000, "inventory": 1500, "cash_and_equivalents": 3000,
            "total_debt": 6000, "dividends_paid": 0, "share_repurchase": 0,
            "sga_expense": 1500, "rnd_expense": 500, "basic_weighted_average_shares": 800,
            "income_tax_expense_total": 600, "income_before_tax_total": 3400,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 3000},
        "FY2025": {"revenue": 18000, "cost_of_revenue": 11000, "operating_income": 3800, "net_income": 3000,
            "operating_cash_flow": 3500, "capital_expenditures": 600, "depreciation_amortization": 500,
            "shareholders_equity": 14000, "total_assets": 35000, "interest_expense": 500,
            "current_assets": 9000, "current_liabilities": 6000, "goodwill": 12000,
            "accounts_receivable": 2500, "inventory": 1800, "cash_and_equivalents": 2500,
            "total_debt": 10000, "dividends_paid": 0, "share_repurchase": 0,
            "sga_expense": 1800, "rnd_expense": 600, "basic_weighted_average_shares": 800,
            "income_tax_expense_total": 700, "income_before_tax_total": 3700,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 5000},
    })


def case_diluter():
    """疯狂增发: 赚钱但大量增发稀释股东"""
    run("Case 5: 疯狂增发 (稀释股东)", "有利润但持续增发，股东越来越薄", {
        "FY2024": {"revenue": 8000, "cost_of_revenue": 5000, "operating_income": 1800, "net_income": 1400,
            "operating_cash_flow": 1600, "capital_expenditures": 300, "depreciation_amortization": 200,
            "shareholders_equity": 6000, "total_assets": 10000, "interest_expense": 100,
            "current_assets": 4000, "current_liabilities": 2000, "goodwill": 500,
            "accounts_receivable": 800, "inventory": 600, "cash_and_equivalents": 2000,
            "total_debt": 2000, "dividends_paid": -100, "share_repurchase": 0,
            "sga_expense": 800, "rnd_expense": 400, "basic_weighted_average_shares": 800,
            "income_tax_expense_total": 300, "income_before_tax_total": 1700,
            "proceeds_from_stock_issuance": 2000, "proceeds_from_debt_issuance": 0},
        "FY2025": {"revenue": 9000, "cost_of_revenue": 5600, "operating_income": 2000, "net_income": 1500,
            "operating_cash_flow": 1700, "capital_expenditures": 350, "depreciation_amortization": 220,
            "shareholders_equity": 8000, "total_assets": 12000, "interest_expense": 90,
            "current_assets": 5000, "current_liabilities": 2200, "goodwill": 500,
            "accounts_receivable": 900, "inventory": 700, "cash_and_equivalents": 2500,
            "total_debt": 1800, "dividends_paid": -100, "share_repurchase": 0,
            "sga_expense": 900, "rnd_expense": 450, "basic_weighted_average_shares": 1000,
            "income_tax_expense_total": 350, "income_before_tax_total": 1850,
            "proceeds_from_stock_issuance": 3000, "proceeds_from_debt_issuance": 0},
    })


def case_self_dealing():
    """管理层自肥: 关联交易+高薪酬"""

    def _df(rows, defaults):
        return pd.DataFrame([{**defaults, "id": i, **r} for i, r in enumerate(rows)])

    rpt = _df([
        {"related_party": "CEO公司", "relationship": "officer",
         "transaction_type": "lease", "amount": 500, "is_ongoing": True},
        {"related_party": "董事关联方", "relationship": "director",
         "transaction_type": "consulting", "amount": 200, "is_ongoing": True},
    ], {"company_id": 1, "period": "FY2025", "currency": "USD", "terms": None,
        "description": None, "raw_post_id": None, "created_at": "2025-01-01"})

    exec_comp = _df([
        {"name": "CEO", "title": "CEO", "role_type": "CEO", "pay_ratio": 500.0,
         "stock_awards": 5000, "total_comp": 8000},
    ], {"company_id": 1, "period": "FY2025", "base_salary": None, "bonus": None,
        "option_awards": None, "non_equity_incentive": None, "other_comp": None,
        "currency": "USD", "median_employee_comp": None, "raw_post_id": None,
        "created_at": "2025-01-01"})

    run("Case 6: 管理层自肥 (关联交易+高薪)", "有利润但管理层在掏空公司", {
        "FY2025": {"revenue": 10000, "cost_of_revenue": 6000, "operating_income": 2500, "net_income": 2000,
            "operating_cash_flow": 2200, "capital_expenditures": 300, "depreciation_amortization": 200,
            "shareholders_equity": 8000, "total_assets": 15000, "interest_expense": 100,
            "current_assets": 5000, "current_liabilities": 3000, "goodwill": 1000,
            "accounts_receivable": 1000, "inventory": 800, "cash_and_equivalents": 2000,
            "total_debt": 3000, "dividends_paid": -200, "share_repurchase": 0,
            "sga_expense": 1500, "rnd_expense": 200, "basic_weighted_average_shares": 500,
            "income_tax_expense_total": 400, "income_before_tax_total": 2400,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
    }, related_party_transactions=rpt, executive_compensations=exec_comp)


def case_berkshire():
    """类伯克希尔: 不分红不回购但留存回报极高"""
    run("Case 7: 类伯克希尔 (不分红但留存回报高)", "从不分红但每留存 1 块钱创造远超 1 块钱的价值", {
        "FY2024": {"revenue": 300000, "cost_of_revenue": 220000, "operating_income": 50000, "net_income": 40000,
            "operating_cash_flow": 45000, "capital_expenditures": 15000, "depreciation_amortization": 10000,
            "shareholders_equity": 500000, "total_assets": 900000, "interest_expense": 2000,
            "current_assets": 150000, "current_liabilities": 80000, "goodwill": 80000,
            "accounts_receivable": 20000, "inventory": 10000, "cash_and_equivalents": 100000,
            "total_debt": 40000, "dividends_paid": 0, "share_repurchase": -5000,
            "sga_expense": 15000, "rnd_expense": 0, "basic_weighted_average_shares": 1450,
            "income_tax_expense_total": 10000, "income_before_tax_total": 50000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
        "FY2025": {"revenue": 320000, "cost_of_revenue": 235000, "operating_income": 53000, "net_income": 43000,
            "operating_cash_flow": 48000, "capital_expenditures": 16000, "depreciation_amortization": 11000,
            "shareholders_equity": 550000, "total_assets": 1000000, "interest_expense": 2100,
            "current_assets": 160000, "current_liabilities": 85000, "goodwill": 82000,
            "accounts_receivable": 22000, "inventory": 11000, "cash_and_equivalents": 110000,
            "total_debt": 42000, "dividends_paid": 0, "share_repurchase": -8000,
            "sga_expense": 16000, "rnd_expense": 0, "basic_weighted_average_shares": 1440,
            "income_tax_expense_total": 11000, "income_before_tax_total": 54000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
    })


if __name__ == "__main__":
    print("=" * 65)
    print("  利润分配检测 · 全场景验证")
    print("=" * 65)

    case_generous()
    case_buyback()
    case_hoarder()
    case_empire()
    case_diluter()
    case_self_dealing()
    case_berkshire()

    print("\n" + "=" * 65)
    print("  验证完成")
    print("=" * 65)
