"""
盈余能力检测 · 全场景验证
=========================
8 个 case 覆盖不同盈余模式。
"""

import pandas as pd

import polaris.features.l0.company  # noqa: F401
import polaris.features.l0.cross_period  # noqa: F401

from polaris.features.types import ComputeContext, FeatureLevel
from polaris.features.registry import get_features
from polaris.chains.earnings import assess_earnings, format_earnings

EMPTY = pd.DataFrame()


def _fli(items, period="FY2025"):
    return pd.DataFrame([
        {"id": i, "statement_id": 1, "item_key": k, "item_label": k,
         "value": v, "parent_key": None, "ordinal": i, "note": None, "period": period}
        for i, (k, v) in enumerate(items.items())
    ])


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


def run(name, desc, fli_map):
    print(f"\n{'─' * 65}")
    print(f"  {name}")
    print(f"  {desc}")
    print(f"{'─' * 65}")
    all_fli = pd.concat([_fli(v, k) for k, v in fli_map.items()], ignore_index=True)
    ctx = ComputeContext(company_id=1, period=list(fli_map.keys())[-1])
    ctx._cache = {
        "financial_line_items": _fli(fli_map[list(fli_map.keys())[-1]]),
        "financial_line_items_all": all_fli,
        **EMPTY_TABLES,
    }
    for level in (FeatureLevel.L0, FeatureLevel.L1, FeatureLevel.L2):
        for feat in get_features(level=level):
            try:
                r = feat.compute_fn(ctx)
            except Exception:
                continue
            if r is not None:
                ctx.features[feat.name] = r.value
    result = assess_earnings(ctx)
    print(format_earnings(result))


# ══════════════════════════════════════════════════════════════
#  Case 1: 类茅台（轻资本，几乎不需要 capex，印钞机）
# ══════════════════════════════════════════════════════════════

MOUTAI = {
    "FY2022": {"revenue": 1200, "cost_of_revenue": 180, "operating_income": 720, "net_income": 600,
        "operating_cash_flow": 650, "capital_expenditures": 50, "depreciation_amortization": 40,
        "shareholders_equity": 1500, "total_assets": 2200, "interest_expense": 5,
        "current_assets": 1000, "current_liabilities": 400, "goodwill": 0,
        "accounts_receivable": 30, "inventory": 200, "cash_and_equivalents": 600,
        "total_debt": 0, "dividends_paid": -300, "share_repurchase": 0,
        "sga_expense": 100, "rnd_expense": 20, "basic_weighted_average_shares": 1256,
        "income_tax_expense_total": 180, "income_before_tax_total": 780,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
    "FY2023": {"revenue": 1400, "cost_of_revenue": 200, "operating_income": 850, "net_income": 700,
        "operating_cash_flow": 770, "capital_expenditures": 60, "depreciation_amortization": 45,
        "shareholders_equity": 1800, "total_assets": 2600, "interest_expense": 3,
        "current_assets": 1200, "current_liabilities": 450, "goodwill": 0,
        "accounts_receivable": 35, "inventory": 220, "cash_and_equivalents": 750,
        "total_debt": 0, "dividends_paid": -350, "share_repurchase": 0,
        "sga_expense": 110, "rnd_expense": 25, "basic_weighted_average_shares": 1256,
        "income_tax_expense_total": 210, "income_before_tax_total": 910,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
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
}

# ══════════════════════════════════════════════════════════════
#  Case 2: 类台积电（重 capex 但回报高）
# ══════════════════════════════════════════════════════════════

TSMC = {
    "FY2022": {"revenue": 70000, "cost_of_revenue": 33000, "operating_income": 27000, "net_income": 25000,
        "operating_cash_flow": 38000, "capital_expenditures": 25000, "depreciation_amortization": 15000,
        "shareholders_equity": 85000, "total_assets": 150000, "interest_expense": 400,
        "current_assets": 50000, "current_liabilities": 25000, "goodwill": 0,
        "accounts_receivable": 8000, "inventory": 10000, "cash_and_equivalents": 25000,
        "total_debt": 20000, "dividends_paid": -12000, "share_repurchase": 0,
        "sga_expense": 2500, "rnd_expense": 5500, "basic_weighted_average_shares": 25900,
        "income_tax_expense_total": 4000, "income_before_tax_total": 29000,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 3000},
    "FY2023": {"revenue": 75000, "cost_of_revenue": 34500, "operating_income": 30000, "net_income": 28000,
        "operating_cash_flow": 40000, "capital_expenditures": 27000, "depreciation_amortization": 16000,
        "shareholders_equity": 90000, "total_assets": 160000, "interest_expense": 450,
        "current_assets": 55000, "current_liabilities": 27000, "goodwill": 0,
        "accounts_receivable": 9000, "inventory": 11000, "cash_and_equivalents": 28000,
        "total_debt": 22000, "dividends_paid": -13000, "share_repurchase": 0,
        "sga_expense": 2700, "rnd_expense": 5800, "basic_weighted_average_shares": 25900,
        "income_tax_expense_total": 4500, "income_before_tax_total": 32500,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 4000},
    "FY2024": {"revenue": 82000, "cost_of_revenue": 37000, "operating_income": 33000, "net_income": 30000,
        "operating_cash_flow": 43000, "capital_expenditures": 28000, "depreciation_amortization": 17000,
        "shareholders_equity": 95000, "total_assets": 170000, "interest_expense": 480,
        "current_assets": 58000, "current_liabilities": 28000, "goodwill": 0,
        "accounts_receivable": 10000, "inventory": 12000, "cash_and_equivalents": 29000,
        "total_debt": 24000, "dividends_paid": -14000, "share_repurchase": 0,
        "sga_expense": 2800, "rnd_expense": 6000, "basic_weighted_average_shares": 25900,
        "income_tax_expense_total": 4800, "income_before_tax_total": 34800,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 4000},
    "FY2025": {"revenue": 90000, "cost_of_revenue": 40500, "operating_income": 36000, "net_income": 33000,
        "operating_cash_flow": 45000, "capital_expenditures": 30000, "depreciation_amortization": 18000,
        "shareholders_equity": 100000, "total_assets": 180000, "interest_expense": 500,
        "current_assets": 60000, "current_liabilities": 30000, "goodwill": 0,
        "accounts_receivable": 10000, "inventory": 12000, "cash_and_equivalents": 30000,
        "total_debt": 25000, "dividends_paid": -15000, "share_repurchase": 0,
        "sga_expense": 3000, "rnd_expense": 6500, "basic_weighted_average_shares": 25900,
        "income_tax_expense_total": 5000, "income_before_tax_total": 38000,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 5000},
}

# ══════════════════════════════════════════════════════════════
#  Case 3: 类 WeWork（烧钱亏损，靠融资续命）
# ══════════════════════════════════════════════════════════════

WEWORK = {
    "FY2024": {"revenue": 3000, "cost_of_revenue": 2800, "operating_income": -1500, "net_income": -2000,
        "operating_cash_flow": -1200, "capital_expenditures": 1500, "depreciation_amortization": 500,
        "shareholders_equity": 2000, "total_assets": 15000, "interest_expense": 800,
        "current_assets": 2000, "current_liabilities": 4000, "goodwill": 3000,
        "accounts_receivable": 500, "inventory": 100, "cash_and_equivalents": 800,
        "total_debt": 8000, "dividends_paid": 0, "share_repurchase": 0,
        "sga_expense": 1500, "rnd_expense": 200, "basic_weighted_average_shares": 1000,
        "income_tax_expense_total": 0, "income_before_tax_total": -2000,
        "proceeds_from_stock_issuance": 3000, "proceeds_from_debt_issuance": 2000},
    "FY2025": {"revenue": 2500, "cost_of_revenue": 2400, "operating_income": -1800, "net_income": -2500,
        "operating_cash_flow": -1500, "capital_expenditures": 1200, "depreciation_amortization": 600,
        "shareholders_equity": 500, "total_assets": 14000, "interest_expense": 900,
        "current_assets": 1500, "current_liabilities": 5000, "goodwill": 3000,
        "accounts_receivable": 400, "inventory": 80, "cash_and_equivalents": 500,
        "total_debt": 9000, "dividends_paid": 0, "share_repurchase": 0,
        "sga_expense": 1400, "rnd_expense": 100, "basic_weighted_average_shares": 1200,
        "income_tax_expense_total": 0, "income_before_tax_total": -2500,
        "proceeds_from_stock_issuance": 4000, "proceeds_from_debt_issuance": 3000},
}

# ══════════════════════════════════════════════════════════════
#  Case 4: 类比亚迪（重研发投入期，利润薄但在增长）
# ══════════════════════════════════════════════════════════════

BYD = {
    "FY2022": {"revenue": 4200, "cost_of_revenue": 3500, "operating_income": 200, "net_income": 170,
        "operating_cash_flow": 500, "capital_expenditures": 600, "depreciation_amortization": 300,
        "shareholders_equity": 2000, "total_assets": 5500, "interest_expense": 100,
        "current_assets": 2000, "current_liabilities": 2500, "goodwill": 50,
        "accounts_receivable": 400, "inventory": 600, "cash_and_equivalents": 500,
        "total_debt": 1500, "dividends_paid": -30, "share_repurchase": 0,
        "sga_expense": 200, "rnd_expense": 400, "basic_weighted_average_shares": 2900,
        "income_tax_expense_total": 30, "income_before_tax_total": 200,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 500},
    "FY2023": {"revenue": 5800, "cost_of_revenue": 4700, "operating_income": 400, "net_income": 300,
        "operating_cash_flow": 800, "capital_expenditures": 800, "depreciation_amortization": 400,
        "shareholders_equity": 2500, "total_assets": 7000, "interest_expense": 120,
        "current_assets": 2500, "current_liabilities": 3000, "goodwill": 50,
        "accounts_receivable": 500, "inventory": 700, "cash_and_equivalents": 600,
        "total_debt": 1800, "dividends_paid": -40, "share_repurchase": 0,
        "sga_expense": 250, "rnd_expense": 500, "basic_weighted_average_shares": 2900,
        "income_tax_expense_total": 50, "income_before_tax_total": 350,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 600},
    "FY2024": {"revenue": 7200, "cost_of_revenue": 5700, "operating_income": 700, "net_income": 550,
        "operating_cash_flow": 1100, "capital_expenditures": 1000, "depreciation_amortization": 500,
        "shareholders_equity": 3200, "total_assets": 8500, "interest_expense": 130,
        "current_assets": 3000, "current_liabilities": 3200, "goodwill": 50,
        "accounts_receivable": 600, "inventory": 800, "cash_and_equivalents": 700,
        "total_debt": 2000, "dividends_paid": -50, "share_repurchase": 0,
        "sga_expense": 280, "rnd_expense": 600, "basic_weighted_average_shares": 2900,
        "income_tax_expense_total": 80, "income_before_tax_total": 630,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 500},
    "FY2025": {"revenue": 9000, "cost_of_revenue": 7000, "operating_income": 1000, "net_income": 800,
        "operating_cash_flow": 1500, "capital_expenditures": 1200, "depreciation_amortization": 600,
        "shareholders_equity": 4000, "total_assets": 10000, "interest_expense": 140,
        "current_assets": 3500, "current_liabilities": 3500, "goodwill": 50,
        "accounts_receivable": 700, "inventory": 900, "cash_and_equivalents": 800,
        "total_debt": 2200, "dividends_paid": -60, "share_repurchase": 0,
        "sga_expense": 300, "rnd_expense": 700, "basic_weighted_average_shares": 2900,
        "income_tax_expense_total": 100, "income_before_tax_total": 900,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 400},
}

# ══════════════════════════════════════════════════════════════
#  Case 5: 纸面利润好但现金流差（应收堆出来的收入）
# ══════════════════════════════════════════════════════════════

PAPER_PROFIT = {
    "FY2024": {"revenue": 5000, "cost_of_revenue": 3000, "operating_income": 1200, "net_income": 900,
        "operating_cash_flow": 300, "capital_expenditures": 200, "depreciation_amortization": 150,
        "shareholders_equity": 3000, "total_assets": 6000, "interest_expense": 50,
        "current_assets": 3500, "current_liabilities": 1500, "goodwill": 200,
        "accounts_receivable": 2000, "inventory": 500, "cash_and_equivalents": 500,
        "total_debt": 1000, "dividends_paid": -100, "share_repurchase": 0,
        "sga_expense": 500, "rnd_expense": 300, "basic_weighted_average_shares": 500,
        "income_tax_expense_total": 200, "income_before_tax_total": 1100,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
    "FY2025": {"revenue": 6000, "cost_of_revenue": 3500, "operating_income": 1500, "net_income": 1100,
        "operating_cash_flow": 200, "capital_expenditures": 250, "depreciation_amortization": 160,
        "shareholders_equity": 3800, "total_assets": 7500, "interest_expense": 60,
        "current_assets": 4500, "current_liabilities": 1800, "goodwill": 200,
        "accounts_receivable": 2800, "inventory": 600, "cash_and_equivalents": 400,
        "total_debt": 1200, "dividends_paid": -120, "share_repurchase": 0,
        "sga_expense": 550, "rnd_expense": 350, "basic_weighted_average_shares": 500,
        "income_tax_expense_total": 250, "income_before_tax_total": 1350,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
}

# ══════════════════════════════════════════════════════════════
#  Case 6: Costco 类（低毛利但效率高，现金流强劲）
# ══════════════════════════════════════════════════════════════

COSTCO = {
    "FY2024": {"revenue": 240000, "cost_of_revenue": 210000, "operating_income": 8200, "net_income": 6800,
        "operating_cash_flow": 10500, "capital_expenditures": 4500, "depreciation_amortization": 2200,
        "shareholders_equity": 16000, "total_assets": 65000, "interest_expense": 180,
        "current_assets": 30000, "current_liabilities": 33000, "goodwill": 900,
        "accounts_receivable": 2000, "inventory": 17000, "cash_and_equivalents": 12000,
        "total_debt": 8000, "dividends_paid": -3800, "share_repurchase": -500,
        "sga_expense": 22000, "rnd_expense": 0, "basic_weighted_average_shares": 443,
        "income_tax_expense_total": 2200, "income_before_tax_total": 9000,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
    "FY2025": {"revenue": 254000, "cost_of_revenue": 221000, "operating_income": 8900, "net_income": 7400,
        "operating_cash_flow": 11000, "capital_expenditures": 4700, "depreciation_amortization": 2300,
        "shareholders_equity": 17000, "total_assets": 69000, "interest_expense": 200,
        "current_assets": 32000, "current_liabilities": 35000, "goodwill": 900,
        "accounts_receivable": 2200, "inventory": 18000, "cash_and_equivalents": 13000,
        "total_debt": 9000, "dividends_paid": -4200, "share_repurchase": -600,
        "sga_expense": 24000, "rnd_expense": 0, "basic_weighted_average_shares": 443,
        "income_tax_expense_total": 2400, "income_before_tax_total": 9800,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
}

# ══════════════════════════════════════════════════════════════
#  Case 7: 收购狂人（利润全砸去收购，股东拿不到钱）
# ══════════════════════════════════════════════════════════════

ACQUIRER = {
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
}

# ══════════════════════════════════════════════════════════════
#  Case 8: 稳定现金牛但零增长（公用事业类）
# ══════════════════════════════════════════════════════════════

UTILITY = {
    "FY2022": {"revenue": 10000, "cost_of_revenue": 6000, "operating_income": 2500, "net_income": 1800,
        "operating_cash_flow": 2800, "capital_expenditures": 1000, "depreciation_amortization": 800,
        "shareholders_equity": 12000, "total_assets": 25000, "interest_expense": 500,
        "current_assets": 3000, "current_liabilities": 2500, "goodwill": 500,
        "accounts_receivable": 800, "inventory": 300, "cash_and_equivalents": 1500,
        "total_debt": 8000, "dividends_paid": -1200, "share_repurchase": 0,
        "sga_expense": 800, "rnd_expense": 50, "basic_weighted_average_shares": 600,
        "income_tax_expense_total": 500, "income_before_tax_total": 2300,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
    "FY2023": {"revenue": 10200, "cost_of_revenue": 6100, "operating_income": 2550, "net_income": 1850,
        "operating_cash_flow": 2850, "capital_expenditures": 1050, "depreciation_amortization": 820,
        "shareholders_equity": 12500, "total_assets": 25500, "interest_expense": 480,
        "current_assets": 3100, "current_liabilities": 2600, "goodwill": 500,
        "accounts_receivable": 820, "inventory": 310, "cash_and_equivalents": 1600,
        "total_debt": 7800, "dividends_paid": -1250, "share_repurchase": 0,
        "sga_expense": 820, "rnd_expense": 50, "basic_weighted_average_shares": 600,
        "income_tax_expense_total": 510, "income_before_tax_total": 2360,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
    "FY2024": {"revenue": 10400, "cost_of_revenue": 6200, "operating_income": 2600, "net_income": 1900,
        "operating_cash_flow": 2900, "capital_expenditures": 1100, "depreciation_amortization": 840,
        "shareholders_equity": 13000, "total_assets": 26000, "interest_expense": 460,
        "current_assets": 3200, "current_liabilities": 2700, "goodwill": 500,
        "accounts_receivable": 840, "inventory": 320, "cash_and_equivalents": 1700,
        "total_debt": 7600, "dividends_paid": -1300, "share_repurchase": 0,
        "sga_expense": 840, "rnd_expense": 50, "basic_weighted_average_shares": 600,
        "income_tax_expense_total": 520, "income_before_tax_total": 2420,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
    "FY2025": {"revenue": 10600, "cost_of_revenue": 6300, "operating_income": 2650, "net_income": 1950,
        "operating_cash_flow": 2950, "capital_expenditures": 1100, "depreciation_amortization": 860,
        "shareholders_equity": 13500, "total_assets": 26500, "interest_expense": 440,
        "current_assets": 3300, "current_liabilities": 2800, "goodwill": 500,
        "accounts_receivable": 860, "inventory": 330, "cash_and_equivalents": 1800,
        "total_debt": 7400, "dividends_paid": -1350, "share_repurchase": 0,
        "sga_expense": 860, "rnd_expense": 50, "basic_weighted_average_shares": 600,
        "income_tax_expense_total": 530, "income_before_tax_total": 2480,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
}

if __name__ == "__main__":
    print("=" * 65)
    print("  盈余能力检测 · 全场景验证")
    print("=" * 65)

    run("Case 1: 类茅台", "轻资本印钞机，几乎不需要 capex", MOUTAI)
    run("Case 2: 类台积电", "重 capex 但回报极高", TSMC)
    run("Case 3: 类 WeWork", "烧钱亏损，靠融资续命", WEWORK)
    run("Case 4: 类比亚迪", "重研发投入期，利润薄但在快速增长", BYD)
    run("Case 5: 纸面利润", "利润好看但现金流差（应收堆出来的）", PAPER_PROFIT)
    run("Case 6: 类 Costco", "低毛利但效率高，现金流强劲", COSTCO)
    run("Case 7: 收购狂人", "利润不错但全砸去收购，商誉暴涨", ACQUIRER)
    run("Case 8: 公用事业", "稳定现金牛，零增长但现金流极稳", UTILITY)

    print("\n" + "=" * 65)
    print("  验证完成")
    print("=" * 65)
