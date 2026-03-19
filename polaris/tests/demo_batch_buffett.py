"""
巴菲特因果链 · 9 家公司批量评估
=================================
可口可乐 / 苹果 / 英伟达 / Google / 阿里巴巴 / 腾讯 / 茅台 / 紫金矿业 / 特变电工

每家公司跑双线全链，逐步输出结果。
数据为近似真实的 mock 数据（基于公开财报 2022-2025）。
"""

import pandas as pd

import polaris.features.l0.company  # noqa: F401
import polaris.features.l0.cross_period  # noqa: F401

from polaris.features.types import ComputeContext, FeatureLevel
from polaris.features.registry import get_features
from polaris.chains.moat import assess_moat, format_moat
from polaris.chains.earnings import assess_earnings, format_earnings
from polaris.chains.distribution import assess_distribution, format_distribution
from polaris.chains.predictability import assess_predictability, format_predictability
from polaris.chains.integrity import assess_integrity, format_integrity
from polaris.chains.character import assess_character, format_character
from polaris.chains.risk import assess_risk, format_risk
from polaris.scoring.engines.dcf import compute_intrinsic_value, reverse_dcf

EMPTY = pd.DataFrame()


# ══════════════════════════════════════════════════════════════
#  工具函数
# ══════════════════════════════════════════════════════════════

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


DS = {"company_id": 1, "period": "FY2025", "segment": None,
      "customer_type": None, "products": None, "channels": None,
      "revenue": None, "growth_yoy": None, "backlog": None,
      "backlog_note": None, "pricing_model": None, "contract_duration": None,
      "recognition_method": None, "description": None,
      "raw_post_id": None, "created_at": "2025-01-01"}
CD = {"company_id": 1, "estimated_investment": None,
      "outcome_market_share_change": None, "event_date": "2024-01",
      "raw_post_id": None, "created_at": "2025-01-01"}
MS = {"company_id": 1, "raw_post_id": None, "created_at": "2025-01-01"}
PA = {"company_id": 1, "raw_post_id": None, "created_at": "2025-01-01"}
GR = {"company_id": 1, "period": "FY2025", "revenue": None, "growth_yoy": None,
      "note": None, "raw_post_id": None, "created_at": "2025-01-01"}
SO = {"company_id": 1, "period": "FY2025", "shares_beneficially_owned": None,
      "raw_post_id": None, "created_at": "2025-01-01"}
EC = {"company_id": 1, "period": "FY2025", "base_salary": None, "bonus": None,
      "option_awards": None, "non_equity_incentive": None, "other_comp": None,
      "currency": "USD", "median_employee_comp": None, "raw_post_id": None,
      "created_at": "2025-01-01"}
US = {"company_id": 1, "period": "FY2025", "segment": None, "supply_type": "component",
      "material_or_service": None, "process_node": None, "purchase_obligation": None,
      "contract_type": None, "prepaid_amount": None, "concentration_risk": None,
      "description": None, "raw_post_id": None, "created_at": "2025-01-01"}
CN = {"company_id": 1, "raw_post_id": None, "capital_required": None,
      "capital_unit": None, "promised_outcome": None, "deadline": None,
      "reported_at": None, "created_at": "2025-01-01"}


# ══════════════════════════════════════════════════════════════
#  公司数据
# ══════════════════════════════════════════════════════════════

COMPANIES = {}

# ── 1. 可口可乐 (KO) ─────────────────────────────────────────
# 单位: 百万美元
COMPANIES["可口可乐 (KO)"] = {
    "fli": {
        "FY2022": {
            "revenue": 43000, "cost_of_revenue": 18000, "operating_income": 11200,
            "net_income": 9500, "operating_cash_flow": 11000, "capital_expenditures": 1500,
            "depreciation_amortization": 1200, "shareholders_equity": 24500,
            "total_assets": 92800, "interest_expense": 900, "current_assets": 22500,
            "current_liabilities": 19700, "goodwill": 26400,
            "accounts_receivable": 3500, "inventory": 4200,
            "cash_and_equivalents": 9500, "total_debt": 35000,
            "dividends_paid": -7600, "share_repurchase": -1500,
            "sga_expense": 12500, "rnd_expense": 0,
            "basic_weighted_average_shares": 4328,
            "income_tax_expense_total": 2100, "income_before_tax_total": 11600,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 5000,
        },
        "FY2023": {
            "revenue": 45800, "cost_of_revenue": 18500, "operating_income": 12500,
            "net_income": 10700, "operating_cash_flow": 11600, "capital_expenditures": 1900,
            "depreciation_amortization": 1300, "shareholders_equity": 25000,
            "total_assets": 98000, "interest_expense": 1000, "current_assets": 22000,
            "current_liabilities": 20500, "goodwill": 27000,
            "accounts_receivable": 3700, "inventory": 4000,
            "cash_and_equivalents": 9400, "total_debt": 36000,
            "dividends_paid": -7900, "share_repurchase": -1800,
            "sga_expense": 13000, "rnd_expense": 0,
            "basic_weighted_average_shares": 4310,
            "income_tax_expense_total": 2300, "income_before_tax_total": 13000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 4000,
        },
        "FY2024": {
            "revenue": 47100, "cost_of_revenue": 19200, "operating_income": 12800,
            "net_income": 10500, "operating_cash_flow": 12000, "capital_expenditures": 2000,
            "depreciation_amortization": 1400, "shareholders_equity": 25200,
            "total_assets": 100000, "interest_expense": 1100, "current_assets": 23000,
            "current_liabilities": 21000, "goodwill": 27500,
            "accounts_receivable": 3800, "inventory": 3800,
            "cash_and_equivalents": 9800, "total_debt": 37000,
            "dividends_paid": -8200, "share_repurchase": -2000,
            "sga_expense": 13500, "rnd_expense": 0,
            "basic_weighted_average_shares": 4290,
            "income_tax_expense_total": 2200, "income_before_tax_total": 12700,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 3500,
        },
        "FY2025": {
            "revenue": 49000, "cost_of_revenue": 19800, "operating_income": 13500,
            "net_income": 11000, "operating_cash_flow": 12500, "capital_expenditures": 2100,
            "depreciation_amortization": 1500, "shareholders_equity": 25500,
            "total_assets": 102000, "interest_expense": 1150, "current_assets": 24000,
            "current_liabilities": 21500, "goodwill": 28000,
            "accounts_receivable": 4000, "inventory": 3700,
            "cash_and_equivalents": 10500, "total_debt": 38000,
            "dividends_paid": -8500, "share_repurchase": -2500,
            "sga_expense": 14000, "rnd_expense": 0,
            "basic_weighted_average_shares": 4270,
            "income_tax_expense_total": 2300, "income_before_tax_total": 13300,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 3000,
        },
    },
    "downstream": [
        {"customer_name": "饮料零售", "revenue_pct": 0.45, "is_recurring": True,
         "revenue_type": "recurring", "product_category": "beverage"},
        {"customer_name": "餐饮渠道", "revenue_pct": 0.30, "is_recurring": True,
         "revenue_type": "recurring", "product_category": "beverage"},
        {"customer_name": "瓶装合作伙伴", "revenue_pct": 0.25, "is_recurring": True,
         "revenue_type": "recurring", "product_category": "beverage"},
    ],
    "upstream": [
        {"supplier_name": "糖和甜味剂供应商", "is_sole_source": False, "geographic_location": "全球"},
        {"supplier_name": "铝罐包装", "is_sole_source": False, "geographic_location": "多地"},
    ],
    "geo": [
        {"region": "United States", "revenue_share": 0.35},
        {"region": "Europe", "revenue_share": 0.20},
        {"region": "Latin America", "revenue_share": 0.15},
        {"region": "Asia Pacific", "revenue_share": 0.20},
        {"region": "Africa", "revenue_share": 0.10},
    ],
    "pricing": [
        {"action": "全球涨价 9%", "price_change_pct": 0.09,
         "product_or_segment": "全线产品", "effective_date": "2023-01",
         "volume_impact_pct": -0.01},
        {"action": "二次涨价 5%", "price_change_pct": 0.05,
         "product_or_segment": "全线产品", "effective_date": "2024-06",
         "volume_impact_pct": 0.0},
    ],
    "market_share": [
        {"period": "FY2023", "share": 0.46, "source": "全球碳酸饮料"},
        {"period": "FY2024", "share": 0.46, "source": "全球碳酸饮料"},
        {"period": "FY2025", "share": 0.47, "source": "全球碳酸饮料"},
    ],
    "competitive": [
        {"competitor_name": "百事", "event_type": "price_war",
         "event_description": "百事跟进涨价但幅度更小",
         "outcome_description": "可口可乐份额不降反升",
         "outcome_market_share_change": 0.01},
    ],
    "peers": [
        {"peer_name": "百事", "metric": "gross_margin", "value": 0.54, "period": "FY2025"},
        {"peer_name": "百事", "metric": "operating_margin", "value": 0.14, "period": "FY2025"},
        {"peer_name": "百事", "metric": "net_margin", "value": 0.10, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "全品类饮料公司转型", "status": "delivered"},
        {"narrative": "零糖产品线扩张", "status": "delivered"},
        {"narrative": "全球瓶装系统优化", "status": "delivered"},
        {"narrative": "高端化策略 Fairlife/Topo Chico", "status": "delivered"},
        {"narrative": "数字化直达消费者", "status": "in_progress"},
    ],
    "ownership": [
        {"name": "James Quincey", "title": "CEO", "percent_of_class": 0.02},
    ],
    "exec_comp": [
        {"name": "James Quincey", "title": "CEO", "role_type": "CEO",
         "pay_ratio": 200.0, "stock_awards": 8000, "total_comp": 18000},
    ],
    # 可口可乐: 职业经理人体系成熟，CEO 交接有序
    "exec_changes": [
        {"name": "Brian Smith", "title": "COO/President", "change_type": "joined"},
    ],
    "market": {"price": 62.0, "shares_outstanding": 4270, "discount_rate": 0.043, "market": "US"},
    "guidance": {"revenue_growth": 0.05},
    "home_market": "United States",
}

# ── 2. 苹果 (AAPL) ───────────────────────────────────────────
# 单位: 百万美元
COMPANIES["苹果 (AAPL)"] = {
    "fli": {
        "FY2022": {
            "revenue": 394300, "cost_of_revenue": 223500, "operating_income": 119400,
            "net_income": 99800, "operating_cash_flow": 122200, "capital_expenditures": 10700,
            "depreciation_amortization": 11100, "shareholders_equity": 50700,
            "total_assets": 352800, "interest_expense": 2900, "current_assets": 135400,
            "current_liabilities": 153900, "goodwill": 0,
            "accounts_receivable": 28200, "inventory": 4900,
            "cash_and_equivalents": 48300, "total_debt": 120100,
            "dividends_paid": -14800, "share_repurchase": -89400,
            "sga_expense": 25100, "rnd_expense": 26300,
            "basic_weighted_average_shares": 16216,
            "income_tax_expense_total": 19300, "income_before_tax_total": 119100,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 9000,
        },
        "FY2023": {
            "revenue": 383300, "cost_of_revenue": 214100, "operating_income": 114300,
            "net_income": 97000, "operating_cash_flow": 110500, "capital_expenditures": 10900,
            "depreciation_amortization": 11500, "shareholders_equity": 62100,
            "total_assets": 352600, "interest_expense": 3500, "current_assets": 143600,
            "current_liabilities": 145300, "goodwill": 0,
            "accounts_receivable": 29500, "inventory": 6300,
            "cash_and_equivalents": 30700, "total_debt": 111100,
            "dividends_paid": -15000, "share_repurchase": -77600,
            "sga_expense": 24900, "rnd_expense": 29900,
            "basic_weighted_average_shares": 15744,
            "income_tax_expense_total": 16700, "income_before_tax_total": 113700,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 5500,
        },
        "FY2024": {
            "revenue": 391000, "cost_of_revenue": 210100, "operating_income": 123200,
            "net_income": 101600, "operating_cash_flow": 118300, "capital_expenditures": 9900,
            "depreciation_amortization": 11400, "shareholders_equity": 56900,
            "total_assets": 364900, "interest_expense": 3300, "current_assets": 152900,
            "current_liabilities": 176300, "goodwill": 0,
            "accounts_receivable": 32400, "inventory": 7200,
            "cash_and_equivalents": 29900, "total_debt": 105000,
            "dividends_paid": -15200, "share_repurchase": -94900,
            "sga_expense": 26100, "rnd_expense": 31400,
            "basic_weighted_average_shares": 15408,
            "income_tax_expense_total": 18200, "income_before_tax_total": 119800,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        "FY2025": {
            "revenue": 420000, "cost_of_revenue": 222600, "operating_income": 134000,
            "net_income": 110000, "operating_cash_flow": 125000, "capital_expenditures": 10000,
            "depreciation_amortization": 11500, "shareholders_equity": 57000,
            "total_assets": 370000, "interest_expense": 3200, "current_assets": 155000,
            "current_liabilities": 180000, "goodwill": 0,
            "accounts_receivable": 34000, "inventory": 7000,
            "cash_and_equivalents": 30000, "total_debt": 100000,
            "dividends_paid": -15500, "share_repurchase": -100000,
            "sga_expense": 27000, "rnd_expense": 33000,
            "basic_weighted_average_shares": 15000,
            "income_tax_expense_total": 19000, "income_before_tax_total": 129000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
    },
    "downstream": [
        {"customer_name": "iPhone用户", "revenue_pct": 0.52, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "consumer_electronics"},
        {"customer_name": "服务收入", "revenue_pct": 0.24, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "cloud_infrastructure"},
        {"customer_name": "Mac/iPad/可穿戴", "revenue_pct": 0.24, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "consumer_electronics"},
    ],
    "upstream": [
        {"supplier_name": "台积电", "is_sole_source": True, "geographic_location": "Taiwan"},
        {"supplier_name": "三星显示", "is_sole_source": False, "geographic_location": "韩国"},
    ],
    "geo": [
        {"region": "Americas", "revenue_share": 0.42},
        {"region": "Europe", "revenue_share": 0.25},
        {"region": "China", "revenue_share": 0.17},
        {"region": "Japan", "revenue_share": 0.06},
        {"region": "Rest of Asia", "revenue_share": 0.10},
    ],
    "pricing": [
        {"action": "iPhone 15 Pro 涨价 $100", "price_change_pct": 0.10,
         "product_or_segment": "iPhone Pro", "effective_date": "2023-09",
         "volume_impact_pct": -0.02},
        {"action": "服务费涨价", "price_change_pct": 0.15,
         "product_or_segment": "Apple Music/TV+", "effective_date": "2024-01",
         "volume_impact_pct": 0.0},
    ],
    "market_share": [
        {"period": "FY2023", "share": 0.20, "source": "全球智能手机"},
        {"period": "FY2024", "share": 0.20, "source": "全球智能手机"},
        {"period": "FY2025", "share": 0.21, "source": "全球智能手机"},
    ],
    "competitive": [
        {"competitor_name": "三星", "event_type": "product_launch",
         "event_description": "Galaxy S24 Ultra AI手机上市",
         "outcome_description": "苹果份额稳定，高端市场仍由苹果主导",
         "outcome_market_share_change": 0.0},
        {"competitor_name": "华为", "event_type": "new_entry",
         "event_description": "华为 Mate 60 Pro 回归中国市场",
         "outcome_description": "苹果中国份额被蚕食约2-3%",
         "outcome_market_share_change": -0.02},
    ],
    "peers": [
        {"peer_name": "三星", "metric": "gross_margin", "value": 0.38, "period": "FY2025"},
        {"peer_name": "三星", "metric": "operating_margin", "value": 0.12, "period": "FY2025"},
        {"peer_name": "三星", "metric": "net_margin", "value": 0.10, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "Apple Intelligence AI整合", "status": "delivered"},
        {"narrative": "服务收入持续增长至千亿级", "status": "delivered"},
        {"narrative": "Apple Vision Pro 空间计算", "status": "missed"},
        {"narrative": "印度制造多元化", "status": "delivered"},
        {"narrative": "健康/汽车新品类", "status": "in_progress"},
    ],
    "ownership": [
        {"name": "Tim Cook", "title": "CEO", "percent_of_class": 0.02},
    ],
    "exec_comp": [
        {"name": "Tim Cook", "title": "CEO", "role_type": "CEO",
         "pay_ratio": 1450.0, "stock_awards": 40000, "total_comp": 63000},
    ],
    # 苹果: Jobs→Cook 成功过渡，Jeff Williams (COO) 是公认接班人
    "exec_changes": [
        {"name": "Jeff Williams", "title": "COO", "change_type": "joined"},
        {"name": "John Ternus", "title": "SVP Hardware Engineering", "change_type": "joined"},
    ],
    "market": {"price": 230.0, "shares_outstanding": 15000, "discount_rate": 0.043, "market": "US"},
    "guidance": {"revenue_growth": 0.07},
    "home_market": "United States",
}

# ── 3. 英伟达 (NVDA) ─────────────────────────────────────────
# 单位: 百万美元，财年偏移 (FY2025 = calendar 2024)
COMPANIES["英伟达 (NVDA)"] = {
    "fli": {
        "FY2022": {
            "revenue": 26900, "cost_of_revenue": 11600, "operating_income": 10100,
            "net_income": 9800, "operating_cash_flow": 5600, "capital_expenditures": 1800,
            "depreciation_amortization": 1500, "shareholders_equity": 26600,
            "total_assets": 44200, "interest_expense": 300, "current_assets": 28500,
            "current_liabilities": 9000, "goodwill": 4400,
            "accounts_receivable": 4100, "inventory": 5200,
            "cash_and_equivalents": 13300, "total_debt": 11000,
            "dividends_paid": -400, "share_repurchase": -10000,
            "sga_expense": 2400, "rnd_expense": 7300,
            "basic_weighted_average_shares": 24690,
            "income_tax_expense_total": 1600, "income_before_tax_total": 11400,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        "FY2023": {
            "revenue": 61000, "cost_of_revenue": 16600, "operating_income": 33000,
            "net_income": 29800, "operating_cash_flow": 28100, "capital_expenditures": 1100,
            "depreciation_amortization": 1600, "shareholders_equity": 42900,
            "total_assets": 65700, "interest_expense": 300, "current_assets": 44300,
            "current_liabilities": 10300, "goodwill": 4400,
            "accounts_receivable": 9900, "inventory": 5300,
            "cash_and_equivalents": 26000, "total_debt": 9700,
            "dividends_paid": -400, "share_repurchase": -9500,
            "sga_expense": 2700, "rnd_expense": 8700,
            "basic_weighted_average_shares": 24580,
            "income_tax_expense_total": 4000, "income_before_tax_total": 33800,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        "FY2024": {
            "revenue": 130500, "cost_of_revenue": 30100, "operating_income": 81100,
            "net_income": 72900, "operating_cash_flow": 64100, "capital_expenditures": 3300,
            "depreciation_amortization": 2100, "shareholders_equity": 65900,
            "total_assets": 96000, "interest_expense": 300, "current_assets": 65600,
            "current_liabilities": 16600, "goodwill": 4400,
            "accounts_receivable": 17500, "inventory": 8000,
            "cash_and_equivalents": 31400, "total_debt": 8500,
            "dividends_paid": -500, "share_repurchase": -26600,
            "sga_expense": 3100, "rnd_expense": 12900,
            "basic_weighted_average_shares": 24500,
            "income_tax_expense_total": 8000, "income_before_tax_total": 80900,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        "FY2025": {
            "revenue": 180000, "cost_of_revenue": 42000, "operating_income": 112000,
            "net_income": 100000, "operating_cash_flow": 90000, "capital_expenditures": 4000,
            "depreciation_amortization": 2500, "shareholders_equity": 80000,
            "total_assets": 120000, "interest_expense": 300, "current_assets": 80000,
            "current_liabilities": 20000, "goodwill": 4400,
            "accounts_receivable": 22000, "inventory": 9000,
            "cash_and_equivalents": 35000, "total_debt": 8000,
            "dividends_paid": -600, "share_repurchase": -30000,
            "sga_expense": 3500, "rnd_expense": 16000,
            "basic_weighted_average_shares": 24400,
            "income_tax_expense_total": 12000, "income_before_tax_total": 112000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
    },
    "downstream": [
        {"customer_name": "数据中心/AI", "revenue_pct": 0.83, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "cloud_infrastructure"},
        {"customer_name": "游戏GPU", "revenue_pct": 0.10, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "consumer_electronics"},
        {"customer_name": "汽车/机器人", "revenue_pct": 0.07, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "consumer_electronics"},
    ],
    "upstream": [
        {"supplier_name": "台积电", "is_sole_source": True, "geographic_location": "Taiwan"},
        {"supplier_name": "SK海力士", "is_sole_source": False, "geographic_location": "韩国"},
    ],
    "geo": [
        {"region": "United States", "revenue_share": 0.26},
        {"region": "Taiwan", "revenue_share": 0.17},
        {"region": "China", "revenue_share": 0.12},
        {"region": "Singapore", "revenue_share": 0.20},
        {"region": "Other", "revenue_share": 0.25},
    ],
    "pricing": [
        {"action": "H100→H200 涨价", "price_change_pct": 0.20,
         "product_or_segment": "数据中心GPU", "effective_date": "2024-06",
         "volume_impact_pct": 0.30},
    ],
    "market_share": [
        {"period": "FY2023", "share": 0.80, "source": "AI训练GPU"},
        {"period": "FY2024", "share": 0.85, "source": "AI训练GPU"},
        {"period": "FY2025", "share": 0.88, "source": "AI训练GPU"},
    ],
    "competitive": [
        {"competitor_name": "AMD", "event_type": "product_launch",
         "event_description": "MI300X 上市挑战 H100",
         "outcome_description": "AMD 拿到约 5% 份额，NVDA 仍主导",
         "outcome_market_share_change": -0.02},
        {"competitor_name": "Google TPU", "event_type": "new_entry",
         "event_description": "Google 自研 TPU v5 用于内部训练",
         "outcome_description": "仅用于 Google 内部，对 NVDA 商业市场影响有限",
         "outcome_market_share_change": 0.0},
    ],
    "peers": [
        {"peer_name": "AMD", "metric": "gross_margin", "value": 0.50, "period": "FY2025"},
        {"peer_name": "AMD", "metric": "operating_margin", "value": 0.22, "period": "FY2025"},
        {"peer_name": "AMD", "metric": "net_margin", "value": 0.18, "period": "FY2025"},
        {"peer_name": "Intel", "metric": "gross_margin", "value": 0.38, "period": "FY2025"},
        {"peer_name": "Intel", "metric": "operating_margin", "value": -0.02, "period": "FY2025"},
        {"peer_name": "Intel", "metric": "net_margin", "value": -0.05, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "AI是新工业革命，NVDA是卖铲人", "status": "delivered"},
        {"narrative": "CUDA生态壁垒", "status": "delivered"},
        {"narrative": "从芯片到系统（DGX/NVLink）", "status": "delivered"},
        {"narrative": "汽车/机器人长期增长", "status": "in_progress"},
    ],
    "ownership": [
        {"name": "Jensen Huang", "title": "创始人/CEO", "percent_of_class": 3.5},
    ],
    "exec_comp": [
        {"name": "Jensen Huang", "title": "创始人/CEO", "role_type": "CEO",
         "pay_ratio": 500.0, "stock_awards": 20000, "total_comp": 34000},
    ],
    # 英伟达: Jensen Huang 是灵魂人物，无明确接班人
    # 但 CUDA 生态有惯性，组织架构已相对成熟
    "exec_changes": [],
    "market": {"price": 140.0, "shares_outstanding": 24400, "discount_rate": 0.043, "market": "US"},
    "guidance": {"revenue_growth": 0.35},
    "home_market": "United States",
}

# ── 4. Google (GOOGL) ────────────────────────────────────────
# 单位: 百万美元
COMPANIES["Google (GOOGL)"] = {
    "fli": {
        "FY2022": {
            "revenue": 282800, "cost_of_revenue": 126200, "operating_income": 74800,
            "net_income": 60000, "operating_cash_flow": 91500, "capital_expenditures": 31500,
            "depreciation_amortization": 15000, "shareholders_equity": 256100,
            "total_assets": 365300, "interest_expense": 400, "current_assets": 164800,
            "current_liabilities": 69300, "goodwill": 28800,
            "accounts_receivable": 40300, "inventory": 0,
            "cash_and_equivalents": 113900, "total_debt": 28500,
            "dividends_paid": 0, "share_repurchase": -59300,
            "sga_expense": 36400, "rnd_expense": 39500,
            "basic_weighted_average_shares": 13044,
            "income_tax_expense_total": 11400, "income_before_tax_total": 71300,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 8000,
        },
        "FY2023": {
            "revenue": 307400, "cost_of_revenue": 133300, "operating_income": 84300,
            "net_income": 73800, "operating_cash_flow": 101700, "capital_expenditures": 32300,
            "depreciation_amortization": 15800, "shareholders_equity": 264700,
            "total_assets": 402400, "interest_expense": 300, "current_assets": 171800,
            "current_liabilities": 81800, "goodwill": 29300,
            "accounts_receivable": 44100, "inventory": 0,
            "cash_and_equivalents": 110900, "total_debt": 29800,
            "dividends_paid": 0, "share_repurchase": -62200,
            "sga_expense": 38000, "rnd_expense": 42300,
            "basic_weighted_average_shares": 12700,
            "income_tax_expense_total": 11900, "income_before_tax_total": 85700,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 10000,
        },
        "FY2024": {
            "revenue": 350000, "cost_of_revenue": 148000, "operating_income": 108000,
            "net_income": 94000, "operating_cash_flow": 117000, "capital_expenditures": 52000,
            "depreciation_amortization": 18000, "shareholders_equity": 280000,
            "total_assets": 430000, "interest_expense": 500, "current_assets": 180000,
            "current_liabilities": 85000, "goodwill": 30000,
            "accounts_receivable": 48000, "inventory": 0,
            "cash_and_equivalents": 100000, "total_debt": 30000,
            "dividends_paid": -2500, "share_repurchase": -65000,
            "sga_expense": 40000, "rnd_expense": 46000,
            "basic_weighted_average_shares": 12400,
            "income_tax_expense_total": 13000, "income_before_tax_total": 107000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        "FY2025": {
            "revenue": 395000, "cost_of_revenue": 163000, "operating_income": 125000,
            "net_income": 108000, "operating_cash_flow": 130000, "capital_expenditures": 60000,
            "depreciation_amortization": 20000, "shareholders_equity": 300000,
            "total_assets": 460000, "interest_expense": 500, "current_assets": 190000,
            "current_liabilities": 90000, "goodwill": 30000,
            "accounts_receivable": 52000, "inventory": 0,
            "cash_and_equivalents": 95000, "total_debt": 32000,
            "dividends_paid": -5000, "share_repurchase": -70000,
            "sga_expense": 42000, "rnd_expense": 50000,
            "basic_weighted_average_shares": 12100,
            "income_tax_expense_total": 15000, "income_before_tax_total": 123000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
    },
    "downstream": [
        {"customer_name": "广告主", "revenue_pct": 0.75, "is_recurring": True,
         "revenue_type": "ad_revenue", "product_category": "cloud_infrastructure"},
        {"customer_name": "云客户", "revenue_pct": 0.15, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "cloud_infrastructure"},
        {"customer_name": "其他", "revenue_pct": 0.10, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "consumer_electronics"},
    ],
    "upstream": [
        {"supplier_name": "自研TPU/服务器", "is_sole_source": False, "geographic_location": "全球"},
    ],
    "geo": [
        {"region": "United States", "revenue_share": 0.47},
        {"region": "Europe", "revenue_share": 0.30},
        {"region": "Asia Pacific", "revenue_share": 0.15},
        {"region": "Other", "revenue_share": 0.08},
    ],
    "pricing": [
        {"action": "YouTube Premium 涨价", "price_change_pct": 0.22,
         "product_or_segment": "YouTube", "effective_date": "2023-11",
         "volume_impact_pct": -0.01},
    ],
    "market_share": [
        {"period": "FY2023", "share": 0.89, "source": "全球搜索"},
        {"period": "FY2024", "share": 0.88, "source": "全球搜索"},
        {"period": "FY2025", "share": 0.87, "source": "全球搜索"},
    ],
    "competitive": [
        {"competitor_name": "Microsoft/Bing+AI", "event_type": "product_launch",
         "event_description": "Bing+ChatGPT 搜索挑战 Google",
         "outcome_description": "Bing 份额微升 1%，Google 主导地位未受实质影响",
         "outcome_market_share_change": -0.01},
    ],
    "peers": [
        {"peer_name": "Meta", "metric": "gross_margin", "value": 0.82, "period": "FY2025"},
        {"peer_name": "Meta", "metric": "operating_margin", "value": 0.37, "period": "FY2025"},
        {"peer_name": "Meta", "metric": "net_margin", "value": 0.33, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "AI搜索 Gemini 整合", "status": "delivered"},
        {"narrative": "云业务盈利", "status": "delivered"},
        {"narrative": "YouTube 成为第二大广告平台", "status": "delivered"},
        {"narrative": "Waymo 自动驾驶商业化", "status": "in_progress"},
    ],
    "ownership": [
        {"name": "Sundar Pichai", "title": "CEO", "percent_of_class": 0.04},
    ],
    "exec_comp": [
        {"name": "Sundar Pichai", "title": "CEO", "role_type": "CEO",
         "pay_ratio": 800.0, "stock_awards": 200000, "total_comp": 226000},
    ],
    # Google: 创始人退居幕后，Pichai 稳定运营多年，组织庞大
    "exec_changes": [
        {"name": "Ruth Porat", "title": "President/CIO", "change_type": "joined"},
    ],
    "market": {"price": 175.0, "shares_outstanding": 12100, "discount_rate": 0.043, "market": "US"},
    "guidance": {"revenue_growth": 0.12},
    "home_market": "United States",
}

# ── 5. 阿里巴巴 (BABA) ──────────────────────────────────────
# 单位: 百万人民币
COMPANIES["阿里巴巴 (BABA)"] = {
    "fli": {
        "FY2022": {
            "revenue": 853100, "cost_of_revenue": 533700, "operating_income": 69600,
            "net_income": 47000, "operating_cash_flow": 154200, "capital_expenditures": 42500,
            "depreciation_amortization": 55000, "shareholders_equity": 930000,
            "total_assets": 1840000, "interest_expense": 8000, "current_assets": 730000,
            "current_liabilities": 530000, "goodwill": 280000,
            "accounts_receivable": 52000, "inventory": 38000,
            "cash_and_equivalents": 450000, "total_debt": 185000,
            "dividends_paid": 0, "share_repurchase": -92000,
            "sga_expense": 80000, "rnd_expense": 55000,
            "basic_weighted_average_shares": 21400,
            "income_tax_expense_total": 22000, "income_before_tax_total": 69000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 30000,
        },
        "FY2023": {
            "revenue": 869000, "cost_of_revenue": 547000, "operating_income": 100000,
            "net_income": 72500, "operating_cash_flow": 170000, "capital_expenditures": 35000,
            "depreciation_amortization": 50000, "shareholders_equity": 900000,
            "total_assets": 1810000, "interest_expense": 7500, "current_assets": 700000,
            "current_liabilities": 520000, "goodwill": 270000,
            "accounts_receivable": 48000, "inventory": 35000,
            "cash_and_equivalents": 400000, "total_debt": 175000,
            "dividends_paid": -31000, "share_repurchase": -138000,
            "sga_expense": 72000, "rnd_expense": 52000,
            "basic_weighted_average_shares": 20600,
            "income_tax_expense_total": 25000, "income_before_tax_total": 97500,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        "FY2024": {
            "revenue": 941200, "cost_of_revenue": 589000, "operating_income": 112000,
            "net_income": 80000, "operating_cash_flow": 178000, "capital_expenditures": 40000,
            "depreciation_amortization": 48000, "shareholders_equity": 850000,
            "total_assets": 1780000, "interest_expense": 7000, "current_assets": 680000,
            "current_liabilities": 510000, "goodwill": 260000,
            "accounts_receivable": 50000, "inventory": 36000,
            "cash_and_equivalents": 380000, "total_debt": 168000,
            "dividends_paid": -40000, "share_repurchase": -125000,
            "sga_expense": 68000, "rnd_expense": 53000,
            "basic_weighted_average_shares": 19800,
            "income_tax_expense_total": 28000, "income_before_tax_total": 108000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        "FY2025": {
            "revenue": 1000000, "cost_of_revenue": 618000, "operating_income": 125000,
            "net_income": 90000, "operating_cash_flow": 185000, "capital_expenditures": 55000,
            "depreciation_amortization": 45000, "shareholders_equity": 820000,
            "total_assets": 1750000, "interest_expense": 6500, "current_assets": 660000,
            "current_liabilities": 500000, "goodwill": 255000,
            "accounts_receivable": 52000, "inventory": 37000,
            "cash_and_equivalents": 360000, "total_debt": 160000,
            "dividends_paid": -45000, "share_repurchase": -130000,
            "sga_expense": 65000, "rnd_expense": 56000,
            "basic_weighted_average_shares": 19200,
            "income_tax_expense_total": 30000, "income_before_tax_total": 120000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
    },
    "downstream": [
        {"customer_name": "电商消费者", "revenue_pct": 0.65, "is_recurring": True,
         "revenue_type": "transaction_fee", "product_category": "consumer_electronics"},
        {"customer_name": "云客户", "revenue_pct": 0.15, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "cloud_infrastructure"},
        {"customer_name": "本地生活/物流", "revenue_pct": 0.20, "is_recurring": True,
         "revenue_type": "recurring", "product_category": "consumer_electronics"},
    ],
    "upstream": [
        {"supplier_name": "云基础设施", "is_sole_source": False, "geographic_location": "中国"},
    ],
    "geo": [
        {"region": "China", "revenue_share": 0.90},
        {"region": "Southeast Asia", "revenue_share": 0.05},
        {"region": "Other", "revenue_share": 0.05},
    ],
    "pricing": [],
    "market_share": [
        {"period": "FY2023", "share": 0.40, "source": "中国电商"},
        {"period": "FY2024", "share": 0.36, "source": "中国电商"},
        {"period": "FY2025", "share": 0.33, "source": "中国电商"},
    ],
    "competitive": [
        {"competitor_name": "拼多多", "event_type": "price_war",
         "event_description": "拼多多低价策略持续侵蚀淘宝份额",
         "outcome_description": "阿里份额从 40% 降至 33%",
         "outcome_market_share_change": -0.07},
        {"competitor_name": "抖音电商", "event_type": "new_entry",
         "event_description": "抖音电商 GMV 突破 3 万亿",
         "outcome_description": "直播电商分流大量流量",
         "outcome_market_share_change": -0.03},
    ],
    "peers": [
        {"peer_name": "拼多多", "metric": "gross_margin", "value": 0.65, "period": "FY2025"},
        {"peer_name": "拼多多", "metric": "operating_margin", "value": 0.30, "period": "FY2025"},
        {"peer_name": "拼多多", "metric": "net_margin", "value": 0.28, "period": "FY2025"},
        {"peer_name": "京东", "metric": "gross_margin", "value": 0.15, "period": "FY2025"},
        {"peer_name": "京东", "metric": "operating_margin", "value": 0.03, "period": "FY2025"},
        {"peer_name": "京东", "metric": "net_margin", "value": 0.02, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "重回电商核心", "status": "delivered"},
        {"narrative": "阿里云独立上市", "status": "missed"},
        {"narrative": "AI驱动电商推荐", "status": "delivered"},
        {"narrative": "大规模回购回馈股东", "status": "delivered"},
        {"narrative": "国际化 Lazada/Trendyol", "status": "in_progress"},
    ],
    "ownership": [
        {"name": "蔡崇信", "title": "董事长", "percent_of_class": 1.0},
    ],
    "exec_comp": [
        {"name": "吴泳铭", "title": "CEO", "role_type": "CEO",
         "pay_ratio": 80.0, "stock_awards": 0, "total_comp": 2000},
    ],
    # 阿里: 频繁换帅（张勇→蔡崇信/吴泳铭），组织震荡
    "exec_changes": [
        {"name": "张勇", "title": "前CEO/董事长", "change_type": "departed"},
        {"name": "蔡崇信", "title": "董事长", "change_type": "joined"},
        {"name": "吴泳铭", "title": "CEO", "change_type": "joined"},
    ],
    "market": {"price": 120.0, "shares_outstanding": 19200, "discount_rate": 0.017, "market": "CN"},
    "guidance": {"revenue_growth": 0.06},
    "home_market": "China",
}

# ── 6. 腾讯 (0700.HK) ───────────────────────────────────────
# 单位: 百万人民币
COMPANIES["腾讯 (0700.HK)"] = {
    "fli": {
        "FY2022": {
            "revenue": 554600, "cost_of_revenue": 310400, "operating_income": 113500,
            "net_income": 88200, "operating_cash_flow": 165000, "capital_expenditures": 32000,
            "depreciation_amortization": 45000, "shareholders_equity": 830000,
            "total_assets": 1630000, "interest_expense": 8000, "current_assets": 540000,
            "current_liabilities": 380000, "goodwill": 95000,
            "accounts_receivable": 32000, "inventory": 0,
            "cash_and_equivalents": 310000, "total_debt": 195000,
            "dividends_paid": -22000, "share_repurchase": -34000,
            "sga_expense": 65000, "rnd_expense": 61500,
            "basic_weighted_average_shares": 9530,
            "income_tax_expense_total": 18000, "income_before_tax_total": 106200,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 20000,
        },
        "FY2023": {
            "revenue": 609200, "cost_of_revenue": 325300, "operating_income": 150000,
            "net_income": 115800, "operating_cash_flow": 195000, "capital_expenditures": 30000,
            "depreciation_amortization": 43000, "shareholders_equity": 900000,
            "total_assets": 1700000, "interest_expense": 7500, "current_assets": 560000,
            "current_liabilities": 390000, "goodwill": 90000,
            "accounts_receivable": 35000, "inventory": 0,
            "cash_and_equivalents": 335000, "total_debt": 190000,
            "dividends_paid": -25000, "share_repurchase": -49000,
            "sga_expense": 60000, "rnd_expense": 61400,
            "basic_weighted_average_shares": 9400,
            "income_tax_expense_total": 21000, "income_before_tax_total": 136800,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        "FY2024": {
            "revenue": 660000, "cost_of_revenue": 345000, "operating_income": 175000,
            "net_income": 135000, "operating_cash_flow": 210000, "capital_expenditures": 35000,
            "depreciation_amortization": 42000, "shareholders_equity": 950000,
            "total_assets": 1750000, "interest_expense": 7000, "current_assets": 580000,
            "current_liabilities": 400000, "goodwill": 88000,
            "accounts_receivable": 37000, "inventory": 0,
            "cash_and_equivalents": 350000, "total_debt": 185000,
            "dividends_paid": -30000, "share_repurchase": -112000,
            "sga_expense": 58000, "rnd_expense": 63000,
            "basic_weighted_average_shares": 9200,
            "income_tax_expense_total": 24000, "income_before_tax_total": 159000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        "FY2025": {
            "revenue": 720000, "cost_of_revenue": 370000, "operating_income": 200000,
            "net_income": 155000, "operating_cash_flow": 230000, "capital_expenditures": 40000,
            "depreciation_amortization": 42000, "shareholders_equity": 980000,
            "total_assets": 1800000, "interest_expense": 6500, "current_assets": 600000,
            "current_liabilities": 410000, "goodwill": 85000,
            "accounts_receivable": 40000, "inventory": 0,
            "cash_and_equivalents": 360000, "total_debt": 180000,
            "dividends_paid": -36000, "share_repurchase": -130000,
            "sga_expense": 55000, "rnd_expense": 65000,
            "basic_weighted_average_shares": 9000,
            "income_tax_expense_total": 27000, "income_before_tax_total": 182000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
    },
    "downstream": [
        {"customer_name": "游戏玩家", "revenue_pct": 0.30, "is_recurring": True,
         "revenue_type": "transaction_fee", "product_category": "gaming"},
        {"customer_name": "广告主", "revenue_pct": 0.22, "is_recurring": True,
         "revenue_type": "ad_revenue", "product_category": "social_media"},
        {"customer_name": "金融科技用户", "revenue_pct": 0.30, "is_recurring": True,
         "revenue_type": "transaction_fee", "product_category": "payment"},
        {"customer_name": "企业服务", "revenue_pct": 0.18, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "cloud_infrastructure"},
    ],
    "upstream": [
        {"supplier_name": "CDN/云基础设施", "is_sole_source": False, "geographic_location": "中国"},
    ],
    "geo": [
        {"region": "China", "revenue_share": 0.92},
        {"region": "Southeast Asia", "revenue_share": 0.05},
        {"region": "Other", "revenue_share": 0.03},
    ],
    "pricing": [
        {"action": "游戏内购涨价", "price_change_pct": 0.10,
         "product_or_segment": "王者荣耀/LOL", "effective_date": "2024-01",
         "volume_impact_pct": 0.0},
    ],
    "market_share": [
        {"period": "FY2023", "share": 0.55, "source": "中国游戏收入"},
        {"period": "FY2024", "share": 0.54, "source": "中国游戏收入"},
        {"period": "FY2025", "share": 0.54, "source": "中国游戏收入"},
    ],
    "competitive": [
        {"competitor_name": "网易", "event_type": "product_launch",
         "event_description": "网易逆水寒手游大热",
         "outcome_description": "腾讯游戏份额微降，但微信生态不受影响",
         "outcome_market_share_change": -0.01},
    ],
    "peers": [
        {"peer_name": "网易", "metric": "gross_margin", "value": 0.60, "period": "FY2025"},
        {"peer_name": "网易", "metric": "operating_margin", "value": 0.24, "period": "FY2025"},
        {"peer_name": "网易", "metric": "net_margin", "value": 0.20, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "微信生态商业化（视频号/小程序）", "status": "delivered"},
        {"narrative": "游戏出海全球化", "status": "delivered"},
        {"narrative": "大规模回购+分红", "status": "delivered"},
        {"narrative": "AI大模型应用", "status": "delivered"},
        {"narrative": "金融科技合规稳健", "status": "delivered"},
    ],
    "ownership": [
        {"name": "马化腾", "title": "创始人/CEO", "percent_of_class": 7.4},
    ],
    "exec_comp": [
        {"name": "马化腾", "title": "创始人/CEO", "role_type": "CEO",
         "pay_ratio": 120.0, "stock_awards": 0, "total_comp": 5000},
    ],
    # 腾讯: 马化腾仍在，但已建立事业群赛马机制，总裁刘炽平长期搭档
    "exec_changes": [
        {"name": "刘炽平", "title": "总裁", "change_type": "joined"},
    ],
    "market": {"price": 500.0, "shares_outstanding": 9000, "discount_rate": 0.017, "market": "HK"},
    "guidance": {"revenue_growth": 0.09},
    "home_market": "China",
}

# ── 7. 贵州茅台 (600519.SH) ──────────────────────────────────
# 单位: 百万人民币
COMPANIES["贵州茅台 (600519)"] = {
    "fli": {
        "FY2022": {
            "revenue": 127600, "cost_of_revenue": 10800, "operating_income": 78000,
            "net_income": 62700, "operating_cash_flow": 65000, "capital_expenditures": 5000,
            "depreciation_amortization": 3500, "shareholders_equity": 195000,
            "total_assets": 255000, "interest_expense": 0, "current_assets": 190000,
            "current_liabilities": 45000, "goodwill": 0,
            "accounts_receivable": 2500, "inventory": 38000,
            "cash_and_equivalents": 140000, "total_debt": 0,
            "dividends_paid": -32200, "share_repurchase": 0,
            "sga_expense": 8000, "rnd_expense": 200,
            "basic_weighted_average_shares": 1256,
            "income_tax_expense_total": 18000, "income_before_tax_total": 80700,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        "FY2023": {
            "revenue": 150600, "cost_of_revenue": 12400, "operating_income": 93500,
            "net_income": 74700, "operating_cash_flow": 78000, "capital_expenditures": 6000,
            "depreciation_amortization": 3800, "shareholders_equity": 230000,
            "total_assets": 300000, "interest_expense": 0, "current_assets": 225000,
            "current_liabilities": 52000, "goodwill": 0,
            "accounts_receivable": 2800, "inventory": 42000,
            "cash_and_equivalents": 170000, "total_debt": 0,
            "dividends_paid": -43000, "share_repurchase": 0,
            "sga_expense": 9000, "rnd_expense": 250,
            "basic_weighted_average_shares": 1256,
            "income_tax_expense_total": 22000, "income_before_tax_total": 96700,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        "FY2024": {
            "revenue": 173500, "cost_of_revenue": 14000, "operating_income": 109000,
            "net_income": 86200, "operating_cash_flow": 90000, "capital_expenditures": 7000,
            "depreciation_amortization": 4000, "shareholders_equity": 270000,
            "total_assets": 345000, "interest_expense": 0, "current_assets": 260000,
            "current_liabilities": 58000, "goodwill": 0,
            "accounts_receivable": 3000, "inventory": 46000,
            "cash_and_equivalents": 200000, "total_debt": 0,
            "dividends_paid": -51000, "share_repurchase": 0,
            "sga_expense": 10000, "rnd_expense": 300,
            "basic_weighted_average_shares": 1256,
            "income_tax_expense_total": 25000, "income_before_tax_total": 111200,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        "FY2025": {
            "revenue": 195000, "cost_of_revenue": 15500, "operating_income": 125000,
            "net_income": 98000, "operating_cash_flow": 102000, "capital_expenditures": 8000,
            "depreciation_amortization": 4200, "shareholders_equity": 310000,
            "total_assets": 400000, "interest_expense": 0, "current_assets": 300000,
            "current_liabilities": 65000, "goodwill": 0,
            "accounts_receivable": 3200, "inventory": 50000,
            "cash_and_equivalents": 235000, "total_debt": 0,
            "dividends_paid": -60000, "share_repurchase": 0,
            "sga_expense": 11000, "rnd_expense": 350,
            "basic_weighted_average_shares": 1256,
            "income_tax_expense_total": 28000, "income_before_tax_total": 126000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
    },
    "downstream": [
        {"customer_name": "经销商网络", "revenue_pct": 0.90, "is_recurring": True,
         "revenue_type": "recurring", "product_category": "liquor"},
        {"customer_name": "直销/i茅台", "revenue_pct": 0.10, "is_recurring": True,
         "revenue_type": "recurring", "product_category": "liquor"},
    ],
    "upstream": [
        {"supplier_name": "赤水河流域高粱", "is_sole_source": False, "geographic_location": "贵州"},
        {"supplier_name": "小麦", "is_sole_source": False, "geographic_location": "中国"},
    ],
    "geo": [
        {"region": "China", "revenue_share": 0.99},
        {"region": "Other", "revenue_share": 0.01},
    ],
    "pricing": [
        {"action": "飞天茅台出厂价上调 20%", "price_change_pct": 0.20,
         "product_or_segment": "飞天茅台", "effective_date": "2024-01",
         "volume_impact_pct": 0.0},
    ],
    "market_share": [
        {"period": "FY2023", "share": 0.55, "source": "中国高端白酒"},
        {"period": "FY2024", "share": 0.56, "source": "中国高端白酒"},
        {"period": "FY2025", "share": 0.57, "source": "中国高端白酒"},
    ],
    "competitive": [
        {"competitor_name": "五粮液", "event_type": "price_war",
         "event_description": "五粮液降价促销试图抢份额",
         "outcome_description": "茅台份额不降反升，五粮液价格体系受损",
         "outcome_market_share_change": 0.01},
    ],
    "peers": [
        {"peer_name": "五粮液", "metric": "gross_margin", "value": 0.75, "period": "FY2025"},
        {"peer_name": "五粮液", "metric": "operating_margin", "value": 0.40, "period": "FY2025"},
        {"peer_name": "五粮液", "metric": "net_margin", "value": 0.35, "period": "FY2025"},
        {"peer_name": "泸州老窖", "metric": "gross_margin", "value": 0.85, "period": "FY2025"},
        {"peer_name": "泸州老窖", "metric": "operating_margin", "value": 0.42, "period": "FY2025"},
        {"peer_name": "泸州老窖", "metric": "net_margin", "value": 0.33, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "茅台文化酒战略", "status": "delivered"},
        {"narrative": "i茅台数字化直销", "status": "delivered"},
        {"narrative": "产能扩至5.6万吨", "status": "delivered"},
        {"narrative": "年轻化产品线（茅台冰淇淋/酱香拿铁）", "status": "delivered"},
        {"narrative": "提高分红比例至75%", "status": "delivered"},
    ],
    "ownership": [
        {"name": "丁雄军", "title": "董事长", "percent_of_class": 0.0},
    ],
    "exec_comp": [
        {"name": "丁雄军", "title": "董事长", "role_type": "CEO",
         "pay_ratio": 15.0, "stock_awards": 0, "total_comp": 120},
    ],
    # 茅台: 国企，董事长由贵州省任命，换人是常态
    # 历任: 季克良→袁仁国→李保芳→高卫东→丁雄军，品牌一直在
    "exec_changes": [
        {"name": "张德芹", "title": "新任董事长", "change_type": "joined"},
    ],
    "market": {"price": 1500.0, "shares_outstanding": 1256, "discount_rate": 0.017, "market": "CN"},
    "guidance": {"revenue_growth": 0.12},
    "home_market": "China",
}

# ── 8. 紫金矿业 (601899) ─────────────────────────────────────
# 单位: 百万人民币
COMPANIES["紫金矿业 (601899)"] = {
    "fli": {
        "FY2022": {
            "revenue": 269600, "cost_of_revenue": 220000, "operating_income": 27000,
            "net_income": 20000, "operating_cash_flow": 35000, "capital_expenditures": 25000,
            "depreciation_amortization": 12000, "shareholders_equity": 75000,
            "total_assets": 230000, "interest_expense": 3500, "current_assets": 80000,
            "current_liabilities": 85000, "goodwill": 5000,
            "accounts_receivable": 8000, "inventory": 30000,
            "cash_and_equivalents": 20000, "total_debt": 65000,
            "dividends_paid": -5500, "share_repurchase": 0,
            "sga_expense": 8000, "rnd_expense": 3000,
            "basic_weighted_average_shares": 26400,
            "income_tax_expense_total": 5000, "income_before_tax_total": 25000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 15000,
        },
        "FY2023": {
            "revenue": 295000, "cost_of_revenue": 240000, "operating_income": 30000,
            "net_income": 21000, "operating_cash_flow": 40000, "capital_expenditures": 30000,
            "depreciation_amortization": 14000, "shareholders_equity": 85000,
            "total_assets": 260000, "interest_expense": 4000, "current_assets": 90000,
            "current_liabilities": 95000, "goodwill": 5500,
            "accounts_receivable": 9000, "inventory": 33000,
            "cash_and_equivalents": 22000, "total_debt": 75000,
            "dividends_paid": -6000, "share_repurchase": 0,
            "sga_expense": 9000, "rnd_expense": 3500,
            "basic_weighted_average_shares": 26400,
            "income_tax_expense_total": 5500, "income_before_tax_total": 26500,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 20000,
        },
        "FY2024": {
            "revenue": 340000, "cost_of_revenue": 270000, "operating_income": 38000,
            "net_income": 28000, "operating_cash_flow": 50000, "capital_expenditures": 35000,
            "depreciation_amortization": 16000, "shareholders_equity": 100000,
            "total_assets": 310000, "interest_expense": 4500, "current_assets": 100000,
            "current_liabilities": 105000, "goodwill": 6000,
            "accounts_receivable": 10000, "inventory": 38000,
            "cash_and_equivalents": 25000, "total_debt": 90000,
            "dividends_paid": -8000, "share_repurchase": 0,
            "sga_expense": 10000, "rnd_expense": 4000,
            "basic_weighted_average_shares": 26400,
            "income_tax_expense_total": 7000, "income_before_tax_total": 35000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 25000,
        },
        "FY2025": {
            "revenue": 400000, "cost_of_revenue": 310000, "operating_income": 48000,
            "net_income": 35000, "operating_cash_flow": 60000, "capital_expenditures": 40000,
            "depreciation_amortization": 18000, "shareholders_equity": 120000,
            "total_assets": 360000, "interest_expense": 5000, "current_assets": 110000,
            "current_liabilities": 115000, "goodwill": 6500,
            "accounts_receivable": 12000, "inventory": 42000,
            "cash_and_equivalents": 28000, "total_debt": 100000,
            "dividends_paid": -10000, "share_repurchase": 0,
            "sga_expense": 11000, "rnd_expense": 4500,
            "basic_weighted_average_shares": 26400,
            "income_tax_expense_total": 8000, "income_before_tax_total": 43000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 30000,
        },
    },
    "downstream": [
        {"customer_name": "金矿冶炼/贸易", "revenue_pct": 0.50, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "commodity"},
        {"customer_name": "铜矿冶炼", "revenue_pct": 0.35, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "commodity"},
        {"customer_name": "锌/锂等其他", "revenue_pct": 0.15, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "commodity"},
    ],
    "upstream": [
        {"supplier_name": "自有矿山", "is_sole_source": False, "geographic_location": "多地"},
    ],
    "geo": [
        {"region": "China", "revenue_share": 0.60},
        {"region": "Africa", "revenue_share": 0.15},
        {"region": "Central Asia", "revenue_share": 0.10},
        {"region": "South America", "revenue_share": 0.10},
        {"region": "Other", "revenue_share": 0.05},
    ],
    "pricing": [],
    "market_share": [
        {"period": "FY2023", "share": 0.03, "source": "全球金矿"},
        {"period": "FY2024", "share": 0.04, "source": "全球金矿"},
        {"period": "FY2025", "share": 0.04, "source": "全球金矿"},
    ],
    "competitive": [],
    "peers": [
        {"peer_name": "Barrick Gold", "metric": "gross_margin", "value": 0.35, "period": "FY2025"},
        {"peer_name": "Barrick Gold", "metric": "operating_margin", "value": 0.18, "period": "FY2025"},
        {"peer_name": "Barrick Gold", "metric": "net_margin", "value": 0.12, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "全球矿业巨头 — 储量扩张", "status": "delivered"},
        {"narrative": "铜金并重战略", "status": "delivered"},
        {"narrative": "海外矿山并购扩张", "status": "delivered"},
        {"narrative": "科技矿山数字化", "status": "in_progress"},
    ],
    "ownership": [
        {"name": "陈景河", "title": "创始人/名誉董事长", "percent_of_class": 1.2},
    ],
    "exec_comp": [
        {"name": "邹来昌", "title": "董事长", "role_type": "CEO",
         "pay_ratio": 30.0, "stock_awards": 0, "total_comp": 350},
    ],
    # 紫金: 陈景河退居名誉董事长，邹来昌接任，有继任
    "exec_changes": [
        {"name": "邹来昌", "title": "董事长", "change_type": "joined"},
    ],
    "market": {"price": 20.0, "shares_outstanding": 26400, "discount_rate": 0.017, "market": "CN"},
    "guidance": {"revenue_growth": 0.15},
    "home_market": "China",
}

# ── 9. 特变电工 (600089) ─────────────────────────────────────
# 单位: 百万人民币
COMPANIES["特变电工 (600089)"] = {
    "fli": {
        "FY2022": {
            "revenue": 102000, "cost_of_revenue": 82000, "operating_income": 13500,
            "net_income": 15600, "operating_cash_flow": 10000, "capital_expenditures": 12000,
            "depreciation_amortization": 5000, "shareholders_equity": 48000,
            "total_assets": 160000, "interest_expense": 2500, "current_assets": 65000,
            "current_liabilities": 68000, "goodwill": 2000,
            "accounts_receivable": 18000, "inventory": 22000,
            "cash_and_equivalents": 12000, "total_debt": 40000,
            "dividends_paid": -5000, "share_repurchase": 0,
            "sga_expense": 3500, "rnd_expense": 4000,
            "basic_weighted_average_shares": 3800,
            "income_tax_expense_total": 2500, "income_before_tax_total": 18100,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 15000,
        },
        "FY2023": {
            "revenue": 95000, "cost_of_revenue": 80000, "operating_income": 8500,
            "net_income": 8000, "operating_cash_flow": 12000, "capital_expenditures": 10000,
            "depreciation_amortization": 5500, "shareholders_equity": 52000,
            "total_assets": 175000, "interest_expense": 2800, "current_assets": 70000,
            "current_liabilities": 72000, "goodwill": 2000,
            "accounts_receivable": 20000, "inventory": 24000,
            "cash_and_equivalents": 10000, "total_debt": 45000,
            "dividends_paid": -4500, "share_repurchase": 0,
            "sga_expense": 3800, "rnd_expense": 4200,
            "basic_weighted_average_shares": 3800,
            "income_tax_expense_total": 2000, "income_before_tax_total": 10000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 18000,
        },
        "FY2024": {
            "revenue": 88000, "cost_of_revenue": 76000, "operating_income": 6000,
            "net_income": 5500, "operating_cash_flow": 9000, "capital_expenditures": 8000,
            "depreciation_amortization": 6000, "shareholders_equity": 54000,
            "total_assets": 185000, "interest_expense": 3000, "current_assets": 72000,
            "current_liabilities": 75000, "goodwill": 2000,
            "accounts_receivable": 22000, "inventory": 25000,
            "cash_and_equivalents": 8000, "total_debt": 50000,
            "dividends_paid": -3500, "share_repurchase": 0,
            "sga_expense": 3600, "rnd_expense": 4500,
            "basic_weighted_average_shares": 3800,
            "income_tax_expense_total": 1500, "income_before_tax_total": 7000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 20000,
        },
        "FY2025": {
            "revenue": 85000, "cost_of_revenue": 73500, "operating_income": 5500,
            "net_income": 5000, "operating_cash_flow": 8500, "capital_expenditures": 7500,
            "depreciation_amortization": 6500, "shareholders_equity": 55000,
            "total_assets": 190000, "interest_expense": 3200, "current_assets": 73000,
            "current_liabilities": 77000, "goodwill": 2000,
            "accounts_receivable": 23000, "inventory": 26000,
            "cash_and_equivalents": 7000, "total_debt": 52000,
            "dividends_paid": -3000, "share_repurchase": 0,
            "sga_expense": 3500, "rnd_expense": 4800,
            "basic_weighted_average_shares": 3800,
            "income_tax_expense_total": 1300, "income_before_tax_total": 6300,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 22000,
        },
    },
    "downstream": [
        {"customer_name": "输变电设备", "revenue_pct": 0.40, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "industrial_equipment",
         "segment_gross_margin": 0.25},
        {"customer_name": "新能源（多晶硅/光伏）", "revenue_pct": 0.35, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "commodity",
         "segment_gross_margin": 0.02},
        {"customer_name": "新材料/煤炭", "revenue_pct": 0.25, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "commodity",
         "segment_gross_margin": 0.15},
    ],
    "upstream": [
        {"supplier_name": "铜/钢材", "is_sole_source": False, "geographic_location": "中国"},
        {"supplier_name": "硅料", "is_sole_source": False, "geographic_location": "新疆"},
    ],
    "geo": [
        {"region": "China", "revenue_share": 0.85},
        {"region": "Central Asia", "revenue_share": 0.10},
        {"region": "Other", "revenue_share": 0.05},
    ],
    "pricing": [],
    "market_share": [
        {"period": "FY2023", "share": 0.10, "source": "中国输变电设备"},
        {"period": "FY2024", "share": 0.10, "source": "中国输变电设备"},
        {"period": "FY2025", "share": 0.10, "source": "中国输变电设备"},
    ],
    "competitive": [
        {"competitor_name": "通威股份", "event_type": "price_war",
         "event_description": "多晶硅价格从 30万/吨 暴跌至 4万/吨",
         "outcome_description": "全行业亏损，特变电工多晶硅业务利润归零",
         "outcome_market_share_change": 0.0},
    ],
    "peers": [
        # 输变电设备竞对
        {"peer_name": "许继电气", "metric": "gross_margin", "value": 0.22, "period": "FY2025",
         "segment": "输变电设备"},
        {"peer_name": "许继电气", "metric": "operating_margin", "value": 0.08, "period": "FY2025",
         "segment": "输变电设备"},
        {"peer_name": "西门子能源", "metric": "gross_margin", "value": 0.20, "period": "FY2025",
         "segment": "输变电设备"},
        # 多晶硅竞对
        {"peer_name": "通威股份", "metric": "gross_margin", "value": 0.05, "period": "FY2025",
         "segment": "新能源（多晶硅/光伏）"},
        {"peer_name": "协鑫科技", "metric": "gross_margin", "value": 0.03, "period": "FY2025",
         "segment": "新能源（多晶硅/光伏）"},
        # 煤炭竞对
        {"peer_name": "中国神华", "metric": "gross_margin", "value": 0.30, "period": "FY2025",
         "segment": "新材料/煤炭"},
    ],
    "narratives": [
        {"narrative": "输变电+新能源双主业", "status": "delivered"},
        {"narrative": "多晶硅产能扩张", "status": "delivered"},
        {"narrative": "一带一路输变电出口", "status": "delivered"},
        {"narrative": "硅料价格回升盈利恢复", "status": "missed"},
    ],
    "ownership": [
        {"name": "张新", "title": "创始人/董事长", "percent_of_class": 15.0},
    ],
    "exec_comp": [
        {"name": "张新", "title": "创始人/董事长", "role_type": "CEO",
         "pay_ratio": 20.0, "stock_awards": 0, "total_comp": 200},
    ],
    # 特变: 张新家族控制，有一定继任安排
    "exec_changes": [
        {"name": "黄汉杰", "title": "总裁", "change_type": "joined"},
    ],
    "market": {"price": 12.0, "shares_outstanding": 3800, "discount_rate": 0.017, "market": "CN"},
    "guidance": {},
    "home_market": "China",
}


# ══════════════════════════════════════════════════════════════
#  构建 Context
# ══════════════════════════════════════════════════════════════

def build_ctx(company_data):
    fli_data = company_data["fli"]
    all_fli = pd.concat([_fli(v, k) for k, v in fli_data.items()], ignore_index=True)

    # executive_changes 需要特殊处理（直接构建 DataFrame）
    ec_rows = company_data.get("exec_changes", [])
    exec_changes = pd.DataFrame([{"id": i, **r} for i, r in enumerate(ec_rows)]) if ec_rows else EMPTY

    ctx = ComputeContext(company_id=1, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli_data["FY2025"]),
        "financial_line_items_all": all_fli,
        "downstream_segments": _df(company_data.get("downstream", []), DS),
        "upstream_segments": _df(company_data.get("upstream", []), US),
        "geographic_revenues": _df(company_data.get("geo", []), GR),
        "pricing_actions": _df(company_data.get("pricing", []), PA),
        "market_share_data": _df(company_data.get("market_share", []), MS),
        "competitive_dynamics": _df(company_data.get("competitive", []), CD),
        "peer_financials": pd.DataFrame(company_data.get("peers", [])),
        "company_narratives": _df(company_data.get("narratives", []), CN),
        "stock_ownership": _df(company_data.get("ownership", []), SO),
        "executive_compensations": _df(company_data.get("exec_comp", []), EC),
        "debt_obligations": EMPTY, "debt_obligations_all": EMPTY,
        "litigations": EMPTY, "operational_issues": EMPTY,
        "related_party_transactions": EMPTY, "non_financial_kpis": EMPTY,
        "audit_opinions": EMPTY, "known_issues": EMPTY,
        "insider_transactions": EMPTY, "executive_changes": exec_changes,
        "equity_offerings": EMPTY, "analyst_estimates": EMPTY,
        "management_guidance": EMPTY, "management_acknowledgments": EMPTY,
        "brand_signals": EMPTY,
    }
    return ctx


def compute_all_features(ctx):
    for level in (FeatureLevel.L0, FeatureLevel.L1, FeatureLevel.L2):
        for feat in get_features(level=level):
            try:
                r = feat.compute_fn(ctx)
            except Exception:
                continue
            if r:
                ctx.features[feat.name] = r.value
    return ctx


# ══════════════════════════════════════════════════════════════
#  主程序
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    for name, data in COMPANIES.items():
        print("\n" + "▓" * 65)
        print(f"  巴菲特因果链 · {name}")
        print("▓" * 65)

        ctx = build_ctx(data)
        compute_all_features(ctx)
        print(f"\n  已算特征: {len(ctx.features)} 个")

        # ── 线 1: 生意评估 ──
        print("\n" + "=" * 65)
        print("  线 1: 生意评估")
        print("=" * 65)

        # 1. 护城河
        moat = assess_moat(ctx)
        print(format_moat(moat))

        # 2. 盈余能力
        earnings = assess_earnings(ctx)
        print(format_earnings(earnings))

        # 3. 利润分配
        dist = assess_distribution(ctx)
        print(format_distribution(dist))

        # 4. 可预测
        pred = assess_predictability(ctx, moat_depth=moat.depth)
        print(format_predictability(pred))

        # 5+6. 可估值 + 安全边际
        mkt = data.get("market", {})
        guidance = data.get("guidance", {})
        market_code = mkt.get("market", "US")

        # 确定性等级: 护城河 deep/extreme + 盈余 holds + 可预测 holds → 高确定性
        line1_quality = (moat.depth in ("extreme", "deep") and
                         earnings.verdict == "holds" and
                         pred.verdict == "holds")
        certainty = "high" if line1_quality else "normal"

        print("\n  可估值 + 安全边际")
        print("  ════════════════════════════════════════════════")
        certainty_labels = {"high": "高确定性 → 无风险利率", "normal": "普通 → 无风险+ERP"}
        print(f"  确定性: {certainty_labels[certainty]}")

        if mkt.get("discount_rate") and mkt.get("shares_outstanding"):
            dcf = compute_intrinsic_value(
                ctx.features, guidance, mkt["discount_rate"],
                mkt["shares_outstanding"], market=market_code,
                certainty=certainty)
            if dcf.intrinsic_value:
                price = mkt.get("price", 0)
                mos = (dcf.intrinsic_value - price) / dcf.intrinsic_value if dcf.intrinsic_value else 0
                unit = "元/股" if market_code in ("CN", "HK") else "$/股"
                print(f"  路径 {dcf.valuation_path}: 内在价值 {dcf.intrinsic_value:,.1f} {unit}")
                print(f"  当前股价: {price} {unit}")
                print(f"  安全边际: {mos:.1%}")
                print(f"  假设: {dcf.key_assumptions}")
            else:
                print(f"  DCF 状态: {dcf.status}")
                mos = None

            oe = ctx.features.get("l0.company.owner_earnings", 0)
            if oe > 0 and mkt.get("price"):
                rdcf = reverse_dcf(mkt["price"], oe, mkt["discount_rate"],
                                   mkt["shares_outstanding"], market=market_code,
                                   certainty=certainty)
                if rdcf.implied_growth_rate is not None:
                    actual = ctx.features.get("l0.company.revenue_growth_yoy", 0)
                    print(f"\n  反向 DCF: 隐含增速 {rdcf.implied_growth_rate:.1%}  "
                          f"实际 {actual:.1%}  "
                          f"偏差 {rdcf.implied_growth_rate - actual:+.1%}")
        else:
            print("  缺市场数据，跳过估值")
            dcf = None
            mos = None

        # ── 线 2: 人和环境 ──
        print("\n" + "=" * 65)
        print("  线 2: 人和环境")
        print("=" * 65)

        home = data.get("home_market", "")
        integrity = assess_integrity(ctx)
        print(format_integrity(integrity))

        character = assess_character(ctx)
        print(format_character(character))

        risk = assess_risk(ctx, home_market=home)
        print(format_risk(risk))

        # ── 综合判断 ──
        print("=" * 65)
        print("  综合判断")
        print("=" * 65)

        print(f"\n  线 1:")
        print(f"    护城河:   {moat.depth} — {moat.summary}")
        print(f"    盈余能力: {earnings.verdict} — {earnings.summary}")
        print(f"    利润分配: {dist.verdict} — {dist.summary}")
        print(f"    可预测:   {pred.verdict} — {pred.summary}")
        if dcf and dcf.intrinsic_value:
            unit = "元/股" if market_code in ("CN", "HK") else "$/股"
            print(f"    可估值:   valued — {dcf.intrinsic_value:,.1f} {unit} (路径 {dcf.valuation_path})")
            if mos is not None:
                print(f"    安全边际: {mos:.1%}")

        print(f"\n  线 2:")
        print(f"    诚信:     {integrity.verdict} — {integrity.summary}")
        print(f"    管理层:   {character.conviction} — {character.summary}")
        print(f"    风险:     {'灾难性' if risk.has_catastrophic else (f'{len(risk.significant)} 项重大' if risk.significant else '可控')} — {risk.summary}")

        # 最终
        line1_ok = (moat.depth not in ("none", "unknown") and
                    earnings.verdict == "holds" and
                    dist.verdict == "holds")
        line2_ok = (integrity.verdict != "breaks" and not risk.has_catastrophic)

        print(f"\n  {'─' * 55}")
        if line1_ok and line2_ok:
            print(f"  结论: ✅ 生意好 + 人可信 + 风险可控 → 可以投资")
        elif line1_ok and not line2_ok:
            if risk.has_catastrophic:
                print(f"  结论: ⚠️ 好生意但有灾难性风险 → 不能买")
            else:
                print(f"  结论: ⚠️ 好生意但诚信存疑 → 需谨慎")
        else:
            broken = []
            if moat.depth in ("none", "unknown"):
                broken.append("护城河")
            if earnings.verdict != "holds":
                broken.append("盈余能力")
            if dist.verdict != "holds":
                broken.append("利润分配")
            print(f"  结论: ❌ 生意链问题在 {', '.join(broken) if broken else '综合不足'}")
        print()
