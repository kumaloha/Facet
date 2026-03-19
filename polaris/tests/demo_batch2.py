"""
巴菲特因果链 · 第二批 6 家公司
安达保险 / 达美乐 / 亚马逊 / 达维塔 / 美国运通 / 美国银行
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

def _fli(items, period="FY2025"):
    return pd.DataFrame([
        {"id": i, "statement_id": 1, "item_key": k, "item_label": k,
         "value": v, "parent_key": None, "ordinal": i, "note": None, "period": period}
        for i, (k, v) in enumerate(items.items())
    ])

def _df(rows, defaults):
    if not rows: return EMPTY
    return pd.DataFrame([{**defaults, "id": i, **r} for i, r in enumerate(rows)])

DS = {"company_id": 1, "period": "FY2025", "segment": None, "customer_type": None,
      "products": None, "channels": None, "revenue": None, "growth_yoy": None,
      "backlog": None, "backlog_note": None, "pricing_model": None,
      "contract_duration": None, "recognition_method": None, "description": None,
      "raw_post_id": None, "created_at": "2025-01-01"}
CD = {"company_id": 1, "estimated_investment": None, "outcome_market_share_change": None,
      "event_date": "2024-01", "raw_post_id": None, "created_at": "2025-01-01"}
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

COMPANIES = {}

# ── 1. 安达保险 (CB) — 全球最大上市财险公司 ──────────────────
# 单位: 百万美元
COMPANIES["安达保险 (CB)"] = {
    "fli": {
        "FY2022": {"revenue": 43100, "cost_of_revenue": 27000, "operating_income": 7000,
            "net_income": 5400, "operating_cash_flow": 10500, "capital_expenditures": 600,
            "depreciation_amortization": 1200, "shareholders_equity": 54000,
            "total_assets": 199000, "interest_expense": 700, "current_assets": 45000,
            "current_liabilities": 40000, "goodwill": 18000,
            "accounts_receivable": 14000, "inventory": 0,
            "cash_and_equivalents": 3500, "total_debt": 14000,
            "dividends_paid": -1400, "share_repurchase": -2800,
            "sga_expense": 7000, "rnd_expense": 0,
            "basic_weighted_average_shares": 422,
            "income_tax_expense_total": 900, "income_before_tax_total": 6300,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 2000},
        "FY2023": {"revenue": 49700, "cost_of_revenue": 30000, "operating_income": 9800,
            "net_income": 9000, "operating_cash_flow": 12600, "capital_expenditures": 700,
            "depreciation_amortization": 1300, "shareholders_equity": 60500,
            "total_assets": 231000, "interest_expense": 800, "current_assets": 50000,
            "current_liabilities": 43000, "goodwill": 18500,
            "accounts_receivable": 15500, "inventory": 0,
            "cash_and_equivalents": 3200, "total_debt": 15500,
            "dividends_paid": -1500, "share_repurchase": -3500,
            "sga_expense": 7500, "rnd_expense": 0,
            "basic_weighted_average_shares": 415,
            "income_tax_expense_total": 1300, "income_before_tax_total": 10300,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 1500},
        "FY2024": {"revenue": 55800, "cost_of_revenue": 33500, "operating_income": 11200,
            "net_income": 9300, "operating_cash_flow": 14200, "capital_expenditures": 800,
            "depreciation_amortization": 1400, "shareholders_equity": 65000,
            "total_assets": 247000, "interest_expense": 900, "current_assets": 55000,
            "current_liabilities": 47000, "goodwill": 19000,
            "accounts_receivable": 17000, "inventory": 0,
            "cash_and_equivalents": 2800, "total_debt": 16500,
            "dividends_paid": -1600, "share_repurchase": -4500,
            "sga_expense": 8000, "rnd_expense": 0,
            "basic_weighted_average_shares": 408,
            "income_tax_expense_total": 1500, "income_before_tax_total": 10800,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 2000},
        "FY2025": {"revenue": 60000, "cost_of_revenue": 35500, "operating_income": 12500,
            "net_income": 10000, "operating_cash_flow": 15000, "capital_expenditures": 800,
            "depreciation_amortization": 1500, "shareholders_equity": 68000,
            "total_assets": 260000, "interest_expense": 1000, "current_assets": 58000,
            "current_liabilities": 50000, "goodwill": 19500,
            "accounts_receivable": 18000, "inventory": 0,
            "cash_and_equivalents": 3000, "total_debt": 17000,
            "dividends_paid": -1700, "share_repurchase": -5000,
            "sga_expense": 8500, "rnd_expense": 0,
            "basic_weighted_average_shares": 400,
            "income_tax_expense_total": 1600, "income_before_tax_total": 11600,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 1500},
    },
    "downstream": [
        {"customer_name": "商业财产险", "revenue_pct": 0.40, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "insurance",
         "switching_cost_level": "high", "contract_duration": "1-year", "product_criticality": "high"},
        {"customer_name": "个人意外/健康险", "revenue_pct": 0.20, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "insurance",
         "switching_cost_level": "high", "product_criticality": "high"},
        {"customer_name": "再保险", "revenue_pct": 0.15, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "insurance"},
        {"customer_name": "寿险/海外", "revenue_pct": 0.25, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "insurance"},
    ],
    "upstream": [],
    "geo": [
        {"region": "United States", "revenue_share": 0.45},
        {"region": "Europe", "revenue_share": 0.20},
        {"region": "Asia Pacific", "revenue_share": 0.20},
        {"region": "Latin America", "revenue_share": 0.15},
    ],
    "pricing": [
        {"action": "商业险费率上调 8%", "price_change_pct": 0.08,
         "product_or_segment": "商业财产险", "effective_date": "2024-01", "volume_impact_pct": 0.02},
    ],
    "market_share": [
        {"period": "FY2023", "share": 0.04, "source": "全球财险"},
        {"period": "FY2024", "share": 0.04, "source": "全球财险"},
        {"period": "FY2025", "share": 0.05, "source": "全球财险"},
    ],
    "competitive": [
        {"competitor_name": "AIG", "event_type": "price_war",
         "event_description": "AIG 在商业险市场低价竞争",
         "outcome_description": "Chubb 凭承保纪律保住利润率，AIG 综合成本率恶化",
         "outcome_market_share_change": 0.01},
    ],
    "peers": [
        {"peer_name": "AIG", "metric": "gross_margin", "value": 0.30, "period": "FY2025"},
        {"peer_name": "AIG", "metric": "operating_margin", "value": 0.08, "period": "FY2025"},
        {"peer_name": "AIG", "metric": "net_margin", "value": 0.05, "period": "FY2025"},
        {"peer_name": "Travelers", "metric": "gross_margin", "value": 0.35, "period": "FY2025"},
        {"peer_name": "Travelers", "metric": "operating_margin", "value": 0.12, "period": "FY2025"},
        {"peer_name": "Travelers", "metric": "net_margin", "value": 0.09, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "全球化承保能力覆盖 54 国", "status": "delivered"},
        {"narrative": "综合成本率持续低于 87%", "status": "delivered"},
        {"narrative": "亚太寿险高增长", "status": "delivered"},
        {"narrative": "每年保费增速超行业", "status": "delivered"},
    ],
    "ownership": [{"name": "Evan Greenberg", "title": "CEO/董事长", "percent_of_class": 0.5}],
    "exec_comp": [{"name": "Evan Greenberg", "title": "CEO", "role_type": "CEO",
         "pay_ratio": 280.0, "stock_awards": 15000, "total_comp": 24000}],
    "exec_changes": [{"name": "John Lupica", "title": "Vice Chairman", "change_type": "joined"}],
    "market": {"price": 290.0, "shares_outstanding": 400, "discount_rate": 0.043, "market": "US"},
    "guidance": {"revenue_growth": 0.08},
    "home_market": "United States",
}

# ── 2. 达美乐 (DPZ) — 全球最大披萨连锁 ──────────────────────
# 单位: 百万美元。轻资产加盟模式，负权益（大量回购+借债回馈）
COMPANIES["达美乐 (DPZ)"] = {
    "fli": {
        "FY2022": {"revenue": 4357, "cost_of_revenue": 3200, "operating_income": 768,
            "net_income": 452, "operating_cash_flow": 620, "capital_expenditures": 100,
            "depreciation_amortization": 80, "shareholders_equity": -3800,
            "total_assets": 5500, "interest_expense": 190, "current_assets": 600,
            "current_liabilities": 800, "goodwill": 500,
            "accounts_receivable": 250, "inventory": 70,
            "cash_and_equivalents": 50, "total_debt": 5000,
            "dividends_paid": -160, "share_repurchase": -350,
            "sga_expense": 400, "rnd_expense": 0,
            "basic_weighted_average_shares": 36,
            "income_tax_expense_total": 120, "income_before_tax_total": 572,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 500},
        "FY2023": {"revenue": 4479, "cost_of_revenue": 3260, "operating_income": 820,
            "net_income": 519, "operating_cash_flow": 680, "capital_expenditures": 110,
            "depreciation_amortization": 85, "shareholders_equity": -4100,
            "total_assets": 5700, "interest_expense": 200, "current_assets": 650,
            "current_liabilities": 850, "goodwill": 500,
            "accounts_receivable": 260, "inventory": 75,
            "cash_and_equivalents": 55, "total_debt": 5200,
            "dividends_paid": -180, "share_repurchase": -400,
            "sga_expense": 410, "rnd_expense": 0,
            "basic_weighted_average_shares": 35,
            "income_tax_expense_total": 130, "income_before_tax_total": 649,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 400},
        "FY2024": {"revenue": 4706, "cost_of_revenue": 3390, "operating_income": 879,
            "net_income": 584, "operating_cash_flow": 720, "capital_expenditures": 120,
            "depreciation_amortization": 90, "shareholders_equity": -4400,
            "total_assets": 5900, "interest_expense": 210, "current_assets": 680,
            "current_liabilities": 880, "goodwill": 500,
            "accounts_receivable": 270, "inventory": 78,
            "cash_and_equivalents": 60, "total_debt": 5400,
            "dividends_paid": -200, "share_repurchase": -450,
            "sga_expense": 420, "rnd_expense": 0,
            "basic_weighted_average_shares": 34,
            "income_tax_expense_total": 140, "income_before_tax_total": 724,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 300},
        "FY2025": {"revenue": 4950, "cost_of_revenue": 3530, "operating_income": 950,
            "net_income": 630, "operating_cash_flow": 770, "capital_expenditures": 130,
            "depreciation_amortization": 95, "shareholders_equity": -4700,
            "total_assets": 6100, "interest_expense": 220, "current_assets": 700,
            "current_liabilities": 900, "goodwill": 500,
            "accounts_receivable": 280, "inventory": 80,
            "cash_and_equivalents": 65, "total_debt": 5600,
            "dividends_paid": -220, "share_repurchase": -500,
            "sga_expense": 430, "rnd_expense": 0,
            "basic_weighted_average_shares": 33,
            "income_tax_expense_total": 150, "income_before_tax_total": 780,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 300},
    },
    "downstream": [
        {"customer_name": "加盟商（美国）", "revenue_pct": 0.55, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "food",
         "switching_cost_level": "high", "contract_duration": "5-year"},
        {"customer_name": "加盟商（国际）", "revenue_pct": 0.30, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "food",
         "switching_cost_level": "high", "contract_duration": "5-year"},
        {"customer_name": "自营门店", "revenue_pct": 0.15, "is_recurring": True,
         "revenue_type": "recurring", "product_category": "food"},
    ],
    "upstream": [{"supplier_name": "供应链中心（自建）", "is_sole_source": False, "geographic_location": "美国"}],
    "geo": [{"region": "United States", "revenue_share": 0.55}, {"region": "Europe", "revenue_share": 0.20},
            {"region": "Asia Pacific", "revenue_share": 0.15}, {"region": "Other", "revenue_share": 0.10}],
    "pricing": [{"action": "平均客单价涨 5%", "price_change_pct": 0.05,
         "product_or_segment": "全品类", "effective_date": "2024-03", "volume_impact_pct": 0.01}],
    "market_share": [
        {"period": "FY2023", "share": 0.20, "source": "美国披萨配送"},
        {"period": "FY2024", "share": 0.21, "source": "美国披萨配送"},
        {"period": "FY2025", "share": 0.22, "source": "美国披萨配送"},
    ],
    "competitive": [
        {"competitor_name": "Pizza Hut", "event_type": "price_war",
         "event_description": "Pizza Hut 降价促销试图挽回份额",
         "outcome_description": "Domino's 靠数字化配送优势份额反升",
         "outcome_market_share_change": 0.01},
    ],
    "peers": [
        {"peer_name": "Pizza Hut (Yum)", "metric": "gross_margin", "value": 0.24, "period": "FY2025"},
        {"peer_name": "Pizza Hut (Yum)", "metric": "operating_margin", "value": 0.15, "period": "FY2025"},
        {"peer_name": "Pizza Hut (Yum)", "metric": "net_margin", "value": 0.10, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "全球门店突破 20000 家", "status": "delivered"},
        {"narrative": "数字化配送占比超 80%", "status": "delivered"},
        {"narrative": "Uber Eats 合作扩大渠道", "status": "delivered"},
        {"narrative": "加盟商同店销售持续增长", "status": "delivered"},
    ],
    "ownership": [{"name": "Russell Weiner", "title": "CEO", "percent_of_class": 0.05}],
    "exec_comp": [{"name": "Russell Weiner", "title": "CEO", "role_type": "CEO",
         "pay_ratio": 200.0, "stock_awards": 5000, "total_comp": 9000}],
    "exec_changes": [{"name": "Sandeep Reddy", "title": "CFO", "change_type": "joined"}],
    "market": {"price": 480.0, "shares_outstanding": 33, "discount_rate": 0.043, "market": "US"},
    "guidance": {"revenue_growth": 0.06},
    "home_market": "United States",
}

# ── 3. 亚马逊 (AMZN) ────────────────────────────────────────
# 单位: 百万美元
COMPANIES["亚马逊 (AMZN)"] = {
    "fli": {
        "FY2022": {"revenue": 514000, "cost_of_revenue": 288800, "operating_income": 12200,
            "net_income": -2700, "operating_cash_flow": 46800, "capital_expenditures": 58700,
            "depreciation_amortization": 34300, "shareholders_equity": 146000,
            "total_assets": 462700, "interest_expense": 2400, "current_assets": 146800,
            "current_liabilities": 155400, "goodwill": 20300,
            "accounts_receivable": 27000, "inventory": 34400,
            "cash_and_equivalents": 54300, "total_debt": 67000,
            "dividends_paid": 0, "share_repurchase": -6000,
            "sga_expense": 11900, "rnd_expense": 73200,
            "basic_weighted_average_shares": 10189,
            "income_tax_expense_total": -3200, "income_before_tax_total": -500,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 13000},
        "FY2023": {"revenue": 575000, "cost_of_revenue": 304700, "operating_income": 36900,
            "net_income": 30400, "operating_cash_flow": 84900, "capital_expenditures": 48100,
            "depreciation_amortization": 36500, "shareholders_equity": 182400,
            "total_assets": 528000, "interest_expense": 3200, "current_assets": 167600,
            "current_liabilities": 164900, "goodwill": 22700,
            "accounts_receivable": 32000, "inventory": 34000,
            "cash_and_equivalents": 73400, "total_debt": 58300,
            "dividends_paid": 0, "share_repurchase": 0,
            "sga_expense": 12400, "rnd_expense": 85600,
            "basic_weighted_average_shares": 10304,
            "income_tax_expense_total": 7100, "income_before_tax_total": 37500,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
        "FY2024": {"revenue": 638000, "cost_of_revenue": 330800, "operating_income": 68600,
            "net_income": 59200, "operating_cash_flow": 115900, "capital_expenditures": 83000,
            "depreciation_amortization": 42000, "shareholders_equity": 236300,
            "total_assets": 624600, "interest_expense": 3400, "current_assets": 180600,
            "current_liabilities": 179400, "goodwill": 23500,
            "accounts_receivable": 37000, "inventory": 33000,
            "cash_and_equivalents": 78800, "total_debt": 53600,
            "dividends_paid": 0, "share_repurchase": 0,
            "sga_expense": 13500, "rnd_expense": 100200,
            "basic_weighted_average_shares": 10560,
            "income_tax_expense_total": 10200, "income_before_tax_total": 69400,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
        "FY2025": {"revenue": 700000, "cost_of_revenue": 357000, "operating_income": 80000,
            "net_income": 65000, "operating_cash_flow": 125000, "capital_expenditures": 95000,
            "depreciation_amortization": 48000, "shareholders_equity": 280000,
            "total_assets": 700000, "interest_expense": 3000, "current_assets": 195000,
            "current_liabilities": 190000, "goodwill": 24000,
            "accounts_receivable": 40000, "inventory": 35000,
            "cash_and_equivalents": 85000, "total_debt": 50000,
            "dividends_paid": 0, "share_repurchase": 0,
            "sga_expense": 14000, "rnd_expense": 110000,
            "basic_weighted_average_shares": 10600,
            "income_tax_expense_total": 12000, "income_before_tax_total": 77000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
    },
    "downstream": [
        {"customer_name": "电商消费者", "revenue_pct": 0.40, "is_recurring": True,
         "revenue_type": "transaction_fee", "product_category": "consumer_electronics"},
        {"customer_name": "AWS 云客户", "revenue_pct": 0.18, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "cloud_infrastructure"},
        {"customer_name": "第三方卖家服务", "revenue_pct": 0.24, "is_recurring": True,
         "revenue_type": "transaction_fee", "product_category": "consumer_electronics"},
        {"customer_name": "广告", "revenue_pct": 0.08, "is_recurring": True,
         "revenue_type": "ad_revenue", "product_category": "cloud_infrastructure"},
        {"customer_name": "Prime 订阅", "revenue_pct": 0.06, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "consumer_electronics"},
        {"customer_name": "实体店/其他", "revenue_pct": 0.04, "is_recurring": True,
         "revenue_type": "recurring", "product_category": "grocery"},
    ],
    "upstream": [{"supplier_name": "自建物流/云基础设施", "is_sole_source": False, "geographic_location": "全球"}],
    "geo": [{"region": "United States", "revenue_share": 0.60}, {"region": "Europe", "revenue_share": 0.25},
            {"region": "Asia Pacific", "revenue_share": 0.10}, {"region": "Other", "revenue_share": 0.05}],
    "pricing": [
        {"action": "Prime 年费从 $139 涨到 $149", "price_change_pct": 0.07,
         "product_or_segment": "Prime", "effective_date": "2024-09", "volume_impact_pct": 0.0},
    ],
    "market_share": [
        {"period": "FY2023", "share": 0.38, "source": "美国电商"},
        {"period": "FY2024", "share": 0.38, "source": "美国电商"},
        {"period": "FY2025", "share": 0.39, "source": "美国电商"},
    ],
    "competitive": [
        {"competitor_name": "Shopify/Temu/Shein", "event_type": "new_entry",
         "event_description": "Temu/Shein 低价模式冲击，Shopify 赋能独立卖家",
         "outcome_description": "Amazon 份额稳定在 38-39%，低端受挤压但 Prime 生态黏性强",
         "outcome_market_share_change": 0.0},
    ],
    "peers": [
        {"peer_name": "Microsoft Azure", "metric": "gross_margin", "value": 0.70, "period": "FY2025"},
        {"peer_name": "Microsoft Azure", "metric": "operating_margin", "value": 0.45, "period": "FY2025"},
        {"peer_name": "Microsoft Azure", "metric": "net_margin", "value": 0.36, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "AWS 保持云市场第一", "status": "delivered"},
        {"narrative": "物流当日达覆盖扩大", "status": "delivered"},
        {"narrative": "广告收入高速增长", "status": "delivered"},
        {"narrative": "AI/Bedrock 赋能 AWS", "status": "delivered"},
        {"narrative": "实体零售（Whole Foods）协同", "status": "in_progress"},
    ],
    "ownership": [{"name": "Andy Jassy", "title": "CEO", "percent_of_class": 0.02}],
    "exec_comp": [{"name": "Andy Jassy", "title": "CEO", "role_type": "CEO",
         "pay_ratio": 6000.0, "stock_awards": 200000, "total_comp": 212000}],
    "exec_changes": [{"name": "Brian Olsavsky", "title": "SVP/CFO", "change_type": "joined"}],
    "market": {"price": 200.0, "shares_outstanding": 10600, "discount_rate": 0.043, "market": "US"},
    "guidance": {"revenue_growth": 0.10},
    "home_market": "United States",
}

# ── 4. 达维塔 (DVA) — 美国肾透析双寡头之一 ──────────────────
# 单位: 百万美元。DaVita + Fresenius 控制美国 75% 透析市场
COMPANIES["达维塔 (DVA)"] = {
    "fli": {
        "FY2022": {"revenue": 11610, "cost_of_revenue": 9400, "operating_income": 1500,
            "net_income": 560, "operating_cash_flow": 1800, "capital_expenditures": 700,
            "depreciation_amortization": 650, "shareholders_equity": 1400,
            "total_assets": 17500, "interest_expense": 450, "current_assets": 3500,
            "current_liabilities": 3200, "goodwill": 6000,
            "accounts_receivable": 2000, "inventory": 0,
            "cash_and_equivalents": 500, "total_debt": 8500,
            "dividends_paid": 0, "share_repurchase": -1200,
            "sga_expense": 700, "rnd_expense": 0,
            "basic_weighted_average_shares": 95,
            "income_tax_expense_total": 200, "income_before_tax_total": 760,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 1000},
        "FY2023": {"revenue": 12140, "cost_of_revenue": 9700, "operating_income": 1700,
            "net_income": 692, "operating_cash_flow": 2060, "capital_expenditures": 700,
            "depreciation_amortization": 660, "shareholders_equity": 1200,
            "total_assets": 18000, "interest_expense": 500, "current_assets": 3600,
            "current_liabilities": 3300, "goodwill": 6000,
            "accounts_receivable": 2100, "inventory": 0,
            "cash_and_equivalents": 450, "total_debt": 8800,
            "dividends_paid": 0, "share_repurchase": -1400,
            "sga_expense": 720, "rnd_expense": 0,
            "basic_weighted_average_shares": 88,
            "income_tax_expense_total": 250, "income_before_tax_total": 942,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 500},
        "FY2024": {"revenue": 12820, "cost_of_revenue": 10100, "operating_income": 1900,
            "net_income": 936, "operating_cash_flow": 2200, "capital_expenditures": 750,
            "depreciation_amortization": 680, "shareholders_equity": 900,
            "total_assets": 18500, "interest_expense": 520, "current_assets": 3700,
            "current_liabilities": 3400, "goodwill": 6000,
            "accounts_receivable": 2200, "inventory": 0,
            "cash_and_equivalents": 400, "total_debt": 9000,
            "dividends_paid": 0, "share_repurchase": -1600,
            "sga_expense": 730, "rnd_expense": 0,
            "basic_weighted_average_shares": 80,
            "income_tax_expense_total": 280, "income_before_tax_total": 1216,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 400},
        "FY2025": {"revenue": 13500, "cost_of_revenue": 10600, "operating_income": 2050,
            "net_income": 1000, "operating_cash_flow": 2300, "capital_expenditures": 800,
            "depreciation_amortization": 700, "shareholders_equity": 500,
            "total_assets": 19000, "interest_expense": 530, "current_assets": 3800,
            "current_liabilities": 3500, "goodwill": 6000,
            "accounts_receivable": 2300, "inventory": 0,
            "cash_and_equivalents": 350, "total_debt": 9200,
            "dividends_paid": 0, "share_repurchase": -1500,
            "sga_expense": 750, "rnd_expense": 0,
            "basic_weighted_average_shares": 73,
            "income_tax_expense_total": 300, "income_before_tax_total": 1300,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 300},
    },
    "downstream": [
        {"customer_name": "Medicare/Medicaid 患者", "revenue_pct": 0.70, "is_recurring": True,
         "revenue_type": "recurring", "product_category": "healthcare",
         "switching_cost_level": "high", "product_criticality": "high"},
        {"customer_name": "商业保险患者", "revenue_pct": 0.25, "is_recurring": True,
         "revenue_type": "recurring", "product_category": "healthcare",
         "switching_cost_level": "high", "product_criticality": "high"},
        {"customer_name": "国际业务", "revenue_pct": 0.05, "is_recurring": True,
         "revenue_type": "recurring", "product_category": "healthcare"},
    ],
    "upstream": [{"supplier_name": "透析设备/耗材", "is_sole_source": False, "geographic_location": "美国"}],
    "geo": [{"region": "United States", "revenue_share": 0.92},
            {"region": "Other", "revenue_share": 0.08}],
    "pricing": [
        {"action": "Medicare 透析报销费率上调 2.1%", "price_change_pct": 0.021,
         "product_or_segment": "透析服务", "effective_date": "2025-01", "volume_impact_pct": 0.0},
    ],
    "market_share": [
        {"period": "FY2023", "share": 0.36, "source": "美国透析中心"},
        {"period": "FY2024", "share": 0.37, "source": "美国透析中心"},
        {"period": "FY2025", "share": 0.37, "source": "美国透析中心"},
    ],
    "competitive": [
        {"competitor_name": "Fresenius Medical Care", "event_type": "exit",
         "event_description": "Fresenius 缩减美国透析业务，关闭部分中心",
         "outcome_description": "DaVita 收购退出中心，巩固双寡头地位"},
    ],
    "peers": [
        {"peer_name": "Fresenius MC", "metric": "gross_margin", "value": 0.18, "period": "FY2025"},
        {"peer_name": "Fresenius MC", "metric": "operating_margin", "value": 0.08, "period": "FY2025"},
        {"peer_name": "Fresenius MC", "metric": "net_margin", "value": 0.04, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "透析中心运营效率持续提升", "status": "delivered"},
        {"narrative": "IKC（肾脏综合护理）模式扩张", "status": "delivered"},
        {"narrative": "积极回购（股数从 130M 降到 73M）", "status": "delivered"},
        {"narrative": "居家透析渗透率提升", "status": "in_progress"},
    ],
    "ownership": [{"name": "Javier Rodriguez", "title": "CEO", "percent_of_class": 0.1}],
    "exec_comp": [{"name": "Javier Rodriguez", "title": "CEO", "role_type": "CEO",
         "pay_ratio": 120.0, "stock_awards": 5000, "total_comp": 10000}],
    "exec_changes": [{"name": "Joel Ackerman", "title": "CFO", "change_type": "joined"}],
    "market": {"price": 170.0, "shares_outstanding": 73, "discount_rate": 0.043, "market": "US"},
    "guidance": {"revenue_growth": 0.05},
    "home_market": "United States",
}

# ── 5. 美国运通 (AXP) — 高端支付网络 ────────────────────────
# 单位: 百万美元
COMPANIES["美国运通 (AXP)"] = {
    "fli": {
        "FY2022": {"revenue": 52900, "cost_of_revenue": 31000, "operating_income": 11200,
            "net_income": 7500, "operating_cash_flow": 18800, "capital_expenditures": 1800,
            "depreciation_amortization": 2000, "shareholders_equity": 22000,
            "total_assets": 228000, "interest_expense": 3200, "current_assets": 40000,
            "current_liabilities": 55000, "goodwill": 5000,
            "accounts_receivable": 55000, "inventory": 0,
            "cash_and_equivalents": 33000, "total_debt": 45000,
            "dividends_paid": -1400, "share_repurchase": -3400,
            "sga_expense": 8000, "rnd_expense": 0,
            "basic_weighted_average_shares": 756,
            "income_tax_expense_total": 2200, "income_before_tax_total": 9700,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 15000},
        "FY2023": {"revenue": 60500, "cost_of_revenue": 36000, "operating_income": 12500,
            "net_income": 8200, "operating_cash_flow": 20500, "capital_expenditures": 2000,
            "depreciation_amortization": 2200, "shareholders_equity": 24500,
            "total_assets": 254000, "interest_expense": 4500, "current_assets": 45000,
            "current_liabilities": 60000, "goodwill": 5000,
            "accounts_receivable": 60000, "inventory": 0,
            "cash_and_equivalents": 35000, "total_debt": 50000,
            "dividends_paid": -1600, "share_repurchase": -4000,
            "sga_expense": 8500, "rnd_expense": 0,
            "basic_weighted_average_shares": 738,
            "income_tax_expense_total": 2500, "income_before_tax_total": 10700,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 18000},
        "FY2024": {"revenue": 65900, "cost_of_revenue": 38500, "operating_income": 14000,
            "net_income": 10000, "operating_cash_flow": 22000, "capital_expenditures": 2200,
            "depreciation_amortization": 2400, "shareholders_equity": 28000,
            "total_assets": 271000, "interest_expense": 5000, "current_assets": 48000,
            "current_liabilities": 65000, "goodwill": 5000,
            "accounts_receivable": 65000, "inventory": 0,
            "cash_and_equivalents": 38000, "total_debt": 53000,
            "dividends_paid": -1800, "share_repurchase": -4500,
            "sga_expense": 9000, "rnd_expense": 0,
            "basic_weighted_average_shares": 720,
            "income_tax_expense_total": 2800, "income_before_tax_total": 12800,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 16000},
        "FY2025": {"revenue": 72000, "cost_of_revenue": 42000, "operating_income": 15500,
            "net_income": 11000, "operating_cash_flow": 24000, "capital_expenditures": 2500,
            "depreciation_amortization": 2600, "shareholders_equity": 31000,
            "total_assets": 290000, "interest_expense": 5500, "current_assets": 50000,
            "current_liabilities": 68000, "goodwill": 5000,
            "accounts_receivable": 70000, "inventory": 0,
            "cash_and_equivalents": 40000, "total_debt": 56000,
            "dividends_paid": -2000, "share_repurchase": -5000,
            "sga_expense": 9500, "rnd_expense": 0,
            "basic_weighted_average_shares": 705,
            "income_tax_expense_total": 3000, "income_before_tax_total": 14000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 14000},
    },
    "downstream": [
        {"customer_name": "高端持卡人", "revenue_pct": 0.55, "is_recurring": True,
         "revenue_type": "transaction_fee", "product_category": "payment",
         "switching_cost_level": "high"},
        {"customer_name": "商户网络", "revenue_pct": 0.30, "is_recurring": True,
         "revenue_type": "transaction_fee", "product_category": "payment"},
        {"customer_name": "企业支付", "revenue_pct": 0.15, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "payment"},
    ],
    "upstream": [],
    "geo": [{"region": "United States", "revenue_share": 0.70},
            {"region": "Europe", "revenue_share": 0.15}, {"region": "Other", "revenue_share": 0.15}],
    "pricing": [{"action": "白金卡年费上调", "price_change_pct": 0.10,
         "product_or_segment": "Platinum Card", "effective_date": "2024-07", "volume_impact_pct": 0.0}],
    "market_share": [
        {"period": "FY2023", "share": 0.24, "source": "美国信用卡消费额"},
        {"period": "FY2024", "share": 0.25, "source": "美国信用卡消费额"},
        {"period": "FY2025", "share": 0.25, "source": "美国信用卡消费额"},
    ],
    "competitive": [
        {"competitor_name": "Visa/Mastercard", "event_type": "product_launch",
         "event_description": "Visa 推出高端 Infinite 卡与 Amex 竞争",
         "outcome_description": "Amex 高端市场份额稳固，Millennial 新卡用户持续增长",
         "outcome_market_share_change": 0.01},
    ],
    "peers": [
        {"peer_name": "Visa", "metric": "gross_margin", "value": 0.80, "period": "FY2025"},
        {"peer_name": "Visa", "metric": "operating_margin", "value": 0.67, "period": "FY2025"},
        {"peer_name": "Visa", "metric": "net_margin", "value": 0.53, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "高端消费者品牌忠诚度最高", "status": "delivered"},
        {"narrative": "年轻用户增长（Millennial/Gen Z）", "status": "delivered"},
        {"narrative": "收入连续多年双位数增长", "status": "delivered"},
        {"narrative": "信贷损失率持续低于行业", "status": "delivered"},
    ],
    "ownership": [{"name": "Stephen Squeri", "title": "CEO/董事长", "percent_of_class": 0.03}],
    "exec_comp": [{"name": "Stephen Squeri", "title": "CEO", "role_type": "CEO",
         "pay_ratio": 350.0, "stock_awards": 15000, "total_comp": 28000}],
    "exec_changes": [{"name": "Christophe Le Caillec", "title": "CFO", "change_type": "joined"}],
    "market": {"price": 300.0, "shares_outstanding": 705, "discount_rate": 0.043, "market": "US"},
    "guidance": {"revenue_growth": 0.09},
    "home_market": "United States",
}

# ── 6. 美国银行 (BAC) ───────────────────────────────────────
# 单位: 百万美元
COMPANIES["美国银行 (BAC)"] = {
    "fli": {
        "FY2022": {"revenue": 94900, "cost_of_revenue": 55000, "operating_income": 18500,
            "net_income": 27500, "operating_cash_flow": 35000, "capital_expenditures": 3000,
            "depreciation_amortization": 3500, "shareholders_equity": 241000,
            "total_assets": 3050000, "interest_expense": 19000, "current_assets": 400000,
            "current_liabilities": 350000, "goodwill": 69000,
            "accounts_receivable": 0, "inventory": 0,
            "cash_and_equivalents": 230000, "total_debt": 290000,
            "dividends_paid": -8700, "share_repurchase": -4200,
            "sga_expense": 18000, "rnd_expense": 0,
            "basic_weighted_average_shares": 8060,
            "income_tax_expense_total": 3500, "income_before_tax_total": 31000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 80000},
        "FY2023": {"revenue": 98600, "cost_of_revenue": 57000, "operating_income": 17000,
            "net_income": 26300, "operating_cash_flow": 32000, "capital_expenditures": 3200,
            "depreciation_amortization": 3700, "shareholders_equity": 248000,
            "total_assets": 3180000, "interest_expense": 35000, "current_assets": 410000,
            "current_liabilities": 360000, "goodwill": 69000,
            "accounts_receivable": 0, "inventory": 0,
            "cash_and_equivalents": 260000, "total_debt": 300000,
            "dividends_paid": -7300, "share_repurchase": -4300,
            "sga_expense": 18500, "rnd_expense": 0,
            "basic_weighted_average_shares": 7970,
            "income_tax_expense_total": 3000, "income_before_tax_total": 29300,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 90000},
        "FY2024": {"revenue": 102000, "cost_of_revenue": 58000, "operating_income": 19000,
            "net_income": 27000, "operating_cash_flow": 34000, "capital_expenditures": 3500,
            "depreciation_amortization": 3800, "shareholders_equity": 265000,
            "total_assets": 3260000, "interest_expense": 38000, "current_assets": 420000,
            "current_liabilities": 370000, "goodwill": 69000,
            "accounts_receivable": 0, "inventory": 0,
            "cash_and_equivalents": 280000, "total_debt": 310000,
            "dividends_paid": -7700, "share_repurchase": -5000,
            "sga_expense": 19000, "rnd_expense": 0,
            "basic_weighted_average_shares": 7900,
            "income_tax_expense_total": 3500, "income_before_tax_total": 30500,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 85000},
        "FY2025": {"revenue": 106000, "cost_of_revenue": 60000, "operating_income": 20000,
            "net_income": 30500, "operating_cash_flow": 38000, "capital_expenditures": 3800,
            "depreciation_amortization": 4000, "shareholders_equity": 280000,
            "total_assets": 3350000, "interest_expense": 35000, "current_assets": 430000,
            "current_liabilities": 380000, "goodwill": 69000,
            "accounts_receivable": 0, "inventory": 0,
            "cash_and_equivalents": 290000, "total_debt": 320000,
            "dividends_paid": -8500, "share_repurchase": -5500,
            "sga_expense": 19500, "rnd_expense": 0,
            "basic_weighted_average_shares": 7800,
            "income_tax_expense_total": 4000, "income_before_tax_total": 34500,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 80000},
    },
    "downstream": [
        {"customer_name": "个人银行客户", "revenue_pct": 0.35, "is_recurring": True,
         "revenue_type": "recurring", "product_category": "banking",
         "switching_cost_level": "high"},
        {"customer_name": "企业/机构客户", "revenue_pct": 0.30, "is_recurring": True,
         "revenue_type": "recurring", "product_category": "banking",
         "switching_cost_level": "high", "contract_duration": "multi-year"},
        {"customer_name": "财富管理/Merrill", "revenue_pct": 0.20, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "banking",
         "switching_cost_level": "high"},
        {"customer_name": "交易/市场", "revenue_pct": 0.15, "is_recurring": True,
         "revenue_type": "transaction_fee", "product_category": "banking"},
    ],
    "upstream": [],
    "geo": [{"region": "United States", "revenue_share": 0.85},
            {"region": "Europe", "revenue_share": 0.10}, {"region": "Other", "revenue_share": 0.05}],
    "pricing": [
        {"action": "NII 受益于加息环境", "price_change_pct": 0.08,
         "product_or_segment": "净利息收入", "effective_date": "2023-01", "volume_impact_pct": 0.0},
    ],
    "market_share": [
        {"period": "FY2023", "share": 0.11, "source": "美国银行存款"},
        {"period": "FY2024", "share": 0.11, "source": "美国银行存款"},
        {"period": "FY2025", "share": 0.11, "source": "美国银行存款"},
    ],
    "competitive": [
        {"competitor_name": "JPMorgan", "event_type": "product_launch",
         "event_description": "JPM 持续在财富管理和投行抢份额",
         "outcome_description": "BAC 零售银行基盘稳固，Merrill 财富管理保持第二",
         "outcome_market_share_change": 0.0},
    ],
    "peers": [
        {"peer_name": "JPMorgan", "metric": "gross_margin", "value": 0.45, "period": "FY2025"},
        {"peer_name": "JPMorgan", "metric": "operating_margin", "value": 0.22, "period": "FY2025"},
        {"peer_name": "JPMorgan", "metric": "net_margin", "value": 0.30, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "数字化银行 4700 万活跃用户", "status": "delivered"},
        {"narrative": "成本效率持续改善", "status": "delivered"},
        {"narrative": "NII 随利率正常化回升", "status": "delivered"},
        {"narrative": "Merrill 财富管理规模增长", "status": "delivered"},
    ],
    "ownership": [{"name": "Brian Moynihan", "title": "CEO/董事长", "percent_of_class": 0.01}],
    "exec_comp": [{"name": "Brian Moynihan", "title": "CEO", "role_type": "CEO",
         "pay_ratio": 300.0, "stock_awards": 15000, "total_comp": 29000}],
    "exec_changes": [{"name": "Alastair Borthwick", "title": "CFO", "change_type": "joined"}],
    "market": {"price": 44.0, "shares_outstanding": 7800, "discount_rate": 0.043, "market": "US"},
    "guidance": {"revenue_growth": 0.04},
    "home_market": "United States",
}


# ══════════════════════════════════════════════════════════════
#  构建 + 跑链（复用 demo_batch_buffett 的逻辑）
# ══════════════════════════════════════════════════════════════

def build_ctx(data):
    fli_data = data["fli"]
    all_fli = pd.concat([_fli(v, k) for k, v in fli_data.items()], ignore_index=True)
    ec_rows = data.get("exec_changes", [])
    exec_changes = pd.DataFrame([{"id": i, **r} for i, r in enumerate(ec_rows)]) if ec_rows else EMPTY
    ki_rows = data.get("known_issues", [])
    ma_rows = data.get("management_acks", [])

    ctx = ComputeContext(company_id=1, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli_data["FY2025"]),
        "financial_line_items_all": all_fli,
        "downstream_segments": _df(data.get("downstream", []), DS),
        "upstream_segments": _df(data.get("upstream", []), US),
        "geographic_revenues": _df(data.get("geo", []), GR),
        "pricing_actions": _df(data.get("pricing", []), PA),
        "market_share_data": _df(data.get("market_share", []), MS),
        "competitive_dynamics": _df(data.get("competitive", []), CD),
        "peer_financials": pd.DataFrame(data.get("peers", [])),
        "company_narratives": _df(data.get("narratives", []), CN),
        "stock_ownership": _df(data.get("ownership", []), SO),
        "executive_compensations": _df(data.get("exec_comp", []), EC),
        "debt_obligations": EMPTY, "debt_obligations_all": EMPTY,
        "litigations": EMPTY, "operational_issues": EMPTY,
        "related_party_transactions": EMPTY, "non_financial_kpis": EMPTY,
        "audit_opinions": EMPTY,
        "known_issues": pd.DataFrame([{"id": i, **r} for i, r in enumerate(ki_rows)]) if ki_rows else EMPTY,
        "insider_transactions": EMPTY, "executive_changes": exec_changes,
        "equity_offerings": EMPTY, "analyst_estimates": EMPTY,
        "management_guidance": EMPTY,
        "management_acknowledgments": pd.DataFrame([{"id": i, **r} for i, r in enumerate(ma_rows)]) if ma_rows else EMPTY,
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


if __name__ == "__main__":
    for name, data in COMPANIES.items():
        print("\n" + "▓" * 65)
        print(f"  巴菲特因果链 · {name}")
        print("▓" * 65)

        ctx = build_ctx(data)
        compute_all_features(ctx)

        moat = assess_moat(ctx)
        earnings = assess_earnings(ctx)
        dist = assess_distribution(ctx)
        pred = assess_predictability(ctx, moat_depth=moat.depth)

        mkt = data["market"]
        guidance = data.get("guidance", {})
        market_code = mkt["market"]

        line1_quality = (moat.depth in ("extreme", "deep") and
                         earnings.verdict == "holds" and
                         pred.verdict == "holds")
        certainty = "high" if line1_quality else "normal"

        dcf = None
        mos = None
        if mkt.get("discount_rate") and mkt.get("shares_outstanding"):
            dcf = compute_intrinsic_value(ctx.features, guidance, mkt["discount_rate"],
                                           mkt["shares_outstanding"], market=market_code,
                                           certainty=certainty)
            if dcf.intrinsic_value:
                mos = (dcf.intrinsic_value - mkt["price"]) / dcf.intrinsic_value

        integrity = assess_integrity(ctx)
        character = assess_character(ctx)
        risk = assess_risk(ctx, home_market=data.get("home_market", ""))

        # ── 汇总输出 ──
        line1_ok = (moat.depth not in ("none", "unknown") and
                    earnings.verdict == "holds" and dist.verdict == "holds")
        line2_ok = (integrity.verdict != "breaks" and not risk.has_catastrophic)

        print(f"\n  线 1:")
        print(f"    护城河:   {moat.depth} — {moat.summary[:70]}")
        print(f"    盈余能力: {earnings.verdict} — {earnings.summary[:70]}")
        print(f"    利润分配: {dist.verdict} — {dist.summary[:70]}")
        print(f"    可预测:   {pred.verdict} — {pred.summary[:70]}")
        if dcf and dcf.intrinsic_value:
            cert_tag = "无风险" if certainty == "high" else "无风险+ERP"
            print(f"    估值:     ${dcf.intrinsic_value:,.0f} vs ${mkt['price']}  "
                  f"安全边际 {mos:.0%}  [{cert_tag}, 路径{dcf.valuation_path}]")
        elif dcf:
            print(f"    估值:     {dcf.status}")

        print(f"  线 2:")
        print(f"    诚信:     {integrity.verdict}")
        print(f"    管理层:   {character.conviction} — {character.summary[:50]}")
        kp = [r for r in risk.risks if r.category == "key_person"]
        kp_tag = kp[0].description[:50] if kp else "无"
        print(f"    关键人:   {kp_tag}")
        cat_risk = "灾难性" if risk.has_catastrophic else (f"{len(risk.significant)}项重大" if risk.significant else "可控")
        print(f"    风险:     {cat_risk}")

        if line1_ok and line2_ok:
            print(f"  → ✅ 可以投资")
        elif line1_ok and not line2_ok:
            print(f"  → ⚠️ 好生意但{'灾难性风险' if risk.has_catastrophic else '诚信存疑'}")
        else:
            broken = []
            if moat.depth in ("none", "unknown"): broken.append("护城河")
            if earnings.verdict != "holds": broken.append("盈余")
            if dist.verdict != "holds": broken.append("分配")
            print(f"  → ❌ 断裂: {', '.join(broken)}")
        print()
