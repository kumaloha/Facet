"""
巴菲特因果链 · 联合健康 (UNH) + Intel (INTC)
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

COMPANIES = {}

# ══════════════════════════════════════════════════════════════
#  联合健康 (UNH) — 美国最大健康险+医疗服务
# ══════════════════════════════════════════════════════════════
# 单位: 百万美元
COMPANIES["联合健康 (UNH)"] = {
    "fli": {
        "FY2022": {
            "revenue": 324200, "cost_of_revenue": 265300, "operating_income": 28800,
            "net_income": 20100, "operating_cash_flow": 28500, "capital_expenditures": 3800,
            "depreciation_amortization": 3200, "shareholders_equity": 72400,
            "total_assets": 245700, "interest_expense": 2600, "current_assets": 73000,
            "current_liabilities": 70500, "goodwill": 90200,
            "accounts_receivable": 15000, "inventory": 0,
            "cash_and_equivalents": 23300, "total_debt": 45200,
            "dividends_paid": -5800, "share_repurchase": -7000,
            "sga_expense": 27000, "rnd_expense": 0,
            "basic_weighted_average_shares": 935,
            "income_tax_expense_total": 5000, "income_before_tax_total": 25100,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 8000,
        },
        "FY2023": {
            "revenue": 371600, "cost_of_revenue": 305800, "operating_income": 32800,
            "net_income": 22400, "operating_cash_flow": 30200, "capital_expenditures": 4200,
            "depreciation_amortization": 3500, "shareholders_equity": 80100,
            "total_assets": 273300, "interest_expense": 3100, "current_assets": 78000,
            "current_liabilities": 77000, "goodwill": 97400,
            "accounts_receivable": 17000, "inventory": 0,
            "cash_and_equivalents": 25600, "total_debt": 51000,
            "dividends_paid": -6300, "share_repurchase": -8500,
            "sga_expense": 30000, "rnd_expense": 0,
            "basic_weighted_average_shares": 928,
            "income_tax_expense_total": 5600, "income_before_tax_total": 28000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 10000,
        },
        "FY2024": {
            "revenue": 400700, "cost_of_revenue": 332800, "operating_income": 32500,
            "net_income": 16200, "operating_cash_flow": 24200, "capital_expenditures": 4500,
            "depreciation_amortization": 3800, "shareholders_equity": 78000,
            "total_assets": 290000, "interest_expense": 3500, "current_assets": 80000,
            "current_liabilities": 82000, "goodwill": 101000,
            "accounts_receivable": 19000, "inventory": 0,
            "cash_and_equivalents": 22000, "total_debt": 58000,
            "dividends_paid": -6800, "share_repurchase": -9000,
            "sga_expense": 32000, "rnd_expense": 0,
            "basic_weighted_average_shares": 920,
            "income_tax_expense_total": 4500, "income_before_tax_total": 20700,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 12000,
        },
        # FY2025 reflecting Change Healthcare cyberattack aftermath,
        # rising medical costs, DOJ antitrust probe, CEO death
        "FY2025": {
            "revenue": 420000, "cost_of_revenue": 352000, "operating_income": 30000,
            "net_income": 14500, "operating_cash_flow": 22000, "capital_expenditures": 4800,
            "depreciation_amortization": 4000, "shareholders_equity": 75000,
            "total_assets": 300000, "interest_expense": 3800, "current_assets": 78000,
            "current_liabilities": 85000, "goodwill": 103000,
            "accounts_receivable": 20000, "inventory": 0,
            "cash_and_equivalents": 20000, "total_debt": 62000,
            "dividends_paid": -7200, "share_repurchase": -5000,
            "sga_expense": 34000, "rnd_expense": 0,
            "basic_weighted_average_shares": 915,
            "income_tax_expense_total": 4000, "income_before_tax_total": 18500,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 14000,
        },
    },
    "downstream": [
        # switching_cost_level: 雇主换保险需重签全员合同、重建医生网络、HR系统对接
        # contract_duration: 雇主团体险通常 1-3 年合同
        # product_criticality: 医疗保险对员工/企业都是关键（出事 = 员工看不了病）
        {"customer_name": "雇主团体险", "revenue_pct": 0.35, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "insurance",
         "switching_cost_level": "high", "contract_duration": "multi-year",
         "product_criticality": "high"},
        {"customer_name": "Medicare Advantage", "revenue_pct": 0.30, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "insurance",
         "switching_cost_level": "high", "contract_duration": "1-year",
         "product_criticality": "high"},
        {"customer_name": "Medicaid", "revenue_pct": 0.10, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "insurance",
         "switching_cost_level": "high", "contract_duration": "multi-year",
         "product_criticality": "high"},
        {"customer_name": "Optum Health 医疗服务", "revenue_pct": 0.15, "is_recurring": True,
         "revenue_type": "recurring", "product_category": "healthcare",
         "switching_cost_level": "high", "product_criticality": "high"},
        {"customer_name": "Optum Rx 药品管理", "revenue_pct": 0.10, "is_recurring": True,
         "revenue_type": "recurring", "product_category": "pharma",
         "switching_cost_level": "high", "product_criticality": "high"},
    ],
    "upstream": [
        {"supplier_name": "医院/医生网络", "is_sole_source": False, "geographic_location": "美国"},
        {"supplier_name": "药品分销商", "is_sole_source": False, "geographic_location": "美国"},
    ],
    "geo": [
        {"region": "United States", "revenue_share": 0.95},
        {"region": "Latin America", "revenue_share": 0.03},
        {"region": "Other", "revenue_share": 0.02},
    ],
    "pricing": [
        # 健康险是刚需，涨价客户走不掉：雇主换保险要重签全员、员工换医生网络
        {"action": "Medicare Advantage 保费上调", "price_change_pct": 0.06,
         "product_or_segment": "Medicare Advantage", "effective_date": "2024-01",
         "volume_impact_pct": 0.02},
        {"action": "雇主团体险保费上调", "price_change_pct": 0.08,
         "product_or_segment": "雇主团体险", "effective_date": "2024-07",
         "volume_impact_pct": -0.01},
    ],
    "market_share": [
        # 按 Medicare Advantage 会员口径（UNH 稳居第一）
        {"period": "FY2023", "share": 0.29, "source": "Medicare Advantage 会员"},
        {"period": "FY2024", "share": 0.29, "source": "Medicare Advantage 会员"},
        {"period": "FY2025", "share": 0.28, "source": "Medicare Advantage 会员"},
    ],
    "competitive": [
        {"competitor_name": "Humana", "event_type": "price_war",
         "event_description": "Humana 在 Medicare Advantage 市场低价竞争",
         "outcome_description": "UNH 保住大部分份额，Humana 因亏损退出部分市场",
         "outcome_market_share_change": 0.0},
        {"competitor_name": "Amazon/One Medical", "event_type": "new_entry",
         "event_description": "Amazon 通过收购 One Medical 进入初级医疗",
         "outcome_description": "Amazon 规模尚小，无法替代 Optum 的 9 万医生网络，影响有限",
         "outcome_market_share_change": 0.0},
        # 转换成本行为证据: 雇主评估替代品但最终留下
        {"competitor_name": "Elevance Health", "event_type": "price_war",
         "event_description": "多家大雇主评估 Elevance 作为 UNH 的替代方案",
         "outcome_description": "因 Optum 闭环数据和医生网络覆盖无法替代，雇主仍留在 UNH",
         "outcome_market_share_change": 0.0},
        # 监管壁垒: 新进入者拿不到牌照
        {"competitor_name": "Oscar Health", "event_type": "new_entry",
         "event_description": "Oscar Health 等互联网保险新进入者试图冲击传统健康险",
         "outcome_description": "Oscar 持续亏损，50州牌照+精算合规壁垒极高，份额不到1%",
         "outcome_market_share_change": 0.0},
        # 有效规模证据: 小玩家退出
        {"competitor_name": "Bright Health", "event_type": "exit",
         "event_description": "Bright Health 退出保险市场",
         "outcome_description": "融资烧钱模式无法跑通，医疗网络谈判没有规模优势"},
        {"competitor_name": "Clover Health", "event_type": "exit",
         "event_description": "Clover Health 大幅缩减 Medicare Advantage 业务",
         "outcome_description": "亏损严重退出多个市场，规模不足无法控制医疗成本"},
    ],
    "peers": [
        {"peer_name": "Elevance Health", "metric": "gross_margin", "value": 0.17, "period": "FY2025"},
        {"peer_name": "Elevance Health", "metric": "operating_margin", "value": 0.05, "period": "FY2025"},
        {"peer_name": "Elevance Health", "metric": "net_margin", "value": 0.03, "period": "FY2025"},
        {"peer_name": "Cigna", "metric": "gross_margin", "value": 0.12, "period": "FY2025"},
        {"peer_name": "Cigna", "metric": "operating_margin", "value": 0.04, "period": "FY2025"},
        {"peer_name": "Cigna", "metric": "net_margin", "value": 0.02, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "Optum 闭环医疗生态", "status": "delivered"},
        {"narrative": "数据驱动降低医疗成本", "status": "delivered"},
        {"narrative": "Change Healthcare 整合完成", "status": "missed"},
        {"narrative": "国际市场扩张（巴西/智利）", "status": "in_progress"},
        {"narrative": "AI 赋能理赔处理", "status": "delivered"},
        {"narrative": "每年两位数 EPS 增长", "status": "missed"},
    ],
    "ownership": [
        {"name": "Andrew Witty", "title": "CEO (临时)", "percent_of_class": 0.01},
    ],
    "exec_comp": [
        {"name": "Andrew Witty", "title": "CEO", "role_type": "CEO",
         "pay_ratio": 350.0, "stock_awards": 15000, "total_comp": 21000},
    ],
    # 2024-2025: Change Healthcare breach fallout, DOJ probe, CEO assassination
    # → known issues + litigations
    "known_issues": [
        {"issue_description": "Change Healthcare 网络攻击导致数十亿美元损失和数据泄露",
         "severity": "critical", "source_type": "news"},
        {"issue_description": "DOJ 反垄断调查 Optum 与保险业务利益冲突",
         "severity": "high", "source_type": "regulatory"},
        {"issue_description": "CEO Brian Thompson 遇刺引发公众对保险行业的强烈不满",
         "severity": "high", "source_type": "news"},
    ],
    "management_acks": [
        {"issue_description": "承认 Change Healthcare 事件造成重大财务和运营影响",
         "response_quality": "adequate", "has_action_plan": True},
        {"issue_description": "DOJ 反垄断调查回应: 合规记录良好，法院特别主事官认定无不当行为证据",
         "response_quality": "defensive", "has_action_plan": True},
        {"issue_description": "CEO Thompson 遇刺后公司表示哀悼，Andrew Witty 接任临时 CEO",
         "response_quality": "adequate", "has_action_plan": False},
    ],
    "litigations": [
        {"status": "ongoing", "claimed_amount": 5000,
         "description": "Change Healthcare 数据泄露集体诉讼"},
        {"status": "pending", "claimed_amount": 0,
         "description": "DOJ 反垄断调查"},
        {"status": "ongoing", "claimed_amount": 2000,
         "description": "州级健康险理赔不当诉讼"},
    ],
    "market": {"price": 330.0, "shares_outstanding": 915, "discount_rate": 0.043, "market": "US"},
    "guidance": {"revenue_growth": 0.05},
    "home_market": "United States",
}

# ══════════════════════════════════════════════════════════════
#  Intel (INTC) — 半导体（衰落期）
# ══════════════════════════════════════════════════════════════
# 单位: 百万美元
COMPANIES["Intel (INTC)"] = {
    "fli": {
        "FY2022": {
            "revenue": 63100, "cost_of_revenue": 36300, "operating_income": 2300,
            "net_income": 8000, "operating_cash_flow": 15400, "capital_expenditures": 25100,
            "depreciation_amortization": 11500, "shareholders_equity": 101300,
            "total_assets": 182100, "interest_expense": 600, "current_assets": 50200,
            "current_liabilities": 32900, "goodwill": 27600,
            "accounts_receivable": 4100, "inventory": 13200,
            "cash_and_equivalents": 28300, "total_debt": 37700,
            "dividends_paid": -6000, "share_repurchase": 0,
            "sga_expense": 7200, "rnd_expense": 17500,
            "basic_weighted_average_shares": 4137,
            "income_tax_expense_total": -500, "income_before_tax_total": 7500,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 6200,
        },
        "FY2023": {
            "revenue": 54200, "cost_of_revenue": 32500, "operating_income": 500,
            "net_income": 1700, "operating_cash_flow": 11500, "capital_expenditures": 25800,
            "depreciation_amortization": 10600, "shareholders_equity": 106000,
            "total_assets": 191600, "interest_expense": 800, "current_assets": 47600,
            "current_liabilities": 28800, "goodwill": 27600,
            "accounts_receivable": 3400, "inventory": 11200,
            "cash_and_equivalents": 25000, "total_debt": 49300,
            "dividends_paid": -3100, "share_repurchase": 0,
            "sga_expense": 5500, "rnd_expense": 16000,
            "basic_weighted_average_shares": 4163,
            "income_tax_expense_total": -300, "income_before_tax_total": 1400,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 11000,
        },
        "FY2024": {
            "revenue": 53100, "cost_of_revenue": 35400, "operating_income": -3900,
            "net_income": -18700, "operating_cash_flow": 8300, "capital_expenditures": 21800,
            "depreciation_amortization": 9800, "shareholders_equity": 87300,
            "total_assets": 196500, "interest_expense": 1200, "current_assets": 40400,
            "current_liabilities": 35300, "goodwill": 27100,
            "accounts_receivable": 3200, "inventory": 11900,
            "cash_and_equivalents": 21000, "total_debt": 51800,
            "dividends_paid": -2100, "share_repurchase": 0,
            "sga_expense": 5600, "rnd_expense": 16200,
            "basic_weighted_average_shares": 4275,
            "income_tax_expense_total": -10000, "income_before_tax_total": -8700,
            "proceeds_from_stock_issuance": 3000, "proceeds_from_debt_issuance": 7000,
        },
        # FY2025: continued struggles, foundry ramp, massive restructuring
        "FY2025": {
            "revenue": 50000, "cost_of_revenue": 34500, "operating_income": -5000,
            "net_income": -12000, "operating_cash_flow": 5000, "capital_expenditures": 20000,
            "depreciation_amortization": 10000, "shareholders_equity": 72000,
            "total_assets": 190000, "interest_expense": 1500, "current_assets": 38000,
            "current_liabilities": 36000, "goodwill": 27000,
            "accounts_receivable": 3000, "inventory": 12000,
            "cash_and_equivalents": 17000, "total_debt": 55000,
            "dividends_paid": -500, "share_repurchase": 0,
            "sga_expense": 5200, "rnd_expense": 15500,
            "basic_weighted_average_shares": 4350,
            "income_tax_expense_total": -2000, "income_before_tax_total": -10000,
            "proceeds_from_stock_issuance": 5000, "proceeds_from_debt_issuance": 8000,
        },
    },
    "downstream": [
        {"customer_name": "PC OEM (联想/HP/Dell)", "revenue_pct": 0.45, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "consumer_electronics"},
        {"customer_name": "数据中心/服务器", "revenue_pct": 0.30, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "cloud_infrastructure"},
        {"customer_name": "代工客户 (Intel Foundry)", "revenue_pct": 0.05, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "consumer_electronics"},
        {"customer_name": "Mobileye 自动驾驶", "revenue_pct": 0.04, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "consumer_electronics"},
        {"customer_name": "其他（FPGA/网络）", "revenue_pct": 0.16, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "consumer_electronics"},
    ],
    "upstream": [
        {"supplier_name": "ASML (光刻机)", "is_sole_source": True, "geographic_location": "Netherlands"},
        {"supplier_name": "化学材料供应商", "is_sole_source": False, "geographic_location": "多地"},
    ],
    "geo": [
        {"region": "United States", "revenue_share": 0.28},
        {"region": "China", "revenue_share": 0.27},
        {"region": "Singapore", "revenue_share": 0.17},
        {"region": "Taiwan", "revenue_share": 0.10},
        {"region": "Other", "revenue_share": 0.18},
    ],
    "pricing": [
        {"action": "Xeon 降价促销应对 AMD EPYC", "price_change_pct": -0.15,
         "product_or_segment": "Xeon 服务器CPU", "effective_date": "2024-01",
         "volume_impact_pct": -0.05},
    ],
    "market_share": [
        {"period": "FY2022", "share": 0.72, "source": "x86 服务器CPU"},
        {"period": "FY2023", "share": 0.65, "source": "x86 服务器CPU"},
        {"period": "FY2024", "share": 0.58, "source": "x86 服务器CPU"},
        {"period": "FY2025", "share": 0.52, "source": "x86 服务器CPU"},
    ],
    "competitive": [
        {"competitor_name": "AMD", "event_type": "product_launch",
         "event_description": "AMD EPYC Genoa/Turin 在性能和能效上全面超越 Intel Xeon",
         "outcome_description": "AMD 服务器份额从 28% 升至 48%，Intel 持续失血",
         "outcome_market_share_change": -0.20},
        {"competitor_name": "NVIDIA", "event_type": "new_entry",
         "event_description": "Grace CPU + Blackwell GPU 系统抢占数据中心算力",
         "outcome_description": "AI 训练/推理市场被 NVIDIA 主导，Intel GPU 项目 (Gaudi) 失败",
         "outcome_market_share_change": -0.05},
        {"competitor_name": "ARM/Qualcomm", "event_type": "product_launch",
         "event_description": "Qualcomm Snapdragon X Elite 冲击 PC 市场",
         "outcome_description": "Windows on ARM 生态逐步成熟，Intel PC 份额面临长期威胁",
         "outcome_market_share_change": -0.02},
    ],
    "peers": [
        {"peer_name": "AMD", "metric": "gross_margin", "value": 0.50, "period": "FY2025"},
        {"peer_name": "AMD", "metric": "operating_margin", "value": 0.22, "period": "FY2025"},
        {"peer_name": "AMD", "metric": "net_margin", "value": 0.18, "period": "FY2025"},
        {"peer_name": "TSMC", "metric": "gross_margin", "value": 0.57, "period": "FY2025"},
        {"peer_name": "TSMC", "metric": "operating_margin", "value": 0.47, "period": "FY2025"},
        {"peer_name": "TSMC", "metric": "net_margin", "value": 0.40, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "IDM 2.0 — 重回制造领先", "status": "missed"},
        {"narrative": "Intel 18A 工艺追上台积电", "status": "missed"},
        {"narrative": "代工业务 (Intel Foundry Services) 盈利", "status": "missed"},
        {"narrative": "Gaudi AI 加速器挑战 NVIDIA", "status": "missed"},
        {"narrative": "US CHIPS Act 获得 $80亿补贴", "status": "delivered"},
        {"narrative": "裁员 15,000 人降本增效", "status": "delivered"},
    ],
    "ownership": [
        {"name": "Lip-Bu Tan", "title": "CEO (2025)", "percent_of_class": 0.01},
    ],
    "exec_comp": [
        {"name": "Pat Gelsinger", "title": "前CEO (离任2024.12)", "role_type": "CEO",
         "pay_ratio": 250.0, "stock_awards": 10000, "total_comp": 18000},
    ],
    "exec_changes": [
        {"name": "Pat Gelsinger", "title": "CEO", "change_type": "departed"},
        {"name": "Lip-Bu Tan", "title": "CEO", "change_type": "joined"},
        {"name": "David Zinsner", "title": "CFO/临时联席CEO", "change_type": "departed"},
        {"name": "Michelle Johnston Holthaus", "title": "CEO Intel Products", "change_type": "joined"},
        {"name": "Nick McKeown", "title": "SVP Intel Foundry", "change_type": "departed"},
    ],
    "known_issues": [
        {"issue_description": "Intel 18A 工艺良率远低于预期，代工客户观望",
         "severity": "critical", "source_type": "analyst"},
        {"issue_description": "连续两年净亏损，现金流无法覆盖巨额 capex",
         "severity": "high", "source_type": "financial"},
    ],
    "management_acks": [],  # 新 CEO 尚未明确回应
    "litigations": [],
    "market": {"price": 22.0, "shares_outstanding": 4350, "discount_rate": 0.043, "market": "US"},
    "guidance": {},  # 无 guidance — 公司自己也不知道未来
    "home_market": "United States",
}


# ══════════════════════════════════════════════════════════════
#  构建 + 跑链
# ══════════════════════════════════════════════════════════════

def build_ctx(company_data):
    fli_data = company_data["fli"]
    all_fli = pd.concat([_fli(v, k) for k, v in fli_data.items()], ignore_index=True)

    ctx = ComputeContext(company_id=1, period="FY2025")

    # litigations
    lit_rows = company_data.get("litigations", [])
    if lit_rows:
        litigations = pd.DataFrame([{"id": i, **r} for i, r in enumerate(lit_rows)])
    else:
        litigations = EMPTY

    # known issues
    ki_rows = company_data.get("known_issues", [])
    if ki_rows:
        known_issues = pd.DataFrame([{"id": i, **r} for i, r in enumerate(ki_rows)])
    else:
        known_issues = EMPTY

    # management acknowledgments
    ma_rows = company_data.get("management_acks", [])
    if ma_rows:
        mgmt_acks = pd.DataFrame([{"id": i, **r} for i, r in enumerate(ma_rows)])
    else:
        mgmt_acks = EMPTY

    # executive changes
    ec_rows = company_data.get("exec_changes", [])
    if ec_rows:
        exec_changes = pd.DataFrame([{"id": i, **r} for i, r in enumerate(ec_rows)])
    else:
        exec_changes = EMPTY

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
        "litigations": litigations,
        "operational_issues": EMPTY,
        "related_party_transactions": EMPTY, "non_financial_kpis": EMPTY,
        "audit_opinions": EMPTY,
        "known_issues": known_issues,
        "insider_transactions": EMPTY,
        "executive_changes": exec_changes,
        "equity_offerings": EMPTY, "analyst_estimates": EMPTY,
        "management_guidance": EMPTY,
        "management_acknowledgments": mgmt_acks,
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
        print(f"\n  已算特征: {len(ctx.features)} 个")

        # ── 线 1 ──
        print("\n" + "=" * 65)
        print("  线 1: 生意评估")
        print("=" * 65)

        moat = assess_moat(ctx)
        print(format_moat(moat))

        earnings = assess_earnings(ctx)
        print(format_earnings(earnings))

        dist = assess_distribution(ctx)
        print(format_distribution(dist))

        pred = assess_predictability(ctx, moat_depth=moat.depth)
        print(format_predictability(pred))

        mkt = data.get("market", {})
        guidance = data.get("guidance", {})
        market_code = mkt.get("market", "US")

        # ── 确定性等级: 决定折现率用无风险还是加 ERP ──
        # 护城河 deep/extreme + 盈余 holds + 可预测 holds → 现金流确定性高
        # 巴菲特只用无风险利率折现这类公司
        line1_quality = (moat.depth in ("extreme", "deep") and
                         earnings.verdict == "holds" and
                         pred.verdict == "holds")
        certainty = "high" if line1_quality else "normal"

        print("\n  可估值 + 安全边际")
        print("  ════════════════════════════════════════════════")
        certainty_labels = {"high": "高确定性 → 无风险利率", "normal": "普通 → 无风险+ERP"}
        print(f"  确定性: {certainty_labels[certainty]}")

        dcf = None
        mos = None
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

        # ── 线 2 ──
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

        # ── 综合 ──
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
