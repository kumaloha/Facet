"""
巴菲特因果链 · 第三批: Meta / AMD / Circle
三家非巴菲特典型持仓，测模型边界
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
    return pd.DataFrame([{"id": i, "statement_id": 1, "item_key": k, "item_label": k,
         "value": v, "parent_key": None, "ordinal": i, "note": None, "period": period}
        for i, (k, v) in enumerate(items.items())])
def _df(rows, defaults):
    if not rows: return EMPTY
    return pd.DataFrame([{**defaults, "id": i, **r} for i, r in enumerate(rows)])

DS = {"company_id":1,"period":"FY2025","segment":None,"customer_type":None,"products":None,
      "channels":None,"revenue":None,"growth_yoy":None,"backlog":None,"backlog_note":None,
      "pricing_model":None,"contract_duration":None,"recognition_method":None,"description":None,
      "raw_post_id":None,"created_at":"2025-01-01"}
CD = {"company_id":1,"estimated_investment":None,"outcome_market_share_change":None,
      "event_date":"2024-01","raw_post_id":None,"created_at":"2025-01-01"}
MS = {"company_id":1,"raw_post_id":None,"created_at":"2025-01-01"}
PA = {"company_id":1,"raw_post_id":None,"created_at":"2025-01-01"}
GR = {"company_id":1,"period":"FY2025","revenue":None,"growth_yoy":None,"note":None,
      "raw_post_id":None,"created_at":"2025-01-01"}
SO = {"company_id":1,"period":"FY2025","shares_beneficially_owned":None,"raw_post_id":None,"created_at":"2025-01-01"}
EC = {"company_id":1,"period":"FY2025","base_salary":None,"bonus":None,"option_awards":None,
      "non_equity_incentive":None,"other_comp":None,"currency":"USD","median_employee_comp":None,
      "raw_post_id":None,"created_at":"2025-01-01"}
US = {"company_id":1,"period":"FY2025","segment":None,"supply_type":"component",
      "material_or_service":None,"process_node":None,"purchase_obligation":None,
      "contract_type":None,"prepaid_amount":None,"concentration_risk":None,
      "description":None,"raw_post_id":None,"created_at":"2025-01-01"}
CN = {"company_id":1,"raw_post_id":None,"capital_required":None,"capital_unit":None,
      "promised_outcome":None,"deadline":None,"reported_at":None,"created_at":"2025-01-01"}

COMPANIES = {}

# ── 1. Meta (META) — 广告帝国 + AI + Metaverse ──────────────
# 单位: 百万美元。巴菲特从不买 Meta — 为什么？
COMPANIES["Meta (META)"] = {
    "fli": {
        "FY2022": {"revenue": 116600, "cost_of_revenue": 25200, "operating_income": 28900,
            "net_income": 23200, "operating_cash_flow": 50500, "capital_expenditures": 31400,
            "depreciation_amortization": 9300, "shareholders_equity": 125700,
            "total_assets": 185700, "interest_expense": 200, "current_assets": 57000,
            "current_liabilities": 27000, "goodwill": 20100,
            "accounts_receivable": 13500, "inventory": 0,
            "cash_and_equivalents": 40700, "total_debt": 10000,
            "dividends_paid": 0, "share_repurchase": -27900,
            "sga_expense": 14600, "rnd_expense": 35300,
            "basic_weighted_average_shares": 2687,
            "income_tax_expense_total": 5600, "income_before_tax_total": 28800,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 10000},
        "FY2023": {"revenue": 134900, "cost_of_revenue": 25900, "operating_income": 46800,
            "net_income": 39100, "operating_cash_flow": 71100, "capital_expenditures": 27300,
            "depreciation_amortization": 11200, "shareholders_equity": 153200,
            "total_assets": 229600, "interest_expense": 600, "current_assets": 68000,
            "current_liabilities": 31000, "goodwill": 20100,
            "accounts_receivable": 16200, "inventory": 0,
            "cash_and_equivalents": 41900, "total_debt": 18400,
            "dividends_paid": 0, "share_repurchase": -20100,
            "sga_expense": 13700, "rnd_expense": 38500,
            "basic_weighted_average_shares": 2571,
            "income_tax_expense_total": 8000, "income_before_tax_total": 47100,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 8000},
        "FY2024": {"revenue": 164500, "cost_of_revenue": 27700, "operating_income": 69400,
            "net_income": 62400, "operating_cash_flow": 91300, "capital_expenditures": 37300,
            "depreciation_amortization": 14700, "shareholders_equity": 217200,
            "total_assets": 366000, "interest_expense": 1000, "current_assets": 85000,
            "current_liabilities": 38000, "goodwill": 21500,
            "accounts_receivable": 19500, "inventory": 0,
            "cash_and_equivalents": 58100, "total_debt": 28800,
            "dividends_paid": -5100, "share_repurchase": -30100,
            "sga_expense": 14000, "rnd_expense": 44400,
            "basic_weighted_average_shares": 2530,
            "income_tax_expense_total": 10300, "income_before_tax_total": 72700,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 12000},
        # FY2025: AI capex 暴涨，Metaverse 继续烧钱
        "FY2025": {"revenue": 190000, "cost_of_revenue": 32000, "operating_income": 80000,
            "net_income": 68000, "operating_cash_flow": 100000, "capital_expenditures": 55000,
            "depreciation_amortization": 18000, "shareholders_equity": 240000,
            "total_assets": 420000, "interest_expense": 1500, "current_assets": 95000,
            "current_liabilities": 42000, "goodwill": 22000,
            "accounts_receivable": 22000, "inventory": 0,
            "cash_and_equivalents": 55000, "total_debt": 35000,
            "dividends_paid": -6500, "share_repurchase": -35000,
            "sga_expense": 15000, "rnd_expense": 50000,
            "basic_weighted_average_shares": 2500,
            "income_tax_expense_total": 12000, "income_before_tax_total": 80000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 10000},
    },
    "downstream": [
        {"customer_name": "广告主（品牌+效果）", "revenue_pct": 0.95, "is_recurring": True,
         "revenue_type": "ad_revenue", "product_category": "social_media"},
        {"customer_name": "Reality Labs/VR", "revenue_pct": 0.03, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "consumer_electronics"},
        {"customer_name": "其他", "revenue_pct": 0.02, "is_recurring": True,
         "revenue_type": "recurring", "product_category": "social_media"},
    ],
    "upstream": [{"supplier_name": "云/服务器（自建+外购）", "is_sole_source": False, "geographic_location": "全球"}],
    "geo": [{"region": "United States", "revenue_share": 0.42},
            {"region": "Europe", "revenue_share": 0.24},
            {"region": "Asia Pacific", "revenue_share": 0.22},
            {"region": "Other", "revenue_share": 0.12}],
    "pricing": [
        {"action": "广告 CPM 涨价", "price_change_pct": 0.15,
         "product_or_segment": "Facebook/Instagram 广告", "effective_date": "2024-01",
         "volume_impact_pct": 0.05},
    ],
    "market_share": [
        {"period": "FY2023", "share": 0.22, "source": "全球数字广告"},
        {"period": "FY2024", "share": 0.23, "source": "全球数字广告"},
        {"period": "FY2025", "share": 0.24, "source": "全球数字广告"},
    ],
    "competitive": [
        {"competitor_name": "TikTok", "event_type": "new_entry",
         "event_description": "TikTok 短视频持续抢夺年轻用户时间和广告预算",
         "outcome_description": "Meta Reels 成功应对，Instagram 用户时间反升，广告份额稳住",
         "outcome_market_share_change": 0.01},
        {"competitor_name": "Apple ATT", "event_type": "regulatory_change",
         "event_description": "Apple ATT 隐私政策限制广告追踪，导致 Meta 广告精准度下降",
         "outcome_description": "Meta 投入 AI 重建广告系统（Advantage+），2024 年广告效率恢复到 ATT 前水平",
         "outcome_market_share_change": 0.0},
    ],
    "peers": [
        {"peer_name": "Google Ads", "metric": "gross_margin", "value": 0.58, "period": "FY2025"},
        {"peer_name": "Google Ads", "metric": "operating_margin", "value": 0.32, "period": "FY2025"},
        {"peer_name": "Google Ads", "metric": "net_margin", "value": 0.27, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "AI 广告系统 Advantage+ 全面升级", "status": "delivered"},
        {"narrative": "Reels 短视频追上 TikTok", "status": "delivered"},
        {"narrative": "Metaverse/Reality Labs 成为主流（累计亏损 $500亿+）", "status": "missed"},
        {"narrative": "Quest VR 头显成为消费级爆品", "status": "missed"},
        {"narrative": "Threads 月活过亿但无商业化", "status": "missed"},
        {"narrative": "AI capex $55B 回报待验证（市场质疑 ROI）", "status": "in_progress"},
        {"narrative": "首次派发股息", "status": "delivered"},
    ],
    "ownership": [{"name": "Mark Zuckerberg", "title": "CEO/创始人/控股", "percent_of_class": 13.5}],
    "exec_comp": [{"name": "Mark Zuckerberg", "title": "CEO", "role_type": "CEO",
         "pay_ratio": 120.0, "stock_awards": 25000, "total_comp": 27000}],
    "exec_changes": [],  # Zuckerberg 就是 Meta，没有继任
    "known_issues": [
        {"issue_description": "Reality Labs 累计亏损超 $500 亿，无商业化时间表",
         "severity": "high", "source_type": "analyst"},
        {"issue_description": "AI capex $55B/年暴涨，ROI 尚未验证，市场质疑资本纪律",
         "severity": "medium", "source_type": "analyst"},
        {"issue_description": "双重股权结构导致 Zuckerberg 不受制约，董事会形同虚设",
         "severity": "high", "source_type": "governance"},
    ],
    "management_acks": [
        {"issue_description": "Zuckerberg 称 Metaverse 是 10 年赌注，短期亏损在预期内",
         "response_quality": "defensive", "has_action_plan": False},
    ],
    "market": {"price": 620.0, "shares_outstanding": 2500, "discount_rate": 0.043, "market": "US"},
    "guidance": {"revenue_growth": 0.15},
    "home_market": "United States",
}

# ── 2. AMD — NVDA 的对手，Intel 的颠覆者 ────────────────────
# 单位: 百万美元
COMPANIES["AMD (AMD)"] = {
    "fli": {
        "FY2022": {"revenue": 23600, "cost_of_revenue": 12200, "operating_income": 1800,
            "net_income": 1300, "operating_cash_flow": 3600, "capital_expenditures": 450,
            "depreciation_amortization": 3800, "shareholders_equity": 54500,
            "total_assets": 67600, "interest_expense": 100, "current_assets": 15000,
            "current_liabilities": 7000, "goodwill": 24200,
            "accounts_receivable": 4100, "inventory": 3300,
            "cash_and_equivalents": 5900, "total_debt": 2500,
            "dividends_paid": 0, "share_repurchase": -3700,
            "sga_expense": 2000, "rnd_expense": 5000,
            "basic_weighted_average_shares": 1610,
            "income_tax_expense_total": 400, "income_before_tax_total": 1700,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
        "FY2023": {"revenue": 22700, "cost_of_revenue": 11700, "operating_income": 400,
            "net_income": 854, "operating_cash_flow": 1700, "capital_expenditures": 550,
            "depreciation_amortization": 3900, "shareholders_equity": 55900,
            "total_assets": 67900, "interest_expense": 100, "current_assets": 14000,
            "current_liabilities": 5500, "goodwill": 24200,
            "accounts_receivable": 5400, "inventory": 4400,
            "cash_and_equivalents": 3800, "total_debt": 2500,
            "dividends_paid": 0, "share_repurchase": -900,
            "sga_expense": 1800, "rnd_expense": 5800,
            "basic_weighted_average_shares": 1616,
            "income_tax_expense_total": -400, "income_before_tax_total": 454,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
        "FY2024": {"revenue": 25800, "cost_of_revenue": 12900, "operating_income": 4000,
            "net_income": 1640, "operating_cash_flow": 3000, "capital_expenditures": 640,
            "depreciation_amortization": 3500, "shareholders_equity": 57600,
            "total_assets": 69200, "interest_expense": 100, "current_assets": 16000,
            "current_liabilities": 6500, "goodwill": 24200,
            "accounts_receivable": 6200, "inventory": 4600,
            "cash_and_equivalents": 3200, "total_debt": 1700,
            "dividends_paid": 0, "share_repurchase": -5000,
            "sga_expense": 1900, "rnd_expense": 6100,
            "basic_weighted_average_shares": 1620,
            "income_tax_expense_total": 300, "income_before_tax_total": 1940,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
        # FY2025: AI GPU (MI300) 贡献增长但仍远落后 NVDA
        "FY2025": {"revenue": 30000, "cost_of_revenue": 14500, "operating_income": 5500,
            "net_income": 3500, "operating_cash_flow": 5000, "capital_expenditures": 800,
            "depreciation_amortization": 3300, "shareholders_equity": 58000,
            "total_assets": 70000, "interest_expense": 100, "current_assets": 17000,
            "current_liabilities": 7000, "goodwill": 24200,
            "accounts_receivable": 7000, "inventory": 4800,
            "cash_and_equivalents": 3500, "total_debt": 1500,
            "dividends_paid": 0, "share_repurchase": -6000,
            "sga_expense": 2000, "rnd_expense": 6500,
            "basic_weighted_average_shares": 1600,
            "income_tax_expense_total": 600, "income_before_tax_total": 4100,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
    },
    "downstream": [
        {"customer_name": "数据中心/AI GPU", "revenue_pct": 0.45, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "cloud_infrastructure"},
        {"customer_name": "PC CPU 客户端", "revenue_pct": 0.25, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "consumer_electronics"},
        {"customer_name": "游戏主机 (Xbox/PS)", "revenue_pct": 0.15, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "gaming"},
        {"customer_name": "嵌入式", "revenue_pct": 0.15, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "consumer_electronics"},
    ],
    "upstream": [{"supplier_name": "台积电", "is_sole_source": True, "geographic_location": "Taiwan"}],
    "geo": [{"region": "United States", "revenue_share": 0.30},
            {"region": "China", "revenue_share": 0.20},
            {"region": "Japan", "revenue_share": 0.15},
            {"region": "Europe", "revenue_share": 0.15},
            {"region": "Other", "revenue_share": 0.20}],
    "pricing": [],  # AMD 在大多数市场是追赶者，靠性价比竞争
    "market_share": [
        {"period": "FY2022", "share": 0.28, "source": "x86 服务器CPU"},
        {"period": "FY2023", "share": 0.35, "source": "x86 服务器CPU"},
        {"period": "FY2024", "share": 0.42, "source": "x86 服务器CPU"},
        {"period": "FY2025", "share": 0.48, "source": "x86 服务器CPU"},
    ],
    "competitive": [
        {"competitor_name": "Intel", "event_type": "product_launch",
         "event_description": "Intel Xeon 在服务器市场节节败退",
         "outcome_description": "AMD EPYC 持续夺份，从 28% 升至 48%，性能+能效全面领先",
         "outcome_market_share_change": 0.20},
        {"competitor_name": "NVIDIA", "event_type": "product_launch",
         "event_description": "MI300X 对标 H100 但 CUDA 生态壁垒阻碍采用",
         "outcome_description": "AMD 在 AI GPU 拿到 ~5-8% 份额，ROCm 生态仍远不如 CUDA",
         "outcome_market_share_change": 0.0},
    ],
    "peers": [
        {"peer_name": "NVIDIA", "metric": "gross_margin", "value": 0.77, "period": "FY2025"},
        {"peer_name": "NVIDIA", "metric": "operating_margin", "value": 0.62, "period": "FY2025"},
        {"peer_name": "NVIDIA", "metric": "net_margin", "value": 0.56, "period": "FY2025"},
        {"peer_name": "Intel", "metric": "gross_margin", "value": 0.31, "period": "FY2025"},
        {"peer_name": "Intel", "metric": "operating_margin", "value": -0.10, "period": "FY2025"},
        {"peer_name": "Intel", "metric": "net_margin", "value": -0.24, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "EPYC 服务器 CPU 持续夺份", "status": "delivered"},
        {"narrative": "MI300X AI GPU 挑战 NVIDIA", "status": "in_progress"},
        {"narrative": "Xilinx 整合带来嵌入式协同", "status": "delivered"},
        {"narrative": "ROCm 开源 AI 软件生态", "status": "in_progress"},
        {"narrative": "Pensando DPU 数据中心加速", "status": "delivered"},
    ],
    "ownership": [{"name": "Lisa Su", "title": "CEO/董事长", "percent_of_class": 0.15}],
    "exec_comp": [{"name": "Lisa Su", "title": "CEO", "role_type": "CEO",
         "pay_ratio": 400.0, "stock_awards": 20000, "total_comp": 30000}],
    "exec_changes": [],  # Lisa Su 是灵魂人物，无明确继任
    "market": {"price": 120.0, "shares_outstanding": 1600, "discount_rate": 0.043, "market": "US"},
    "guidance": {"revenue_growth": 0.16},
    "home_market": "United States",
}

# ── 3. Circle (CRCL) — USDC 稳定币发行商 ────────────────────
# 单位: 百万美元。2025 年 IPO，加密基础设施
# FY2025: 收入 $2.7B（主要是 USDC 储备利息），净亏损 $70M（IPO 股权激励）
COMPANIES["Circle (CRCL)"] = {
    "fli": {
        # Circle 只有 2024-2025 公开数据（IPO 前有限）
        "FY2023": {"revenue": 1000, "cost_of_revenue": 500, "operating_income": 200,
            "net_income": 100, "operating_cash_flow": 300, "capital_expenditures": 50,
            "depreciation_amortization": 30, "shareholders_equity": 1500,
            "total_assets": 4000, "interest_expense": 10, "current_assets": 2500,
            "current_liabilities": 500, "goodwill": 500,
            "accounts_receivable": 100, "inventory": 0,
            "cash_and_equivalents": 2000, "total_debt": 200,
            "dividends_paid": 0, "share_repurchase": 0,
            "sga_expense": 200, "rnd_expense": 100,
            "basic_weighted_average_shares": 500,
            "income_tax_expense_total": 30, "income_before_tax_total": 130,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
        "FY2024": {"revenue": 1650, "cost_of_revenue": 800, "operating_income": 350,
            "net_income": 230, "operating_cash_flow": 500, "capital_expenditures": 80,
            "depreciation_amortization": 40, "shareholders_equity": 2000,
            "total_assets": 5500, "interest_expense": 15, "current_assets": 3500,
            "current_liabilities": 600, "goodwill": 500,
            "accounts_receivable": 120, "inventory": 0,
            "cash_and_equivalents": 3000, "total_debt": 200,
            "dividends_paid": 0, "share_repurchase": 0,
            "sga_expense": 280, "rnd_expense": 150,
            "basic_weighted_average_shares": 520,
            "income_tax_expense_total": 50, "income_before_tax_total": 280,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0},
        # FY2025: IPO 年，SBC 导致亏损
        "FY2025": {"revenue": 2700, "cost_of_revenue": 1300, "operating_income": 400,
            "net_income": -70, "operating_cash_flow": 700, "capital_expenditures": 120,
            "depreciation_amortization": 50, "shareholders_equity": 3500,
            "total_assets": 8000, "interest_expense": 20, "current_assets": 5500,
            "current_liabilities": 800, "goodwill": 500,
            "accounts_receivable": 150, "inventory": 0,
            "cash_and_equivalents": 4500, "total_debt": 200,
            "dividends_paid": 0, "share_repurchase": 0,
            "sga_expense": 500, "rnd_expense": 300,
            "basic_weighted_average_shares": 640,
            "income_tax_expense_total": 60, "income_before_tax_total": -10,
            "proceeds_from_stock_issuance": 1500, "proceeds_from_debt_issuance": 0},
    },
    "downstream": [
        {"customer_name": "USDC 持有人/交易所", "revenue_pct": 0.75, "is_recurring": True,
         "revenue_type": "transaction_fee", "product_category": "payment"},
        {"customer_name": "Circle Payments Network", "revenue_pct": 0.15, "is_recurring": True,
         "revenue_type": "transaction_fee", "product_category": "payment"},
        {"customer_name": "Arc 企业解决方案", "revenue_pct": 0.10, "is_recurring": True,
         "revenue_type": "subscription", "product_category": "payment"},
    ],
    "upstream": [{"supplier_name": "银行合作伙伴（储备托管）", "is_sole_source": False, "geographic_location": "美国"}],
    "geo": [{"region": "United States", "revenue_share": 0.60},
            {"region": "Europe", "revenue_share": 0.20},
            {"region": "Other", "revenue_share": 0.20}],
    "pricing": [],  # 收入主要来自储备利息（跟随利率），不是定价
    "market_share": [
        {"period": "FY2023", "share": 0.23, "source": "稳定币市值"},
        {"period": "FY2024", "share": 0.25, "source": "稳定币市值"},
        {"period": "FY2025", "share": 0.26, "source": "稳定币市值"},
    ],
    "competitive": [
        {"competitor_name": "Tether (USDT)", "event_type": "price_war",
         "event_description": "Tether 市占率 72% 主导稳定币市场，但合规透明度差",
         "outcome_description": "USDC 凭合规和透明度在机构市场拿到份额，但零售仍被 USDT 主导",
         "outcome_market_share_change": 0.01},
    ],
    "peers": [],  # 没有直接可比上市同行
    "narratives": [
        {"narrative": "USDC 成为最合规的稳定币", "status": "delivered"},
        {"narrative": "IPO 上市（NYSE: CRCL）", "status": "delivered"},
        {"narrative": "Circle Payments Network 全球支付", "status": "in_progress"},
        {"narrative": "利率下降后收入模型转型", "status": "in_progress"},
    ],
    "ownership": [{"name": "Jeremy Allaire", "title": "CEO/创始人", "percent_of_class": 10.0}],
    "exec_comp": [{"name": "Jeremy Allaire", "title": "CEO", "role_type": "CEO",
         "pay_ratio": 50.0, "stock_awards": 5000, "total_comp": 6000}],
    "exec_changes": [],
    "known_issues": [
        {"issue_description": "收入高度依赖利率（储备利息占 75%），降息周期收入将大幅下降",
         "severity": "high", "source_type": "analyst"},
        {"issue_description": "稳定币监管框架尚不明确，政策风险高",
         "severity": "high", "source_type": "regulatory"},
    ],
    "management_acks": [
        {"issue_description": "已在转型收入结构，降低利率依赖，发展 Payments Network 和 Arc",
         "response_quality": "adequate", "has_action_plan": True},
    ],
    "market": {"price": 65.0, "shares_outstanding": 640, "discount_rate": 0.043, "market": "US"},
    "guidance": {},
    "home_market": "United States",
}


# ══════════════════════════════════════════════════════════════
def build_ctx(data):
    fli_data = data["fli"]
    all_fli = pd.concat([_fli(v, k) for k, v in fli_data.items()], ignore_index=True)
    ec_rows = data.get("exec_changes", [])
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
        "insider_transactions": EMPTY,
        "executive_changes": pd.DataFrame([{"id": i, **r} for i, r in enumerate(ec_rows)]) if ec_rows else EMPTY,
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
                         earnings.verdict == "holds" and pred.verdict == "holds")
        certainty = "high" if line1_quality else "normal"

        dcf = mos = None
        if mkt.get("discount_rate") and mkt.get("shares_outstanding"):
            dcf = compute_intrinsic_value(ctx.features, guidance, mkt["discount_rate"],
                                           mkt["shares_outstanding"], market=market_code,
                                           certainty=certainty)
            if dcf.intrinsic_value:
                mos = (dcf.intrinsic_value - mkt["price"]) / dcf.intrinsic_value

        integrity = assess_integrity(ctx)
        character = assess_character(ctx)
        risk = assess_risk(ctx, home_market=data.get("home_market", ""))

        line1_ok = (moat.depth not in ("none", "unknown") and
                    earnings.verdict == "holds" and dist.verdict == "holds")
        line2_ok = (integrity.verdict != "breaks" and not risk.has_catastrophic)

        print(f"\n  线 1:")
        print(f"    护城河:   {moat.depth} — {moat.summary[:80]}")
        print(f"    盈余能力: {earnings.verdict} — {earnings.summary[:80]}")
        print(f"    利润分配: {dist.verdict} — {dist.summary[:80]}")
        print(f"    可预测:   {pred.verdict} — {pred.summary[:80]}")
        if dcf and dcf.intrinsic_value:
            ct = "无风险" if certainty == "high" else "无风险+ERP"
            print(f"    估值:     ${dcf.intrinsic_value:,.0f} vs ${mkt['price']}  "
                  f"安全边际 {mos:.0%}  [{ct}, 路径{dcf.valuation_path}]")
        elif dcf:
            print(f"    估值:     {dcf.status}")

        print(f"  线 2:")
        print(f"    诚信:     {integrity.verdict} — {integrity.summary[:60]}")
        print(f"    管理层:   {character.conviction} — {character.summary[:50]}")
        kp = [r for r in risk.risks if r.category == "key_person"]
        print(f"    关键人:   {kp[0].level + ': ' + kp[0].description[:50] if kp else '无'}")
        cr = "灾难性" if risk.has_catastrophic else (f"{len(risk.significant)}项重大" if risk.significant else "可控")
        print(f"    风险:     {cr}")

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
