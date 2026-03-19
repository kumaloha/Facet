"""
巴菲特因果链 · 第四批: 甲骨文 / 沃尔玛 / 高通 / SEA / 拼多多
"""
import pandas as pd
import polaris.features.l0.company  # noqa: F401
import polaris.features.l0.cross_period  # noqa: F401
from polaris.features.types import ComputeContext, FeatureLevel
from polaris.features.registry import get_features
from polaris.chains.moat import assess_moat
from polaris.chains.earnings import assess_earnings
from polaris.chains.distribution import assess_distribution
from polaris.chains.predictability import assess_predictability
from polaris.chains.integrity import assess_integrity
from polaris.chains.character import assess_character
from polaris.chains.risk import assess_risk
from polaris.scoring.engines.dcf import compute_intrinsic_value, reverse_dcf

EMPTY = pd.DataFrame()
def _fli(items, period="FY2025"):
    return pd.DataFrame([{"id":i,"statement_id":1,"item_key":k,"item_label":k,
        "value":v,"parent_key":None,"ordinal":i,"note":None,"period":period}
        for i,(k,v) in enumerate(items.items())])
def _df(rows, defaults):
    if not rows: return EMPTY
    return pd.DataFrame([{**defaults,"id":i,**r} for i,r in enumerate(rows)])

DS={"company_id":1,"period":"FY2025","segment":None,"customer_type":None,"products":None,
    "channels":None,"revenue":None,"growth_yoy":None,"backlog":None,"backlog_note":None,
    "pricing_model":None,"contract_duration":None,"recognition_method":None,"description":None,
    "raw_post_id":None,"created_at":"2025-01-01"}
CD={"company_id":1,"estimated_investment":None,"outcome_market_share_change":None,
    "event_date":"2024-01","raw_post_id":None,"created_at":"2025-01-01"}
MS={"company_id":1,"raw_post_id":None,"created_at":"2025-01-01"}
PA={"company_id":1,"raw_post_id":None,"created_at":"2025-01-01"}
GR={"company_id":1,"period":"FY2025","revenue":None,"growth_yoy":None,"note":None,
    "raw_post_id":None,"created_at":"2025-01-01"}
SO={"company_id":1,"period":"FY2025","shares_beneficially_owned":None,"raw_post_id":None,"created_at":"2025-01-01"}
EC={"company_id":1,"period":"FY2025","base_salary":None,"bonus":None,"option_awards":None,
    "non_equity_incentive":None,"other_comp":None,"currency":"USD","median_employee_comp":None,
    "raw_post_id":None,"created_at":"2025-01-01"}
US={"company_id":1,"period":"FY2025","segment":None,"supply_type":"component",
    "material_or_service":None,"process_node":None,"purchase_obligation":None,
    "contract_type":None,"prepaid_amount":None,"concentration_risk":None,
    "description":None,"raw_post_id":None,"created_at":"2025-01-01"}
CN={"company_id":1,"raw_post_id":None,"capital_required":None,"capital_unit":None,
    "promised_outcome":None,"deadline":None,"reported_at":None,"created_at":"2025-01-01"}

C = {}

# ── 1. 甲骨文 (ORCL) — 企业数据库/云转型 ─────────────────────
# 单位: 百万美元。财年截止 5/31
C["甲骨文 (ORCL)"] = {
    "fli": {
        "FY2022": {"revenue":42400,"cost_of_revenue":13500,"operating_income":13700,
            "net_income":6700,"operating_cash_flow":13400,"capital_expenditures":4500,
            "depreciation_amortization":3000,"shareholders_equity":5500,
            "total_assets":131000,"interest_expense":3200,"current_assets":22000,
            "current_liabilities":25000,"goodwill":43000,
            "accounts_receivable":7000,"inventory":0,
            "cash_and_equivalents":6000,"total_debt":72000,
            "dividends_paid":-3500,"share_repurchase":-3000,
            "sga_expense":9000,"rnd_expense":7000,
            "basic_weighted_average_shares":2700,
            "income_tax_expense_total":1500,"income_before_tax_total":8200,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":20000},
        "FY2023": {"revenue":50000,"cost_of_revenue":17000,"operating_income":15000,
            "net_income":9100,"operating_cash_flow":18000,"capital_expenditures":8700,
            "depreciation_amortization":4000,"shareholders_equity":-5000,
            "total_assets":135000,"interest_expense":3500,"current_assets":21000,
            "current_liabilities":28000,"goodwill":43000,
            "accounts_receivable":7500,"inventory":0,
            "cash_and_equivalents":10000,"total_debt":86000,
            "dividends_paid":-4000,"share_repurchase":0,
            "sga_expense":9200,"rnd_expense":8000,
            "basic_weighted_average_shares":2700,
            "income_tax_expense_total":2000,"income_before_tax_total":11100,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":15000},
        "FY2024": {"revenue":53000,"cost_of_revenue":17500,"operating_income":16500,
            "net_income":12500,"operating_cash_flow":18600,"capital_expenditures":6900,
            "depreciation_amortization":5000,"shareholders_equity":-2000,
            "total_assets":140000,"interest_expense":3600,"current_assets":22000,
            "current_liabilities":30000,"goodwill":43500,
            "accounts_receivable":8000,"inventory":0,
            "cash_and_equivalents":11000,"total_debt":85000,
            "dividends_paid":-4300,"share_repurchase":-1500,
            "sga_expense":9500,"rnd_expense":8500,
            "basic_weighted_average_shares":2730,
            "income_tax_expense_total":2500,"income_before_tax_total":15000,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":10000},
        "FY2025": {"revenue":57400,"cost_of_revenue":19000,"operating_income":18000,
            "net_income":15400,"operating_cash_flow":20800,"capital_expenditures":21200,
            "depreciation_amortization":6000,"shareholders_equity":2000,
            "total_assets":160000,"interest_expense":3800,"current_assets":24000,
            "current_liabilities":32000,"goodwill":44000,
            "accounts_receivable":9000,"inventory":0,
            "cash_and_equivalents":12000,"total_debt":88000,
            "dividends_paid":-4600,"share_repurchase":0,
            "sga_expense":9800,"rnd_expense":9000,
            "basic_weighted_average_shares":2800,
            "income_tax_expense_total":3000,"income_before_tax_total":18400,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":20000},
    },
    "downstream": [
        {"customer_name":"云基础设施 (OCI)","revenue_pct":0.30,"is_recurring":True,
         "revenue_type":"subscription","product_category":"cloud_infrastructure",
         "switching_cost_level":"high","contract_duration":"multi-year"},
        {"customer_name":"数据库/中间件许可","revenue_pct":0.25,"is_recurring":True,
         "revenue_type":"license","product_category":"operating_system",
         "switching_cost_level":"high","contract_duration":"multi-year"},
        {"customer_name":"SaaS 应用 (Fusion/NetSuite)","revenue_pct":0.30,"is_recurring":True,
         "revenue_type":"saas","product_category":"cloud_infrastructure",
         "switching_cost_level":"high","contract_duration":"3-year"},
        {"customer_name":"硬件/服务","revenue_pct":0.15,"is_recurring":False,
         "revenue_type":"recurring","product_category":"cloud_infrastructure"},
    ],
    "upstream": [],
    "geo": [{"region":"United States","revenue_share":0.55},{"region":"Europe","revenue_share":0.25},
            {"region":"Asia Pacific","revenue_share":0.15},{"region":"Other","revenue_share":0.05}],
    "pricing": [{"action":"OCI 云价格保持低于 AWS 30%","price_change_pct":-0.05,
         "product_or_segment":"OCI","effective_date":"2024-01","volume_impact_pct":0.30}],
    "market_share": [{"period":"FY2023","share":0.02,"source":"全球公有云"},
                     {"period":"FY2024","share":0.03,"source":"全球公有云"},
                     {"period":"FY2025","share":0.04,"source":"全球公有云"}],
    "competitive": [
        {"competitor_name":"AWS/Azure","event_type":"product_launch",
         "event_description":"AWS/Azure 主导公有云市场，OCI 市占率不到 5%",
         "outcome_description":"OCI 靠 Oracle 数据库存量客户迁移，云收入增速超 50%",
         "outcome_market_share_change":0.01},
    ],
    "peers": [
        {"peer_name":"Microsoft","metric":"gross_margin","value":0.70,"period":"FY2025"},
        {"peer_name":"Microsoft","metric":"operating_margin","value":0.45,"period":"FY2025"},
        {"peer_name":"Microsoft","metric":"net_margin","value":0.36,"period":"FY2025"},
    ],
    "narratives": [
        {"narrative":"OCI 云收入增速 50%+","status":"delivered"},
        {"narrative":"存量数据库客户迁移到云","status":"delivered"},
        {"narrative":"AI 基础设施合作（NVIDIA/OpenAI）","status":"delivered"},
        {"narrative":"多云策略（Azure+OCI 联合）","status":"delivered"},
        {"narrative":"Cerner 医疗 IT 整合","status":"in_progress"},
    ],
    "ownership": [{"name":"Larry Ellison","title":"创始人/CTO/董事长","percent_of_class":42.0}],
    "exec_comp": [{"name":"Safra Catz","title":"CEO","role_type":"CEO",
         "pay_ratio":200.0,"stock_awards":20000,"total_comp":22000}],
    "exec_changes": [{"name":"Safra Catz","title":"CEO","change_type":"joined"}],
    "market": {"price":190.0,"shares_outstanding":2800,"discount_rate":0.043,"market":"US"},
    "guidance": {"revenue_growth":0.15},
    "home_market": "United States",
}

# ── 2. 沃尔玛 (WMT) — 全球最大零售商 ────────────────────────
# 单位: 百万美元。财年截止 1/31
C["沃尔玛 (WMT)"] = {
    "fli": {
        # 沃尔玛 2024.2 做了 3:1 拆股，所有年份统一用拆股后口径
        "FY2022": {"revenue":573000,"cost_of_revenue":434000,"operating_income":25900,
            "net_income":11700,"operating_cash_flow":35700,"capital_expenditures":20600,
            "depreciation_amortization":10700,"shareholders_equity":83200,
            "total_assets":245000,"interest_expense":2100,"current_assets":80000,
            "current_liabilities":92000,"goodwill":28200,
            "accounts_receivable":7700,"inventory":56500,
            "cash_and_equivalents":14800,"total_debt":42000,
            "dividends_paid":-6100,"share_repurchase":-9700,
            "sga_expense":117000,"rnd_expense":0,
            "basic_weighted_average_shares":8160,
            "income_tax_expense_total":4800,"income_before_tax_total":16500,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":5000},
        "FY2023": {"revenue":611000,"cost_of_revenue":464000,"operating_income":26800,
            "net_income":11700,"operating_cash_flow":35700,"capital_expenditures":16900,
            "depreciation_amortization":11300,"shareholders_equity":76700,
            "total_assets":244000,"interest_expense":2300,"current_assets":75000,
            "current_liabilities":92000,"goodwill":28200,
            "accounts_receivable":7900,"inventory":54000,
            "cash_and_equivalents":8600,"total_debt":42000,
            "dividends_paid":-6100,"share_repurchase":-9400,
            "sga_expense":118000,"rnd_expense":0,
            "basic_weighted_average_shares":8130,
            "income_tax_expense_total":5600,"income_before_tax_total":17300,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":3000},
        "FY2024": {"revenue":648000,"cost_of_revenue":490000,"operating_income":27900,
            "net_income":15500,"operating_cash_flow":36400,"capital_expenditures":23800,
            "depreciation_amortization":11900,"shareholders_equity":85000,
            "total_assets":253000,"interest_expense":2400,"current_assets":77000,
            "current_liabilities":95000,"goodwill":28500,
            "accounts_receivable":8300,"inventory":55000,
            "cash_and_equivalents":9100,"total_debt":44000,
            "dividends_paid":-6200,"share_repurchase":-2700,
            "sga_expense":121000,"rnd_expense":0,
            "basic_weighted_average_shares":8040,
            "income_tax_expense_total":5400,"income_before_tax_total":20900,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":5000},
        "FY2025": {"revenue":681000,"cost_of_revenue":512000,"operating_income":30000,
            "net_income":19400,"operating_cash_flow":38400,"capital_expenditures":24700,
            "depreciation_amortization":12500,"shareholders_equity":91000,
            "total_assets":260000,"interest_expense":2500,"current_assets":80000,
            "current_liabilities":98000,"goodwill":29000,
            "accounts_receivable":8800,"inventory":57000,
            "cash_and_equivalents":9000,"total_debt":46000,
            "dividends_paid":-6500,"share_repurchase":-4000,
            "sga_expense":125000,"rnd_expense":0,
            "basic_weighted_average_shares":8100,
            "income_tax_expense_total":5800,"income_before_tax_total":25200,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":3000},
    },
    "downstream": [
        {"customer_name":"美国零售消费者","revenue_pct":0.65,"is_recurring":True,
         "revenue_type":"recurring","product_category":"grocery"},
        {"customer_name":"Sam's Club 会员","revenue_pct":0.12,"is_recurring":True,
         "revenue_type":"subscription","product_category":"grocery"},
        {"customer_name":"国际零售","revenue_pct":0.18,"is_recurring":True,
         "revenue_type":"recurring","product_category":"grocery"},
        {"customer_name":"电商/广告","revenue_pct":0.05,"is_recurring":True,
         "revenue_type":"ad_revenue","product_category":"consumer_electronics"},
    ],
    "upstream": [{"supplier_name":"全球供应商网络","is_sole_source":False,"geographic_location":"全球"}],
    "geo": [{"region":"United States","revenue_share":0.80},{"region":"Mexico/Central America","revenue_share":0.07},
            {"region":"China","revenue_share":0.05},{"region":"Other","revenue_share":0.08}],
    "pricing": [{"action":"EDLP 持续低价策略","price_change_pct":-0.02,
         "product_or_segment":"全品类","effective_date":"2024-01","volume_impact_pct":0.03}],
    "market_share": [{"period":"FY2023","share":0.26,"source":"美国零售"},
                     {"period":"FY2024","share":0.27,"source":"美国零售"},
                     {"period":"FY2025","share":0.27,"source":"美国零售"}],
    "competitive": [
        {"competitor_name":"Amazon","event_type":"price_war",
         "event_description":"Amazon 生鲜/杂货持续渗透",
         "outcome_description":"沃尔玛靠门店网络+配送到店守住杂货份额，线上增速 20%+",
         "outcome_market_share_change":0.01},
    ],
    "peers": [
        {"peer_name":"Costco","metric":"gross_margin","value":0.13,"period":"FY2025"},
        {"peer_name":"Costco","metric":"operating_margin","value":0.04,"period":"FY2025"},
        {"peer_name":"Costco","metric":"net_margin","value":0.03,"period":"FY2025"},
        {"peer_name":"Target","metric":"gross_margin","value":0.28,"period":"FY2025"},
        {"peer_name":"Target","metric":"operating_margin","value":0.05,"period":"FY2025"},
        {"peer_name":"Target","metric":"net_margin","value":0.03,"period":"FY2025"},
    ],
    "narratives": [
        {"narrative":"电商 GMV 增速 20%+","status":"delivered"},
        {"narrative":"广告业务 Walmart Connect 高速增长","status":"delivered"},
        {"narrative":"Walmart+ 会员数突破","status":"delivered"},
        {"narrative":"自动化配送中心投资","status":"delivered"},
    ],
    "ownership": [{"name":"Walton 家族","title":"控股家族","percent_of_class":47.0}],
    "exec_comp": [{"name":"Doug McMillon","title":"CEO","role_type":"CEO",
         "pay_ratio":1000.0,"stock_awards":15000,"total_comp":27000}],
    "exec_changes": [{"name":"John David Rainey","title":"CFO","change_type":"joined"}],
    "market": {"price":95.0,"shares_outstanding":8100,"discount_rate":0.043,"market":"US"},
    "guidance": {"revenue_growth":0.04},
    "home_market": "United States",
}

# ── 3. 高通 (QCOM) — 手机芯片霸主 + ARM PC 进击 ─────────────
# 单位: 百万美元。财年截止 9/30
C["高通 (QCOM)"] = {
    "fli": {
        "FY2022": {"revenue":44200,"cost_of_revenue":21500,"operating_income":15900,
            "net_income":12900,"operating_cash_flow":9100,"capital_expenditures":2100,
            "depreciation_amortization":2800,"shareholders_equity":11500,
            "total_assets":50000,"interest_expense":700,"current_assets":22000,
            "current_liabilities":12000,"goodwill":11300,
            "accounts_receivable":5500,"inventory":7000,
            "cash_and_equivalents":3000,"total_debt":16000,
            "dividends_paid":-3200,"share_repurchase":-4400,
            "sga_expense":2600,"rnd_expense":8200,
            "basic_weighted_average_shares":1120,
            "income_tax_expense_total":2200,"income_before_tax_total":15100,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":0},
        "FY2023": {"revenue":35800,"cost_of_revenue":18500,"operating_income":7800,
            "net_income":7200,"operating_cash_flow":11300,"capital_expenditures":1800,
            "depreciation_amortization":2500,"shareholders_equity":9800,
            "total_assets":48000,"interest_expense":800,"current_assets":21000,
            "current_liabilities":10500,"goodwill":11300,
            "accounts_receivable":4000,"inventory":5500,
            "cash_and_equivalents":8500,"total_debt":16000,
            "dividends_paid":-3300,"share_repurchase":-3400,
            "sga_expense":2400,"rnd_expense":8800,
            "basic_weighted_average_shares":1110,
            "income_tax_expense_total":1400,"income_before_tax_total":8600,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":0},
        "FY2024": {"revenue":38900,"cost_of_revenue":18800,"operating_income":10100,
            "net_income":10100,"operating_cash_flow":12200,"capital_expenditures":1600,
            "depreciation_amortization":2400,"shareholders_equity":9500,
            "total_assets":50000,"interest_expense":800,"current_assets":23000,
            "current_liabilities":13000,"goodwill":11300,
            "accounts_receivable":5000,"inventory":5500,
            "cash_and_equivalents":7800,"total_debt":15500,
            "dividends_paid":-3500,"share_repurchase":-4200,
            "sga_expense":2500,"rnd_expense":9200,
            "basic_weighted_average_shares":1100,
            "income_tax_expense_total":1500,"income_before_tax_total":11600,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":0},
        "FY2025": {"revenue":44000,"cost_of_revenue":21000,"operating_income":12000,
            "net_income":11000,"operating_cash_flow":14000,"capital_expenditures":2000,
            "depreciation_amortization":2500,"shareholders_equity":10000,
            "total_assets":52000,"interest_expense":800,"current_assets":24000,
            "current_liabilities":13500,"goodwill":11300,
            "accounts_receivable":5500,"inventory":5000,
            "cash_and_equivalents":8000,"total_debt":15000,
            "dividends_paid":-3700,"share_repurchase":-5000,
            "sga_expense":2600,"rnd_expense":9500,
            "basic_weighted_average_shares":1080,
            "income_tax_expense_total":1800,"income_before_tax_total":12800,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":0},
    },
    "downstream": [
        {"customer_name":"手机 OEM (三星/小米/OPPO)","revenue_pct":0.60,"is_recurring":False,
         "revenue_type":"recurring","product_category":"consumer_electronics"},
        {"customer_name":"专利许可费","revenue_pct":0.20,"is_recurring":True,
         "revenue_type":"license","product_category":"consumer_electronics"},
        {"customer_name":"汽车/IoT","revenue_pct":0.12,"is_recurring":False,
         "revenue_type":"recurring","product_category":"consumer_electronics"},
        {"customer_name":"PC (Snapdragon X)","revenue_pct":0.08,"is_recurring":False,
         "revenue_type":"recurring","product_category":"consumer_electronics"},
    ],
    "upstream": [{"supplier_name":"台积电/三星代工","is_sole_source":False,"geographic_location":"Taiwan/韩国"}],
    "geo": [{"region":"China","revenue_share":0.62},{"region":"United States","revenue_share":0.10},
            {"region":"South Korea","revenue_share":0.12},{"region":"Other","revenue_share":0.16}],
    "pricing": [],
    "market_share": [{"period":"FY2023","share":0.30,"source":"手机AP芯片"},
                     {"period":"FY2024","share":0.28,"source":"手机AP芯片"},
                     {"period":"FY2025","share":0.27,"source":"手机AP芯片"}],
    "competitive": [
        {"competitor_name":"联发科","event_type":"price_war",
         "event_description":"联发科天玑 9000+ 在中高端蚕食高通份额",
         "outcome_description":"高通在旗舰机保持领先但中端份额持续被抢",
         "outcome_market_share_change":-0.02},
        {"competitor_name":"苹果/华为","event_type":"new_entry",
         "event_description":"苹果自研芯片成功后，华为麒麟回归，大客户自研趋势加速",
         "outcome_description":"高通失去苹果基带订单，华为回归进一步压缩中国市场",
         "outcome_market_share_change":-0.03},
        # 专利护城河证据: 任何手机厂商都必须向高通交 3G/4G/5G 专利许可费
        {"competitor_name":"华为","event_type":"patent_challenge",
         "event_description":"华为在多国挑战高通 5G 标准必要专利（SEP）有效性",
         "outcome_description":"法院裁定高通专利有效，华为仍需支付许可费",
         "outcome_market_share_change":0.0},
        # 但：每代通信标准需要重新建立专利池，6G 时代谁领先不确定
        {"competitor_name":"6G 标准","event_type":"patent_expiration",
         "event_description":"5G 标准必要专利 2030 年代逐步过期，6G 专利竞赛已开始",
         "outcome_description":"华为在 6G 专利申请数量上领先高通",
         "outcome_market_share_change":0.0},
    ],
    "peers": [
        {"peer_name":"联发科","metric":"gross_margin","value":0.48,"period":"FY2025"},
        {"peer_name":"联发科","metric":"operating_margin","value":0.18,"period":"FY2025"},
        {"peer_name":"联发科","metric":"net_margin","value":0.15,"period":"FY2025"},
    ],
    "narratives": [
        {"narrative":"Snapdragon X PC 芯片挑战 Intel/AMD","status":"in_progress"},
        {"narrative":"汽车芯片 $300 亿设计订单","status":"delivered"},
        {"narrative":"AI 手机推动换机潮","status":"in_progress"},
        {"narrative":"保住苹果 5G 基带订单","status":"missed"},
    ],
    "ownership": [{"name":"Cristiano Amon","title":"CEO","percent_of_class":0.05}],
    "exec_comp": [{"name":"Cristiano Amon","title":"CEO","role_type":"CEO",
         "pay_ratio":250.0,"stock_awards":15000,"total_comp":24000}],
    "exec_changes": [{"name":"Akash Palkhiwala","title":"CFO","change_type":"joined"}],
    "market": {"price":160.0,"shares_outstanding":1080,"discount_rate":0.043,"market":"US"},
    "guidance": {"revenue_growth":0.10},
    "home_market": "United States",
}

# ── 4. SEA (SE) — 东南亚互联网三合一 ────────────────────────
# 单位: 百万美元
C["SEA (SE)"] = {
    "fli": {
        "FY2022": {"revenue":12400,"cost_of_revenue":7400,"operating_income":-1600,
            "net_income":-1700,"operating_cash_flow":1500,"capital_expenditures":800,
            "depreciation_amortization":900,"shareholders_equity":11000,
            "total_assets":27000,"interest_expense":300,"current_assets":12000,
            "current_liabilities":7000,"goodwill":3000,
            "accounts_receivable":1000,"inventory":0,
            "cash_and_equivalents":7500,"total_debt":5500,
            "dividends_paid":0,"share_repurchase":0,
            "sga_expense":3500,"rnd_expense":1200,
            "basic_weighted_average_shares":570,
            "income_tax_expense_total":200,"income_before_tax_total":-1500,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":2000},
        "FY2023": {"revenue":13100,"cost_of_revenue":7100,"operating_income":200,
            "net_income":163,"operating_cash_flow":2800,"capital_expenditures":500,
            "depreciation_amortization":800,"shareholders_equity":11500,
            "total_assets":26000,"interest_expense":300,"current_assets":13000,
            "current_liabilities":7500,"goodwill":3000,
            "accounts_receivable":1100,"inventory":0,
            "cash_and_equivalents":8000,"total_debt":5000,
            "dividends_paid":0,"share_repurchase":-500,
            "sga_expense":2500,"rnd_expense":1000,
            "basic_weighted_average_shares":575,
            "income_tax_expense_total":100,"income_before_tax_total":263,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":0},
        "FY2024": {"revenue":16800,"cost_of_revenue":8800,"operating_income":600,
            "net_income":448,"operating_cash_flow":3200,"capital_expenditures":600,
            "depreciation_amortization":750,"shareholders_equity":13000,
            "total_assets":28000,"interest_expense":250,"current_assets":14000,
            "current_liabilities":8000,"goodwill":3000,
            "accounts_receivable":1200,"inventory":0,
            "cash_and_equivalents":8500,"total_debt":4500,
            "dividends_paid":0,"share_repurchase":-700,
            "sga_expense":3000,"rnd_expense":1100,
            "basic_weighted_average_shares":580,
            "income_tax_expense_total":150,"income_before_tax_total":598,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":0},
        "FY2025": {"revenue":21000,"cost_of_revenue":10500,"operating_income":1500,
            "net_income":900,"operating_cash_flow":4000,"capital_expenditures":800,
            "depreciation_amortization":700,"shareholders_equity":14000,
            "total_assets":30000,"interest_expense":200,"current_assets":15000,
            "current_liabilities":8500,"goodwill":3000,
            "accounts_receivable":1400,"inventory":0,
            "cash_and_equivalents":9000,"total_debt":4000,
            "dividends_paid":0,"share_repurchase":-1000,
            "sga_expense":3500,"rnd_expense":1300,
            "basic_weighted_average_shares":585,
            "income_tax_expense_total":200,"income_before_tax_total":1100,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":0},
    },
    "downstream": [
        {"customer_name":"Shopee 电商","revenue_pct":0.65,"is_recurring":True,
         "revenue_type":"transaction_fee","product_category":"consumer_electronics"},
        {"customer_name":"SeaMoney 金融","revenue_pct":0.12,"is_recurring":True,
         "revenue_type":"transaction_fee","product_category":"payment"},
        {"customer_name":"Garena 游戏","revenue_pct":0.10,"is_recurring":True,
         "revenue_type":"transaction_fee","product_category":"gaming"},
        {"customer_name":"广告","revenue_pct":0.08,"is_recurring":True,
         "revenue_type":"ad_revenue","product_category":"consumer_electronics"},
        {"customer_name":"其他","revenue_pct":0.05,"is_recurring":False,
         "revenue_type":"recurring","product_category":"consumer_electronics"},
    ],
    "upstream": [],
    "geo": [{"region":"Southeast Asia","revenue_share":0.65},{"region":"Taiwan","revenue_share":0.10},
            {"region":"Brazil","revenue_share":0.15},{"region":"Other","revenue_share":0.10}],
    "pricing": [],
    "market_share": [{"period":"FY2023","share":0.48,"source":"东南亚电商"},
                     {"period":"FY2024","share":0.46,"source":"东南亚电商"},
                     {"period":"FY2025","share":0.43,"source":"东南亚电商"}],
    "competitive": [
        {"competitor_name":"Lazada (阿里)","event_type":"price_war",
         "event_description":"Lazada 烧钱补贴",
         "outcome_description":"Shopee 被迫跟进补贴，2022 年利润从正转巨亏 $17 亿",
         "outcome_market_share_change":0.0},
        {"competitor_name":"TikTok Shop","event_type":"new_entry",
         "event_description":"TikTok Shop 2023 年底因印尼政策暂时撤退，但 2024 年回归后 GMV 暴涨，与 Tokopedia 合并",
         "outcome_description":"TikTok Shop 份额快速升至 15-20%，Shopee 被迫烧钱保份额",
         "outcome_market_share_change":-0.05},
    ],
    "peers": [
        {"peer_name":"MercadoLibre","metric":"gross_margin","value":0.50,"period":"FY2025"},
        {"peer_name":"MercadoLibre","metric":"operating_margin","value":0.12,"period":"FY2025"},
        {"peer_name":"MercadoLibre","metric":"net_margin","value":0.08,"period":"FY2025"},
    ],
    "narratives": [
        {"narrative":"Shopee 东南亚电商第一","status":"delivered"},
        {"narrative":"扭亏为盈（连续两年盈利）","status":"delivered"},
        {"narrative":"SeaMoney 金融科技盈利","status":"delivered"},
        {"narrative":"Garena Free Fire 回归增长","status":"missed"},
        {"narrative":"巴西市场站稳脚跟","status":"delivered"},
        {"narrative":"抵御 TikTok Shop 竞争","status":"missed"},
    ],
    "ownership": [{"name":"Forrest Li","title":"CEO/创始人","percent_of_class":20.0}],
    "exec_comp": [{"name":"Forrest Li","title":"CEO","role_type":"CEO",
         "pay_ratio":80.0,"stock_awards":3000,"total_comp":4000}],
    "exec_changes": [],
    "market": {"price":130.0,"shares_outstanding":585,"discount_rate":0.043,"market":"US"},
    "guidance": {"revenue_growth":0.20},
    "home_market": "Singapore",
}

# ── 5. 拼多多 (PDD) — 中国+Temu 全球低价电商 ────────────────
# 单位: 百万人民币
C["拼多多 (PDD)"] = {
    "fli": {
        "FY2022": {"revenue":130600,"cost_of_revenue":42500,"operating_income":40000,
            "net_income":31500,"operating_cash_flow":45000,"capital_expenditures":1500,
            "depreciation_amortization":3000,"shareholders_equity":135000,
            "total_assets":215000,"interest_expense":100,"current_assets":160000,
            "current_liabilities":65000,"goodwill":0,
            "accounts_receivable":2000,"inventory":0,
            "cash_and_equivalents":100000,"total_debt":0,
            "dividends_paid":0,"share_repurchase":-5000,
            "sga_expense":40000,"rnd_expense":10000,
            "basic_weighted_average_shares":4800,
            "income_tax_expense_total":5000,"income_before_tax_total":36500,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":0},
        "FY2023": {"revenue":247600,"cost_of_revenue":72000,"operating_income":82000,
            "net_income":60000,"operating_cash_flow":75000,"capital_expenditures":2000,
            "depreciation_amortization":3500,"shareholders_equity":200000,
            "total_assets":320000,"interest_expense":100,"current_assets":250000,
            "current_liabilities":100000,"goodwill":0,
            "accounts_receivable":3000,"inventory":0,
            "cash_and_equivalents":185000,"total_debt":0,
            "dividends_paid":0,"share_repurchase":-8000,
            "sga_expense":75000,"rnd_expense":11000,
            "basic_weighted_average_shares":4900,
            "income_tax_expense_total":10000,"income_before_tax_total":70000,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":0},
        "FY2024": {"revenue":393800,"cost_of_revenue":115000,"operating_income":110000,
            "net_income":83000,"operating_cash_flow":115000,"capital_expenditures":3000,
            "depreciation_amortization":4000,"shareholders_equity":280000,
            "total_assets":450000,"interest_expense":100,"current_assets":370000,
            "current_liabilities":140000,"goodwill":0,
            "accounts_receivable":4000,"inventory":0,
            "cash_and_equivalents":280000,"total_debt":0,
            "dividends_paid":0,"share_repurchase":-15000,
            "sga_expense":105000,"rnd_expense":12000,
            "basic_weighted_average_shares":5000,
            "income_tax_expense_total":15000,"income_before_tax_total":98000,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":0},
        "FY2025": {"revenue":450000,"cost_of_revenue":140000,"operating_income":115000,
            "net_income":85000,"operating_cash_flow":120000,"capital_expenditures":4000,
            "depreciation_amortization":4500,"shareholders_equity":350000,
            "total_assets":550000,"interest_expense":100,"current_assets":450000,
            "current_liabilities":160000,"goodwill":0,
            "accounts_receivable":5000,"inventory":0,
            "cash_and_equivalents":340000,"total_debt":0,
            "dividends_paid":0,"share_repurchase":-20000,
            "sga_expense":110000,"rnd_expense":13000,
            "basic_weighted_average_shares":5100,
            "income_tax_expense_total":16000,"income_before_tax_total":101000,
            "proceeds_from_stock_issuance":0,"proceeds_from_debt_issuance":0},
    },
    "downstream": [
        {"customer_name":"国内消费者","revenue_pct":0.60,"is_recurring":True,
         "revenue_type":"transaction_fee","product_category":"consumer_electronics"},
        {"customer_name":"Temu 全球消费者","revenue_pct":0.35,"is_recurring":True,
         "revenue_type":"transaction_fee","product_category":"consumer_electronics"},
        {"customer_name":"广告","revenue_pct":0.05,"is_recurring":True,
         "revenue_type":"ad_revenue","product_category":"consumer_electronics"},
    ],
    "upstream": [],
    "geo": [{"region":"China","revenue_share":0.60},{"region":"United States","revenue_share":0.15},
            {"region":"Europe","revenue_share":0.10},{"region":"Other","revenue_share":0.15}],
    "pricing": [],
    "market_share": [{"period":"FY2023","share":0.22,"source":"中国电商"},
                     {"period":"FY2024","share":0.25,"source":"中国电商"},
                     {"period":"FY2025","share":0.28,"source":"中国电商"}],
    "competitive": [
        # 拼多多是进攻者不是防守者——份额是靠低价抢来的不是靠护城河守住的
        {"competitor_name":"阿里巴巴","event_type":"price_war",
         "event_description":"拼多多低价策略侵蚀淘宝份额，但阿里已反击（百亿补贴+价格力排名）",
         "outcome_description":"阿里反击后拼多多增速放缓，份额增长趋平",
         "outcome_market_share_change":0.02},
        {"competitor_name":"抖音电商","event_type":"new_entry",
         "event_description":"抖音电商 GMV 突破 4 万亿，直播+货架双模式",
         "outcome_description":"抖音电商分流价格敏感用户，拼多多核心用户群被蚕食",
         "outcome_market_share_change":-0.02},
    ],
    "peers": [
        {"peer_name":"阿里巴巴","metric":"gross_margin","value":0.38,"period":"FY2025"},
        {"peer_name":"阿里巴巴","metric":"operating_margin","value":0.13,"period":"FY2025"},
        {"peer_name":"阿里巴巴","metric":"net_margin","value":0.09,"period":"FY2025"},
    ],
    "narratives": [
        {"narrative":"国内电商份额持续提升","status":"delivered"},
        {"narrative":"Temu 全球化高速扩张","status":"delivered"},
        {"narrative":"极致低价供应链效率","status":"delivered"},
        {"narrative":"开始回购回馈股东","status":"delivered"},
        {"narrative":"Temu 盈利","status":"missed"},
        {"narrative":"商家生态健康可持续","status":"missed"},
    ],
    "ownership": [{"name":"陈磊","title":"CEO/董事长","percent_of_class":5.5}],
    "exec_comp": [{"name":"陈磊","title":"CEO","role_type":"CEO",
         "pay_ratio":30.0,"stock_awards":0,"total_comp":500}],
    "exec_changes": [],
    "known_issues": [
        {"issue_description":"Temu 面临美国关税政策变化（de minimis 取消），成本可能大幅上升",
         "severity":"high","source_type":"regulatory"},
        {"issue_description":"拼多多商家生态抽佣过高引发大规模抗议",
         "severity":"medium","source_type":"news"},
    ],
    "management_acks": [
        {"issue_description":"承认 Temu 面临政策不确定性，正在调整供应链本地化策略",
         "response_quality":"adequate","has_action_plan":True},
        {"issue_description":"承诺降低商家费率，投入百亿补贴商家",
         "response_quality":"strong","has_action_plan":True},
    ],
    "market": {"price":110.0,"shares_outstanding":5100,"discount_rate":0.017,"market":"CN"},
    "guidance": {"revenue_growth":0.15},
    "home_market": "China",
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
        "debt_obligations":EMPTY,"debt_obligations_all":EMPTY,"litigations":EMPTY,
        "operational_issues":EMPTY,"related_party_transactions":EMPTY,"non_financial_kpis":EMPTY,
        "audit_opinions":EMPTY,
        "known_issues": pd.DataFrame([{"id":i,**r} for i,r in enumerate(ki_rows)]) if ki_rows else EMPTY,
        "insider_transactions":EMPTY,
        "executive_changes": pd.DataFrame([{"id":i,**r} for i,r in enumerate(ec_rows)]) if ec_rows else EMPTY,
        "equity_offerings":EMPTY,"analyst_estimates":EMPTY,"management_guidance":EMPTY,
        "management_acknowledgments": pd.DataFrame([{"id":i,**r} for i,r in enumerate(ma_rows)]) if ma_rows else EMPTY,
        "brand_signals":EMPTY,
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
    for name, data in C.items():
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
        mc = mkt["market"]
        l1q = (moat.depth in ("extreme","deep") and earnings.verdict=="holds" and pred.verdict=="holds")
        cert = "high" if l1q else "normal"
        dcf = mos = None
        if mkt.get("discount_rate") and mkt.get("shares_outstanding"):
            dcf = compute_intrinsic_value(ctx.features, guidance, mkt["discount_rate"],
                                           mkt["shares_outstanding"], market=mc, certainty=cert)
            if dcf.intrinsic_value:
                mos = (dcf.intrinsic_value - mkt["price"]) / dcf.intrinsic_value
        integrity = assess_integrity(ctx)
        character = assess_character(ctx)
        risk = assess_risk(ctx, home_market=data.get("home_market", ""))
        l1ok = moat.depth not in ("none","unknown") and earnings.verdict=="holds" and dist.verdict=="holds"
        l2ok = integrity.verdict != "breaks" and not risk.has_catastrophic

        unit = "元" if mc in ("CN","HK") else "$"
        print(f"\n  线 1:")
        print(f"    护城河:   {moat.depth} — {moat.summary[:80]}")
        print(f"    盈余能力: {earnings.verdict} — {earnings.summary[:80]}")
        print(f"    利润分配: {dist.verdict} — {dist.summary[:80]}")
        print(f"    可预测:   {pred.verdict} — {pred.summary[:80]}")
        if dcf and dcf.intrinsic_value:
            ct = "无风险" if cert=="high" else "无风险+ERP"
            print(f"    估值:     {unit}{dcf.intrinsic_value:,.0f} vs {unit}{mkt['price']}  "
                  f"安全边际 {mos:.0%}  [{ct}, 路径{dcf.valuation_path}]")
        elif dcf:
            print(f"    估值:     {dcf.status}")
        print(f"  线 2:")
        print(f"    诚信:     {integrity.verdict} — {integrity.summary[:60]}")
        print(f"    管理层:   {character.conviction} — {character.summary[:50]}")
        kp = [r for r in risk.risks if r.category == "key_person"]
        print(f"    关键人:   {kp[0].level+': '+kp[0].description[:50] if kp else '无'}")
        cr = "灾难性" if risk.has_catastrophic else (f"{len(risk.significant)}项重大" if risk.significant else "可控")
        print(f"    风险:     {cr}")
        # 安全边际检查: 生意再好，太贵不买
        overvalued = mos is not None and mos < 0

        if l1ok and l2ok and not overvalued:
            print(f"  → ✅ 可以投资")
        elif l1ok and l2ok and overvalued:
            print(f"  → ⚠️ 好生意但太贵（安全边际 {mos:.0%}），等便宜再买")
        elif l1ok and not l2ok:
            print(f"  → ⚠️ 好生意但{'灾难性风险' if risk.has_catastrophic else '诚信存疑'}")
        else:
            broken = []
            if moat.depth in ("none","unknown"): broken.append("护城河")
            if earnings.verdict != "holds": broken.append("盈余")
            if dist.verdict != "holds": broken.append("分配")
            print(f"  → ❌ 断裂: {', '.join(broken)}")
        print()
