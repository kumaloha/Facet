"""
大规模 Mock 压力测试
====================
模拟 20 家不同类型的公司，每家 8 期财务数据（FY2018-FY2025），
跑完整特征计算 + 三流派评分管线。

公司类型覆盖：
- SaaS 高增长        - 传统制造业       - 金融（高杠杆）
- 消费品（稳定现金牛） - 周期股（波动大）   - 亏损独角兽
- 科技垄断           - 药企（研发重）     - 零售低利润
- 地产高负债          - 军工（政府客户）   - 能源（重资产）
- 奢侈品（高毛利）    - 电信（稳定低增长）  - 中概（地域风险）
- 日本财阀（交叉持股） - 加密交易所        - 半导体周期
- 并购驱动           - 家族企业
"""

import random
import time
from dataclasses import dataclass

import pandas as pd

from polaris.features.types import ComputeContext, FeatureResult, FeatureLevel
from polaris.features.registry import get_features
from polaris.scoring.scorer import score_company, format_report

import polaris.features.l0.company  # noqa: F401
import polaris.features.l0.cross_period  # noqa: F401

EMPTY = pd.DataFrame()
PERIODS = ["FY2018", "FY2019", "FY2020", "FY2021", "FY2022", "FY2023", "FY2024", "FY2025"]


# ── Mock 数据工厂（复用自 test_mock_pipeline）──────────────────────

def _fli(items: dict[str, float], period: str = "FY2025") -> pd.DataFrame:
    rows = [{"id": i, "statement_id": 1, "item_key": k, "item_label": k,
             "value": v, "parent_key": None, "ordinal": i, "note": None, "period": period}
            for i, (k, v) in enumerate(items.items())]
    return pd.DataFrame(rows)


def _downstream(segments: list[dict]) -> pd.DataFrame:
    defaults = {"id": 0, "company_id": 1, "period": "FY2025", "segment": None,
                "customer_type": None, "products": None, "channels": None,
                "revenue": None, "growth_yoy": None, "backlog": None,
                "backlog_note": None, "pricing_model": None, "contract_duration": None,
                "recognition_method": None, "description": None,
                "raw_post_id": None, "created_at": "2025-01-01"}
    return pd.DataFrame([{**defaults, "id": i, **s} for i, s in enumerate(segments)])


def _upstream(suppliers: list[dict]) -> pd.DataFrame:
    defaults = {"id": 0, "company_id": 1, "period": "FY2025", "segment": None,
                "supply_type": "component", "material_or_service": None,
                "process_node": None, "purchase_obligation": None,
                "contract_type": None, "prepaid_amount": None,
                "concentration_risk": None, "description": None,
                "raw_post_id": None, "created_at": "2025-01-01"}
    return pd.DataFrame([{**defaults, "id": i, **s} for i, s in enumerate(suppliers)])


def _geo(regions: list[dict]) -> pd.DataFrame:
    defaults = {"id": 0, "company_id": 1, "period": "FY2025",
                "revenue": None, "growth_yoy": None, "note": None,
                "raw_post_id": None, "created_at": "2025-01-01"}
    return pd.DataFrame([{**defaults, "id": i, **r} for i, r in enumerate(regions)])


def _debt(obligations: list[dict]) -> pd.DataFrame:
    defaults = {"id": 0, "company_id": 1, "period": "FY2025",
                "instrument_name": "Note", "debt_type": "unsecured", "currency": "USD",
                "interest_rate": None, "maturity_date": None, "is_secured": False,
                "note": None, "raw_post_id": None, "created_at": "2025-01-01"}
    return pd.DataFrame([{**defaults, "id": i, **d} for i, d in enumerate(obligations)])


def _exec_comp(execs: list[dict]) -> pd.DataFrame:
    defaults = {"id": 0, "company_id": 1, "period": "FY2025", "role_type": "officer",
                "base_salary": None, "bonus": None, "option_awards": None,
                "non_equity_incentive": None, "other_comp": None, "currency": "USD",
                "median_employee_comp": None, "raw_post_id": None, "created_at": "2025-01-01"}
    return pd.DataFrame([{**defaults, "id": i, **e} for i, e in enumerate(execs)])


def _ownership(owners: list[dict]) -> pd.DataFrame:
    defaults = {"id": 0, "company_id": 1, "period": "FY2025",
                "shares_beneficially_owned": None, "raw_post_id": None, "created_at": "2025-01-01"}
    return pd.DataFrame([{**defaults, "id": i, **o} for i, o in enumerate(owners)])


def _narratives(items: list[dict]) -> pd.DataFrame:
    defaults = {"id": 0, "company_id": 1, "raw_post_id": None, "capital_required": None,
                "capital_unit": None, "promised_outcome": None, "deadline": None,
                "reported_at": None, "created_at": "2025-01-01"}
    return pd.DataFrame([{**defaults, "id": i, **n} for i, n in enumerate(items)])


def _litigations(items: list[dict]) -> pd.DataFrame:
    defaults = {"id": 0, "company_id": 1, "case_name": "Case", "case_type": "civil",
                "counterparty": None, "filed_at": None, "currency": "USD",
                "description": None, "resolution": None, "resolved_at": None,
                "raw_post_id": None, "created_at": "2025-01-01"}
    return pd.DataFrame([{**defaults, "id": i, **l} for i, l in enumerate(items)])


def _ops_issues(issues: list[dict]) -> pd.DataFrame:
    defaults = {"id": 0, "company_id": 1, "period": "FY2025", "raw_post_id": None,
                "performance": None, "attribution": None, "created_at": "2025-01-01"}
    return pd.DataFrame([{**defaults, "id": i, **o} for i, o in enumerate(issues)])


def _rpt(txns: list[dict]) -> pd.DataFrame:
    defaults = {"id": 0, "company_id": 1, "period": "FY2025", "currency": "USD",
                "terms": None, "description": None, "raw_post_id": None, "created_at": "2025-01-01"}
    return pd.DataFrame([{**defaults, "id": i, **t} for i, t in enumerate(txns)])


# ── 公司原型定义 ──────────────────────────────────────────────────

@dataclass
class CompanyProfile:
    name: str
    ticker: str
    archetype: str  # 公司类型
    # 基期财务（FY2018），后续按 growth 递推
    revenue: float
    cogs_pct: float           # 成本率
    opex_pct: float           # 营业费用率（SGA+R&D）
    tax_rate: float
    capex_pct: float          # capex/revenue
    da_pct: float             # 折旧/revenue
    ocf_margin: float         # OCF/revenue
    equity: float
    total_assets: float
    debt: float
    cash: float
    interest_rate: float
    current_debt_pct: float   # 短期债务占比
    shares: float
    # 增长参数
    revenue_growth: float     # 年均增速
    growth_volatility: float  # 增速波动
    margin_drift: float       # 毛利率年漂移
    debt_growth: float        # 债务增速
    # 非财务
    recurring_pct: float
    top_customer_pct: float
    sole_source_pct: float
    china_revenue_pct: float
    mgmt_ownership: float
    ceo_pay_ratio: float
    narrative_fulfillment: float
    litigation_count: int
    rpt_amount_pct: float     # 关联交易/revenue
    ops_issue_count: int
    # 融资
    equity_issuance_pct: float   # 股权融资/capex
    debt_issuance_pct: float     # 债务融资/capex


COMPANY_PROFILES: list[CompanyProfile] = [
    CompanyProfile("CloudSaaS Inc", "SAAS", "SaaS高增长",
                   revenue=2000, cogs_pct=0.25, opex_pct=0.45, tax_rate=0.15,
                   capex_pct=0.03, da_pct=0.02, ocf_margin=0.30,
                   equity=5000, total_assets=8000, debt=1000, cash=3000,
                   interest_rate=0.04, current_debt_pct=0.2, shares=500,
                   revenue_growth=0.35, growth_volatility=0.08, margin_drift=0.005, debt_growth=0.05,
                   recurring_pct=0.90, top_customer_pct=0.08, sole_source_pct=0.1, china_revenue_pct=0.05,
                   mgmt_ownership=12.0, ceo_pay_ratio=80, narrative_fulfillment=0.85,
                   litigation_count=0, rpt_amount_pct=0.0, ops_issue_count=2,
                   equity_issuance_pct=0.0, debt_issuance_pct=0.0),

    CompanyProfile("SteelWorks Corp", "STWK", "传统制造业",
                   revenue=50000, cogs_pct=0.75, opex_pct=0.12, tax_rate=0.22,
                   capex_pct=0.08, da_pct=0.06, ocf_margin=0.10,
                   equity=20000, total_assets=60000, debt=15000, cash=3000,
                   interest_rate=0.05, current_debt_pct=0.3, shares=2000,
                   revenue_growth=0.03, growth_volatility=0.06, margin_drift=-0.002, debt_growth=0.02,
                   recurring_pct=0.20, top_customer_pct=0.12, sole_source_pct=0.05, china_revenue_pct=0.15,
                   mgmt_ownership=2.0, ceo_pay_ratio=200, narrative_fulfillment=0.60,
                   litigation_count=2, rpt_amount_pct=0.01, ops_issue_count=8,
                   equity_issuance_pct=0.0, debt_issuance_pct=0.3),

    CompanyProfile("MegaBank Holdings", "MBHD", "金融高杠杆",
                   revenue=30000, cogs_pct=0.10, opex_pct=0.50, tax_rate=0.20,
                   capex_pct=0.02, da_pct=0.01, ocf_margin=0.35,
                   equity=15000, total_assets=300000, debt=250000, cash=20000,
                   interest_rate=0.035, current_debt_pct=0.5, shares=3000,
                   revenue_growth=0.05, growth_volatility=0.10, margin_drift=0.0, debt_growth=0.04,
                   recurring_pct=0.70, top_customer_pct=0.03, sole_source_pct=0.0, china_revenue_pct=0.02,
                   mgmt_ownership=1.0, ceo_pay_ratio=400, narrative_fulfillment=0.55,
                   litigation_count=8, rpt_amount_pct=0.02, ops_issue_count=12,
                   equity_issuance_pct=0.0, debt_issuance_pct=0.5),

    CompanyProfile("CokeClone Brands", "COKE", "消费品现金牛",
                   revenue=40000, cogs_pct=0.35, opex_pct=0.25, tax_rate=0.21,
                   capex_pct=0.04, da_pct=0.03, ocf_margin=0.28,
                   equity=25000, total_assets=50000, debt=10000, cash=8000,
                   interest_rate=0.035, current_debt_pct=0.2, shares=4000,
                   revenue_growth=0.04, growth_volatility=0.02, margin_drift=0.001, debt_growth=0.01,
                   recurring_pct=0.85, top_customer_pct=0.05, sole_source_pct=0.02, china_revenue_pct=0.10,
                   mgmt_ownership=3.0, ceo_pay_ratio=250, narrative_fulfillment=0.80,
                   litigation_count=1, rpt_amount_pct=0.005, ops_issue_count=3,
                   equity_issuance_pct=0.0, debt_issuance_pct=0.0),

    CompanyProfile("CycloChem Industries", "CYCL", "周期股波动大",
                   revenue=20000, cogs_pct=0.60, opex_pct=0.15, tax_rate=0.20,
                   capex_pct=0.10, da_pct=0.07, ocf_margin=0.15,
                   equity=12000, total_assets=35000, debt=8000, cash=2000,
                   interest_rate=0.05, current_debt_pct=0.35, shares=1500,
                   revenue_growth=0.02, growth_volatility=0.25, margin_drift=0.0, debt_growth=0.03,
                   recurring_pct=0.10, top_customer_pct=0.20, sole_source_pct=0.15, china_revenue_pct=0.25,
                   mgmt_ownership=5.0, ceo_pay_ratio=150, narrative_fulfillment=0.50,
                   litigation_count=3, rpt_amount_pct=0.02, ops_issue_count=7,
                   equity_issuance_pct=0.0, debt_issuance_pct=0.2),

    CompanyProfile("BurnRate Unicorn", "BURN", "亏损独角兽",
                   revenue=800, cogs_pct=0.40, opex_pct=0.80, tax_rate=0.0,
                   capex_pct=0.15, da_pct=0.05, ocf_margin=-0.20,
                   equity=2000, total_assets=5000, debt=500, cash=3000,
                   interest_rate=0.06, current_debt_pct=0.1, shares=300,
                   revenue_growth=0.60, growth_volatility=0.15, margin_drift=0.02, debt_growth=0.10,
                   recurring_pct=0.50, top_customer_pct=0.25, sole_source_pct=0.30, china_revenue_pct=0.0,
                   mgmt_ownership=25.0, ceo_pay_ratio=50, narrative_fulfillment=0.40,
                   litigation_count=0, rpt_amount_pct=0.0, ops_issue_count=4,
                   equity_issuance_pct=3.0, debt_issuance_pct=1.0),

    CompanyProfile("SearchMonopoly", "SRCH", "科技垄断",
                   revenue=80000, cogs_pct=0.30, opex_pct=0.25, tax_rate=0.16,
                   capex_pct=0.12, da_pct=0.08, ocf_margin=0.35,
                   equity=100000, total_assets=150000, debt=15000, cash=50000,
                   interest_rate=0.03, current_debt_pct=0.15, shares=6000,
                   revenue_growth=0.15, growth_volatility=0.05, margin_drift=0.002, debt_growth=0.03,
                   recurring_pct=0.95, top_customer_pct=0.02, sole_source_pct=0.0, china_revenue_pct=0.08,
                   mgmt_ownership=10.0, ceo_pay_ratio=300, narrative_fulfillment=0.75,
                   litigation_count=5, rpt_amount_pct=0.001, ops_issue_count=6,
                   equity_issuance_pct=0.0, debt_issuance_pct=0.0),

    CompanyProfile("BioPharmaCure", "BIOP", "药企研发重",
                   revenue=15000, cogs_pct=0.20, opex_pct=0.50, tax_rate=0.12,
                   capex_pct=0.05, da_pct=0.03, ocf_margin=0.25,
                   equity=20000, total_assets=35000, debt=5000, cash=10000,
                   interest_rate=0.04, current_debt_pct=0.2, shares=1000,
                   revenue_growth=0.10, growth_volatility=0.12, margin_drift=0.0, debt_growth=0.02,
                   recurring_pct=0.60, top_customer_pct=0.15, sole_source_pct=0.20, china_revenue_pct=0.12,
                   mgmt_ownership=4.0, ceo_pay_ratio=350, narrative_fulfillment=0.45,
                   litigation_count=4, rpt_amount_pct=0.01, ops_issue_count=5,
                   equity_issuance_pct=0.5, debt_issuance_pct=0.0),

    CompanyProfile("DiscountMart", "DSCM", "零售低利润",
                   revenue=200000, cogs_pct=0.78, opex_pct=0.16, tax_rate=0.22,
                   capex_pct=0.03, da_pct=0.02, ocf_margin=0.05,
                   equity=30000, total_assets=80000, debt=20000, cash=5000,
                   interest_rate=0.04, current_debt_pct=0.4, shares=5000,
                   revenue_growth=0.06, growth_volatility=0.03, margin_drift=-0.001, debt_growth=0.03,
                   recurring_pct=0.30, top_customer_pct=0.01, sole_source_pct=0.0, china_revenue_pct=0.30,
                   mgmt_ownership=0.5, ceo_pay_ratio=600, narrative_fulfillment=0.70,
                   litigation_count=2, rpt_amount_pct=0.003, ops_issue_count=4,
                   equity_issuance_pct=0.0, debt_issuance_pct=0.2),

    CompanyProfile("PropertyDev Group", "PROP", "地产高负债",
                   revenue=25000, cogs_pct=0.65, opex_pct=0.10, tax_rate=0.25,
                   capex_pct=0.02, da_pct=0.01, ocf_margin=0.08,
                   equity=10000, total_assets=80000, debt=55000, cash=3000,
                   interest_rate=0.06, current_debt_pct=0.45, shares=2000,
                   revenue_growth=0.08, growth_volatility=0.15, margin_drift=-0.005, debt_growth=0.10,
                   recurring_pct=0.05, top_customer_pct=0.03, sole_source_pct=0.0, china_revenue_pct=0.80,
                   mgmt_ownership=30.0, ceo_pay_ratio=100, narrative_fulfillment=0.35,
                   litigation_count=6, rpt_amount_pct=0.08, ops_issue_count=15,
                   equity_issuance_pct=0.5, debt_issuance_pct=3.0),

    CompanyProfile("DefenseTech Corp", "DFTC", "军工政府客户",
                   revenue=18000, cogs_pct=0.55, opex_pct=0.20, tax_rate=0.21,
                   capex_pct=0.06, da_pct=0.04, ocf_margin=0.12,
                   equity=15000, total_assets=30000, debt=5000, cash=4000,
                   interest_rate=0.04, current_debt_pct=0.2, shares=800,
                   revenue_growth=0.05, growth_volatility=0.03, margin_drift=0.001, debt_growth=0.02,
                   recurring_pct=0.70, top_customer_pct=0.40, sole_source_pct=0.10, china_revenue_pct=0.0,
                   mgmt_ownership=2.0, ceo_pay_ratio=280, narrative_fulfillment=0.65,
                   litigation_count=1, rpt_amount_pct=0.005, ops_issue_count=3,
                   equity_issuance_pct=0.0, debt_issuance_pct=0.0),

    CompanyProfile("PetroGiant", "PTRO", "能源重资产",
                   revenue=120000, cogs_pct=0.65, opex_pct=0.10, tax_rate=0.30,
                   capex_pct=0.15, da_pct=0.10, ocf_margin=0.20,
                   equity=80000, total_assets=200000, debt=40000, cash=10000,
                   interest_rate=0.045, current_debt_pct=0.25, shares=8000,
                   revenue_growth=0.02, growth_volatility=0.20, margin_drift=0.0, debt_growth=0.02,
                   recurring_pct=0.05, top_customer_pct=0.05, sole_source_pct=0.0, china_revenue_pct=0.08,
                   mgmt_ownership=0.3, ceo_pay_ratio=500, narrative_fulfillment=0.55,
                   litigation_count=3, rpt_amount_pct=0.01, ops_issue_count=6,
                   equity_issuance_pct=0.0, debt_issuance_pct=0.1),

    CompanyProfile("LuxuryMaison", "LUXE", "奢侈品高毛利",
                   revenue=12000, cogs_pct=0.18, opex_pct=0.35, tax_rate=0.20,
                   capex_pct=0.04, da_pct=0.03, ocf_margin=0.30,
                   equity=18000, total_assets=25000, debt=3000, cash=6000,
                   interest_rate=0.03, current_debt_pct=0.15, shares=600,
                   revenue_growth=0.08, growth_volatility=0.04, margin_drift=0.002, debt_growth=0.01,
                   recurring_pct=0.40, top_customer_pct=0.02, sole_source_pct=0.25, china_revenue_pct=0.22,
                   mgmt_ownership=35.0, ceo_pay_ratio=90, narrative_fulfillment=0.90,
                   litigation_count=0, rpt_amount_pct=0.0, ops_issue_count=1,
                   equity_issuance_pct=0.0, debt_issuance_pct=0.0),

    CompanyProfile("TelecomStable", "TELC", "电信稳定低增长",
                   revenue=60000, cogs_pct=0.40, opex_pct=0.30, tax_rate=0.22,
                   capex_pct=0.12, da_pct=0.10, ocf_margin=0.22,
                   equity=40000, total_assets=100000, debt=30000, cash=5000,
                   interest_rate=0.04, current_debt_pct=0.3, shares=5000,
                   revenue_growth=0.02, growth_volatility=0.02, margin_drift=0.0, debt_growth=0.01,
                   recurring_pct=0.90, top_customer_pct=0.02, sole_source_pct=0.05, china_revenue_pct=0.0,
                   mgmt_ownership=0.5, ceo_pay_ratio=350, narrative_fulfillment=0.65,
                   litigation_count=2, rpt_amount_pct=0.005, ops_issue_count=4,
                   equity_issuance_pct=0.0, debt_issuance_pct=0.1),

    CompanyProfile("ChinaTechGlobal", "CNTG", "中概地域风险",
                   revenue=35000, cogs_pct=0.45, opex_pct=0.25, tax_rate=0.15,
                   capex_pct=0.08, da_pct=0.05, ocf_margin=0.20,
                   equity=25000, total_assets=50000, debt=8000, cash=12000,
                   interest_rate=0.04, current_debt_pct=0.3, shares=2000,
                   revenue_growth=0.20, growth_volatility=0.10, margin_drift=0.0, debt_growth=0.05,
                   recurring_pct=0.60, top_customer_pct=0.06, sole_source_pct=0.10, china_revenue_pct=0.70,
                   mgmt_ownership=20.0, ceo_pay_ratio=60, narrative_fulfillment=0.50,
                   litigation_count=1, rpt_amount_pct=0.03, ops_issue_count=5,
                   equity_issuance_pct=0.0, debt_issuance_pct=0.1),

    CompanyProfile("Zaibatsu Holdings", "ZBTS", "日本财阀交叉持股",
                   revenue=90000, cogs_pct=0.70, opex_pct=0.15, tax_rate=0.30,
                   capex_pct=0.05, da_pct=0.04, ocf_margin=0.10,
                   equity=50000, total_assets=120000, debt=25000, cash=15000,
                   interest_rate=0.02, current_debt_pct=0.2, shares=10000,
                   revenue_growth=0.01, growth_volatility=0.03, margin_drift=0.0, debt_growth=0.01,
                   recurring_pct=0.30, top_customer_pct=0.04, sole_source_pct=0.0, china_revenue_pct=0.15,
                   mgmt_ownership=0.2, ceo_pay_ratio=30, narrative_fulfillment=0.70,
                   litigation_count=1, rpt_amount_pct=0.04, ops_issue_count=3,
                   equity_issuance_pct=0.0, debt_issuance_pct=0.0),

    CompanyProfile("CryptoExchange", "CREX", "加密交易所",
                   revenue=5000, cogs_pct=0.15, opex_pct=0.40, tax_rate=0.10,
                   capex_pct=0.05, da_pct=0.02, ocf_margin=0.35,
                   equity=3000, total_assets=8000, debt=500, cash=4000,
                   interest_rate=0.08, current_debt_pct=0.5, shares=200,
                   revenue_growth=0.50, growth_volatility=0.40, margin_drift=0.0, debt_growth=0.20,
                   recurring_pct=0.20, top_customer_pct=0.01, sole_source_pct=0.0, china_revenue_pct=0.05,
                   mgmt_ownership=40.0, ceo_pay_ratio=150, narrative_fulfillment=0.30,
                   litigation_count=3, rpt_amount_pct=0.05, ops_issue_count=8,
                   equity_issuance_pct=1.0, debt_issuance_pct=0.5),

    CompanyProfile("ChipCycle Semiconductor", "CHIP", "半导体周期",
                   revenue=25000, cogs_pct=0.45, opex_pct=0.20, tax_rate=0.15,
                   capex_pct=0.20, da_pct=0.12, ocf_margin=0.25,
                   equity=30000, total_assets=55000, debt=8000, cash=10000,
                   interest_rate=0.035, current_debt_pct=0.2, shares=1500,
                   revenue_growth=0.12, growth_volatility=0.30, margin_drift=0.0, debt_growth=0.05,
                   recurring_pct=0.15, top_customer_pct=0.18, sole_source_pct=0.20, china_revenue_pct=0.30,
                   mgmt_ownership=5.0, ceo_pay_ratio=200, narrative_fulfillment=0.60,
                   litigation_count=2, rpt_amount_pct=0.01, ops_issue_count=5,
                   equity_issuance_pct=0.0, debt_issuance_pct=0.2),

    CompanyProfile("AcquiGrowth Inc", "ACQG", "并购驱动",
                   revenue=15000, cogs_pct=0.50, opex_pct=0.20, tax_rate=0.20,
                   capex_pct=0.03, da_pct=0.02, ocf_margin=0.20,
                   equity=8000, total_assets=40000, debt=20000, cash=3000,
                   interest_rate=0.055, current_debt_pct=0.35, shares=1000,
                   revenue_growth=0.25, growth_volatility=0.10, margin_drift=-0.005, debt_growth=0.20,
                   recurring_pct=0.40, top_customer_pct=0.10, sole_source_pct=0.05, china_revenue_pct=0.05,
                   mgmt_ownership=8.0, ceo_pay_ratio=180, narrative_fulfillment=0.55,
                   litigation_count=2, rpt_amount_pct=0.02, ops_issue_count=6,
                   equity_issuance_pct=0.5, debt_issuance_pct=1.5),

    CompanyProfile("FamilyLegacy Corp", "FMLY", "家族企业",
                   revenue=8000, cogs_pct=0.40, opex_pct=0.25, tax_rate=0.20,
                   capex_pct=0.04, da_pct=0.03, ocf_margin=0.22,
                   equity=12000, total_assets=18000, debt=2000, cash=4000,
                   interest_rate=0.035, current_debt_pct=0.15, shares=400,
                   revenue_growth=0.05, growth_volatility=0.03, margin_drift=0.001, debt_growth=0.0,
                   recurring_pct=0.50, top_customer_pct=0.08, sole_source_pct=0.05, china_revenue_pct=0.02,
                   mgmt_ownership=55.0, ceo_pay_ratio=40, narrative_fulfillment=0.85,
                   litigation_count=0, rpt_amount_pct=0.06, ops_issue_count=2,
                   equity_issuance_pct=0.0, debt_issuance_pct=0.0),
]


# ── 从 Profile 生成 ComputeContext ─────────────────────────────────

def build_context(p: CompanyProfile, cid: int) -> ComputeContext:
    """从公司原型生成 8 期模拟数据的 ComputeContext。"""
    random.seed(hash(p.ticker))

    # 生成 8 期财务数据
    all_fli_frames = []
    all_debt_frames = []
    rev = p.revenue
    debt = p.debt
    equity = p.equity
    assets = p.total_assets
    cash = p.cash
    shares = p.shares
    goodwill = assets * 0.05
    ar = rev * 0.12
    inv = rev * 0.08

    for period in PERIODS:
        noise = 1 + random.gauss(0, p.growth_volatility)
        growth = p.revenue_growth * noise
        rev = rev * (1 + growth)
        debt = debt * (1 + p.debt_growth)
        equity = equity * (1 + growth * 0.5)
        assets = equity + debt
        cash = cash * (1 + growth * 0.3)
        shares = shares * (1 + random.gauss(-0.01, 0.02))  # slight buyback drift
        goodwill = goodwill * (1 + p.debt_growth * 0.5)
        ar = rev * (0.12 + random.gauss(0, 0.02))
        inv = rev * (0.08 + random.gauss(0, 0.01))

        cogs = rev * (p.cogs_pct + p.margin_drift * (PERIODS.index(period) - 4))
        opex = rev * p.opex_pct
        op_income = rev - cogs - opex
        interest = debt * p.interest_rate
        ni = (op_income - interest) * (1 - p.tax_rate)
        ocf = rev * p.ocf_margin
        capex = rev * p.capex_pct
        da = rev * p.da_pct

        items = {
            "revenue": rev, "cost_of_revenue": cogs,
            "operating_income": op_income, "net_income": ni,
            "operating_cash_flow": ocf, "capital_expenditures": capex,
            "depreciation_amortization": da,
            "shareholders_equity": equity, "total_assets": assets,
            "interest_expense": interest,
            "current_assets": cash + ar + inv,
            "current_liabilities": assets * 0.15,
            "goodwill": goodwill,
            "accounts_receivable": ar, "inventory": inv,
            "cash_and_equivalents": cash, "total_debt": debt,
            "dividends_paid": -ni * 0.3 if ni > 0 else 0,
            "share_repurchase": -ni * 0.2 if ni > 0 else 0,
            "sga_expense": opex * 0.6, "rnd_expense": opex * 0.4,
            "basic_weighted_average_shares": shares,
            "income_tax_expense_total": max(0, (op_income - interest) * p.tax_rate),
            "income_before_tax_total": op_income - interest,
            "proceeds_from_stock_issuance": capex * p.equity_issuance_pct,
            "proceeds_from_debt_issuance": capex * p.debt_issuance_pct,
        }
        all_fli_frames.append(_fli(items, period))

        current_debt_amt = debt * p.current_debt_pct
        long_debt_amt = debt * (1 - p.current_debt_pct)
        all_debt_frames.append(_debt([
            {"principal": long_debt_amt, "is_current": False, "interest_rate": p.interest_rate, "period": period},
            {"principal": current_debt_amt, "is_current": True, "interest_rate": p.interest_rate + 0.01, "period": period},
        ]))

    all_fli = pd.concat(all_fli_frames, ignore_index=True)
    all_debt = pd.concat(all_debt_frames, ignore_index=True)
    current_fli = all_fli[all_fli["period"] == "FY2025"]
    current_debt = all_debt[all_debt["period"] == "FY2025"]

    n_cust = max(3, int(1 / max(p.top_customer_pct, 0.01)))
    customers = [
        {"customer_name": f"Customer_{j}", "revenue_pct": p.top_customer_pct / (j + 1),
         "is_recurring": random.random() < p.recurring_pct, "revenue_type": random.choice(["license", "subscription", "project"])}
        for j in range(min(n_cust, 8))
    ]

    n_sup = random.randint(3, 6)
    suppliers = [
        {"supplier_name": f"Supplier_{j}", "is_sole_source": random.random() < p.sole_source_pct,
         "geographic_location": random.choice(["US", "China", "Taiwan", "Germany", "Japan"])}
        for j in range(n_sup)
    ]

    china_share = p.china_revenue_pct
    regions = [
        {"region": "United States", "revenue_share": max(0, 1 - china_share - 0.20)},
        {"region": "China", "revenue_share": china_share},
        {"region": "Europe", "revenue_share": 0.12},
        {"region": "Other", "revenue_share": 0.08},
    ]

    narratives = [{"narrative": f"Plan {j}", "status": random.choice(
        ["delivered"] * int(p.narrative_fulfillment * 10) + ["missed"] * int((1 - p.narrative_fulfillment) * 10)
    )} for j in range(random.randint(4, 10))]

    litigations = [
        {"status": random.choice(["pending", "ongoing"]), "accrued_amount": random.uniform(50, 500),
         "claimed_amount": random.uniform(200, 2000)}
        for _ in range(p.litigation_count)
    ]

    ops_issues = [
        {"topic": f"Issue {j}", "risk": random.choice(["high", "moderate", None]),
         "guidance": random.choice(["improving", "stable", None])}
        for j in range(p.ops_issue_count)
    ]

    rpt_txns = []
    if p.rpt_amount_pct > 0:
        rpt_txns = [{"related_party": "Related Co", "relationship": "director",
                      "transaction_type": "consulting", "amount": rev * p.rpt_amount_pct,
                      "is_ongoing": True}]

    ctx = ComputeContext(company_id=cid, period="FY2025")
    ctx._cache = {
        "financial_line_items": current_fli,
        "downstream_segments": _downstream(customers),
        "upstream_segments": _upstream(suppliers),
        "geographic_revenues": _geo(regions),
        "debt_obligations": current_debt,
        "executive_compensations": _exec_comp([
            {"name": "CEO", "title": "CEO", "role_type": "CEO",
             "pay_ratio": p.ceo_pay_ratio, "stock_awards": 5000, "total_comp": 8000},
        ]),
        "stock_ownership": _ownership([
            {"name": "CEO", "title": "CEO", "percent_of_class": p.mgmt_ownership},
            {"name": "Fund A", "title": None, "percent_of_class": 8.0},
        ]),
        "company_narratives": _narratives(narratives),
        "litigations": _litigations(litigations),
        "operational_issues": _ops_issues(ops_issues),
        "related_party_transactions": _rpt(rpt_txns),
        "non_financial_kpis": EMPTY,
        "financial_line_items_all": all_fli,
        "debt_obligations_all": all_debt,
        "pricing_actions": EMPTY, "market_share_data": EMPTY,
        "audit_opinions": EMPTY, "known_issues": EMPTY,
        "insider_transactions": EMPTY, "executive_changes": EMPTY,
        "equity_offerings": EMPTY, "analyst_estimates": EMPTY,
        "management_guidance": EMPTY,
    }
    return ctx


def compute_all_features(ctx: ComputeContext) -> dict[str, FeatureResult]:
    results = {}
    for level in (FeatureLevel.L0, FeatureLevel.L1, FeatureLevel.L2):
        for feat in get_features(level=level):
            try:
                result = feat.compute_fn(ctx)
            except Exception:
                continue
            if result is not None:
                ctx.features[feat.name] = result.value
                results[feat.name] = result
    return results


# ── 主入口 ────────────────────────────────────────────────────────

def main():
    print("=" * 80)
    print(f"  AXION 大规模 Mock 压力测试 — {len(COMPANY_PROFILES)} 家公司 × 8 期")
    print("=" * 80)
    print()

    all_results = []
    total_features = 0
    total_rules_fired = 0
    errors = []

    t0 = time.time()

    for i, profile in enumerate(COMPANY_PROFILES):
        ctx = build_context(profile, cid=i + 1)
        feature_results = compute_all_features(ctx)
        features = {n: r.value for n, r in feature_results.items()}
        total_features += len(features)

        try:
            score = score_company(i + 1, profile.name, profile.ticker, "FY2025", features)
        except Exception as e:
            errors.append((profile.ticker, str(e)))
            continue

        b = score.buffett.school_score if score.buffett else None
        d = score.dalio.school_score if score.dalio else None
        s = score.soros.school_score if score.soros else None

        rules_fired = sum(len(ss.drivers) for ss in [b, d, s] if ss)
        total_rules_fired += rules_fired

        all_results.append({
            "ticker": profile.ticker,
            "archetype": profile.archetype,
            "features": len(features),
            "buffett": f"{b.score:.1f}" if b else "N/A",
            "b_signal": b.signal if b else "N/A",
            "dalio": f"{d.score:.1f}" if d else "N/A",
            "d_signal": d.signal if d else "N/A",
            "soros": f"{s.score:.1f}" if s else "N/A",
            "s_signal": s.signal if s else "N/A",
            "rules_fired": rules_fired,
            "b_filters": "通过" if score.buffett and score.buffett.filters_passed else "未通过",
            "reflexivity": score.soros.reflexivity_phase if score.soros else "N/A",
        })

    elapsed = time.time() - t0

    # ── 汇总表 ──
    print(f"{'Ticker':<6} {'类型':<16} {'特征':>4} {'巴菲特':>8} {'信号':<8} {'达利欧':>8} {'信号':<6} {'索罗斯':>8} {'信号':<6} {'规则':>4} {'过滤':<6}")
    print("-" * 110)
    for r in all_results:
        print(f"{r['ticker']:<6} {r['archetype']:<16} {r['features']:>4} "
              f"{r['buffett']:>8} {r['b_signal']:<8} "
              f"{r['dalio']:>8} {r['d_signal']:<6} "
              f"{r['soros']:>8} {r['s_signal']:<6} "
              f"{r['rules_fired']:>4} {r['b_filters']:<6}")

    # ── 统计 ──
    print()
    print("=" * 80)
    print(f"  公司数: {len(COMPANY_PROFILES)}")
    print(f"  总特征计算: {total_features} ({total_features / len(COMPANY_PROFILES):.0f} avg/company)")
    print(f"  总规则触发: {total_rules_fired} ({total_rules_fired / len(COMPANY_PROFILES):.0f} avg/company)")
    print(f"  错误: {len(errors)}")
    if errors:
        for ticker, err in errors:
            print(f"    {ticker}: {err}")
    print(f"  耗时: {elapsed:.2f}s ({elapsed / len(COMPANY_PROFILES) * 1000:.0f}ms/company)")
    print("=" * 80)

    # ── 排行榜 ──
    print()
    print("  巴菲特 Top 5:")
    sorted_b = sorted(all_results, key=lambda x: float(x["buffett"]) if x["buffett"] != "N/A" else 0, reverse=True)
    for r in sorted_b[:5]:
        print(f"    {r['ticker']:<6} {r['buffett']:>5} [{r['b_signal']}]  — {r['archetype']}")

    print()
    print("  达利欧 最脆弱 5 家:")
    sorted_d = sorted(all_results, key=lambda x: float(x["dalio"]) if x["dalio"] != "N/A" else 10)
    for r in sorted_d[:5]:
        print(f"    {r['ticker']:<6} {r['dalio']:>5} [{r['d_signal']}]  — {r['archetype']}")

    print()
    print("  索罗斯 最高风险 5 家:")
    sorted_s = sorted(all_results, key=lambda x: float(x["soros"]) if x["soros"] != "N/A" else 10)
    for r in sorted_s[:5]:
        print(f"    {r['ticker']:<6} {r['soros']:>5} [{r['s_signal']}]  — {r['archetype']}")

    # ── 打印 2 家详细报告 ──
    print()
    print("=" * 80)
    print("  详细报告样例")
    print("=" * 80)

    for ticker in ["LUXE", "PROP"]:
        profile = next(p for p in COMPANY_PROFILES if p.ticker == ticker)
        idx = COMPANY_PROFILES.index(profile)
        ctx = build_context(profile, cid=idx + 1)
        fr = compute_all_features(ctx)
        features = {n: r.value for n, r in fr.items()}
        score = score_company(idx + 1, profile.name, profile.ticker, "FY2025", features)
        print(format_report(score))

    # ── 断言检查 ──
    print()
    print("  断言检查...")
    for r in all_results:
        b = float(r["buffett"]) if r["buffett"] != "N/A" else 5.0
        d = float(r["dalio"]) if r["dalio"] != "N/A" else 5.0
        s = float(r["soros"]) if r["soros"] != "N/A" else 5.0
        assert 1.0 <= b <= 10.0, f"{r['ticker']} buffett={b} out of range"
        assert 1.0 <= d <= 10.0, f"{r['ticker']} dalio={d} out of range"
        assert 1.0 <= s <= 10.0, f"{r['ticker']} soros={s} out of range"
        assert r["features"] > 0, f"{r['ticker']} has 0 features"

    # 高毛利公司应在巴菲特得分较高
    luxe = next(r for r in all_results if r["ticker"] == "LUXE")
    prop = next(r for r in all_results if r["ticker"] == "PROP")
    assert float(luxe["buffett"]) > float(prop["buffett"]), "LUXE should beat PROP on Buffett"

    # 地产高杠杆在达利欧应更脆弱
    assert float(prop["dalio"]) < float(luxe["dalio"]), "PROP should be more fragile than LUXE on Dalio"

    print("  全部通过!")


if __name__ == "__main__":
    main()
