"""
护城河检测 · 真实公司验证
=========================
用近似真实数据验证 4 家公司:
  腾讯 — 网络效应 (微信)
  抖音 — 弱网络效应 (竞品活得不错)
  Costco — 成本优势 (效率型低毛利)
  紫金矿业 — 无形资产(矿权) + 成本优势
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

DS = {"company_id": 1, "period": "FY2025", "segment": None,
      "customer_type": None, "products": None, "channels": None,
      "revenue": None, "growth_yoy": None, "backlog": None,
      "backlog_note": None, "pricing_model": None, "contract_duration": None,
      "recognition_method": None, "description": None,
      "raw_post_id": None, "created_at": "2025-01-01"}

CD = {"company_id": 1, "estimated_investment": None,
      "outcome_market_share_change": None, "event_date": "2024-01",
      "raw_post_id": None, "created_at": "2025-01-01"}

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
    "management_acknowledgments": EMPTY, "brand_signals": EMPTY,
}


def compute(ctx):
    for level in (FeatureLevel.L0, FeatureLevel.L1, FeatureLevel.L2):
        for feat in get_features(level=level):
            try:
                r = feat.compute_fn(ctx)
            except Exception:
                continue
            if r is not None:
                ctx.features[feat.name] = r.value


def run(name, desc, ctx):
    print(f"\n{'▓' * 65}")
    print(f"  {name}")
    print(f"  {desc}")
    print(f"{'▓' * 65}")
    compute(ctx)
    result = assess_moat(ctx)
    print(format_moat(result))


# ══════════════════════════════════════════════════════════════
#  腾讯 — 网络效应
# ══════════════════════════════════════════════════════════════

def tencent():
    # 近似 FY2024 数据 (亿元人民币)
    fli = {
        "revenue": 6_257, "cost_of_revenue": 3_380,
        "operating_income": 1_807, "net_income": 1_577,
        "operating_cash_flow": 2_068, "capital_expenditures": 518,
        "depreciation_amortization": 600,
        "shareholders_equity": 8_500, "total_assets": 16_000,
        "interest_expense": 120, "current_assets": 5_000,
        "current_liabilities": 4_200, "goodwill": 800,
        "accounts_receivable": 500, "inventory": 50,
        "cash_and_equivalents": 2_500, "total_debt": 2_800,
        "dividends_paid": -300, "share_repurchase": -1_000,
        "sga_expense": 500, "rnd_expense": 600,
        "basic_weighted_average_shares": 9_400,  # 百万股
        "income_tax_expense_total": 350, "income_before_tax_total": 1_927,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
    }

    ctx = ComputeContext(company_id=1, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli),
        "financial_line_items_all": _fli(fli),
        "downstream_segments": _df([
            {"customer_name": "游戏", "revenue_pct": 0.32, "is_recurring": True, "revenue_type": "subscription"},
            {"customer_name": "社交网络", "revenue_pct": 0.25, "is_recurring": True, "revenue_type": "subscription"},
            {"customer_name": "广告", "revenue_pct": 0.20, "is_recurring": True, "revenue_type": "ad_revenue"},
            {"customer_name": "金融科技", "revenue_pct": 0.18, "is_recurring": True, "revenue_type": "transaction_fee"},
            {"customer_name": "云及其他", "revenue_pct": 0.05, "is_recurring": True, "revenue_type": "saas"},
        ], DS),
        "pricing_actions": EMPTY,
        "market_share_data": _df([
            {"period": "FY2023", "share": 0.95, "source": "即时通讯 MAU", "market_segment": "IM"},
            {"period": "FY2024", "share": 0.95, "source": "即时通讯 MAU", "market_segment": "IM"},
            {"period": "FY2025", "share": 0.94, "source": "即时通讯 MAU", "market_segment": "IM"},
        ], {"company_id": 1, "raw_post_id": None, "created_at": "2025-01-01"}),
        "competitive_dynamics": _df([
            {"competitor_name": "阿里·来往", "event_type": "product_launch",
             "event_description": "阿里投入 10 亿推广来往 IM 产品",
             "outcome_description": "来往于 2015 年关停，用户未达千万", "outcome_market_share_change": 0.0},
            {"competitor_name": "字节·飞聊", "event_type": "product_launch",
             "event_description": "字节跳动发布飞聊社交产品",
             "outcome_description": "飞聊于 2021 年关停，失败", "outcome_market_share_change": 0.0},
            {"competitor_name": "字节·多闪", "event_type": "product_launch",
             "event_description": "字节跳动发布多闪视频社交",
             "outcome_description": "多闪已关停", "outcome_market_share_change": 0.0},
            {"competitor_name": "子弹短信", "event_type": "product_launch",
             "event_description": "罗永浩子弹短信短暂爆火",
             "outcome_description": "快速退潮，月活归零，失败", "outcome_market_share_change": 0.0},
        ], CD),
        **EMPTY_TABLES,
    }
    run("腾讯 (Tencent)", "预期: 网络效应极强，竞品全部失败", ctx)


# ══════════════════════════════════════════════════════════════
#  抖音 — 弱网络效应
# ══════════════════════════════════════════════════════════════

def douyin():
    fli = {
        "revenue": 12_000, "cost_of_revenue": 6_000,
        "operating_income": 3_000, "net_income": 2_500,
        "operating_cash_flow": 3_500, "capital_expenditures": 1_500,
        "depreciation_amortization": 800,
        "shareholders_equity": 10_000, "total_assets": 20_000,
        "interest_expense": 100, "current_assets": 8_000,
        "current_liabilities": 5_000, "goodwill": 500,
        "accounts_receivable": 1_000, "inventory": 100,
        "cash_and_equivalents": 5_000, "total_debt": 2_000,
        "dividends_paid": 0, "share_repurchase": 0,
        "sga_expense": 2_000, "rnd_expense": 2_000,
        "basic_weighted_average_shares": 2_000,
        "income_tax_expense_total": 500, "income_before_tax_total": 3_000,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
    }

    ctx = ComputeContext(company_id=2, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli),
        "financial_line_items_all": _fli(fli),
        "downstream_segments": _df([
            {"customer_name": "广告主", "revenue_pct": 0.70, "is_recurring": True, "revenue_type": "ad_revenue"},
            {"customer_name": "直播电商", "revenue_pct": 0.20, "is_recurring": True, "revenue_type": "transaction_fee"},
            {"customer_name": "其他", "revenue_pct": 0.10, "is_recurring": False, "revenue_type": "other"},
        ], DS),
        "pricing_actions": EMPTY,
        "market_share_data": _df([
            {"period": "FY2023", "share": 0.45, "source": "短视频 DAU", "market_segment": "短视频"},
            {"period": "FY2024", "share": 0.43, "source": "短视频 DAU", "market_segment": "短视频"},
            {"period": "FY2025", "share": 0.41, "source": "短视频 DAU", "market_segment": "短视频"},
        ], {"company_id": 2, "raw_post_id": None, "created_at": "2025-01-01"}),
        "competitive_dynamics": _df([
            {"competitor_name": "腾讯·微视", "event_type": "product_launch",
             "event_description": "腾讯投入 30 亿补贴微视",
             "outcome_description": "微视未能突破，已关停，失败", "outcome_market_share_change": 0.0},
            {"competitor_name": "腾讯·视频号", "event_type": "product_launch",
             "event_description": "视频号嵌入微信生态，快速增长",
             "outcome_description": "视频号 DAU 超 4.5 亿，抢到一定份额",
             "outcome_market_share_change": -0.03},
            {"competitor_name": "快手", "event_type": "product_launch",
             "event_description": "快手持续投入，维持短视频第二位",
             "outcome_description": "快手 DAU 稳定在 3.8 亿",
             "outcome_market_share_change": -0.01},
            {"competitor_name": "小红书", "event_type": "new_entry",
             "event_description": "小红书短视频功能强化",
             "outcome_description": "小红书 DAU 快速增长至 1.2 亿",
             "outcome_market_share_change": -0.02},
        ], CD),
        **EMPTY_TABLES,
    }
    run("抖音 (Douyin)", "预期: 有护城河但不深，竞品活着且在蚕食", ctx)


# ══════════════════════════════════════════════════════════════
#  Costco — 成本优势
# ══════════════════════════════════════════════════════════════

def costco():
    # 近似 FY2024 (百万美元)
    fli = {
        "revenue": 254_000, "cost_of_revenue": 221_000,
        "operating_income": 8_900, "net_income": 7_400,
        "operating_cash_flow": 11_000, "capital_expenditures": 4_700,
        "depreciation_amortization": 2_300,
        "shareholders_equity": 17_000, "total_assets": 69_000,
        "interest_expense": 200, "current_assets": 32_000,
        "current_liabilities": 35_000, "goodwill": 900,
        "accounts_receivable": 2_200, "inventory": 18_000,
        "cash_and_equivalents": 13_000, "total_debt": 9_000,
        "dividends_paid": -4_200, "share_repurchase": -600,
        "sga_expense": 24_000, "rnd_expense": 0,
        "basic_weighted_average_shares": 443,
        "income_tax_expense_total": 2_400, "income_before_tax_total": 9_800,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
    }

    ctx = ComputeContext(company_id=3, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli),
        "financial_line_items_all": _fli(fli),
        "downstream_segments": _df([
            {"customer_name": "会员消费者", "revenue_pct": 0.85, "is_recurring": True,
             "revenue_type": "recurring", "contract_duration": "1 year"},
            {"customer_name": "会员费", "revenue_pct": 0.02, "is_recurring": True,
             "revenue_type": "subscription", "contract_duration": "1 year"},
            {"customer_name": "企业会员", "revenue_pct": 0.13, "is_recurring": True,
             "revenue_type": "recurring", "contract_duration": "1 year"},
        ], DS),
        "pricing_actions": EMPTY,
        "market_share_data": _df([
            {"period": "FY2023", "share": 0.06, "source": "美国零售份额", "market_segment": "零售"},
            {"period": "FY2024", "share": 0.065, "source": "美国零售份额", "market_segment": "零售"},
            {"period": "FY2025", "share": 0.07, "source": "美国零售份额", "market_segment": "零售"},
        ], {"company_id": 3, "raw_post_id": None, "created_at": "2025-01-01"}),
        "competitive_dynamics": EMPTY,
        **EMPTY_TABLES,
    }
    run("Costco", "预期: 成本优势(效率型低毛利) + 转换成本(会员制)", ctx)


# ══════════════════════════════════════════════════════════════
#  紫金矿业 — 特许经营权 + 成本优势
# ══════════════════════════════════════════════════════════════

def zijin():
    # 近似 FY2024 (亿元人民币)
    fli = {
        "revenue": 3_000, "cost_of_revenue": 2_400,
        "operating_income": 400, "net_income": 320,
        "operating_cash_flow": 500, "capital_expenditures": 300,
        "depreciation_amortization": 200,
        "shareholders_equity": 1_500, "total_assets": 4_000,
        "interest_expense": 80, "current_assets": 1_200,
        "current_liabilities": 1_000, "goodwill": 50,
        "accounts_receivable": 200, "inventory": 400,
        "cash_and_equivalents": 300, "total_debt": 1_200,
        "dividends_paid": -80, "share_repurchase": 0,
        "sga_expense": 100, "rnd_expense": 30,
        "basic_weighted_average_shares": 26_000,  # 百万股
        "income_tax_expense_total": 80, "income_before_tax_total": 400,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 200,
    }

    ctx = ComputeContext(company_id=4, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli),
        "financial_line_items_all": _fli(fli),
        "downstream_segments": _df([
            {"customer_name": "铜产品", "revenue_pct": 0.45, "is_recurring": False, "revenue_type": "commodity"},
            {"customer_name": "金产品", "revenue_pct": 0.35, "is_recurring": False, "revenue_type": "commodity"},
            {"customer_name": "锌及其他", "revenue_pct": 0.20, "is_recurring": False, "revenue_type": "commodity"},
        ], DS),
        "pricing_actions": EMPTY,
        "market_share_data": EMPTY,
        "competitive_dynamics": EMPTY,
        **EMPTY_TABLES,
    }
    run("紫金矿业 (Zijin)", "预期: 成本优势(低成本)，但缺行业对比数据", ctx)


if __name__ == "__main__":
    tencent()
    douyin()
    costco()
    zijin()
