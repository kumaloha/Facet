"""
护城河决策树 · 全路径验证
=========================
每个二级分类至少一个 mock case 验证。
"""

import pandas as pd

import polaris.features.l0.company  # noqa: F401
import polaris.features.l0.cross_period  # noqa: F401

from polaris.features.types import ComputeContext, FeatureLevel
from polaris.features.registry import get_features
from polaris.chains.moat import assess_moat, format_moat

EMPTY = pd.DataFrame()

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
    "competitive_dynamics": EMPTY, "peer_financials": EMPTY,
}


def _fli(items, period="FY2025"):
    return pd.DataFrame([
        {"id": i, "statement_id": 1, "item_key": k, "item_label": k,
         "value": v, "parent_key": None, "ordinal": i, "note": None, "period": period}
        for i, (k, v) in enumerate(items.items())
    ])

def _df(rows, defaults):
    if not rows: return EMPTY
    return pd.DataFrame([{**defaults, "id": i, **r} for i, r in enumerate(rows)])

# 通用财务数据（健康公司）
GOOD_FLI = {
    "revenue": 50_000, "cost_of_revenue": 15_000,
    "operating_income": 20_000, "net_income": 16_000,
    "operating_cash_flow": 20_000, "capital_expenditures": 3_000,
    "depreciation_amortization": 2_000,
    "shareholders_equity": 60_000, "total_assets": 90_000,
    "interest_expense": 200, "current_assets": 35_000,
    "current_liabilities": 12_000, "goodwill": 2_000,
    "accounts_receivable": 4_000, "inventory": 3_000,
    "cash_and_equivalents": 20_000, "total_debt": 5_000,
    "dividends_paid": -5_000, "share_repurchase": -3_000,
    "sga_expense": 5_000, "rnd_expense": 5_000,
    "basic_weighted_average_shares": 1_000,
    "income_tax_expense_total": 4_000, "income_before_tax_total": 20_000,
    "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
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


def run(name, ctx):
    print(f"\n{'─' * 60}")
    print(f"  {name}")
    print(f"{'─' * 60}")
    compute(ctx)
    result = assess_moat(ctx)
    print(format_moat(result))


# ══════════════════════════════════════════════════════════════
#  无形资产
# ══════════════════════════════════════════════════════════════

def test_brand_trust():
    """品牌·信任默选: 有正面社交信号"""
    ctx = ComputeContext(company_id=1, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(GOOD_FLI), "financial_line_items_all": _fli(GOOD_FLI),
        "downstream_segments": EMPTY, "pricing_actions": EMPTY, "market_share_data": EMPTY,
        "brand_signals": _df([
            {"signal_type": "viral_praise", "platform": "xiaohongshu",
             "description": "大量用户自发晒产品", "sentiment_score": 0.8, "reach_estimate": 500_000},
            {"signal_type": "viral_praise", "platform": "weibo",
             "description": "品牌话题自然登上热搜", "sentiment_score": 0.7, "reach_estimate": 2_000_000},
            {"signal_type": "organic_mention", "platform": "douyin",
             "description": "开箱视频大量自然流量", "sentiment_score": 0.6, "reach_estimate": 300_000},
        ], {"company_id": 1, "event_date": "2025-01", "raw_post_id": None, "created_at": "2025-01-01"}),
        **{k: v for k, v in EMPTY_TABLES.items() if k not in ("brand_signals",)},
    }
    run("无形资产·品牌信任默选 (正面社交信号)", ctx)


def test_know_how():
    """商业秘密/know-how: 竞品尝试复制但失败"""
    ctx = ComputeContext(company_id=1, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(GOOD_FLI), "financial_line_items_all": _fli(GOOD_FLI),
        "downstream_segments": EMPTY, "pricing_actions": EMPTY, "market_share_data": EMPTY,
        "competitive_dynamics": _df([
            {"competitor_name": "竞对A", "event_type": "product_launch",
             "event_description": "竞对A 投入 50 亿试图复制核心工艺",
             "outcome_description": "良率远低于目标，无法量产，失败"},
            {"competitor_name": "竞对B", "event_type": "talent_poaching",
             "event_description": "竞对B 挖走 5 名核心工程师",
             "outcome_description": "仍无法复制核心技术，良率差距明显"},
        ], CD),
        **{k: v for k, v in EMPTY_TABLES.items() if k not in ("competitive_dynamics",)},
    }
    run("无形资产·know-how (竞品复制失败)", ctx)


# ══════════════════════════════════════════════════════════════
#  转换成本
# ══════════════════════════════════════════════════════════════

def test_switching_behavioral():
    """转换成本·系统嵌入: 客户评估替代品但留下"""
    ctx = ComputeContext(company_id=1, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(GOOD_FLI), "financial_line_items_all": _fli(GOOD_FLI),
        "downstream_segments": _df([
            {"customer_name": "大客户A", "revenue_pct": 0.30, "is_recurring": True,
             "revenue_type": "saas", "switching_cost_level": "high"},
        ], DS),
        "pricing_actions": EMPTY, "market_share_data": EMPTY,
        "competitive_dynamics": _df([
            {"competitor_name": "新产品X", "event_type": "product_launch",
             "event_description": "大客户A 评估了替代方案 X",
             "outcome_description": "最终仍留在原平台，迁移成本太高"},
        ], CD),
        **{k: v for k, v in EMPTY_TABLES.items() if k not in ("competitive_dynamics",)},
    }
    run("转换成本·系统嵌入 (客户评估替代品但留下)", ctx)


def test_data_migration_weakening():
    """数据迁移壁垒削弱: 竞品推出迁移工具"""
    ctx = ComputeContext(company_id=1, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(GOOD_FLI), "financial_line_items_all": _fli(GOOD_FLI),
        "downstream_segments": _df([
            {"customer_name": "用户群", "revenue_pct": 1.0, "is_recurring": True,
             "revenue_type": "saas"},
        ], DS),
        "pricing_actions": EMPTY, "market_share_data": EMPTY,
        "competitive_dynamics": _df([
            {"competitor_name": "竞对C", "event_type": "migration_tool",
             "event_description": "竞对C 发布一键数据迁移工具",
             "outcome_description": "部分客户开始迁移"},
        ], CD),
        **{k: v for k, v in EMPTY_TABLES.items() if k not in ("competitive_dynamics",)},
    }
    run("转换成本·数据迁移壁垒削弱 (竞品推出迁移工具)", ctx)


def test_risk_asymmetry():
    """风险不对称: 产品便宜但出事代价大"""
    ctx = ComputeContext(company_id=1, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(GOOD_FLI), "financial_line_items_all": _fli(GOOD_FLI),
        "downstream_segments": _df([
            {"customer_name": "航空公司A", "revenue_pct": 0.15, "is_recurring": True,
             "revenue_type": "recurring", "product_criticality": "high", "cost_share_pct": 0.01},
            {"customer_name": "航空公司B", "revenue_pct": 0.12, "is_recurring": True,
             "revenue_type": "recurring", "product_criticality": "high", "cost_share_pct": 0.02},
        ], DS),
        "pricing_actions": EMPTY, "market_share_data": EMPTY,
        **EMPTY_TABLES,
    }
    run("转换成本·风险不对称 (航空零件: 便宜但出事致命)", ctx)


# ══════════════════════════════════════════════════════════════
#  成本优势
# ══════════════════════════════════════════════════════════════

def test_counter_positioning():
    """反定位: 对手抄了会自毁"""
    ctx = ComputeContext(company_id=1, period="FY2025")
    # Vanguard 类低费率指数基金
    vanguard_fli = dict(GOOD_FLI)
    vanguard_fli["cost_of_revenue"] = 45_000  # 低毛利
    vanguard_fli["revenue"] = 50_000
    ctx._cache = {
        "financial_line_items": _fli(vanguard_fli), "financial_line_items_all": _fli(vanguard_fli),
        "downstream_segments": _df([
            {"customer_name": "被动基金投资者", "revenue_pct": 1.0, "is_recurring": True,
             "revenue_type": "subscription"},
        ], DS),
        "pricing_actions": EMPTY, "market_share_data": EMPTY,
        "competitive_dynamics": _df([
            {"competitor_name": "传统主动基金公司", "event_type": "product_launch",
             "event_description": "传统基金公司被迫推出低费率指数产品",
             "outcome_description": "低费率产品蚕食自家高费率产品收入，自毁利润结构"},
        ], CD),
        **{k: v for k, v in EMPTY_TABLES.items() if k not in ("competitive_dynamics",)},
    }
    run("成本优势·反定位 (Vanguard 类: 对手抄了自毁)", ctx)


# ══════════════════════════════════════════════════════════════
#  有效规模
# ══════════════════════════════════════════════════════════════

def test_natural_monopoly():
    """自然垄断: 新进入者亏损退出"""
    ctx = ComputeContext(company_id=1, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(GOOD_FLI), "financial_line_items_all": _fli(GOOD_FLI),
        "downstream_segments": EMPTY, "pricing_actions": EMPTY,
        "market_share_data": _df([
            {"period": "FY2023", "share": 0.85, "source": "区域市场"},
            {"period": "FY2024", "share": 0.87, "source": "区域市场"},
            {"period": "FY2025", "share": 0.88, "source": "区域市场"},
        ], MS),
        "competitive_dynamics": _df([
            {"competitor_name": "新进入者X", "event_type": "new_entry",
             "event_description": "X 公司尝试进入本区域市场",
             "outcome_description": "运营 2 年后持续亏损，宣布退出"},
            {"competitor_name": "新进入者Y", "event_type": "new_entry",
             "event_description": "Y 公司获得牌照进入",
             "outcome_description": "客户不足，亏损退出，失败"},
        ], CD),
        **{k: v for k, v in EMPTY_TABLES.items() if k not in ("competitive_dynamics",)},
    }
    run("有效规模·自然垄断 (新进入者全部亏损退出)", ctx)


# ══════════════════════════════════════════════════════════════
#  伪护城河
# ══════════════════════════════════════════════════════════════

def test_anti_moat():
    """伪护城河: 烧钱买份额"""
    bad_fli = {
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
    ctx = ComputeContext(company_id=99, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(bad_fli), "financial_line_items_all": _fli(bad_fli),
        "downstream_segments": EMPTY, "pricing_actions": EMPTY, "market_share_data": EMPTY,
        **EMPTY_TABLES,
    }
    run("伪护城河 (烧钱买份额)", ctx)


if __name__ == "__main__":
    print("=" * 60)
    print("  护城河决策树 · 全路径验证")
    print("=" * 60)

    # 无形资产
    test_brand_trust()
    test_know_how()

    # 转换成本
    test_switching_behavioral()
    test_data_migration_weakening()
    test_risk_asymmetry()

    # 成本优势
    test_counter_positioning()

    # 有效规模
    test_natural_monopoly()

    # 伪护城河
    test_anti_moat()

    print("\n" + "=" * 60)
    print("  全路径验证完成")
    print("=" * 60)
