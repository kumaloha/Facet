"""
可预测性检测 · 全场景验证
"""

import pandas as pd

import polaris.features.l0.company  # noqa: F401
import polaris.features.l0.cross_period  # noqa: F401

from polaris.features.types import ComputeContext, FeatureLevel
from polaris.features.registry import get_features
from polaris.chains.predictability import assess_predictability, format_predictability

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

DS = {"company_id": 1, "period": "FY2025", "segment": None,
      "customer_type": None, "products": None, "channels": None,
      "revenue": None, "growth_yoy": None, "backlog": None,
      "backlog_note": None, "pricing_model": None, "contract_duration": None,
      "recognition_method": None, "description": None,
      "raw_post_id": None, "created_at": "2025-01-01"}


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


HEALTHY_FLI = {"revenue": 50000, "cost_of_revenue": 20000, "operating_income": 18000,
    "net_income": 14000, "operating_cash_flow": 17000, "capital_expenditures": 2000,
    "depreciation_amortization": 1500, "shareholders_equity": 60000, "total_assets": 80000,
    "interest_expense": 200, "current_assets": 30000, "current_liabilities": 10000,
    "goodwill": 1000, "accounts_receivable": 3000, "inventory": 2000,
    "cash_and_equivalents": 20000, "total_debt": 3000, "dividends_paid": -5000,
    "share_repurchase": -3000, "sga_expense": 4000, "rnd_expense": 3000,
    "basic_weighted_average_shares": 1000, "income_tax_expense_total": 4000,
    "income_before_tax_total": 18000, "proceeds_from_stock_issuance": 0,
    "proceeds_from_debt_issuance": 0}

SICK_FLI = {"revenue": 10000, "cost_of_revenue": 7000, "operating_income": 1500,
    "net_income": 300, "operating_cash_flow": 1000, "capital_expenditures": 500,
    "depreciation_amortization": 400, "shareholders_equity": 3000, "total_assets": 25000,
    "interest_expense": 1500, "current_assets": 5000, "current_liabilities": 8000,
    "goodwill": 2000, "accounts_receivable": 2000, "inventory": 3000,
    "cash_and_equivalents": 1000, "total_debt": 18000, "dividends_paid": 0,
    "share_repurchase": 0, "sga_expense": 1500, "rnd_expense": 200,
    "basic_weighted_average_shares": 500, "income_tax_expense_total": 100,
    "income_before_tax_total": 400, "proceeds_from_stock_issuance": 0,
    "proceeds_from_debt_issuance": 3000}


def run(name, desc, fli, moat_depth, downstream=None):
    print(f"\n{'─' * 65}")
    print(f"  {name}")
    print(f"  {desc}")
    print(f"{'─' * 65}")
    ctx = ComputeContext(company_id=1, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli),
        "financial_line_items_all": _fli(fli),
        "downstream_segments": downstream if downstream is not None else EMPTY,
        **{k: v for k, v in EMPTY_TABLES.items() if k != "downstream_segments"},
    }
    for level in (FeatureLevel.L0, FeatureLevel.L1, FeatureLevel.L2):
        for feat in get_features(level=level):
            try:
                r = feat.compute_fn(ctx)
            except Exception:
                continue
            if r is not None:
                ctx.features[feat.name] = r.value
    result = assess_predictability(ctx, moat_depth=moat_depth)
    print(format_predictability(result))


if __name__ == "__main__":
    print("=" * 65)
    print("  可预测性检测 · 全场景验证")
    print("=" * 65)

    # Case 1: 茅台 — 成瘾品 + 护城河极深
    run("Case 1: 类茅台", "成瘾品 + 护城河极深 → 可预测",
        HEALTHY_FLI, "extreme",
        _df([{"customer_name": "消费者", "revenue_pct": 1.0,
              "is_recurring": True, "revenue_type": "recurring",
              "product_category": "liquor"}], DS))

    # Case 2: 电力公司 — 基础设施 + 护城河深
    run("Case 2: 电力公司", "基础设施 + 护城河深 → 可预测",
        HEALTHY_FLI, "deep",
        _df([{"customer_name": "居民用户", "revenue_pct": 0.6,
              "is_recurring": True, "revenue_type": "utility",
              "product_category": "electricity"},
             {"customer_name": "工业用户", "revenue_pct": 0.4,
              "is_recurring": True, "revenue_type": "utility",
              "product_category": "electricity"}], DS))

    # Case 3: 可口可乐 — 必需品(饮料) + 成瘾品(咖啡因)
    run("Case 3: 类可口可乐", "饮料/咖啡因 + 护城河极深 → 可预测",
        HEALTHY_FLI, "extreme",
        _df([{"customer_name": "全球消费者", "revenue_pct": 1.0,
              "is_recurring": True, "revenue_type": "recurring",
              "product_category": "beverage"}], DS))

    # Case 4: 时尚品牌 — 潮流，不可预测
    run("Case 4: 时尚品牌", "潮流型 + 护城河浅 → 不可预测",
        HEALTHY_FLI, "shallow",
        _df([{"customer_name": "消费者", "revenue_pct": 1.0,
              "is_recurring": False, "revenue_type": "retail",
              "product_category": "fashion"}], DS))

    # Case 5: 高债务公司 — 财务有病，可预测会变差
    run("Case 5: 高债务公司", "D/E 6.0 + 利息覆盖率差 → 可预测（会恶化）",
        SICK_FLI, "deep",
        _df([{"customer_name": "客户", "revenue_pct": 1.0,
              "is_recurring": True, "revenue_type": "recurring",
              "product_category": "food"}], DS))

    # Case 6: 无护城河 — 直接不可预测
    run("Case 6: 无护城河", "护城河 = none → 不可预测",
        HEALTHY_FLI, "none",
        _df([{"customer_name": "客户", "revenue_pct": 1.0,
              "is_recurring": True, "revenue_type": "recurring",
              "product_category": "food"}], DS))

    # Case 7: 强护城河但无业务分类数据
    run("Case 7: 强护城河无分类", "护城河极深但没有业务类型标签 → 存疑",
        HEALTHY_FLI, "extreme")

    # Case 8: SaaS 公司 — 有护城河但不是必需/成瘾/基础设施
    run("Case 8: SaaS 公司", "订阅模式但不是必需品类 → 不可预测?",
        HEALTHY_FLI, "deep",
        _df([{"customer_name": "企业客户", "revenue_pct": 1.0,
              "is_recurring": True, "revenue_type": "saas",
              "product_category": "software"}], DS))

    print("\n" + "=" * 65)
    print("  验证完成")
    print("=" * 65)
