"""
护城河检测 · 输入输出逐步展示
==============================
Case 1: BrandCorp (类茅台) — 完整数据，走完全流程
"""

import json
import pandas as pd

import polaris.features.l0.company  # noqa: F401
import polaris.features.l0.cross_period  # noqa: F401

from polaris.features.types import ComputeContext, FeatureLevel
from polaris.features.registry import get_features
from polaris.chains.moat import assess_moat, format_moat, MoatResult

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


def banner(title):
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"{'═' * 65}")


def section(title):
    print(f"\n  {'─' * 55}")
    print(f"  {title}")
    print(f"  {'─' * 55}")


# ══════════════════════════════════════════════════════════════
#  构造输入
# ══════════════════════════════════════════════════════════════

DS_DEFAULTS = {
    "company_id": 1, "period": "FY2025", "segment": None,
    "customer_type": None, "products": None, "channels": None,
    "revenue": None, "growth_yoy": None, "backlog": None,
    "backlog_note": None, "pricing_model": None, "contract_duration": None,
    "recognition_method": None, "description": None,
    "raw_post_id": None, "created_at": "2025-01-01",
}

fli_data = {
    "FY2024": {
        "revenue": 45_000, "cost_of_revenue": 9_000,
        "operating_income": 22_000, "net_income": 18_000,
        "operating_cash_flow": 20_000, "capital_expenditures": 2_000,
        "depreciation_amortization": 1_500,
        "shareholders_equity": 70_000, "total_assets": 90_000,
        "interest_expense": 100, "current_assets": 35_000,
        "current_liabilities": 10_000, "goodwill": 1_000,
        "accounts_receivable": 3_000, "inventory": 5_000,
        "cash_and_equivalents": 22_000, "total_debt": 2_000,
        "dividends_paid": -9_000, "share_repurchase": -4_000,
        "sga_expense": 4_500, "rnd_expense": 1_000,
        "basic_weighted_average_shares": 1_000,
        "income_tax_expense_total": 4_000, "income_before_tax_total": 22_000,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
    },
    "FY2025": {
        "revenue": 50_000, "cost_of_revenue": 10_000,
        "operating_income": 25_000, "net_income": 20_000,
        "operating_cash_flow": 23_000, "capital_expenditures": 2_000,
        "depreciation_amortization": 1_500,
        "shareholders_equity": 80_000, "total_assets": 100_000,
        "interest_expense": 100, "current_assets": 40_000,
        "current_liabilities": 10_000, "goodwill": 1_000,
        "accounts_receivable": 3_000, "inventory": 5_000,
        "cash_and_equivalents": 25_000, "total_debt": 2_000,
        "dividends_paid": -10_000, "share_repurchase": -5_000,
        "sga_expense": 5_000, "rnd_expense": 1_000,
        "basic_weighted_average_shares": 1_000,
        "income_tax_expense_total": 5_000, "income_before_tax_total": 25_000,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
    },
}

downstream_data = [
    {"customer_name": "零售渠道", "revenue_pct": 0.60, "is_recurring": True,
     "revenue_type": "recurring", "contract_duration": "1 year"},
    {"customer_name": "批发渠道", "revenue_pct": 0.30, "is_recurring": True,
     "revenue_type": "recurring"},
    {"customer_name": "电商直营", "revenue_pct": 0.10, "is_recurring": True,
     "revenue_type": "subscription"},
]

upstream_data = [
    {"supplier_name": "原料 A", "is_sole_source": False, "geographic_location": "国内"},
    {"supplier_name": "原料 B", "is_sole_source": False, "geographic_location": "国内"},
]

pricing_data = [
    {"action": "核心产品提价 8%", "price_change_pct": 0.08,
     "product_or_segment": "核心产品", "effective_date": "2024-01",
     "volume_impact_pct": 0.02},
    {"action": "核心产品提价 12%", "price_change_pct": 0.12,
     "product_or_segment": "核心产品", "effective_date": "2025-01",
     "volume_impact_pct": 0.05},
]

market_share_data = [
    {"period": "FY2023", "share": 0.32, "source": "行业协会"},
    {"period": "FY2024", "share": 0.35, "source": "行业协会"},
    {"period": "FY2025", "share": 0.37, "source": "行业协会"},
]


def build_context():
    all_fli = pd.concat([_fli(v, k) for k, v in fli_data.items()], ignore_index=True)

    ctx = ComputeContext(company_id=1, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli_data["FY2025"]),
        "financial_line_items_all": all_fli,
        "downstream_segments": _df(downstream_data, DS_DEFAULTS),
        "upstream_segments": _df(upstream_data, {
            "company_id": 1, "period": "FY2025", "segment": None,
            "supply_type": "component", "material_or_service": None,
            "process_node": None, "purchase_obligation": None,
            "contract_type": None, "prepaid_amount": None,
            "concentration_risk": None, "description": None,
            "raw_post_id": None, "created_at": "2025-01-01"}),
        "pricing_actions": _df(pricing_data, {
            "company_id": 1, "raw_post_id": None, "created_at": "2025-01-01"}),
        "market_share_data": _df(market_share_data, {
            "company_id": 1, "raw_post_id": None, "created_at": "2025-01-01"}),
        "geographic_revenues": EMPTY, "debt_obligations": EMPTY,
        "debt_obligations_all": EMPTY, "executive_compensations": EMPTY,
        "stock_ownership": EMPTY, "company_narratives": EMPTY,
        "litigations": EMPTY, "operational_issues": EMPTY,
        "related_party_transactions": EMPTY, "non_financial_kpis": EMPTY,
        "audit_opinions": EMPTY, "known_issues": EMPTY,
        "insider_transactions": EMPTY, "executive_changes": EMPTY,
        "equity_offerings": EMPTY, "analyst_estimates": EMPTY,
        "management_guidance": EMPTY, "management_acknowledgments": EMPTY,
    }
    return ctx


def compute_features(ctx):
    results = {}
    for level in (FeatureLevel.L0, FeatureLevel.L1, FeatureLevel.L2):
        for feat in get_features(level=level):
            try:
                r = feat.compute_fn(ctx)
            except Exception:
                continue
            if r is not None:
                ctx.features[feat.name] = r.value
                results[feat.name] = r
    return results


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "▓" * 65)
    print("  护城河检测 · 输入输出逐步展示")
    print("  公司: BrandCorp (类茅台)")
    print("▓" * 65)

    # ── STEP 1: INPUT — Anchor 原始数据 ──
    banner("STEP 1: INPUT — Anchor 原始数据")

    section("financial_line_items (FY2025)")
    for k, v in fli_data["FY2025"].items():
        print(f"    {k:<45s} {v:>12,.0f}")

    section("downstream_segments")
    for seg in downstream_data:
        print(f"    {seg['customer_name']:<15s} "
              f"占比={seg['revenue_pct']:.0%}  "
              f"经常性={seg['is_recurring']}  "
              f"类型={seg['revenue_type']}")

    section("pricing_actions")
    for pa in pricing_data:
        print(f"    {pa['effective_date']}  {pa['action']:<25s}  "
              f"销量影响={pa['volume_impact_pct']:+.0%}")

    section("market_share_data")
    for ms in market_share_data:
        print(f"    {ms['period']}  份额={ms['share']:.0%}  来源={ms['source']}")

    # ── STEP 2: 特征计算 ──
    banner("STEP 2: 特征计算 (L0)")

    ctx = build_context()
    feat_results = compute_features(ctx)

    section("护城河相关特征")
    moat_feats = [
        "gross_margin", "gross_margin_stability", "gross_margin_delta",
        "operating_margin", "net_margin", "owner_earnings",
        "recurring_revenue_pct", "revenue_growth_yoy",
        "capex_to_revenue", "shareholder_yield",
        "share_dilution_rate",
    ]
    for key in moat_feats:
        full = f"l0.company.{key}"
        val = ctx.features.get(full)
        if val is not None:
            print(f"    {key:<35s} = {val:>12.4f}")
        else:
            print(f"    {key:<35s} = {'(无)':>12s}")

    # ── STEP 3: 护城河检测 ──
    banner("STEP 3: 护城河检测 (assess_moat)")

    section("INPUT → assess_moat()")
    print(f"    ComputeContext:")
    print(f"      company_id = {ctx.company_id}")
    print(f"      period = {ctx.period}")
    print(f"      已算特征数 = {len(ctx.features)}")
    print(f"      Anchor 表:")
    for table, df in sorted(ctx._cache.items()):
        rows = len(df) if not df.empty else 0
        if rows > 0:
            print(f"        {table}: {rows} rows")

    section("检测过程")

    result = assess_moat(ctx)

    # 逐类展示
    for cat in result.categories:
        detected_names = cat.detected_names
        if detected_names or any(sub.evidence for sub in cat.subtypes):
            status = "检测到" if detected_names else "未检测到"
            print(f"\n    [{cat.name}] → {status}")

            for sub in cat.subtypes:
                if not sub.evidence:
                    continue
                has_real_evidence = any(e.supports is not None for e in sub.evidence)
                if sub.detected or has_real_evidence:
                    det = "✓" if sub.detected else "?"
                    print(f"      {det} {sub.name}")
                    for ev in sub.evidence:
                        sup = "+" if ev.supports is True else ("-" if ev.supports is False else " ")
                        print(f"        [{sup}] {ev.observation}")
                        print(f"             来源={ev.source}  强度={ev.strength.value}")
                    if sub.detail:
                        print(f"        → {sub.detail}")

    if result.anti_moat:
        print(f"\n    [伪护城河检测]")
        for ev in result.anti_moat:
            print(f"      [-] {ev.observation}")

    # ── STEP 4: OUTPUT ──
    banner("STEP 4: OUTPUT — MoatResult")

    section("结构化输出")
    print(f"    depth          = {result.depth}")
    print(f"    summary        = {result.summary}")
    print(f"    anti_moat      = {len(result.anti_moat)} 条")
    print(f"    detected types = {result.all_detected}")
    print(f"    categories:")
    for cat in result.categories:
        d = cat.detected
        print(f"      {cat.name}: detected={d}")
        for sub in cat.subtypes:
            if sub.detected is not None or sub.detail:
                print(f"        {sub.name}: detected={sub.detected}  evidence={len(sub.evidence)}条")

    section("格式化报告")
    print(format_moat(result))
