"""
巴菲特因果链 · 比亚迪完整评估
==============================
双线并行，全部模块串通。
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
from polaris.principles.engines.dcf import compute_intrinsic_value, reverse_dcf

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

# ══════════════════════════════════════════════════════════════
#  比亚迪 Mock 数据（近似 2022-2025）
# ══════════════════════════════════════════════════════════════

# 单位: 亿元人民币
BYD_FLI = {
    "FY2022": {
        "revenue": 4241, "cost_of_revenue": 3510, "operating_income": 332,
        "net_income": 166, "operating_cash_flow": 655, "capital_expenditures": 538,
        "depreciation_amortization": 280, "shareholders_equity": 1350,
        "total_assets": 5800, "interest_expense": 55, "current_assets": 2800,
        "current_liabilities": 3200, "goodwill": 10,
        "accounts_receivable": 500, "inventory": 700,
        "cash_and_equivalents": 600, "total_debt": 800,
        "dividends_paid": -30, "share_repurchase": 0,
        "sga_expense": 200, "rnd_expense": 186,
        "basic_weighted_average_shares": 29.1,
        "income_tax_expense_total": 25, "income_before_tax_total": 191,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 200,
    },
    "FY2023": {
        "revenue": 6023, "cost_of_revenue": 4854, "operating_income": 565,
        "net_income": 300, "operating_cash_flow": 1080, "capital_expenditures": 780,
        "depreciation_amortization": 380, "shareholders_equity": 1800,
        "total_assets": 7800, "interest_expense": 65, "current_assets": 3500,
        "current_liabilities": 4200, "goodwill": 12,
        "accounts_receivable": 650, "inventory": 850,
        "cash_and_equivalents": 800, "total_debt": 1000,
        "dividends_paid": -42, "share_repurchase": 0,
        "sga_expense": 250, "rnd_expense": 396,
        "basic_weighted_average_shares": 29.1,
        "income_tax_expense_total": 40, "income_before_tax_total": 340,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 300,
    },
    "FY2024": {
        "revenue": 7771, "cost_of_revenue": 6145, "operating_income": 820,
        "net_income": 402, "operating_cash_flow": 1450, "capital_expenditures": 950,
        "depreciation_amortization": 480, "shareholders_equity": 2300,
        "total_assets": 9500, "interest_expense": 70, "current_assets": 4200,
        "current_liabilities": 5000, "goodwill": 12,
        "accounts_receivable": 780, "inventory": 950,
        "cash_and_equivalents": 1000, "total_debt": 1100,
        "dividends_paid": -60, "share_repurchase": 0,
        "sga_expense": 300, "rnd_expense": 500,
        "basic_weighted_average_shares": 29.1,
        "income_tax_expense_total": 55, "income_before_tax_total": 457,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 200,
    },
    "FY2025": {
        "revenue": 10000, "cost_of_revenue": 7800, "operating_income": 1100,
        "net_income": 550, "operating_cash_flow": 1900, "capital_expenditures": 1100,
        "depreciation_amortization": 580, "shareholders_equity": 2900,
        "total_assets": 11500, "interest_expense": 75, "current_assets": 5000,
        "current_liabilities": 5800, "goodwill": 12,
        "accounts_receivable": 900, "inventory": 1100,
        "cash_and_equivalents": 1200, "total_debt": 1200,
        "dividends_paid": -80, "share_repurchase": 0,
        "sga_expense": 350, "rnd_expense": 600,
        "basic_weighted_average_shares": 29.1,
        "income_tax_expense_total": 70, "income_before_tax_total": 620,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 150,
    },
}


def build_byd():
    all_fli = pd.concat([_fli(v, k) for k, v in BYD_FLI.items()], ignore_index=True)
    all_debt = EMPTY  # 简化

    ctx = ComputeContext(company_id=1, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(BYD_FLI["FY2025"]),
        "financial_line_items_all": all_fli,
        "downstream_segments": _df([
            {"customer_name": "新能源汽车", "revenue_pct": 0.75, "is_recurring": False,
             "revenue_type": "recurring", "product_category": "consumer_electronics"},
            {"customer_name": "电池及储能", "revenue_pct": 0.15, "is_recurring": True,
             "revenue_type": "recurring", "product_category": "consumer_electronics"},
            {"customer_name": "手机零部件", "revenue_pct": 0.10, "is_recurring": True,
             "revenue_type": "recurring"},
        ], DS),
        "upstream_segments": _df([
            {"supplier_name": "锂矿自有", "is_sole_source": False, "geographic_location": "中国"},
            {"supplier_name": "芯片供应商", "is_sole_source": False, "geographic_location": "多地"},
        ], {"company_id": 1, "period": "FY2025", "segment": None, "supply_type": "component",
            "material_or_service": None, "process_node": None, "purchase_obligation": None,
            "contract_type": None, "prepaid_amount": None, "concentration_risk": None,
            "description": None, "raw_post_id": None, "created_at": "2025-01-01"}),
        "geographic_revenues": _df([
            {"region": "China", "revenue_share": 0.60},
            {"region": "Europe", "revenue_share": 0.15},
            {"region": "Southeast Asia", "revenue_share": 0.15},
            {"region": "Other", "revenue_share": 0.10},
        ], {"company_id": 1, "period": "FY2025", "revenue": None, "growth_yoy": None,
            "note": None, "raw_post_id": None, "created_at": "2025-01-01"}),
        "pricing_actions": _df([
            {"action": "秦PLUS 官降 2 万", "price_change_pct": -0.15,
             "product_or_segment": "秦PLUS", "effective_date": "2024-02",
             "volume_impact_pct": 0.40},
        ], PA),
        "market_share_data": _df([
            {"period": "FY2023", "share": 0.35, "source": "中国新能源车市场"},
            {"period": "FY2024", "share": 0.38, "source": "中国新能源车市场"},
            {"period": "FY2025", "share": 0.40, "source": "中国新能源车市场"},
        ], MS),
        "competitive_dynamics": _df([
            {"competitor_name": "特斯拉", "event_type": "price_war",
             "event_description": "特斯拉 Model 3 降价 15% 发起价格战",
             "outcome_description": "比亚迪份额反升至 40%，特斯拉中国份额下降",
             "outcome_market_share_change": 0.02},
            {"competitor_name": "小米汽车", "event_type": "new_entry",
             "event_description": "小米 SU7 上市，首月交付过万",
             "outcome_description": "抢到一定份额但主要影响特斯拉，比亚迪未受明显影响",
             "outcome_market_share_change": 0.0},
        ], CD),
        "peer_financials": pd.DataFrame([
            {"peer_name": "特斯拉", "metric": "gross_margin", "value": 0.18, "period": "FY2025"},
            {"peer_name": "特斯拉", "metric": "operating_margin", "value": 0.08, "period": "FY2025"},
            {"peer_name": "特斯拉", "metric": "net_margin", "value": 0.06, "period": "FY2025"},
            {"peer_name": "吉利", "metric": "gross_margin", "value": 0.15, "period": "FY2025"},
            {"peer_name": "吉利", "metric": "operating_margin", "value": 0.05, "period": "FY2025"},
            {"peer_name": "吉利", "metric": "net_margin", "value": 0.03, "period": "FY2025"},
            {"peer_name": "长城", "metric": "gross_margin", "value": 0.17, "period": "FY2025"},
            {"peer_name": "长城", "metric": "operating_margin", "value": 0.06, "period": "FY2025"},
            {"peer_name": "长城", "metric": "net_margin", "value": 0.04, "period": "FY2025"},
        ]),
        "company_narratives": _df([
            {"narrative": "新能源汽车全球领导者", "status": "delivered"},
            {"narrative": "刀片电池技术革命", "status": "delivered"},
            {"narrative": "智能驾驶天神之眼", "status": "delivered"},
            {"narrative": "出海战略: 进入欧洲和东南亚", "status": "delivered"},
            {"narrative": "高端品牌仰望", "status": "delivered"},
            {"narrative": "年销量目标 400 万", "status": "missed"},
        ], {"company_id": 1, "raw_post_id": None, "capital_required": None,
            "capital_unit": None, "promised_outcome": None, "deadline": None,
            "reported_at": None, "created_at": "2025-01-01"}),
        "stock_ownership": _df([
            {"name": "王传福", "title": "创始人/CEO", "percent_of_class": 17.6},
        ], {"company_id": 1, "period": "FY2025", "shares_beneficially_owned": None,
            "raw_post_id": None, "created_at": "2025-01-01"}),
        "executive_compensations": _df([
            {"name": "王传福", "title": "创始人/CEO", "role_type": "CEO",
             "pay_ratio": 50.0, "stock_awards": 0, "total_comp": 500},
        ], {"company_id": 1, "period": "FY2025", "base_salary": None, "bonus": None,
            "option_awards": None, "non_equity_incentive": None, "other_comp": None,
            "currency": "CNY", "median_employee_comp": None, "raw_post_id": None,
            "created_at": "2025-01-01"}),
        "debt_obligations": EMPTY, "debt_obligations_all": EMPTY,
        "litigations": EMPTY, "operational_issues": EMPTY,
        "related_party_transactions": EMPTY, "non_financial_kpis": EMPTY,
        "audit_opinions": EMPTY, "known_issues": EMPTY,
        "insider_transactions": EMPTY, "executive_changes": EMPTY,
        "equity_offerings": EMPTY, "analyst_estimates": EMPTY,
        "management_guidance": EMPTY, "management_acknowledgments": EMPTY,
        "brand_signals": EMPTY,
    }
    return ctx


if __name__ == "__main__":
    print("▓" * 65)
    print("  巴菲特因果链 · 比亚迪 (BYD) 完整评估")
    print("▓" * 65)

    ctx = build_byd()

    # 算特征
    for level in (FeatureLevel.L0, FeatureLevel.L1, FeatureLevel.L2):
        for feat in get_features(level=level):
            try:
                r = feat.compute_fn(ctx)
            except:
                continue
            if r:
                ctx.features[feat.name] = r.value

    print(f"\n  已算特征: {len(ctx.features)} 个")

    # ══════════════════════════════════════════════════════════
    #  线 1: 生意评估
    # ══════════════════════════════════════════════════════════

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
    market = {"price": 350, "shares_outstanding": 29.1, "discount_rate": 0.017}

    print("\n  可估值 + 安全边际")
    print("  ════════════════════════════════════════════════")

    dcf = compute_intrinsic_value(ctx.features, {"revenue_growth": 0.20}, market["discount_rate"],
                                  market["shares_outstanding"], market="CN")
    if dcf.intrinsic_value:
        mos = (dcf.intrinsic_value - market["price"]) / dcf.intrinsic_value
        print(f"  路径 {dcf.valuation_path}: 内在价值 {dcf.intrinsic_value:,.0f} 元/股")
        print(f"  当前股价: {market['price']} 元")
        print(f"  安全边际: {mos:.1%}")
        print(f"  假设: {dcf.key_assumptions}")

    rdcf = reverse_dcf(market["price"], ctx.features.get("l0.company.owner_earnings", 0),
                       market["discount_rate"], market["shares_outstanding"], market="CN")
    if rdcf.implied_growth_rate is not None:
        actual = ctx.features.get("l0.company.revenue_growth_yoy", 0)
        print(f"\n  反向 DCF: 隐含增速 {rdcf.implied_growth_rate:.1%}  实际 {actual:.1%}  偏差 {rdcf.implied_growth_rate - actual:+.1%}")

    # ══════════════════════════════════════════════════════════
    #  线 2: 人和环境
    # ══════════════════════════════════════════════════════════

    print("\n" + "=" * 65)
    print("  线 2: 人和环境")
    print("=" * 65)

    integrity = assess_integrity(ctx)
    print(format_integrity(integrity))

    character = assess_character(ctx)
    print(format_character(character))

    risk = assess_risk(ctx, home_market="China")
    print(format_risk(risk))

    # ══════════════════════════════════════════════════════════
    #  综合判断
    # ══════════════════════════════════════════════════════════

    print("=" * 65)
    print("  综合判断")
    print("=" * 65)

    print(f"\n  线 1:")
    print(f"    护城河:   {moat.depth} — {moat.summary}")
    print(f"    盈余能力: {earnings.verdict} — {earnings.summary}")
    print(f"    利润分配: {dist.verdict} — {dist.summary}")
    print(f"    可预测:   {pred.verdict} — {pred.summary}")
    if dcf.intrinsic_value:
        print(f"    可估值:   valued — {dcf.intrinsic_value:,.0f} 元/股 (路径 {dcf.valuation_path})")
        print(f"    安全边际: {mos:.1%}")

    print(f"\n  线 2:")
    print(f"    诚信:     {integrity.verdict} — {integrity.summary}")
    print(f"    管理层:   {character.conviction} — {character.summary}")
    print(f"    风险:     {'灾难性' if risk.has_catastrophic else ('重大' if risk.significant else '可控')} — {risk.summary}")

    # 最终
    line1_ok = (moat.depth not in ("none", "unknown") and
                earnings.verdict == "holds" and
                dist.verdict == "holds")
    line2_ok = (integrity.verdict != "breaks" and not risk.has_catastrophic)

    print(f"\n  {'─' * 55}")
    if line1_ok and line2_ok:
        print(f"  结论: 生意好 + 人可信 + 风险可控 → 可以投资")
    elif line1_ok and not line2_ok:
        if risk.has_catastrophic:
            print(f"  结论: 好生意但有灾难性风险 → 不能买")
        else:
            print(f"  结论: 好生意但诚信存疑 → 需谨慎")
    else:
        broken = []
        if moat.depth in ("none", "unknown"): broken.append("护城河")
        if earnings.verdict != "holds": broken.append("盈余能力")
        if dist.verdict != "holds": broken.append("利润分配")
        print(f"  结论: 生意链问题在 {', '.join(broken)}")
    print()
