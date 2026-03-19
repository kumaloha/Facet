"""
巴菲特因果链 · 西方石油 (OXY)
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


# ══════════════════════════════════════════════════════════════
#  西方石油 (OXY) — 巴菲特重仓的非典型标的
# ══════════════════════════════════════════════════════════════
# 单位: 百万美元
# 特点: 大宗商品、重资产、周期性强、高杠杆
# 巴菲特持股 28%+，另有 $10B 优先股 + 认购权证

OXY = {
    "fli": {
        # 2022: 油价 $95+ 暴利年
        "FY2022": {
            "revenue": 36600, "cost_of_revenue": 17500, "operating_income": 14500,
            "net_income": 12500, "operating_cash_flow": 17600, "capital_expenditures": 4500,
            "depreciation_amortization": 5200, "shareholders_equity": 30200,
            "total_assets": 72000, "interest_expense": 1400, "current_assets": 12000,
            "current_liabilities": 11500, "goodwill": 1200,
            "accounts_receivable": 5500, "inventory": 1200,
            "cash_and_equivalents": 1500, "total_debt": 22000,
            "dividends_paid": -800, "share_repurchase": -3600,
            "sga_expense": 2000, "rnd_expense": 0,
            "basic_weighted_average_shares": 920,
            "income_tax_expense_total": 2200, "income_before_tax_total": 14700,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        # 2023: 油价回落至 $75-80
        "FY2023": {
            "revenue": 23200, "cost_of_revenue": 13500, "operating_income": 5700,
            "net_income": 4700, "operating_cash_flow": 11500, "capital_expenditures": 6400,
            "depreciation_amortization": 5800, "shareholders_equity": 33500,
            "total_assets": 74000, "interest_expense": 1200, "current_assets": 10000,
            "current_liabilities": 10500, "goodwill": 1200,
            "accounts_receivable": 4200, "inventory": 1100,
            "cash_and_equivalents": 1200, "total_debt": 19500,
            "dividends_paid": -1400, "share_repurchase": -1000,
            "sga_expense": 1800, "rnd_expense": 0,
            "basic_weighted_average_shares": 900,
            "income_tax_expense_total": 1200, "income_before_tax_total": 5900,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        # 2024: CrownRock 收购完成 ($12.4B)，规模扩大但负债增加
        "FY2024": {
            "revenue": 22000, "cost_of_revenue": 12800, "operating_income": 4800,
            "net_income": 2700, "operating_cash_flow": 11300, "capital_expenditures": 7500,
            "depreciation_amortization": 7000, "shareholders_equity": 36800,
            "total_assets": 83500, "interest_expense": 1500, "current_assets": 10500,
            "current_liabilities": 12000, "goodwill": 1500,
            "accounts_receivable": 4800, "inventory": 1000,
            "cash_and_equivalents": 800, "total_debt": 25000,
            "dividends_paid": -1600, "share_repurchase": -600,
            "sga_expense": 1900, "rnd_expense": 0,
            "basic_weighted_average_shares": 890,
            "income_tax_expense_total": 800, "income_before_tax_total": 3500,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 8000,
        },
        # 2025: 降本增效 + 债务削减，但油价 $60-65 压制利润
        "FY2025": {
            "revenue": 21600, "cost_of_revenue": 12500, "operating_income": 4500,
            "net_income": 2400, "operating_cash_flow": 10500, "capital_expenditures": 6800,
            "depreciation_amortization": 7200, "shareholders_equity": 37000,
            "total_assets": 82000, "interest_expense": 1300, "current_assets": 10000,
            "current_liabilities": 11000, "goodwill": 1500,
            "accounts_receivable": 4500, "inventory": 1100,
            "cash_and_equivalents": 600, "total_debt": 21700,
            "dividends_paid": -1700, "share_repurchase": -400,
            "sga_expense": 1700, "rnd_expense": 0,
            "basic_weighted_average_shares": 885,
            "income_tax_expense_total": 700, "income_before_tax_total": 3100,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 2000,
        },
    },
    "downstream": [
        # OXY 卖的是原油——大宗商品，无差异化
        {"customer_name": "原油销售", "revenue_pct": 0.55, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "commodity"},
        {"customer_name": "天然气/NGL", "revenue_pct": 0.15, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "commodity"},
        {"customer_name": "化工品 (OxyChem)", "revenue_pct": 0.20, "is_recurring": True,
         "revenue_type": "recurring", "product_category": "commodity"},
        {"customer_name": "碳捕获 (1PointFive)", "revenue_pct": 0.02, "is_recurring": False,
         "revenue_type": "recurring", "product_category": "commodity"},
        {"customer_name": "中游/运输", "revenue_pct": 0.08, "is_recurring": True,
         "revenue_type": "recurring", "product_category": "pipeline"},
    ],
    "upstream": [
        {"supplier_name": "油服公司 (Halliburton/SLB)", "is_sole_source": False, "geographic_location": "美国"},
        {"supplier_name": "钻井材料", "is_sole_source": False, "geographic_location": "美国"},
    ],
    "geo": [
        {"region": "United States", "revenue_share": 0.75},
        {"region": "Middle East (Oman/UAE)", "revenue_share": 0.15},
        {"region": "Other", "revenue_share": 0.10},
    ],
    "pricing": [
        # 石油公司无定价权——价格由全球市场决定
        # 但可以看成本端: OXY Permian 盈亏平衡 $40/桶
    ],
    "market_share": [
        # 全球石油产量中 OXY 占比极小
        {"period": "FY2023", "share": 0.012, "source": "全球原油产量"},
        {"period": "FY2024", "share": 0.014, "source": "全球原油产量"},
        {"period": "FY2025", "share": 0.014, "source": "全球原油产量"},
    ],
    "competitive": [
        # 低成本生存测试: 2020 油价暴跌到 $20，OXY 险些倒闭但活了下来
        {"competitor_name": "行业", "event_type": "industry_downturn",
         "event_description": "2020 年油价暴跌至 $20/桶，全行业亏损",
         "outcome_description": "OXY 削减分红 99%、出售资产但存活，2022 年油价回升后大幅偿债",
         "outcome_market_share_change": 0.0},
        # CrownRock 收购 — 扩大 Permian 低成本库存
        {"competitor_name": "CrownRock", "event_type": "product_launch",
         "event_description": "OXY 以 $12.4B 收购 CrownRock，新增 17万桶/日产量",
         "outcome_description": "Permian 低盈亏平衡库存增加 33%，5 个月内达到去杠杆里程碑",
         "outcome_market_share_change": 0.002},
    ],
    "peers": [
        # 油气行业同行对比
        {"peer_name": "ExxonMobil", "metric": "gross_margin", "value": 0.30, "period": "FY2025"},
        {"peer_name": "ExxonMobil", "metric": "operating_margin", "value": 0.12, "period": "FY2025"},
        {"peer_name": "ExxonMobil", "metric": "net_margin", "value": 0.09, "period": "FY2025"},
        {"peer_name": "Chevron", "metric": "gross_margin", "value": 0.32, "period": "FY2025"},
        {"peer_name": "Chevron", "metric": "operating_margin", "value": 0.14, "period": "FY2025"},
        {"peer_name": "Chevron", "metric": "net_margin", "value": 0.10, "period": "FY2025"},
        {"peer_name": "ConocoPhillips", "metric": "gross_margin", "value": 0.42, "period": "FY2025"},
        {"peer_name": "ConocoPhillips", "metric": "operating_margin", "value": 0.28, "period": "FY2025"},
        {"peer_name": "ConocoPhillips", "metric": "net_margin", "value": 0.20, "period": "FY2025"},
    ],
    "narratives": [
        {"narrative": "Permian Basin 低成本领导者", "status": "delivered"},
        {"narrative": "CrownRock 收购整合完成", "status": "delivered"},
        {"narrative": "债务从 $30B 削减到目标 $15B", "status": "in_progress"},
        {"narrative": "碳捕获 1PointFive 技术领先", "status": "in_progress"},
        {"narrative": "OxyChem 化工业务稳定现金流", "status": "delivered"},
        {"narrative": "年化降本 $20亿", "status": "delivered"},
    ],
    "ownership": [
        {"name": "Vicki Hollub", "title": "CEO", "percent_of_class": 0.1},
    ],
    "exec_comp": [
        {"name": "Vicki Hollub", "title": "CEO", "role_type": "CEO",
         "pay_ratio": 180.0, "stock_awards": 8000, "total_comp": 16000},
    ],
    "known_issues": [
        {"issue_description": "CrownRock 收购导致短期债务飙升至 $25B",
         "severity": "medium", "source_type": "financial"},
    ],
    "management_acks": [
        {"issue_description": "承认 CrownRock 收购短期增加杠杆，已设定 $15B 目标并 5 个月达到去杠杆里程碑",
         "response_quality": "strong", "has_action_plan": True},
    ],
    "market": {"price": 45.0, "shares_outstanding": 885, "discount_rate": 0.043, "market": "US"},
    # 石油公司没有 revenue guidance — 收入取决于油价
    "guidance": {},
    "home_market": "United States",
}


# ══════════════════════════════════════════════════════════════
#  构建 + 跑链
# ══════════════════════════════════════════════════════════════

def build_ctx(data):
    fli_data = data["fli"]
    all_fli = pd.concat([_fli(v, k) for k, v in fli_data.items()], ignore_index=True)
    ctx = ComputeContext(company_id=1, period="FY2025")

    ki_rows = data.get("known_issues", [])
    ma_rows = data.get("management_acks", [])

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
        "insider_transactions": EMPTY, "executive_changes": EMPTY,
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
    data = OXY
    name = "西方石油 (OXY)"

    print("\n" + "▓" * 65)
    print(f"  巴菲特因果链 · {name}")
    print("▓" * 65)

    ctx = build_ctx(data)
    compute_all_features(ctx)
    print(f"\n  已算特征: {len(ctx.features)} 个")

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

    mkt = data["market"]
    guidance = data["guidance"]
    market_code = mkt["market"]

    line1_quality = (moat.depth in ("extreme", "deep") and
                     earnings.verdict == "holds" and
                     pred.verdict == "holds")
    certainty = "high" if line1_quality else "normal"

    print("\n  可估值 + 安全边际")
    print("  " + "═" * 48)
    labels = {"high": "高确定性 → 无风险利率", "normal": "普通 → 无风险+ERP"}
    print(f"  确定性: {labels[certainty]}")

    dcf = None
    mos = None
    if mkt.get("discount_rate") and mkt.get("shares_outstanding"):
        dcf = compute_intrinsic_value(ctx.features, guidance, mkt["discount_rate"],
                                       mkt["shares_outstanding"], market=market_code,
                                       certainty=certainty)
        if dcf.intrinsic_value:
            price = mkt["price"]
            mos = (dcf.intrinsic_value - price) / dcf.intrinsic_value
            print(f"  路径 {dcf.valuation_path}: 内在价值 {dcf.intrinsic_value:,.1f} $/股")
            print(f"  当前股价: {price} $/股")
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
                      f"实际 {actual:.1%}  偏差 {rdcf.implied_growth_rate - actual:+.1%}")

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

    print("=" * 65)
    print("  综合判断")
    print("=" * 65)

    print(f"\n  线 1:")
    print(f"    护城河:   {moat.depth} — {moat.summary}")
    print(f"    盈余能力: {earnings.verdict} — {earnings.summary}")
    print(f"    利润分配: {dist.verdict} — {dist.summary}")
    print(f"    可预测:   {pred.verdict} — {pred.summary}")
    if dcf and dcf.intrinsic_value:
        print(f"    可估值:   valued — {dcf.intrinsic_value:,.1f} $/股 (路径 {dcf.valuation_path})")
        if mos is not None:
            print(f"    安全边际: {mos:.1%}")

    print(f"\n  线 2:")
    print(f"    诚信:     {integrity.verdict} — {integrity.summary}")
    print(f"    管理层:   {character.conviction} — {character.summary}")
    cat_risk = "灾难性" if risk.has_catastrophic else (f"{len(risk.significant)} 项重大" if risk.significant else "可控")
    print(f"    风险:     {cat_risk} — {risk.summary}")

    line1_ok = (moat.depth not in ("none", "unknown") and
                earnings.verdict == "holds" and dist.verdict == "holds")
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
