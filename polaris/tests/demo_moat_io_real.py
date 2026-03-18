"""
护城河检测 · 4 家真实公司 · 逐步输入输出
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


def banner(title):
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"{'═' * 65}")


def section(title):
    print(f"\n  {'─' * 55}")
    print(f"  {title}")
    print(f"  {'─' * 55}")


def show_io(name, desc, ctx, anchor_tables):
    print(f"\n{'▓' * 65}")
    print(f"  {name}")
    print(f"  {desc}")
    print(f"{'▓' * 65}")

    # ── STEP 1: INPUT ──
    banner("STEP 1: INPUT — Anchor 原始数据")

    for table_name, display_fn in anchor_tables:
        section(table_name)
        display_fn()

    # ── STEP 2: 特征计算 ──
    banner("STEP 2: 特征计算")
    compute(ctx)

    moat_feats = [
        "gross_margin", "operating_margin", "net_margin",
        "owner_earnings", "recurring_revenue_pct",
        "capex_to_revenue", "share_dilution_rate",
    ]
    for key in moat_feats:
        val = ctx.features.get(f"l0.company.{key}")
        if val is not None:
            print(f"    {key:<30s} = {val:>12.4f}")

    # ── STEP 3: 检测 ──
    banner("STEP 3: 护城河检测")
    result = assess_moat(ctx)

    for cat in result.categories:
        detected_names = cat.detected_names
        if not detected_names and not any(
            sub.detail for sub in cat.subtypes
        ):
            continue

        status = "检测到" if detected_names else "有信号"
        print(f"\n    [{cat.name}] → {status}")
        for sub in cat.subtypes:
            if not sub.detected and not sub.detail:
                continue
            det = "✓" if sub.detected else "?"
            print(f"      {det} {sub.name}")
            for ev in sub.evidence:
                sup = "+" if ev.supports is True else ("-" if ev.supports is False else " ")
                print(f"        [{sup}] ({ev.strength.value}) {ev.observation}")
            if sub.detail:
                print(f"        → {sub.detail}")

    if result.anti_moat:
        print(f"\n    [伪护城河]")
        for ev in result.anti_moat:
            print(f"      [-] {ev.observation}")

    # ── STEP 4: OUTPUT ──
    banner("STEP 4: OUTPUT")
    print(f"    depth    = {result.depth}")
    print(f"    detected = {result.all_detected}")
    print(f"    summary  = {result.summary}")
    print()


# ══════════════════════════════════════════════════════════════
#  腾讯
# ══════════════════════════════════════════════════════════════

def tencent():
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
        "basic_weighted_average_shares": 9_400,
        "income_tax_expense_total": 350, "income_before_tax_total": 1_927,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
    }

    downstream = [
        {"customer_name": "游戏", "revenue_pct": 0.32, "is_recurring": True, "revenue_type": "subscription"},
        {"customer_name": "社交网络", "revenue_pct": 0.25, "is_recurring": True, "revenue_type": "subscription"},
        {"customer_name": "广告", "revenue_pct": 0.20, "is_recurring": True, "revenue_type": "ad_revenue"},
        {"customer_name": "金融科技", "revenue_pct": 0.18, "is_recurring": True, "revenue_type": "transaction_fee"},
        {"customer_name": "云", "revenue_pct": 0.05, "is_recurring": True, "revenue_type": "saas"},
    ]

    competitive = [
        {"competitor_name": "阿里·来往", "event_type": "product_launch",
         "event_description": "阿里投入 10 亿推广来往",
         "outcome_description": "来往 2015 年关停，失败", "outcome_market_share_change": 0.0},
        {"competitor_name": "字节·飞聊", "event_type": "product_launch",
         "event_description": "字节发布飞聊社交产品",
         "outcome_description": "飞聊 2021 年关停，失败", "outcome_market_share_change": 0.0},
        {"competitor_name": "字节·多闪", "event_type": "product_launch",
         "event_description": "字节发布多闪",
         "outcome_description": "多闪已关停，失败", "outcome_market_share_change": 0.0},
        {"competitor_name": "子弹短信", "event_type": "product_launch",
         "event_description": "子弹短信短暂爆火",
         "outcome_description": "快速退潮，月活归零，失败", "outcome_market_share_change": 0.0},
    ]

    market_share = [
        {"period": "FY2023", "share": 0.95, "source": "IM MAU"},
        {"period": "FY2024", "share": 0.95, "source": "IM MAU"},
        {"period": "FY2025", "share": 0.94, "source": "IM MAU"},
    ]

    ctx = ComputeContext(company_id=1, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli), "financial_line_items_all": _fli(fli),
        "downstream_segments": _df(downstream, DS),
        "pricing_actions": EMPTY,
        "market_share_data": _df(market_share, MS),
        "competitive_dynamics": _df(competitive, CD),
        **EMPTY_TABLES,
    }

    def show_ds():
        for s in downstream:
            print(f"    {s['customer_name']:<10s} {s['revenue_pct']:>5.0%}  {s['revenue_type']}")

    def show_cd():
        for c in competitive:
            print(f"    {c['competitor_name']:<15s} → {c['outcome_description']}")

    def show_ms():
        for m in market_share:
            print(f"    {m['period']}  {m['share']:.0%}")

    show_io("腾讯", "预期: 网络效应极强", ctx, [
        ("downstream_segments", show_ds),
        ("competitive_dynamics (竞品进攻记录)", show_cd),
        ("market_share_data (IM 市占率)", show_ms),
    ])


# ══════════════════════════════════════════════════════════════
#  抖音
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

    downstream = [
        {"customer_name": "广告主", "revenue_pct": 0.70, "is_recurring": True, "revenue_type": "ad_revenue"},
        {"customer_name": "直播电商", "revenue_pct": 0.20, "is_recurring": True, "revenue_type": "transaction_fee"},
        {"customer_name": "其他", "revenue_pct": 0.10, "is_recurring": False, "revenue_type": "other"},
    ]

    competitive = [
        {"competitor_name": "腾讯·微视", "event_type": "product_launch",
         "event_description": "腾讯 30 亿补贴微视",
         "outcome_description": "微视关停，失败", "outcome_market_share_change": 0.0},
        {"competitor_name": "腾讯·视频号", "event_type": "product_launch",
         "event_description": "视频号嵌入微信生态",
         "outcome_description": "DAU 超 4.5 亿，抢到份额",
         "outcome_market_share_change": -0.03},
        {"competitor_name": "快手", "event_type": "product_launch",
         "event_description": "快手持续投入",
         "outcome_description": "DAU 3.8 亿，稳住第二",
         "outcome_market_share_change": -0.01},
        {"competitor_name": "小红书", "event_type": "new_entry",
         "event_description": "小红书短视频强化",
         "outcome_description": "DAU 增长至 1.2 亿",
         "outcome_market_share_change": -0.02},
    ]

    ctx = ComputeContext(company_id=2, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli), "financial_line_items_all": _fli(fli),
        "downstream_segments": _df(downstream, DS),
        "pricing_actions": EMPTY,
        "market_share_data": EMPTY,
        "competitive_dynamics": _df(competitive, CD),
        **EMPTY_TABLES,
    }

    def show_ds():
        for s in downstream:
            print(f"    {s['customer_name']:<10s} {s['revenue_pct']:>5.0%}  {s['revenue_type']}")

    def show_cd():
        for c in competitive:
            chg = c['outcome_market_share_change']
            print(f"    {c['competitor_name']:<15s} → {c['outcome_description']}"
                  f"  (份额影响: {chg:+.0%})")

    show_io("抖音", "预期: 有护城河但在被蚕食", ctx, [
        ("downstream_segments", show_ds),
        ("competitive_dynamics", show_cd),
    ])


# ══════════════════════════════════════════════════════════════
#  Costco
# ══════════════════════════════════════════════════════════════

def costco():
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

    downstream = [
        {"customer_name": "会员消费者", "revenue_pct": 0.85, "is_recurring": True,
         "revenue_type": "subscription", "contract_duration": "1 year"},
        {"customer_name": "会员费", "revenue_pct": 0.02, "is_recurring": True,
         "revenue_type": "subscription", "contract_duration": "1 year"},
        {"customer_name": "企业会员", "revenue_pct": 0.13, "is_recurring": True,
         "revenue_type": "subscription", "contract_duration": "1 year"},
    ]

    pricing = [
        {"action": "会员费 $60→$65", "price_change_pct": 0.083,
         "product_or_segment": "会员年费", "effective_date": "2024-09",
         "volume_impact_pct": 0.03},
    ]

    market_share = [
        {"period": "FY2023", "share": 0.060, "source": "美国零售"},
        {"period": "FY2024", "share": 0.065, "source": "美国零售"},
        {"period": "FY2025", "share": 0.070, "source": "美国零售"},
    ]

    competitive = [
        {"competitor_name": "Sam's Club", "event_type": "price_war",
         "event_description": "Sam's Club 降低会员费至 $45 对标 Costco",
         "outcome_description": "Costco 续费率仍维持 92.7%，会员数增长 7%，未受影响",
         "outcome_market_share_change": 0.005},
    ]

    peer_data = [
        {"peer_name": "Walmart", "metric": "gross_margin", "value": 0.245, "period": "FY2025"},
        {"peer_name": "Walmart", "metric": "operating_margin", "value": 0.042, "period": "FY2025"},
        {"peer_name": "Walmart", "metric": "net_margin", "value": 0.026, "period": "FY2025"},
        {"peer_name": "Target", "metric": "gross_margin", "value": 0.275, "period": "FY2025"},
        {"peer_name": "Target", "metric": "operating_margin", "value": 0.058, "period": "FY2025"},
        {"peer_name": "Target", "metric": "net_margin", "value": 0.038, "period": "FY2025"},
        {"peer_name": "Sam's Club", "metric": "gross_margin", "value": 0.130, "period": "FY2025"},
        {"peer_name": "Sam's Club", "metric": "operating_margin", "value": 0.025, "period": "FY2025"},
        {"peer_name": "Sam's Club", "metric": "net_margin", "value": 0.018, "period": "FY2025"},
    ]

    ctx = ComputeContext(company_id=3, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli), "financial_line_items_all": _fli(fli),
        "downstream_segments": _df(downstream, DS),
        "pricing_actions": _df(pricing, {"company_id": 3, "raw_post_id": None, "created_at": "2025-01-01"}),
        "market_share_data": _df(market_share, MS),
        "competitive_dynamics": _df(competitive, CD),
        "peer_financials": pd.DataFrame(peer_data),
        **EMPTY_TABLES,
    }

    def show_fli():
        keys = ["revenue", "cost_of_revenue", "operating_income", "net_income"]
        for k in keys:
            print(f"    {k:<25s} {fli[k]:>12,.0f}")
        gm = (fli["revenue"] - fli["cost_of_revenue"]) / fli["revenue"]
        om = fli["operating_income"] / fli["revenue"]
        print(f"    {'gross_margin':<25s} {gm:>12.1%}")
        print(f"    {'operating_margin':<25s} {om:>12.1%}")

    def show_ds():
        for s in downstream:
            dur = s.get("contract_duration", "")
            print(f"    {s['customer_name']:<15s} {s['revenue_pct']:>5.0%}  "
                  f"{s['revenue_type']:<15s} {dur}")

    def show_pa():
        for p in pricing:
            print(f"    {p['effective_date']}  {p['action']:<25s}  "
                  f"会员数影响={p['volume_impact_pct']:+.0%}")

    def show_cd():
        for c in competitive:
            chg = c['outcome_market_share_change']
            print(f"    {c['competitor_name']:<15s} → {c['event_description']}")
            print(f"    {'':15s}   结果: {c['outcome_description']} ({chg:+.1%})")

    def show_peers():
        for name in ["Walmart", "Target", "Sam's Club"]:
            rows = [p for p in peer_data if p["peer_name"] == name]
            gm = next((r["value"] for r in rows if r["metric"] == "gross_margin"), None)
            om = next((r["value"] for r in rows if r["metric"] == "operating_margin"), None)
            print(f"    {name:<15s} 毛利率={gm:.1%}  营业利润率={om:.1%}")
        print(f"    {'Costco':<15s} 毛利率={0.130:.1%}  营业利润率={0.035:.1%}")

    show_io("Costco", "预期: 效率型成本优势 + 会员制转换成本", ctx, [
        ("financial_line_items (关键)", show_fli),
        ("downstream_segments", show_ds),
        ("pricing_actions (会员费提价)", show_pa),
        ("competitive_dynamics", show_cd),
        ("peer_financials (同行对比)", show_peers),
    ])


# ══════════════════════════════════════════════════════════════
#  紫金矿业
# ══════════════════════════════════════════════════════════════

def zijin():
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
        "basic_weighted_average_shares": 26_000,
        "income_tax_expense_total": 80, "income_before_tax_total": 400,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 200,
    }

    downstream = [
        {"customer_name": "铜产品", "revenue_pct": 0.45, "is_recurring": False, "revenue_type": "commodity"},
        {"customer_name": "金产品", "revenue_pct": 0.35, "is_recurring": False, "revenue_type": "commodity"},
        {"customer_name": "锌及其他", "revenue_pct": 0.20, "is_recurring": False, "revenue_type": "commodity"},
    ]

    competitive = [
        {"competitor_name": "Freeport-McMoRan", "event_type": "industry_downturn",
         "event_description": "2015-16 铜价暴跌 40%，全球铜矿行业亏损",
         "outcome_description": "紫金仍盈利，逆势收购刚果 Kamoa 铜矿",
         "outcome_market_share_change": 0.02},
        {"competitor_name": "嘉能可", "event_type": "industry_downturn",
         "event_description": "2015 嘉能可债务危机，被迫卖资产",
         "outcome_description": "紫金趁低价收购多个矿权，扩张产能",
         "outcome_market_share_change": 0.03},
        {"competitor_name": "多家中小矿企", "event_type": "industry_downturn",
         "event_description": "铜价低谷期，中小矿企停产或破产",
         "outcome_description": "紫金凭借低成本持续盈利运营",
         "outcome_market_share_change": 0.01},
    ]

    peer_data = [
        {"peer_name": "Freeport-McMoRan", "metric": "gross_margin", "value": 0.15, "period": "FY2025"},
        {"peer_name": "Freeport-McMoRan", "metric": "operating_margin", "value": 0.08, "period": "FY2025"},
        {"peer_name": "Freeport-McMoRan", "metric": "net_margin", "value": 0.05, "period": "FY2025"},
        {"peer_name": "洛阳钼业", "metric": "gross_margin", "value": 0.12, "period": "FY2025"},
        {"peer_name": "洛阳钼业", "metric": "operating_margin", "value": 0.07, "period": "FY2025"},
        {"peer_name": "洛阳钼业", "metric": "net_margin", "value": 0.04, "period": "FY2025"},
        {"peer_name": "江西铜业", "metric": "gross_margin", "value": 0.05, "period": "FY2025"},
        {"peer_name": "江西铜业", "metric": "operating_margin", "value": 0.03, "period": "FY2025"},
        {"peer_name": "江西铜业", "metric": "net_margin", "value": 0.02, "period": "FY2025"},
    ]

    ctx = ComputeContext(company_id=4, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli), "financial_line_items_all": _fli(fli),
        "downstream_segments": _df(downstream, DS),
        "pricing_actions": EMPTY,
        "market_share_data": EMPTY,
        "competitive_dynamics": _df(competitive, CD),
        "peer_financials": pd.DataFrame(peer_data),
        **EMPTY_TABLES,
    }

    def show_fli():
        keys = ["revenue", "cost_of_revenue", "operating_income", "net_income"]
        for k in keys:
            print(f"    {k:<25s} {fli[k]:>12,.0f}")
        gm = (fli["revenue"] - fli["cost_of_revenue"]) / fli["revenue"]
        om = fli["operating_income"] / fli["revenue"]
        print(f"    {'gross_margin':<25s} {gm:>12.1%}")
        print(f"    {'operating_margin':<25s} {om:>12.1%}")

    def show_ds():
        for s in downstream:
            print(f"    {s['customer_name']:<10s} {s['revenue_pct']:>5.0%}  {s['revenue_type']}")

    def show_cd():
        for c in competitive:
            print(f"    {c['competitor_name']:<20s}")
            print(f"      事件: {c['event_description']}")
            print(f"      结果: {c['outcome_description']}")

    def show_peers():
        for name in ["Freeport-McMoRan", "洛阳钼业", "江西铜业"]:
            rows = [p for p in peer_data if p["peer_name"] == name]
            gm = next((r["value"] for r in rows if r["metric"] == "gross_margin"), None)
            om = next((r["value"] for r in rows if r["metric"] == "operating_margin"), None)
            print(f"    {name:<20s} 毛利率={gm:.1%}  营业利润率={om:.1%}")
        print(f"    {'紫金矿业':<20s} 毛利率={0.200:.1%}  营业利润率={0.133:.1%}")

    show_io("紫金矿业", "预期: 成本优势(低谷存活+同行领先) + 矿权(缺牌照数据)", ctx, [
        ("financial_line_items (关键)", show_fli),
        ("downstream_segments", show_ds),
        ("competitive_dynamics (行业低谷)", show_cd),
        ("peer_financials (同行对比)", show_peers),
    ])


def tsmc():
    # 近似 FY2024 (百万美元)
    fli = {
        "revenue": 90_000, "cost_of_revenue": 40_500,
        "operating_income": 36_000, "net_income": 33_000,
        "operating_cash_flow": 45_000, "capital_expenditures": 30_000,
        "depreciation_amortization": 18_000,
        "shareholders_equity": 100_000, "total_assets": 180_000,
        "interest_expense": 500, "current_assets": 60_000,
        "current_liabilities": 30_000, "goodwill": 0,
        "accounts_receivable": 10_000, "inventory": 12_000,
        "cash_and_equivalents": 30_000, "total_debt": 25_000,
        "dividends_paid": -15_000, "share_repurchase": 0,
        "sga_expense": 3_000, "rnd_expense": 6_500,
        "basic_weighted_average_shares": 25_900,
        "income_tax_expense_total": 5_000, "income_before_tax_total": 38_000,
        "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 5_000,
    }

    downstream = [
        {"customer_name": "Apple", "revenue_pct": 0.25, "is_recurring": True,
         "revenue_type": "license", "contract_duration": "3 years"},
        {"customer_name": "NVIDIA", "revenue_pct": 0.12, "is_recurring": True,
         "revenue_type": "license", "contract_duration": "2 years"},
        {"customer_name": "AMD", "revenue_pct": 0.08, "is_recurring": True,
         "revenue_type": "license", "contract_duration": "2 years"},
        {"customer_name": "Qualcomm", "revenue_pct": 0.07, "is_recurring": True,
         "revenue_type": "license", "contract_duration": "2 years"},
        {"customer_name": "其他", "revenue_pct": 0.48, "is_recurring": True,
         "revenue_type": "license"},
    ]

    competitive = [
        {"competitor_name": "Intel Foundry", "event_type": "new_entry",
         "event_description": "Intel 投入 200 亿美元建代工厂，争取台积电客户",
         "outcome_description": "良率远低于台积电，Apple/NVIDIA 未转单，失败",
         "outcome_market_share_change": 0.0},
        {"competitor_name": "Samsung Foundry", "event_type": "product_launch",
         "event_description": "Samsung 3nm GAA 工艺上线，争取高端订单",
         "outcome_description": "良率问题导致 Qualcomm 部分订单回流台积电",
         "outcome_market_share_change": 0.02},
        {"competitor_name": "Apple 自研", "event_type": "product_launch",
         "event_description": "Apple 曾评估 Samsung 和 Intel 作为备选代工",
         "outcome_description": "最终仍全部留在台积电，无法替代",
         "outcome_market_share_change": 0.0},
    ]

    pricing = [
        {"action": "先进制程涨价 6%", "price_change_pct": 0.06,
         "product_or_segment": "5nm/3nm 代工", "effective_date": "2023-01",
         "volume_impact_pct": 0.10},
        {"action": "先进制程再涨价 5%", "price_change_pct": 0.05,
         "product_or_segment": "3nm/2nm 代工", "effective_date": "2024-01",
         "volume_impact_pct": 0.15},
    ]

    market_share = [
        {"period": "FY2023", "share": 0.59, "source": "全球代工市场"},
        {"period": "FY2024", "share": 0.61, "source": "全球代工市场"},
        {"period": "FY2025", "share": 0.64, "source": "全球代工市场"},
    ]

    peer_data = [
        {"peer_name": "Samsung Foundry", "metric": "gross_margin", "value": 0.15, "period": "FY2025"},
        {"peer_name": "Samsung Foundry", "metric": "operating_margin", "value": 0.03, "period": "FY2025"},
        {"peer_name": "Samsung Foundry", "metric": "net_margin", "value": 0.01, "period": "FY2025"},
        {"peer_name": "GlobalFoundries", "metric": "gross_margin", "value": 0.25, "period": "FY2025"},
        {"peer_name": "GlobalFoundries", "metric": "operating_margin", "value": 0.12, "period": "FY2025"},
        {"peer_name": "GlobalFoundries", "metric": "net_margin", "value": 0.09, "period": "FY2025"},
        {"peer_name": "Intel Foundry", "metric": "gross_margin", "value": -0.10, "period": "FY2025"},
        {"peer_name": "Intel Foundry", "metric": "operating_margin", "value": -0.30, "period": "FY2025"},
        {"peer_name": "Intel Foundry", "metric": "net_margin", "value": -0.35, "period": "FY2025"},
    ]

    ctx = ComputeContext(company_id=5, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli), "financial_line_items_all": _fli(fli),
        "downstream_segments": _df(downstream, DS),
        "pricing_actions": _df(pricing, {"company_id": 5, "raw_post_id": None, "created_at": "2025-01-01"}),
        "market_share_data": _df(market_share, MS),
        "competitive_dynamics": _df(competitive, CD),
        "peer_financials": pd.DataFrame(peer_data),
        **EMPTY_TABLES,
    }

    def show_fli():
        keys = ["revenue", "cost_of_revenue", "operating_income", "net_income",
                "capital_expenditures", "rnd_expense"]
        for k in keys:
            print(f"    {k:<25s} {fli[k]:>12,.0f}")
        gm = (fli["revenue"] - fli["cost_of_revenue"]) / fli["revenue"]
        om = fli["operating_income"] / fli["revenue"]
        print(f"    {'gross_margin':<25s} {gm:>12.1%}")
        print(f"    {'operating_margin':<25s} {om:>12.1%}")

    def show_ds():
        for s in downstream:
            dur = s.get("contract_duration", "")
            print(f"    {s['customer_name']:<12s} {s['revenue_pct']:>5.0%}  {dur}")

    def show_pa():
        for p in pricing:
            print(f"    {p['effective_date']}  {p['action']:<30s}  订单量={p['volume_impact_pct']:+.0%}")

    def show_cd():
        for c in competitive:
            chg = c['outcome_market_share_change']
            print(f"    {c['competitor_name']:<18s} → {c['event_description']}")
            print(f"    {'':18s}   结果: {c['outcome_description']} ({chg:+.0%})")

    def show_peers():
        for name in ["Samsung Foundry", "GlobalFoundries", "Intel Foundry"]:
            rows = [p for p in peer_data if p["peer_name"] == name]
            gm = next((r["value"] for r in rows if r["metric"] == "gross_margin"), None)
            om = next((r["value"] for r in rows if r["metric"] == "operating_margin"), None)
            print(f"    {name:<20s} 毛利率={gm:>6.1%}  营业利润率={om:>6.1%}")
        print(f"    {'台积电':<20s} 毛利率={0.55:>6.1%}  营业利润率={0.40:>6.1%}")

    def show_ms():
        for m in market_share:
            print(f"    {m['period']}  {m['share']:.0%}")

    show_io("台积电 (TSMC)", "预期: 多层护城河叠加", ctx, [
        ("financial_line_items (关键)", show_fli),
        ("downstream_segments (客户)", show_ds),
        ("pricing_actions (代工涨价)", show_pa),
        ("competitive_dynamics (竞品进攻)", show_cd),
        ("peer_financials (同行对比)", show_peers),
        ("market_share_data (代工市占率)", show_ms),
    ])


if __name__ == "__main__":
    tencent()
    douyin()
    costco()
    zijin()
    tsmc()
