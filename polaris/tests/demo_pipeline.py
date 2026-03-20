"""
Polaris 全管线 Mock Demo
========================
每一步打印 INPUT → OUTPUT，不依赖任何外部 DB。

管线：
  Step 1: Mock Anchor 数据 → ComputeContext
  Step 2: ComputeContext → Feature 向量 (L0 → L1 → L2)
  Step 3: Feature 向量 → 三流派评分 (Buffett / Dalio / Soros)
  Step 4: Feature + 市场数据 → DCF 内在价值 + 反向 DCF
  Step 5: 因果图构建 + 查询
  Step 6: 格式化报告输出
"""

import json
import pandas as pd

# ── 触发特征/规则注册 ─────────────────────────────────────────
import polaris.features.l0.company  # noqa: F401
import polaris.features.l0.cross_period  # noqa: F401

from polaris.features.types import ComputeContext, FeatureLevel, FeatureResult
from polaris.features.registry import get_features
from polaris.principles.pipeline import run_pipeline, evaluate_school, format_decision
from polaris.principles.dimensions import School
from polaris.principles.engines.dcf import (
    compute_intrinsic_value, reverse_dcf, forward_dcf,
)
from polaris.causal.graph import CausalGraph, Variable, Link


# ══════════════════════════════════════════════════════════════
#  工具函数
# ══════════════════════════════════════════════════════════════

def banner(title: str):
    print(f"\n{'═' * 70}")
    print(f"  {title}")
    print(f"{'═' * 70}")


def section(title: str):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def show_dict(d: dict, indent: int = 4, max_items: int = 999):
    prefix = " " * indent
    for i, (k, v) in enumerate(sorted(d.items())):
        if i >= max_items:
            print(f"{prefix}... ({len(d) - max_items} more)")
            break
        if isinstance(v, float):
            print(f"{prefix}{k}: {v:.4f}")
        else:
            print(f"{prefix}{k}: {v}")


# ══════════════════════════════════════════════════════════════
#  Mock 数据（复用 test_mock_pipeline 的工厂）
# ══════════════════════════════════════════════════════════════

def _fli(items: dict[str, float], period: str = "FY2025") -> pd.DataFrame:
    rows = [
        {"id": i, "statement_id": 1, "item_key": k, "item_label": k,
         "value": v, "parent_key": None, "ordinal": i, "note": None, "period": period}
        for i, (k, v) in enumerate(items.items())
    ]
    return pd.DataFrame(rows)


def _downstream(segments: list[dict]) -> pd.DataFrame:
    defaults = {
        "id": 0, "company_id": 1, "period": "FY2025", "segment": None,
        "customer_type": None, "products": None, "channels": None,
        "revenue": None, "growth_yoy": None, "backlog": None,
        "backlog_note": None, "pricing_model": None, "contract_duration": None,
        "recognition_method": None, "description": None,
        "raw_post_id": None, "created_at": "2025-01-01",
    }
    return pd.DataFrame([{**defaults, "id": i, **s} for i, s in enumerate(segments)])


def _upstream(suppliers: list[dict]) -> pd.DataFrame:
    defaults = {
        "id": 0, "company_id": 1, "period": "FY2025", "segment": None,
        "supply_type": "component", "material_or_service": None,
        "process_node": None, "purchase_obligation": None,
        "contract_type": None, "prepaid_amount": None,
        "concentration_risk": None, "description": None,
        "raw_post_id": None, "created_at": "2025-01-01",
    }
    return pd.DataFrame([{**defaults, "id": i, **s} for i, s in enumerate(suppliers)])


def _geo(regions: list[dict]) -> pd.DataFrame:
    defaults = {
        "id": 0, "company_id": 1, "period": "FY2025",
        "revenue": None, "growth_yoy": None, "note": None,
        "raw_post_id": None, "created_at": "2025-01-01",
    }
    return pd.DataFrame([{**defaults, "id": i, **r} for i, r in enumerate(regions)])


def _debt(obligations: list[dict]) -> pd.DataFrame:
    defaults = {
        "id": 0, "company_id": 1, "period": "FY2025",
        "instrument_name": "Note", "debt_type": "unsecured",
        "currency": "USD", "interest_rate": None,
        "maturity_date": None, "is_secured": False,
        "note": None, "raw_post_id": None, "created_at": "2025-01-01",
    }
    return pd.DataFrame([{**defaults, "id": i, **d} for i, d in enumerate(obligations)])


def _exec_comp(execs: list[dict]) -> pd.DataFrame:
    defaults = {
        "id": 0, "company_id": 1, "period": "FY2025",
        "role_type": "officer", "base_salary": None, "bonus": None,
        "option_awards": None, "non_equity_incentive": None,
        "other_comp": None, "currency": "USD",
        "median_employee_comp": None, "raw_post_id": None,
        "created_at": "2025-01-01",
    }
    return pd.DataFrame([{**defaults, "id": i, **e} for i, e in enumerate(execs)])


def _ownership(owners: list[dict]) -> pd.DataFrame:
    defaults = {
        "id": 0, "company_id": 1, "period": "FY2025",
        "shares_beneficially_owned": None,
        "raw_post_id": None, "created_at": "2025-01-01",
    }
    return pd.DataFrame([{**defaults, "id": i, **o} for i, o in enumerate(owners)])


def _narratives(items: list[dict]) -> pd.DataFrame:
    defaults = {
        "id": 0, "company_id": 1, "raw_post_id": None,
        "capital_required": None, "capital_unit": None,
        "promised_outcome": None, "deadline": None,
        "reported_at": None, "created_at": "2025-01-01",
    }
    return pd.DataFrame([{**defaults, "id": i, **n} for i, n in enumerate(items)])


def _empty_tables() -> dict[str, pd.DataFrame]:
    """所有可选表返回空 DataFrame。"""
    return {k: pd.DataFrame() for k in [
        "litigations", "operational_issues", "related_party_transactions",
        "non_financial_kpis", "pricing_actions", "market_share_data",
        "audit_opinions", "known_issues", "insider_transactions",
        "executive_changes", "equity_offerings", "analyst_estimates",
        "management_guidance", "management_acknowledgments",
    ]}


EMPTY = pd.DataFrame()


# ══════════════════════════════════════════════════════════════
#  STEP 1: 构造 Mock Anchor 数据
# ══════════════════════════════════════════════════════════════

def step1_build_context() -> ComputeContext:
    banner("STEP 1: Mock Anchor 数据 → ComputeContext")

    # --- INPUT: 模拟 NVDA 类似的 4 年财务数据 ---
    fli_by_period = {
        "FY2022": {
            "revenue": 28_000, "cost_of_revenue": 9_800,
            "operating_income": 12_000, "net_income": 9_500,
            "operating_cash_flow": 11_000, "capital_expenditures": 1_200,
            "depreciation_amortization": 1_000,
            "shareholders_equity": 40_000, "total_assets": 55_000,
            "interest_expense": 300, "current_assets": 25_000,
            "current_liabilities": 10_000, "goodwill": 3_000,
            "accounts_receivable": 4_000, "inventory": 2_000,
            "cash_and_equivalents": 12_000, "total_debt": 5_000,
            "dividends_paid": -2_000, "share_repurchase": -3_000,
            "sga_expense": 3_000, "rnd_expense": 4_000,
            "basic_weighted_average_shares": 1_000,
            "income_tax_expense_total": 2_500, "income_before_tax_total": 12_000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        "FY2023": {
            "revenue": 32_000, "cost_of_revenue": 11_200,
            "operating_income": 14_000, "net_income": 11_000,
            "operating_cash_flow": 13_000, "capital_expenditures": 1_400,
            "depreciation_amortization": 1_100,
            "shareholders_equity": 45_000, "total_assets": 60_000,
            "interest_expense": 280, "current_assets": 28_000,
            "current_liabilities": 11_000, "goodwill": 3_200,
            "accounts_receivable": 4_500, "inventory": 2_100,
            "cash_and_equivalents": 14_000, "total_debt": 4_500,
            "dividends_paid": -2_500, "share_repurchase": -3_500,
            "sga_expense": 3_200, "rnd_expense": 4_500,
            "basic_weighted_average_shares": 980,
            "income_tax_expense_total": 3_000, "income_before_tax_total": 14_000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        "FY2024": {
            "revenue": 37_000, "cost_of_revenue": 12_600,
            "operating_income": 17_000, "net_income": 13_500,
            "operating_cash_flow": 16_000, "capital_expenditures": 1_600,
            "depreciation_amortization": 1_200,
            "shareholders_equity": 52_000, "total_assets": 68_000,
            "interest_expense": 250, "current_assets": 32_000,
            "current_liabilities": 12_000, "goodwill": 3_300,
            "accounts_receivable": 5_000, "inventory": 2_200,
            "cash_and_equivalents": 17_000, "total_debt": 4_000,
            "dividends_paid": -3_000, "share_repurchase": -4_000,
            "sga_expense": 3_400, "rnd_expense": 5_000,
            "basic_weighted_average_shares": 960,
            "income_tax_expense_total": 3_500, "income_before_tax_total": 17_000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        "FY2025": {
            "revenue": 43_000, "cost_of_revenue": 14_200,
            "operating_income": 20_000, "net_income": 16_000,
            "operating_cash_flow": 19_000, "capital_expenditures": 1_800,
            "depreciation_amortization": 1_400,
            "shareholders_equity": 60_000, "total_assets": 78_000,
            "interest_expense": 220, "current_assets": 38_000,
            "current_liabilities": 13_000, "goodwill": 3_500,
            "accounts_receivable": 5_500, "inventory": 2_400,
            "cash_and_equivalents": 20_000, "total_debt": 3_500,
            "dividends_paid": -3_500, "share_repurchase": -5_000,
            "sga_expense": 3_600, "rnd_expense": 5_500,
            "basic_weighted_average_shares": 940,
            "income_tax_expense_total": 4_000, "income_before_tax_total": 20_000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
    }

    print("\n  INPUT: 4 年财务数据 (FY2022-FY2025)")
    for period, items in fli_by_period.items():
        rev = items["revenue"]
        ni = items["net_income"]
        ocf = items["operating_cash_flow"]
        print(f"    {period}: revenue={rev:,}  net_income={ni:,}  ocf={ocf:,}")

    # 合并多期
    all_fli = pd.concat(
        [_fli(items, period) for period, items in fli_by_period.items()],
        ignore_index=True,
    )
    all_debt = pd.concat([
        _debt([{"principal": 2000, "is_current": False, "interest_rate": 0.035, "period": p},
               {"principal": 1500, "is_current": True, "interest_rate": 0.03, "period": p}])
        for p in fli_by_period
    ], ignore_index=True)

    ctx = ComputeContext(company_id=1, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli_by_period["FY2025"]),
        "downstream_segments": _downstream([
            {"customer_name": "Enterprise A", "revenue_pct": 0.15, "is_recurring": True, "revenue_type": "license", "backlog": 5000},
            {"customer_name": "Enterprise B", "revenue_pct": 0.12, "is_recurring": True, "revenue_type": "subscription", "backlog": 3000},
            {"customer_name": "Enterprise C", "revenue_pct": 0.08, "is_recurring": True, "revenue_type": "subscription"},
            {"customer_name": "Government D", "revenue_pct": 0.06, "is_recurring": False, "revenue_type": "project"},
            {"customer_name": "Others", "revenue_pct": 0.59, "is_recurring": True, "revenue_type": "license"},
        ]),
        "upstream_segments": _upstream([
            {"supplier_name": "TSMC", "is_sole_source": True, "geographic_location": "Taiwan"},
            {"supplier_name": "Samsung", "is_sole_source": False, "geographic_location": "South Korea"},
            {"supplier_name": "Intel", "is_sole_source": False, "geographic_location": "United States"},
            {"supplier_name": "ASML", "is_sole_source": True, "geographic_location": "Netherlands"},
        ]),
        "geographic_revenues": _geo([
            {"region": "United States", "revenue_share": 0.45},
            {"region": "China", "revenue_share": 0.20},
            {"region": "Europe", "revenue_share": 0.18},
            {"region": "Japan", "revenue_share": 0.10},
            {"region": "Other", "revenue_share": 0.07},
        ]),
        "debt_obligations": _debt([
            {"principal": 2000, "is_current": False, "interest_rate": 0.035},
            {"principal": 1500, "is_current": True, "interest_rate": 0.03},
        ]),
        "executive_compensations": _exec_comp([
            {"name": "CEO", "title": "CEO", "role_type": "CEO", "pay_ratio": 120.0, "stock_awards": 8_000, "total_comp": 10_000},
            {"name": "CFO", "title": "CFO", "role_type": "officer", "pay_ratio": None, "stock_awards": 3_000, "total_comp": 4_500},
        ]),
        "stock_ownership": _ownership([
            {"name": "CEO", "title": "CEO", "percent_of_class": 8.0},
            {"name": "CFO", "title": "CFO", "percent_of_class": 2.0},
        ]),
        "company_narratives": _narratives([
            {"narrative": "Expand cloud", "status": "delivered"},
            {"narrative": "Enter new market", "status": "delivered"},
            {"narrative": "Reduce costs", "status": "delivered"},
            {"narrative": "Hire 1000 engineers", "status": "missed"},
            {"narrative": "Launch product X", "status": "delivered"},
        ]),
        "litigations": pd.DataFrame([
            {"id": 0, "company_id": 1, "status": "resolved", "accrued_amount": 50, "claimed_amount": 100,
             "case_name": "Case", "case_type": "civil", "counterparty": None, "filed_at": None,
             "currency": "USD", "description": None, "resolution": None, "resolved_at": None,
             "raw_post_id": None, "created_at": "2025-01-01"},
        ]),
        "operational_issues": pd.DataFrame([
            {"id": i, "company_id": 1, "period": "FY2025", "topic": t, "risk": r, "guidance": g,
             "raw_post_id": None, "performance": None, "attribution": None, "created_at": "2025-01-01"}
            for i, (t, r, g) in enumerate([
                ("Supply shortage", "moderate", "Improving"),
                ("Demand growth", None, "Strong"),
            ])
        ]),
        "related_party_transactions": pd.DataFrame([
            {"id": 0, "company_id": 1, "period": "FY2025", "related_party": "Board firm",
             "relationship": "director", "transaction_type": "consulting", "amount": 50,
             "is_ongoing": False, "currency": "USD", "terms": None, "description": None,
             "raw_post_id": None, "created_at": "2025-01-01"},
        ]),
        "non_financial_kpis": EMPTY,
        "financial_line_items_all": all_fli,
        "debt_obligations_all": all_debt,
        "pricing_actions": EMPTY,
        "market_share_data": EMPTY,
        "audit_opinions": EMPTY,
        "known_issues": EMPTY,
        "insider_transactions": EMPTY,
        "executive_changes": EMPTY,
        "equity_offerings": EMPTY,
        "analyst_estimates": EMPTY,
        "management_guidance": EMPTY,
        "management_acknowledgments": EMPTY,
    }

    print(f"\n  OUTPUT: ComputeContext(company_id=1, period='FY2025')")
    print(f"    缓存表数量: {len(ctx._cache)}")
    for k, v in sorted(ctx._cache.items()):
        rows = len(v) if not v.empty else 0
        print(f"    {k}: {rows} rows")

    return ctx


# ══════════════════════════════════════════════════════════════
#  STEP 2: 特征计算 L0 → L1 → L2
# ══════════════════════════════════════════════════════════════

def step2_compute_features(ctx: ComputeContext) -> dict[str, float]:
    banner("STEP 2: ComputeContext → Feature 向量")

    print(f"\n  INPUT: ComputeContext(company_id={ctx.company_id}, period='{ctx.period}')")

    # 统计注册特征
    registered = {}
    for level in (FeatureLevel.L0, FeatureLevel.L1, FeatureLevel.L2):
        feats = get_features(level=level)
        registered[level.value] = len(feats)
    print(f"  已注册特征: L0={registered['l0']}, L1={registered['l1']}, L2={registered['l2']}")

    # 计算
    results: dict[str, FeatureResult] = {}
    failed = []
    for level in (FeatureLevel.L0, FeatureLevel.L1, FeatureLevel.L2):
        for feat in get_features(level=level):
            try:
                result = feat.compute_fn(ctx)
            except Exception as e:
                failed.append((feat.name, str(e)[:60]))
                continue
            if result is not None:
                ctx.features[feat.name] = result.value
                results[feat.name] = result

    features = {n: r.value for n, r in results.items()}

    section("OUTPUT: Feature 向量")
    print(f"  成功: {len(features)} 个特征")
    if failed:
        print(f"  失败: {len(failed)} 个")
        for name, err in failed[:5]:
            print(f"    ✗ {name}: {err}")

    # 按分类展示
    categories = {}
    for name, val in sorted(features.items()):
        parts = name.split(".")
        cat = parts[-1].split("_")[0] if len(parts) > 2 else "other"
        categories.setdefault(cat, []).append((name, val))

    print(f"\n  全部特征值:")
    for name, val in sorted(features.items()):
        print(f"    {name}: {val:.4f}")

    return features


# ══════════════════════════════════════════════════════════════
#  STEP 3: 三流派评分
# ══════════════════════════════════════════════════════════════

def step3_evaluate_schools(features: dict[str, float]):
    banner("STEP 3: Feature 向量 → 三流派独立评分")

    print(f"\n  INPUT: {len(features)} 个特征")
    print(f"  关键特征:")
    key_feats = [
        "l0.company.gross_margin", "l0.company.net_margin",
        "l0.company.owner_earnings", "l0.company.debt_to_equity",
        "l0.company.ocf_to_net_income", "l0.company.interest_coverage",
        "l0.company.narrative_fulfillment_rate",
        "l0.company.financing_dependency",
    ]
    for name in key_feats:
        val = features.get(name)
        if val is not None:
            print(f"    {name}: {val:.4f}")
        else:
            print(f"    {name}: (无)")

    section("OUTPUT: 巴菲特评分")
    b = evaluate_school(School.BUFFETT, features)
    print(f"  得分: {b.score:.1f}/10  信号: {b.signal}  原始分: {b.raw_points:.1f}")
    print(f"  Top 驱动因子:")
    for d in b.drivers:
        print(f"    {d.contribution:+.1f}  {d.rule_name}: {d.description}")

    section("OUTPUT: 达利欧评分")
    d = evaluate_school(School.DALIO, features)
    print(f"  得分: {d.score:.1f}/10  信号: {d.signal}  原始分: {d.raw_points:.1f}")
    print(f"  Top 驱动因子:")
    for dr in d.drivers:
        print(f"    {dr.contribution:+.1f}  {dr.rule_name}: {dr.description}")

    section("OUTPUT: 索罗斯评分")
    s = evaluate_school(School.SOROS, features)
    print(f"  得分: {s.score:.1f}/10  信号: {s.signal}  原始分: {s.raw_points:.1f}")
    print(f"  Top 驱动因子:")
    for dr in s.drivers:
        print(f"    {dr.contribution:+.1f}  {dr.rule_name}: {dr.description}")


# ══════════════════════════════════════════════════════════════
#  STEP 4: DCF 内在价值 + 反向 DCF
# ══════════════════════════════════════════════════════════════

def step4_dcf(features: dict[str, float]):
    banner("STEP 4: DCF 估值引擎")

    # Mock 市场数据
    mock_market = {
        "price": 135.0,
        "shares_outstanding": 940.0,
        "discount_rate": 0.045,  # 10Y Treasury = 4.5%
        "guidance": {
            "revenue_growth": 0.15,  # 管理层指引: 15% 增速
        },
    }

    section("INPUT: 正向 DCF (巴菲特)")
    oe = features.get("l0.company.owner_earnings", 0)
    print(f"  owner_earnings: {oe:,.0f}")
    print(f"  discount_rate: {mock_market['discount_rate']:.1%}")
    print(f"  shares_outstanding: {mock_market['shares_outstanding']:,.0f}")
    print(f"  guidance: {json.dumps(mock_market['guidance'])}")

    dcf = compute_intrinsic_value(
        features=features,
        guidance=mock_market["guidance"],
        discount_rate=mock_market["discount_rate"],
        shares_outstanding=mock_market["shares_outstanding"],
    )

    print(f"\n  OUTPUT:")
    print(f"    status: {dcf.status}")
    print(f"    valuation_path: {dcf.valuation_path}")
    if dcf.intrinsic_value is not None:
        print(f"    intrinsic_value: ${dcf.intrinsic_value:,.2f} / 股")
    if dcf.key_assumptions:
        print(f"    key_assumptions: {dcf.key_assumptions}")

    section("INPUT: 反向 DCF (索罗斯)")
    print(f"  current_price: ${mock_market['price']}")
    print(f"  owner_earnings: {oe:,.0f}")
    print(f"  discount_rate: {mock_market['discount_rate']:.1%}")

    rdcf = reverse_dcf(
        current_price=mock_market["price"],
        current_owner_earnings=oe,
        discount_rate=mock_market["discount_rate"],
        shares_outstanding=mock_market["shares_outstanding"],
    )

    print(f"\n  OUTPUT:")
    print(f"    status: {rdcf.status}")
    if rdcf.implied_growth_rate is not None:
        print(f"    implied_growth_rate: {rdcf.implied_growth_rate:.2%}")
        actual = features.get("l0.company.revenue_growth_yoy")
        if actual is not None:
            gap = rdcf.implied_growth_rate - actual
            print(f"    actual_growth_rate: {actual:.2%}")
            print(f"    expectation_gap: {gap:+.2%}")
            if gap > 0.05:
                print(f"    → 市场过度乐观，隐含增速远高于实际")
            elif gap < -0.05:
                print(f"    → 市场过度悲观，存在预期差机会")
            else:
                print(f"    → 预期差不大，市场定价合理")

    return mock_market


# ══════════════════════════════════════════════════════════════
#  STEP 5: 因果图（纯内存 mock）
# ══════════════════════════════════════════════════════════════

def step5_causal_graph():
    banner("STEP 5: 因果图构建与查询")

    # 手动构建图（不走 DB）
    g = CausalGraph()

    # Mock 变量
    variables = [
        Variable(1, "ai_chip_demand", "market", "AI 芯片需求", True),
        Variable(2, "datacenter_capex", "market", "数据中心资本支出", True),
        Variable(3, "gpu_revenue", "financial", "GPU 收入", True),
        Variable(4, "gross_margin", "financial", "毛利率", True),
        Variable(5, "rd_investment", "financial", "研发投入", True),
        Variable(6, "product_leadership", "competitive", "产品领先地位", False),
        Variable(7, "pricing_power", "competitive", "定价权", False),
        Variable(8, "stock_price", "market", "股价", True),
    ]
    for v in variables:
        g.variables[v.id] = v
        g.var_by_name[v.name] = v

    # Mock 因果链
    links = [
        Link(1, 1, 2, "AI 需求驱动数据中心投资", "strong", "1-2 quarters", None, 0.9),
        Link(2, 2, 3, "数据中心采购 GPU", "strong", "1 quarter", None, 0.85),
        Link(3, 3, 4, "规模效应提升毛利", "moderate", "same quarter", "需求持续增长", 0.7),
        Link(4, 4, 5, "高毛利支撑研发投入", "moderate", "1-2 quarters", None, 0.75),
        Link(5, 5, 6, "持续研发保持领先", "moderate", "2-4 quarters", None, 0.65),
        Link(6, 6, 7, "技术领先带来定价权", "strong", None, "无有力竞争者", 0.8),
        Link(7, 7, 3, "定价权推高收入（正向循环）", "moderate", "1 quarter", None, 0.7),
        Link(8, 3, 8, "收入增长推动股价", "strong", "immediate", None, 0.6),
    ]
    for l in links:
        g.links[l.id] = l
        g._outgoing.setdefault(l.cause_id, []).append(l)
        g._incoming.setdefault(l.effect_id, []).append(l)

    section("INPUT: 因果图")
    print(f"  变量: {len(g.variables)} 个")
    for v in variables:
        obs = "可观测" if v.observable else "不可观测"
        print(f"    [{v.domain}] {v.name} ({obs})")
    print(f"  因果链: {len(g.links)} 条")
    for l in links:
        cause = g.variables[l.cause_id].name
        effect = g.variables[l.effect_id].name
        print(f"    {cause} →[{l.mechanism}]→ {effect}  (conf={l.confidence:.0%})")

    section("OUTPUT: 下游传导 (从 ai_chip_demand 出发)")
    chains = g.downstream(1, max_depth=4)
    print(f"  找到 {len(chains)} 条传导路径:")
    for i, chain in enumerate(chains[:10]):
        desc = chain.describe(g.variables)
        print(f"    [{i+1}] {desc}")
        print(f"        最低置信度: {chain.min_confidence:.0%}")

    section("OUTPUT: 上游溯因 (stock_price 的原因)")
    chains = g.upstream(8, max_depth=4)
    print(f"  找到 {len(chains)} 条原因路径:")
    for i, chain in enumerate(chains[:10]):
        desc = chain.describe(g.variables)
        print(f"    [{i+1}] {desc}")
        print(f"        最低置信度: {chain.min_confidence:.0%}")

    # 矛盾检测
    contras = g.contradictions()
    print(f"\n  矛盾检测: {'无矛盾' if not contras else f'{len(contras)} 对矛盾'}")


# ══════════════════════════════════════════════════════════════
#  STEP 6: 完整管线（run_pipeline 一键跑通）
# ══════════════════════════════════════════════════════════════

def step6_full_pipeline(features: dict[str, float], mock_market: dict):
    banner("STEP 6: run_pipeline 完整管线 + 报告")

    section("INPUT")
    print(f"  company: GoodCorp (GOOD)")
    print(f"  period: FY2025")
    print(f"  features: {len(features)} 个")
    print(f"  market_context: price=${mock_market['price']}, "
          f"shares={mock_market['shares_outstanding']:,.0f}, "
          f"discount_rate={mock_market['discount_rate']:.1%}")

    result = run_pipeline(
        company_id=1,
        company_name="GoodCorp",
        ticker="GOOD",
        period="FY2025",
        features=features,
        market_context=mock_market,
    )

    section("OUTPUT: DecisionContext 结构")
    print(f"  buffett:")
    print(f"    score: {result.buffett.school_score.score:.1f}/10")
    print(f"    signal: {result.buffett.school_score.signal}")
    print(f"    filters_passed: {result.buffett.filters_passed}")
    print(f"    filter_details: {result.buffett.filter_details}")
    print(f"    valuation_status: {result.buffett.valuation_status}")
    if result.buffett.intrinsic_value:
        print(f"    intrinsic_value: ${result.buffett.intrinsic_value:,.2f}")
        print(f"    valuation_path: {result.buffett.valuation_path}")
    print(f"  dalio:")
    print(f"    score: {result.dalio.school_score.score:.1f}/10")
    print(f"    signal: {result.dalio.school_score.signal}")
    print(f"  soros:")
    print(f"    score: {result.soros.school_score.score:.1f}/10")
    print(f"    signal: {result.soros.school_score.signal}")
    print(f"    reflexivity_phase: {result.soros.reflexivity_phase}")
    if result.soros.expectation_gap is not None:
        print(f"    expectation_gap: {result.soros.expectation_gap:+.2%}")

    section("OUTPUT: 格式化报告")
    print(format_decision(result))


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "▓" * 70)
    print("  POLARIS 全管线 Mock Demo")
    print("  每一步: INPUT → 处理 → OUTPUT")
    print("▓" * 70)

    ctx = step1_build_context()
    features = step2_compute_features(ctx)
    step3_evaluate_schools(features)
    mock_market = step4_dcf(features)
    step5_causal_graph()
    step6_full_pipeline(features, mock_market)

    print("\n" + "▓" * 70)
    print("  Done. 全管线无 DB 依赖，所有数据均为 mock。")
    print("▓" * 70 + "\n")
