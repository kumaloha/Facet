"""
巴菲特流派逐步拆解
==================
每一步展开 INPUT → 处理逻辑 → OUTPUT，无遗漏。

Step 1: Anchor 原始数据 → get_item() 读取
Step 2: get_item() 值 → L0 特征计算（逐个）
Step 3: 特征向量 → Filter 逐条检查
Step 4: 特征向量 → Rule 逐条评分
Step 5: 原始分 → 归一化 → 信号
Step 6: 特征 + 市场数据 → DCF 内在价值
Step 7: 最终 BuffettResult 组装
"""

import json
import pandas as pd

import polaris.features.l0.company  # noqa: F401
import polaris.features.l0.cross_period  # noqa: F401
import polaris.principles.v1.buffett  # noqa: F401

from polaris.features.types import ComputeContext, FeatureLevel, FeatureResult
from polaris.features.registry import get_features
from polaris.principles.rules import get_rules
from polaris.principles.dimensions import School
from polaris.principles.pipeline import (
    BUFFETT_FILTER_THRESHOLDS,
    SCHOOL_RANGES,
    _normalize,
    _check_buffett_filters,
    _score_to_signal_buffett,
)
from polaris.principles.engines.dcf import compute_intrinsic_value, reverse_dcf


def banner(title: str):
    print(f"\n{'═' * 70}")
    print(f"  {title}")
    print(f"{'═' * 70}")


def section(title: str):
    print(f"\n  {'─' * 56}")
    print(f"  {title}")
    print(f"  {'─' * 56}")


# ══════════════════════════════════════════════════════════════
#  Mock 数据构造（精简版）
# ══════════════════════════════════════════════════════════════

def _fli(items: dict[str, float], period: str = "FY2025") -> pd.DataFrame:
    return pd.DataFrame([
        {"id": i, "statement_id": 1, "item_key": k, "item_label": k,
         "value": v, "parent_key": None, "ordinal": i, "note": None, "period": period}
        for i, (k, v) in enumerate(items.items())
    ])

def _simple_df(rows, defaults):
    return pd.DataFrame([{**defaults, "id": i, **r} for i, r in enumerate(rows)])

EMPTY = pd.DataFrame()

FLI_BY_PERIOD = {
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


def build_context() -> ComputeContext:
    all_fli = pd.concat(
        [_fli(items, period) for period, items in FLI_BY_PERIOD.items()],
        ignore_index=True,
    )
    ctx = ComputeContext(company_id=1, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(FLI_BY_PERIOD["FY2025"]),
        "financial_line_items_all": all_fli,
        "downstream_segments": _simple_df([
            {"customer_name": "Enterprise A", "revenue_pct": 0.15, "is_recurring": True, "revenue_type": "license", "backlog": 5000},
            {"customer_name": "Enterprise B", "revenue_pct": 0.12, "is_recurring": True, "revenue_type": "subscription", "backlog": 3000},
            {"customer_name": "Enterprise C", "revenue_pct": 0.08, "is_recurring": True, "revenue_type": "subscription"},
            {"customer_name": "Government D", "revenue_pct": 0.06, "is_recurring": False, "revenue_type": "project"},
            {"customer_name": "Others", "revenue_pct": 0.59, "is_recurring": True, "revenue_type": "license"},
        ], {"company_id": 1, "period": "FY2025", "segment": None, "customer_type": None,
            "products": None, "channels": None, "revenue": None, "growth_yoy": None,
            "backlog": None, "backlog_note": None, "pricing_model": None,
            "contract_duration": None, "recognition_method": None, "description": None,
            "raw_post_id": None, "created_at": "2025-01-01"}),
        "upstream_segments": _simple_df([
            {"supplier_name": "TSMC", "is_sole_source": True, "geographic_location": "Taiwan"},
            {"supplier_name": "Samsung", "is_sole_source": False, "geographic_location": "South Korea"},
            {"supplier_name": "Intel", "is_sole_source": False, "geographic_location": "United States"},
            {"supplier_name": "ASML", "is_sole_source": True, "geographic_location": "Netherlands"},
        ], {"company_id": 1, "period": "FY2025", "segment": None, "supply_type": "component",
            "material_or_service": None, "process_node": None, "purchase_obligation": None,
            "contract_type": None, "prepaid_amount": None, "concentration_risk": None,
            "description": None, "raw_post_id": None, "created_at": "2025-01-01"}),
        "geographic_revenues": _simple_df([
            {"region": "United States", "revenue_share": 0.45},
            {"region": "China", "revenue_share": 0.20},
            {"region": "Europe", "revenue_share": 0.18},
            {"region": "Japan", "revenue_share": 0.10},
            {"region": "Other", "revenue_share": 0.07},
        ], {"company_id": 1, "period": "FY2025", "revenue": None, "growth_yoy": None,
            "note": None, "raw_post_id": None, "created_at": "2025-01-01"}),
        "debt_obligations": _simple_df([
            {"principal": 2000, "is_current": False, "interest_rate": 0.035},
            {"principal": 1500, "is_current": True, "interest_rate": 0.03},
        ], {"company_id": 1, "period": "FY2025", "instrument_name": "Note",
            "debt_type": "unsecured", "currency": "USD", "maturity_date": None,
            "is_secured": False, "note": None, "raw_post_id": None, "created_at": "2025-01-01"}),
        "debt_obligations_all": pd.concat([
            _simple_df([
                {"principal": 2000, "is_current": False, "interest_rate": 0.035, "period": p},
                {"principal": 1500, "is_current": True, "interest_rate": 0.03, "period": p},
            ], {"company_id": 1, "instrument_name": "Note", "debt_type": "unsecured",
                "currency": "USD", "maturity_date": None, "is_secured": False,
                "note": None, "raw_post_id": None, "created_at": "2025-01-01"})
            for p in FLI_BY_PERIOD
        ], ignore_index=True),
        "executive_compensations": _simple_df([
            {"name": "CEO", "title": "CEO", "role_type": "CEO", "pay_ratio": 120.0, "stock_awards": 8_000, "total_comp": 10_000},
            {"name": "CFO", "title": "CFO", "role_type": "officer", "pay_ratio": None, "stock_awards": 3_000, "total_comp": 4_500},
        ], {"company_id": 1, "period": "FY2025", "base_salary": None, "bonus": None,
            "option_awards": None, "non_equity_incentive": None, "other_comp": None,
            "currency": "USD", "median_employee_comp": None, "raw_post_id": None,
            "created_at": "2025-01-01"}),
        "stock_ownership": _simple_df([
            {"name": "CEO", "title": "CEO", "percent_of_class": 8.0},
            {"name": "CFO", "title": "CFO", "percent_of_class": 2.0},
        ], {"company_id": 1, "period": "FY2025", "shares_beneficially_owned": None,
            "raw_post_id": None, "created_at": "2025-01-01"}),
        "company_narratives": _simple_df([
            {"narrative": "Expand cloud", "status": "delivered"},
            {"narrative": "Enter new market", "status": "delivered"},
            {"narrative": "Reduce costs", "status": "delivered"},
            {"narrative": "Hire 1000 engineers", "status": "missed"},
            {"narrative": "Launch product X", "status": "delivered"},
        ], {"company_id": 1, "raw_post_id": None, "capital_required": None,
            "capital_unit": None, "promised_outcome": None, "deadline": None,
            "reported_at": None, "created_at": "2025-01-01"}),
        "litigations": _simple_df([
            {"status": "resolved", "accrued_amount": 50, "claimed_amount": 100,
             "case_name": "Case", "case_type": "civil"},
        ], {"company_id": 1, "counterparty": None, "filed_at": None, "currency": "USD",
            "description": None, "resolution": None, "resolved_at": None,
            "raw_post_id": None, "created_at": "2025-01-01"}),
        "operational_issues": _simple_df([
            {"topic": "Supply shortage", "risk": "moderate", "guidance": "Improving"},
            {"topic": "Demand growth", "risk": None, "guidance": "Strong"},
        ], {"company_id": 1, "period": "FY2025", "raw_post_id": None,
            "performance": None, "attribution": None, "created_at": "2025-01-01"}),
        "related_party_transactions": _simple_df([
            {"related_party": "Board firm", "relationship": "director",
             "transaction_type": "consulting", "amount": 50, "is_ongoing": False},
        ], {"company_id": 1, "period": "FY2025", "currency": "USD", "terms": None,
            "description": None, "raw_post_id": None, "created_at": "2025-01-01"}),
        "non_financial_kpis": EMPTY, "pricing_actions": EMPTY,
        "market_share_data": EMPTY, "audit_opinions": EMPTY,
        "known_issues": EMPTY, "insider_transactions": EMPTY,
        "executive_changes": EMPTY, "equity_offerings": EMPTY,
        "analyst_estimates": EMPTY, "management_guidance": EMPTY,
        "management_acknowledgments": EMPTY,
    }
    return ctx


# ══════════════════════════════════════════════════════════════
#  STEP 1: Anchor 原始数据
# ══════════════════════════════════════════════════════════════

def step1_raw_data():
    banner("STEP 1: Anchor 原始数据 (financial_line_items)")

    print("\n  当期 FY2025:")
    fli = FLI_BY_PERIOD["FY2025"]
    for k, v in fli.items():
        print(f"    {k:<45s} = {v:>12,.0f}")

    print(f"\n  历史数据窗口: {list(FLI_BY_PERIOD.keys())}")
    print("  关键科目跨期对比:")
    keys = ["revenue", "net_income", "operating_cash_flow", "capital_expenditures", "total_debt"]
    header = f"    {'':30s}" + "".join(f"{p:>10s}" for p in FLI_BY_PERIOD)
    print(header)
    for k in keys:
        vals = "".join(f"{FLI_BY_PERIOD[p][k]:>10,.0f}" for p in FLI_BY_PERIOD)
        print(f"    {k:30s}{vals}")


# ══════════════════════════════════════════════════════════════
#  STEP 2: 特征计算（逐个）
# ══════════════════════════════════════════════════════════════

def step2_compute_features(ctx: ComputeContext) -> dict[str, float]:
    banner("STEP 2: L0 特征计算（逐个）")

    # 只跑巴菲特相关的特征（domain=company, level=L0）
    results: dict[str, FeatureResult] = {}
    failed: list[tuple[str, str]] = []
    skipped: list[str] = []

    for level in (FeatureLevel.L0, FeatureLevel.L1, FeatureLevel.L2):
        for feat in get_features(level=level):
            try:
                result = feat.compute_fn(ctx)
            except Exception as e:
                failed.append((feat.name, str(e)[:80]))
                continue
            if result is None:
                skipped.append(feat.name)
                continue
            ctx.features[feat.name] = result.value
            results[feat.name] = result

    features = {n: r.value for n, r in results.items()}

    # 按巴菲特维度分组展示
    buffett_groups = {
        "商业模式": [
            "recurring_revenue_pct", "top_customer_concentration",
            "top3_customer_concentration", "backlog_coverage",
            "revenue_type_diversity",
        ],
        "护城河": [
            "gross_margin", "operating_margin", "incremental_roic",
            "gross_margin_stability", "gross_margin_delta",
            "consecutive_margin_expansion",
        ],
        "所有者盈余": [
            "owner_earnings", "owner_earnings_margin",
            "owner_earnings_to_net_income", "capex_to_revenue",
            "depreciation_to_capex", "maintenance_capex_ratio",
        ],
        "盈利质量": [
            "ocf_to_net_income", "accruals_ratio",
            "receivables_growth_vs_revenue", "inventory_growth_vs_revenue",
            "net_margin",
        ],
        "资本配置": [
            "shareholder_yield", "dividend_payout_ratio",
            "buyback_to_net_income", "goodwill_to_assets",
            "goodwill_growth_vs_revenue_growth",
        ],
        "管理层": [
            "narrative_fulfillment_rate", "narrative_count",
            "mgmt_ownership_pct", "ceo_pay_ratio",
            "exec_stock_award_pct", "litigation_count",
            "related_party_amount_to_revenue",
        ],
        "可预测性": [
            "consecutive_revenue_growth", "consecutive_positive_fcf",
            "revenue_growth_yoy", "owner_earnings_growth_yoy",
            "net_margin_stability", "roe_stability", "roe",
        ],
        "财务安全": [
            "debt_to_equity", "interest_coverage", "current_ratio",
            "debt_to_owner_earnings", "cash_to_short_term_debt",
            "net_debt_to_ebitda",
        ],
    }

    for group_name, keys in buffett_groups.items():
        section(f"{group_name}")
        for short_key in keys:
            full_key = f"l0.company.{short_key}"
            val = features.get(full_key)
            if val is not None:
                # 显示计算公式提示
                detail = results[full_key].detail or ""
                if detail:
                    detail = f"  ({detail})"
                print(f"    {short_key:<42s} = {val:>12.4f}{detail}")
            else:
                print(f"    {short_key:<42s} = {'(未算出)':>12s}")

    if failed:
        section("计算失败")
        for name, err in failed:
            print(f"    {name}: {err}")

    if skipped:
        section("返回 None（数据不足）")
        for name in skipped:
            print(f"    {name}")

    print(f"\n  总计: 成功 {len(features)}, 失败 {len(failed)}, 跳过 {len(skipped)}")
    return features


# ══════════════════════════════════════════════════════════════
#  STEP 3: Filter 逐条检查
# ══════════════════════════════════════════════════════════════

def step3_filters(features: dict[str, float]):
    banner("STEP 3: 巴菲特过滤器（逐条）")

    print("\n  过滤逻辑：全部维度通过 → 可进入 DCF 估值")
    print("  任一维度失败 → 不估值，信号降级\n")

    all_passed = True
    for dim_name, checks in BUFFETT_FILTER_THRESHOLDS.items():
        dim_passed = True
        print(f"  [{dim_name}]")
        for feat_name, (op, threshold) in checks.items():
            val = features.get(feat_name)
            if val is None:
                print(f"    {feat_name}")
                print(f"      值: (无)  阈值: {op} {threshold}  → 跳过（数据不足）")
                continue

            if op == "<=" and val > threshold:
                passed = False
            elif op == ">=" and val < threshold:
                passed = False
            else:
                passed = True

            mark = "PASS" if passed else "FAIL"
            print(f"    {feat_name}")
            print(f"      值: {val:.4f}  {op} {threshold}  → {mark}")
            if not passed:
                dim_passed = False

        status = "PASS" if dim_passed else "FAIL"
        print(f"    → 维度结果: {status}\n")
        if not dim_passed:
            all_passed = False

    print(f"  ══ 过滤总结果: {'全部通过 → 可估值' if all_passed else '未通过 → 不估值'} ══")
    return all_passed


# ══════════════════════════════════════════════════════════════
#  STEP 4: Rule 逐条评分
# ══════════════════════════════════════════════════════════════

def step4_rules(features: dict[str, float]) -> tuple[float, float]:
    banner("STEP 4: 巴菲特规则评分（逐条）")

    rules = get_rules(School.BUFFETT)
    raw_total = 0.0
    active_count = 0

    print(f"\n  共 {len(rules)} 条规则\n")
    print(f"  {'规则名':<35s} {'分数':>6s}  说明")
    print(f"  {'─' * 35} {'─' * 6}  {'─' * 40}")

    for r in rules:
        try:
            pts = r.evaluate_fn(features)
        except Exception:
            pts = 0.0

        raw_total += pts

        if pts != 0:
            active_count += 1
            print(f"  {r.name:<35s} {pts:>+6.1f}  {r.description}")
        else:
            print(f"  {r.name:<35s} {'0':>6s}  {r.description}")

    print(f"\n  原始分合计: {raw_total:+.1f}  (触发 {active_count}/{len(rules)} 条规则)")

    # 归一化
    min_raw, max_raw = SCHOOL_RANGES[School.BUFFETT]
    score = _normalize(raw_total, min_raw, max_raw)
    signal = _score_to_signal_buffett(score)

    section("归一化")
    print(f"    原始分: {raw_total:+.1f}")
    print(f"    范围: [{min_raw}, {max_raw}]")
    print(f"    公式: 1 + 9 × ({raw_total:.1f} - ({min_raw})) / ({max_raw} - ({min_raw}))")
    print(f"    归一化得分: {score:.1f} / 10")
    print(f"    信号: {signal}")

    return raw_total, score


# ══════════════════════════════════════════════════════════════
#  STEP 5: DCF 估值
# ══════════════════════════════════════════════════════════════

def step5_dcf(features: dict[str, float], filters_passed: bool):
    banner("STEP 5: DCF 内在价值")

    mock_market = {
        "price": 135.0,
        "shares_outstanding": 940.0,
        "discount_rate": 0.045,
        "guidance": {"revenue_growth": 0.15},
    }

    if not filters_passed:
        print("\n  过滤未通过 → 跳过 DCF")
        print("  valuation_status = 'unvaluable'")
        return mock_market

    oe = features.get("l0.company.owner_earnings")
    dr = mock_market["discount_rate"]
    shares = mock_market["shares_outstanding"]
    guidance = mock_market["guidance"]

    section("INPUT")
    print(f"    owner_earnings       = {oe:,.0f}")
    print(f"    discount_rate        = {dr:.1%}  (10Y Treasury)")
    print(f"    shares_outstanding   = {shares:,.0f}")
    print(f"    guidance             = {json.dumps(guidance)}")

    section("路径选择")
    print(f"    路径 A (capex+ROIC): guidance 无 capex → 跳过")
    print(f"    路径 B (revenue_growth): guidance 有 revenue_growth=0.15 → 命中")

    dcf = compute_intrinsic_value(
        features=features,
        guidance=guidance,
        discount_rate=dr,
        shares_outstanding=shares,
    )

    section("OUTPUT: 正向 DCF")
    print(f"    status           = {dcf.status}")
    print(f"    valuation_path   = {dcf.valuation_path}")
    if dcf.intrinsic_value is not None:
        print(f"    intrinsic_value  = ${dcf.intrinsic_value:,.2f} / 股")
    print(f"    key_assumptions  = {dcf.key_assumptions}")

    # 反向 DCF
    section("INPUT: 反向 DCF")
    print(f"    current_price    = ${mock_market['price']}")
    print(f"    owner_earnings   = {oe:,.0f}")
    print(f"    discount_rate    = {dr:.1%}")
    print(f"    shares           = {shares:,.0f}")

    rdcf = reverse_dcf(
        current_price=mock_market["price"],
        current_owner_earnings=oe,
        discount_rate=dr,
        shares_outstanding=shares,
    )

    section("OUTPUT: 反向 DCF")
    print(f"    status                = {rdcf.status}")
    if rdcf.implied_growth_rate is not None:
        print(f"    implied_growth_rate   = {rdcf.implied_growth_rate:.2%}")
        actual = features.get("l0.company.revenue_growth_yoy")
        if actual is not None:
            gap = rdcf.implied_growth_rate - actual
            print(f"    actual_growth_rate    = {actual:.2%}")
            print(f"    expectation_gap       = {gap:+.2%}")

    # 安全边际
    if dcf.intrinsic_value is not None:
        margin_of_safety = (dcf.intrinsic_value - mock_market["price"]) / dcf.intrinsic_value
        section("安全边际")
        print(f"    内在价值  = ${dcf.intrinsic_value:,.2f}")
        print(f"    当前股价  = ${mock_market['price']:,.2f}")
        print(f"    安全边际  = {margin_of_safety:.1%}")
        if margin_of_safety > 0.25:
            print(f"    → 安全边际充足 (> 25%)")
        elif margin_of_safety > 0:
            print(f"    → 有一定安全边际")
        else:
            print(f"    → 无安全边际，股价高于内在价值")

    return mock_market


# ══════════════════════════════════════════════════════════════
#  STEP 6: 最终组装
# ══════════════════════════════════════════════════════════════

def step6_assemble(features, filters_passed, raw_total, score):
    banner("STEP 6: BuffettResult 最终组装")

    from polaris.principles.pipeline import run_pipeline

    mock_market = {
        "price": 135.0,
        "shares_outstanding": 940.0,
        "discount_rate": 0.045,
        "guidance": {"revenue_growth": 0.15},
    }

    result = run_pipeline(
        company_id=1,
        company_name="GoodCorp",
        ticker="GOOD",
        period="FY2025",
        features=features,
        market_context=mock_market,
    )

    b = result.buffett
    print(f"""
  BuffettResult {{
    school_score: {{
      score:       {b.school_score.score:.1f} / 10
      raw_points:  {b.school_score.raw_points:+.1f}
      signal:      "{b.school_score.signal}"
      drivers:     [{len(b.school_score.drivers)} 条]""")
    for d in b.school_score.drivers:
        print(f"        {d.contribution:+.1f}  {d.rule_name}: {d.description}")
    print(f"""    }}
    filters_passed:   {b.filters_passed}
    filter_details:   {b.filter_details}
    valuation_status: "{b.valuation_status}"
    valuation_path:   "{b.valuation_path}"
    intrinsic_value:  {f'${b.intrinsic_value:,.2f}' if b.intrinsic_value else 'None'}
    key_assumptions:  {b.key_assumptions}
  }}""")


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "▓" * 70)
    print("  巴菲特流派 · 逐步拆解 Demo")
    print("  公司: GoodCorp (mock NVDA-like)")
    print("▓" * 70)

    step1_raw_data()
    ctx = build_context()
    features = step2_compute_features(ctx)
    filters_passed = step3_filters(features)
    raw_total, score = step4_rules(features)
    step5_dcf(features, filters_passed)
    step6_assemble(features, filters_passed, raw_total, score)

    print("\n" + "▓" * 70)
    print("  Done.")
    print("▓" * 70 + "\n")
