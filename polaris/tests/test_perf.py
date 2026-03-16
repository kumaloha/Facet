"""
性能压测
========
1. 规模测试：500 家公司 × 8 期，测总吞吐量
2. 热点分析：逐特征计时，找出最慢的特征
3. 内存测试：观察大批量处理的内存增长
4. 并发模拟：多公司串行 vs 批量对比
"""

import gc
import os
import random
import statistics
import sys
import time
import tracemalloc

import pandas as pd

from polaris.features.types import ComputeContext, FeatureResult, FeatureLevel
from polaris.features.registry import get_features

import polaris.features.l0.company  # noqa: F401
import polaris.features.l0.cross_period  # noqa: F401

from polaris.scoring.scorer import score_company

EMPTY = pd.DataFrame()
PERIODS = [f"FY{y}" for y in range(2016, 2026)]  # 10 期


# ── 快速数据工厂（最小化构造开销）────────────────────────────────


def _fli_fast(items: dict[str, float], period: str) -> list[dict]:
    return [{"id": i, "statement_id": 1, "item_key": k, "item_label": k,
             "value": v, "parent_key": None, "ordinal": i, "note": None, "period": period}
            for i, (k, v) in enumerate(items.items())]


def build_context_fast(seed: int) -> ComputeContext:
    """快速生成随机公司，最小化 DataFrame 构造开销。"""
    rng = random.Random(seed)

    # 随机财务参数
    base_rev = rng.uniform(1000, 200000)
    cogs_pct = rng.uniform(0.15, 0.80)
    opex_pct = rng.uniform(0.10, 0.50)
    tax_rate = rng.uniform(0.10, 0.30)
    capex_pct = rng.uniform(0.02, 0.20)
    da_pct = rng.uniform(0.01, 0.12)
    ocf_margin = rng.uniform(-0.10, 0.40)
    growth = rng.uniform(-0.05, 0.40)
    debt_ratio = rng.uniform(0.05, 0.80)
    interest_rate = rng.uniform(0.02, 0.10)

    # 生成 10 期
    all_rows = []
    rev = base_rev
    for period in PERIODS:
        rev *= (1 + growth + rng.gauss(0, 0.05))
        cogs = rev * cogs_pct
        opex = rev * opex_pct
        op_inc = rev - cogs - opex
        equity = rev * rng.uniform(0.5, 3.0)
        debt = equity * debt_ratio / (1 - debt_ratio + 0.001)
        assets = equity + debt
        interest = debt * interest_rate
        ni = max((op_inc - interest) * (1 - tax_rate), -rev * 0.3)
        ocf = rev * ocf_margin
        cash = rev * rng.uniform(0.05, 0.5)
        capex = rev * capex_pct
        shares = rng.uniform(100, 10000)

        items = {
            "revenue": rev, "cost_of_revenue": cogs,
            "operating_income": op_inc, "net_income": ni,
            "operating_cash_flow": ocf, "capital_expenditures": capex,
            "depreciation_amortization": rev * da_pct,
            "shareholders_equity": equity, "total_assets": assets,
            "interest_expense": interest,
            "current_assets": cash + rev * 0.15,
            "current_liabilities": assets * rng.uniform(0.08, 0.25),
            "goodwill": assets * rng.uniform(0, 0.15),
            "accounts_receivable": rev * rng.uniform(0.08, 0.18),
            "inventory": rev * rng.uniform(0.03, 0.12),
            "cash_and_equivalents": cash,
            "total_debt": debt,
            "dividends_paid": -ni * 0.3 if ni > 0 else 0,
            "share_repurchase": -ni * 0.2 if ni > 0 else 0,
            "sga_expense": opex * 0.6, "rnd_expense": opex * 0.4,
            "basic_weighted_average_shares": shares,
            "income_tax_expense_total": max(0, (op_inc - interest) * tax_rate),
            "income_before_tax_total": op_inc - interest,
            "proceeds_from_stock_issuance": capex * rng.uniform(0, 1.5),
            "proceeds_from_debt_issuance": capex * rng.uniform(0, 2.0),
        }
        all_rows.extend(_fli_fast(items, period))

    all_fli = pd.DataFrame(all_rows)
    current_fli = all_fli[all_fli["period"] == "FY2025"]

    # 非财务表（最小化但非空）
    n_cust = rng.randint(2, 6)
    ds_rows = [{"id": j, "company_id": seed, "period": "FY2025", "segment": None,
                "customer_name": f"C{j}", "customer_type": None, "products": None,
                "channels": None, "revenue": None, "revenue_pct": rng.uniform(0.02, 0.4),
                "growth_yoy": None, "backlog": rng.uniform(0, 5000) if rng.random() > 0.5 else None,
                "backlog_note": None, "pricing_model": None, "contract_duration": None,
                "revenue_type": rng.choice(["license", "subscription", "project"]),
                "is_recurring": rng.random() > 0.4, "recognition_method": None,
                "description": None, "raw_post_id": None, "created_at": "2025-01-01"}
               for j in range(n_cust)]

    n_sup = rng.randint(2, 5)
    us_rows = [{"id": j, "company_id": seed, "period": "FY2025", "segment": None,
                "supplier_name": f"S{j}", "supply_type": "component",
                "material_or_service": None, "process_node": None,
                "geographic_location": rng.choice(["US", "China", "Taiwan", "Germany"]),
                "is_sole_source": rng.random() > 0.7, "purchase_obligation": None,
                "lead_time": None, "contract_type": None, "prepaid_amount": None,
                "concentration_risk": None, "description": None,
                "raw_post_id": None, "created_at": "2025-01-01"}
               for j in range(n_sup)]

    geo_rows = [{"id": j, "company_id": seed, "period": "FY2025",
                 "region": r, "revenue_share": s, "revenue": None,
                 "growth_yoy": None, "note": None, "raw_post_id": None, "created_at": "2025-01-01"}
                for j, (r, s) in enumerate([
                    ("US", rng.uniform(0.2, 0.7)),
                    ("China", rng.uniform(0, 0.4)),
                    ("Europe", rng.uniform(0.05, 0.25)),
                    ("Other", rng.uniform(0.02, 0.1)),
                ])]

    debt_cur = debt * rng.uniform(0.1, 0.5)
    debt_long = debt - debt_cur
    debt_rows = [{"id": 0, "company_id": seed, "period": "FY2025",
                  "instrument_name": "LT", "debt_type": "unsecured", "currency": "USD",
                  "principal": debt_long, "interest_rate": interest_rate,
                  "maturity_date": None, "is_secured": False, "is_current": False,
                  "note": None, "raw_post_id": None, "created_at": "2025-01-01"},
                 {"id": 1, "company_id": seed, "period": "FY2025",
                  "instrument_name": "ST", "debt_type": "unsecured", "currency": "USD",
                  "principal": debt_cur, "interest_rate": interest_rate + 0.01,
                  "maturity_date": None, "is_secured": False, "is_current": True,
                  "note": None, "raw_post_id": None, "created_at": "2025-01-01"}]

    # 多期 debt
    all_debt_rows = []
    d = equity * debt_ratio / (1 - debt_ratio + 0.001)
    for period in PERIODS:
        d *= (1 + rng.uniform(-0.05, 0.15))
        dc = d * rng.uniform(0.1, 0.5)
        dl = d - dc
        all_debt_rows.extend([
            {"id": 0, "company_id": seed, "period": period, "instrument_name": "LT",
             "debt_type": "unsecured", "currency": "USD", "principal": dl,
             "interest_rate": interest_rate, "maturity_date": None,
             "is_secured": False, "is_current": False, "note": None,
             "raw_post_id": None, "created_at": "2025-01-01"},
            {"id": 1, "company_id": seed, "period": period, "instrument_name": "ST",
             "debt_type": "unsecured", "currency": "USD", "principal": dc,
             "interest_rate": interest_rate + 0.01, "maturity_date": None,
             "is_secured": False, "is_current": True, "note": None,
             "raw_post_id": None, "created_at": "2025-01-01"},
        ])

    exec_rows = [{"id": 0, "company_id": seed, "period": "FY2025",
                  "role_type": "CEO", "name": "CEO", "title": "CEO",
                  "base_salary": None, "bonus": None, "stock_awards": 5000,
                  "option_awards": None, "non_equity_incentive": None,
                  "other_comp": None, "total_comp": 8000, "currency": "USD",
                  "pay_ratio": rng.uniform(30, 600), "median_employee_comp": None,
                  "raw_post_id": None, "created_at": "2025-01-01"}]

    own_rows = [{"id": 0, "company_id": seed, "period": "FY2025",
                 "name": "CEO", "title": "CEO",
                 "percent_of_class": rng.uniform(0.1, 40),
                 "shares_beneficially_owned": None,
                 "raw_post_id": None, "created_at": "2025-01-01"}]

    n_narr = rng.randint(3, 8)
    fulfillment = rng.uniform(0.2, 0.9)
    narr_rows = [{"id": j, "company_id": seed, "raw_post_id": None,
                  "narrative": f"N{j}", "capital_required": None, "capital_unit": None,
                  "promised_outcome": None, "deadline": None,
                  "status": "delivered" if rng.random() < fulfillment else rng.choice(["missed", "abandoned"]),
                  "reported_at": None, "created_at": "2025-01-01"}
                 for j in range(n_narr)]

    n_lit = rng.randint(0, 5)
    lit_rows = [{"id": j, "company_id": seed, "case_name": f"Case{j}", "case_type": "civil",
                 "status": rng.choice(["pending", "ongoing"]),
                 "counterparty": None, "filed_at": None,
                 "accrued_amount": rng.uniform(10, 500), "claimed_amount": rng.uniform(100, 2000),
                 "currency": "USD", "description": None, "resolution": None,
                 "resolved_at": None, "raw_post_id": None, "created_at": "2025-01-01"}
                for j in range(n_lit)]

    n_ops = rng.randint(1, 12)
    ops_rows = [{"id": j, "company_id": seed, "period": "FY2025", "raw_post_id": None,
                 "topic": f"T{j}", "performance": None, "attribution": None,
                 "risk": rng.choice(["high", "moderate", None]),
                 "guidance": rng.choice(["improving", None]),
                 "created_at": "2025-01-01"}
                for j in range(n_ops)]

    rpt_rows = []
    if rng.random() > 0.5:
        rpt_rows = [{"id": 0, "company_id": seed, "period": "FY2025",
                     "related_party": "RP", "relationship": "director",
                     "transaction_type": "consulting",
                     "amount": rev * rng.uniform(0.001, 0.1),
                     "currency": "USD", "terms": None, "is_ongoing": True,
                     "description": None, "raw_post_id": None, "created_at": "2025-01-01"}]

    ctx = ComputeContext(company_id=seed, period="FY2025")
    ctx._cache = {
        "financial_line_items": current_fli,
        "downstream_segments": pd.DataFrame(ds_rows),
        "upstream_segments": pd.DataFrame(us_rows),
        "geographic_revenues": pd.DataFrame(geo_rows),
        "debt_obligations": pd.DataFrame(debt_rows),
        "executive_compensations": pd.DataFrame(exec_rows),
        "stock_ownership": pd.DataFrame(own_rows),
        "company_narratives": pd.DataFrame(narr_rows),
        "litigations": pd.DataFrame(lit_rows) if lit_rows else EMPTY,
        "operational_issues": pd.DataFrame(ops_rows),
        "related_party_transactions": pd.DataFrame(rpt_rows) if rpt_rows else EMPTY,
        "non_financial_kpis": EMPTY,
        "financial_line_items_all": all_fli,
        "debt_obligations_all": pd.DataFrame(all_debt_rows),
        "pricing_actions": EMPTY, "market_share_data": EMPTY,
        "audit_opinions": EMPTY, "known_issues": EMPTY,
        "insider_transactions": EMPTY, "executive_changes": EMPTY,
        "equity_offerings": EMPTY, "analyst_estimates": EMPTY,
        "management_guidance": EMPTY,
    }
    return ctx


def compute_all(ctx: ComputeContext) -> dict[str, FeatureResult]:
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


# ══════════════════════════════════════════════════════════════════════
# 测试项
# ══════════════════════════════════════════════════════════════════════


def test_throughput(n: int = 500):
    """吞吐量测试：N 家公司全链路。"""
    print(f"\n{'=' * 70}")
    print(f"  吞吐量测试: {n} 家公司 × 10 期")
    print(f"{'=' * 70}")

    feature_counts = []
    rule_counts = []
    latencies = []

    t0 = time.time()
    for i in range(n):
        t1 = time.time()
        ctx = build_context_fast(seed=i)
        fr = compute_all(ctx)
        features = {name: r.value for name, r in fr.items()}
        score = score_company(i, f"Co{i}", f"T{i}", "FY2025", features)
        latencies.append(time.time() - t1)
        feature_counts.append(len(features))

        rules = 0
        for s in [score.buffett, score.dalio, score.soros]:
            if s:
                rules += len(s.school_score.drivers)
        rule_counts.append(rules)

    total = time.time() - t0

    print(f"  总耗时:          {total:.2f}s")
    print(f"  吞吐量:          {n / total:.0f} companies/sec")
    print(f"  平均延迟:        {statistics.mean(latencies) * 1000:.1f}ms")
    print(f"  P50 延迟:        {statistics.median(latencies) * 1000:.1f}ms")
    print(f"  P95 延迟:        {sorted(latencies)[int(n * 0.95)] * 1000:.1f}ms")
    print(f"  P99 延迟:        {sorted(latencies)[int(n * 0.99)] * 1000:.1f}ms")
    print(f"  最大延迟:        {max(latencies) * 1000:.1f}ms")
    print(f"  平均特征数:      {statistics.mean(feature_counts):.0f}")
    print(f"  平均规则触发:    {statistics.mean(rule_counts):.0f}")
    return total


def test_feature_hotspots():
    """热点分析：逐特征计时，找出最慢的。"""
    print(f"\n{'=' * 70}")
    print(f"  特征热点分析 (100 家公司)")
    print(f"{'=' * 70}")

    feature_times: dict[str, list[float]] = {}
    all_features = []
    for level in (FeatureLevel.L0, FeatureLevel.L1, FeatureLevel.L2):
        all_features.extend(get_features(level=level))

    for i in range(100):
        ctx = build_context_fast(seed=i + 10000)
        for feat in all_features:
            t1 = time.perf_counter()
            try:
                r = feat.compute_fn(ctx)
                if r is not None:
                    ctx.features[feat.name] = r.value
            except Exception:
                pass
            elapsed = time.perf_counter() - t1
            feature_times.setdefault(feat.name, []).append(elapsed)

    # 按平均耗时排序
    ranked = sorted(feature_times.items(), key=lambda x: statistics.mean(x[1]), reverse=True)

    print(f"\n  {'特征名':<52} {'平均(ms)':>8} {'P95(ms)':>8} {'Max(ms)':>8}")
    print(f"  {'-' * 80}")
    for name, times in ranked[:20]:
        avg = statistics.mean(times) * 1000
        p95 = sorted(times)[int(len(times) * 0.95)] * 1000
        mx = max(times) * 1000
        flag = " ⚠" if avg > 1.0 else ""
        print(f"  {name:<52} {avg:>8.3f} {p95:>8.3f} {mx:>8.3f}{flag}")

    total_avg = sum(statistics.mean(t) for t in feature_times.values()) * 1000
    print(f"\n  全部特征总计: {total_avg:.1f}ms / company")
    print(f"  注册特征数: {len(all_features)}")
    return ranked


def test_memory():
    """内存测试：处理大批量时的内存增长。"""
    print(f"\n{'=' * 70}")
    print(f"  内存测试: 1000 家公司")
    print(f"{'=' * 70}")

    tracemalloc.start()
    gc.collect()
    snap0 = tracemalloc.take_snapshot()
    mem0 = tracemalloc.get_traced_memory()[0]

    for i in range(1000):
        ctx = build_context_fast(seed=i + 20000)
        fr = compute_all(ctx)
        features = {n: r.value for n, r in fr.items()}
        score = score_company(i, f"Co{i}", f"T{i}", "FY2025", features)
        # 模拟真实场景：不保留引用
        del ctx, fr, features, score

    gc.collect()
    mem1 = tracemalloc.get_traced_memory()[0]
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()

    print(f"  起始内存:   {mem0 / 1024 / 1024:.1f} MB")
    print(f"  结束内存:   {mem1 / 1024 / 1024:.1f} MB")
    print(f"  峰值内存:   {peak / 1024 / 1024:.1f} MB")
    print(f"  内存增长:   {(mem1 - mem0) / 1024 / 1024:.1f} MB")
    print(f"  每公司峰值: {peak / 1000 / 1024:.1f} KB/company")

    if (mem1 - mem0) / 1024 / 1024 > 100:
        print("  ⚠ 内存泄漏风险：1000 家公司后增长 >100MB")
    else:
        print("  ✓ 无明显内存泄漏")


def test_context_creation_overhead():
    """DataFrame 构造开销测试。"""
    print(f"\n{'=' * 70}")
    print(f"  数据构造开销分析 (100 家)")
    print(f"{'=' * 70}")

    build_times = []
    compute_times = []
    score_times = []

    for i in range(100):
        t1 = time.perf_counter()
        ctx = build_context_fast(seed=i + 30000)
        build_times.append(time.perf_counter() - t1)

        t2 = time.perf_counter()
        fr = compute_all(ctx)
        features = {n: r.value for n, r in fr.items()}
        compute_times.append(time.perf_counter() - t2)

        t3 = time.perf_counter()
        score = score_company(i, f"Co{i}", f"T{i}", "FY2025", features)
        score_times.append(time.perf_counter() - t3)

    print(f"  数据构造: {statistics.mean(build_times) * 1000:.2f}ms avg")
    print(f"  特征计算: {statistics.mean(compute_times) * 1000:.2f}ms avg")
    print(f"  规则评分: {statistics.mean(score_times) * 1000:.2f}ms avg")
    total = (statistics.mean(build_times) + statistics.mean(compute_times) + statistics.mean(score_times)) * 1000
    print(f"  合计:     {total:.2f}ms avg")
    print(f"  占比:     构造 {statistics.mean(build_times) / (total / 1000) * 100:.0f}%"
          f"  特征 {statistics.mean(compute_times) / (total / 1000) * 100:.0f}%"
          f"  评分 {statistics.mean(score_times) / (total / 1000) * 100:.0f}%")


def main():
    print("=" * 70)
    print("  AXION 性能压测")
    print("=" * 70)

    # 1. 吞吐量
    test_throughput(500)

    # 2. 热点
    test_feature_hotspots()

    # 3. 内存
    test_memory()

    # 4. 开销分解
    test_context_creation_overhead()

    print(f"\n{'=' * 70}")
    print("  性能压测完成")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
