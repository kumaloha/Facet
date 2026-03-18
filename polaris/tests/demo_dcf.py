"""
DCF 估值 · 输入输出展示
======================
4 家公司 × 正向 DCF + 反向 DCF
展示每一步的输入、路径选择、输出。
"""

from polaris.scoring.engines.dcf import (
    compute_intrinsic_value, reverse_dcf, forward_dcf, DCFResult, ReverseDCFResult,
)


def banner(title, desc=""):
    print(f"\n{'─' * 65}")
    print(f"  {title}")
    if desc:
        print(f"  {desc}")
    print(f"{'─' * 65}")


def show_forward(name, features, guidance, market):
    banner(f"{name} · 正向 DCF")

    oe = features.get("l0.company.owner_earnings", 0)
    dr = market["discount_rate"]
    shares = market["shares_outstanding"]

    print(f"\n  INPUT:")
    print(f"    所有者盈余        = {oe:>12,.0f}")
    print(f"    折现率            = {dr:>12.1%}  (10Y Treasury)")
    print(f"    流通股数          = {shares:>12,.0f}")
    print(f"    管理层指引        = {guidance}")

    # 展示路径选择逻辑
    print(f"\n  路径选择:")
    roic = guidance.get("roic_target") or features.get("l0.company.incremental_roic")
    if guidance.get("capex") is not None and roic is not None:
        payout = features.get("l0.company.dividend_payout_ratio", 0.0)
        growth = (1 - payout) * roic
        print(f"    路径 A: capex={guidance['capex']} + ROIC={roic:.0%} → 隐含增速={growth:.1%} ← 命中")
    elif guidance.get("revenue_growth") is not None:
        if guidance.get("capex") is not None:
            print(f"    路径 A: capex={guidance['capex']} 但无 ROIC → 跳过")
        print(f"    路径 B: revenue_growth={guidance['revenue_growth']:.1%} ← 命中")
    elif guidance.get("eps") is not None:
        print(f"    路径 C: EPS={guidance['eps']} ← 命中")
    elif guidance.get("operating_margin") is not None or guidance.get("net_margin") is not None:
        margin = guidance.get("operating_margin") or guidance.get("net_margin")
        hist = features.get("l0.company.revenue_growth_yoy")
        print(f"    路径 D: margin={margin:.1%} + 历史增速={hist:.1%} ← 命中" if hist else f"    路径 D: margin={margin:.1%} 但无历史增速")
    else:
        print(f"    无可用路径")

    dcf = compute_intrinsic_value(
        features=features, guidance=guidance,
        discount_rate=dr, shares_outstanding=shares,
    )

    print(f"\n  OUTPUT:")
    print(f"    status            = {dcf.status}")
    print(f"    valuation_path    = {dcf.valuation_path}")
    if dcf.intrinsic_value is not None:
        print(f"    内在价值/股       = ${dcf.intrinsic_value:,.2f}")
    if dcf.key_assumptions:
        for k, v in dcf.key_assumptions.items():
            fmt = f"{v:.2%}" if isinstance(v, float) and abs(v) < 1 else f"{v}"
            print(f"    假设: {k} = {fmt}")

    return dcf


def show_reverse(name, features, market, dcf_result):
    print(f"\n  反向 DCF:")

    oe = features.get("l0.company.owner_earnings", 0)
    price = market["price"]
    dr = market["discount_rate"]
    shares = market["shares_outstanding"]

    print(f"    INPUT: 股价=${price:,.2f}, OE={oe:,.0f}, 折现率={dr:.1%}")

    rdcf = reverse_dcf(
        current_price=price, current_owner_earnings=oe,
        discount_rate=dr, shares_outstanding=shares,
    )

    print(f"    OUTPUT: 市场隐含增速 = {rdcf.implied_growth_rate:.2%}" if rdcf.implied_growth_rate else "    OUTPUT: 无法计算")

    actual_growth = features.get("l0.company.revenue_growth_yoy")
    if rdcf.implied_growth_rate is not None and actual_growth is not None:
        gap = rdcf.implied_growth_rate - actual_growth
        print(f"    实际增速          = {actual_growth:.2%}")
        print(f"    预期偏差          = {gap:+.2%}")
        if gap > 0.05:
            print(f"    → 市场过于乐观")
        elif gap < -0.05:
            print(f"    → 市场过于悲观，可能有机会")
        else:
            print(f"    → 市场定价合理")

    # 安全边际
    if dcf_result.intrinsic_value is not None:
        mos = (dcf_result.intrinsic_value - price) / dcf_result.intrinsic_value
        print(f"\n  安全边际:")
        print(f"    内在价值          = ${dcf_result.intrinsic_value:,.2f}")
        print(f"    当前股价          = ${price:,.2f}")
        print(f"    安全边际          = {mos:.1%}")
        if mos > 0.30:
            print(f"    → 折扣充足，有吸引力")
        elif mos > 0:
            print(f"    → 有折扣但不算大")
        else:
            print(f"    → 股价高于内在价值，无安全边际")


def run(name, desc, features, guidance, market):
    banner(name, desc)
    dcf = show_forward(name, features, guidance, market)
    show_reverse(name, features, market, dcf)


if __name__ == "__main__":
    print("=" * 65)
    print("  DCF 估值 · 输入输出展示")
    print("=" * 65)

    # ── Case 1: 茅台 (路径 B — revenue_growth guidance) ──
    run("类茅台", "路径 B: 管理层指引收入增速 15%",
        features={
            "l0.company.owner_earnings": 1000,
            "l0.company.owner_earnings_margin": 0.53,
            "l0.company.dividend_payout_ratio": 0.50,
            "l0.company.revenue_growth_yoy": 0.15,
            "l0.company.incremental_roic": 0.60,
        },
        guidance={"revenue_growth": 0.15},
        market={"price": 1800, "shares_outstanding": 1256, "discount_rate": 0.04},
    )

    # ── Case 2: 台积电 (路径 A — capex + ROIC) ──
    run("类台积电", "路径 A: capex guidance + ROIC",
        features={
            "l0.company.owner_earnings": 21000,
            "l0.company.owner_earnings_margin": 0.23,
            "l0.company.dividend_payout_ratio": 0.45,
            "l0.company.revenue_growth_yoy": 0.10,
            "l0.company.incremental_roic": 0.40,
        },
        guidance={"capex": 30000, "roic_target": 0.40},
        market={"price": 180, "shares_outstanding": 25900, "discount_rate": 0.045},
    )

    # ── Case 3: 苹果 (路径 C — EPS guidance) ──
    run("类苹果", "路径 C: EPS guidance",
        features={
            "l0.company.owner_earnings": 100000,
            "l0.company.owner_earnings_to_net_income": 1.05,
            "l0.company.dividend_payout_ratio": 0.15,
            "l0.company.revenue_growth_yoy": 0.05,
        },
        guidance={"eps": 7.50},
        market={"price": 230, "shares_outstanding": 15000, "discount_rate": 0.045},
    )

    # ── Case 4: 公用事业 (路径 D — margin + 历史增速) ──
    run("公用事业", "路径 D: margin guidance + 历史增速 2%",
        features={
            "l0.company.owner_earnings": 1800,
            "l0.company.owner_earnings_margin": 0.17,
            "l0.company.dividend_payout_ratio": 0.65,
            "l0.company.revenue_growth_yoy": 0.02,
            "l0.company.revenue": 10600,
        },
        guidance={"operating_margin": 0.25},
        market={"price": 65, "shares_outstanding": 600, "discount_rate": 0.04},
    )

    # ── Case 5: 高增长 (路径 B — 高增速 vs 股价是否 price in) ──
    run("高增长公司", "路径 B: 管理层指引增速 25%，看市场是否过于乐观",
        features={
            "l0.company.owner_earnings": 5000,
            "l0.company.dividend_payout_ratio": 0.0,
            "l0.company.revenue_growth_yoy": 0.25,
        },
        guidance={"revenue_growth": 0.25},
        market={"price": 500, "shares_outstanding": 1000, "discount_rate": 0.045},
    )

    # ── Case 6: 无 guidance 但有 ROIC (路径 E) ──
    run("无 guidance + 有 ROIC", "路径 E: 无指引，用 ROIC × 留存率估算增速",
        features={
            "l0.company.owner_earnings": 3000,
            "l0.company.dividend_payout_ratio": 0.30,
            "l0.company.incremental_roic": 0.25,
        },
        guidance={},
        market={"price": 100, "shares_outstanding": 500, "discount_rate": 0.045},
    )

    # ── Case 7: 无 guidance 无 ROIC 但有历史增速 (路径 F) ──
    run("无 guidance + 有历史增速", "路径 F: 用历史增速封顶 10%",
        features={
            "l0.company.owner_earnings": 3000,
            "l0.company.dividend_payout_ratio": 0.30,
            "l0.company.revenue_growth_yoy": 0.20,  # 历史 20% 但封顶 10%
        },
        guidance={},
        market={"price": 100, "shares_outstanding": 500, "discount_rate": 0.045},
    )

    # ── Case 8: 什么都没有 (路径 G 零增长永续) ──
    run("什么都没有", "路径 G: 零增长永续，最保守底线",
        features={
            "l0.company.owner_earnings": 3000,
            "l0.company.dividend_payout_ratio": 0.30,
        },
        guidance={},
        market={"price": 100, "shares_outstanding": 500, "discount_rate": 0.045},
    )

    print("\n" + "=" * 65)
    print("  完成")
    print("=" * 65)
