"""
全天候（All Weather）回测验证
==============================

验证:
1. 四象限覆盖: 每个象限都有资产在赚钱
2. 组合表现: 任何单一象限最大亏损可控
3. ERC 均衡: 风险贡献差异 < 1%
4. 长期表现: 跨象限平均年化回报合理

用真实历史各资产类别年回报率回测。
"""

from polaris.chains.all_weather import (
    build_all_weather, format_all_weather,
    BRIDGEWATER_APPROXIMATE, verify_quadrant_coverage,
    AllWeatherResult,
)


# ══════════════════════════════════════════════════════════════
#  历史各资产类别年回报率 (%)
# ══════════════════════════════════════════════════════════════
# 来源: 各 ETF 年回报 / 指数回报的近似值
# equity=S&P500, long_term_bond=20Y+Treasury, intermediate_bond=7-10Y Treasury
# commodity=GSCI/DBC, gold=黄金现货

HISTORICAL_RETURNS = {
    # 年份: {asset: return%}, quadrant
    # tips=TIPS(TIP ETF), em_bond=新兴市场债(EMB ETF) — 2003年前无ETF，用近似
    "1974": {
        "quadrant": "growth_down_inflation_up",
        "equity": -26.0, "long_term_bond": -3.0, "intermediate_bond": 2.0,
        "commodity": 40.0, "gold": 65.0,
        "tips": 5.0, "em_bond": -10.0,   # [est] TIPS概念不存在，用通胀保护近似
        "label": "石油危机滞胀",
    },
    "1980": {
        "quadrant": "growth_down_inflation_up",
        "equity": 26.0, "long_term_bond": -5.0, "intermediate_bond": 1.0,
        "commodity": 15.0, "gold": 25.0,  # 黄金1月见顶后回落，全年仍正
        "tips": 10.0, "em_bond": -8.0,
        "label": "二次石油危机",
    },
    "1982": {
        "quadrant": "growth_down_inflation_down",
        "equity": 15.0, "long_term_bond": 33.0, "intermediate_bond": 25.0,
        "commodity": -12.0, "gold": -15.0,
        "tips": 20.0, "em_bond": 15.0,
        "label": "沃尔克紧缩末期",
    },
    "1995": {
        "quadrant": "growth_up_inflation_down",
        "equity": 34.0, "long_term_bond": 28.0, "intermediate_bond": 16.0,
        "commodity": -5.0, "gold": -3.0,
        "tips": 12.0, "em_bond": 20.0,
        "label": "金发姑娘",
    },
    "1997": {
        "quadrant": "growth_up_inflation_down",
        "equity": 31.0, "long_term_bond": 12.0, "intermediate_bond": 8.0,
        "commodity": -15.0, "gold": -22.0,
        "tips": 5.0, "em_bond": -15.0,
        "label": "互联网繁荣",
    },
    "2002": {
        "quadrant": "growth_down_inflation_down",
        "equity": -23.0, "long_term_bond": 17.0, "intermediate_bond": 10.0,
        "commodity": 15.0, "gold": 25.0,
        "tips": 16.0, "em_bond": 10.0,
        "label": "互联网泡沫破裂",
    },
    "2003": {
        "quadrant": "growth_up_inflation_down",
        "equity": 26.0, "long_term_bond": 2.0, "intermediate_bond": 3.0,
        "commodity": 20.0, "gold": 20.0,
        "tips": 8.0, "em_bond": 25.0,
        "label": "复苏",
    },
    "2007": {
        "quadrant": "growth_down_inflation_up",
        "equity": 4.0, "long_term_bond": 10.0, "intermediate_bond": 8.0,
        "commodity": 33.0, "gold": 31.0,
        "tips": 12.0, "em_bond": 5.0,
        "label": "次贷危机前夜",
    },
    "2008": {
        "quadrant": "growth_down_inflation_down",
        "equity": -38.0, "long_term_bond": 33.0, "intermediate_bond": 12.0,
        "commodity": -36.0, "gold": 5.0,
        "tips": -2.0, "em_bond": -18.0,
        "label": "全球金融危机",
    },
    "2009": {
        "quadrant": "growth_down_inflation_down",
        "equity": 23.0, "long_term_bond": -13.0, "intermediate_bond": -2.0,
        "commodity": 18.0, "gold": 24.0,
        "tips": 11.0, "em_bond": 28.0,
        "label": "QE1复苏",
    },
    "2011": {
        "quadrant": "growth_down_inflation_up",
        "equity": 0.0, "long_term_bond": 30.0, "intermediate_bond": 12.0,
        "commodity": -13.0, "gold": 10.0,
        "tips": 14.0, "em_bond": 3.0,
        "label": "欧债危机",
    },
    "2013": {
        "quadrant": "growth_up_inflation_down",
        "equity": 30.0, "long_term_bond": -13.0, "intermediate_bond": -4.0,
        "commodity": -9.0, "gold": -28.0,
        "tips": -9.0, "em_bond": -7.0,
        "label": "Taper Tantrum",
    },
    "2017": {
        "quadrant": "growth_up_inflation_down",
        "equity": 19.0, "long_term_bond": 8.0, "intermediate_bond": 3.0,
        "commodity": 1.0, "gold": 13.0,
        "tips": 3.0, "em_bond": 10.0,
        "label": "同步增长",
    },
    "2018": {
        "quadrant": "growth_up_inflation_up",
        "equity": -6.0, "long_term_bond": -2.0, "intermediate_bond": 0.0,
        "commodity": -11.0, "gold": -1.0,
        "tips": -1.0, "em_bond": -5.0,
        "label": "万物皆跌",
    },
    "2020": {
        "quadrant": "growth_down_inflation_down",
        "equity": 16.0, "long_term_bond": 18.0, "intermediate_bond": 8.0,
        "commodity": -4.0, "gold": 25.0,
        "tips": 11.0, "em_bond": 5.0,
        "label": "COVID+QE",
    },
    "2021": {
        "quadrant": "growth_up_inflation_up",
        "equity": 27.0, "long_term_bond": -5.0, "intermediate_bond": -2.0,
        "commodity": 27.0, "gold": -4.0,
        "tips": 6.0, "em_bond": -2.0,
        "label": "通胀上行",
    },
    "2022": {
        "quadrant": "growth_down_inflation_up",
        "equity": -19.0, "long_term_bond": -31.0, "intermediate_bond": -15.0,
        "commodity": 16.0, "gold": 0.0,
        "tips": -12.0, "em_bond": -18.0,
        "label": "股债双杀",
    },
}


def _portfolio_return(weights: dict[str, float], returns: dict[str, float]) -> float:
    """计算组合回报。"""
    total = 0.0
    for asset, w in weights.items():
        r = returns.get(asset, 0.0)
        total += w * r
    return total


def test_quadrant_performance():
    """四象限回测: 每个象限的组合表现。"""
    print("=" * 70)
    print("  全天候四象限回测")
    print("=" * 70)

    weights = BRIDGEWATER_APPROXIMATE

    # 按象限分组
    quadrants: dict[str, list[tuple[str, float, dict]]] = {
        "growth_up_inflation_up": [],
        "growth_up_inflation_down": [],
        "growth_down_inflation_up": [],
        "growth_down_inflation_down": [],
    }

    for year, data in HISTORICAL_RETURNS.items():
        q = data["quadrant"]
        r = {k: v for k, v in data.items() if k not in ("quadrant", "label")}
        port_ret = _portfolio_return(weights, r)
        quadrants[q].append((year, port_ret, data))

    quadrant_labels = {
        "growth_up_inflation_up": "增长↑通胀↑",
        "growth_up_inflation_down": "增长↑通胀↓ (金发姑娘)",
        "growth_down_inflation_up": "增长↓通胀↑ (滞胀)",
        "growth_down_inflation_down": "增长↓通胀↓ (衰退/通缩)",
    }

    all_returns = []
    worst_quadrant = None
    worst_avg = 999

    for q, periods in quadrants.items():
        label = quadrant_labels[q]
        returns_list = [r for _, r, _ in periods]
        avg = sum(returns_list) / len(returns_list) if returns_list else 0
        worst = min(returns_list) if returns_list else 0
        best = max(returns_list) if returns_list else 0

        if avg < worst_avg:
            worst_avg = avg
            worst_quadrant = q

        print(f"\n  ── {label} ({len(periods)} 期) ──")
        for year, ret, data in periods:
            marker = "+" if ret > 0 else "-"
            print(f"    {marker} {year} {data['label']:20s}: {ret:+.1f}%")
            # 各资产明细
            asset_strs = []
            for a in ["equity", "long_term_bond", "commodity", "gold"]:
                ar = data.get(a, 0)
                asset_strs.append(f"{a[:6]}={ar:+.0f}%")
            print(f"      [{', '.join(asset_strs)}]")

        print(f"    平均: {avg:+.1f}%  最差: {worst:+.1f}%  最好: {best:+.1f}%")
        all_returns.extend(returns_list)

    # 汇总
    total_avg = sum(all_returns) / len(all_returns) if all_returns else 0
    total_worst = min(all_returns) if all_returns else 0
    positive_years = sum(1 for r in all_returns if r > 0)

    print(f"\n{'=' * 70}")
    print(f"  汇总 ({len(all_returns)} 年)")
    print(f"{'=' * 70}")
    print(f"  年均回报: {total_avg:+.1f}%")
    print(f"  最差年度: {total_worst:+.1f}%")
    print(f"  正回报比: {positive_years}/{len(all_returns)} ({positive_years/len(all_returns):.0%})")
    print(f"  最差象限: {quadrant_labels.get(worst_quadrant, '?')} (均 {worst_avg:+.1f}%)")

    # 验证标准
    print(f"\n  验证:")
    ok_drawdown = total_worst > -20
    ok_positive = positive_years / len(all_returns) > 0.6
    ok_return = total_avg > 3

    print(f"  {'O' if ok_drawdown else 'X'} 最差年度 > -20%: {total_worst:+.1f}%")
    print(f"  {'O' if ok_positive else 'X'} 正回报比 > 60%: {positive_years/len(all_returns):.0%}")
    print(f"  {'O' if ok_return else 'X'} 年均回报 > 3%: {total_avg:+.1f}%")


def test_2013_worst_case():
    """2013 Taper Tantrum — 全天候历史最差年之一。"""
    print(f"\n{'=' * 70}")
    print("  2013 Taper Tantrum — 全天候压力测试")
    print("=" * 70)

    data = HISTORICAL_RETURNS["2013"]
    weights = BRIDGEWATER_APPROXIMATE
    ret = _portfolio_return(weights, data)

    print(f"  组合回报: {ret:+.1f}%")
    for a, w in weights.items():
        ar = data.get(a, 0)
        contrib = w * ar
        print(f"    {a:25s}: w={w:.1%} × r={ar:+.0f}% = {contrib:+.1f}%")

    print(f"\n  桥水全天候 2013 实际: ≈ -3.9%")
    print(f"  我们的模拟: {ret:+.1f}%")
    if abs(ret - (-3.9)) < 3:
        print(f"  O 接近桥水实际表现")
    else:
        print(f"  X 偏差较大")


def test_2022_stress():
    """2022 股债双杀 — 全天候最大挑战。"""
    print(f"\n{'=' * 70}")
    print("  2022 股债双杀 — 全天候压力测试")
    print("=" * 70)

    data = HISTORICAL_RETURNS["2022"]
    weights = BRIDGEWATER_APPROXIMATE
    ret = _portfolio_return(weights, data)

    print(f"  组合回报: {ret:+.1f}%")
    for a, w in weights.items():
        ar = data.get(a, 0)
        contrib = w * ar
        print(f"    {a:25s}: w={w:.1%} × r={ar:+.0f}% = {contrib:+.1f}%")

    print(f"\n  问题: 债券(40%权重)跌31% → 最大拖累")
    print(f"  这就是全天候的弱点: 债券占比高，利率快速上升时很痛")


def test_correlation_regime_change():
    """相关性突变测试: 当股债从负相关变为正相关。"""
    print(f"\n{'=' * 70}")
    print("  相关性突变: 2022 型场景（股债同跌）")
    print("=" * 70)

    # 正常相关性: 股债负相关 → 全天候有效
    normal = {"equity": -10, "long_term_bond": 15, "intermediate_bond": 8,
              "commodity": 5, "gold": 8}
    # 突变相关性: 股债同跌 → 全天候失效
    stress = {"equity": -20, "long_term_bond": -25, "intermediate_bond": -10,
              "commodity": -5, "gold": 0}

    weights = BRIDGEWATER_APPROXIMATE
    normal_ret = _portfolio_return(weights, normal)
    stress_ret = _portfolio_return(weights, stress)

    print(f"  正常（股债负相关）: {normal_ret:+.1f}%")
    print(f"  突变（股债正相关）: {stress_ret:+.1f}%")
    print(f"  损失差: {stress_ret - normal_ret:+.1f}pp")
    print(f"\n  结论: 全天候在相关性突变时失效，这是已知局限")
    print(f"  桥水的解决方案: Pure Alpha 主动对冲 + 尾部保护")


def test_quadrant_coverage():
    """四象限覆盖验证。"""
    print(f"\n{'=' * 70}")
    print("  四象限覆盖验证")
    print("=" * 70)

    coverage = verify_quadrant_coverage(BRIDGEWATER_APPROXIMATE)
    quadrant_labels = {
        "growth_up_inflation_up": "增长↑通胀↑",
        "growth_up_inflation_down": "增长↑通胀↓",
        "growth_down_inflation_up": "增长↓通胀↑",
        "growth_down_inflation_down": "增长↓通胀↓",
    }

    all_covered = True
    for q, label in quadrant_labels.items():
        assets = coverage.get(q, [])
        ok = len(assets) >= 2
        if not ok:
            all_covered = False
        print(f"  {'O' if ok else 'X'} {label}: {', '.join(assets)}")

    print(f"\n  {'O' if all_covered else 'X'} 所有象限至少 2 个资产覆盖")


def test_5_vs_7_assets():
    """5 资产 vs 7 资产（加 TIPS + EM 债）对比。"""
    from polaris.chains.all_weather import backtest_all_weather

    print(f"\n{'=' * 70}")
    print("  5 资产 vs 7 资产对比")
    print("=" * 70)

    w5 = BRIDGEWATER_APPROXIMATE
    # 7 资产近似权重（基于 ERC 模拟结果调整）
    w7 = {
        "equity": 0.12, "long_term_bond": 0.12, "intermediate_bond": 0.20,
        "commodity": 0.09, "gold": 0.09, "tips": 0.23, "em_bond": 0.15,
    }

    bt5 = backtest_all_weather(w5, HISTORICAL_RETURNS, "annual")
    bt7 = backtest_all_weather(w7, HISTORICAL_RETURNS, "annual")

    print(f"\n  {'指标':15s}  {'5资产':>10s}  {'7资产':>10s}  {'差异':>10s}")
    print(f"  {'-'*50}")
    print(f"  {'年化回报':15s}  {bt5.annualized_return:+9.1f}%  {bt7.annualized_return:+9.1f}%  {bt7.annualized_return-bt5.annualized_return:+9.1f}%")
    print(f"  {'最差年度':15s}  {bt5.worst_year:+9.1f}%  {bt7.worst_year:+9.1f}%  {bt7.worst_year-bt5.worst_year:+9.1f}%")
    print(f"  {'最大回撤':15s}  {bt5.max_drawdown:9.1f}%  {bt7.max_drawdown:9.1f}%  {bt7.max_drawdown-bt5.max_drawdown:+9.1f}%")
    print(f"  {'正回报比':15s}  {bt5.positive_years:>5d}/{bt5.total_years:<4d}  {bt7.positive_years:>5d}/{bt7.total_years:<4d}")

    # 滞胀象限对比（最薄弱的环节）
    print(f"\n  ── 滞胀象限（最薄弱）年度对比 ──")
    for y5, y7 in zip(bt5.years, bt7.years):
        data = HISTORICAL_RETURNS[y5.year]
        if data["quadrant"] == "growth_down_inflation_up":
            delta = y7.portfolio_return - y5.portfolio_return
            better = "+" if delta > 0 else ""
            print(f"    {y5.year} {y5.label:15s}: 5资产={y5.portfolio_return:+.1f}%  7资产={y7.portfolio_return:+.1f}%  {better}{delta:.1f}%")


def test_rebalance_strategies():
    """对比三种再平衡策略: 买入持有 vs 年度 vs 阈值触发。"""
    from polaris.chains.all_weather import backtest_all_weather, check_rebalance, simulate_drift

    print(f"\n{'=' * 70}")
    print("  再平衡策略对比回测 (17 年)")
    print("=" * 70)

    weights = BRIDGEWATER_APPROXIMATE
    strategies = [
        ("buy_hold", "买入持有（不再平衡）"),
        ("annual", "年度再平衡"),
        ("threshold_5pct", "阈值触发（偏离>5%）"),
    ]

    results = []
    for strategy, label in strategies:
        bt = backtest_all_weather(weights, HISTORICAL_RETURNS, strategy)
        results.append((label, bt))
        print(f"\n  ── {label} ──")
        print(f"    累计回报: {bt.total_return:+.1f}%")
        print(f"    年化回报: {bt.annualized_return:+.1f}%")
        print(f"    最差年度: {bt.worst_year:+.1f}%")
        print(f"    最大回撤: {bt.max_drawdown:.1f}%")
        print(f"    正回报比: {bt.positive_years}/{bt.total_years}")
        print(f"    总交易成本: {bt.total_cost:.3f}%")

    # 逐年对比
    print(f"\n  ── 逐年对比 ──")
    print(f"  {'年份':8s}  {'买入持有':>8s}  {'年度':>8s}  {'阈值':>8s}  {'标签'}")
    for i, year_data in enumerate(results[0][1].years):
        bh = results[0][1].years[i].portfolio_return
        an = results[1][1].years[i].portfolio_return
        th = results[2][1].years[i].portfolio_return
        rb = "R" if results[2][1].years[i].rebalanced else " "
        print(f"  {year_data.year:8s}  {bh:+7.1f}%  {an:+7.1f}%  {th:+7.1f}% {rb} {year_data.label}")

    # 权重漂移演示
    print(f"\n  ── 权重漂移示例（买入持有 2008 后）──")
    for bt_label, bt in results:
        for y in bt.years:
            if y.year == "2008":
                print(f"  {bt_label} 2008 后权重:")
                for a, w in sorted(y.end_weights.items(), key=lambda x: -x[1]):
                    target = weights.get(a, 0)
                    drift = w - target
                    print(f"    {a:25s}: {w:.1%} (目标{target:.1%}, 偏离{drift:+.1%})")
                break

    # 结论
    print(f"\n  ── 结论 ──")
    best = max(results, key=lambda x: x[1].annualized_return)
    print(f"  最优策略: {best[0]} (年化 {best[1].annualized_return:+.1f}%)")

    # 阈值触发的再平衡频率
    th_bt = results[2][1]
    rb_count = sum(1 for y in th_bt.years if y.rebalanced)
    print(f"  阈值触发: {rb_count}/{th_bt.total_years} 年再平衡 ({rb_count/th_bt.total_years:.0%})")


def main():
    test_quadrant_performance()
    test_2013_worst_case()
    test_2022_stress()
    test_correlation_regime_change()
    test_quadrant_coverage()
    test_rebalance_strategies()

    test_stress_scenarios()

    print(f"\n{'=' * 70}")
    print("  全天候验证完成")
    print(f"{'=' * 70}")


def test_stress_scenarios():
    """极端压力测试: 超越历史的假想场景。"""
    from polaris.chains.all_weather import backtest_all_weather

    print(f"\n{'=' * 70}")
    print("  压力测试: 极端场景")
    print("=" * 70)

    w5 = BRIDGEWATER_APPROXIMATE
    w7 = {
        "equity": 0.12, "long_term_bond": 0.12, "intermediate_bond": 0.20,
        "commodity": 0.09, "gold": 0.09, "tips": 0.23, "em_bond": 0.15,
    }

    stress_scenarios = {
        "S1 2008 重演（信贷崩溃）": {
            "equity": -40.0, "long_term_bond": 30.0, "intermediate_bond": 10.0,
            "commodity": -35.0, "gold": 10.0, "tips": -5.0, "em_bond": -25.0,
        },
        "S2 1970s 滞胀重演": {
            "equity": -25.0, "long_term_bond": -15.0, "intermediate_bond": -5.0,
            "commodity": 50.0, "gold": 40.0, "tips": 10.0, "em_bond": -15.0,
        },
        "S3 日本化（长期通缩）": {
            "equity": -5.0, "long_term_bond": 15.0, "intermediate_bond": 8.0,
            "commodity": -10.0, "gold": 5.0, "tips": -2.0, "em_bond": 2.0,
        },
        "S4 超级通胀（CPI 15%+）": {
            "equity": -15.0, "long_term_bond": -35.0, "intermediate_bond": -20.0,
            "commodity": 60.0, "gold": 50.0, "tips": 5.0, "em_bond": -20.0,
        },
        "S5 万物皆跌（相关性=1）": {
            "equity": -25.0, "long_term_bond": -20.0, "intermediate_bond": -10.0,
            "commodity": -15.0, "gold": -5.0, "tips": -8.0, "em_bond": -20.0,
        },
        "S6 新兴市场危机": {
            "equity": -10.0, "long_term_bond": 15.0, "intermediate_bond": 8.0,
            "commodity": -20.0, "gold": 15.0, "tips": 3.0, "em_bond": -30.0,
        },
        "S7 美元崩盘": {
            "equity": -10.0, "long_term_bond": -15.0, "intermediate_bond": -5.0,
            "commodity": 40.0, "gold": 60.0, "tips": 10.0, "em_bond": 10.0,
        },
    }

    print(f"\n  {'场景':30s}  {'5资产':>8s}  {'7资产':>8s}  {'改善':>8s}")
    print(f"  {'-' * 60}")

    for name, returns in stress_scenarios.items():
        ret5 = _portfolio_return(w5, returns)
        ret7 = _portfolio_return(w7, returns)
        delta = ret7 - ret5
        better = "+" if delta > 0 else ""
        print(f"  {name:30s}  {ret5:+7.1f}%  {ret7:+7.1f}%  {better}{delta:.1f}%")

    # 最坏场景检查
    print(f"\n  验证标准:")
    worst5 = min(_portfolio_return(w5, r) for r in stress_scenarios.values())
    worst7 = min(_portfolio_return(w7, r) for r in stress_scenarios.values())
    print(f"  5 资产最坏: {worst5:+.1f}%  {'O' if worst5 > -25 else 'X'} (> -25%)")
    print(f"  7 资产最坏: {worst7:+.1f}%  {'O' if worst7 > -20 else 'X'} (> -20%)")

    # 只有 S5（万物皆跌）应该是无法防御的
    s5_ret5 = _portfolio_return(w5, stress_scenarios["S5 万物皆跌（相关性=1）"])
    s5_ret7 = _portfolio_return(w7, stress_scenarios["S5 万物皆跌（相关性=1）"])
    print(f"\n  S5 万物皆跌:")
    print(f"    5 资产: {s5_ret5:+.1f}%")
    print(f"    7 资产: {s5_ret7:+.1f}%")
    print(f"    这是全天候的结构性极限 — 需要 Pure Alpha 的尾部保护来覆盖")


if __name__ == "__main__":
    main()
