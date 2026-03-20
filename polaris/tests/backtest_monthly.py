"""
月度回测引擎 — 真实 ETF 月回报 + 月度信号
==========================================

从年度玩具升级到月度真实回测:
- 216 个月（2007.01 - 2024.12）
- 月度 ETF 回报（SPY/TLT/IEF/DBC/GLD/TIP/EMB）
- 月度 VIX/收益率信号
- 月度再平衡
"""

import json
import statistics
from pathlib import Path
from polaris.chains.all_weather import BRIDGEWATER_APPROXIMATE
from polaris.chains.soros import compute_complacency_signal, MarketImplied

DATA_DIR = Path(__file__).parent

# 加载数据
with open(DATA_DIR / "data_monthly_returns.json") as f:
    MONTHLY_RETURNS = json.load(f)

with open(DATA_DIR / "data_monthly_signals.json") as f:
    MONTHLY_SIGNALS = json.load(f)


def backtest_monthly(
    base_weights: dict[str, float],
    strategy: str = "all_weather",
    rebalance_freq: int = 1,       # 每 N 个月再平衡
    leverage: float = 1.0,
    use_soros: bool = False,
) -> dict:
    """月度回测。

    strategy:
      "all_weather" — 固定权重，定期再平衡
      "soros_overlay" — 全天候 + 索罗斯自满/恐慌信号调整
    """
    months = sorted(MONTHLY_RETURNS.keys())
    current_weights = {k: v * leverage for k, v in base_weights.items()}

    cumulative = 1.0
    peak = 1.0
    max_dd = 0.0
    monthly_rets = []
    equity_curve = [1.0]

    for i, month in enumerate(months):
        rets = MONTHLY_RETURNS[month]
        signals = MONTHLY_SIGNALS.get(month, {})

        # 索罗斯自满/恐慌信号
        soros_adj = {}
        if use_soros and signals.get("vix") is not None:
            market = MarketImplied(
                vix=signals.get("vix"),
                vix_term_structure=(
                    -0.3 if signals.get("vix_change", 0) > 5
                    else 0.3 if signals.get("vix_change", 0) < -3
                    else 0.0
                ),
            )
            soros_adj = compute_complacency_signal(market)

        # 计算月回报
        port_ret = 0.0
        for asset, w in current_weights.items():
            if asset not in rets:
                continue
            adj_w = w
            # 索罗斯调整
            if soros_adj:
                adj_w += soros_adj.get(asset, 0) * leverage
            port_ret += adj_w * rets[asset] / 100

        # 杠杆成本（月化）
        rf_monthly = signals.get("treasury_3m", 2.0) / 12 / 100
        borrow = max(0, sum(abs(v) for v in current_weights.values()) - 1.0)
        cost = borrow * rf_monthly
        port_ret -= cost

        monthly_rets.append(port_ret * 100)
        cumulative *= (1 + port_ret)
        equity_curve.append(cumulative)

        if cumulative > peak:
            peak = cumulative
        dd = (peak - cumulative) / peak
        if dd > max_dd:
            max_dd = dd

        # 再平衡
        if (i + 1) % rebalance_freq == 0:
            current_weights = {k: v * leverage for k, v in base_weights.items()}

    n_months = len(monthly_rets)
    n_years = n_months / 12
    ann_ret = (cumulative ** (1 / n_years) - 1) * 100 if n_years > 0 else 0
    ann_vol = statistics.stdev(monthly_rets) * (12 ** 0.5) if n_months > 1 else 0
    avg_rf = statistics.mean(
        MONTHLY_SIGNALS.get(m, {}).get("treasury_3m", 2.0)
        for m in months
    )
    sharpe = (ann_ret - avg_rf) / ann_vol if ann_vol > 0 else 0

    return {
        "months": n_months,
        "ann_return": round(ann_ret, 2),
        "ann_vol": round(ann_vol, 2),
        "sharpe": round(sharpe, 2),
        "cumulative": round((cumulative - 1) * 100, 1),
        "max_drawdown": round(max_dd * 100, 1),
        "worst_month": round(min(monthly_rets), 2),
        "best_month": round(max(monthly_rets), 2),
        "positive_months": sum(1 for r in monthly_rets if r > 0),
        "monthly_rets": monthly_rets,
        "equity_curve": equity_curve,
    }


def main():
    aw = BRIDGEWATER_APPROXIMATE

    strategies = [
        ("全天候 1x", "all_weather", 1, 1.0, False),
        ("全天候 2x", "all_weather", 1, 2.0, False),
        ("全天候+索罗斯 1x", "soros_overlay", 1, 1.0, True),
        ("全天候+索罗斯 2x", "soros_overlay", 1, 2.0, True),
    ]

    # 基准: 纯 SPY
    spy_weights = {"equity": 1.0}
    spy = backtest_monthly(spy_weights, rebalance_freq=12)

    # 60/40
    bench = {"equity": 0.60, "long_term_bond": 0.20, "intermediate_bond": 0.20}
    b6040 = backtest_monthly(bench, rebalance_freq=3)

    print("=" * 85)
    print(f"  月度回测 2007.01 - 2024.12 ({spy['months']} 个月)")
    print("=" * 85)
    print(f"  {'策略':25s} {'年化':>7s} {'波动率':>7s} {'夏普':>6s} {'累计':>8s} {'回撤':>7s} {'最差月':>7s} {'正月':>5s}")
    print(f"  {'-'*80}")
    print(f"  {'S&P 500':25s} {spy['ann_return']:+6.1f}% {spy['ann_vol']:6.1f}% {spy['sharpe']:5.2f} {spy['cumulative']:+7.0f}% {spy['max_drawdown']:6.1f}% {spy['worst_month']:+6.1f}% {spy['positive_months']:>3d}/{spy['months']}")
    print(f"  {'60/40':25s} {b6040['ann_return']:+6.1f}% {b6040['ann_vol']:6.1f}% {b6040['sharpe']:5.2f} {b6040['cumulative']:+7.0f}% {b6040['max_drawdown']:6.1f}% {b6040['worst_month']:+6.1f}% {b6040['positive_months']:>3d}/{b6040['months']}")
    print(f"  {'-'*80}")

    for label, strategy, rb_freq, lev, use_soros in strategies:
        result = backtest_monthly(aw, strategy, rb_freq, lev, use_soros)
        marker = " ★" if result["sharpe"] > spy["sharpe"] else ""
        print(f"  {label:25s} {result['ann_return']:+6.1f}% {result['ann_vol']:6.1f}% {result['sharpe']:5.2f} {result['cumulative']:+7.0f}% {result['max_drawdown']:6.1f}% {result['worst_month']:+6.1f}% {result['positive_months']:>3d}/{result['months']}{marker}")

    # 索罗斯信号分析
    print(f"\n{'=' * 85}")
    print("  索罗斯自满/恐慌信号触发统计")
    print("=" * 85)
    complacent_months = 0
    panic_months = 0
    for month in sorted(MONTHLY_SIGNALS.keys()):
        sig = MONTHLY_SIGNALS[month]
        vix = sig.get("vix")
        if vix is None:
            continue
        if vix < 14:
            complacent_months += 1
        elif vix > 30:
            panic_months += 1

    total = len(MONTHLY_SIGNALS)
    print(f"  自满月(VIX<14): {complacent_months}/{total} ({complacent_months/total:.0%})")
    print(f"  恐慌月(VIX>30): {panic_months}/{total} ({panic_months/total:.0%})")
    print(f"  中性月: {total-complacent_months-panic_months}/{total} ({(total-complacent_months-panic_months)/total:.0%})")

    # 年度汇总
    print(f"\n{'=' * 85}")
    print("  逐年对比（月度回测汇总到年度）")
    print("=" * 85)

    aw_1x = backtest_monthly(aw, rebalance_freq=1)
    soros_2x = backtest_monthly(aw, "soros_overlay", 1, 2.0, True)

    print(f"  {'年':6s} {'全天候1x':>8s} {'索罗斯2x':>8s} {'S&P':>8s} {'差异':>8s}")
    print(f"  {'-'*45}")

    months = sorted(MONTHLY_RETURNS.keys())
    for year in range(2007, 2025):
        yr_months = [m for m in months if m.startswith(str(year))]
        if not yr_months:
            continue

        start_idx = months.index(yr_months[0])
        end_idx = months.index(yr_months[-1])

        def year_ret(curve, start, end):
            if end + 1 < len(curve) and start < len(curve):
                return (curve[end + 1] / curve[start] - 1) * 100
            return 0

        aw_yr = year_ret(aw_1x["equity_curve"], start_idx, end_idx)
        sr_yr = year_ret(soros_2x["equity_curve"], start_idx, end_idx)
        sp_yr = year_ret(spy["equity_curve"], start_idx, end_idx)
        diff = sr_yr - sp_yr

        print(f"  {year} {aw_yr:+7.1f}% {sr_yr:+7.1f}% {sp_yr:+7.1f}% {diff:+7.1f}%")


if __name__ == "__main__":
    main()
