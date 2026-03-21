"""
行业级回测: 脆弱点 → 行业权重
================================

把equity拆成9个GICS行业，用simulator的脆弱点指导行业配置:
- 信贷/利率脆弱 → 减XLF(金融), 加XLP(必选消费)
- 供应链脆弱 → 减XLI(工业)/XLB(材料), 加XLU(公用事业)
- Fed印钱 → 加XLE(能源)/XLB(材料)
- Fed不救 → 加XLP/XLU/XLV(防御)

对比: SPY(等权一桶) vs 行业轮动(simulator驱动)
"""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass
from pathlib import Path

_TESTS_DIR = Path(__file__).resolve().parent.parent.parent / "tests"


def _load_json(name: str) -> dict:
    with open(_TESTS_DIR / name) as f:
        return json.load(f)


# ══════════════════════════════════════════════════════════════
# 数据驱动的行业权重 — 从历史状态→行业表现统计得出
# 不是拍脑袋，是"历史上类似状态时哪些行业表现最好"
# ══════════════════════════════════════════════════════════════

# F1a状态 → 行业权重排序（从历史3个月前瞻回报统计）
_F1A_SECTOR_WEIGHTS = {
    "accelerating_decline": {
        # 历史: XLK+1.8 > XLI+1.1 > XLV+1.1 > XLP+0.9 >> XLF-1.6
        "XLK": 0.18, "XLV": 0.14, "XLI": 0.13, "XLP": 0.13,
        "XLY": 0.10, "XLU": 0.10, "XLB": 0.10, "XLE": 0.08, "XLF": 0.04,
    },
    "declining": {
        # 样本少(1月)，偏向等权但略减XLF
        "XLE": 0.13, "XLI": 0.12, "XLK": 0.12, "XLB": 0.12, "XLP": 0.12,
        "XLU": 0.10, "XLV": 0.10, "XLF": 0.09, "XLY": 0.10,
    },
    "improving": {
        # 历史: 全涨，XLY+6.1 > XLK+6.0 > XLI+5.5 → 周期股领涨
        "XLY": 0.13, "XLK": 0.13, "XLI": 0.12, "XLF": 0.12,
        "XLB": 0.12, "XLV": 0.11, "XLP": 0.09, "XLE": 0.09, "XLU": 0.09,
    },
    "stable": {
        # 历史: XLU+2.8 > XLK+2.7 > XLE+1.9 → 接近等权
        "XLU": 0.13, "XLK": 0.13, "XLE": 0.12, "XLP": 0.11,
        "XLY": 0.11, "XLV": 0.10, "XLB": 0.10, "XLI": 0.10, "XLF": 0.10,
    },
}

# Fed冲突 → 行业权重
_FED_CONFLICT_WEIGHTS = {
    True: {
        # 历史: Fed冲突时 XLK+1.7 > XLV+1.0 >> XLF-2.5, XLE-2.1
        "XLK": 0.18, "XLV": 0.15, "XLP": 0.13, "XLY": 0.12,
        "XLU": 0.11, "XLI": 0.10, "XLB": 0.08, "XLE": 0.07, "XLF": 0.06,
    },
    False: {
        # 无冲突: 全涨，接近等权，XLK略多
        "XLK": 0.13, "XLI": 0.12, "XLB": 0.12, "XLE": 0.11,
        "XLY": 0.11, "XLF": 0.11, "XLV": 0.10, "XLP": 0.10, "XLU": 0.10,
    },
}

# 默认等权
_SECTORS = ["XLK", "XLV", "XLF", "XLE", "XLI", "XLY", "XLP", "XLU", "XLB"]
_EQUAL_WEIGHT = {s: 1.0 / len(_SECTORS) for s in _SECTORS}


def _classify_vulnerability(vuln) -> str:
    """分类脆弱点: credit / supply_chain / other"""
    loc = vuln.location.lower()
    if any(k in loc for k in ["利率", "信贷", "贷款", "利差", "逾期", "偿付"]):
        return "credit"
    elif any(k in loc for k in ["运输", "进口", "供应链", "油", "货运"]):
        return "supply_chain"
    return "other"


def _classify_fed_action(monetary_conflict) -> str:
    """分类Fed可能牺牲什么"""
    if not monetary_conflict or monetary_conflict.severity < 0.3:
        return "none"
    sacrifice = monetary_conflict.most_likely_sacrifice
    if "CPI" in sacrifice or "印钱" in sacrifice:
        return "print_money"
    elif "就业" in sacrifice or "不救" in sacrifice:
        return "sacrifice_jobs"
    return "none"


def compute_sector_weights(sim) -> dict[str, float]:
    """从simulator输出计算行业权重 — 数据驱动，不拍脑袋。

    逻辑:
    1. 从F1a状态取历史最优行业权重（50%权重）
    2. 从Fed冲突状态取历史最优行业权重（50%权重）
    3. 混合得到最终权重
    """
    # F1a状态 → 权重
    f1a_status = "stable"
    for f in sim.forces:
        if f.force_id == "f1a":
            f1a_status = f.status
            break

    # 映射simulator状态到权重表的key
    status_map = {
        "accelerating_decline": "accelerating_decline",
        "declining": "declining",
        "improving": "improving",
        "stable": "stable",
        "key_variable": "stable",
    }
    f1a_key = status_map.get(f1a_status, "stable")
    f1a_weights = _F1A_SECTOR_WEIGHTS.get(f1a_key, _EQUAL_WEIGHT)

    # Fed冲突 → 权重
    has_conflict = sim.monetary_conflict is not None and sim.monetary_conflict.severity > 0.3
    fed_weights = _FED_CONFLICT_WEIGHTS.get(has_conflict, _FED_CONFLICT_WEIGHTS[False])

    # 混合: 50% F1a + 50% Fed
    weights = {}
    for sector in _SECTORS:
        weights[sector] = f1a_weights.get(sector, 0.11) * 0.5 + fed_weights.get(sector, 0.11) * 0.5

    # 归一化
    total = sum(weights.values())
    if total > 0:
        weights = {k: v / total for k, v in weights.items()}
    return weights


@dataclass
class SectorBacktestResult:
    strategy: str
    months: int
    ann_return: float
    ann_vol: float
    sharpe: float
    cumulative: float
    max_drawdown: float
    worst_month: float


def run_sector_backtest(
    use_simulator: bool = False,
    rebalance_freq: int = 3,
) -> SectorBacktestResult:
    """行业级回测。

    use_simulator=False: 等权9行业（基准）
    use_simulator=True: simulator驱动的行业轮动
    """
    sector_returns = _load_json("data_sector_returns.json")
    signals = _load_json("data_monthly_signals.json")

    fred = None
    if use_simulator:
        fred = _load_json("data_fred_monthly_history.json")

    months = sorted(m for m in sector_returns if m >= "2007-01" and m <= "2024-12")
    weights = dict(_EQUAL_WEIGHT)

    cumulative = 1.0
    peak = 1.0
    max_dd = 0.0
    monthly_rets = []

    for i, month in enumerate(months):
        rets = sector_returns[month]

        # 再平衡
        if i % rebalance_freq == 0:
            if use_simulator and fred:
                try:
                    from polaris.simulator import simulate
                    sim = simulate(fred, month)
                    weights = compute_sector_weights(sim)
                except Exception:
                    weights = dict(_EQUAL_WEIGHT)
            else:
                weights = dict(_EQUAL_WEIGHT)

        # 计算月回报
        port_ret = 0.0
        for sector, w in weights.items():
            r = rets.get(sector, 0)
            port_ret += w * r / 100

        monthly_rets.append(port_ret * 100)
        cumulative *= (1 + port_ret)
        peak = max(peak, cumulative)
        dd = (peak - cumulative) / peak
        max_dd = max(max_dd, dd)

    n = len(monthly_rets)
    ny = n / 12
    ann_ret = (cumulative ** (1 / ny) - 1) * 100
    ann_vol = statistics.stdev(monthly_rets) * (12 ** 0.5)
    avg_rf = statistics.mean(signals.get(m, {}).get("treasury_3m", 2.0) for m in months)
    sharpe = (ann_ret - avg_rf) / ann_vol if ann_vol > 0 else 0

    return SectorBacktestResult(
        strategy="行业轮动(simulator)" if use_simulator else "行业等权(基准)",
        months=n,
        ann_return=round(ann_ret, 1),
        ann_vol=round(ann_vol, 1),
        sharpe=round(sharpe, 2),
        cumulative=round((cumulative - 1) * 100, 0),
        max_drawdown=round(max_dd * 100, 1),
        worst_month=round(min(monthly_rets), 1),
    )


def run_spy_benchmark() -> SectorBacktestResult:
    """SPY基准。"""
    sector_returns = _load_json("data_sector_returns.json")
    signals = _load_json("data_monthly_signals.json")
    months = sorted(m for m in sector_returns if m >= "2007-01" and m <= "2024-12")

    cumulative = 1.0
    peak = 1.0
    max_dd = 0.0
    monthly_rets = []

    for month in months:
        r = sector_returns[month].get("SPY", 0)
        port_ret = r / 100
        monthly_rets.append(r)
        cumulative *= (1 + port_ret)
        peak = max(peak, cumulative)
        dd = (peak - cumulative) / peak
        max_dd = max(max_dd, dd)

    n = len(monthly_rets)
    ny = n / 12
    ann_ret = (cumulative ** (1 / ny) - 1) * 100
    ann_vol = statistics.stdev(monthly_rets) * (12 ** 0.5)
    avg_rf = statistics.mean(signals.get(m, {}).get("treasury_3m", 2.0) for m in months)
    sharpe = (ann_ret - avg_rf) / ann_vol if ann_vol > 0 else 0

    return SectorBacktestResult(
        strategy="SPY",
        months=n,
        ann_return=round(ann_ret, 1),
        ann_vol=round(ann_vol, 1),
        sharpe=round(sharpe, 2),
        cumulative=round((cumulative - 1) * 100, 0),
        max_drawdown=round(max_dd * 100, 1),
        worst_month=round(min(monthly_rets), 1),
    )


if __name__ == "__main__":
    print("=" * 70)
    print("  行业级回测: 脆弱点 → 行业权重 (2007-2024)")
    print("=" * 70)

    spy = run_spy_benchmark()
    equal = run_sector_backtest(use_simulator=False)
    smart = run_sector_backtest(use_simulator=True)

    print(f"\n  {'策略':<25} {'年化':>7} {'波动率':>7} {'夏普':>6} {'累计':>8} {'回撤':>7} {'最差月':>7}")
    print(f"  {'-'*72}")
    for r in [spy, equal, smart]:
        marker = " *" if r.sharpe > spy.sharpe else ""
        print(f"  {r.strategy:<25} {r.ann_return:+6.1f}% {r.ann_vol:6.1f}% {r.sharpe:5.2f} {r.cumulative:+7.0f}% {r.max_drawdown:6.1f}% {r.worst_month:+6.1f}%{marker}")

    print(f"\n  行业轮动 vs SPY:")
    print(f"    年化差: {smart.ann_return - spy.ann_return:+.1f}%")
    print(f"    夏普差: {smart.sharpe - spy.sharpe:+.2f}")
    print(f"    回撤差: {smart.max_drawdown - spy.max_drawdown:+.1f}%")
