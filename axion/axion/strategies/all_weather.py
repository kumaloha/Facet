"""
全天候（All Weather）组合构建
============================

达利欧的核心思想: 不预测未来，而是构建一个在任何经济环境下都能存活的组合。

四象限:
  增长↑通胀↑ → 股票/大宗商品
  增长↑通胀↓ → 股票/名义债券
  增长↓通胀↑ → 大宗商品/通胀挂钩债
  增长↓通胀↓ → 名义债券/黄金

等风险贡献: 每个资产类别贡献相等的组合风险（不是相等的资本）。
波动率低的资产（债券）占更多资本，波动率高的（大宗）占更少。

数据源: Anchor DB 中的 stock_quotes（代理 ETF 的日线数据）。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field


# ── 资产类别定义 ────────────────────────────────────────────

@dataclass
class AssetClass:
    """资产类别。"""
    name: str
    etf: str                    # 代理 ETF ticker
    quadrants: list[str]        # 在哪些象限中表现好
    description: str = ""


# 全天候的 5 个核心资产类别 + 2 个扩展
ASSET_CLASSES = [
    AssetClass("equity", "SPY", ["growth_up_inflation_down", "growth_up_inflation_up"],
               "全球股票 — 增长资产"),
    AssetClass("long_term_bond", "TLT", ["growth_down_inflation_down", "growth_up_inflation_down"],
               "长期国债 — 通缩/降息避险"),
    AssetClass("intermediate_bond", "IEF", ["growth_down_inflation_down"],
               "中期国债 — 稳定收益"),
    AssetClass("commodity", "DBC", ["growth_up_inflation_up", "growth_down_inflation_up"],
               "大宗商品 — 通胀对冲"),
    AssetClass("gold", "GLD", ["growth_down_inflation_up", "growth_down_inflation_down"],
               "黄金 — 终极避险 + 通胀对冲"),
    AssetClass("tips", "TIP", ["growth_down_inflation_up", "growth_up_inflation_up"],
               "通胀挂钩债 — 实际收益率"),
    AssetClass("em_bond", "EMB", ["growth_up_inflation_up"],
               "新兴市场债 — 增长/利差"),
]

# 经典 5 资产 All Weather
DEFAULT_ASSET_NAMES = ["equity", "long_term_bond", "intermediate_bond", "commodity", "gold"]

# 扩展 7 资产（加入 TIPS + EM 债，改善滞胀象限覆盖）
EXTENDED_ASSET_NAMES = ["equity", "long_term_bond", "intermediate_bond", "commodity", "gold", "tips", "em_bond"]

# Bridgewater 公开的 All Weather 近似权重
BRIDGEWATER_APPROXIMATE = {
    "equity": 0.30,
    "long_term_bond": 0.40,
    "intermediate_bond": 0.15,
    "commodity": 0.075,
    "gold": 0.075,
}


# ── 数据获取 ────────────────────────────────────────────────


def _fetch_returns(
    etf_map: dict[str, str],
    lookback_days: int = 252,
) -> dict[str, list[float]] | None:
    """从 Anchor DB 获取各 ETF 的日收益率序列。

    Returns: {asset_name: [daily_returns]} 或 None（数据不足）
    """
    try:
        from polaris.db.anchor import query_df_safe
    except Exception:
        return None

    import numpy as np

    all_returns: dict[str, list[float]] = {}

    for asset_name, etf in etf_map.items():
        df = query_df_safe(
            "SELECT price_close FROM stock_quotes "
            "WHERE ticker = :ticker ORDER BY trade_date DESC LIMIT :n",
            {"ticker": etf, "n": lookback_days + 1},
        )
        if df.empty or len(df) < 60:
            return None

        prices = df["price_close"].astype(float).values[::-1]
        returns = list(np.diff(np.log(prices)))
        all_returns[asset_name] = returns

    # 对齐长度（取最短的）
    min_len = min(len(r) for r in all_returns.values())
    for k in all_returns:
        all_returns[k] = all_returns[k][-min_len:]

    return all_returns


# ── 波动率 + 相关性 ────────────────────────────────────────


@dataclass
class RiskMetrics:
    """风险度量。"""
    volatilities: dict[str, float]                      # 年化波动率
    correlation_matrix: dict[str, dict[str, float]]     # 相关性矩阵
    covariance_matrix: dict[str, dict[str, float]]      # 协方差矩阵
    lookback_days: int = 252


def compute_risk_metrics(
    returns: dict[str, list[float]],
) -> RiskMetrics:
    """从日收益率序列计算波动率 + 相关性 + 协方差矩阵。"""
    import numpy as np

    assets = sorted(returns.keys())
    n = len(assets)

    # 构建收益率矩阵 (T x N)
    T = len(returns[assets[0]])
    ret_matrix = np.zeros((T, n))
    for j, asset in enumerate(assets):
        ret_matrix[:, j] = returns[asset][:T]

    # 协方差矩阵（年化）
    cov_daily = np.cov(ret_matrix, rowvar=False)
    cov_annual = cov_daily * 252

    # 波动率
    vols = {assets[j]: float(np.sqrt(cov_annual[j, j])) for j in range(n)}

    # 相关性矩阵
    corr = np.corrcoef(ret_matrix, rowvar=False)
    corr_dict: dict[str, dict[str, float]] = {}
    cov_dict: dict[str, dict[str, float]] = {}
    for i, a1 in enumerate(assets):
        corr_dict[a1] = {}
        cov_dict[a1] = {}
        for j, a2 in enumerate(assets):
            corr_dict[a1][a2] = float(corr[i, j])
            cov_dict[a1][a2] = float(cov_annual[i, j])

    return RiskMetrics(
        volatilities=vols,
        correlation_matrix=corr_dict,
        covariance_matrix=cov_dict,
        lookback_days=T,
    )


# ── 等风险贡献（ERC）优化 ──────────────────────────────────


def compute_erc_weights(
    risk_metrics: RiskMetrics,
    asset_names: list[str] | None = None,
    max_iterations: int = 500,
    tolerance: float = 1e-8,
) -> dict[str, float]:
    """等风险贡献（Equal Risk Contribution）权重优化。

    目标: 每个资产对组合总风险的贡献相等。
    方法: Maillard, Roncalli, Teïletche (2010) 的 Cyclical Coordinate Descent。

    比简单的乘法调整更稳健——每次只调一个权重，逐个坐标下降。
    """
    import numpy as np

    if asset_names is None:
        asset_names = sorted(risk_metrics.volatilities.keys())

    n = len(asset_names)
    if n == 0:
        return {}

    # 构建协方差矩阵
    cov = np.zeros((n, n))
    for i, a1 in enumerate(asset_names):
        for j, a2 in enumerate(asset_names):
            cov[i, j] = risk_metrics.covariance_matrix.get(a1, {}).get(a2, 0.0)

    # 初始权重: 等权
    w = np.ones(n) / n

    # Cyclical Coordinate Descent
    for iteration in range(max_iterations):
        w_old = w.copy()

        for i in range(n):
            # 组合方差
            port_var = float(w @ cov @ w)
            if port_var <= 0:
                break

            # 资产 i 的边际风险贡献
            marginal_i = float(cov[i, :] @ w)

            # 其他资产的协方差贡献
            cross_term = float(cov[i, :] @ w) - w[i] * cov[i, i]

            # 求解: w_i 使得 w_i * marginal_i = target_rc
            # target_rc = sqrt(port_var) / n
            # w_i * (w_i * cov[i,i] + cross_term) = target_rc^2 / n... 简化为二次方程
            a_coeff = cov[i, i]
            b_coeff = cross_term
            c_coeff = -port_var / n  # 目标: 每个资产贡献 = port_var / n (不开根号的版本)

            # w_i^2 * a + w_i * b + c = 0 → 解二次方程
            discriminant = b_coeff ** 2 - 4 * a_coeff * c_coeff
            if discriminant < 0 or a_coeff == 0:
                continue

            w_i_new = (-b_coeff + math.sqrt(discriminant)) / (2 * a_coeff)
            w[i] = max(w_i_new, 1e-6)  # 确保正权重

        # 归一化
        w = w / w.sum()

        # 收敛检查
        delta = float(np.max(np.abs(w - w_old)))
        if delta < tolerance:
            break

    return {asset_names[i]: round(float(w[i]), 4) for i in range(n)}


def verify_risk_contributions(
    weights: dict[str, float],
    risk_metrics: RiskMetrics,
) -> dict[str, float]:
    """验证每个资产的风险贡献是否相等。"""
    import numpy as np

    assets = sorted(weights.keys())
    n = len(assets)
    w = np.array([weights[a] for a in assets])

    cov = np.zeros((n, n))
    for i, a1 in enumerate(assets):
        for j, a2 in enumerate(assets):
            cov[i, j] = risk_metrics.covariance_matrix.get(a1, {}).get(a2, 0.0)

    port_var = float(w @ cov @ w)
    if port_var <= 0:
        return {a: 0.0 for a in assets}
    port_vol = math.sqrt(port_var)

    marginal = cov @ w
    risk_contrib = w * marginal / port_vol

    return {assets[i]: round(float(risk_contrib[i]), 4) for i in range(n)}


# ── 四象限覆盖验证 ─────────────────────────────────────────


def verify_quadrant_coverage(
    weights: dict[str, float],
) -> dict[str, list[str]]:
    """验证每个象限都有足够的资产覆盖。"""
    quadrants = {
        "growth_up_inflation_up": [],
        "growth_up_inflation_down": [],
        "growth_down_inflation_up": [],
        "growth_down_inflation_down": [],
    }

    asset_map = {a.name: a for a in ASSET_CLASSES}
    for asset_name, weight in weights.items():
        ac = asset_map.get(asset_name)
        if ac is None or weight < 0.01:
            continue
        for q in ac.quadrants:
            if q in quadrants:
                quadrants[q].append(f"{asset_name}({weight:.0%})")

    return quadrants


# ── 组合指标 ───────────────────────────────────────────────


@dataclass
class AllWeatherResult:
    """全天候组合构建结果。"""
    weights: dict[str, float]
    method: str                                    # "erc" / "inverse_vol" / "bridgewater_approx"
    risk_metrics: RiskMetrics | None = None
    risk_contributions: dict[str, float] = field(default_factory=dict)
    portfolio_volatility: float = 0.0
    quadrant_coverage: dict[str, list[str]] = field(default_factory=dict)
    detail: str = ""


def build_all_weather(
    asset_names: list[str] | None = None,
    lookback_days: int = 252,
) -> AllWeatherResult:
    """构建全天候组合。

    优先级:
    1. 有 ETF 数据 → ERC 优化（完整版）
    2. 有波动率但无相关性 → 逆波动率（简化版）
    3. 无数据 → Bridgewater 公开近似权重（降级版）
    """
    if asset_names is None:
        asset_names = DEFAULT_ASSET_NAMES

    etf_map = {}
    asset_map = {a.name: a for a in ASSET_CLASSES}
    for name in asset_names:
        ac = asset_map.get(name)
        if ac:
            etf_map[name] = ac.etf

    # 尝试获取收益率数据
    returns = _fetch_returns(etf_map, lookback_days)

    if returns is not None:
        # 完整版: ERC 优化
        risk_metrics = compute_risk_metrics(returns)
        weights = compute_erc_weights(risk_metrics, asset_names)
        risk_contribs = verify_risk_contributions(weights, risk_metrics)

        import numpy as np
        w = np.array([weights[a] for a in asset_names])
        n = len(asset_names)
        cov = np.zeros((n, n))
        for i, a1 in enumerate(asset_names):
            for j, a2 in enumerate(asset_names):
                cov[i, j] = risk_metrics.covariance_matrix.get(a1, {}).get(a2, 0.0)
        port_vol = float(np.sqrt(w @ cov @ w))

        coverage = verify_quadrant_coverage(weights)

        return AllWeatherResult(
            weights=weights,
            method="erc",
            risk_metrics=risk_metrics,
            risk_contributions=risk_contribs,
            portfolio_volatility=port_vol,
            quadrant_coverage=coverage,
            detail=f"ERC 优化 ({risk_metrics.lookback_days} 日数据)",
        )

    # 降级: Bridgewater 近似权重
    weights = {k: v for k, v in BRIDGEWATER_APPROXIMATE.items() if k in asset_names}
    total = sum(weights.values())
    if total > 0:
        weights = {k: round(v / total, 4) for k, v in weights.items()}

    coverage = verify_quadrant_coverage(weights)

    return AllWeatherResult(
        weights=weights,
        method="bridgewater_approx",
        quadrant_coverage=coverage,
        detail="Bridgewater 公开近似权重（无 ETF 数据）",
    )


# ── 格式化 ─────────────────────────────────────────────────


# ── 动态再平衡 ─────────────────────────────────────────────


@dataclass
class RebalanceAction:
    """再平衡动作。"""
    asset: str
    current_weight: float
    target_weight: float
    trade_direction: str    # "buy" / "sell"
    trade_size: float       # 交易量占组合比例
    reason: str


@dataclass
class RebalanceResult:
    """再平衡检查结果。"""
    needs_rebalance: bool
    actions: list[RebalanceAction] = field(default_factory=list)
    max_drift: float = 0.0
    estimated_turnover: float = 0.0       # 总换手率
    estimated_cost: float = 0.0           # 估计交易成本 (%)
    detail: str = ""


def check_rebalance(
    current_weights: dict[str, float],
    target_weights: dict[str, float],
    threshold: float = 0.05,
    cost_bps: float = 10.0,
) -> RebalanceResult:
    """检查是否需要再平衡。

    策略: 阈值触发 — 任何资产偏离目标 > threshold 时触发。
    这比定期再平衡更高效（只在需要时交易，减少成本）。

    Args:
        current_weights: 当前实际权重
        target_weights: ERC 目标权重
        threshold: 偏离触发阈值 (默认 5%)
        cost_bps: 单边交易成本 (bps, 默认 10bp)
    """
    result = RebalanceResult(needs_rebalance=False)

    all_assets = set(list(current_weights.keys()) + list(target_weights.keys()))
    total_turnover = 0.0

    for asset in sorted(all_assets):
        current = current_weights.get(asset, 0.0)
        target = target_weights.get(asset, 0.0)
        drift = current - target

        if abs(drift) > result.max_drift:
            result.max_drift = abs(drift)

        if abs(drift) > threshold:
            result.needs_rebalance = True
            direction = "sell" if drift > 0 else "buy"
            trade = abs(drift)
            total_turnover += trade

            result.actions.append(RebalanceAction(
                asset=asset,
                current_weight=current,
                target_weight=target,
                trade_direction=direction,
                trade_size=round(trade, 4),
                reason=f"偏离 {drift:+.1%} > 阈值 {threshold:.0%}",
            ))

    result.estimated_turnover = round(total_turnover, 4)
    result.estimated_cost = round(total_turnover * cost_bps / 10000, 6)

    if result.needs_rebalance:
        result.detail = (
            f"需要再平衡: {len(result.actions)} 个资产偏离, "
            f"最大偏离 {result.max_drift:.1%}, "
            f"换手率 {result.estimated_turnover:.1%}, "
            f"估计成本 {result.estimated_cost:.3%}"
        )
    else:
        result.detail = f"无需再平衡: 最大偏离 {result.max_drift:.1%} < 阈值 {threshold:.0%}"

    return result


def simulate_drift(
    weights: dict[str, float],
    returns: dict[str, float],
) -> dict[str, float]:
    """模拟一期回报后的权重漂移。"""
    new_values = {}
    for asset, w in weights.items():
        r = returns.get(asset, 0.0)
        new_values[asset] = w * (1 + r / 100.0)

    total = sum(new_values.values())
    if total <= 0:
        return weights
    return {k: v / total for k, v in new_values.items()}


# ── 回测引擎 ──────────────────────────────────────────────


@dataclass
class BacktestYear:
    """单年回测结果。"""
    year: str
    label: str
    start_weights: dict[str, float]
    end_weights: dict[str, float]
    portfolio_return: float
    rebalanced: bool
    rebalance_cost: float = 0.0


@dataclass
class BacktestResult:
    """完整回测结果。"""
    years: list[BacktestYear]
    strategy: str                       # "buy_hold" / "threshold_5pct" / "annual"
    total_return: float                 # 累计回报
    annualized_return: float
    worst_year: float
    best_year: float
    max_drawdown: float                 # 最大回撤（简化: 年度粒度）
    positive_years: int
    total_years: int
    total_cost: float                   # 总交易成本


def backtest_all_weather(
    initial_weights: dict[str, float],
    annual_returns: dict[str, dict],
    rebalance_strategy: str = "threshold_5pct",
    threshold: float = 0.05,
    cost_bps: float = 10.0,
) -> BacktestResult:
    """全天候组合回测。

    策略:
    - "buy_hold": 买入持有，不再平衡
    - "annual": 每年末再平衡回目标权重
    - "threshold_5pct": 偏离 > 5% 时再平衡
    """
    years_list: list[BacktestYear] = []
    current_weights = initial_weights.copy()
    cumulative = 1.0
    peak = 1.0
    max_dd = 0.0
    total_cost = 0.0

    sorted_years = sorted(annual_returns.keys())

    for year in sorted_years:
        data = annual_returns[year]
        returns = {k: v for k, v in data.items() if k not in ("quadrant", "label")}
        label = data.get("label", "")

        # 计算组合回报
        port_ret = sum(current_weights.get(a, 0) * returns.get(a, 0) / 100
                       for a in current_weights)

        start_w = current_weights.copy()

        # 权重漂移
        drifted = simulate_drift(current_weights, returns)

        # 再平衡检查
        rebalanced = False
        rb_cost = 0.0

        if rebalance_strategy == "annual":
            rb = check_rebalance(drifted, initial_weights, threshold=0.0, cost_bps=cost_bps)
            current_weights = initial_weights.copy()
            rebalanced = True
            rb_cost = rb.estimated_cost
        elif rebalance_strategy == "threshold_5pct":
            rb = check_rebalance(drifted, initial_weights, threshold=threshold, cost_bps=cost_bps)
            if rb.needs_rebalance:
                current_weights = initial_weights.copy()
                rebalanced = True
                rb_cost = rb.estimated_cost
            else:
                current_weights = drifted
        else:  # buy_hold
            current_weights = drifted

        total_cost += rb_cost

        # 累计
        cumulative *= (1 + port_ret - rb_cost)
        if cumulative > peak:
            peak = cumulative
        dd = (peak - cumulative) / peak
        if dd > max_dd:
            max_dd = dd

        years_list.append(BacktestYear(
            year=year, label=label,
            start_weights=start_w,
            end_weights=current_weights.copy(),
            portfolio_return=round(port_ret * 100, 2),
            rebalanced=rebalanced,
            rebalance_cost=round(rb_cost * 100, 4),
        ))

    returns_list = [y.portfolio_return for y in years_list]
    n = len(returns_list)
    ann_ret = (cumulative ** (1.0 / n) - 1) * 100 if n > 0 else 0

    return BacktestResult(
        years=years_list,
        strategy=rebalance_strategy,
        total_return=round((cumulative - 1) * 100, 2),
        annualized_return=round(ann_ret, 2),
        worst_year=min(returns_list) if returns_list else 0,
        best_year=max(returns_list) if returns_list else 0,
        max_drawdown=round(max_dd * 100, 2),
        positive_years=sum(1 for r in returns_list if r > 0),
        total_years=n,
        total_cost=round(total_cost * 100, 4),
    )


def format_all_weather(result: AllWeatherResult) -> str:
    """格式化全天候组合报告。"""
    lines = [""]
    lines.append("  全天候（All Weather）组合")
    lines.append("  ════════════════════════════════════════════════")
    lines.append(f"  方法: {result.detail}")

    # 权重
    lines.append("\n  资产配置:")
    for asset, w in sorted(result.weights.items(), key=lambda x: -x[1]):
        rc = result.risk_contributions.get(asset)
        rc_str = f"  RC={rc:.1%}" if rc else ""
        vol = result.risk_metrics.volatilities.get(asset) if result.risk_metrics else None
        vol_str = f"  vol={vol:.1%}" if vol else ""
        lines.append(f"    {asset:25s} {w:6.1%}{vol_str}{rc_str}")

    if result.portfolio_volatility > 0:
        lines.append(f"\n  组合波动率: {result.portfolio_volatility:.1%}")

    # 风险贡献均衡度
    if result.risk_contributions:
        rcs = list(result.risk_contributions.values())
        max_rc = max(rcs) if rcs else 0
        min_rc = min(rcs) if rcs else 0
        spread = max_rc - min_rc if rcs else 0
        lines.append(f"  风险贡献: max={max_rc:.1%} min={min_rc:.1%} 差={spread:.2%}")
        if spread < 0.02:
            lines.append(f"  ✓ 风险贡献均衡（差异 < 2%）")
        else:
            lines.append(f"  ⚠ 风险贡献不均（差异 {spread:.1%}）")

    # 相关性矩阵
    if result.risk_metrics:
        lines.append("\n  相关性矩阵:")
        assets = sorted(result.risk_metrics.correlation_matrix.keys())
        header = "    " + " ".join(f"{a[:6]:>7s}" for a in assets)
        lines.append(header)
        for a1 in assets:
            row = f"    {a1[:6]:6s}"
            for a2 in assets:
                corr = result.risk_metrics.correlation_matrix[a1][a2]
                row += f" {corr:+6.2f} "
            lines.append(row)

    # 四象限覆盖
    lines.append("\n  四象限覆盖:")
    quadrant_labels = {
        "growth_up_inflation_up": "增长↑通胀↑",
        "growth_up_inflation_down": "增长↑通胀↓",
        "growth_down_inflation_up": "增长↓通胀↑",
        "growth_down_inflation_down": "增长↓通胀↓",
    }
    for q, label in quadrant_labels.items():
        assets_in_q = result.quadrant_coverage.get(q, [])
        status = "✓" if assets_in_q else "✗"
        lines.append(f"    {status} {label}: {', '.join(assets_in_q) if assets_in_q else '无覆盖!'}")

    lines.append("")
    return "\n".join(lines)
