"""
Axion 回测引擎
==============

从执行层视角运行历史回测: FRED数据 → 五大力量 → 信号 → 组合调整 → 月度P&L。

原始逻辑来自 tests/backtest_dalio_full.py，重构为可配置的 BacktestEngine 类。

策略层级:
  1. 纯 All Weather (固定权重)
  2. All Weather + 大周期倾斜 (长期信号)
  3. All Weather + 大周期 + Pure Alpha (中期信号)
  4. All Weather + 大周期 + PA + 索罗斯 (全部)

用法:
  python -m axion.backtest          # 标准回测 (pure 模式)
  python -m axion.backtest --full   # full 模式
"""

from __future__ import annotations

import json
import math
import statistics
from dataclasses import dataclass, field
from pathlib import Path


# ══════════════════════════════════════════════════════════════
#  数据路径
# ══════════════════════════════════════════════════════════════

# 数据文件暂时留在 tests/，用项目根定位
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent  # src/axion/backtest.py → project root
_TESTS_DIR = _PROJECT_ROOT / "tests"


def _load_json(name: str) -> dict:
    """加载 tests/ 下的 JSON 数据文件。"""
    path = _TESTS_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"数据文件不存在: {path}")
    with open(path) as f:
        return json.load(f)


# ══════════════════════════════════════════════════════════════
#  BacktestResult
# ══════════════════════════════════════════════════════════════


@dataclass
class BacktestResult:
    """结构化回测结果。"""
    strategy: str
    mode: str
    leverage: float
    months: int
    ann_return: float
    ann_vol: float
    sharpe: float
    cumulative: float
    max_drawdown: float
    worst_month: float
    best_month: float
    positive_months: int
    monthly_rets: list[float] = field(default_factory=list, repr=False)
    equity_curve: list[float] = field(default_factory=list, repr=False)
    weight_history: list = field(default_factory=list, repr=False)


# ══════════════════════════════════════════════════════════════
#  FRED 历史数据
# ══════════════════════════════════════════════════════════════

FRED_CACHE_PATH = _TESTS_DIR / "data_fred_monthly_history.json"


def fetch_fred_history() -> dict:
    """从 FRED 拉取 2006-2025 所有达利欧需要的序列，缓存到本地。"""
    from anchor.config import settings
    from fredapi import Fred
    import pandas as pd

    api_key = settings.fred_api_key
    if not api_key:
        raise RuntimeError("需要 FRED_API_KEY")

    fred = Fred(api_key=api_key)
    start = "2005-01-01"

    series_map = {
        "fed_funds_rate": "FEDFUNDS",
        "cpi_index": "CPIAUCSL",
        "gdp_growth": "A191RL1Q225SBEA",
        "unemployment": "UNRATE",
        "credit_total": "TOTBKCR",
        "total_debt_gdp": "GFDEGDQ188S",
        "mortgage_delinquency": "DRSFRMACBS",
        "financial_leverage": "NFCILEVERAGE",
        "lending_standards": "DRTSCILM",
        "household_debt_gdp": "HDTGPDUSQ163N",
        "mortgage_debt_service": "MDSP",
        "consumer_sentiment": "UMCSENT",
        "gini": "SIPOVGINIUSA",
        "fiscal_deficit_gdp": "FYFSGDA188S",
        "nonfarm_productivity": "OPHNFB",
        "trade_balance": "BOPGSTB",
        "dollar_index": "DTWEXBGS",
        "wti_oil": "DCOILWTICO",
        "epu_index": "USEPUINDXD",
        "food_cpi": "CPIUFDSL",
        "rd_spending": "Y694RC1Q027SBEA",
        "nasdaq": "NASDAQCOM",
        "credit_spread_hy": "BAMLH0A0HYM2",
        "credit_spread_ig": "BAMLC0A0CM",
        "vix_daily": "VIXCLS",
    }

    result = {}
    for key, sid in series_map.items():
        try:
            s = fred.get_series(sid, observation_start=start)
            if s is not None and not s.empty:
                monthly = {}
                for idx, val in s.items():
                    if val != val:  # NaN
                        continue
                    ym = idx.strftime("%Y-%m")
                    monthly[ym] = float(val)
                result[key] = monthly
                print(f"  {key}: {len(monthly)} months")
        except Exception as e:
            print(f"  {key}: FAILED ({e})")

    with open(FRED_CACHE_PATH, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nSaved to {FRED_CACHE_PATH}")
    return result


def load_fred_history() -> dict:
    """加载缓存的 FRED 历史数据。"""
    if not FRED_CACHE_PATH.exists():
        print("FRED 历史缓存不存在，正在拉取...")
        return fetch_fred_history()
    with open(FRED_CACHE_PATH) as f:
        return json.load(f)


# ══════════════════════════════════════════════════════════════
#  工具函数
# ══════════════════════════════════════════════════════════════


def _yoy(series: dict, month: str) -> float | None:
    """计算同比增速。"""
    year = int(month[:4])
    rest = month[4:]
    prev_month = f"{year - 1}{rest}"
    cur = series.get(month)
    prev = series.get(prev_month)
    if cur is not None and prev is not None and prev != 0:
        return ((cur / prev) - 1) * 100
    return None


def _latest_value(series: dict, month: str) -> float | None:
    """获取某月或之前最近的值（处理季度/年度序列）。"""
    if month in series:
        return series[month]
    year, mo = int(month[:4]), int(month[5:7])
    for back in range(1, 13):
        mo2 = mo - back
        yr2 = year
        if mo2 <= 0:
            mo2 += 12
            yr2 -= 1
        key = f"{yr2}-{mo2:02d}"
        if key in series:
            return series[key]
    return None


def _compute_momentum(monthly_returns: dict, fred: dict, end_month: str, months: int = 12) -> dict:
    """从月度回报算各资产的 N 月动量。"""
    all_months = sorted(monthly_returns.keys())
    end_idx = all_months.index(end_month) if end_month in all_months else len(all_months) - 1
    start_idx = max(0, end_idx - months + 1)
    window = all_months[start_idx:end_idx + 1]

    momentum = {}
    for asset in ["equity", "long_term_bond", "gold", "commodity", "em_bond"]:
        cum = 1.0
        for m in window:
            r = monthly_returns.get(m, {}).get(asset, 0)
            cum *= (1 + r / 100)
        momentum[asset] = round((cum - 1) * 100, 1)
    return momentum


def _compute_trend_duration(monthly_returns: dict, end_month: str) -> int:
    """估算股票正动量持续了几个月。"""
    all_months = sorted(monthly_returns.keys())
    end_idx = all_months.index(end_month) if end_month in all_months else len(all_months) - 1

    duration = 0
    for i in range(end_idx, max(0, end_idx - 60), -1):
        m = all_months[i]
        start_6m = max(0, i - 5)
        window_rets = [monthly_returns.get(all_months[j], {}).get("equity", 0) for j in range(start_6m, i + 1)]
        avg = sum(window_rets) / len(window_rets) if window_rets else 0
        if avg > 0:
            duration += 1
        else:
            break
    return duration


# ══════════════════════════════════════════════════════════════
#  五大力量数据构造
# ══════════════════════════════════════════════════════════════


def build_monthly_forces_data(fred: dict, month: str) -> tuple[dict, dict, dict, dict, dict]:
    """从 FRED 历史数据构造某月的五大力量输入。"""
    # F1
    macro = {}
    v = _latest_value(fred.get("fed_funds_rate", {}), month)
    if v is not None:
        macro["fed_funds_rate"] = v
    v = _yoy(fred.get("cpi_index", {}), month)
    if v is not None:
        macro["cpi_actual"] = v
    v = _latest_value(fred.get("gdp_growth", {}), month)
    if v is not None:
        macro["gdp_growth_actual"] = v
    v = _latest_value(fred.get("unemployment", {}), month)
    if v is not None:
        macro["unemployment_rate"] = v
    v = _yoy(fred.get("credit_total", {}), month)
    if v is not None:
        macro["credit_growth"] = v
    v = _latest_value(fred.get("total_debt_gdp", {}), month)
    if v is not None:
        macro["total_debt_to_gdp"] = v
    v = _latest_value(fred.get("mortgage_delinquency", {}), month)
    if v is not None:
        macro["mortgage_delinquency"] = v
    v = _latest_value(fred.get("financial_leverage", {}), month)
    if v is not None:
        macro["financial_leverage"] = v
    v = _latest_value(fred.get("lending_standards", {}), month)
    if v is not None:
        macro["lending_standards"] = v
    v = _latest_value(fred.get("household_debt_gdp", {}), month)
    if v is not None:
        macro["household_debt_gdp"] = v
    v = _latest_value(fred.get("mortgage_debt_service", {}), month)
    if v is not None:
        macro["mortgage_debt_service"] = v

    # F2
    internal = {}
    v = _latest_value(fred.get("consumer_sentiment", {}), month)
    if v is not None:
        internal["consumer_sentiment"] = v
    v = _latest_value(fred.get("gini", {}), month)
    if v is not None:
        internal["gini_coefficient"] = v
    v = _latest_value(fred.get("fiscal_deficit_gdp", {}), month)
    if v is not None:
        internal["fiscal_deficit_to_gdp"] = abs(v)
    v = _yoy(fred.get("nonfarm_productivity", {}), month)
    if v is not None:
        internal["nonfarm_productivity_growth"] = v

    # F3
    external = {}
    v = _yoy(fred.get("wti_oil", {}), month)
    if v is not None:
        external["oil_price_yoy"] = v
    v = _yoy(fred.get("dollar_index", {}), month)
    if v is not None:
        external["dollar_index_yoy"] = v
    v = _latest_value(fred.get("trade_balance", {}), month)
    if v is not None:
        external["trade_balance"] = v / 1000
    v = _latest_value(fred.get("epu_index", {}), month)
    if v is not None:
        external["epu_index"] = v

    # F4
    nature = {}
    v = _yoy(fred.get("food_cpi", {}), month)
    if v is not None:
        nature["food_price_yoy"] = v

    # F5
    tech = {}
    v = _yoy(fred.get("nonfarm_productivity", {}), month)
    if v is not None:
        tech["productivity_growth"] = v
    v = _yoy(fred.get("rd_spending", {}), month)
    if v is not None:
        tech["rd_spending_growth"] = v
    v = _yoy(fred.get("nasdaq", {}), month)
    if v is not None:
        tech["nasdaq_yoy"] = v

    return macro, internal, external, nature, tech


# ══════════════════════════════════════════════════════════════
#  权重调整逻辑
# ══════════════════════════════════════════════════════════════

from axion.strategies.all_weather import BRIDGEWATER_APPROXIMATE
from polaris.chains.dalio_forces import build_five_forces_view, ForceDirection
from polaris.chains.dalio import MacroContext, _propagate_causal_graph
from polaris.chains.dalio_simulation import (
    analyze_principal_contradiction,
    build_scenarios_for_force,
    simulate_scenarios,
    generate_time_tagged_signals,
    identify_long_cycle_position,
    compute_force_shocks,
    inject_shocks_to_nodes,
    PortfolioLayer,
    TimeHorizon,
)

# All Weather 大周期倾斜规则
BIG_CYCLE_TILTS = {
    "debt_late": {
        "gold": +0.05,
        "inflation_linked_bond": +0.05,
        "long_term_bond": -0.05,
        "intermediate_bond": -0.03,
        "equity": -0.02,
    },
    "empire_declining": {
        "gold": +0.03,
        "equity": -0.02,
        "em_bond": -0.01,
    },
    "tech_revolution": {
        "equity": +0.03,
        "commodity": -0.02,
        "gold": -0.01,
    },
}

PA_MAX_TILT = 0.08  # Pure Alpha 单资产最大调整幅度
SOROS_MAX_TILT = 0.10  # 索罗斯单资产最大调整幅度


def apply_big_cycle_tilts(base_weights: dict, macro_data: dict) -> dict:
    """根据大周期位置调整 All Weather 权重。"""
    tilts = dict(base_weights)

    # 加息风暴检测: 通胀失控 + 零利率 → 久期全杀
    cpi = macro_data.get("cpi_actual", 0)
    rate = macro_data.get("fed_funds_rate", 0)

    if cpi > 5 and rate < cpi - 3:
        duration_penalty = min((cpi - rate - 3) * 0.03, 0.15)
        tilts["long_term_bond"] = max(0, tilts.get("long_term_bond", 0) - duration_penalty * 1.5)
        tilts["intermediate_bond"] = max(0, tilts.get("intermediate_bond", 0) - duration_penalty)
        tilts["inflation_linked_bond"] = max(0, tilts.get("inflation_linked_bond", 0) - duration_penalty * 0.5)
        tilts["equity"] = max(0, tilts.get("equity", 0) - duration_penalty * 0.5)
        tilts["commodity"] = tilts.get("commodity", 0) + duration_penalty * 1.5
        tilts["gold"] = tilts.get("gold", 0) + duration_penalty * 0.5

    # 常规大周期倾斜
    positions = identify_long_cycle_position(macro_data)
    for pos in positions:
        for asset, delta in BIG_CYCLE_TILTS.get(pos, {}).items():
            if asset in tilts:
                tilts[asset] = max(0, tilts[asset] + delta)

    # 归一化
    total = sum(tilts.values())
    if total > 0:
        tilts = {k: v / total for k, v in tilts.items()}
    return tilts


def apply_pure_alpha(base_weights: dict, view, graph_nodes, macro_data: dict) -> dict:
    """根据主要矛盾的场景推演调整权重。"""
    analysis = analyze_principal_contradiction(view, graph_nodes)
    if not analysis.principal or analysis.principal.score < 0.01:
        return base_weights

    force = view.get_force(analysis.principal.force_id)
    if not force:
        return base_weights

    shocks = compute_force_shocks(view)
    if not shocks:
        return base_weights

    from polaris.chains.dalio import _compute_asset_impacts
    shocked_nodes = inject_shocks_to_nodes(graph_nodes, shocks)
    impacts = _compute_asset_impacts(shocked_nodes)

    ASSET_MAP = {
        "equity_cyclical": "equity",
        "equity_defensive": "equity",
        "long_term_bond": "long_term_bond",
        "intermediate_bond": "intermediate_bond",
        "commodity": "commodity",
        "gold": "gold",
        "inflation_linked_bond": "inflation_linked_bond",
        "em_bond": "em_bond",
        "cash": None,
    }

    tilts = dict(base_weights)
    for ai in impacts:
        mapped = ASSET_MAP.get(ai.asset_type)
        if mapped and mapped in tilts:
            sign = 1 if ai.direction == "overweight" else -1
            delta = sign * min(abs(ai.raw_score) * 0.3, PA_MAX_TILT)
            delta *= min(analysis.principal.score * 5, 1.0)
            tilts[mapped] = max(0, tilts[mapped] + delta)

    total = sum(tilts.values())
    if total > 0:
        tilts = {k: v / total for k, v in tilts.items()}
    return tilts


def build_market_state(
    monthly_returns: dict, fred: dict, signals: dict, month: str,
    force_directions: dict | None = None,
):
    """从历史数据构造索罗斯需要的 MarketState。"""
    from polaris.chains.soros import MarketState

    mom_12 = _compute_momentum(monthly_returns, fred, month, 12)
    mom_3 = _compute_momentum(monthly_returns, fred, month, 3)
    mom_6 = _compute_momentum(monthly_returns, fred, month, 6)
    duration = _compute_trend_duration(monthly_returns, month)

    sig = signals.get(month, {})
    vix = sig.get("vix")
    yc = sig.get("yield_curve")

    all_months = sorted(signals.keys())
    idx = all_months.index(month) if month in all_months else -1
    vix_change = sig.get("vix_change")
    vix_6m_ago = None
    if idx >= 6:
        vix_6m_ago = signals.get(all_months[idx - 6], {}).get("vix")

    hy = _latest_value(fred.get("credit_spread_hy", {}), month)
    hy_3m_ago = None
    if idx >= 3:
        hy_3m_ago = _latest_value(fred.get("credit_spread_hy", {}), all_months[idx - 3])
    hy_6m_ago = None
    if idx >= 6:
        hy_6m_ago = _latest_value(fred.get("credit_spread_hy", {}), all_months[idx - 6])

    spread_chg_3m = None
    if hy is not None and hy_3m_ago is not None:
        spread_chg_3m = hy - hy_3m_ago

    market = MarketState(
        momentum_equity=mom_12.get("equity"),
        momentum_long_bond=mom_12.get("long_term_bond"),
        momentum_gold=mom_12.get("gold"),
        momentum_commodity=mom_12.get("commodity"),
        momentum_em_bond=mom_12.get("em_bond"),
        vix=vix,
        vix_change_1m=vix_change,
        vix_6m_ago=vix_6m_ago,
        credit_spread_hy=hy,
        credit_spread_change_3m=spread_chg_3m,
        credit_spread_hy_6m_ago=hy_6m_ago,
        yield_curve_10y_3m=yc,
        trend_duration_months=duration,
        momentum_equity_3m=mom_3.get("equity"),
        momentum_equity_6m=mom_6.get("equity"),
        snapshot_date=month,
    )

    return market, force_directions


def apply_soros_overlay(base_weights: dict, market, force_directions: dict | None = None) -> dict:
    """索罗斯信号叠加到权重上。"""
    from polaris.chains.soros import evaluate_soros

    insight = evaluate_soros(market, force_directions=force_directions)
    ts = insight.trade_signal
    if not ts:
        return base_weights

    weights = dict(base_weights)

    for asset, ride_w in ts.ride_assets.items():
        if asset in weights:
            weights[asset] = weights[asset] + ride_w * SOROS_MAX_TILT

    for asset, hedge_w in ts.hedge_assets.items():
        if asset in ("gold", "long_term_bond", "intermediate_bond"):
            if asset in weights:
                weights[asset] = weights[asset] + hedge_w * SOROS_MAX_TILT
        else:
            if asset in weights:
                weights[asset] = max(0, weights[asset] - hedge_w * SOROS_MAX_TILT)

    if insight.reflexivity_feedback and insight.reflexivity_feedback.fragility > 0.5:
        frag = insight.reflexivity_feedback.fragility
        cash_shift = (frag - 0.5) * 0.15
        total = sum(weights.values())
        if total > 0:
            scale = max(0.8, 1 - cash_shift)
            weights = {k: v * scale for k, v in weights.items()}

    total = sum(weights.values())
    if total > 0:
        weights = {k: v / total for k, v in weights.items()}
    return weights


# ══════════════════════════════════════════════════════════════
#  BacktestEngine
# ══════════════════════════════════════════════════════════════


class BacktestEngine:
    """可配置的回测引擎。

    strategy:
      "aw_only"              — 纯 All Weather (Bridgewater 近似)
      "aw_cycle"             — All Weather + 大周期倾斜
      "aw_cycle_alpha"       — All Weather + 大周期 + Pure Alpha
      "aw_cycle_alpha_soros" — 全部: AW + 大周期 + PA + 索罗斯

    mode:
      "pure" — 百分位+趋势+金融原理 (默认)
      "full" — 现有逻辑（含所有后验补丁和硬编码阈值）
    """

    def __init__(
        self,
        strategy: str = "aw_cycle_alpha_soros",
        leverage: float = 1.0,
        mode: str = "pure",
        rebalance_freq: int = 3,
        derivative_yaml: str | None = None,
        use_stock_selection: bool = False,
        max_stock_corr: float = 0.7,
    ):
        self.strategy = strategy
        self.leverage = leverage
        self.mode = mode
        self.rebalance_freq = rebalance_freq
        self.derivative_yaml = derivative_yaml
        self.use_stock_selection = use_stock_selection
        self.max_stock_corr = max_stock_corr

        # 加载数据
        self.monthly_returns = _load_json("data_monthly_returns.json")
        self.monthly_signals = _load_json("data_monthly_signals.json")
        self.fred = load_fred_history()
        self.base_aw = dict(BRIDGEWATER_APPROXIMATE)

        # 选股数据 (按需加载)
        self._berkshire_holdings = None
        self._stock_returns = None
        if self.use_stock_selection:
            self._load_stock_data()

    def _load_stock_data(self):
        """加载选股所需数据。"""
        from axion.stock_selection import load_berkshire_holdings, load_stock_returns
        self._berkshire_holdings = load_berkshire_holdings()
        self._stock_returns = load_stock_returns()

    def _get_stock_portfolio_return(self, month: str, force_score: float = 0.0) -> float | None:
        """获取选股组合的月回报。

        force_score: 达利欧Force方向 (-1到+1)，控制选股严格度:
          负面(< -0.5) → conservative: 低相关性(0.5)，更分散
          中性          → neutral: 中等相关性(0.7)
          正面(> 0.5)  → aggressive: 高相关性(0.85)，允许集中
        """
        if not self._berkshire_holdings or not self._stock_returns:
            return None

        from axion.stock_selection import get_buffett_portfolio, compute_stock_portfolio_return
        from polaris.chains.buffett import ScreeningProfile

        # 根据市场状态调整选股参数
        profile = ScreeningProfile.from_force_direction(force_score)
        if profile.name == "conservative":
            max_corr = 0.5   # 更严格的相关性过滤，更分散
        elif profile.name == "aggressive":
            max_corr = 0.85  # 允许更集中
        else:
            max_corr = 0.7   # 中等

        portfolio = get_buffett_portfolio(
            month, self._berkshire_holdings, self._stock_returns,
            max_corr=max_corr,
        )
        if not portfolio:
            return None
        return compute_stock_portfolio_return(portfolio, self._stock_returns, month)

    def run(self) -> BacktestResult:
        """执行回测，返回结果。"""
        months = sorted(self.monthly_returns.keys())

        cumulative = 1.0
        peak = 1.0
        max_dd = 0.0
        monthly_rets: list[float] = []
        equity_curve = [1.0]
        weight_history: list = []

        current_weights = {k: v * self.leverage for k, v in self.base_aw.items()}
        dalio_base_weights = dict(self.base_aw)
        soros_prior_phase = None
        force_dirs = None

        for i, month in enumerate(months):
            rets = self.monthly_returns[month]

            # 达利欧层: 按 rebalance_freq 再平衡
            if i % self.rebalance_freq == 0:
                macro_d, internal_d, external_d, nature_d, tech_d = \
                    build_monthly_forces_data(self.fred, month)
                all_data = macro_d | internal_d | external_d | tech_d

                if self.strategy == "aw_only":
                    dalio_base_weights = dict(self.base_aw)
                elif self.strategy == "aw_cycle":
                    dalio_base_weights = apply_big_cycle_tilts(self.base_aw, all_data)
                elif self.strategy in ("aw_cycle_alpha", "aw_cycle_alpha_soros"):
                    dalio_base_weights = apply_big_cycle_tilts(self.base_aw, all_data)
                    try:
                        if self.mode == "pure":
                            from polaris.chains.forces_pure import assess_forces_pure
                            from anchor.compute.percentile_trend import ForceDirection as PTForceDirection
                            pure_results = assess_forces_pure(self.fred, month)
                            dir_map_pure = {
                                PTForceDirection.STRONGLY_POSITIVE: 1.0,
                                PTForceDirection.POSITIVE: 0.5,
                                PTForceDirection.NEUTRAL: 0.0,
                                PTForceDirection.NEGATIVE: -0.5,
                                PTForceDirection.STRONGLY_NEGATIVE: -1.0,
                            }
                            force_dirs = {}
                            # F1拆分适配: force1a+force1b → force1（取更负面的）
                            _f1a = pure_results.get("force1a")
                            _f1b = pure_results.get("force1b")
                            if _f1a and _f1b:
                                _s1a = dir_map_pure.get(_f1a[0], 0.0)
                                _s1b = dir_map_pure.get(_f1b[0], 0.0)
                                force_dirs[1] = min(_s1a, _s1b)  # 取更负面的
                            elif "force1" in pure_results:
                                force_dirs[1] = dir_map_pure.get(pure_results["force1"][0], 0.0)
                            for fkey, (direction, conf, _) in pure_results.items():
                                fid = {"force2": 2, "force3": 3, "force4": 4, "force5": 5}.get(fkey)
                                if fid:
                                    force_dirs[fid] = dir_map_pure.get(direction, 0.0)

                            macro_ctx = MacroContext(
                                fed_funds_rate=macro_d.get("fed_funds_rate"),
                                credit_growth=macro_d.get("credit_growth"),
                                cpi_actual=macro_d.get("cpi_actual"),
                                gdp_growth_actual=macro_d.get("gdp_growth_actual"),
                                unemployment_rate=macro_d.get("unemployment_rate"),
                                total_debt_to_gdp=macro_d.get("total_debt_to_gdp"),
                                financial_sector_leverage=macro_d.get("financial_leverage"),
                                fiscal_deficit_to_gdp=internal_d.get("fiscal_deficit_to_gdp"),
                            )
                            graph = _propagate_causal_graph(macro_ctx)

                            view = build_five_forces_view(
                                macro_data=macro_d, internal_data=internal_d,
                                external_data=external_d, nature_data=nature_d,
                                tech_data=tech_d,
                            )
                            from polaris.chains.dalio_forces import ForceDirection as PFD
                            _pure_dir_map = {
                                1.0: PFD.STRONGLY_POSITIVE,
                                0.5: PFD.POSITIVE,
                                0.0: PFD.NEUTRAL,
                                -0.5: PFD.NEGATIVE,
                                -1.0: PFD.STRONGLY_NEGATIVE,
                            }
                            for f in view.forces:
                                pd_score = force_dirs.get(f.force_id, 0.0)
                                f.system_direction = _pure_dir_map.get(pd_score, PFD.NEUTRAL)

                            dalio_base_weights = apply_pure_alpha(
                                dalio_base_weights, view, graph.nodes, all_data)
                        else:
                            view = build_five_forces_view(
                                macro_data=macro_d, internal_data=internal_d,
                                external_data=external_d, nature_data=nature_d,
                                tech_data=tech_d,
                            )
                            macro_ctx = MacroContext(
                                fed_funds_rate=macro_d.get("fed_funds_rate"),
                                credit_growth=macro_d.get("credit_growth"),
                                cpi_actual=macro_d.get("cpi_actual"),
                                gdp_growth_actual=macro_d.get("gdp_growth_actual"),
                                unemployment_rate=macro_d.get("unemployment_rate"),
                                total_debt_to_gdp=macro_d.get("total_debt_to_gdp"),
                                financial_sector_leverage=macro_d.get("financial_leverage"),
                                fiscal_deficit_to_gdp=internal_d.get("fiscal_deficit_to_gdp"),
                            )
                            graph = _propagate_causal_graph(macro_ctx)
                            dalio_base_weights = apply_pure_alpha(
                                dalio_base_weights, view, graph.nodes, all_data)

                            dir_map = {
                                ForceDirection.STRONGLY_POSITIVE: 1.0,
                                ForceDirection.POSITIVE: 0.5,
                                ForceDirection.NEUTRAL: 0.0,
                                ForceDirection.NEGATIVE: -0.5,
                                ForceDirection.STRONGLY_NEGATIVE: -1.0,
                            }
                            force_dirs = {f.force_id: dir_map[f.effective_direction] for f in view.forces}
                    except Exception:
                        pass

            # 索罗斯层: 月度再平衡
            weights = dict(dalio_base_weights)
            if self.strategy == "aw_cycle_alpha_soros":
                try:
                    from polaris.chains.soros import evaluate_soros, ReflexivityPhase
                    mkt, _ = build_market_state(
                        self.monthly_returns, self.fred, self.monthly_signals, month, force_dirs
                    )
                    insight = evaluate_soros(mkt, force_directions=force_dirs,
                                             prior_phase=soros_prior_phase)
                    soros_prior_phase = insight.phase
                    # 用带状态的 insight 重算 overlay
                    ts = insight.trade_signal
                    if ts:
                        weights = dict(dalio_base_weights)
                        for asset, ride_w in ts.ride_assets.items():
                            if asset in weights:
                                weights[asset] += ride_w * SOROS_MAX_TILT
                        for asset, hedge_w in ts.hedge_assets.items():
                            if asset in ("gold", "long_term_bond", "intermediate_bond"):
                                if asset in weights:
                                    weights[asset] += hedge_w * SOROS_MAX_TILT
                            else:
                                if asset in weights:
                                    weights[asset] = max(0, weights[asset] - hedge_w * SOROS_MAX_TILT)
                        if insight.reflexivity_feedback and insight.reflexivity_feedback.fragility > 0.5:
                            frag = insight.reflexivity_feedback.fragility
                            scale = max(0.8, 1 - (frag - 0.5) * 0.15)
                            weights = {k: v * scale for k, v in weights.items()}
                        total = sum(weights.values())
                        if total > 0:
                            weights = {k: v / total for k, v in weights.items()}
                except Exception:
                    pass

            current_weights = {k: v * self.leverage for k, v in weights.items()}
            weight_history.append((month, dict(current_weights)))

            # 计算月回报
            signals = self.monthly_signals.get(month, {})
            # 计算综合force score用于选股profile
            _force_score = 0.0
            if force_dirs:
                _scores = [v for v in force_dirs.values() if v is not None]
                if _scores:
                    _force_score = sum(_scores) / len(_scores)

            port_ret = 0.0
            for asset, w in current_weights.items():
                if asset == "equity" and self.use_stock_selection:
                    # 选股替代equity ETF，严格度随市场状态调整
                    stock_ret = self._get_stock_portfolio_return(month, _force_score)
                    r = stock_ret if stock_ret is not None else rets.get(asset, 0)
                else:
                    r = rets.get(asset, 0)
                port_ret += w * r / 100

            # 杠杆成本
            rf_monthly = signals.get("treasury_3m", 2.0) / 12 / 100
            borrow = max(0, sum(abs(v) for v in current_weights.values()) - 1.0)
            port_ret -= borrow * rf_monthly

            monthly_rets.append(port_ret * 100)
            cumulative *= (1 + port_ret)
            equity_curve.append(cumulative)
            peak = max(peak, cumulative)
            dd = (peak - cumulative) / peak
            max_dd = max(max_dd, dd)

        n_months = len(monthly_rets)
        n_years = n_months / 12
        ann_ret = (cumulative ** (1 / n_years) - 1) * 100 if n_years > 0 else 0
        ann_vol = statistics.stdev(monthly_rets) * (12 ** 0.5) if n_months > 1 else 0
        avg_rf = statistics.mean(
            self.monthly_signals.get(m, {}).get("treasury_3m", 2.0) for m in months
        )
        sharpe = (ann_ret - avg_rf) / ann_vol if ann_vol > 0 else 0

        return BacktestResult(
            strategy=self.strategy,
            mode=self.mode,
            leverage=self.leverage,
            months=n_months,
            ann_return=round(ann_ret, 2),
            ann_vol=round(ann_vol, 2),
            sharpe=round(sharpe, 2),
            cumulative=round((cumulative - 1) * 100, 1),
            max_drawdown=round(max_dd * 100, 1),
            worst_month=round(min(monthly_rets), 2),
            best_month=round(max(monthly_rets), 2),
            positive_months=sum(1 for r in monthly_rets if r > 0),
            monthly_rets=monthly_rets,
            equity_curve=equity_curve,
            weight_history=weight_history,
        )

    def run_leverage_scan(self, leverages: list[float] | None = None) -> list[BacktestResult]:
        """跑多个杠杆水平。"""
        if leverages is None:
            leverages = [1.0, 1.8]
        results = []
        orig_lev = self.leverage
        for lev in leverages:
            self.leverage = lev
            results.append(self.run())
        self.leverage = orig_lev
        return results


# ══════════════════════════════════════════════════════════════
#  简单基准回测
# ══════════════════════════════════════════════════════════════


def _backtest_simple(weights: dict, rebalance_freq: int = 1) -> BacktestResult:
    """简单的固定权重回测（基准用）。"""
    monthly_returns = _load_json("data_monthly_returns.json")
    monthly_signals = _load_json("data_monthly_signals.json")
    months = sorted(monthly_returns.keys())

    cumulative = 1.0
    peak = 1.0
    max_dd = 0.0
    monthly_rets: list[float] = []
    equity_curve = [1.0]

    for i, month in enumerate(months):
        rets = monthly_returns[month]
        port_ret = sum(w * rets.get(a, 0) / 100 for a, w in weights.items())
        monthly_rets.append(port_ret * 100)
        cumulative *= (1 + port_ret)
        equity_curve.append(cumulative)
        peak = max(peak, cumulative)
        dd = (peak - cumulative) / peak
        max_dd = max(max_dd, dd)

    n = len(monthly_rets)
    ny = n / 12
    ann_ret = (cumulative ** (1 / ny) - 1) * 100 if ny > 0 else 0
    ann_vol = statistics.stdev(monthly_rets) * (12 ** 0.5) if n > 1 else 0
    avg_rf = statistics.mean(monthly_signals.get(m, {}).get("treasury_3m", 2.0) for m in months)
    sharpe = (ann_ret - avg_rf) / ann_vol if ann_vol > 0 else 0

    # 推断 strategy name
    if len(weights) == 1 and "equity" in weights:
        strat_name = "S&P 500"
    elif weights.get("equity", 0) == 0.6:
        strat_name = "60/40"
    else:
        strat_name = "benchmark"

    return BacktestResult(
        strategy=strat_name,
        mode="benchmark",
        leverage=1.0,
        months=n,
        ann_return=round(ann_ret, 2),
        ann_vol=round(ann_vol, 2),
        sharpe=round(sharpe, 2),
        cumulative=round((cumulative - 1) * 100, 1),
        max_drawdown=round(max_dd * 100, 1),
        worst_month=round(min(monthly_rets), 2),
        best_month=round(max(monthly_rets), 2) if monthly_rets else 0,
        positive_months=sum(1 for r in monthly_rets if r > 0),
        monthly_rets=monthly_rets,
        equity_curve=equity_curve,
    )


# ══════════════════════════════════════════════════════════════
#  格式化输出
# ══════════════════════════════════════════════════════════════


STRATEGY_LABELS = {
    "aw_only": "纯 All Weather",
    "aw_cycle": "AW + 大周期倾斜",
    "aw_cycle_alpha": "AW + 大周期 + PA",
    "aw_cycle_alpha_soros": "AW + 大周期 + PA + 索罗斯",
    "S&P 500": "S&P 500",
    "60/40": "60/40",
    "benchmark": "Benchmark",
}


def format_results(results: list[BacktestResult], benchmarks: list[BacktestResult] | None = None) -> str:
    """格式化回测结果为可读表格。"""
    lines = []
    lines.append("=" * 90)
    lines.append("  回测结果")
    lines.append("=" * 90)

    header = f"  {'策略':36s} {'年化':>7s} {'波动率':>7s} {'夏普':>6s} {'累计':>8s} {'回撤':>7s} {'最差月':>7s}"
    lines.append(header)
    lines.append(f"  {'-' * 85}")

    # 先输出基准
    if benchmarks:
        for r in benchmarks:
            label = STRATEGY_LABELS.get(r.strategy, r.strategy)
            lines.append(
                f"  {label:36s} {r.ann_return:+6.1f}% {r.ann_vol:6.1f}% "
                f"{r.sharpe:5.2f} {r.cumulative:+7.0f}% {r.max_drawdown:6.1f}% "
                f"{r.worst_month:+6.1f}%"
            )
        lines.append(f"  {'-' * 85}")

    # 策略结果
    spy_sharpe = None
    if benchmarks:
        for b in benchmarks:
            if b.strategy == "S&P 500":
                spy_sharpe = b.sharpe
                break

    for r in results:
        label = STRATEGY_LABELS.get(r.strategy, r.strategy)
        mode_tag = " [PURE]" if r.mode == "pure" and r.strategy not in ("aw_only", "aw_cycle") else ""
        lev_label = f"{label}{mode_tag} {r.leverage:.1f}x"
        marker = ""
        if spy_sharpe is not None and r.sharpe > spy_sharpe:
            marker = " *"
        lines.append(
            f"  {lev_label:36s} {r.ann_return:+6.1f}% {r.ann_vol:6.1f}% "
            f"{r.sharpe:5.2f} {r.cumulative:+7.0f}% {r.max_drawdown:6.1f}% "
            f"{r.worst_month:+6.1f}%{marker}"
        )

    lines.append("")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════
#  标准回测入口
# ══════════════════════════════════════════════════════════════


def run_standard_backtest(mode: str = "pure", stock_selection: bool = False) -> str:
    """跑标准回测: S&P + 60/40 基准 + 4策略 x 多杠杆。

    默认杠杆: 1.0x 和波动率匹配 S&P 的 ~1.8x。
    stock_selection: True 时用巴菲特持仓+相关性过滤替代equity ETF。
    """
    ss_tag = " + 巴菲特选股" if stock_selection else ""
    print("=" * 90)
    print(f"  Axion 回测引擎 — {mode} 模式{ss_tag}")
    print("=" * 90)

    # 基准
    spy = _backtest_simple({"equity": 1.0}, 12)
    b6040 = _backtest_simple({"equity": 0.60, "long_term_bond": 0.20, "intermediate_bond": 0.20}, 3)
    benchmarks = [spy, b6040]

    strategies = ["aw_only", "aw_cycle", "aw_cycle_alpha", "aw_cycle_alpha_soros"]
    leverages = [1.0, 1.8]

    all_results: list[BacktestResult] = []
    for strat in strategies:
        for lev in leverages:
            engine = BacktestEngine(
                strategy=strat, leverage=lev, mode=mode,
                use_stock_selection=stock_selection,
            )
            r = engine.run()
            all_results.append(r)
            label = STRATEGY_LABELS.get(strat, strat)
            mode_tag = " [PURE]" if mode == "pure" and strat not in ("aw_only", "aw_cycle") else ""
            ss_mark = " +选股" if stock_selection else ""
            lev_label = f"{label}{mode_tag}{ss_mark} {lev:.1f}x"
            marker = " *" if r.sharpe > spy.sharpe else ""
            print(
                f"  {lev_label:42s} {r.ann_return:+6.1f}% {r.ann_vol:6.1f}% "
                f"{r.sharpe:5.2f} {r.cumulative:+7.0f}% {r.max_drawdown:6.1f}% "
                f"{r.worst_month:+6.1f}%{marker}"
            )

    output = format_results(all_results, benchmarks)
    print(output)
    return output


# ══════════════════════════════════════════════════════════════
#  __main__
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    mode = "full" if "--full" in sys.argv else "pure"
    stock_sel = "--stocks" in sys.argv
    both_modes = "--compare" in sys.argv

    if both_modes:
        run_standard_backtest(mode=mode, stock_selection=False)
        print("\n")
        run_standard_backtest(mode=mode, stock_selection=True)
    else:
        run_standard_backtest(mode=mode, stock_selection=stock_sel)
