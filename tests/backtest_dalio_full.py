"""
达利欧全流程月度回测
====================

完整链路:
  FRED 历史数据 → 五大力量 → 主要矛盾 → 场景推演 → 信号分流
  → All Weather 底仓 + Pure Alpha 叠加 → 月度回测 216 个月

对比:
  1. 纯 All Weather (固定权重)
  2. All Weather + 大周期倾斜 (长期信号)
  3. All Weather + 大周期 + Pure Alpha (中期信号)
  4. S&P 500 / 60/40 基准
"""

import json
import math
import statistics
from datetime import date, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).parent

# 加载月度回报和信号
with open(DATA_DIR / "data_monthly_returns.json") as f:
    MONTHLY_RETURNS = json.load(f)

with open(DATA_DIR / "data_monthly_signals.json") as f:
    MONTHLY_SIGNALS = json.load(f)


# ══════════════════════════════════════════════════════════════
#  FRED 历史数据缓存
# ══════════════════════════════════════════════════════════════

FRED_CACHE_PATH = DATA_DIR / "data_fred_monthly_history.json"


def fetch_fred_history():
    """从 FRED 拉取 2006-2025 所有达利欧需要的序列，缓存到本地。"""
    from anchor.config import settings
    from fredapi import Fred
    import pandas as pd

    api_key = settings.fred_api_key
    if not api_key:
        raise RuntimeError("需要 FRED_API_KEY")

    fred = Fred(api_key=api_key)
    start = "2005-01-01"  # 多拉一年算 YoY

    series_map = {
        # F1
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
        # F2
        "consumer_sentiment": "UMCSENT",
        "gini": "SIPOVGINIUSA",
        "fiscal_deficit_gdp": "FYFSGDA188S",
        "nonfarm_productivity": "OPHNFB",
        # F3
        "trade_balance": "BOPGSTB",
        "dollar_index": "DTWEXBGS",
        "wti_oil": "DCOILWTICO",
        "epu_index": "USEPUINDXD",
        # F4
        "food_cpi": "CPIUFDSL",
        # F5
        "rd_spending": "Y694RC1Q027SBEA",
        "nasdaq": "NASDAQCOM",
        # Soros 信号
        "credit_spread_hy": "BAMLH0A0HYM2",
        "credit_spread_ig": "BAMLC0A0CM",
        "vix_daily": "VIXCLS",
    }

    result = {}
    for key, sid in series_map.items():
        try:
            s = fred.get_series(sid, observation_start=start)
            if s is not None and not s.empty:
                # 转为 {YYYY-MM: value}
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
    # 精确匹配
    if month in series:
        return series[month]
    # 往前找最近的
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


# ══════════════════════════════════════════════════════════════
#  构造每月的五大力量数据
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
        external["trade_balance"] = v / 1000  # 百万→十亿
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
#  回测引擎
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
# 大周期位置 → 权重调整 (增量, 不是绝对值)
BIG_CYCLE_TILTS = {
    "debt_late": {
        "gold": +0.05,              # 增配黄金
        "inflation_linked_bond": +0.05,  # 增配 TIPS
        "long_term_bond": -0.05,    # 减配长期名义债
        "intermediate_bond": -0.03,
        "equity": -0.02,
    },
    "empire_declining": {
        "gold": +0.03,
        "equity": -0.02,
        "em_bond": -0.01,
    },
    "tech_revolution": {
        "equity": +0.03,            # 生产率受益
        "commodity": -0.02,         # 单位成本下降
        "gold": -0.01,
    },
}

# Pure Alpha: 主要矛盾 Force → 资产调整
# 简化: 用因果图 asset_impacts 的方向做小幅调整
PA_MAX_TILT = 0.08  # Pure Alpha 单资产最大调整幅度


def apply_big_cycle_tilts(base_weights: dict, macro_data: dict) -> dict:
    """根据大周期位置调整 All Weather 权重。"""
    tilts = dict(base_weights)

    # ── 加息风暴检测: 通胀失控 + 零利率 → 久期全杀 ──
    cpi = macro_data.get("cpi_actual", 0)
    rate = macro_data.get("fed_funds_rate", 0)
    real_rate = rate - cpi

    if cpi > 5 and rate < cpi - 3:
        # 实际利率极度负值 → Fed 必然大幅加息
        # 减久期, 加现金和商品
        duration_penalty = min((cpi - rate - 3) * 0.03, 0.15)  # 最多减15%
        tilts["long_term_bond"] = max(0, tilts.get("long_term_bond", 0) - duration_penalty * 1.5)
        tilts["intermediate_bond"] = max(0, tilts.get("intermediate_bond", 0) - duration_penalty)
        tilts["inflation_linked_bond"] = max(0, tilts.get("inflation_linked_bond", 0) - duration_penalty * 0.5)
        tilts["equity"] = max(0, tilts.get("equity", 0) - duration_penalty * 0.5)
        tilts["commodity"] = tilts.get("commodity", 0) + duration_penalty * 1.5
        tilts["gold"] = tilts.get("gold", 0) + duration_penalty * 0.5
        # 剩余给现金(如果有的话, 没有就等于降杠杆)

    # ── 常规大周期倾斜 ──
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

    # 用因果图注入冲击，看资产影响
    shocks = compute_force_shocks(view)
    if not shocks:
        return base_weights

    from polaris.chains.dalio import _compute_asset_impacts
    shocked_nodes = inject_shocks_to_nodes(graph_nodes, shocks)
    impacts = _compute_asset_impacts(shocked_nodes)

    # 资产映射: 因果图资产名 → 回测资产名
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
            # 根据方向和分数做小幅调整
            sign = 1 if ai.direction == "overweight" else -1
            delta = sign * min(abs(ai.raw_score) * 0.3, PA_MAX_TILT)
            # 矛盾分越高，Pure Alpha 信心越大
            delta *= min(analysis.principal.score * 5, 1.0)
            tilts[mapped] = max(0, tilts[mapped] + delta)

    # 归一化
    total = sum(tilts.values())
    if total > 0:
        tilts = {k: v / total for k, v in tilts.items()}
    return tilts


# ── 索罗斯叠加 ──

from polaris.chains.soros import MarketState, evaluate_soros, SorosAction
from polaris.chains.dalio_forces import ForceDirection

SOROS_MAX_TILT = 0.10  # 索罗斯单资产最大调整幅度


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

    # 从当前往回数，连续正回报的月数（用 6 月滚动平均判断趋势）
    duration = 0
    cum_6m = 0
    for i in range(end_idx, max(0, end_idx - 60), -1):
        m = all_months[i]
        r = monthly_returns.get(m, {}).get("equity", 0)
        cum_6m = r  # 简化: 用单月回报正负判断
        # 更好的方法: 用 6 月累计
        start_6m = max(0, i - 5)
        window_rets = [monthly_returns.get(all_months[j], {}).get("equity", 0) for j in range(start_6m, i + 1)]
        avg = sum(window_rets) / len(window_rets) if window_rets else 0
        if avg > 0:
            duration += 1
        else:
            break
    return duration


def build_market_state(
    monthly_returns: dict, fred: dict, signals: dict, month: str,
    force_directions: dict | None = None,
) -> tuple[MarketState, dict | None]:
    """从历史数据构造索罗斯需要的 MarketState。"""
    mom_12 = _compute_momentum(monthly_returns, fred, month, 12)
    mom_3 = _compute_momentum(monthly_returns, fred, month, 3)
    mom_6 = _compute_momentum(monthly_returns, fred, month, 6)
    duration = _compute_trend_duration(monthly_returns, month)

    sig = signals.get(month, {})
    vix = sig.get("vix")
    yc = sig.get("yield_curve")

    # VIX 变化
    all_months = sorted(signals.keys())
    idx = all_months.index(month) if month in all_months else -1
    vix_change = sig.get("vix_change")
    vix_6m_ago = None
    if idx >= 6:
        vix_6m_ago = signals.get(all_months[idx - 6], {}).get("vix")

    # 信用利差 from FRED cache
    hy = _latest_value(fred.get("credit_spread_hy", {}), month)
    # 3 月前利差
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


def apply_soros_overlay(base_weights: dict, market: MarketState,
                         force_directions: dict | None = None) -> dict:
    """索罗斯信号叠加到权重上。"""
    insight = evaluate_soros(market, force_directions=force_directions)
    ts = insight.trade_signal
    if not ts:
        return base_weights

    weights = dict(base_weights)

    # 主仓调整: ride_assets
    for asset, ride_w in ts.ride_assets.items():
        if asset in weights:
            weights[asset] = weights[asset] + ride_w * SOROS_MAX_TILT

    # 对冲调整: hedge_assets
    for asset, hedge_w in ts.hedge_assets.items():
        if asset in ("gold", "long_term_bond", "intermediate_bond"):
            # 避险做多
            if asset in weights:
                weights[asset] = weights[asset] + hedge_w * SOROS_MAX_TILT
        else:
            # 风险资产做空(减仓)
            if asset in weights:
                weights[asset] = max(0, weights[asset] - hedge_w * SOROS_MAX_TILT)

    # 反身性反馈: 脆弱性高时全面降仓(增加现金)
    if insight.reflexivity_feedback and insight.reflexivity_feedback.fragility > 0.5:
        frag = insight.reflexivity_feedback.fragility
        cash_shift = (frag - 0.5) * 0.15  # fragility 0.7 → 降3%
        total = sum(weights.values())
        if total > 0:
            scale = max(0.8, 1 - cash_shift)
            weights = {k: v * scale for k, v in weights.items()}

    # 归一化
    total = sum(weights.values())
    if total > 0:
        weights = {k: v / total for k, v in weights.items()}
    return weights


def backtest_dalio_full(
    strategy: str = "aw_only",
    leverage: float = 1.0,
    rebalance_freq: int = 3,  # 季度再平衡
) -> dict:
    """达利欧全流程月度回测。

    strategy:
      "aw_only"            — 纯 All Weather (Bridgewater 近似)
      "aw_cycle"           — All Weather + 大周期倾斜
      "aw_cycle_alpha"     — All Weather + 大周期 + Pure Alpha
      "aw_cycle_alpha_soros" — 全部: AW + 大周期 + PA + 索罗斯
    """
    fred = load_fred_history()
    months = sorted(MONTHLY_RETURNS.keys())
    base_aw = BRIDGEWATER_APPROXIMATE

    cumulative = 1.0
    peak = 1.0
    max_dd = 0.0
    monthly_rets = []
    equity_curve = [1.0]
    weight_history = []

    current_weights = {k: v * leverage for k, v in base_aw.items()}

    # 达利欧基础权重 (季度更新)
    dalio_base_weights = dict(base_aw)
    # 索罗斯状态延续
    soros_prior_phase = None
    force_dirs = None

    for i, month in enumerate(months):
        rets = MONTHLY_RETURNS[month]

        # ── 达利欧层: 季度再平衡 (大周期+PA) ──
        if i % rebalance_freq == 0:
            macro_d, internal_d, external_d, nature_d, tech_d = \
                build_monthly_forces_data(fred, month)
            all_data = macro_d | internal_d | external_d | tech_d

            if strategy == "aw_only":
                dalio_base_weights = dict(base_aw)
            elif strategy == "aw_cycle":
                dalio_base_weights = apply_big_cycle_tilts(base_aw, all_data)
            elif strategy in ("aw_cycle_alpha", "aw_cycle_alpha_soros"):
                dalio_base_weights = apply_big_cycle_tilts(base_aw, all_data)
                try:
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

        # ── 索罗斯层: 月度再平衡 (独立于达利欧的频率) ──
        weights = dict(dalio_base_weights)
        if strategy == "aw_cycle_alpha_soros":
            try:
                from polaris.chains.soros import ReflexivityPhase
                mkt, _ = build_market_state(
                    MONTHLY_RETURNS, fred, MONTHLY_SIGNALS, month, force_dirs
                )
                insight = evaluate_soros(mkt, force_directions=force_dirs,
                                         prior_phase=soros_prior_phase)
                soros_prior_phase = insight.phase  # 状态传递
                weights = apply_soros_overlay(weights, mkt, force_dirs)
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

        current_weights = {k: v * leverage for k, v in weights.items()}
        weight_history.append((month, dict(current_weights)))

        # 计算月回报
        signals = MONTHLY_SIGNALS.get(month, {})
        port_ret = 0.0
        for asset, w in current_weights.items():
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
        MONTHLY_SIGNALS.get(m, {}).get("treasury_3m", 2.0) for m in months
    )
    sharpe = (ann_ret - avg_rf) / ann_vol if ann_vol > 0 else 0

    return {
        "strategy": strategy,
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
        "weight_history": weight_history,
    }


def _backtest_simple(weights: dict, rebalance_freq: int = 1) -> dict:
    """简单的固定权重回测（基准用）。"""
    months = sorted(MONTHLY_RETURNS.keys())
    cumulative = 1.0
    peak = 1.0
    max_dd = 0.0
    monthly_rets = []
    equity_curve = [1.0]

    for i, month in enumerate(months):
        rets = MONTHLY_RETURNS[month]
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
    avg_rf = statistics.mean(MONTHLY_SIGNALS.get(m, {}).get("treasury_3m", 2.0) for m in months)
    sharpe = (ann_ret - avg_rf) / ann_vol if ann_vol > 0 else 0

    return {
        "months": n, "ann_return": round(ann_ret, 2), "ann_vol": round(ann_vol, 2),
        "sharpe": round(sharpe, 2), "cumulative": round((cumulative - 1) * 100, 1),
        "max_drawdown": round(max_dd * 100, 1), "worst_month": round(min(monthly_rets), 2),
        "equity_curve": equity_curve,
    }


def main():
    print("=" * 90)
    print("  达利欧全流程月度回测 2007.01 - 2024.12")
    print("=" * 90)

    # 基准
    spy = backtest_dalio_full.__wrapped__ if hasattr(backtest_dalio_full, '__wrapped__') else None
    # 基准用自己的引擎跑
    spy = backtest_dalio_full("aw_only", leverage=1.0, rebalance_freq=12)
    # hack: 直接用 equity=1.0 跑 SPY
    spy_weights_save = dict(BRIDGEWATER_APPROXIMATE)
    import tests.backtest_dalio_full as _self
    # 简单方式: 直接算 SPY 和 60/40
    spy = _backtest_simple({"equity": 1.0}, 12)
    b6040 = _backtest_simple({"equity": 0.60, "long_term_bond": 0.20, "intermediate_bond": 0.20}, 3)

    strategies = [
        ("纯 All Weather", "aw_only"),
        ("AW + 大周期倾斜", "aw_cycle"),
        ("AW + 大周期 + PA", "aw_cycle_alpha"),
        ("AW + 大周期 + PA + 索罗斯", "aw_cycle_alpha_soros"),
    ]

    print(f"\n  {'策略':30s} {'年化':>7s} {'波动率':>7s} {'夏普':>6s} {'累计':>8s} {'回撤':>7s} {'最差月':>7s}")
    print(f"  {'-' * 80}")
    print(f"  {'S&P 500':30s} {spy['ann_return']:+6.1f}% {spy['ann_vol']:6.1f}% {spy['sharpe']:5.2f} {spy['cumulative']:+7.0f}% {spy['max_drawdown']:6.1f}% {spy['worst_month']:+6.1f}%")
    print(f"  {'60/40':30s} {b6040['ann_return']:+6.1f}% {b6040['ann_vol']:6.1f}% {b6040['sharpe']:5.2f} {b6040['cumulative']:+7.0f}% {b6040['max_drawdown']:6.1f}% {b6040['worst_month']:+6.1f}%")
    print(f"  {'-' * 80}")

    results = {}
    leverages = [1.0, 1.5, 2.0, 2.5, 3.0]
    for label, strat in strategies:
        for lev in leverages:
            r = backtest_dalio_full(strat, leverage=lev, rebalance_freq=3)
            lev_label = f"{label} {lev:.1f}x"
            results[lev_label] = r
            marker = " ★" if r["sharpe"] > spy["sharpe"] else ""
            print(f"  {lev_label:30s} {r['ann_return']:+6.1f}% {r['ann_vol']:6.1f}% {r['sharpe']:5.2f} {r['cumulative']:+7.0f}% {r['max_drawdown']:6.1f}% {r['worst_month']:+6.1f}%{marker}")

    # 逐年对比
    print(f"\n{'=' * 90}")
    print("  逐年对比")
    print("=" * 90)

    aw = results.get("纯 All Weather 1.0x")
    alpha = results.get("AW + 大周期 + PA 2.5x")
    soros = results.get("AW + 大周期 + PA + 索罗斯 2.5x")

    if not soros:
        soros = alpha  # fallback

    print(f"  {'年':6s} {'AW 1x':>8s} {'AW+PA 2.5x':>11s} {'全部 2.5x':>10s} {'S&P':>8s}")
    print(f"  {'-' * 50}")

    months = sorted(MONTHLY_RETURNS.keys())
    for year in range(2007, 2025):
        yr_months = [m for m in months if m.startswith(str(year))]
        if not yr_months:
            continue
        start_idx = months.index(yr_months[0])
        end_idx = months.index(yr_months[-1])

        def yr(curve):
            if end_idx + 1 < len(curve) and start_idx < len(curve):
                return (curve[end_idx + 1] / curve[start_idx] - 1) * 100
            return 0

        print(f"  {year} {yr(aw['equity_curve']):+7.1f}% {yr(alpha['equity_curve']):+10.1f}% {yr(soros['equity_curve']):+9.1f}% {yr(spy['equity_curve']):+7.1f}%")


if __name__ == "__main__":
    main()
