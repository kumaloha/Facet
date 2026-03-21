"""
市场信号计算层
==============

从 stock_quotes / macro_indicators 计算衍生信号：
- 资产动量 (3m/6m/12m)
- 行业 ETF 相对动量 (资金流向)
- 宏观指标 lag 值 (索罗斯反身性阶段)

职责: 读DB → 计算 → 返回结构化数据。不做认知判断。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta

from loguru import logger
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from anchor.database.session import AsyncSessionLocal
from anchor.models import MacroIndicator, StockQuote


# ── 资产 → ETF 映射 (索罗斯 MarketState 需要) ──────────────────────

ASSET_ETF_MAP = {
    "equity": "SPY",
    "long_term_bond": "TLT",
    "gold": "GLD",
    "commodity": "DBC",
    # em_bond 没有对应 ETF 在跟踪列表里，暂缺
}

# ── GICS 行业 ETF ───────────────────────────────────────────────────

SECTOR_ETFS = {
    "XLK": "科技",
    "XLV": "医疗保健",
    "XLF": "金融",
    "XLE": "能源",
    "XLI": "工业",
    "XLY": "可选消费",
    "XLP": "必选消费",
    "XLU": "公用事业",
    "XLB": "材料",
    "XLRE": "房地产",
    "XLC": "通信服务",
}


# ── 数据结构 ────────────────────────────────────────────────────────


@dataclass
class AssetMomentum:
    """单个资产的多周期动量。"""
    ticker: str
    name: str
    mom_3m: float | None = None   # 3个月回报 %
    mom_6m: float | None = None   # 6个月回报 %
    mom_12m: float | None = None  # 12个月回报 %


@dataclass
class SectorFlow:
    """行业 ETF 相对于 SPY 的动量，用于识别资金流向。"""
    ticker: str
    sector_name: str
    abs_mom_3m: float | None = None    # 绝对动量 %
    abs_mom_6m: float | None = None
    abs_mom_12m: float | None = None
    rel_mom_3m: float | None = None    # 相对 SPY 动量 %
    rel_mom_6m: float | None = None
    rel_mom_12m: float | None = None


@dataclass
class MacroLag:
    """宏观指标的当前值和历史值，用于索罗斯趋势判断。"""
    indicator: str
    current: float | None = None
    ago_3m: float | None = None
    ago_6m: float | None = None
    change_3m: float | None = None   # current - ago_3m
    change_6m: float | None = None   # current - ago_6m


@dataclass
class MarketSignals:
    """完整的市场信号包 — Polaris 索罗斯链的输入。"""
    snapshot_date: str = ""
    # 资产动量
    asset_momentum: dict[str, AssetMomentum] = field(default_factory=dict)
    # 行业资金流向
    sector_flows: list[SectorFlow] = field(default_factory=list)
    # 宏观 lag
    macro_lags: dict[str, MacroLag] = field(default_factory=dict)
    # 便捷: 排序后的热门/冷门行业
    top_sectors: list[str] = field(default_factory=list)    # 相对动量最强的3个
    bottom_sectors: list[str] = field(default_factory=list)  # 最弱的3个


# ── 核心计算 ────────────────────────────────────────────────────────


async def _get_prices(
    session: AsyncSession, ticker: str, days: int = 400,
) -> list[tuple[date, float]]:
    """获取 ticker 的 (trade_date, price_close) 列表，按日期排序。"""
    cutoff = date.today() - timedelta(days=days)
    stmt = (
        select(StockQuote.trade_date, StockQuote.price_close)
        .where(StockQuote.ticker == ticker, StockQuote.trade_date >= cutoff)
        .order_by(StockQuote.trade_date)
    )
    result = await session.exec(stmt)
    return [(r[0], r[1]) for r in result.all() if r[1] is not None]


def _calc_momentum(prices: list[tuple[date, float]], months: int) -> float | None:
    """从价格序列计算 N 个月动量 (回报率 %)。"""
    if not prices:
        return None
    latest_date, latest_price = prices[-1]
    target = latest_date - timedelta(days=months * 30)
    # 找最接近 target 的价格
    past = [(d, p) for d, p in prices if d <= target]
    if not past:
        return None
    _, past_price = past[-1]
    if past_price == 0:
        return None
    return (latest_price / past_price - 1) * 100


async def compute_asset_momentum(session: AsyncSession) -> dict[str, AssetMomentum]:
    """计算五类资产的 3m/6m/12m 动量。"""
    result = {}
    for asset_name, ticker in ASSET_ETF_MAP.items():
        prices = await _get_prices(session, ticker)
        result[asset_name] = AssetMomentum(
            ticker=ticker,
            name=asset_name,
            mom_3m=_calc_momentum(prices, 3),
            mom_6m=_calc_momentum(prices, 6),
            mom_12m=_calc_momentum(prices, 12),
        )
    return result


async def compute_sector_flows(session: AsyncSession) -> list[SectorFlow]:
    """计算 11 个行业 ETF 的绝对和相对（vs SPY）动量。"""
    spy_prices = await _get_prices(session, "SPY")
    spy_mom = {
        3: _calc_momentum(spy_prices, 3),
        6: _calc_momentum(spy_prices, 6),
        12: _calc_momentum(spy_prices, 12),
    }

    flows = []
    for ticker, sector_name in SECTOR_ETFS.items():
        prices = await _get_prices(session, ticker)
        abs_3 = _calc_momentum(prices, 3)
        abs_6 = _calc_momentum(prices, 6)
        abs_12 = _calc_momentum(prices, 12)

        def _rel(abs_val, spy_val):
            if abs_val is not None and spy_val is not None:
                return round(abs_val - spy_val, 2)
            return None

        flows.append(SectorFlow(
            ticker=ticker,
            sector_name=sector_name,
            abs_mom_3m=round(abs_3, 2) if abs_3 is not None else None,
            abs_mom_6m=round(abs_6, 2) if abs_6 is not None else None,
            abs_mom_12m=round(abs_12, 2) if abs_12 is not None else None,
            rel_mom_3m=_rel(abs_3, spy_mom[3]),
            rel_mom_6m=_rel(abs_6, spy_mom[6]),
            rel_mom_12m=_rel(abs_12, spy_mom[12]),
        ))

    # 按 3m 相对动量排序
    flows.sort(key=lambda f: f.rel_mom_3m or -999, reverse=True)
    return flows


async def _get_macro_value(
    session: AsyncSession, indicator: str, near_date: date,
) -> float | None:
    """获取最接近 near_date 的宏观指标值。"""
    # 往前找最近30天内的值
    start = near_date - timedelta(days=30)
    stmt = (
        select(MacroIndicator.value)
        .where(
            MacroIndicator.indicator == indicator,
            MacroIndicator.trade_date >= start,
            MacroIndicator.trade_date <= near_date,
        )
        .order_by(MacroIndicator.trade_date.desc())
        .limit(1)
    )
    result = await session.exec(stmt)
    row = result.first()
    return float(row) if row is not None else None


async def compute_macro_lags(
    session: AsyncSession,
    indicators: list[str] | None = None,
) -> dict[str, MacroLag]:
    """计算宏观指标的当前值和 3m/6m 前的值。

    默认计算索罗斯链需要的: vix, credit_spread_hy, yield_curve_10y_3m
    """
    if indicators is None:
        indicators = ["vix", "credit_spread_hy", "credit_spread_ig", "yield_curve_10y_3m"]

    today = date.today()
    ago_3m = today - timedelta(days=90)
    ago_6m = today - timedelta(days=180)

    result = {}
    for ind in indicators:
        current = await _get_macro_value(session, ind, today)
        val_3m = await _get_macro_value(session, ind, ago_3m)
        val_6m = await _get_macro_value(session, ind, ago_6m)

        change_3m = None
        change_6m = None
        if current is not None and val_3m is not None:
            change_3m = round(current - val_3m, 4)
        if current is not None and val_6m is not None:
            change_6m = round(current - val_6m, 4)

        result[ind] = MacroLag(
            indicator=ind,
            current=current,
            ago_3m=val_3m,
            ago_6m=val_6m,
            change_3m=change_3m,
            change_6m=change_6m,
        )
    return result


# ── 统一入口 ────────────────────────────────────────────────────────


async def compute_market_signals() -> MarketSignals:
    """计算完整的市场信号包。

    Returns: MarketSignals，可直接用于构建索罗斯 MarketState。
    """
    async with AsyncSessionLocal() as session:
        momentum = await compute_asset_momentum(session)
        flows = await compute_sector_flows(session)
        lags = await compute_macro_lags(session)

    # 提取 top/bottom 行业
    valid_flows = [f for f in flows if f.rel_mom_3m is not None]
    top = [f.sector_name for f in valid_flows[:3]]
    bottom = [f.sector_name for f in valid_flows[-3:]]

    signals = MarketSignals(
        snapshot_date=date.today().isoformat(),
        asset_momentum=momentum,
        sector_flows=flows,
        macro_lags=lags,
        top_sectors=top,
        bottom_sectors=bottom,
    )

    logger.info(
        f"[MarketSignals] {len(momentum)} assets, {len(flows)} sectors, "
        f"{len(lags)} macro lags | "
        f"hot={top} cold={bottom}"
    )
    return signals


def build_soros_market_state(signals: MarketSignals):
    """从 MarketSignals 构建索罗斯 MarketState。

    这是 Anchor(计算) → Polaris(认知) 的桥梁。
    """
    from polaris.chains.soros import MarketState

    mom = signals.asset_momentum
    lags = signals.macro_lags

    vix_lag = lags.get("vix")
    hy_lag = lags.get("credit_spread_hy")
    yc_lag = lags.get("yield_curve_10y_3m")

    # 趋势持续时间: 用 equity 动量方向判断
    eq = mom.get("equity")
    duration = None
    if eq and eq.mom_3m is not None and eq.mom_6m is not None and eq.mom_12m is not None:
        # 所有周期同向 = 持续趋势
        signs = [eq.mom_3m > 0, eq.mom_6m > 0, eq.mom_12m > 0]
        if all(signs) or not any(signs):
            duration = 12  # 至少持续12个月

    return MarketState(
        momentum_equity=eq.mom_12m if eq else None,
        momentum_long_bond=mom["long_term_bond"].mom_12m if "long_term_bond" in mom else None,
        momentum_gold=mom["gold"].mom_12m if "gold" in mom else None,
        momentum_commodity=mom["commodity"].mom_12m if "commodity" in mom else None,
        vix=vix_lag.current if vix_lag else None,
        vix_change_1m=None,  # 需要月度数据，暂缺
        vix_6m_ago=vix_lag.ago_6m if vix_lag else None,
        credit_spread_hy=hy_lag.current if hy_lag else None,
        credit_spread_change_3m=hy_lag.change_3m if hy_lag else None,
        credit_spread_hy_6m_ago=hy_lag.ago_6m if hy_lag else None,
        yield_curve_10y_3m=yc_lag.current if yc_lag else None,
        yield_curve_change_3m=yc_lag.change_3m if yc_lag else None,
        trend_duration_months=duration,
        momentum_equity_3m=eq.mom_3m if eq else None,
        momentum_equity_6m=eq.mom_6m if eq else None,
        snapshot_date=signals.snapshot_date,
    )
