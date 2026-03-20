"""
用真实历史回报验证达利欧引擎的资产配置建议
=============================================

1. 从 data_historical_returns.py 读取每年各资产的真实回报
2. 客观规则: 回报最高2个 = winners, 最低2个 = losers
3. 跑达利欧引擎，看超配/低配是否命中 winners/losers
"""

from data_historical_returns import ACTUAL_RETURNS
from polaris.chains.dalio import MacroContext, evaluate
# soros chain rewritten - old imports removed

# ── 资产类别映射: ACTUAL_RETURNS key → 达利欧引擎 asset_type ──
# 达利欧引擎用: equity_cyclical, nominal_bond, inflation_linked_bond, commodity, gold, cash
ASSET_MAP = {
    "equity":            "equity_cyclical",
    "long_term_bond":    "long_term_bond",
    "intermediate_bond": "intermediate_bond",
    "commodity":         "commodity",
    "gold":              "gold",
    "tips":              "inflation_linked_bond",
    "em_bond":           "em_bond",
}

# 达利欧引擎认识的资产类型
ENGINE_ASSET_TYPES = {"equity_cyclical", "equity_defensive", "long_term_bond",
                      "intermediate_bond", "inflation_linked_bond", "commodity",
                      "gold", "cash", "em_bond"}


def get_winners_losers(year_data: dict[str, float], n: int = 2):
    """从真实回报中选出前n个winners和后n个losers。"""
    if len(year_data) < n * 2:
        # 资产太少，各选1个
        n = max(1, len(year_data) // 2)

    sorted_assets = sorted(year_data.items(), key=lambda x: x[1], reverse=True)
    winners = sorted_assets[:n]
    losers = sorted_assets[-n:]
    return winners, losers


def map_to_engine_type(actual_key: str) -> str | None:
    """将实际资产key映射到引擎资产类型。"""
    engine_type = ASSET_MAP.get(actual_key)
    if engine_type and engine_type in ENGINE_ASSET_TYPES:
        return engine_type
    return None


# ── 历史宏观数据 (年度快照) ──────────────────────────────────────
# 只覆盖有完整资产数据的年份 (2008+)
# 数据来源: FRED, BLS, BEA
MACRO_SNAPSHOTS = {
    2008: MacroContext(
        gdp_growth_actual=-0.1, gdp_growth_expected=2.0,
        cpi_actual=3.8, cpi_expected=2.5,
        fed_funds_rate=2.0, treasury_10y=3.7, treasury_2y=2.0,
        credit_growth=-2.0, total_debt_to_gdp=350,
        vix=32, unemployment_rate=5.8,
        fiscal_deficit_to_gdp=3.2,
        snapshot_date="2008-06-01",
    ),
    2009: MacroContext(
        gdp_growth_actual=-2.5, gdp_growth_expected=1.0,
        cpi_actual=-0.4, cpi_expected=2.0,
        fed_funds_rate=0.25, treasury_10y=3.3, treasury_2y=0.9,
        credit_growth=-5.0, total_debt_to_gdp=380,
        vix=25, unemployment_rate=9.3,
        fiscal_deficit_to_gdp=10.0,
        snapshot_date="2009-06-01",
    ),
    2010: MacroContext(
        gdp_growth_actual=2.6, gdp_growth_expected=3.0,
        cpi_actual=1.6, cpi_expected=2.0,
        fed_funds_rate=0.25, treasury_10y=3.2, treasury_2y=0.6,
        credit_growth=0.5, total_debt_to_gdp=370,
        vix=22, unemployment_rate=9.6,
        fiscal_deficit_to_gdp=9.0,
        snapshot_date="2010-06-01",
    ),
    2011: MacroContext(
        gdp_growth_actual=1.6, gdp_growth_expected=3.0,
        cpi_actual=3.2, cpi_expected=2.0,
        fed_funds_rate=0.25, treasury_10y=2.8, treasury_2y=0.4,
        credit_growth=2.0, total_debt_to_gdp=365,
        vix=24, unemployment_rate=8.9,
        fiscal_deficit_to_gdp=8.7,
        snapshot_date="2011-06-01",
    ),
    2012: MacroContext(
        gdp_growth_actual=2.2, gdp_growth_expected=2.5,
        cpi_actual=2.1, cpi_expected=2.0,
        fed_funds_rate=0.25, treasury_10y=1.8, treasury_2y=0.3,
        credit_growth=4.0, total_debt_to_gdp=360,
        vix=18, unemployment_rate=8.1,
        fiscal_deficit_to_gdp=7.0,
        snapshot_date="2012-06-01",
    ),
    2013: MacroContext(
        gdp_growth_actual=1.8, gdp_growth_expected=2.5,
        cpi_actual=1.5, cpi_expected=2.0,
        fed_funds_rate=0.25, treasury_10y=2.0, treasury_2y=0.3,
        credit_growth=3.5, total_debt_to_gdp=350,
        vix=14, unemployment_rate=7.4,
        fiscal_deficit_to_gdp=4.1,
        snapshot_date="2013-06-01",
    ),
    2014: MacroContext(
        gdp_growth_actual=2.5, gdp_growth_expected=3.0,
        cpi_actual=1.6, cpi_expected=2.0,
        fed_funds_rate=0.25, treasury_10y=2.5, treasury_2y=0.5,
        credit_growth=5.0, total_debt_to_gdp=345,
        vix=12, unemployment_rate=6.2,
        fiscal_deficit_to_gdp=2.8,
        snapshot_date="2014-06-01",
    ),
    2015: MacroContext(
        gdp_growth_actual=2.9, gdp_growth_expected=3.0,
        cpi_actual=0.1, cpi_expected=2.0,
        fed_funds_rate=0.25, treasury_10y=2.1, treasury_2y=0.6,
        credit_growth=4.0, total_debt_to_gdp=345,
        vix=16, unemployment_rate=5.3,
        fiscal_deficit_to_gdp=2.5,
        snapshot_date="2015-06-01",
    ),
    2016: MacroContext(
        gdp_growth_actual=1.6, gdp_growth_expected=2.5,
        cpi_actual=1.3, cpi_expected=2.0,
        fed_funds_rate=0.5, treasury_10y=1.8, treasury_2y=0.8,
        credit_growth=3.5, total_debt_to_gdp=350,
        vix=15, unemployment_rate=4.9,
        fiscal_deficit_to_gdp=3.2,
        snapshot_date="2016-06-01",
    ),
    2017: MacroContext(
        gdp_growth_actual=2.4, gdp_growth_expected=2.0,
        cpi_actual=2.1, cpi_expected=2.0,
        fed_funds_rate=1.25, treasury_10y=2.3, treasury_2y=1.4,
        credit_growth=4.0, total_debt_to_gdp=345,
        vix=11, unemployment_rate=4.4,
        fiscal_deficit_to_gdp=3.5,
        snapshot_date="2017-06-01",
    ),
    2018: MacroContext(
        gdp_growth_actual=2.9, gdp_growth_expected=2.5,
        cpi_actual=2.4, cpi_expected=2.0,
        fed_funds_rate=2.0, treasury_10y=2.9, treasury_2y=2.6,
        credit_growth=4.5, total_debt_to_gdp=340,
        vix=17, unemployment_rate=3.9,
        fiscal_deficit_to_gdp=3.8,
        snapshot_date="2018-06-01",
    ),
    2019: MacroContext(
        gdp_growth_actual=2.3, gdp_growth_expected=2.5,
        cpi_actual=1.8, cpi_expected=2.0,
        fed_funds_rate=2.4, treasury_10y=2.1, treasury_2y=1.8,
        credit_growth=4.0, total_debt_to_gdp=345,
        vix=15, unemployment_rate=3.7,
        fiscal_deficit_to_gdp=4.6,
        snapshot_date="2019-06-01",
    ),
    2020: MacroContext(
        gdp_growth_actual=-3.5, gdp_growth_expected=2.0,
        cpi_actual=1.2, cpi_expected=2.0,
        fed_funds_rate=0.25, treasury_10y=0.9, treasury_2y=0.15,
        credit_growth=8.0, total_debt_to_gdp=400,
        vix=30, unemployment_rate=8.1,
        fiscal_deficit_to_gdp=15.0,
        snapshot_date="2020-06-01",
    ),
    2021: MacroContext(
        gdp_growth_actual=5.7, gdp_growth_expected=4.0,
        cpi_actual=4.7, cpi_expected=2.5,
        fed_funds_rate=0.25, treasury_10y=1.5, treasury_2y=0.25,
        credit_growth=6.0, total_debt_to_gdp=390,
        vix=20, unemployment_rate=5.4,
        fiscal_deficit_to_gdp=12.0,
        snapshot_date="2021-06-01",
    ),
    2022: MacroContext(
        gdp_growth_actual=-0.6, gdp_growth_expected=2.0,
        cpi_actual=8.0, cpi_expected=2.5,
        fed_funds_rate=1.75, treasury_10y=3.0, treasury_2y=2.9,
        credit_growth=7.0, total_debt_to_gdp=370,
        vix=27, unemployment_rate=3.6,
        fiscal_deficit_to_gdp=5.5,
        snapshot_date="2022-06-01",
    ),
    2023: MacroContext(
        gdp_growth_actual=2.5, gdp_growth_expected=1.0,
        cpi_actual=4.1, cpi_expected=3.0,
        fed_funds_rate=5.25, treasury_10y=3.8, treasury_2y=4.7,
        credit_growth=2.0, total_debt_to_gdp=360,
        vix=14, unemployment_rate=3.6,
        fiscal_deficit_to_gdp=6.3,
        snapshot_date="2023-06-01",
    ),
    2024: MacroContext(
        gdp_growth_actual=2.8, gdp_growth_expected=2.0,
        cpi_actual=3.0, cpi_expected=2.5,
        fed_funds_rate=5.3, treasury_10y=4.2, treasury_2y=4.5,
        credit_growth=3.0, total_debt_to_gdp=365,
        vix=15, unemployment_rate=4.0,
        fiscal_deficit_to_gdp=6.0,
        snapshot_date="2024-06-01",
    ),
}


def main():
    print("=" * 80)
    print("  达利欧引擎 vs 真实历史回报 — 逐年验证")
    print("=" * 80)

    total_winner_hits = 0
    total_winner_possible = 0
    total_loser_hits = 0
    total_loser_possible = 0
    year_results = []

    for year in sorted(MACRO_SNAPSHOTS):
        if year not in ACTUAL_RETURNS:
            continue

        year_data = ACTUAL_RETURNS[year]
        macro = MACRO_SNAPSHOTS[year]

        # 1. 真实 winners/losers
        winners, losers = get_winners_losers(year_data)

        # 注入前期回报（均值回归信号）
        prev_year = year - 1
        if prev_year in ACTUAL_RETURNS:
            prior = ACTUAL_RETURNS[prev_year]
            macro.prior_returns = {
                ASSET_MAP.get(k, k): v for k, v in prior.items()
                if ASSET_MAP.get(k) in ENGINE_ASSET_TYPES
            }

        # 2. 引擎输出 — 达利欧 + 索罗斯联合
        # 先跑达利欧获取基本面预测
        chain = evaluate(macro)
        ow = {t.asset_type for t in chain.active_tilts if t.direction == "overweight"}
        uw = {t.asset_type for t in chain.active_tilts if t.direction == "underweight"}

        # 索罗斯调整（使用真实 FRED 数据）
