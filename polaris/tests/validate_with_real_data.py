"""
用真实历史回报验证达利欧引擎的资产配置建议
=============================================

1. 从 data_historical_returns.py 读取每年各资产的真实回报
2. 客观规则: 回报最高2个 = winners, 最低2个 = losers
3. 跑达利欧引擎，看超配/低配是否命中 winners/losers
"""

from data_historical_returns import ACTUAL_RETURNS
from polaris.chains.dalio import MacroContext, evaluate, to_dalio_result
from polaris.chains.soros import MarketImplied, evaluate_soros, compute_soros_adjustments

# ── 资产类别映射: ACTUAL_RETURNS key → 达利欧引擎 asset_type ──
# 达利欧引擎用: equity_cyclical, nominal_bond, inflation_linked_bond, commodity, gold, cash
ASSET_MAP = {
    "equity":            "equity_cyclical",
    "long_term_bond":    "nominal_bond",
    "intermediate_bond": "nominal_bond",       # 也是名义债券，但优先用 long_term_bond
    "commodity":         "commodity",
    "gold":              "gold",
    "tips":              "inflation_linked_bond",
    "em_bond":           "em_bond",            # 引擎没有 em_bond，不参与匹配
}

# 达利欧引擎认识的资产类型
ENGINE_ASSET_TYPES = {"equity_cyclical", "equity_defensive", "nominal_bond",
                      "inflation_linked_bond", "commodity", "gold", "cash"}


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

        # 2. 引擎输出 — 达利欧 + 索罗斯联合
        # 先跑达利欧获取基本面预测
        chain = evaluate(macro)
        dalio_result = to_dalio_result(chain)

        # 跑索罗斯: 用市场数据检测偏差
        market = MarketImplied(
            breakeven_5y=None,  # 会在下面按年填入
            credit_spread_hy=None,
            vix=macro.vix,
        )
        # 每年的 breakeven 和 credit spread 近似值
        _market_data = {
            2008: (1.0, 18.0), 2009: (1.5, 8.0), 2010: (1.8, 6.5), 2011: (2.0, 7.5),
            2012: (2.0, 5.5), 2013: (2.2, 4.5), 2014: (1.8, 4.0), 2015: (1.5, 5.5),
            2016: (1.7, 5.0), 2017: (1.8, 3.5), 2018: (2.0, 4.0), 2019: (1.6, 4.0),
            2020: (1.2, 8.0), 2021: (2.5, 3.0), 2022: (2.8, 5.0), 2023: (2.3, 4.5),
            2024: (2.3, 3.5),
        }
        if year in _market_data:
            market.breakeven_5y, market.credit_spread_hy = _market_data[year]

        soros_result = evaluate_soros(dalio_result, market, current_rate=macro.fed_funds_rate)
        soros_adj = compute_soros_adjustments(soros_result)

        ow = {t.asset_type for t in chain.active_tilts if t.direction == "overweight"}
        uw = {t.asset_type for t in chain.active_tilts if t.direction == "underweight"}

        # 索罗斯调整: 需要真实 FRED 数据才可靠。近似数据噪音太大,暂不翻转。
        # TODO: 接入真实 FRED breakeven/credit spread 后启用
        # if soros_adj: ...

        quadrant = chain.regime.quadrant if chain.regime else "N/A"

        # 3. 匹配
        winner_hits = []
        winner_misses = []
        for asset_key, ret in winners:
            engine_type = map_to_engine_type(asset_key)
            if engine_type is None:
                continue
            if engine_type in ow:
                winner_hits.append((asset_key, engine_type, ret))
            else:
                winner_misses.append((asset_key, engine_type, ret))

        loser_hits = []
        loser_misses = []
        for asset_key, ret in losers:
            engine_type = map_to_engine_type(asset_key)
            if engine_type is None:
                continue
            if engine_type in uw:
                loser_hits.append((asset_key, engine_type, ret))
            else:
                loser_misses.append((asset_key, engine_type, ret))

        w_total = len(winner_hits) + len(winner_misses)
        l_total = len(loser_hits) + len(loser_misses)
        total_winner_hits += len(winner_hits)
        total_winner_possible += w_total
        total_loser_hits += len(loser_hits)
        total_loser_possible += l_total

        score = len(winner_hits) + len(loser_hits)
        possible = w_total + l_total
        pct = f"{score}/{possible}" if possible > 0 else "N/A"

        year_results.append((year, quadrant, score, possible, winner_hits,
                             winner_misses, loser_hits, loser_misses))

        # 4. 打印
        print(f"\n{'─' * 80}")
        print(f"  {year}  |  象限: {quadrant}  |  得分: {pct}")
        print(f"{'─' * 80}")

        print(f"  真实回报: ", end="")
        print(", ".join(f"{k}={v:+.1f}%" for k, v in sorted(year_data.items(), key=lambda x: -x[1])))

        print(f"  引擎超配: {', '.join(sorted(ow)) if ow else '无'}")
        print(f"  引擎低配: {', '.join(sorted(uw)) if uw else '无'}")

        print(f"  Winners (top 2): ", end="")
        for k, r in winners:
            engine_t = map_to_engine_type(k)
            hit = "HIT" if engine_t and engine_t in ow else "MISS"
            print(f"{k}({r:+.1f}%)[{hit}]  ", end="")
        print()

        print(f"  Losers  (bot 2): ", end="")
        for k, r in losers:
            engine_t = map_to_engine_type(k)
            hit = "HIT" if engine_t and engine_t in uw else "MISS"
            print(f"{k}({r:+.1f}%)[{hit}]  ", end="")
        print()

    # ── 总结 ──────────────────────────────────────────────
    print(f"\n\n{'=' * 80}")
    print(f"  总分统计")
    print(f"{'=' * 80}")

    total_hits = total_winner_hits + total_loser_hits
    total_possible = total_winner_possible + total_loser_possible

    print(f"  Winner 命中: {total_winner_hits}/{total_winner_possible} "
          f"({total_winner_hits/total_winner_possible:.0%})" if total_winner_possible else "  Winner: N/A")
    print(f"  Loser  命中: {total_loser_hits}/{total_loser_possible} "
          f"({total_loser_hits/total_loser_possible:.0%})" if total_loser_possible else "  Loser: N/A")
    print(f"  综合命中率:  {total_hits}/{total_possible} "
          f"({total_hits/total_possible:.0%})" if total_possible else "  综合: N/A")

    print(f"\n  逐年得分:")
    for year, quadrant, score, possible, *_ in year_results:
        bar = "+" * score + "-" * (possible - score) if possible > 0 else ""
        print(f"    {year} [{quadrant:>30s}]  {score}/{possible}  {bar}")


if __name__ == "__main__":
    main()
