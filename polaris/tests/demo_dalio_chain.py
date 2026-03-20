"""
达利欧因果链 Demo — 四种典型宏观情景
====================================

情景 1: 滞胀（2026-03 模拟）— 增长↓通胀↑，最危险象限
情景 2: 金发姑娘 — 增长↑通胀↓，最舒服象限
情景 3: 通缩衰退 — 增长↓通胀↓，债券牛市
情景 4: 过热 — 增长↑通胀↑，大宗商品行情
"""

from polaris.chains.dalio import MacroContext, evaluate, format_dalio, to_dalio_result


def main():
    scenarios = {
        "滞胀 (Stagflation)": MacroContext(
            gdp_growth_actual=1.2, gdp_growth_expected=2.2,
            cpi_actual=4.0, cpi_expected=2.5,
            fed_funds_rate=4.75, treasury_10y=4.3, treasury_2y=4.6,
            credit_growth=-1.0, total_debt_to_gdp=350,
            vix=28, sp500_earnings_yield=4.5,
            unemployment_rate=5.8, fiscal_deficit_to_gdp=7.0,
            snapshot_date="2026-03-20",
        ),
        "金发姑娘 (Goldilocks)": MacroContext(
            gdp_growth_actual=3.0, gdp_growth_expected=2.5,
            cpi_actual=1.8, cpi_expected=2.0,
            fed_funds_rate=1.5, treasury_10y=2.5, treasury_2y=2.0,
            credit_growth=7.0, total_debt_to_gdp=200,
            vix=13, sp500_earnings_yield=5.5,
            unemployment_rate=3.8, fiscal_deficit_to_gdp=2.5,
            snapshot_date="2026-03-20",
        ),
        "通缩衰退 (Deflationary Recession)": MacroContext(
            gdp_growth_actual=0.5, gdp_growth_expected=2.0,
            cpi_actual=0.8, cpi_expected=2.0,
            fed_funds_rate=0.25, treasury_10y=1.5, treasury_2y=0.5,
            credit_growth=-3.0, total_debt_to_gdp=380,
            vix=38, sp500_earnings_yield=6.0,
            unemployment_rate=7.5, fiscal_deficit_to_gdp=9.0,
            snapshot_date="2026-03-20",
        ),
        "过热 (Overheating)": MacroContext(
            gdp_growth_actual=4.0, gdp_growth_expected=2.5,
            cpi_actual=5.0, cpi_expected=2.5,
            fed_funds_rate=5.5, treasury_10y=5.0, treasury_2y=5.2,
            credit_growth=10.0, total_debt_to_gdp=280,
            vix=18, sp500_earnings_yield=4.0,
            unemployment_rate=3.2, fiscal_deficit_to_gdp=4.0,
            snapshot_date="2026-03-20",
        ),
    }

    for name, macro in scenarios.items():
        print(f"\n{'#' * 64}")
        print(f"  情景: {name}")
        print(f"{'#' * 64}")

        chain = evaluate(macro)
        print(format_dalio(chain))

        # 验证 DalioResult 转换
        dr = to_dalio_result(chain)
        print(f"  → DalioResult.signal = {dr.school_score.signal}")
        print(f"  → Tilts: {len(dr.active_tilts)}, Hedges: {len(dr.hedge_specs)}")
        print()


if __name__ == "__main__":
    main()
