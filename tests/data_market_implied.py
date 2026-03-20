"""真实市场隐含预期数据 (June-end values, 2007-2024)
=======================================================
数据来源:
  breakeven_5y      — FRED T5YIE (5-Year Breakeven Inflation Rate, %)
  credit_spread_hy  — FRED BAMLH0A0HYM2 (ICE BofA US HY OAS, %)
  vix               — Yahoo Finance ^VIX (CBOE VIX, June-end close)
  vix_slope         — ^VIX June-end minus May-end (positive = rising fear)
  implied_rate_change — FRED DGS2 minus DFF (2Y yield - Fed Funds Rate, %)
                        positive = market expects rate hikes
                        negative = market expects rate cuts
最后更新: 2026-03-20
"""

# {year: {metric: value}}
# All values rounded to 1 decimal place; None = data unavailable
MARKET_IMPLIED_REAL = {
    2007: {"breakeven_5y": 2.3, "credit_spread_hy": 3.0, "vix": 16.2, "vix_slope": 3.1, "implied_rate_change": -0.4},
    2008: {"breakeven_5y": 2.6, "credit_spread_hy": 7.3, "vix": 24.0, "vix_slope": 6.2, "implied_rate_change": 0.1},
    2009: {"breakeven_5y": 1.3, "credit_spread_hy": 10.6, "vix": 26.4, "vix_slope": -2.5, "implied_rate_change": 0.9},
    2010: {"breakeven_5y": 1.5, "credit_spread_hy": 7.1, "vix": 34.5, "vix_slope": 2.4, "implied_rate_change": 0.5},
    2011: {"breakeven_5y": 2.0, "credit_spread_hy": 5.4, "vix": 16.5, "vix_slope": 1.1, "implied_rate_change": 0.4},
    2012: {"breakeven_5y": 1.7, "credit_spread_hy": 6.4, "vix": 17.1, "vix_slope": -7.0, "implied_rate_change": 0.2},
    2013: {"breakeven_5y": 1.8, "credit_spread_hy": 5.2, "vix": 16.9, "vix_slope": 0.6, "implied_rate_change": 0.3},
    2014: {"breakeven_5y": 2.0, "credit_spread_hy": 3.5, "vix": 11.6, "vix_slope": 0.2, "implied_rate_change": 0.4},
    2015: {"breakeven_5y": 1.6, "credit_spread_hy": 5.0, "vix": 18.2, "vix_slope": 4.4, "implied_rate_change": 0.5},
    2016: {"breakeven_5y": 1.4, "credit_spread_hy": 6.2, "vix": 15.6, "vix_slope": 1.4, "implied_rate_change": 0.3},
    2017: {"breakeven_5y": 1.6, "credit_spread_hy": 3.8, "vix": 11.2, "vix_slope": 0.8, "implied_rate_change": 0.3},
    2018: {"breakeven_5y": 2.1, "credit_spread_hy": 3.7, "vix": 16.1, "vix_slope": 0.7, "implied_rate_change": 0.6},
    2019: {"breakeven_5y": 1.5, "credit_spread_hy": 4.1, "vix": 15.1, "vix_slope": -3.6, "implied_rate_change": -0.6},
    2020: {"breakeven_5y": 1.2, "credit_spread_hy": 6.4, "vix": 30.4, "vix_slope": 2.9, "implied_rate_change": 0.1},
    2021: {"breakeven_5y": 2.5, "credit_spread_hy": 3.0, "vix": 15.8, "vix_slope": -1.0, "implied_rate_change": 0.1},
    2022: {"breakeven_5y": 2.6, "credit_spread_hy": 5.9, "vix": 28.7, "vix_slope": 2.5, "implied_rate_change": 1.3},
    2023: {"breakeven_5y": 2.2, "credit_spread_hy": 4.0, "vix": 13.6, "vix_slope": -4.3, "implied_rate_change": -0.2},
    2024: {"breakeven_5y": 2.2, "credit_spread_hy": 3.2, "vix": 12.4, "vix_slope": -0.5, "implied_rate_change": -0.6},
}
