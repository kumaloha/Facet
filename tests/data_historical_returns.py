"""
真实历史资产回报率 (%) — 从 yfinance 拉取
==========================================
数据来源: Yahoo Finance ETF/指数历史价格
最后更新: 2026-03-20

资产类别:
  equity           — SPY (1993+) / ^GSPC (1973-1992)
  long_term_bond   — TLT (2002+)
  intermediate_bond— IEF (2002+)
  commodity        — DBC (2006+)
  gold             — GLD (2004+) / GC=F (1973-2003)
  tips             — TIP (2003+)
  em_bond          — EMB (2007+)
"""

# {year: {asset_class: annual_return_pct}}
ACTUAL_RETURNS = {
    1985: {"equity": 26.4},
    1986: {"equity": 14.6},
    1987: {"equity": 2.0},
    1988: {"equity": 12.4},
    1989: {"equity": 27.3},
    1990: {"equity": -6.6},
    1991: {"equity": 26.3},
    1992: {"equity": 4.5},
    1993: {"equity": 7.9},
    1994: {"equity": -0.4},
    1995: {"equity": 36.8},
    1996: {"equity": 22.1},
    1997: {"equity": 32.0},
    1998: {"equity": 27.9},
    1999: {"equity": 20.0},
    2000: {"equity": -10.9, "gold": -0.7},
    2001: {"equity": -12.6, "gold": 3.8},
    2002: {"equity": -22.5, "gold": 13.6, "intermediate_bond": 6.2, "long_term_bond": 9.7},
    2003: {"equity": 26.7, "gold": 20.1, "intermediate_bond": 4.3, "long_term_bond": 1.6},
    2004: {"equity": 9.7, "gold": -1.4, "intermediate_bond": 3.7, "long_term_bond": 8.3, "tips": 7.6},
    2005: {"equity": 3.7, "gold": 20.0, "intermediate_bond": 2.0, "long_term_bond": 8.1, "tips": 1.2},
    2006: {"commodity": 0.1, "equity": 14.6, "gold": 20.9, "intermediate_bond": 2.1, "long_term_bond": 0.3, "tips": 0.4},
    2007: {"commodity": 30.0, "equity": 4.1, "gold": 29.7, "intermediate_bond": 9.2, "long_term_bond": 8.5, "tips": 10.3},
    2008: {"commodity": -33.9, "em_bond": -2.9, "equity": -37.4, "gold": 3.5, "intermediate_bond": 17.1, "long_term_bond": 32.8, "tips": -0.2},
    2009: {"commodity": 14.2, "em_bond": 14.2, "equity": 25.4, "gold": 24.9, "intermediate_bond": -7.2, "long_term_bond": -22.8, "tips": 8.3},
    2010: {"commodity": 9.5, "em_bond": 9.9, "equity": 13.5, "gold": 26.3, "intermediate_bond": 8.6, "long_term_bond": 8.3, "tips": 5.6},
    2011: {"commodity": -3.6, "em_bond": 6.5, "equity": 0.5, "gold": 9.6, "intermediate_bond": 16.0, "long_term_bond": 34.6, "tips": 13.6},
    2012: {"commodity": 1.6, "em_bond": 15.9, "equity": 13.1, "gold": 4.7, "intermediate_bond": 4.0, "long_term_bond": 3.5, "tips": 6.2},
    2013: {"commodity": -8.7, "em_bond": -8.6, "equity": 29.1, "gold": -29.0, "intermediate_bond": -6.0, "long_term_bond": -12.9, "tips": -8.4},
    2014: {"commodity": -27.7, "em_bond": 5.1, "equity": 13.3, "gold": -3.7, "intermediate_bond": 8.6, "long_term_bond": 26.9, "tips": 3.5},
    2015: {"commodity": -26.9, "em_bond": -0.2, "equity": 0.2, "gold": -9.8, "intermediate_bond": 1.1, "long_term_bond": -2.5, "tips": -1.9},
    2016: {"commodity": 18.0, "em_bond": 8.9, "equity": 13.2, "gold": 6.3, "intermediate_bond": 0.2, "long_term_bond": -0.4, "tips": 3.9},
    2017: {"commodity": 4.0, "em_bond": 9.2, "equity": 20.3, "gold": 12.8, "intermediate_bond": 2.7, "long_term_bond": 9.4, "tips": 2.5},
    2018: {"commodity": -13.1, "em_bond": -7.0, "equity": -5.5, "gold": -2.7, "intermediate_bond": 0.7, "long_term_bond": -1.8, "tips": -1.6},
    2019: {"commodity": 11.4, "em_bond": 14.9, "equity": 32.7, "gold": 17.8, "intermediate_bond": 7.6, "long_term_bond": 13.6, "tips": 8.1},
    2020: {"commodity": -8.0, "em_bond": 4.5, "equity": 17.2, "gold": 24.0, "intermediate_bond": 9.4, "long_term_bond": 16.8, "tips": 10.4},
    2021: {"commodity": 40.0, "em_bond": -3.0, "equity": 27.8, "gold": -6.1, "intermediate_bond": -3.3, "long_term_bond": -4.2, "tips": 5.0},
    2022: {"commodity": 19.0, "em_bond": -19.0, "equity": -18.8, "gold": 0.5, "intermediate_bond": -15.0, "long_term_bond": -30.7, "tips": -12.3},
    2023: {"commodity": -9.5, "em_bond": 8.4, "equity": 25.0, "gold": 11.6, "intermediate_bond": 1.9, "long_term_bond": -0.5, "tips": 2.8},
    2024: {"commodity": -3.4, "em_bond": 4.7, "equity": 25.3, "gold": 26.5, "intermediate_bond": -0.8, "long_term_bond": -8.1, "tips": 1.6},
}
