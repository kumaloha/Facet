"""
从 yfinance 拉取 1973-2024 各资产类别的年回报率
=================================================
ETF 不存在的早期年份用指数/期货近似。
输出写入 data_historical_returns.py
"""

import yfinance as yf
import pandas as pd
from datetime import datetime

# ── 配置 ──────────────────────────────────────────────
# (ticker, asset_class, start_year)
# 优先级从上到下：ETF > 指数/期货
SOURCES = [
    # Equity
    ("SPY",   "equity",            1993),
    ("^GSPC", "equity",            1973),  # fallback for pre-1993
    # Long-term bond
    ("TLT",   "long_term_bond",    2002),
    # Intermediate bond
    ("IEF",   "intermediate_bond", 2002),
    # Commodity
    ("DBC",   "commodity",         2006),
    # Gold
    ("GLD",   "gold",              2004),
    ("GC=F",  "gold",              1973),  # fallback for pre-2004
    # TIPS
    ("TIP",   "tips",              2003),
    # EM Bond
    ("EMB",   "em_bond",           2007),
]

END_YEAR = 2024


def fetch_annual_returns(ticker: str, start_year: int) -> dict[int, float]:
    """Download monthly data and compute calendar-year returns."""
    print(f"  Fetching {ticker} from {start_year}...")
    start = f"{start_year}-01-01"
    end = f"{END_YEAR + 1}-01-31"  # extra month to ensure Dec close is captured

    df = yf.download(ticker, start=start, end=end, interval="1mo", progress=False)
    if df.empty:
        print(f"    ⚠ No data for {ticker}")
        return {}

    # yfinance may return MultiIndex columns for single ticker; flatten
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    results = {}
    for year in range(start_year, END_YEAR + 1):
        year_data = df[df.index.year == year]
        if year_data.empty or len(year_data) < 2:
            continue

        # Use first Open of the year and last Close of the year
        first_open = year_data["Open"].iloc[0]
        last_close = year_data["Close"].iloc[-1]

        if pd.isna(first_open) or pd.isna(last_close) or first_open == 0:
            continue

        annual_return = (last_close / first_open - 1) * 100
        results[year] = round(float(annual_return), 1)

    print(f"    Got {len(results)} years: {min(results)}-{max(results)}" if results else "    No valid years")
    return results


def build_combined_returns() -> dict[int, dict[str, float]]:
    """Fetch all sources; for each asset_class, prefer ETF over index fallback."""
    # asset_class -> {year: return}
    asset_data: dict[str, dict[int, float]] = {}

    for ticker, asset_class, start_year in SOURCES:
        returns = fetch_annual_returns(ticker, start_year)
        if asset_class not in asset_data:
            asset_data[asset_class] = {}

        # Only fill years not already covered (ETF listed first = higher priority)
        for year, ret in returns.items():
            if year not in asset_data[asset_class]:
                asset_data[asset_class][year] = ret

    # Pivot to {year: {asset_class: return}}
    all_years = set()
    for ac_returns in asset_data.values():
        all_years.update(ac_returns.keys())

    combined = {}
    for year in sorted(all_years):
        row = {}
        for ac, ac_returns in sorted(asset_data.items()):
            if year in ac_returns:
                row[ac] = ac_returns[year]
        if row:
            combined[year] = row

    return combined


def write_output(data: dict[int, dict[str, float]], path: str):
    """Write the data as a Python module."""
    lines = [
        '"""',
        "真实历史资产回报率 (%) — 从 yfinance 拉取",
        "==========================================",
        "数据来源: Yahoo Finance ETF/指数历史价格",
        f"最后更新: {datetime.now().strftime('%Y-%m-%d')}",
        "",
        "资产类别:",
        "  equity           — SPY (1993+) / ^GSPC (1973-1992)",
        "  long_term_bond   — TLT (2002+)",
        "  intermediate_bond— IEF (2002+)",
        "  commodity        — DBC (2006+)",
        "  gold             — GLD (2004+) / GC=F (1973-2003)",
        "  tips             — TIP (2003+)",
        "  em_bond          — EMB (2007+)",
        '"""',
        "",
        "# {year: {asset_class: annual_return_pct}}",
        "ACTUAL_RETURNS = {",
    ]

    for year in sorted(data):
        items = data[year]
        parts = [f'"{k}": {v}' for k, v in sorted(items.items())]
        lines.append(f"    {year}: {{{', '.join(parts)}}},")

    lines.append("}")
    lines.append("")

    with open(path, "w") as f:
        f.write("\n".join(lines))
    print(f"\nWritten to {path}")


def main():
    print("Fetching historical returns from yfinance...\n")
    data = build_combined_returns()

    # Summary
    print(f"\nTotal years: {len(data)}")
    for year in sorted(data):
        assets = ", ".join(f"{k}={v}" for k, v in sorted(data[year].items()))
        print(f"  {year}: {assets}")

    output_path = "/Users/kuma/Projects/Facet/polaris/tests/data_historical_returns.py"
    write_output(data, output_path)


if __name__ == "__main__":
    main()
