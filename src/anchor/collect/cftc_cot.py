"""
CFTC Commitments of Traders (COT) 数据
======================================
追踪期货市场各类玩家的持仓 — 谁在做多谁在做空。

数据源: CFTC 每周二采集, 周五公布
- 当前周: https://www.cftc.gov/dea/newcot/deafut.txt
- 历史:   https://www.cftc.gov/files/dea/history/deafut_txt_{year}.zip

三类持仓者:
1. Commercial (商业套保) — 真实需求方, 最懂基本面
2. Non-commercial (大投机者/基金) — 趋势追随, 拥挤=反转信号
3. Non-reportable (散户) — 通常是噪音, 极端时反指标
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field

import httpx
from loguru import logger

COT_URL = "https://www.cftc.gov/dea/newcot/deafut.txt"

# 我们关心的品种 (CFTC_Contract_Market_Code)
TRACKED_CONTRACTS = {
    "GOLD": "088691",
    "CRUDE_OIL_WTI": "067651",
    "TREASURY_BONDS": "020601",
    "SP500_EMINI": "13874A",
}

# deafut.txt 列名 (固定宽度CSV)
# 关键列位置 (0-indexed):
#   0: Market_and_Exchange_Names
#   3: As_of_Date_In_Form_YYMMDD (or YYYY-MM-DD)
#   7: CFTC_Contract_Market_Code
#  11: NonComm_Positions_Long_All
#  12: NonComm_Positions_Short_All
#  15: Comm_Positions_Long_All
#  16: Comm_Positions_Short_All


@dataclass
class COTPosition:
    """单个品种的 COT 持仓快照。"""
    contract: str               # e.g. "GOLD"
    date: str                   # YYYY-MM-DD
    commercial_long: int
    commercial_short: int
    commercial_net: int         # long - short
    noncommercial_long: int     # 大投机者/基金
    noncommercial_short: int
    noncommercial_net: int      # long - short
    name_raw: str = ""          # 原始品种名


@dataclass
class COTSnapshot:
    """一期 COT 报告的汇总。"""
    report_date: str
    positions: dict[str, COTPosition] = field(default_factory=dict)

    def summary(self) -> str:
        lines = [f"COT Report: {self.report_date}"]
        for key, pos in self.positions.items():
            lines.append(
                f"  {key}: Commercial net={pos.commercial_net:+,d}  "
                f"Spec net={pos.noncommercial_net:+,d}"
            )
        return "\n".join(lines)


def _parse_cot_row(row: list[str]) -> tuple[str | None, COTPosition | None]:
    """解析 deafut.txt 的一行，如果是我们跟踪的品种则返回 (key, COTPosition)。

    deafut.txt 列布局 (Futures Only, Short Format, 无表头):
      [0]  Market_and_Exchange_Names
      [1]  As_of_Date (YYMMDD)
      [2]  As_of_Date (YYYY-MM-DD)
      [3]  CFTC_Contract_Market_Code    ← 品种代码
      [4]  CFTC_Market_Code (exchange)
      [5]  CFTC_Region_Code
      [6]  CFTC_Commodity_Code
      [7]  Open_Interest_All
      [8]  NonComm_Positions_Long_All   ← 大投机者多头
      [9]  NonComm_Positions_Short_All  ← 大投机者空头
      [10] NonComm_Positions_Spreading
      [11] Comm_Positions_Long_All      ← 商业套保多头
      [12] Comm_Positions_Short_All     ← 商业套保空头
    """
    if len(row) < 13:
        return None, None

    contract_code = row[3].strip()

    # 匹配我们关心的品种
    matched_key = None
    for key, code in TRACKED_CONTRACTS.items():
        if contract_code == code:
            matched_key = key
            break

    if matched_key is None:
        return None, None

    try:
        date_str = row[2].strip()

        nc_long = int(row[8].strip().replace(",", ""))
        nc_short = int(row[9].strip().replace(",", ""))
        c_long = int(row[11].strip().replace(",", ""))
        c_short = int(row[12].strip().replace(",", ""))

        return matched_key, COTPosition(
            contract=matched_key,
            date=date_str,
            commercial_long=c_long,
            commercial_short=c_short,
            commercial_net=c_long - c_short,
            noncommercial_long=nc_long,
            noncommercial_short=nc_short,
            noncommercial_net=nc_long - nc_short,
            name_raw=row[0].strip(),
        )
    except (ValueError, IndexError) as e:
        logger.debug(f"COT parse error for {matched_key}: {e}")
        return None, None


def fetch_cot_current() -> COTSnapshot:
    """抓取最新一周的 COT 数据。

    Returns:
        COTSnapshot，包含我们跟踪品种的持仓。
        如果网络错误则返回空 snapshot。
    """
    try:
        resp = httpx.get(COT_URL, timeout=30, follow_redirects=True)
        resp.raise_for_status()
    except httpx.HTTPError as e:
        logger.warning(f"[CFTC] Failed to fetch COT: {e}")
        return COTSnapshot(report_date="unknown")

    text = resp.text
    reader = csv.reader(io.StringIO(text))

    # deafut.txt 没有表头行，直接是数据
    positions: dict[str, COTPosition] = {}
    report_date = "unknown"

    for row in reader:
        key, pos = _parse_cot_row(row)
        if key is not None and pos is not None:
            positions[key] = pos
            report_date = pos.date

    snapshot = COTSnapshot(report_date=report_date, positions=positions)
    logger.info(f"[CFTC] COT parsed: {len(positions)} contracts for {report_date}")
    return snapshot


def cot_to_dict(snapshot: COTSnapshot) -> dict:
    """将 COTSnapshot 转换为可 JSON 序列化的 dict。"""
    return {
        "report_date": snapshot.report_date,
        "positions": {
            key: {
                "contract": pos.contract,
                "date": pos.date,
                "commercial_long": pos.commercial_long,
                "commercial_short": pos.commercial_short,
                "commercial_net": pos.commercial_net,
                "noncommercial_long": pos.noncommercial_long,
                "noncommercial_short": pos.noncommercial_short,
                "noncommercial_net": pos.noncommercial_net,
                "name_raw": pos.name_raw,
            }
            for key, pos in snapshot.positions.items()
        },
    }


# ── 自测 ──

if __name__ == "__main__":
    print("=" * 60)
    print("cftc_cot.py — 抓取当前 COT 数据")
    print("=" * 60)

    snapshot = fetch_cot_current()
    print(snapshot.summary())

    if snapshot.positions:
        print(f"\n成功解析 {len(snapshot.positions)}/{len(TRACKED_CONTRACTS)} 个品种")
        d = cot_to_dict(snapshot)
        import json
        print(json.dumps(d, indent=2))
    else:
        print("\n未能解析任何品种 (可能是网络问题或CFTC格式变更)")
