"""
Force 4 自然之力数据采集
========================

数据源:
  1. FAO Food Price Index — 全球食品价格月度
  2. NOAA NCEI — 美国十亿美元灾害
  3. USGS Earthquake API — 全球地震实时
  4. disease.sh — 全球疫情实时
  5. WHO Disease Outbreak News — 疫情爆发通报
"""

from __future__ import annotations

import asyncio
from datetime import date, timedelta
from loguru import logger

import httpx


# ══════════════════════════════════════════════════════════════
#  FAO Food Price Index
# ══════════════════════════════════════════════════════════════


async def fetch_fao_food_price_index() -> list[dict]:
    """FAO 食品价格指数（月度，24种商品加权）。

    基期 2014-2016=100。>130=供给压力显著，>150=粮食危机。
    2022年3月峰值159.7（俄乌战争）。

    Returns: [{date, value, yoy_change}]
    """
    url = "https://www.fao.org/faostat/api/v2/data/FOOD_INDEX"
    # FAO API 比较复杂，用简单的 CSV 下载替代
    # 实际用 FAOSTAT bulk download 或爬取月报
    csv_url = "https://www.fao.org/worldfoodsituation/foodpricesindex/en/"

    results = []
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            # FAO 提供 JSON 端点
            resp = await client.get(
                "https://fpma.fao.org/giews/fpmat4/#/dashboard/tool/international",
                follow_redirects=True,
            )
            # FAO API 不稳定，降级方案：用 FRED 的食品 CPI 作为代理
            logger.info("[FAO] Food Price Index endpoint accessed (parse needed)")
    except Exception as e:
        logger.warning(f"[FAO] Failed: {e}. Use FRED CPIUFDSL as proxy.")

    return results


# ══════════════════════════════════════════════════════════════
#  NOAA Billion-Dollar Disasters
# ══════════════════════════════════════════════════════════════


async def fetch_noaa_disasters(year: int | None = None) -> dict:
    """NOAA 十亿美元灾害统计。

    Returns: {year, count, total_cost_billions, events: [{name, type, cost}]}
    """
    if year is None:
        year = date.today().year

    url = f"https://www.ncei.noaa.gov/access/billions/events.pdf"
    # NOAA 提供 JSON 时间序列
    time_series_url = "https://www.ncei.noaa.gov/access/billions/time-series"

    result = {"year": year, "count": 0, "total_cost_billions": 0, "events": []}

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            # 尝试 JSON 端点
            resp = await client.get(
                f"https://www.ncei.noaa.gov/access/billions/time-series/US",
                follow_redirects=True,
            )
            if resp.status_code == 200:
                logger.info(f"[NOAA] Disasters data fetched for {year}")
                # 需要解析 HTML/JSON
            else:
                logger.warning(f"[NOAA] Status {resp.status_code}")
    except Exception as e:
        logger.warning(f"[NOAA] Failed: {e}")

    return result


# ══════════════════════════════════════════════════════════════
#  USGS Earthquake API
# ══════════════════════════════════════════════════════════════


async def fetch_significant_earthquakes(days: int = 30) -> list[dict]:
    """USGS 近期重大地震（M5.5+）。

    Returns: [{time, magnitude, place, depth_km, tsunami_flag}]
    """
    end = date.today()
    start = end - timedelta(days=days)

    url = (
        f"https://earthquake.usgs.gov/fdsnws/event/1/query?"
        f"format=geojson&starttime={start}&endtime={end}"
        f"&minmagnitude=5.5&orderby=magnitude"
    )

    results = []
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                for feature in data.get("features", [])[:20]:
                    props = feature["properties"]
                    results.append({
                        "time": props.get("time"),
                        "magnitude": props.get("mag"),
                        "place": props.get("place"),
                        "tsunami": props.get("tsunami", 0),
                    })
                logger.info(f"[USGS] {len(results)} significant earthquakes in last {days} days")
            else:
                logger.warning(f"[USGS] Status {resp.status_code}")
    except Exception as e:
        logger.warning(f"[USGS] Failed: {e}")

    return results


# ══════════════════════════════════════════════════════════════
#  disease.sh — 全球疫情数据
# ══════════════════════════════════════════════════════════════


async def fetch_disease_data() -> dict:
    """全球疫情当前状态。

    Returns: {
        covid: {active, deaths_today, cases_today},
        influenza: {...},
        alerts: [...]
    }
    """
    result = {"covid": None, "influenza": None, "alerts": []}

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # COVID 全球汇总
            resp = await client.get("https://disease.sh/v3/covid-19/all")
            if resp.status_code == 200:
                data = resp.json()
                result["covid"] = {
                    "active": data.get("active", 0),
                    "deaths_today": data.get("todayDeaths", 0),
                    "cases_today": data.get("todayCases", 0),
                    "cases_per_million": data.get("casesPerOneMillion", 0),
                }
                logger.info(f"[disease.sh] COVID data fetched")

            # 流感
            resp2 = await client.get("https://disease.sh/v3/influenza/cdc/ILI")
            if resp2.status_code == 200:
                data2 = resp2.json()
                if data2:
                    latest = data2[-1] if isinstance(data2, list) else data2
                    result["influenza"] = latest
                    logger.info(f"[disease.sh] Influenza data fetched")

    except Exception as e:
        logger.warning(f"[disease.sh] Failed: {e}")

    return result


# ══════════════════════════════════════════════════════════════
#  WHO Disease Outbreak News
# ══════════════════════════════════════════════════════════════


async def fetch_who_outbreaks(limit: int = 10) -> list[dict]:
    """WHO 最新疫情爆发通报。

    Returns: [{title, date, disease, country, url}]
    """
    url = "https://www.who.int/api/news/diseaseoutbreaknews"

    results = []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, params={"sf": "", "sort": "PublishDate", "order": "Descending", "take": limit})
            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("value", [])[:limit]:
                    results.append({
                        "title": item.get("Title", ""),
                        "date": item.get("PublishDate", ""),
                        "url": f"https://www.who.int{item.get('UrlName', '')}",
                    })
                logger.info(f"[WHO] {len(results)} outbreak news items")
            else:
                logger.warning(f"[WHO] Status {resp.status_code}")
    except Exception as e:
        logger.warning(f"[WHO] Failed: {e}")

    return results


# ══════════════════════════════════════════════════════════════
#  综合: Force 4 数据快照
# ══════════════════════════════════════════════════════════════


async def fetch_force4_snapshot() -> dict:
    """采集 Force 4 所有数据源，返回综合快照。"""

    earthquakes, diseases, who_news = await asyncio.gather(
        fetch_significant_earthquakes(30),
        fetch_disease_data(),
        fetch_who_outbreaks(5),
        return_exceptions=True,
    )

    snapshot = {
        "date": str(date.today()),
        "earthquakes": earthquakes if not isinstance(earthquakes, Exception) else [],
        "disease": diseases if not isinstance(diseases, Exception) else {},
        "who_alerts": who_news if not isinstance(who_news, Exception) else [],
        "summary": {},
    }

    # 汇总
    eq_list = snapshot["earthquakes"]
    if eq_list:
        max_eq = max(eq_list, key=lambda x: x.get("magnitude", 0))
        snapshot["summary"]["max_earthquake_30d"] = max_eq.get("magnitude", 0)
        snapshot["summary"]["significant_earthquakes_30d"] = len(eq_list)
        snapshot["summary"]["tsunami_warnings"] = sum(1 for e in eq_list if e.get("tsunami"))

    disease = snapshot["disease"]
    if disease.get("covid"):
        covid = disease["covid"]
        snapshot["summary"]["covid_active_global"] = covid.get("active", 0)
        snapshot["summary"]["covid_daily_deaths"] = covid.get("deaths_today", 0)

    if snapshot["who_alerts"]:
        snapshot["summary"]["who_latest_alert"] = snapshot["who_alerts"][0].get("title", "")

    return snapshot


def format_force4_snapshot(snapshot: dict) -> str:
    """格式化 Force 4 数据快照。"""
    lines = [""]
    lines.append("  Force 4: 自然之力 — 一手数据")
    lines.append("  ════════════════════════════════════════════════")

    s = snapshot.get("summary", {})

    # 地震
    eq_count = s.get("significant_earthquakes_30d", 0)
    max_mag = s.get("max_earthquake_30d", 0)
    tsunami = s.get("tsunami_warnings", 0)
    lines.append(f"\n  地震 (USGS, 30天):")
    lines.append(f"    M5.5+: {eq_count}次  最大: M{max_mag:.1f}  海啸警报: {tsunami}")
    for eq in snapshot.get("earthquakes", [])[:5]:
        lines.append(f"    · M{eq.get('magnitude', 0):.1f} {eq.get('place', '')}")

    # 疫情
    covid = snapshot.get("disease", {}).get("covid")
    lines.append(f"\n  疫情 (disease.sh):")
    if covid:
        lines.append(f"    COVID活跃: {covid.get('active', 0):,}  今日死亡: {covid.get('deaths_today', 0):,}")
    else:
        lines.append(f"    无数据")

    # WHO
    lines.append(f"\n  WHO 最新通报:")
    for alert in snapshot.get("who_alerts", [])[:3]:
        lines.append(f"    · {alert.get('title', '')[:60]}")
        lines.append(f"      {alert.get('date', '')[:10]}")

    lines.append("")
    return "\n".join(lines)
