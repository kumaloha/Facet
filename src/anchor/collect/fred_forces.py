"""
FRED → 五大力量数据映射
========================

直接从 FRED 拉取最新值，按五大力量分组，
返回 dalio_forces.py 各 assess_force*() 期望的 dict 格式。

不经过数据库，适合实时分析。
"""

from __future__ import annotations

from datetime import date, timedelta
from loguru import logger


# ── FRED Series → 五大力量字段映射 ──────────────────────────────────────

# Force 1: 债务/信贷周期
_FORCE1_SERIES = {
    # (FRED series_id, output_key, transform)
    # transform: "direct" = 直接用最新值
    #            "yoy_index" = 用指数算同比%
    #            "yoy_level" = 用绝对值算同比%
    ("FEDFUNDS", "fed_funds_rate", "direct"),
    ("TOTBKCR", "credit_growth", "yoy_level"),          # 银行信贷总量 → 同比%
    ("GFDEGDQ188S", "total_debt_to_gdp", "direct"),     # 联邦债务/GDP %
    ("UNRATE", "unemployment_rate", "direct"),
    ("CPIAUCSL", "cpi_actual", "yoy_index"),            # CPI 指数 → 同比%
    ("A191RL1Q225SBEA", "gdp_growth_actual", "direct"), # 实际GDP同比%
    # 结构性
    ("MDSP", "mortgage_debt_service", "direct"),         # 房贷偿付比/收入 %
    ("DRSFRMACBS", "mortgage_delinquency", "direct"),    # 房贷逾期率 %
    ("CSUSHPISA", "case_shiller_hpi", "direct"),         # Case-Shiller 房价指数
    ("NFCILEVERAGE", "financial_leverage", "direct"),    # 芝加哥联储杠杆子指数
    ("DRTSCILM", "lending_standards", "direct"),         # 贷款标准收紧%
    ("HDTGPDUSQ163N", "household_debt_gdp", "direct"),  # 家庭债务/GDP %
    # 央行资产负债表 (QE/QT = 过去15年最大的流动性力量)
    ("WALCL", "fed_balance_sheet", "direct"),            # Fed 总资产 (百万$)
    ("WALCL", "fed_bs_yoy", "yoy_level"),               # Fed 资产同比% (QE→正, QT→负)
    # 影子银行代理信号
    ("SOFR", "sofr_rate", "direct"),                     # 担保隔夜融资利率 (影子银行融资成本)
    ("RRPONTSYD", "reverse_repo", "direct"),             # 逆回购余额 (十亿$, 流动性方向)
}

# Force 2: 内部秩序
_FORCE2_SERIES = {
    ("UMCSENT", "consumer_sentiment", "direct"),         # 密歇根消费者信心
    ("SIPOVGINIUSA", "gini_coefficient", "direct"),      # 基尼系数 (0-100制)
    ("LES1252881600Q", "real_weekly_earnings_growth", "yoy_level"),  # 实际周薪 → 同比%
    ("OPHNFB", "nonfarm_productivity_growth", "yoy_index"),  # 非农生产率 → 同比%
    ("FYFSGDA188S", "fiscal_deficit_to_gdp", "abs"),     # 联邦赤字/GDP (取绝对值)
    ("MEHOINUSA672N", "real_median_income", "direct"),    # 实际中位收入
    # 劳动力微观 (领先就业指标)
    ("JTSJOL", "jolts_openings", "direct"),              # 职位空缺数 (千)
    ("JTSQUR", "jolts_quits_rate", "direct"),            # 离职率 % (员工信心=领先消费)
}

# Force 3: 外部秩序
_FORCE3_SERIES = {
    ("BOPGSTB", "trade_balance", "millions_to_billions"),  # 商品贸易差额 百万$ → 十亿$
    ("DTWEXBGS", "dollar_index_yoy", "yoy_index"),         # 美元指数 → 同比%
    ("DCOILWTICO", "oil_price_yoy", "yoy_index"),          # WTI → 同比%
    ("USEPUINDXD", "epu_index", "direct"),                 # 经济政策不确定性指数 (日度)
}

# Force 4: 自然之力（FRED 部分，其余来自 nature.py）
_FORCE4_SERIES = {
    ("CPIUFDSL", "food_price_yoy", "yoy_index"),         # 食品CPI → 同比%
}

# Force 5: 人类创造力/技术
_FORCE5_SERIES = {
    ("OPHNFB", "productivity_growth", "yoy_index"),        # 非农生产率 → 同比%
    ("Y694RC1Q027SBEA", "rd_spending_growth", "yoy_index"), # R&D支出 → 同比%
    ("NASDAQCOM", "nasdaq_yoy", "yoy_index"),              # NASDAQ综合 → 同比% (科技板块代理)
}

# ── 索罗斯: 衍生品 + 市场信念信号 (跨 Force, 单独分组) ──
_SOROS_SERIES = {
    ("VIXCLS", "vix", "direct"),                           # VIX (日度)
    ("VXVCLS", "vix_3m", "direct"),                        # VIX 3个月 (期限结构用)
    ("SKEWCLS", "skew_index", "direct"),                    # CBOE SKEW (尾部保护需求, 日度)
    ("BAMLC0A0CM", "credit_spread_ig", "direct"),          # 投资级信用利差
    ("BAMLH0A0HYM2", "credit_spread_hy", "direct"),       # 高收益信用利差
    ("T5YIE", "breakeven_5y", "direct"),                   # 5年通胀预期
    ("T10YIE", "breakeven_10y", "direct"),                 # 10年通胀预期
    # 人口 (大周期结构参数)
    ("LFWA64TTUSM647S", "working_age_pop", "yoy_level"),   # 劳动年龄人口同比% (极慢)
}


def _fetch_fred_latest(series_ids: list[str], api_key: str, lookback_days: int = 1000) -> dict[str, "pd.Series"]:
    """批量从 FRED 拉取序列，返回 {series_id: pandas.Series}。"""
    from fredapi import Fred
    fred = Fred(api_key=api_key)
    start = (date.today() - timedelta(days=lookback_days)).isoformat()
    results = {}
    for sid in series_ids:
        try:
            s = fred.get_series(sid, observation_start=start)
            if s is not None and not s.empty:
                results[sid] = s.dropna()
        except Exception as e:
            logger.warning(f"[FRED] {sid}: {e}")
    return results


def _apply_transform(series, transform: str) -> float | None:
    """对 FRED 原始序列应用变换，返回最新值。

    自动检测数据频率（日/周/月/季度/年），用正确的回看窗口。
    """
    if series is None or series.empty:
        return None

    if transform == "direct":
        return float(series.iloc[-1])

    if transform == "abs":
        return abs(float(series.iloc[-1]))

    if transform in ("yoy_index", "yoy_level"):
        latest = float(series.iloc[-1])

        # 自动检测频率并选择回看窗口
        target_date = series.index[-1] - timedelta(days=365)
        past = series[series.index <= target_date]

        if past.empty:
            # 数据不足一年，尝试用最早的值
            if len(series) < 2:
                return None
            past_val = float(series.iloc[0])
            # 计算实际时间跨度，年化
            days_span = (series.index[-1] - series.index[0]).days
            if days_span < 90 or past_val == 0:
                return None
            return ((latest / past_val) ** (365.0 / days_span) - 1) * 100

        past_val = float(past.iloc[-1])
        if past_val == 0:
            return 0.0

        return ((latest / past_val) - 1) * 100

    if transform == "millions_to_billions":
        return float(series.iloc[-1]) / 1000

    return float(series.iloc[-1])


def fetch_five_forces_from_fred(api_key: str | None = None) -> dict[str, dict]:
    """从 FRED 拉取所有五大力量数据，返回按 Force 分组的 dict。

    Returns:
        {
            "force1": {"fed_funds_rate": 4.33, "credit_growth": 3.5, ...},
            "force2": {"consumer_sentiment": 57.9, "gini_coefficient": 41.5, ...},
            "force3": {"trade_balance": -98000, "oil_price_yoy": -5, ...},
            "force4": {"food_price_yoy": 2.1, ...},
            "force5": {"productivity_growth": 2.1, "rd_spending_growth": 8, ...},
        }
    """
    if api_key is None:
        from anchor.config import settings
        api_key = settings.fred_api_key
    if not api_key:
        logger.error("[FRED] No API key. Set FRED_API_KEY in .env")
        return {f"force{i}": {} for i in range(1, 6)}

    # 收集所有需要的 series
    all_mappings = [
        (1, _FORCE1_SERIES),
        (2, _FORCE2_SERIES),
        (3, _FORCE3_SERIES),
        (4, _FORCE4_SERIES),
        (5, _FORCE5_SERIES),
        ("soros", _SOROS_SERIES),
    ]
    all_series_ids = set()
    for _, mappings in all_mappings:
        for sid, _, _ in mappings:
            all_series_ids.add(sid)

    # 批量拉取
    logger.info(f"[FRED] Fetching {len(all_series_ids)} series for five forces...")
    raw = _fetch_fred_latest(sorted(all_series_ids), api_key)
    logger.info(f"[FRED] Got {len(raw)}/{len(all_series_ids)} series")

    # 按 Force 分组转换
    result = {}
    for force_id, mappings in all_mappings:
        d = {}
        for sid, key, transform in mappings:
            val = _apply_transform(raw.get(sid), transform)
            if val is not None:
                d[key] = round(val, 2)
        group_key = f"force{force_id}" if isinstance(force_id, int) else str(force_id)
        result[group_key] = d
        logger.info(f"[FRED] {group_key}: {len(d)} indicators")

    return result


def fetch_and_build_forces_data(api_key: str | None = None) -> tuple[dict, dict, dict, dict, dict]:
    """便捷函数：直接返回五个 dict，对应 build_five_forces_view 的五个参数。

    Returns: (macro_data, internal_data, external_data, nature_data, tech_data)
    """
    forces = fetch_five_forces_from_fred(api_key)
    return (
        forces.get("force1", {}),
        forces.get("force2", {}),
        forces.get("force3", {}),
        forces.get("force4", {}),
        forces.get("force5", {}),
    )
