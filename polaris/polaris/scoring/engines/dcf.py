"""
DCF 引擎
========
正向 DCF（巴菲特·内在价值） + 反向 DCF（索罗斯·隐含增速）。

两者共享 owner_earnings、折现率等基础数据。
折现率 = 10Y Treasury yield，来自外部数据。

使用条件：
- 正向 DCF：公司必须通过巴菲特过滤 + 有 management_guidance
- 反向 DCF：需要当前股价（外部数据）
"""

from dataclasses import dataclass

# 预测年限
PROJECTION_YEARS = 10


@dataclass
class DCFResult:
    intrinsic_value: float | None = None
    valuation_path: str | None = None  # A / B / C / D
    key_assumptions: dict[str, float] | None = None
    status: str = "not_computed"  # valued / unvaluable / divergent / not_computed


@dataclass
class ReverseDCFResult:
    implied_growth_rate: float | None = None
    status: str = "not_computed"  # computed / needs_price / not_computed


def forward_dcf(
    current_owner_earnings: float,
    growth_rate: float,
    discount_rate: float,
    payout_ratio: float = 0.0,
    years: int = PROJECTION_YEARS,
) -> float:
    """正向 DCF：从基本面推算内在价值。

    路径通用公式：
    PV = Σ [OE × (1+g)^t] / (1+r)^t + 永续价值

    永续价值 = 第 N 年 OE / r（假设零增长永续）
    """
    if discount_rate <= 0:
        return 0.0

    pv = 0.0
    oe = current_owner_earnings

    for t in range(1, years + 1):
        oe = oe * (1 + growth_rate)
        pv += oe / (1 + discount_rate) ** t

    # 永续价值
    terminal = oe / discount_rate
    pv += terminal / (1 + discount_rate) ** years

    return pv


# 各市场股权风险溢价 (ERP)
# 来源: Damodaran 年度更新 + 本地估算
# 折现率 = 无风险利率 + ERP
MARKET_ERP: dict[str, float] = {
    # 成熟市场
    "US": 0.045,       # 美国: 国债利率本身够高，ERP ~4.5%
    "JP": 0.035,       # 日本: 成熟但低利率
    "UK": 0.04,
    "DE": 0.04,        # 德国
    "FR": 0.04,
    "AU": 0.04,        # 澳大利亚
    "CA": 0.04,        # 加拿大
    "HK": 0.05,        # 香港
    "SG": 0.04,        # 新加坡
    "KR": 0.045,       # 韩国
    "TW": 0.045,       # 台湾
    # 新兴市场
    "CN": 0.05,        # 中国
    "IN": 0.06,        # 印度
    "BR": 0.07,        # 巴西
    "MX": 0.06,        # 墨西哥
    "ID": 0.06,        # 印尼
    "VN": 0.07,        # 越南
    "SA": 0.05,        # 沙特
    "ZA": 0.06,        # 南非
}

# 默认 ERP（未知市场）
DEFAULT_ERP = 0.05

# 全球折现率下限
DISCOUNT_RATE_FLOOR = 0.04


def get_discount_rate(risk_free_rate: float, market: str = "US") -> float:
    """计算折现率 = 无风险利率 + 市场 ERP，有下限保护。"""
    erp = MARKET_ERP.get(market.upper(), DEFAULT_ERP)
    dr = risk_free_rate + erp
    return max(dr, DISCOUNT_RATE_FLOOR)


def compute_intrinsic_value(
    features: dict[str, float],
    guidance: dict[str, float | None],
    discount_rate: float,
    shares_outstanding: float,
    market: str = "US",
) -> DCFResult:
    """根据可用 guidance 选择计算路径。

    路径优先级：A > B > C > D > E > F > G
    - A-D: 有 guidance
    - E-G: 无 guidance fallback

    折现率按市场调整（无风险利率 + ERP），有下限保护。
    """
    # 按市场调整折现率
    discount_rate = get_discount_rate(discount_rate, market)

    oe = features.get("l0.company.owner_earnings")
    inc_roic = features.get("l0.company.incremental_roic")
    oe_to_ni = features.get("l0.company.owner_earnings_to_net_income")

    # 正常化 OE: 重投入期（capex >> 折旧）时 OE 被压低。
    # 如果 ROIC 好（投资有回报），超出折旧的 capex 是增长投资，不应全额扣减。
    # 当 OE/NI < 0.3（95%+利润被 capex 吃掉）且 ROIC > 10%，用净利润替代。
    if (oe is not None and oe_to_ni is not None and oe_to_ni < 0.3
            and inc_roic is not None and inc_roic > 0.10):
        normalized_ni = oe / oe_to_ni  # 还原净利润
        if normalized_ni > 0:
            oe = normalized_ni  # 重投入期: 用净利润近似

    if oe is None or oe <= 0:
        return DCFResult(status="unvaluable")

    oe_margin = features.get("l0.company.owner_earnings_margin")
    revenue = features.get("l0.company.revenue")
    payout = features.get("l0.company.dividend_payout_ratio", 0.0)

    # 路径 A: capex + ROIC
    roic = guidance.get("roic_target") or features.get("l0.company.incremental_roic")
    if guidance.get("capex") is not None and roic is not None:
        retention = 1 - payout
        growth = retention * roic
        iv = forward_dcf(oe, growth, discount_rate, payout)
        return DCFResult(
            intrinsic_value=iv / shares_outstanding if shares_outstanding > 0 else iv,
            valuation_path="A",
            key_assumptions={"growth": growth, "roic": roic, "discount_rate": discount_rate},
            status="valued",
        )

    # 路径 B: revenue_growth
    rev_growth = guidance.get("revenue_growth")
    if rev_growth is not None:
        iv = forward_dcf(oe, rev_growth, discount_rate, payout)
        return DCFResult(
            intrinsic_value=iv / shares_outstanding if shares_outstanding > 0 else iv,
            valuation_path="B",
            key_assumptions={"revenue_growth": rev_growth, "discount_rate": discount_rate},
            status="valued",
        )

    # 路径 C: EPS
    eps = guidance.get("eps")
    if eps is not None and shares_outstanding > 0:
        ni_future = eps * shares_outstanding
        oe_ni_ratio = features.get("l0.company.owner_earnings_to_net_income", 1.0)
        oe_future = ni_future * oe_ni_ratio
        # 单期估算
        iv = oe_future / discount_rate if discount_rate > 0 else 0
        return DCFResult(
            intrinsic_value=iv / shares_outstanding if shares_outstanding > 0 else iv,
            valuation_path="C",
            key_assumptions={"eps": eps, "discount_rate": discount_rate},
            status="valued",
        )

    # 路径 D: margin guidance + 历史增速
    margin = guidance.get("operating_margin") or guidance.get("net_margin")
    hist_growth = features.get("l0.company.revenue_growth_yoy")
    if margin is not None and hist_growth is not None and revenue is not None:
        future_rev = revenue * (1 + hist_growth)
        future_oe = future_rev * margin  # 近似
        growth = hist_growth  # 用历史增速
        iv = forward_dcf(future_oe, growth, discount_rate, payout)
        return DCFResult(
            intrinsic_value=iv / shares_outstanding if shares_outstanding > 0 else iv,
            valuation_path="D",
            key_assumptions={"margin": margin, "hist_growth": hist_growth, "discount_rate": discount_rate},
            status="valued",
        )

    # ── Fallback 路径（无 guidance）──

    # 路径 E: ROIC × 留存率
    if roic is not None and roic > 0:
        retention = 1 - payout
        growth = min(retention * roic, 0.15)  # 封顶 15%
        iv = forward_dcf(oe, growth, discount_rate, payout)
        return DCFResult(
            intrinsic_value=iv / shares_outstanding if shares_outstanding > 0 else iv,
            valuation_path="E",
            key_assumptions={"roic": roic, "retention": retention, "growth": growth,
                             "discount_rate": discount_rate},
            status="valued",
        )

    # 路径 F: 历史增速，封顶 10%
    if hist_growth is not None and hist_growth > 0:
        capped_growth = min(hist_growth, 0.10)
        iv = forward_dcf(oe, capped_growth, discount_rate, payout)
        return DCFResult(
            intrinsic_value=iv / shares_outstanding if shares_outstanding > 0 else iv,
            valuation_path="F",
            key_assumptions={"hist_growth": hist_growth, "capped_growth": capped_growth,
                             "discount_rate": discount_rate},
            status="valued",
        )

    # 路径 G: 零增长永续（最保守底线）
    iv = oe / discount_rate
    return DCFResult(
        intrinsic_value=iv / shares_outstanding if shares_outstanding > 0 else iv,
        valuation_path="G",
        key_assumptions={"growth": 0.0, "discount_rate": discount_rate},
        status="valued",
    )


def reverse_dcf(
    current_price: float,
    current_owner_earnings: float,
    discount_rate: float,
    shares_outstanding: float,
    years: int = PROJECTION_YEARS,
    market: str = "US",
) -> ReverseDCFResult:
    """反向 DCF：从股价反推市场隐含增速。

    求解 g 使得 DCF(OE, g, r) = current_price × shares_outstanding
    使用二分法求解。
    """
    discount_rate = get_discount_rate(discount_rate, market)

    if current_owner_earnings <= 0 or shares_outstanding <= 0 or discount_rate <= 0:
        return ReverseDCFResult(status="not_computed")

    market_cap = current_price * shares_outstanding

    # 二分法：增速范围 -50% ~ +100%
    lo, hi = -0.50, 1.00
    for _ in range(100):
        mid = (lo + hi) / 2
        dcf_val = forward_dcf(current_owner_earnings, mid, discount_rate, years=years)
        if dcf_val < market_cap:
            lo = mid
        else:
            hi = mid
        if abs(hi - lo) < 0.0001:
            break

    implied_growth = (lo + hi) / 2
    return ReverseDCFResult(
        implied_growth_rate=implied_growth,
        status="computed",
    )
