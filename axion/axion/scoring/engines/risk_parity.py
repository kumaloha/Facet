"""
达利欧·风险平价引擎
====================
按波动率倒数分配权重，使每类资产对组合的风险贡献相等。

全部输入来自外部数据（价格时间序列），非 Anchor。
"""

from dataclasses import dataclass, field

import numpy as np

# 资产池定义
ASSET_POOL = {
    "VTI": "股票（美国全市场）",
    "TLT": "长期国债（20年+）",
    "IEF": "中期国债（7-10年）",
    "GLD": "黄金",
    "DBC": "大宗商品",
}

# 行业 → 资产桶映射
INDUSTRY_TO_BUCKET = {
    "Technology": "VTI",
    "Consumer": "VTI",
    "Healthcare": "VTI",
    "Financials": "VTI",
    "Energy": "DBC",
    "Mining": "DBC",
    "Agriculture": "DBC",
    "Utilities": "TLT",
    "Real Estate": "TLT",
    "Precious Metals": "GLD",
}


@dataclass
class RiskParityResult:
    weights: dict[str, float] = field(default_factory=dict)
    volatilities: dict[str, float] = field(default_factory=dict)
    portfolio_volatility: float | None = None
    status: str = "needs_external_data"


def compute_risk_parity_weights(
    volatilities: dict[str, float],
) -> RiskParityResult:
    """计算风险平价权重：weight_i ∝ 1/σ_i。

    volatilities: {asset_name: annualized_volatility}
    """
    if not volatilities:
        return RiskParityResult()

    # 过滤掉 0 或负波动率
    valid = {k: v for k, v in volatilities.items() if v > 0}
    if not valid:
        return RiskParityResult()

    raw_weights = {k: 1.0 / v for k, v in valid.items()}
    total = sum(raw_weights.values())
    weights = {k: w / total for k, w in raw_weights.items()}

    # 组合波动率（简化：不考虑相关性，仅加权平均）
    portfolio_vol = sum(weights[k] * valid[k] for k in weights)

    return RiskParityResult(
        weights=weights,
        volatilities=valid,
        portfolio_volatility=portfolio_vol,
        status="computed",
    )


def compute_annualized_volatility(daily_returns: list[float], window: int = 252) -> float:
    """从日收益率计算年化波动率。"""
    if len(daily_returns) < window:
        returns = daily_returns
    else:
        returns = daily_returns[-window:]

    if len(returns) < 20:
        return 0.0

    return float(np.std(returns) * np.sqrt(252))


def map_company_to_bucket(industry: str) -> str:
    """将公司行业映射到资产桶。"""
    return INDUSTRY_TO_BUCKET.get(industry, "VTI")
