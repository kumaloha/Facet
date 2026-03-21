"""
百分位 + 趋势评估基础设施
========================

纯函数模块，为 Polaris 认知层提供：
- 历史百分位计算（当前值在历史分布中的位置）
- 趋势档位检测（3m vs 12m 斜率比较）
- 信号档位综合判断（百分位 × 趋势 → 5 档）
- 多指标聚合 → Force 方向 + 置信度

设计原则:
  - 无副作用，不依赖 DB / 网络
  - 同时支持实时（DB 数据）和回测（dict 数据）
  - 所有计算只用 <= 当前时点的数据，严禁前瞻
  - anchor 层模块，不反向依赖 polaris（ForceDirection 本地定义）
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


# ━━ 枚举 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TrendTier(str, Enum):
    """趋势档位：3m 斜率 vs 12m 斜率。"""
    ACCELERATING_DETERIORATION = "accelerating_deterioration"
    DETERIORATING = "deteriorating"
    STABLE = "stable"
    IMPROVING = "improving"
    ACCELERATING_IMPROVEMENT = "accelerating_improvement"


class SignalTier(str, Enum):
    """综合信号档位：百分位 × 趋势。"""
    EXTREME_DETERIORATION = "extreme_deterioration"
    DETERIORATING = "deteriorating"
    NEUTRAL = "neutral"
    IMPROVING = "improving"
    EXTREME_IMPROVEMENT = "extreme_improvement"


class ForceDirection(str, Enum):
    """
    Force 方向（与 polaris.chains.dalio_forces.ForceDirection 值兼容）。
    在 anchor 层本地定义以避免循环依赖。
    """
    STRONGLY_POSITIVE = "strongly_positive"
    POSITIVE = "positive"
    NEUTRAL = "neutral"
    NEGATIVE = "negative"
    STRONGLY_NEGATIVE = "strongly_negative"


# ━━ 数据结构 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@dataclass
class IndicatorAssessment:
    """单个指标的完整评估结果。"""
    name: str
    value: Optional[float]
    percentile: Optional[float]       # 0-100
    trend: Optional[TrendTier]
    tier: SignalTier


# ━━ 核心计算函数 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def compute_percentile(current_value: float, history: list[float]) -> float:
    """
    计算 current_value 在 history 中的百分位（0-100）。

    使用 "weak" 方式：百分位 = (严格小于 current_value 的数量) / len(history) * 100。
    与 scipy.stats.percentileofscore(kind='weak') 一致但不依赖 scipy。

    边界:
      - history 为空 → 返回 50.0（无信息，取中性）
      - history 只有 1 个值 → 要么 0 要么 100
    """
    if not history:
        return 50.0

    n = len(history)
    count_le = sum(1 for v in history if v <= current_value)
    return (count_le / n) * 100.0


def _linear_slope(values: list[float]) -> float:
    """
    用最小二乘法计算 values 的线性斜率。
    x 轴为 0, 1, 2, ... (等间距时间步)。
    返回斜率（每步变化量）。

    要求 len(values) >= 2，否则返回 0.0。
    """
    n = len(values)
    if n < 2:
        return 0.0

    # x = 0..n-1
    sum_x = n * (n - 1) / 2.0
    sum_y = sum(values)
    sum_xy = sum(i * v for i, v in enumerate(values))
    sum_x2 = n * (n - 1) * (2 * n - 1) / 6.0

    denom = n * sum_x2 - sum_x * sum_x
    if denom == 0:
        return 0.0

    return (n * sum_xy - sum_x * sum_y) / denom


def compute_trend(
    values_3m: list[float],
    values_12m: list[float],
) -> TrendTier:
    """
    从最近 3 个月和 12 个月的值序列计算趋势档位。

    逻辑:
      1. 分别算 3m 和 12m 的线性斜率
      2. 两者同号 + 3m 绝对斜率 > 12m 绝对斜率 → 加速
      3. 两者同号 + 3m 绝对斜率 <= 12m 绝对斜率 → 非加速
      4. 斜率接近零 (|slope| < threshold) → STABLE

    threshold 用 12m 序列的标准差 * 0.05 做自适应阈值，
    如果标准差为 0 则视为 STABLE。

    边界:
      - values_3m 或 values_12m 不足 2 个值 → STABLE
    """
    if len(values_3m) < 2 or len(values_12m) < 2:
        return TrendTier.STABLE

    slope_3m = _linear_slope(values_3m)
    slope_12m = _linear_slope(values_12m)

    # 自适应阈值：基于 12m 数据的标准差
    mean_12m = sum(values_12m) / len(values_12m)
    var_12m = sum((v - mean_12m) ** 2 for v in values_12m) / len(values_12m)
    std_12m = var_12m ** 0.5

    if std_12m == 0:
        return TrendTier.STABLE

    threshold = std_12m * 0.05

    # 两个斜率都接近零 → 稳定
    if abs(slope_3m) < threshold and abs(slope_12m) < threshold:
        return TrendTier.STABLE

    # 判断方向：正斜率 = 值在上升
    # 注意：这里只判"趋势方向"，不判好坏（好坏由 higher_is_worse 在 signal_tier 里处理）
    # 上升 = DETERIORATING（值在变大），下降 = IMPROVING（值在变小）
    # 这个命名假设 higher_is_worse=True；当 higher_is_worse=False 时，
    # compute_signal_tier 会翻转解读。

    if slope_3m > threshold:
        # 值在上升
        if slope_12m > 0 and abs(slope_3m) > abs(slope_12m):
            return TrendTier.ACCELERATING_DETERIORATION
        return TrendTier.DETERIORATING
    elif slope_3m < -threshold:
        # 值在下降
        if slope_12m < 0 and abs(slope_3m) > abs(slope_12m):
            return TrendTier.ACCELERATING_IMPROVEMENT
        return TrendTier.IMPROVING
    else:
        # 3m 斜率接近零，看 12m
        if slope_12m > threshold:
            return TrendTier.DETERIORATING
        elif slope_12m < -threshold:
            return TrendTier.IMPROVING
        return TrendTier.STABLE


def compute_signal_tier(
    percentile: float,
    trend: TrendTier,
    higher_is_worse: bool = True,
) -> SignalTier:
    """
    结合百分位和趋势给出综合信号档位。

    higher_is_worse=True（默认）: 高百分位 = 恶化（如失业率、通胀）
    higher_is_worse=False: 低百分位 = 恶化（如 GDP 增速、消费者信心）

    当 higher_is_worse=False 时，翻转百分位和趋势的解读。
    """
    # 统一到 "高=差" 视角
    if not higher_is_worse:
        percentile = 100.0 - percentile
        # 翻转趋势
        trend_flip = {
            TrendTier.ACCELERATING_DETERIORATION: TrendTier.ACCELERATING_IMPROVEMENT,
            TrendTier.DETERIORATING: TrendTier.IMPROVING,
            TrendTier.STABLE: TrendTier.STABLE,
            TrendTier.IMPROVING: TrendTier.DETERIORATING,
            TrendTier.ACCELERATING_IMPROVEMENT: TrendTier.ACCELERATING_DETERIORATION,
        }
        trend = trend_flip[trend]

    # 极端恶化：百分位 > 80 + 加速恶化
    if percentile > 80 and trend == TrendTier.ACCELERATING_DETERIORATION:
        return SignalTier.EXTREME_DETERIORATION

    # 恶化：百分位 > 60 + 在恶化（含加速）
    if percentile > 60 and trend in (
        TrendTier.DETERIORATING,
        TrendTier.ACCELERATING_DETERIORATION,
    ):
        return SignalTier.DETERIORATING

    # 极端改善：百分位 < 20 + 加速改善
    if percentile < 20 and trend == TrendTier.ACCELERATING_IMPROVEMENT:
        return SignalTier.EXTREME_IMPROVEMENT

    # 改善：百分位 < 40 + 在改善（含加速）
    if percentile < 40 and trend in (
        TrendTier.IMPROVING,
        TrendTier.ACCELERATING_IMPROVEMENT,
    ):
        return SignalTier.IMPROVING

    # 其余 → 中性（包括百分位 40-60 或趋势稳定）
    return SignalTier.NEUTRAL


# ━━ 一站式评估 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def assess_indicator(
    name: str,
    current: Optional[float],
    history: list[float],
    higher_is_worse: bool = True,
) -> IndicatorAssessment:
    """
    一站式指标评估：从原始值 → IndicatorAssessment。

    history 应按时间顺序排列（最早在前）。
    current 不应包含在 history 中。

    如果 current 为 None，返回 NEUTRAL tier（数据缺失时保守处理）。
    """
    if current is None:
        return IndicatorAssessment(
            name=name,
            value=None,
            percentile=None,
            trend=None,
            tier=SignalTier.NEUTRAL,
        )

    pct = compute_percentile(current, history)

    # 趋势需要从 history 中切出最近 3m 和 12m
    # history 按时间顺序排列，最后一个是最近的
    values_3m = history[-3:] + [current] if len(history) >= 1 else [current]
    values_12m = history[-12:] + [current] if len(history) >= 1 else [current]

    trend = compute_trend(values_3m, values_12m)
    tier = compute_signal_tier(pct, trend, higher_is_worse)

    return IndicatorAssessment(
        name=name,
        value=current,
        percentile=round(pct, 1),
        trend=trend,
        tier=tier,
    )


def assess_from_fred_history(
    indicator_key: str,
    month: str,
    fred_history: dict[str, float],
    higher_is_worse: bool = True,
) -> IndicatorAssessment:
    """
    专为回测设计：从 fred_history dict 中取该指标到 month 为止的所有历史值。

    Args:
        indicator_key: 指标名称（如 "UNRATE"）
        month: 当前回测月份，格式 "YYYY-MM"
        fred_history: {"YYYY-MM": float_value} 该指标的全部历史
        higher_is_worse: True 表示值越高越差

    Returns:
        IndicatorAssessment

    无前瞻偏差保证: 只使用 key <= month 的数据。
    """
    # 按月份排序，只取 <= month 的数据
    sorted_months = sorted(k for k in fred_history if k <= month)

    if not sorted_months:
        return IndicatorAssessment(
            name=indicator_key,
            value=None,
            percentile=None,
            trend=None,
            tier=SignalTier.NEUTRAL,
        )

    current_month = sorted_months[-1]
    current_value = fred_history[current_month]

    # history = 除了当前月之外的所有历史值（按时间顺序）
    history_values = [fred_history[m] for m in sorted_months[:-1]]

    return assess_indicator(
        name=indicator_key,
        current=current_value,
        history=history_values,
        higher_is_worse=higher_is_worse,
    )


# ━━ 多指标聚合 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


# SignalTier → 数值映射（用于加权聚合）
_TIER_SCORE = {
    SignalTier.EXTREME_DETERIORATION: -2,
    SignalTier.DETERIORATING: -1,
    SignalTier.NEUTRAL: 0,
    SignalTier.IMPROVING: +1,
    SignalTier.EXTREME_IMPROVEMENT: +2,
}

# 数值 → ForceDirection 映射
_SCORE_THRESHOLDS = [
    (-1.5, ForceDirection.STRONGLY_NEGATIVE),
    (-0.5, ForceDirection.NEGATIVE),
    (0.5, ForceDirection.NEUTRAL),
    (1.5, ForceDirection.POSITIVE),
]


def aggregate_force_direction(
    assessments: list[IndicatorAssessment],
) -> tuple[ForceDirection, float]:
    """
    从多个指标评估聚合出 Force 方向和置信度。

    方法:
      1. 每个 tier 映射为分数: EXTREME_DETERIORATION=-2 .. EXTREME_IMPROVEMENT=+2
      2. 极端值额外加权: EXTREME_* 权重 1.5，其他 1.0
      3. 加权平均 → 连续分数
      4. 分数映射到 ForceDirection（5 档阈值）
      5. 置信度 = |加权平均| / 2.0，clamp 到 [0, 1]
         越接近极端越确定；分数散布在中间 → 低置信度

    边界:
      - assessments 为空 → (NEUTRAL, 0.0)
      - 全部 NEUTRAL → (NEUTRAL, 0.0)

    Returns:
        (ForceDirection, confidence: 0.0-1.0)
    """
    if not assessments:
        return ForceDirection.NEUTRAL, 0.0

    total_weight = 0.0
    weighted_sum = 0.0

    for a in assessments:
        score = _TIER_SCORE[a.tier]
        weight = 1.5 if abs(score) == 2 else 1.0
        weighted_sum += score * weight
        total_weight += weight

    if total_weight == 0:
        return ForceDirection.NEUTRAL, 0.0

    avg_score = weighted_sum / total_weight

    # 映射到 ForceDirection
    direction = ForceDirection.STRONGLY_POSITIVE  # 默认最高档
    for threshold, d in _SCORE_THRESHOLDS:
        if avg_score < threshold:
            direction = d
            break

    # 置信度
    confidence = min(abs(avg_score) / 2.0, 1.0)

    return direction, round(confidence, 3)


# ━━ 自测 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


if __name__ == "__main__":
    print("=" * 60)
    print("percentile_trend.py 自测")
    print("=" * 60)

    errors = 0

    def check(label: str, got, expected):
        global errors
        ok = got == expected
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {label}: got={got}, expected={expected}")
        if not ok:
            errors += 1

    # ── 百分位计算 ──

    print("\n--- compute_percentile ---")

    # 空 history → 50
    check("empty history", compute_percentile(5.0, []), 50.0)

    # 单值，current 等于 history
    check("single value, equal", compute_percentile(5.0, [5.0]), 100.0)

    # 已知分布: [1,2,3,4,5], current=3 → 3个<=3 → 60%
    check("known distribution", compute_percentile(3.0, [1, 2, 3, 4, 5]), 60.0)

    # current 小于所有 history
    check("below all", compute_percentile(0.0, [1, 2, 3, 4, 5]), 0.0)

    # current 大于所有 history
    check("above all", compute_percentile(10.0, [1, 2, 3, 4, 5]), 100.0)

    # ── 线性斜率 ──

    print("\n--- _linear_slope ---")

    check("constant", _linear_slope([5, 5, 5, 5]), 0.0)
    check("perfect rise", _linear_slope([0, 1, 2, 3]), 1.0)
    check("perfect fall", _linear_slope([3, 2, 1, 0]), -1.0)
    check("single value", _linear_slope([42]), 0.0)
    check("empty", _linear_slope([]), 0.0)

    # ── 趋势检测 ──

    print("\n--- compute_trend ---")

    # 稳定序列
    check("stable", compute_trend([5, 5, 5], [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5]),
          TrendTier.STABLE)

    # 12m 上升但 3m 上升更快 → 加速恶化
    v12_up = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    v3_accel_up = [12, 16, 20]
    t = compute_trend(v3_accel_up, v12_up)
    check("accelerating deterioration", t, TrendTier.ACCELERATING_DETERIORATION)

    # 12m 下降但 3m 下降更快 → 加速改善
    v12_down = [12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
    v3_accel_down = [3, 1, -3]
    t = compute_trend(v3_accel_down, v12_down)
    check("accelerating improvement", t, TrendTier.ACCELERATING_IMPROVEMENT)

    # 上升但不加速
    v3_slow_up = [10, 10.5, 11]
    t = compute_trend(v3_slow_up, v12_up)
    # 3m 斜率 0.5, 12m 斜率 1.0 → 同向但 3m 不比 12m 陡
    check("deteriorating (not accelerating)", t, TrendTier.DETERIORATING)

    # 不足数据 → STABLE
    check("insufficient data", compute_trend([5], [5]), TrendTier.STABLE)

    # ── 信号档位 ──

    print("\n--- compute_signal_tier ---")

    check("extreme deterioration",
          compute_signal_tier(85, TrendTier.ACCELERATING_DETERIORATION),
          SignalTier.EXTREME_DETERIORATION)

    check("deteriorating",
          compute_signal_tier(70, TrendTier.DETERIORATING),
          SignalTier.DETERIORATING)

    check("neutral (mid percentile)",
          compute_signal_tier(50, TrendTier.STABLE),
          SignalTier.NEUTRAL)

    check("improving",
          compute_signal_tier(30, TrendTier.IMPROVING),
          SignalTier.IMPROVING)

    check("extreme improvement",
          compute_signal_tier(15, TrendTier.ACCELERATING_IMPROVEMENT),
          SignalTier.EXTREME_IMPROVEMENT)

    # higher_is_worse=False: GDP增速，低百分位=差
    check("flipped: low pct + deteriorating → extreme_deterioration",
          compute_signal_tier(15, TrendTier.ACCELERATING_IMPROVEMENT, higher_is_worse=False),
          SignalTier.EXTREME_DETERIORATION)

    check("flipped: high pct + improving → extreme_improvement",
          compute_signal_tier(85, TrendTier.ACCELERATING_DETERIORATION, higher_is_worse=False),
          SignalTier.EXTREME_IMPROVEMENT)

    # ── assess_from_fred_history: 无前瞻偏差 ──

    print("\n--- assess_from_fred_history (no lookahead) ---")

    fred = {
        "2020-01": 3.5,
        "2020-02": 3.6,
        "2020-03": 4.4,
        "2020-04": 14.7,  # COVID spike
        "2020-05": 13.3,
        "2020-06": 11.1,
    }

    # 在 2020-03，不应看到 2020-04 的数据
    result = assess_from_fred_history("UNRATE", "2020-03", fred)
    assert result.value == 4.4, f"Expected 4.4, got {result.value}"
    # 百分位应基于 [3.5, 3.6] 的 history，4.4 > 两者 → 100%
    check("no lookahead: value", result.value, 4.4)
    check("no lookahead: percentile", result.percentile, 100.0)
    print(f"  [INFO] tier={result.tier} (data-dependent, not checked)")

    # 在 2020-01，无历史 → 百分位 50（中性）
    result_first = assess_from_fred_history("UNRATE", "2020-01", fred)
    check("first month: no history", result_first.percentile, 50.0)

    # 未来月份不存在 → None
    result_future = assess_from_fred_history("UNRATE", "2019-12", fred)
    check("before all data: value is None", result_future.value, None)

    # ── aggregate_force_direction ──

    print("\n--- aggregate_force_direction ---")

    # 全部恶化
    all_bad = [
        IndicatorAssessment("a", 1, 90, TrendTier.ACCELERATING_DETERIORATION,
                            SignalTier.EXTREME_DETERIORATION),
        IndicatorAssessment("b", 2, 75, TrendTier.DETERIORATING,
                            SignalTier.DETERIORATING),
    ]
    direction, conf = aggregate_force_direction(all_bad)
    check("all bad → strongly_negative", direction, ForceDirection.STRONGLY_NEGATIVE)
    assert conf > 0.5, f"Expected confidence > 0.5, got {conf}"
    print(f"  [INFO] confidence={conf}")

    # 全部改善
    all_good = [
        IndicatorAssessment("c", 3, 10, TrendTier.ACCELERATING_IMPROVEMENT,
                            SignalTier.EXTREME_IMPROVEMENT),
        IndicatorAssessment("d", 4, 25, TrendTier.IMPROVING,
                            SignalTier.IMPROVING),
    ]
    direction, conf = aggregate_force_direction(all_good)
    check("all good → strongly_positive", direction, ForceDirection.STRONGLY_POSITIVE)

    # 混合 → 接近中性
    mixed = [
        IndicatorAssessment("e", 5, 90, TrendTier.DETERIORATING,
                            SignalTier.DETERIORATING),
        IndicatorAssessment("f", 6, 10, TrendTier.IMPROVING,
                            SignalTier.IMPROVING),
    ]
    direction, conf = aggregate_force_direction(mixed)
    check("mixed → neutral", direction, ForceDirection.NEUTRAL)

    # 空列表
    direction, conf = aggregate_force_direction([])
    check("empty → neutral", direction, ForceDirection.NEUTRAL)
    check("empty → zero confidence", conf, 0.0)

    # ── 汇总 ──

    print("\n" + "=" * 60)
    if errors == 0:
        print(f"ALL TESTS PASSED")
    else:
        print(f"{errors} TEST(S) FAILED")
    print("=" * 60)
