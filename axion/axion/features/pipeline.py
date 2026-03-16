"""
特征计算管线
============
L0 → L1 → L2 分层计算。
"""

from axion.features.registry import get_features
from axion.features.types import ComputeContext, FeatureLevel, FeatureResult

# 导入特征模块，触发 @feature 装饰器注册
import axion.features.l0.company  # noqa: F401
import axion.features.l0.cross_period  # noqa: F401
import axion.features.l1  # noqa: F401
import axion.features.l2  # noqa: F401


def compute_features(company_id: int, period: str) -> dict[str, FeatureResult]:
    """计算一家公司一个期的全部特征。

    返回 {feature_name: FeatureResult}。
    """
    ctx = ComputeContext(company_id=company_id, period=period)
    results: dict[str, FeatureResult] = {}

    for level in (FeatureLevel.L0, FeatureLevel.L1, FeatureLevel.L2):
        for feat in get_features(level=level):
            try:
                result = feat.compute_fn(ctx)
            except Exception as e:
                # 单个特征失败不影响其他特征
                print(f"  [WARN] Feature {feat.name} failed: {e}")
                continue
            if result is not None:
                ctx.features[feat.name] = result.value
                results[feat.name] = result

    return results
