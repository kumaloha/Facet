"""
特征注册表
==========
用装饰器注册特征函数。新增特征只需写一个 @feature 装饰的函数。
"""

from collections.abc import Callable
from dataclasses import dataclass

from polaris.features.types import ComputeContext, FeatureLevel, FeatureResult

# 全局注册表
_registry: dict[str, "FeatureDef"] = {}


@dataclass
class FeatureDef:
    name: str
    level: FeatureLevel
    domain: str
    compute_fn: Callable[[ComputeContext], FeatureResult | None]
    version: int = 1


def feature(
    name: str,
    level: FeatureLevel,
    domain: str,
    version: int = 1,
):
    """装饰器：注册一个特征。

    用法：
        @feature("l0.company.gross_margin", FeatureLevel.L0, "company")
        def gross_margin(ctx: ComputeContext) -> FeatureResult | None:
            ...
    """

    def decorator(fn: Callable[[ComputeContext], FeatureResult | None]):
        _registry[name] = FeatureDef(
            name=name, level=level, domain=domain, compute_fn=fn, version=version
        )
        return fn

    return decorator


def get_features(level: FeatureLevel | None = None) -> list[FeatureDef]:
    """获取已注册的特征列表，可按 level 过滤。"""
    if level is None:
        return list(_registry.values())
    return [f for f in _registry.values() if f.level == level]


def get_feature(name: str) -> FeatureDef | None:
    return _registry.get(name)
