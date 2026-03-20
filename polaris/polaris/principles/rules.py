"""
规则注册表
==========
用装饰器注册评分规则。新增规则只需写一个 @rule 装饰的函数。

规则按流派（School）分组，每条规则属于某个流派。
"""

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass

from polaris.principles.dimensions import School


# 全局注册表
_rules: dict[School, list["RuleDef"]] = defaultdict(list)


@dataclass
class RuleDef:
    name: str
    school: School
    evaluate_fn: Callable[[dict[str, float]], float]
    description: str = ""


def rule(name: str, school: School, description: str = ""):
    """装饰器：注册一条评分规则。

    用法：
        @rule("gross_margin_high", School.BUFFETT, "毛利率 > 40%")
        def gross_margin_high(features: dict[str, float]) -> float:
            gm = features.get("l0.company.gross_margin")
            return 2.0 if gm and gm > 0.40 else 0.0
    """

    def decorator(fn: Callable[[dict[str, float]], float]):
        _rules[school].append(
            RuleDef(name=name, school=school, evaluate_fn=fn, description=description)
        )
        return fn

    return decorator


def get_rules(school: School) -> list[RuleDef]:
    return _rules.get(school, [])


def get_all_rules() -> dict[School, list[RuleDef]]:
    return dict(_rules)
