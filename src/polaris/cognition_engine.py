"""
认知引擎
========

基础层(pure) + 认知补丁(derivative) = 完整认知

架构:
  1. Pure 底座: forces_pure.assess_forces_pure() — 百分位+趋势+金融原理
  2. 认知补丁: 专家规则叠加在底座之上 — 条件(百分位+趋势) → 动作(方向调整)
  3. 每套补丁是一个"认知派生"，可从DB或YAML加载

用法:
  # 纯净底座
  engine = CognitionEngine()
  result = engine.evaluate(fred_history, month)

  # 叠加补丁
  engine = CognitionEngine(derivative_name="legacy_v1")
  result = engine.evaluate(fred_history, month)

  # 回测场景: 直接传入补丁（不读DB）
  engine = CognitionEngine(patches=[...])
  result = engine.evaluate(fred_history, month)
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

from anchor.compute.percentile_trend import (
    ForceDirection,
    IndicatorAssessment,
    SignalTier,
    TrendTier,
    aggregate_force_direction,
    assess_from_fred_history,
)
from polaris.chains.forces_pure import (
    _build_derived_series,
    assess_forces_pure,
)


# ── 数据结构 ──────────────────────────────────────────────────────────


@dataclass
class PatchCondition:
    """补丁条件: 百分位 + 趋势的组合判断。"""
    indicator: str                          # derived_series key
    percentile_op: Optional[str] = None     # ">" or "<"
    percentile_val: Optional[float] = None  # 0-100
    trend: Optional[str] = None             # TrendTier.value string
    higher_is_worse: bool = True            # 该指标的方向性


@dataclass
class PatchAction:
    """补丁动作: 对某个 force 方向的调整。"""
    force_id: int                           # 1-5
    direction_adjustment: float = 0.0       # 加到聚合分数上 (-2 ~ +2 范围)
    contradiction: str = ""                 # 矛盾描述


@dataclass
class Patch:
    """一条认知补丁 = 条件(AND) + 动作。"""
    name: str
    description: str = ""
    conditions: list[PatchCondition] = field(default_factory=list)
    action: PatchAction = field(default_factory=lambda: PatchAction(force_id=1))
    enabled: bool = True


@dataclass
class PatchTriggerRecord:
    """补丁触发记录 — 用于回测分析。"""
    patch_name: str
    force_id: int
    adjustment: float
    contradiction: str
    matched_conditions: list[str]   # 每个满足条件的描述


@dataclass
class CognitionResult:
    """认知引擎输出。"""
    # force_key -> (direction, confidence, assessments)
    forces: dict[str, tuple[ForceDirection, float, list[IndicatorAssessment]]]
    # 补丁触发记录
    patch_triggers: list[PatchTriggerRecord] = field(default_factory=list)
    # 补丁名称
    derivative_name: str = "pure"


# ── 指标方向映射 — 哪些指标 higher_is_worse ──────────────────────────

# 与 forces_pure.py 的指标定义保持一致
_HIGHER_IS_WORSE_MAP = {
    "fed_funds_rate": True,
    "credit_growth": False,
    "total_debt_gdp": True,
    "unemployment": True,
    "cpi_yoy": True,
    "gdp_growth": False,
    "mortgage_delinquency": True,
    "financial_leverage": True,
    "lending_standards": True,
    "household_debt_gdp": True,
    "mortgage_debt_service": True,
    "credit_spread_hy": True,
    "consumer_sentiment": False,
    "gini": True,
    "fiscal_deficit_gdp": True,
    "trade_balance_abs": True,
    "dollar_yoy": True,       # 大波动=差
    "oil_yoy_abs": True,
    "epu_index": True,
    "food_cpi_yoy": True,
    "productivity_growth": False,
    "rd_spending_growth": False,
}


# ── 聚合分数映射 (与 percentile_trend.py 一致) ───────────────────────

_TIER_SCORE = {
    SignalTier.EXTREME_DETERIORATION: -2,
    SignalTier.DETERIORATING: -1,
    SignalTier.NEUTRAL: 0,
    SignalTier.IMPROVING: +1,
    SignalTier.EXTREME_IMPROVEMENT: +2,
}

_SCORE_THRESHOLDS = [
    (-1.5, ForceDirection.STRONGLY_NEGATIVE),
    (-0.5, ForceDirection.NEGATIVE),
    (0.5, ForceDirection.NEUTRAL),
    (1.5, ForceDirection.POSITIVE),
]


# ── 认知引擎 ─────────────────────────────────────────────────────────


class CognitionEngine:
    """认知引擎: pure 底座 + 可选补丁。

    Args:
        derivative_name: 从 YAML 文件加载补丁 (在 data/derivatives/ 目录)
        patches: 直接传入补丁列表 (回测用，优先于 derivative_name)
    """

    def __init__(
        self,
        derivative_name: Optional[str] = None,
        patches: Optional[list[Patch]] = None,
    ):
        self._derivative_name = derivative_name or "pure"
        if patches is not None:
            self._patches = [p for p in patches if p.enabled]
        elif derivative_name:
            self._patches = self._load_patches_from_yaml(derivative_name)
        else:
            self._patches = []

    @property
    def derivative_name(self) -> str:
        return self._derivative_name

    @property
    def patch_count(self) -> int:
        return len(self._patches)

    def evaluate(
        self,
        fred_history: dict,
        month: str,
    ) -> CognitionResult:
        """执行完整认知评估。

        Args:
            fred_history: 原始 FRED 月度数据 {"indicator": {"YYYY-MM": value}}
            month: 当前回测月份 "YYYY-MM"

        Returns:
            CognitionResult
        """
        # Step 1: 跑 pure 底座
        base_results = assess_forces_pure(fred_history, month)

        # 如果没有补丁，直接返回
        if not self._patches:
            return CognitionResult(
                forces=base_results,
                derivative_name=self._derivative_name,
            )

        # Step 2: 构建衍生序列 (补丁条件检查需要)
        derived = _build_derived_series(fred_history)

        # Step 3: 应用补丁
        adjusted, triggers = self._apply_patches(base_results, derived, month)

        return CognitionResult(
            forces=adjusted,
            patch_triggers=triggers,
            derivative_name=self._derivative_name,
        )

    # ── 补丁加载 ─────────────────────────────────────────────────

    @staticmethod
    def _load_patches_from_yaml(derivative_name: str) -> list[Patch]:
        """从 data/derivatives/{name}.yaml 加载补丁。"""
        # 寻找项目根目录下的 data/derivatives/
        search_paths = [
            Path(__file__).resolve().parent.parent.parent / "data" / "derivatives",
            Path.cwd() / "data" / "derivatives",
        ]

        yaml_path = None
        for base in search_paths:
            candidate = base / f"{derivative_name}.yaml"
            if candidate.exists():
                yaml_path = candidate
                break

        if yaml_path is None:
            raise FileNotFoundError(
                f"认知派生 '{derivative_name}' 未找到。"
                f"搜索路径: {[str(p) for p in search_paths]}"
            )

        with open(yaml_path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)

        patches = []
        for p_data in data.get("patches", []):
            conditions = []
            for c in p_data.get("conditions", []):
                conditions.append(PatchCondition(
                    indicator=c["indicator"],
                    percentile_op=c.get("percentile_op"),
                    percentile_val=c.get("percentile_val"),
                    trend=c.get("trend"),
                    higher_is_worse=c.get("higher_is_worse",
                                          _HIGHER_IS_WORSE_MAP.get(c["indicator"], True)),
                ))

            action_data = p_data.get("action", {})
            action = PatchAction(
                force_id=action_data.get("force_id", 1),
                direction_adjustment=action_data.get("direction_adjustment", 0.0),
                contradiction=action_data.get("contradiction", ""),
            )

            patches.append(Patch(
                name=p_data["name"],
                description=p_data.get("description", ""),
                conditions=conditions,
                action=action,
                enabled=p_data.get("enabled", True),
            ))

        return [p for p in patches if p.enabled]

    @staticmethod
    def _load_patches_from_db(derivative_name: str) -> list[Patch]:
        """从 DB 加载补丁 (需要 sqlmodel session)。

        当前仅预留接口，实际使用 YAML 加载。
        DB 加载在接入实时管道时启用。
        """
        raise NotImplementedError(
            "DB 补丁加载暂未实现。请使用 YAML 模式: "
            "CognitionEngine(derivative_name='xxx')"
        )

    # ── 补丁应用 ─────────────────────────────────────────────────

    def _apply_patches(
        self,
        base_results: dict,
        derived: dict[str, dict[str, float]],
        month: str,
    ) -> tuple[dict, list[PatchTriggerRecord]]:
        """检查每个补丁的条件，如果满足则应用动作。

        Args:
            base_results: assess_forces_pure 的原始输出
            derived: _build_derived_series 的输出
            month: 当前月份

        Returns:
            (adjusted_results, trigger_records)
        """
        # 先计算每个 force 的原始聚合分数
        force_scores: dict[str, float] = {}
        for force_key, (direction, confidence, assessments) in base_results.items():
            if not assessments:
                force_scores[force_key] = 0.0
                continue
            total_weight = 0.0
            weighted_sum = 0.0
            for a in assessments:
                score = _TIER_SCORE[a.tier]
                weight = 1.5 if abs(score) == 2 else 1.0
                weighted_sum += score * weight
                total_weight += weight
            force_scores[force_key] = weighted_sum / total_weight if total_weight > 0 else 0.0

        triggers: list[PatchTriggerRecord] = []

        for patch in self._patches:
            # 检查所有条件 (AND 逻辑)
            all_met = True
            matched_descs: list[str] = []

            for cond in patch.conditions:
                met, desc = self._check_condition(cond, derived, month)
                if not met:
                    all_met = False
                    break
                matched_descs.append(desc)

            if not all_met:
                continue

            # 条件全部满足 → 应用动作
            force_key = f"force{patch.action.force_id}"
            if force_key not in force_scores:
                continue

            force_scores[force_key] += patch.action.direction_adjustment
            # clamp 到 [-2, 2]
            force_scores[force_key] = max(-2.0, min(2.0, force_scores[force_key]))

            triggers.append(PatchTriggerRecord(
                patch_name=patch.name,
                force_id=patch.action.force_id,
                adjustment=patch.action.direction_adjustment,
                contradiction=patch.action.contradiction,
                matched_conditions=matched_descs,
            ))

        # 从调整后的分数重建 force results
        adjusted = {}
        for force_key, (direction, confidence, assessments) in base_results.items():
            new_score = force_scores.get(force_key, 0.0)

            # 分数 → ForceDirection
            new_direction = ForceDirection.STRONGLY_POSITIVE
            for threshold, d in _SCORE_THRESHOLDS:
                if new_score < threshold:
                    new_direction = d
                    break

            # 置信度
            new_confidence = min(abs(new_score) / 2.0, 1.0)

            adjusted[force_key] = (new_direction, round(new_confidence, 3), assessments)

        return adjusted, triggers

    def _check_condition(
        self,
        condition: PatchCondition,
        derived: dict[str, dict[str, float]],
        month: str,
    ) -> tuple[bool, str]:
        """检查单个补丁条件。

        Returns:
            (is_met, description)
        """
        series = derived.get(condition.indicator)
        if series is None:
            return False, f"{condition.indicator}: 数据缺失"

        assessment = assess_from_fred_history(
            condition.indicator,
            month,
            series,
            higher_is_worse=condition.higher_is_worse,
        )

        if assessment.value is None:
            return False, f"{condition.indicator}: 当月无数据"

        desc_parts = [f"{condition.indicator}"]

        # 检查百分位条件
        if condition.percentile_op and condition.percentile_val is not None:
            pct = assessment.percentile
            if pct is None:
                return False, f"{condition.indicator}: 无百分位"

            if condition.percentile_op == ">" and pct <= condition.percentile_val:
                return False, f"{condition.indicator}: 百分位{pct}未>{condition.percentile_val}"
            if condition.percentile_op == "<" and pct >= condition.percentile_val:
                return False, f"{condition.indicator}: 百分位{pct}未<{condition.percentile_val}"

            desc_parts.append(f"P{pct:.0f}{condition.percentile_op}{condition.percentile_val}")

        # 检查趋势条件
        if condition.trend:
            if assessment.trend is None:
                return False, f"{condition.indicator}: 无趋势"
            if assessment.trend.value != condition.trend:
                return False, f"{condition.indicator}: 趋势{assessment.trend.value}!={condition.trend}"
            desc_parts.append(f"趋势={condition.trend}")

        return True, " ".join(desc_parts)


# ── 便捷函数 ─────────────────────────────────────────────────────────


def evaluate_pure(fred_history: dict, month: str) -> CognitionResult:
    """纯净底座评估 (快捷方式)。"""
    return CognitionEngine().evaluate(fred_history, month)


def evaluate_with_derivative(
    derivative_name: str,
    fred_history: dict,
    month: str,
) -> CognitionResult:
    """带认知派生评估 (快捷方式)。"""
    return CognitionEngine(derivative_name=derivative_name).evaluate(fred_history, month)


# ── 自测 ─────────────────────────────────────────────────────────────


if __name__ == "__main__":
    print("=" * 60)
    print("cognition_engine.py 自测")
    print("=" * 60)

    errors = 0

    def check(label, got, expected):
        global errors
        ok = got == expected
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {label}: got={got}, expected={expected}")
        if not ok:
            errors += 1

    # ── Test 1: PatchCondition / Patch 数据结构 ──
    print("\n--- 数据结构 ---")
    cond = PatchCondition(indicator="lending_standards", percentile_op=">", percentile_val=60)
    check("condition.indicator", cond.indicator, "lending_standards")
    check("condition.higher_is_worse default", cond.higher_is_worse, True)

    action = PatchAction(force_id=1, direction_adjustment=-0.3, contradiction="test")
    patch = Patch(name="test_patch", conditions=[cond], action=action)
    check("patch.name", patch.name, "test_patch")
    check("patch.enabled", patch.enabled, True)

    # ── Test 2: Engine without patches = pure mode ──
    print("\n--- Engine(pure mode) ---")
    engine = CognitionEngine()
    check("engine.derivative_name", engine.derivative_name, "pure")
    check("engine.patch_count", engine.patch_count, 0)

    # 用最小 fred_history 测试
    minimal_fred = {
        "fed_funds_rate": {
            "2020-01": 1.5, "2020-02": 1.5, "2020-03": 0.25,
            "2020-04": 0.25, "2020-05": 0.25, "2020-06": 0.25,
        },
        "unemployment": {
            "2020-01": 3.5, "2020-02": 3.6, "2020-03": 4.4,
            "2020-04": 14.7, "2020-05": 13.3, "2020-06": 11.1,
        },
    }
    result = engine.evaluate(minimal_fred, "2020-06")
    check("result type", type(result).__name__, "CognitionResult")
    check("result has force1", "force1" in result.forces, True)
    check("result no triggers", len(result.patch_triggers), 0)
    print(f"  [INFO] force1 direction={result.forces['force1'][0].value}")

    # ── Test 3: Engine with inline patches ──
    print("\n--- Engine(inline patches) ---")
    test_patch = Patch(
        name="test_high_unemployment",
        conditions=[
            PatchCondition(
                indicator="unemployment",
                percentile_op=">",
                percentile_val=50,
                higher_is_worse=True,
            ),
        ],
        action=PatchAction(
            force_id=1,
            direction_adjustment=-0.5,
            contradiction="失业率高但其他指标可能还行",
        ),
    )
    engine_patched = CognitionEngine(patches=[test_patch])
    check("patched engine patch_count", engine_patched.patch_count, 1)

    result_patched = engine_patched.evaluate(minimal_fred, "2020-06")
    # 2020-06 unemployment=11.1, 历史=[3.5,3.6,4.4,14.7,13.3], 百分位应该很高
    print(f"  [INFO] patched force1 direction={result_patched.forces['force1'][0].value}")
    print(f"  [INFO] triggers={len(result_patched.patch_triggers)}")
    if result_patched.patch_triggers:
        t = result_patched.patch_triggers[0]
        print(f"  [INFO] trigger: {t.patch_name}, adj={t.adjustment}, conditions={t.matched_conditions}")
        check("trigger fired", t.patch_name, "test_high_unemployment")

    # ── Test 4: Disabled patch should not fire ──
    print("\n--- Disabled patch ---")
    disabled_patch = Patch(
        name="disabled_test",
        conditions=[PatchCondition(indicator="unemployment", percentile_op=">", percentile_val=0)],
        action=PatchAction(force_id=1, direction_adjustment=-1.0),
        enabled=False,
    )
    engine_disabled = CognitionEngine(patches=[disabled_patch])
    check("disabled patch excluded", engine_disabled.patch_count, 0)

    # ── Test 5: YAML loading ──
    print("\n--- YAML loading ---")
    try:
        engine_yaml = CognitionEngine(derivative_name="legacy_v1")
        print(f"  [INFO] legacy_v1 loaded: {engine_yaml.patch_count} patches")
        check("legacy_v1 has patches", engine_yaml.patch_count > 0, True)
    except FileNotFoundError as e:
        print(f"  [SKIP] legacy_v1.yaml not found (create it first): {e}")
    except Exception as e:
        print(f"  [FAIL] Unexpected error loading YAML: {e}")
        errors += 1

    # ── Test 6: Condition checking edge cases ──
    print("\n--- Condition edge cases ---")
    engine_edge = CognitionEngine(patches=[
        Patch(
            name="missing_indicator",
            conditions=[PatchCondition(indicator="nonexistent_indicator", percentile_op=">", percentile_val=50)],
            action=PatchAction(force_id=1, direction_adjustment=-0.1),
        ),
    ])
    result_edge = engine_edge.evaluate(minimal_fred, "2020-06")
    check("missing indicator: no trigger", len(result_edge.patch_triggers), 0)

    # ── Test 7: Score clamping ──
    print("\n--- Score clamping ---")
    extreme_patches = [
        Patch(
            name=f"extreme_{i}",
            conditions=[PatchCondition(indicator="unemployment", percentile_op=">", percentile_val=0)],
            action=PatchAction(force_id=1, direction_adjustment=-2.0),
        )
        for i in range(5)
    ]
    engine_extreme = CognitionEngine(patches=extreme_patches)
    result_extreme = engine_extreme.evaluate(minimal_fred, "2020-06")
    # 无论叠加多少负向补丁，分数不应低于 -2
    check("extreme direction", result_extreme.forces["force1"][0], ForceDirection.STRONGLY_NEGATIVE)

    # ── 汇总 ──
    print("\n" + "=" * 60)
    if errors == 0:
        print("ALL TESTS PASSED")
    else:
        print(f"{errors} TEST(S) FAILED")
    print("=" * 60)
