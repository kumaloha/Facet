"""
评分引擎
========
特征向量 → 三流派各自评分 → 各流派独立结论。
不输出综合分，每个流派产出独立的结构化分析。

三流派：
- 巴菲特：过滤 → 评分 → 内在价值
- 达利欧：公司脆弱度 + 宏观象限（需外部数据）
- 索罗斯：反身性特征 + 反向 DCF（需外部数据）
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone

from polaris.scoring.dimensions import (
    SCHOOL_LABELS,
    SCHOOL_QUESTIONS,
    BuffettResult,
    DalioResult,
    Driver,
    School,
    SchoolScore,
    SorosResult,
)
from polaris.scoring.rules import get_rules

# 导入规则模块，触发 @rule 装饰器注册
import polaris.scoring.v1.buffett  # noqa: F401
import polaris.scoring.v1.dalio  # noqa: F401
import polaris.scoring.v1.soros  # noqa: F401


# ── 每流派归一化参数 ──────────────────────────────────────────────
# 基于各流派规则的实际点数范围设定，而非统一硬编码。
SCHOOL_RANGES: dict[School, tuple[float, float]] = {
    School.BUFFETT: (-20.0, 25.0),
    School.DALIO: (-12.0, 5.0),
    School.SOROS: (-12.0, 5.0),
}


@dataclass
class CompanyAnalysis:
    """公司三流派分析结果（替代原 CompanyScore）。"""
    company_id: int
    company_name: str
    ticker: str
    period: str
    model_version: str = "v1"
    buffett: BuffettResult | None = None
    dalio: DalioResult | None = None
    soros: SorosResult | None = None
    scored_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# 向后兼容旧 CLI
CompanyScore = CompanyAnalysis


def _normalize(raw: float, min_raw: float, max_raw: float) -> float:
    """将原始分数线性映射到 1-10。"""
    if max_raw == min_raw:
        return 5.5
    score = 1.0 + 9.0 * (raw - min_raw) / (max_raw - min_raw)
    return max(1.0, min(10.0, score))


def _score_to_signal_buffett(score: float) -> str:
    if score >= 7.0:
        return "值得持有"
    elif score >= 4.0:
        return "观望"
    else:
        return "不值得持有"


def _score_to_signal_dalio(score: float) -> str:
    if score >= 7.0:
        return "安全"
    elif score >= 4.0:
        return "中性"
    else:
        return "脆弱"


def _score_to_signal_soros(score: float) -> str:
    if score >= 7.0:
        return "机会"
    elif score >= 4.0:
        return "中性"
    else:
        return "风险"


SIGNAL_FNS = {
    School.BUFFETT: _score_to_signal_buffett,
    School.DALIO: _score_to_signal_dalio,
    School.SOROS: _score_to_signal_soros,
}


def score_school(school: School, features: dict[str, float]) -> SchoolScore:
    """对一个流派评分：执行全部规则 → 求和 → 归一化。"""
    rules = get_rules(school)
    drivers: list[Driver] = []
    raw_total = 0.0

    for r in rules:
        try:
            pts = r.evaluate_fn(features)
        except Exception:
            pts = 0.0
        raw_total += pts
        if pts != 0:
            drivers.append(
                Driver(rule_name=r.name, contribution=pts, description=r.description)
            )

    drivers.sort(key=lambda d: abs(d.contribution), reverse=True)

    min_raw, max_raw = SCHOOL_RANGES.get(school, (-5.0, 10.0))
    score = _normalize(raw_total, min_raw, max_raw)
    signal_fn = SIGNAL_FNS.get(school, _score_to_signal_buffett)

    return SchoolScore(
        school=school,
        score=score,
        raw_points=raw_total,
        signal=signal_fn(score),
        drivers=drivers[:5],
    )


# ── 巴菲特过滤逻辑 ──────────────────────────────────────────────

BUFFETT_FILTER_THRESHOLDS = {
    "predictability": {
        "l0.company.gross_margin_stability": ("<=", 0.10),
        "l0.company.consecutive_revenue_growth": (">=", 4),
    },
    "earnings_quality": {
        "l0.company.ocf_to_net_income": (">=", 0.6),
    },
    "financial_safety": {
        "l0.company.debt_to_equity": ("<=", 3.0),
        "l0.company.interest_coverage": (">=", 2.0),
    },
    "management": {
        "l0.company.narrative_fulfillment_rate": (">=", 0.5),
    },
    "moat": {
        "l0.company.gross_margin": (">=", 0.25),
    },
}


def _check_buffett_filters(features: dict[str, float]) -> tuple[bool, dict[str, bool]]:
    """检查巴菲特过滤条件。返回 (全部通过, 每个维度通过/失败)。"""
    details: dict[str, bool] = {}
    all_passed = True

    for dim_name, checks in BUFFETT_FILTER_THRESHOLDS.items():
        dim_passed = True
        for feat_name, (op, threshold) in checks.items():
            val = features.get(feat_name)
            if val is None:
                # 数据不足，不判定为失败，但标注
                continue
            if op == "<=" and val > threshold:
                dim_passed = False
            elif op == ">=" and val < threshold:
                dim_passed = False
        details[dim_name] = dim_passed
        if not dim_passed:
            all_passed = False

    return all_passed, details


# ── Soros 反身性阶段判定 ──────────────────────────────────────────

def _determine_reflexivity_phase(features: dict[str, float]) -> str:
    """根据特征判定反身性阶段。"""
    gap = features.get("l0.company.expectation_gap")
    fin_dep = features.get("l0.company.financing_dependency")
    misses = features.get("l0.company.consecutive_misses", 0)
    beats = features.get("l0.company.consecutive_beats", 0)

    # 数据不足
    if gap is None:
        # 没有 expectation_gap 时，从 financing_dependency 推断
        if fin_dep is not None and fin_dep > 1:
            return "正向加强（融资依赖高）"
        return "数据不足"

    if abs(gap) < 0.02:
        return "中性/潜伏"

    if gap > 0:
        # 市场过度乐观
        if misses > 0 or (fin_dep is not None and fin_dep > 1):
            return "正向脆弱，可能反转"
        return "正向加强中"
    else:
        # 市场过度悲观
        if beats > 2:
            return "负向过度，可能反弹"
        return "负向加强中"


# ── 主入口 ───────────────────────────────────────────────────────


def score_company(
    company_id: int,
    company_name: str,
    ticker: str,
    period: str,
    features: dict[str, float],
) -> CompanyAnalysis:
    """对一家公司执行三流派分析。"""
    result = CompanyAnalysis(
        company_id=company_id,
        company_name=company_name,
        ticker=ticker,
        period=period,
    )

    # ── 巴菲特 ──
    b_score = score_school(School.BUFFETT, features)
    passed, filter_details = _check_buffett_filters(features)
    result.buffett = BuffettResult(
        school_score=b_score,
        filters_passed=passed,
        filter_details=filter_details,
        # 内在价值需要外部数据（折现率），此处标记状态
        valuation_status="valued" if passed else "unvaluable",
    )

    # ── 达利欧 ──
    d_score = score_school(School.DALIO, features)
    result.dalio = DalioResult(
        school_score=d_score,
        # 象限和风险平价需要外部宏观数据，此处为 None
        # 公司脆弱度从规则得分反映
        vulnerability_score=d_score.score,
        vulnerability_drivers=d_score.drivers,
    )

    # ── 索罗斯 ──
    s_score = score_school(School.SOROS, features)
    phase = _determine_reflexivity_phase(features)
    result.soros = SorosResult(
        school_score=s_score,
        reflexivity_phase=phase,
        financing_dependency=features.get("l0.company.financing_dependency"),
        leverage_acceleration=features.get("l0.company.leverage_acceleration"),
    )

    return result


# ── 报告格式化 ───────────────────────────────────────────────────


def format_report(analysis: CompanyAnalysis) -> str:
    """格式化三流派分析报告（终端输出）。"""
    lines = [""]
    lines.append("=" * 60)
    lines.append("  AXION 三流派分析报告")
    lines.append("=" * 60)
    lines.append(
        f"  {analysis.company_name} ({analysis.ticker})"
        f"  |  {analysis.period}  |  {analysis.model_version}"
    )
    lines.append("")

    # ── 巴菲特 ──
    if analysis.buffett:
        b = analysis.buffett
        ss = b.school_score
        lines.append("-" * 60)
        lines.append(f"  {SCHOOL_LABELS[School.BUFFETT]}  {ss.score:.1f}/10  [{ss.signal}]")
        lines.append(f'  "{SCHOOL_QUESTIONS[School.BUFFETT]}"')

        if b.filter_details:
            status = "通过" if b.filters_passed else "未通过"
            lines.append(f"  过滤: {status}")
            for dim, passed in b.filter_details.items():
                mark = "+" if passed else "x"
                lines.append(f"    [{mark}] {dim}")

        lines.append(f"  估值状态: {b.valuation_status}")
        if b.intrinsic_value is not None:
            lines.append(f"  内在价值: {b.intrinsic_value:.2f} (路径 {b.valuation_path})")

        if ss.drivers:
            lines.append("  驱动因子:")
            for d in ss.drivers:
                lines.append(f"    {d.contribution:+.1f}  {d.rule_name}: {d.description}")
        lines.append("")

    # ── 达利欧 ──
    if analysis.dalio:
        dl = analysis.dalio
        ss = dl.school_score
        lines.append("-" * 60)
        lines.append(f"  {SCHOOL_LABELS[School.DALIO]}  {ss.score:.1f}/10  [{ss.signal}]")
        lines.append(f'  "{SCHOOL_QUESTIONS[School.DALIO]}"')

        if dl.quadrant:
            lines.append(f"  象限: {dl.quadrant}")
            lines.append(f"  估值信号: {dl.valuation_signal}")
        else:
            lines.append("  象限: 需要外部宏观数据")

        if dl.risk_parity_weights:
            lines.append(f"  风险平价权重: {dl.risk_parity_weights}")
        else:
            lines.append("  风险平价: 需要外部价格数据")

        if ss.drivers:
            lines.append("  脆弱度驱动因子:")
            for d in ss.drivers:
                lines.append(f"    {d.contribution:+.1f}  {d.rule_name}: {d.description}")
        lines.append("")

    # ── 索罗斯 ──
    if analysis.soros:
        sr = analysis.soros
        ss = sr.school_score
        lines.append("-" * 60)
        lines.append(f"  {SCHOOL_LABELS[School.SOROS]}  {ss.score:.1f}/10  [{ss.signal}]")
        lines.append(f'  "{SCHOOL_QUESTIONS[School.SOROS]}"')
        lines.append(f"  反身性阶段: {sr.reflexivity_phase}")

        if sr.expectation_gap is not None:
            lines.append(f"  预期偏差: {sr.expectation_gap:+.2%}")
        else:
            lines.append("  预期偏差: 需要外部价格数据（反向 DCF）")

        if sr.financing_dependency is not None:
            lines.append(f"  融资依赖度: {sr.financing_dependency:.2f}")
        if sr.leverage_acceleration is not None:
            lines.append(f"  杠杆加速度: {sr.leverage_acceleration:+.4f}")

        if ss.drivers:
            lines.append("  驱动因子:")
            for d in ss.drivers:
                lines.append(f"    {d.contribution:+.1f}  {d.rule_name}: {d.description}")
        lines.append("")

    lines.append("=" * 60)
    return "\n".join(lines)
