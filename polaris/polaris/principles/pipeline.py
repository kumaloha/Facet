"""
分层决策流水线
==============
Layer 1: 巴菲特 — 标的筛选（值不值得持有）
Layer 2: 达利欧 — 环境预测（因果引擎 + 前向投射）
Layer 3: 索罗斯 — 市场偏差（达利欧预测 vs 市场定价）

输出 DecisionContext 包含三层分层决策，不是三个独立评分。
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone

from polaris.principles.dimensions import (
    SCHOOL_LABELS,
    SCHOOL_QUESTIONS,
    BuffettResult,
    DalioResult,
    Driver,
    School,
    SchoolScore,
    SorosResult,
)
from polaris.principles.rules import get_rules

# 导入规则模块，触发 @rule 装饰器注册
import polaris.principles.v1.buffett  # noqa: F401
# dalio v1 规则已删除 — 达利欧链重构为宏观周期定位 + 主动押注，见 docs/dalio_chain_design.md
import polaris.principles.v1.soros  # noqa: F401


# ── 每流派归一化参数 ──────────────────────────────────────────────
# 基于各流派规则的实际点数范围设定，而非统一硬编码。
SCHOOL_RANGES: dict[School, tuple[float, float]] = {
    School.BUFFETT: (-20.0, 25.0),
    # DALIO: 不再用 V1 规则评分，链重构后由 chains/dalio.py 直接输出结构化结果
    School.SOROS: (-12.0, 5.0),
}


@dataclass
class DecisionContext:
    """三层分层决策上下文。"""
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
DecisionContext = DecisionContext


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


def evaluate_school(school: School, features: dict[str, float]) -> SchoolScore:
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
        "l0.company.consecutive_revenue_growth": (">=", 2),
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


def _fetch_market_context(ticker: str, company_id: int) -> dict:
    """从 Anchor DB 获取市场数据上下文。

    Returns:
        {
            "price": float | None,
            "shares_outstanding": float | None,
            "discount_rate": float | None,
            "guidance": dict[str, float | None],
        }
    """
    from polaris.db.anchor import get_guidance_dict, get_latest_macro, get_latest_stock_quote

    quote = get_latest_stock_quote(ticker)
    treasury = get_latest_macro("treasury_10y")

    return {
        "price": quote.get("price_close") if quote else None,
        "shares_outstanding": quote.get("shares_outstanding") if quote else None,
        "discount_rate": treasury / 100.0 if treasury is not None else None,  # ^TNX 以百分比存储
        "guidance": get_guidance_dict(company_id),
    }


def run_pipeline(
    company_id: int,
    company_name: str,
    ticker: str,
    period: str,
    features: dict[str, float],
    market_context: dict | None = None,
) -> DecisionContext:
    """对一家公司执行三流派分析。

    market_context: 可选，直接注入市场数据（用于测试或无 DB 场景）。
        格式: {"price", "shares_outstanding", "discount_rate", "guidance": {...}}
        不传时从 Anchor DB 获取。
    """
    result = DecisionContext(
        company_id=company_id,
        company_name=company_name,
        ticker=ticker,
        period=period,
    )

    # 获取市场数据
    if market_context is not None:
        mkt = market_context
    else:
        mkt = _fetch_market_context(ticker, company_id)

    # ── 巴菲特 ──
    b_score = evaluate_school(School.BUFFETT, features)
    passed, filter_details = _check_buffett_filters(features)

    # 过滤未通过时，信号不能高于"观望"
    if not passed and b_score.signal == "值得持有":
        b_score = SchoolScore(
            school=b_score.school,
            score=b_score.score,
            raw_points=b_score.raw_points,
            signal="观望（过滤未通过）",
            drivers=b_score.drivers,
        )

    buffett_result = BuffettResult(
        school_score=b_score,
        filters_passed=passed,
        filter_details=filter_details,
        valuation_status="unvaluable",
    )

    # 过滤通过 + 有市场数据 → 计算内在价值
    if passed and mkt["discount_rate"] is not None and mkt["shares_outstanding"] is not None:
        from polaris.principles.engines.dcf import compute_intrinsic_value

        dcf = compute_intrinsic_value(
            features=features,
            guidance=mkt["guidance"],
            discount_rate=mkt["discount_rate"],
            shares_outstanding=mkt["shares_outstanding"],
        )
        buffett_result.intrinsic_value = dcf.intrinsic_value
        buffett_result.valuation_path = dcf.valuation_path
        buffett_result.valuation_status = dcf.status
        buffett_result.key_assumptions = dcf.key_assumptions or {}

    result.buffett = buffett_result

    # ── 达利欧 ──
    # 达利欧链: 宏观周期定位 → 类型选择 → 主动押注 → 对冲规格
    # 输入是宏观数据，不是公司数据 — 同一时间点对所有公司输出相同
    try:
        from polaris.db.anchor import build_macro_context
        from polaris.chains.dalio import evaluate as dalio_evaluate, to_dalio_result
        macro_ctx = build_macro_context()
        dalio_chain = dalio_evaluate(macro_ctx)
        result.dalio = to_dalio_result(dalio_chain)
    except Exception as e:
        result.dalio = DalioResult(
            school_score=SchoolScore(
                school=School.DALIO,
                score=0.0,
                raw_points=0.0,
                signal=f"链执行失败: {e}",
            ),
        )

    # ── 索罗斯 ──
    s_score = evaluate_school(School.SOROS, features)
    phase = _determine_reflexivity_phase(features)

    soros_result = SorosResult(
        school_score=s_score,
        reflexivity_phase=phase,
        financing_dependency=features.get("l0.company.financing_dependency"),
        leverage_acceleration=features.get("l0.company.leverage_acceleration"),
    )

    # 有股价 + 市场数据 → 计算预期差（反向 DCF）
    oe = features.get("l0.company.owner_earnings")
    if (
        mkt["price"] is not None
        and mkt["shares_outstanding"] is not None
        and mkt["discount_rate"] is not None
        and oe is not None
        and oe > 0
    ):
        from polaris.principles.engines.dcf import reverse_dcf

        rdcf = reverse_dcf(
            current_price=mkt["price"],
            current_owner_earnings=oe,
            discount_rate=mkt["discount_rate"],
            shares_outstanding=mkt["shares_outstanding"],
        )
        if rdcf.status == "computed":
            soros_result.implied_growth_rate = rdcf.implied_growth_rate
            actual_growth = features.get("l0.company.revenue_growth_yoy")
            soros_result.actual_growth_rate = actual_growth
            if actual_growth is not None and rdcf.implied_growth_rate is not None:
                soros_result.expectation_gap = rdcf.implied_growth_rate - actual_growth

    result.soros = soros_result

    return result


# ── 报告格式化 ───────────────────────────────────────────────────


def format_decision(analysis: DecisionContext) -> str:
    """格式化三流派分析报告（终端输出）。"""
    lines = [""]
    lines.append("=" * 60)
    lines.append("  POLARIS 三流派分析报告")
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
        lines.append(f"  {SCHOOL_LABELS[School.DALIO]}  [{ss.signal}]")
        lines.append(f'  "{SCHOOL_QUESTIONS[School.DALIO]}"')

        if dl.regime:
            r = dl.regime
            lines.append(f"  象限: {r.quadrant}  (置信度 {r.confidence:.0%})")
            lines.append(f"  短期周期: {r.short_cycle_phase}")
            lines.append(f"  长期周期: {r.long_cycle_phase}")
        else:
            lines.append("  周期定位: 待接入宏观数据")

        if dl.active_tilts:
            lines.append("  主动押注:")
            for t in dl.active_tilts:
                lines.append(
                    f"    {t.direction} {t.asset_type}"
                    f"  (幅度 {t.magnitude:.0%}, 置信度 {t.confidence:.0%})"
                )
                lines.append(f"      逻辑: {t.thesis}")
                lines.append(f"      失效: {t.decay_trigger}")

        if dl.hedge_specs:
            lines.append("  对冲需求:")
            for h in dl.hedge_specs:
                lines.append(f"    保护: {h.protects_against}")
                lines.append(f"    最大可接受亏损: {h.max_acceptable_loss:.0%}")
                lines.append(f"    裸露敞口: {h.unhedged_exposure}")

        if dl.risk_parity_baseline:
            lines.append(f"  风险平价底仓: {dl.risk_parity_baseline}")
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
