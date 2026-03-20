"""
诚信检测
========
核心问题: 数据可信吗？管理层在藏什么？

三层:
  1. 硬证据（一票否决）: SEC 处罚、审计非标、财务重述
  2. 已知问题: 财报暴露的 + 第三方发现的
  3. 管理层承认的: 致股东信/电话会里提到的
  差集 = 藏了什么

已知问题来源:
  - 财报数据（Polaris 自动检测）: 债务高、现金流恶化、应收暴涨
  - 第三方（Anchor 提取）: 新闻、分析师报告、诉讼
"""

from __future__ import annotations

from dataclasses import dataclass, field

from polaris.features.types import ComputeContext


@dataclass
class IntegrityResult:
    # 硬证据
    hard_fails: list[str] = field(default_factory=list)

    # 已知问题
    issues_from_financials: list[str] = field(default_factory=list)
    issues_from_third_party: list[str] = field(default_factory=list)
    all_known_issues: list[str] = field(default_factory=list)

    # 管理层承认
    acknowledged: list[str] = field(default_factory=list)

    # 差集
    hidden: list[str] = field(default_factory=list)

    # 综合
    verdict: str = ""  # holds / breaks / unclear
    summary: str = ""


def _feat(ctx: ComputeContext, key: str) -> float | None:
    return ctx.features.get(f"l0.company.{key}")


def assess_integrity(ctx: ComputeContext) -> IntegrityResult:
    r = IntegrityResult()

    # ══════════════════════════════════════════════════════════
    #  1. 硬证据（一票否决）
    # ══════════════════════════════════════════════════════════

    # 审计意见
    audit = ctx.get_audit_opinions()
    if not audit.empty and "opinion_type" in audit.columns:
        non_standard = audit[audit["opinion_type"] != "unqualified"]
        if not non_standard.empty:
            types = non_standard["opinion_type"].tolist()
            r.hard_fails.append(f"审计意见非标准: {types}")

        # 强调事项
        if "emphasis_matters" in audit.columns:
            em = audit["emphasis_matters"].dropna()
            if not em.empty:
                for matter in em:
                    if matter.strip():
                        r.hard_fails.append(f"审计强调事项: {matter}")

    # ══════════════════════════════════════════════════════════
    #  2. 已知问题 — 财报数据暴露的
    # ══════════════════════════════════════════════════════════

    # 债务问题
    # 判断特殊业务类型: 负权益公司（回购导致）+ 银行/保险/支付
    ds = ctx.get_downstream_segments()
    ds_cats = set()
    if not ds.empty and "product_category" in ds.columns:
        ds_cats = set(ds["product_category"].dropna().str.lower().unique())
    is_financial = bool(ds_cats & {"banking", "insurance", "payment"})

    equity_val = None
    fli = ctx.get_financial_line_items()
    if not fli.empty:
        eq_rows = fli[fli["item_key"] == "shareholders_equity"]
        if not eq_rows.empty:
            equity_val = float(eq_rows.iloc[0]["value"])

    de = _feat(ctx, "debt_to_equity")
    if de is not None and de > 3.0 and (equity_val is None or equity_val > 0):
        r.issues_from_financials.append(f"高杠杆: D/E = {de:.1f}")

    ic = _feat(ctx, "interest_coverage")
    if ic is not None and ic < 2.0 and not is_financial:
        r.issues_from_financials.append(f"偿债压力: 利息覆盖率 = {ic:.1f}")

    net_debt_ebitda = _feat(ctx, "net_debt_to_ebitda")
    if net_debt_ebitda is not None and net_debt_ebitda > 5.0:
        if equity_val is not None and equity_val < 0 and net_debt_ebitda < 7.0:
            pass  # 负权益公司合理杠杆
        else:
            r.issues_from_financials.append(f"净债务/EBITDA = {net_debt_ebitda:.1f}")

    # 现金流问题
    ocf_ni = _feat(ctx, "ocf_to_net_income")
    if ocf_ni is not None and ocf_ni < 0.5:
        r.issues_from_financials.append(f"现金流背离利润: OCF/NI = {ocf_ni:.2f}")

    # 应收暴涨
    ar_vs_rev = _feat(ctx, "receivables_growth_vs_revenue")
    if ar_vs_rev is not None and ar_vs_rev > 0.15:
        r.issues_from_financials.append(f"应收增速远超收入: +{ar_vs_rev:.0%}")

    # 存货暴涨
    inv_vs_rev = _feat(ctx, "inventory_growth_vs_revenue")
    if inv_vs_rev is not None and inv_vs_rev > 0.15:
        r.issues_from_financials.append(f"存货增速远超收入: +{inv_vs_rev:.0%}")

    # 商誉过高
    gw = _feat(ctx, "goodwill_to_assets")
    if gw is not None and gw > 0.30:
        r.issues_from_financials.append(f"商誉/总资产 = {gw:.0%}，商誉减值风险")

    # 关联交易
    rpt = _feat(ctx, "related_party_amount_to_revenue")
    if rpt is not None and rpt > 0.05:
        r.issues_from_financials.append(f"关联交易/收入 = {rpt:.1%}")

    # ══════════════════════════════════════════════════════════
    #  2b. 已知问题 — 第三方发现的
    # ══════════════════════════════════════════════════════════

    ki = ctx.get_known_issues()
    if not ki.empty and "issue_description" in ki.columns:
        for _, row in ki.iterrows():
            desc = row["issue_description"]
            severity = row.get("severity", "")
            source = row.get("source_type", "")
            r.issues_from_third_party.append(
                f"[{source}] {desc}" + (f" (severity: {severity})" if severity else ""))

    r.all_known_issues = r.issues_from_financials + r.issues_from_third_party

    # ══════════════════════════════════════════════════════════
    #  3. 管理层承认的
    # ══════════════════════════════════════════════════════════

    # 管理层回应分两类:
    #   有效承认: response_quality in (strong, adequate) 或有 action_plan
    #   敷衍回应: response_quality = defensive 且无 action_plan
    #     → "我知道但我不打算改" ≈ 没承认
    effective_acks = []
    dismissive_acks = []

    ma = ctx.get_management_acknowledgments()
    if not ma.empty and "issue_description" in ma.columns:
        for _, row in ma.iterrows():
            desc = row["issue_description"]
            quality = row.get("response_quality", "")
            has_plan = row.get("has_action_plan", False)
            label = (f"{desc}" + (f" [{quality}]" if quality else "") +
                     (" (有改进计划)" if has_plan else ""))

            if quality == "defensive" and not has_plan:
                # "我知道但我不改" = 敷衍，不算有效承认
                dismissive_acks.append(label)
            else:
                effective_acks.append(label)

    r.acknowledged = effective_acks

    # ══════════════════════════════════════════════════════════
    #  4. 差集: 藏了什么
    # ══════════════════════════════════════════════════════════
    # 只比对第三方发现的问题（外部事件、诉讼、监管调查）。
    # 财报指标（issues_from_financials）已经在公开报表中披露，
    # 不需要管理层额外"承认"——10-K/年报本身就是披露。
    # 差集 = 第三方发现但管理层没回应的。

    # 差集只用有效承认来匹配，敷衍回应不算
    all_valid_ack_text = " ".join(r.acknowledged).lower() if r.acknowledged else ""

    if r.issues_from_third_party and (r.acknowledged or dismissive_acks):
        for issue in r.issues_from_third_party:
            keywords = [w for w in issue.lower().split() if len(w) > 2]
            mentioned = any(kw in all_valid_ack_text for kw in keywords[:3]) if keywords else False
            if not mentioned:
                r.hidden.append(issue)
        # 敷衍回应对应的问题也算隐瞒 — "我知道但不打算改"和没说一样
        if dismissive_acks:
            r.hidden.append(f"敷衍回应 {len(dismissive_acks)} 条: "
                            + "; ".join(d.split("[")[0].strip() for d in dismissive_acks))
    elif r.issues_from_third_party and not r.acknowledged and not dismissive_acks:
        # 有第三方问题但完全没有管理层回应数据
        pass  # 不能判定为藏，可能是数据缺失

    # ══════════════════════════════════════════════════════════
    #  综合判定
    # ══════════════════════════════════════════════════════════

    if r.hard_fails:
        r.verdict = "breaks"
        r.summary = "硬证据: " + "; ".join(r.hard_fails)
    elif len(r.hidden) >= 3:
        r.verdict = "breaks"
        r.summary = f"管理层隐瞒了 {len(r.hidden)} 个已知问题"
    elif r.hidden:
        r.verdict = "unclear"
        r.summary = f"管理层可能隐瞒了 {len(r.hidden)} 个问题: " + "; ".join(r.hidden[:2])
    elif r.all_known_issues and r.acknowledged:
        r.verdict = "holds"
        r.summary = f"已知 {len(r.all_known_issues)} 个问题，管理层均有回应"
    elif not r.all_known_issues:
        r.verdict = "holds"
        r.summary = "未发现明显问题"
    else:
        r.verdict = "unclear"
        r.summary = f"有 {len(r.all_known_issues)} 个已知问题，但无管理层回应数据"

    return r


def format_integrity(result: IntegrityResult) -> str:
    lines = [""]
    lines.append("  诚信检测")
    lines.append("  ════════════════════════════════════════════════")

    if result.hard_fails:
        lines.append(f"\n  ✗ 硬证据（一票否决）")
        for hf in result.hard_fails:
            lines.append(f"    ⚠ {hf}")

    if result.issues_from_financials:
        lines.append(f"\n  ▸ 财报暴露的问题 ({len(result.issues_from_financials)})")
        for issue in result.issues_from_financials:
            lines.append(f"    · {issue}")

    if result.issues_from_third_party:
        lines.append(f"\n  ▸ 第三方发现的问题 ({len(result.issues_from_third_party)})")
        for issue in result.issues_from_third_party:
            lines.append(f"    · {issue}")

    if result.acknowledged:
        lines.append(f"\n  ▸ 管理层承认的 ({len(result.acknowledged)})")
        for ack in result.acknowledged:
            lines.append(f"    ✓ {ack}")

    if result.hidden:
        lines.append(f"\n  ⚠ 管理层隐瞒的（差集）({len(result.hidden)})")
        for h in result.hidden:
            lines.append(f"    ✗ {h}")

    lines.append(f"\n  ════════════════════════════════════════════════")
    v = {"holds": "可信", "breaks": "不可信", "unclear": "存疑"}
    lines.append(f"  {v.get(result.verdict, result.verdict)}: {result.summary}")
    lines.append("")
    return "\n".join(lines)
