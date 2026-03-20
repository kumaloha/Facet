"""
巴菲特因果链 · 全量最终评估
所有提过的公司，每一步输入输出全部展示。
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import pandas as pd
import polaris.features.l0.company  # noqa: F401
import polaris.features.l0.cross_period  # noqa: F401
from polaris.features.types import ComputeContext, FeatureLevel
from polaris.features.registry import get_features
from polaris.chains.business_type import infer_business_type, format_business_type
from polaris.chains.moat import assess_moat, format_moat
from polaris.chains.earnings import assess_earnings, format_earnings
from polaris.chains.distribution import assess_distribution, format_distribution
from polaris.chains.predictability import assess_predictability, format_predictability
from polaris.chains.integrity import assess_integrity, format_integrity
from polaris.chains.character import assess_character, format_character
from polaris.chains.risk import assess_risk, format_risk
from polaris.principles.engines.dcf import compute_intrinsic_value, reverse_dcf

# ── 加载所有公司数据 ──
from demo_batch_buffett import COMPANIES as C1, build_ctx as b1
from demo_batch2 import COMPANIES as C2, build_ctx as b2
from demo_batch3 import COMPANIES as C3, build_ctx as b3
from demo_batch4 import C as C4, build_ctx as b4
from demo_unh_intc import COMPANIES as C5, build_ctx as b5
from demo_oxy import OXY, build_ctx as b_oxy

ALL = [
    # batch 1
    ("可口可乐 (KO)", C1, b1, {}),
    ("苹果 (AAPL)", C1, b1, {"profit_volatility": "stable"}),
    ("英伟达 (NVDA)", C1, b1, {}),
    ("Google (GOOGL)", C1, b1, {}),
    ("阿里巴巴 (BABA)", C1, b1, {}),
    ("腾讯 (0700.HK)", C1, b1, {}),
    ("贵州茅台 (600519)", C1, b1, {}),
    ("紫金矿业 (601899)", C1, b1, {}),
    ("特变电工 (600089)", C1, b1, {"business_model": "commodity"}),
    # batch 2
    ("安达保险 (CB)", C2, b2, {}),
    ("达美乐 (DPZ)", C2, b2, {}),
    ("亚马逊 (AMZN)", C2, b2, {}),
    ("达维塔 (DVA)", C2, b2, {"profit_volatility": "stable"}),
    ("美国运通 (AXP)", C2, b2, {}),
    ("美国银行 (BAC)", C2, b2, {}),
    # UNH + Intel
    ("联合健康 (UNH)", C5, b5, {}),
    ("Intel (INTC)", C5, b5, {}),
    # batch 3
    ("Meta (META)", C3, b3, {}),
    ("AMD (AMD)", C3, b3, {}),
    ("Circle (CRCL)", C3, b3, {}),
    # batch 4
    ("甲骨文 (ORCL)", C4, b4, {}),
    ("沃尔玛 (WMT)", C4, b4, {}),
    ("高通 (QCOM)", C4, b4, {}),
    ("SEA (SE)", C4, b4, {}),
    ("拼多多 (PDD)", C4, b4, {}),
]


def compute_all_features(ctx):
    for level in (FeatureLevel.L0, FeatureLevel.L1, FeatureLevel.L2):
        for feat in get_features(level=level):
            try:
                r = feat.compute_fn(ctx)
            except Exception:
                continue
            if r:
                ctx.features[feat.name] = r.value
    return ctx


if __name__ == "__main__":
    # OXY 单独处理
    ALL.append(("西方石油 (OXY)", {"__oxy__": True}, b_oxy, {}))

    for name, companies, builder, overrides in ALL:
        print("\n" + "▓" * 65)
        print(f"  巴菲特因果链 · {name}")
        print("▓" * 65)

        if isinstance(companies, dict) and "__oxy__" in companies:
            data = OXY
        else:
            data = companies[name]

        ctx = builder(data)
        compute_all_features(ctx)
        print(f"\n  已算特征: {len(ctx.features)} 个")

        # ── Step 0: 生意画像 ──
        bt = infer_business_type(ctx, overrides=overrides if overrides else None)
        print(format_business_type(bt))

        # ── 线 1: 生意评估 ──
        print("=" * 65)
        print("  线 1: 生意评估")
        print("=" * 65)

        # Step 1: 护城河
        moat = assess_moat(ctx)
        print(format_moat(moat))

        # Step 2: 盈余能力
        earnings = assess_earnings(ctx)
        print(format_earnings(earnings))

        # Step 3: 利润分配
        dist = assess_distribution(ctx)
        print(format_distribution(dist))

        # Step 4: 可预测性
        pred = assess_predictability(ctx, moat_depth=moat.depth)
        print(format_predictability(pred))

        # Step 5+6: 估值 + 安全边际
        mkt = data.get("market", {})
        guidance = data.get("guidance", {})
        market_code = mkt.get("market", "US")

        line1_quality = (moat.depth in ("extreme", "deep") and
                         earnings.verdict == "holds" and
                         pred.verdict == "holds")
        certainty = "high" if line1_quality else "normal"

        print("\n  可估值 + 安全边际")
        print("  " + "═" * 48)
        labels = {"high": "高确定性 → 无风险利率", "normal": "普通 → 无风险+ERP"}
        print(f"  确定性: {labels[certainty]}")

        dcf = mos = None
        if mkt.get("discount_rate") and mkt.get("shares_outstanding"):
            dcf = compute_intrinsic_value(ctx.features, guidance, mkt["discount_rate"],
                                           mkt["shares_outstanding"], market=market_code,
                                           certainty=certainty)
            if dcf.intrinsic_value:
                mos = (dcf.intrinsic_value - mkt["price"]) / dcf.intrinsic_value
                unit = "元/股" if market_code in ("CN", "HK") else "$/股"
                print(f"  路径 {dcf.valuation_path}: 内在价值 {dcf.intrinsic_value:,.1f} {unit}")
                print(f"  当前股价: {mkt['price']} {unit}")
                print(f"  安全边际: {mos:.1%}")
                print(f"  假设: {dcf.key_assumptions}")
            else:
                print(f"  DCF 状态: {dcf.status}")

            oe = ctx.features.get("l0.company.owner_earnings", 0)
            if oe > 0 and mkt.get("price"):
                rdcf = reverse_dcf(mkt["price"], oe, mkt["discount_rate"],
                                   mkt["shares_outstanding"], market=market_code,
                                   certainty=certainty)
                if rdcf.implied_growth_rate is not None:
                    actual = ctx.features.get("l0.company.revenue_growth_yoy", 0)
                    print(f"\n  反向 DCF: 隐含增速 {rdcf.implied_growth_rate:.1%}  "
                          f"实际 {actual:.1%}  偏差 {rdcf.implied_growth_rate - actual:+.1%}")

        # ── 线 2: 人和环境 ──
        print("\n" + "=" * 65)
        print("  线 2: 人和环境")
        print("=" * 65)

        home = data.get("home_market", "")
        integrity = assess_integrity(ctx)
        print(format_integrity(integrity))

        character = assess_character(ctx)
        print(format_character(character))

        risk = assess_risk(ctx, home_market=home)
        print(format_risk(risk))

        # ── 综合判断 ──
        print("=" * 65)
        print("  综合判断")
        print("=" * 65)

        print(f"\n  生意画像: 商业模式={bt.business_model} 利润特征={bt.profit_volatility} "
              f"资本结构={bt.capital_structure} 护城河持久性={bt.moat_durability}")

        print(f"\n  线 1:")
        print(f"    护城河:   {moat.depth} — {moat.summary}")
        print(f"    盈余能力: {earnings.verdict} — {earnings.summary}")
        print(f"    利润分配: {dist.verdict} — {dist.summary}")
        print(f"    可预测:   {pred.verdict} — {pred.summary}")
        if dcf and dcf.intrinsic_value:
            unit = "元/股" if market_code in ("CN", "HK") else "$/股"
            print(f"    可估值:   valued — {dcf.intrinsic_value:,.1f} {unit} (路径 {dcf.valuation_path})")
            if mos is not None:
                print(f"    安全边际: {mos:.1%}")

        print(f"\n  线 2:")
        print(f"    诚信:     {integrity.verdict} — {integrity.summary}")
        print(f"    管理层:   {character.conviction} — {character.summary}")
        cat_risk = ("灾难性" if risk.has_catastrophic else
                   (f"{len(risk.significant)} 项重大" if risk.significant else "可控"))
        print(f"    风险:     {cat_risk} — {risk.summary}")

        # 最终结论
        line1_ok = (moat.depth not in ("none", "unknown") and
                    earnings.verdict == "holds" and
                    dist.verdict == "holds")
        line2_ok = (integrity.verdict != "breaks" and not risk.has_catastrophic)
        overvalued = mos is not None and mos < 0

        # 护城河持久性降级: expiring 或 none → 额外标注
        durability_warning = ""
        if bt.moat_durability == "expiring":
            durability_warning = "（护城河有期限）"
        elif bt.moat_durability == "none":
            durability_warning = "（无结构性护城河）"
        elif bt.moat_durability == "needs_reinvestment":
            durability_warning = "（护城河需持续投入）"

        print(f"\n  {'─' * 55}")
        if not line1_ok:
            broken = []
            if moat.depth in ("none", "unknown"):
                broken.append("护城河")
            if earnings.verdict != "holds":
                broken.append("盈余能力")
            if dist.verdict != "holds":
                broken.append("利润分配")
            print(f"  结论: ❌ 生意链断裂: {', '.join(broken)}")
        elif not line2_ok:
            if risk.has_catastrophic:
                print(f"  结论: ⚠️ 好生意但有灾难性风险 → 不能买")
            else:
                print(f"  结论: ⚠️ 好生意但诚信存疑 → 需谨慎")
        elif overvalued:
            print(f"  结论: ⚠️ 好生意但太贵（安全边际 {mos:.0%}），等便宜再买{durability_warning}")
        else:
            print(f"  结论: ✅ 可以投资{durability_warning}")
        print()
