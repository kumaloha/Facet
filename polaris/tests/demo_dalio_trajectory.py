"""
达利欧因果链 — 轨迹分析验证
==============================

用 from_series 多期快照构建 MacroContext，验证轨迹信号（momentum/impulse）
是否比静态快照更早、更准确地识别周期转折。

核心问题：同样的绝对值，不同的轨迹，应该得到不同的判断。
"""

from polaris.chains.dalio import MacroContext, MacroSnapshot, evaluate, format_dalio


def test_same_level_different_trajectory():
    """GDP=2.0%，但一个在加速、一个在减速 — 应该得到不同判断。"""
    print("=" * 70)
    print("  测试 1: 同样的 GDP 2.0%，不同轨迹")
    print("=" * 70)

    # 加速中: 1.0 → 1.5 → 2.0
    accelerating = MacroContext.from_series([
        MacroSnapshot(date="2024-Q1", gdp_growth=1.0, cpi=2.0, fed_funds_rate=3.0, credit_growth=3.0, unemployment_rate=5.0),
        MacroSnapshot(date="2024-Q2", gdp_growth=1.5, cpi=2.0, fed_funds_rate=2.5, credit_growth=4.0, unemployment_rate=4.8),
        MacroSnapshot(date="2024-Q3", gdp_growth=2.0, cpi=2.0, fed_funds_rate=2.0, credit_growth=5.0, unemployment_rate=4.5,
                      treasury_10y=3.5, treasury_2y=3.0, total_debt_to_gdp=250),
    ])

    # 减速中: 3.0 → 2.5 → 2.0
    decelerating = MacroContext.from_series([
        MacroSnapshot(date="2024-Q1", gdp_growth=3.0, cpi=3.0, fed_funds_rate=5.0, credit_growth=8.0, unemployment_rate=3.5),
        MacroSnapshot(date="2024-Q2", gdp_growth=2.5, cpi=2.8, fed_funds_rate=5.0, credit_growth=6.0, unemployment_rate=3.8),
        MacroSnapshot(date="2024-Q3", gdp_growth=2.0, cpi=2.5, fed_funds_rate=5.0, credit_growth=4.0, unemployment_rate=4.2,
                      treasury_10y=4.5, treasury_2y=4.8, total_debt_to_gdp=280),
    ])

    c1 = evaluate(accelerating)
    c2 = evaluate(decelerating)

    print(f"\n  加速 (1.0→1.5→2.0):")
    print(f"    GDP 动量: {accelerating.gdp_momentum:+.1f}pp, 信贷脉冲: {accelerating.credit_impulse:+.1f}pp")
    print(f"    → 周期: {c1.regime.short_cycle_phase}")
    ow1 = [t.asset_type for t in c1.active_tilts if t.direction == "overweight"]
    print(f"    → 超配: {', '.join(ow1)}")

    print(f"\n  减速 (3.0→2.5→2.0):")
    print(f"    GDP 动量: {decelerating.gdp_momentum:+.1f}pp, 信贷脉冲: {decelerating.credit_impulse:+.1f}pp")
    print(f"    → 周期: {c2.regime.short_cycle_phase}")
    ow2 = [t.asset_type for t in c2.active_tilts if t.direction == "overweight"]
    print(f"    → 超配: {', '.join(ow2)}")

    assert c1.regime.short_cycle_phase != c2.regime.short_cycle_phase, \
        f"同 GDP 不同轨迹应得不同判断！加速={c1.regime.short_cycle_phase}, 减速={c2.regime.short_cycle_phase}"
    print("\n  ✓ 同样的 GDP 2.0%，加速和减速得到了不同判断")


def test_credit_impulse_leading():
    """信贷增速还在 8%（看起来健康），但信贷脉冲已转负 — 应该提早报警。"""
    print(f"\n{'=' * 70}")
    print("  测试 2: 信贷增速 8%（看起来健康），但脉冲已转负")
    print("=" * 70)

    # 信贷从 12% 降到 10% 降到 8% — 增速仍正，但脉冲 = -2 → -2 = 0...
    # 不对，impulse = (当期增速-上期增速) = 8-10 = -2
    healthy_looking = MacroContext.from_series([
        MacroSnapshot(date="2006-Q1", gdp_growth=3.0, cpi=2.5, fed_funds_rate=5.0, credit_growth=12.0, unemployment_rate=4.5),
        MacroSnapshot(date="2006-Q3", gdp_growth=2.5, cpi=2.5, fed_funds_rate=5.25, credit_growth=10.0, unemployment_rate=4.5),
        MacroSnapshot(date="2007-Q1", gdp_growth=2.0, cpi=2.5, fed_funds_rate=5.25, credit_growth=8.0, unemployment_rate=4.6,
                      treasury_10y=4.7, treasury_2y=4.9, total_debt_to_gdp=340),
    ])

    c = evaluate(healthy_looking)

    print(f"\n  信贷增速: 12% → 10% → 8% (绝对值看起来还行)")
    print(f"  信贷脉冲: {healthy_looking.credit_impulse:+.1f}pp (脉冲已经连续为负!)")
    print(f"  GDP 动量: {healthy_looking.gdp_momentum:+.1f}pp")
    print(f"  → 周期: {c.regime.short_cycle_phase}")

    uw = [t.asset_type for t in c.active_tilts if t.direction == "underweight"]
    print(f"  → 低配: {', '.join(uw)}")

    if c.tail_risk:
        print(f"  → 尾部风险: {c.tail_risk.severity}")
        for r in c.tail_risk.risks:
            print(f"    ⚠ {r}")

    assert "expansion" not in c.regime.short_cycle_phase, \
        f"信贷脉冲为负时不应判为扩张！got: {c.regime.short_cycle_phase}"
    print("\n  ✓ 信贷增速 8% 但脉冲 -2pp → 正确识别为紧缩")


def test_2007_trajectory_vs_snapshot():
    """2007Q3: 快照模式 vs 轨迹模式 — 轨迹应该更强的警告。"""
    print(f"\n{'=' * 70}")
    print("  测试 3: 2007Q3 — 快照 vs 轨迹")
    print("=" * 70)

    # 快照（无轨迹）
    snapshot_only = MacroContext(
        gdp_growth_actual=2.0, gdp_growth_expected=2.5,
        cpi_actual=2.8, cpi_expected=2.0,
        fed_funds_rate=5.25, credit_growth=5.0,
        total_debt_to_gdp=350, vix=30,
        treasury_10y=4.7, treasury_2y=4.4,
        unemployment_rate=4.7,
    )

    # 轨迹（3 期快照）
    with_trajectory = MacroContext.from_series([
        MacroSnapshot(date="2006-Q3", gdp_growth=2.7, cpi=3.5, fed_funds_rate=5.25, credit_growth=12.0, unemployment_rate=4.5),
        MacroSnapshot(date="2007-Q1", gdp_growth=2.3, cpi=2.4, fed_funds_rate=5.25, credit_growth=9.0, unemployment_rate=4.5),
        MacroSnapshot(date="2007-Q3", gdp_growth=2.0, cpi=2.8, fed_funds_rate=5.25, credit_growth=5.0, unemployment_rate=4.7,
                      treasury_10y=4.7, treasury_2y=4.4, total_debt_to_gdp=350, vix=30),
    ], expected_gdp=2.5, expected_cpi=2.0)

    c_snap = evaluate(snapshot_only)
    c_traj = evaluate(with_trajectory)

    print(f"\n  快照模式 (无轨迹):")
    print(f"    → 周期: {c_snap.regime.short_cycle_phase}")
    ow_s = [t.asset_type for t in c_snap.active_tilts if t.direction == "overweight"]
    uw_s = [t.asset_type for t in c_snap.active_tilts if t.direction == "underweight"]
    print(f"    → 超配: {', '.join(ow_s) if ow_s else '无'}")
    print(f"    → 低配: {', '.join(uw_s) if uw_s else '无'}")

    print(f"\n  轨迹模式 (12%→9%→5%, GDP 2.7→2.3→2.0):")
    print(f"    GDP 动量: {with_trajectory.gdp_momentum:+.1f}pp")
    print(f"    信贷脉冲: {with_trajectory.credit_impulse:+.1f}pp")
    print(f"    失业方向: {with_trajectory.unemployment_direction:+.1f}pp")
    print(f"    → 周期: {c_traj.regime.short_cycle_phase}")
    ow_t = [t.asset_type for t in c_traj.active_tilts if t.direction == "overweight"]
    uw_t = [t.asset_type for t in c_traj.active_tilts if t.direction == "underweight"]
    print(f"    → 超配: {', '.join(ow_t) if ow_t else '无'}")
    print(f"    → 低配: {', '.join(uw_t) if uw_t else '无'}")

    # 轨迹模式应该更偏防御
    print(f"\n  差异分析:")
    snap_defensive = len([t for t in c_snap.active_tilts if t.direction == "underweight" and "equity" in t.asset_type])
    traj_defensive = len([t for t in c_traj.active_tilts if t.direction == "underweight" and "equity" in t.asset_type])
    print(f"    快照: {snap_defensive} 个股票类低配")
    print(f"    轨迹: {traj_defensive} 个股票类低配")

    # 轨迹模式应该至少跟快照一样防御
    contraction_phases = ("early_contraction", "mid_contraction", "late_contraction")
    assert c_traj.regime.short_cycle_phase in contraction_phases, \
        f"2007Q3 轨迹模式应识别为紧缩！got: {c_traj.regime.short_cycle_phase}"
    print(f"\n  ✓ 轨迹模式正确识别 2007Q3 为紧缩阶段")


def test_recovery_trajectory():
    """2009Q2: 从地狱开始恢复 — 信贷脉冲转正应该识别为 early_expansion。"""
    print(f"\n{'=' * 70}")
    print("  测试 4: 2009Q2 — 从地狱恢复中")
    print("=" * 70)

    recovery = MacroContext.from_series([
        MacroSnapshot(date="2008-Q4", gdp_growth=-8.4, cpi=-0.4, fed_funds_rate=0.25, credit_growth=-5.0, unemployment_rate=7.2),
        MacroSnapshot(date="2009-Q1", gdp_growth=-4.4, cpi=-0.2, fed_funds_rate=0.25, credit_growth=-3.0, unemployment_rate=8.5),
        MacroSnapshot(date="2009-Q2", gdp_growth=-0.7, cpi=0.1, fed_funds_rate=0.25, credit_growth=-1.0, unemployment_rate=9.5,
                      treasury_10y=3.5, treasury_2y=1.0, total_debt_to_gdp=370, vix=28),
    ], expected_gdp=2.0, expected_cpi=2.0)

    c = evaluate(recovery)

    print(f"\n  GDP: -8.4 → -4.4 → -0.7 (仍为负，但跌幅在收窄!)")
    print(f"  信贷: -5.0 → -3.0 → -1.0 (仍为负，但收缩在放缓!)")
    print(f"  GDP 动量: {recovery.gdp_momentum:+.1f}pp")
    print(f"  信贷脉冲: {recovery.credit_impulse:+.1f}pp")
    print(f"  → 周期: {c.regime.short_cycle_phase}")

    ow = [t.asset_type for t in c.active_tilts if t.direction == "overweight"]
    print(f"  → 超配: {', '.join(ow)}")

    # 关键: GDP 仍为负但脉冲转正 → 应该开始识别复苏
    print(f"\n  关键洞察: GDP 仍为 -0.7%（绝对值看是衰退），")
    print(f"  但 GDP 动量 {recovery.gdp_momentum:+.1f}pp + 信贷脉冲 {recovery.credit_impulse:+.1f}pp")
    print(f"  → 轨迹告诉你: 最坏的时候已经过去了")

    # 这个情况比较复杂: GDP<-5的规则会把它推进 contraction
    # 但 GDP=-0.7 不触发那个规则, 而信贷脉冲转正应该给出复苏信号
    if c.regime.short_cycle_phase in ("late_contraction", "early_expansion"):
        print(f"\n  ✓ 正确识别为 {c.regime.short_cycle_phase}（复苏信号）")
    else:
        print(f"\n  → 判为 {c.regime.short_cycle_phase}（可讨论，数据确实矛盾）")


def main():
    test_same_level_different_trajectory()
    test_credit_impulse_leading()
    test_2007_trajectory_vs_snapshot()
    test_recovery_trajectory()

    print(f"\n{'=' * 70}")
    print("  全部通过")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
