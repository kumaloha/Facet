"""
达利欧因果链 — 高级测试
========================

测试三个新能力:
1. 债务结构拆解: 同样总债务，不同结构 → 不同风险
2. 百分位归一化: 同样利率，不同历史背景 → 不同判断
3. 更多历史时代: 1994/2015/2018/日本1990
"""

from polaris.chains.dalio import (
    MacroContext, MacroSnapshot, evaluate, format_dalio,
    _propagate_causal_graph,
)


# ══════════════════════════════════════════════════════════════
#  测试组 1: 债务结构拆解
# ══════════════════════════════════════════════════════════════

def test_debt_structure():
    """同样 total_debt/GDP=300%，但结构完全不同 → 风险应该不同。"""
    print("=" * 70)
    print("  测试组 1: 债务结构拆解")
    print("=" * 70)

    base = dict(
        gdp_growth_actual=2.0, gdp_growth_expected=2.5,
        cpi_actual=2.5, cpi_expected=2.0,
        fed_funds_rate=4.0, credit_growth=3.0,
        total_debt_to_gdp=300, unemployment_rate=5.0,
    )

    # 场景 A: 政府债高，私人债低（日本模式）— 相对安全
    japan_style = MacroContext(
        **base,
        government_debt_to_gdp=200,
        household_debt_to_income=80,
        corporate_debt_to_gdp=60,
        financial_sector_leverage=12,
    )

    # 场景 B: 家庭债高 + 金融杠杆高（2007 美国模式）— 极度危险
    us2007_style = MacroContext(
        **base,
        government_debt_to_gdp=60,
        household_debt_to_income=130,
        corporate_debt_to_gdp=80,
        financial_sector_leverage=30,
    )

    # 场景 C: 只有总量，无结构（降级）
    total_only = MacroContext(**base)

    g_japan = _propagate_causal_graph(japan_style)
    g_us = _propagate_causal_graph(us2007_style)
    g_total = _propagate_causal_graph(total_only)

    ds_japan = g_japan.nodes["debt_service_burden"]
    ds_us = g_us.nodes["debt_service_burden"]
    ds_total = g_total.nodes["debt_service_burden"]

    print(f"\n  同样 total_debt/GDP=300%, rate=4%:")
    print(f"  A) 日本模式 (政府债高):    偿债负担 = {ds_japan.value:+.3f}  {ds_japan.detail}")
    print(f"     {ds_japan.inputs_used}")
    print(f"  B) 2007美国 (家庭+金融高): 偿债负担 = {ds_us.value:+.3f}  {ds_us.detail}")
    print(f"     {ds_us.inputs_used}")
    print(f"  C) 只有总量:               偿债负担 = {ds_total.value:+.3f}  {ds_total.detail}")

    # 美国模式应该比日本模式危险得多
    assert ds_us.value < ds_japan.value, \
        f"2007美国结构应比日本结构更危险! 美国={ds_us.value:.3f} 日本={ds_japan.value:.3f}"

    # 违约压力也应该不同
    dp_japan = g_japan.nodes["default_pressure"].value
    dp_us = g_us.nodes["default_pressure"].value
    print(f"\n  违约压力:")
    print(f"  A) 日本模式: {dp_japan:+.3f}")
    print(f"  B) 2007美国: {dp_us:+.3f}")
    assert dp_us > dp_japan, "2007美国违约压力应更高"

    # 资产配置也应该不同
    c_japan = evaluate(japan_style)
    c_us = evaluate(us2007_style)
    ow_japan = [t.asset_type for t in c_japan.active_tilts if t.direction == "overweight"]
    ow_us = [t.asset_type for t in c_us.active_tilts if t.direction == "overweight"]

    print(f"\n  资产配置:")
    print(f"  A) 日本: 超配 {ow_japan}")
    print(f"  B) 美国: 超配 {ow_us}")

    print("\n  ✓ 同样总债务，不同结构 → 不同风险评估")


def test_debt_structure_2007_vs_2019():
    """2007 vs 2019: 都是高债务，但 2007 家庭/金融脆弱，2019 企业债脆弱。"""
    print(f"\n{'=' * 70}")
    print("  测试: 2007 vs 2019 债务结构")
    print("=" * 70)

    # 2007: 家庭 + 金融杠杆炸弹
    m2007 = MacroContext(
        gdp_growth_actual=1.5, gdp_growth_expected=2.5,
        cpi_actual=2.8, cpi_expected=2.0,
        fed_funds_rate=5.25, credit_growth=9.0,
        total_debt_to_gdp=340,
        household_debt_to_income=130,
        corporate_debt_to_gdp=70,
        government_debt_to_gdp=60,
        financial_sector_leverage=28,
        unemployment_rate=4.5,
    )

    # 2019: 企业债 BBB 泡沫（CLO/杠杆贷款）
    m2019 = MacroContext(
        gdp_growth_actual=2.3, gdp_growth_expected=2.0,
        cpi_actual=1.8, cpi_expected=2.0,
        fed_funds_rate=2.5, credit_growth=5.0,
        total_debt_to_gdp=350,
        household_debt_to_income=95,     # 家庭去杠杆了
        corporate_debt_to_gdp=105,       # 企业债创新高
        government_debt_to_gdp=105,      # 政府债也高了
        financial_sector_leverage=15,     # 银行杠杆降了（监管加强）
        unemployment_rate=3.5,
    )

    g7 = _propagate_causal_graph(m2007)
    g19 = _propagate_causal_graph(m2019)

    print(f"\n  2007 (家庭+金融脆弱):")
    print(f"    偿债负担: {g7.nodes['debt_service_burden'].value:+.3f}")
    print(f"    消费者:   {g7.nodes['consumer_health'].value:+.3f}")
    print(f"    违约压力: {g7.nodes['default_pressure'].value:+.3f}")
    print(f"    反馈: {g7.feedback_active[:2]}")

    print(f"\n  2019 (企业债脆弱):")
    print(f"    偿债负担: {g19.nodes['debt_service_burden'].value:+.3f}")
    print(f"    消费者:   {g19.nodes['consumer_health'].value:+.3f}")
    print(f"    违约压力: {g19.nodes['default_pressure'].value:+.3f}")
    print(f"    反馈: {g19.feedback_active[:2]}")

    # 2007 的消费者更脆弱（家庭债高）
    assert g7.nodes["consumer_health"].value < g19.nodes["consumer_health"].value, \
        "2007 消费者应比 2019 更脆弱"

    print("\n  ✓ 2007 家庭脆弱 vs 2019 企业脆弱 — 正确区分")


# ══════════════════════════════════════════════════════════════
#  测试组 2: 百分位归一化
# ══════════════════════════════════════════════════════════════

def test_percentile_normalization():
    """同样 5% 利率，在 1970s 是低的（历史中位 8%），在 2010s 是高的（中位 0.5%）。"""
    print(f"\n{'=' * 70}")
    print("  测试组 2: 百分位归一化")
    print("=" * 70)

    base = dict(
        gdp_growth_actual=2.0, gdp_growth_expected=2.0,
        cpi_actual=3.0, cpi_expected=2.0,
        fed_funds_rate=5.0,
        credit_growth=3.0, unemployment_rate=5.0,
    )

    # 1970s 背景: 利率中位 8%, 5% 算低
    ctx_70s = MacroContext(
        **base,
        hist_rate_median=8.0, hist_rate_p25=6.0, hist_rate_p75=11.0,
        hist_unemployment_median=6.5,
        hist_gdp_median=3.5,
    )

    # 2010s 背景: 利率中位 0.5%, 5% 算极高
    ctx_10s = MacroContext(
        **base,
        hist_rate_median=0.5, hist_rate_p25=0.25, hist_rate_p75=1.5,
        hist_unemployment_median=5.0,
        hist_gdp_median=2.2,
    )

    # 无历史（降级到硬编码）
    ctx_none = MacroContext(**base)

    g70 = _propagate_causal_graph(ctx_70s)
    g10 = _propagate_causal_graph(ctx_10s)
    g_none = _propagate_causal_graph(ctx_none)

    p70 = g70.nodes["policy_response"].value
    p10 = g10.nodes["policy_response"].value
    p_none = g_none.nodes["policy_response"].value

    print(f"\n  同样 fed_funds_rate=5.0%:")
    print(f"  1970s 背景 (中位 8%):  policy = {p70:+.3f}  → {'宽松' if p70 > 0 else '紧缩'}")
    print(f"  2010s 背景 (中位 0.5%): policy = {p10:+.3f}  → {'宽松' if p10 > 0 else '紧缩'}")
    print(f"  无历史 (硬编码 3%):    policy = {p_none:+.3f}  → {'宽松' if p_none > 0 else '紧缩'}")

    # 在 70s 背景下，5% 应该偏宽松（低于中位 8%）
    # 在 10s 背景下，5% 应该很紧缩（远高于中位 0.5%）
    assert p70 > p10, f"5% 在 70s 应比在 10s 更宽松! 70s={p70:.3f} 10s={p10:.3f}"

    # 资产配置差异
    c70 = evaluate(ctx_70s)
    c10 = evaluate(ctx_10s)
    ow70 = [t.asset_type for t in c70.active_tilts if t.direction == "overweight"]
    ow10 = [t.asset_type for t in c10.active_tilts if t.direction == "overweight"]
    print(f"\n  1970s: 超配 {ow70}")
    print(f"  2010s: 超配 {ow10}")

    print("\n  ✓ 同样利率，不同历史背景 → 不同政策判断")


# ══════════════════════════════════════════════════════════════
#  测试组 3: 更多历史时代
# ══════════════════════════════════════════════════════════════

EXTRA_HISTORICAL = {
    "1994 债券大屠杀 (格林斯潘意外加息)": {
        "macro": MacroContext(
            gdp_growth_actual=4.0, gdp_growth_expected=2.5,
            cpi_actual=2.6, cpi_expected=3.0,
            fed_funds_rate=5.5,     # 从 3% 急升到 5.5%
            treasury_10y=7.8,       # 10Y 从 5.6% 飙到 7.8%
            treasury_2y=6.5,
            credit_growth=6.0,
            total_debt_to_gdp=230,
            vix=18,
            unemployment_rate=5.5,
            fiscal_deficit_to_gdp=2.9,
        ),
        "trajectory": [
            MacroSnapshot(date="1993-Q4", gdp_growth=2.8, cpi=2.7, fed_funds_rate=3.0, credit_growth=4.0, unemployment_rate=6.5),
            MacroSnapshot(date="1994-Q2", gdp_growth=3.5, cpi=2.6, fed_funds_rate=4.25, credit_growth=5.0, unemployment_rate=6.0),
            MacroSnapshot(date="1994-Q4", gdp_growth=4.0, cpi=2.6, fed_funds_rate=5.5, credit_growth=6.0, unemployment_rate=5.5,
                          treasury_10y=7.8, treasury_2y=6.5, total_debt_to_gdp=230),
        ],
        "expected_winners": ["cash"],              # 1994 年现金是唯一安全资产
        "expected_losers": ["nominal_bond"],         # 债券大屠杀
        "fact": "格林斯潘 1994 年意外加息 6 次，10Y 从 5.6%→7.8%。债券史上最差年份之一。股市持平。新兴市场(墨西哥)崩盘。",
    },

    "2015Q3 中国股灾 + 人民币贬值": {
        "macro": MacroContext(
            gdp_growth_actual=2.0, gdp_growth_expected=2.5,
            cpi_actual=0.1, cpi_expected=2.0,
            fed_funds_rate=0.25,     # 还在零利率
            treasury_10y=2.2,
            treasury_2y=0.7,
            credit_growth=5.0,
            total_debt_to_gdp=330,
            vix=40,                  # 8月暴涨
            unemployment_rate=5.1,
            fiscal_deficit_to_gdp=2.5,
        ),
        "expected_winners": ["nominal_bond", "gold", "cash"],
        "expected_losers": ["equity_cyclical", "commodity"],
        "fact": "中国股市崩盘50%+人民币突然贬值→全球 risk-off。S&P500 8月跌11%。VIX飙到40。避险资产全涨。",
    },

    "2018Q4 美联储缩表 + 圣诞暴跌": {
        "macro": MacroContext(
            gdp_growth_actual=2.5, gdp_growth_expected=3.0,
            cpi_actual=2.2, cpi_expected=2.0,
            fed_funds_rate=2.5,     # 已加息 9 次
            treasury_10y=2.7,
            treasury_2y=2.7,        # 曲线接近倒挂
            credit_growth=4.0,
            total_debt_to_gdp=340,
            vix=25,
            unemployment_rate=3.7,
            fiscal_deficit_to_gdp=3.8,
        ),
        "trajectory": [
            MacroSnapshot(date="2018-Q2", gdp_growth=3.2, cpi=2.9, fed_funds_rate=2.0, credit_growth=5.0, unemployment_rate=3.8),
            MacroSnapshot(date="2018-Q3", gdp_growth=2.9, cpi=2.3, fed_funds_rate=2.25, credit_growth=4.5, unemployment_rate=3.7),
            MacroSnapshot(date="2018-Q4", gdp_growth=2.5, cpi=2.2, fed_funds_rate=2.5, credit_growth=4.0, unemployment_rate=3.7,
                          treasury_10y=2.7, treasury_2y=2.7, total_debt_to_gdp=340, vix=25),
        ],
        "expected_winners": ["nominal_bond", "cash", "gold"],
        "expected_losers": ["equity_cyclical"],
        "fact": "美联储连续加息+缩表(量化紧缩)。S&P500 Q4跌20%。鲍威尔2019.1月转向('耐心')后才止血。",
    },

    "日本 1990 泡沫破裂": {
        "macro": MacroContext(
            gdp_growth_actual=5.0, gdp_growth_expected=4.0,
            cpi_actual=3.1, cpi_expected=2.0,
            fed_funds_rate=6.0,     # 日银利率（用作代理）
            treasury_10y=7.0,
            treasury_2y=7.5,
            credit_growth=12.0,     # 信贷疯涨
            total_debt_to_gdp=400,
            household_debt_to_income=120,
            corporate_debt_to_gdp=150,  # 极高
            financial_sector_leverage=25,
            vix=None,
            unemployment_rate=2.1,   # 极低
            fiscal_deficit_to_gdp=0.5,
        ),
        "expected_winners": ["cash"],
        "expected_losers": ["equity_cyclical"],
        "fact": "日经 1989.12 触顶 38957，之后 30 年没回来。房地产泡沫 + 企业债泡沫 + 银行杠杆 → 长期通缩。泡沫破裂前一刻看起来经济很好。",
    },
}


def test_extra_historical():
    print(f"\n{'=' * 70}")
    print("  测试组 3: 更多历史时代")
    print("=" * 70)

    for name, scenario in EXTRA_HISTORICAL.items():
        macro = scenario["macro"]
        ew = scenario["expected_winners"]
        el = scenario["expected_losers"]

        # 用轨迹（如果有）
        if "trajectory" in scenario:
            macro = MacroContext.from_series(scenario["trajectory"])

        chain = evaluate(macro)
        g = _propagate_causal_graph(macro)

        ow = [t.asset_type for t in chain.active_tilts if t.direction == "overweight"]
        uw = [t.asset_type for t in chain.active_tilts if t.direction == "underweight"]

        w_hits = [w for w in ew if w in ow]
        l_hits = [l for l in el if l in uw]
        score = len(w_hits) + len(l_hits)
        total = len(ew) + len(el)

        ok = "✓" if score == total else " "
        print(f"\n  {ok} {name}: {score}/{total}")
        print(f"    象限: {chain.regime.quadrant}, 周期: {chain.regime.short_cycle_phase}")

        # 显示关键节点
        n = g.nodes
        print(f"    debt_service={n['debt_service_burden'].value:+.2f}"
              f" consumer={n['consumer_health'].value:+.2f}"
              f" corporate={n['corporate_health'].value:+.2f}"
              f" default={n['default_pressure'].value:+.2f}"
              f" policy={n['policy_response'].value:+.2f}")

        print(f"    超配: {ow}")
        print(f"    低配: {uw}")

        if score < total:
            miss_w = [w for w in ew if w not in ow]
            miss_l = [l for l in el if l not in uw]
            if miss_w: print(f"    ✗ 应超配但未: {miss_w}")
            if miss_l: print(f"    ✗ 应低配但未: {miss_l}")

        print(f"    事实: {scenario['fact'][:80]}...")

        if g.feedback_active:
            print(f"    反馈: {g.feedback_active[0]}")


# ══════════════════════════════════════════════════════════════
#  测试组 4: 因果引擎一致性
# ══════════════════════════════════════════════════════════════

def test_causal_consistency():
    """因果关系的方向一致性测试。"""
    print(f"\n{'=' * 70}")
    print("  测试组 4: 因果引擎一致性")
    print("=" * 70)

    # 测试: 加息应该让偿债负担加重
    base = MacroContext(
        gdp_growth_actual=2.0, cpi_actual=2.0,
        credit_growth=5.0, total_debt_to_gdp=300,
        unemployment_rate=5.0,
    )

    low_rate = MacroContext(**{**base.__dict__, "fed_funds_rate": 1.0})
    high_rate = MacroContext(**{**base.__dict__, "fed_funds_rate": 6.0})

    g_low = _propagate_causal_graph(low_rate)
    g_high = _propagate_causal_graph(high_rate)

    ds_low = g_low.nodes["debt_service_burden"].value
    ds_high = g_high.nodes["debt_service_burden"].value
    print(f"\n  利率 1% → 偿债负担 {ds_low:+.3f}")
    print(f"  利率 6% → 偿债负担 {ds_high:+.3f}")
    assert ds_high < ds_low, "高利率应增加偿债负担"
    print("  ✓ 高利率 → 偿债负担加重")

    # 测试: 高失业应该让消费者更差
    low_unemp = MacroContext(**{**base.__dict__, "fed_funds_rate": 3.0, "unemployment_rate": 3.5})
    high_unemp = MacroContext(**{**base.__dict__, "fed_funds_rate": 3.0, "unemployment_rate": 8.0})

    g_lu = _propagate_causal_graph(low_unemp)
    g_hu = _propagate_causal_graph(high_unemp)

    ch_low = g_lu.nodes["consumer_health"].value
    ch_high = g_hu.nodes["consumer_health"].value
    print(f"\n  失业 3.5% → 消费者 {ch_low:+.3f}")
    print(f"  失业 8.0% → 消费者 {ch_high:+.3f}")
    assert ch_low > ch_high, "低失业应让消费者更健康"
    print("  ✓ 高失业 → 消费者恶化")

    # 测试: 高通胀应该增加通胀压力
    low_cpi = MacroContext(**{**base.__dict__, "fed_funds_rate": 3.0, "cpi_actual": 1.0, "cpi_expected": 2.0})
    high_cpi = MacroContext(**{**base.__dict__, "fed_funds_rate": 3.0, "cpi_actual": 6.0, "cpi_expected": 2.0})

    g_lc = _propagate_causal_graph(low_cpi)
    g_hc = _propagate_causal_graph(high_cpi)

    ip_low = g_lc.nodes["inflation_pressure"].value
    ip_high = g_hc.nodes["inflation_pressure"].value
    print(f"\n  CPI 1.0% → 通胀压力 {ip_low:+.3f}")
    print(f"  CPI 6.0% → 通胀压力 {ip_high:+.3f}")
    assert ip_high > ip_low, "高CPI应增加通胀压力"
    print("  ✓ 高通胀 → 通胀压力增大")

    # 测试: 反馈循环 — 高违约应收紧信贷
    safe = MacroContext(
        gdp_growth_actual=3.0, cpi_actual=2.0, fed_funds_rate=2.0,
        credit_growth=8.0, total_debt_to_gdp=150, unemployment_rate=4.0,
    )
    dangerous = MacroContext(
        gdp_growth_actual=-3.0, cpi_actual=2.0, fed_funds_rate=5.0,
        credit_growth=-3.0, total_debt_to_gdp=350, unemployment_rate=8.0,
    )

    g_safe = _propagate_causal_graph(safe)
    g_danger = _propagate_causal_graph(dangerous)

    print(f"\n  安全环境: 违约压力 {g_safe.nodes['default_pressure'].value:+.3f}, 反馈 {len(g_safe.feedback_active)} 个")
    print(f"  危险环境: 违约压力 {g_danger.nodes['default_pressure'].value:+.3f}, 反馈 {len(g_danger.feedback_active)} 个")
    assert g_danger.nodes["default_pressure"].value > g_safe.nodes["default_pressure"].value
    print("  ✓ 危险环境 → 违约压力高 + 反馈循环活跃")


def main():
    test_debt_structure()
    test_debt_structure_2007_vs_2019()
    test_percentile_normalization()
    test_extra_historical()
    test_causal_consistency()

    print(f"\n{'=' * 70}")
    print("  全部完成")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
