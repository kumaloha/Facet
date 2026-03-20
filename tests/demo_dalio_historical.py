"""
达利欧因果链 — 历史时代验证
============================

用真实宏观数据回测，检验链的判断是否符合历史事实。
每个时代附带"正确答案"，对比链输出。
"""

from polaris.chains.dalio import MacroContext, evaluate, format_dalio


HISTORICAL_SCENARIOS = {
    # ── 1970s 滞胀 ──────────────────────────────────────────────
    "1974 石油危机滞胀": {
        "macro": MacroContext(
            gdp_growth_actual=-0.5,
            gdp_growth_expected=3.0,
            cpi_actual=11.0,
            cpi_expected=4.0,
            fed_funds_rate=10.5,
            treasury_10y=7.5,
            treasury_2y=8.0,
            credit_growth=-2.0,
            total_debt_to_gdp=150,
            vix=None,  # VIX 1993 年才有
            unemployment_rate=7.2,
            fiscal_deficit_to_gdp=0.4,
            snapshot_date="1974-12-01",
        ),
        "expected_quadrant": "growth_down_inflation_up",
        "expected_winners": ["commodity", "gold"],
        "expected_losers": ["equity_cyclical", "nominal_bond"],
        "historical_fact": (
            "1974: 石油禁运→原油翻4倍，S&P500 跌26%，10Y国债大亏。"
            "黄金从42$/oz涨到183$/oz。大宗商品暴涨。"
            "保罗·沃尔克还没上台，通胀失控。"
        ),
    },

    # ── 1982 沃尔克紧缩末期 ──────────────────────────────────────
    "1982 沃尔克紧缩末期": {
        "macro": MacroContext(
            gdp_growth_actual=-1.8,
            gdp_growth_expected=2.0,
            cpi_actual=6.2,
            cpi_expected=8.0,
            fed_funds_rate=12.0,
            treasury_10y=13.0,
            treasury_2y=12.5,
            credit_growth=-1.0,
            total_debt_to_gdp=130,
            unemployment_rate=10.8,
            fiscal_deficit_to_gdp=4.0,
            snapshot_date="1982-09-01",
        ),
        "expected_quadrant": "growth_down_inflation_down",
        "expected_winners": ["nominal_bond", "gold"],
        "expected_losers": ["equity_cyclical", "commodity"],
        "historical_fact": (
            "1982Q3: 沃尔克暴力加息到20%后开始降息，通胀从14%回落到6%。"
            "这是史上最大的债券牛市起点。"
            "S&P500在8月触底后开始40年大牛市。"
            "正确操作: 重仓长期国债（1982-2020长债年化11%），开始左侧布局股票。"
        ),
    },

    # ── 1995-1999 克林顿繁荣 ────────────────────────────────────
    "1997 互联网繁荣中期": {
        "macro": MacroContext(
            gdp_growth_actual=4.5,
            gdp_growth_expected=3.0,
            cpi_actual=2.3,
            cpi_expected=2.5,
            fed_funds_rate=5.5,
            treasury_10y=6.3,
            treasury_2y=5.9,
            credit_growth=8.0,
            total_debt_to_gdp=180,
            unemployment_rate=4.9,
            fiscal_deficit_to_gdp=0.3,  # 克林顿时期实现了财政盈余
            snapshot_date="1997-06-01",
        ),
        "expected_quadrant": "growth_up_inflation_down",
        "expected_winners": ["equity_cyclical", "nominal_bond"],
        "expected_losers": ["gold", "commodity"],
        "historical_fact": (
            "1997: 互联网革命推动生产率爆炸式增长，通胀压力低。"
            "S&P500 涨33%，纳斯达克涨22%。"
            "黄金从380跌到290。大宗商品因亚洲金融危机暴跌。"
            "典型金发姑娘: 增长强劲、通胀温和、财政盈余。"
        ),
    },

    # ── 2000 互联网泡沫破裂 ──────────────────────────────────────
    "2001 互联网泡沫破裂": {
        "macro": MacroContext(
            gdp_growth_actual=1.0,
            gdp_growth_expected=3.5,
            cpi_actual=2.8,
            cpi_expected=2.5,
            fed_funds_rate=3.5,   # 格林斯潘已开始降息
            treasury_10y=5.0,
            treasury_2y=3.8,
            credit_growth=1.0,
            total_debt_to_gdp=190,
            vix=32,
            unemployment_rate=5.7,
            fiscal_deficit_to_gdp=1.3,
            snapshot_date="2001-09-01",
        ),
        "expected_quadrant": "growth_down_inflation_up",
        "expected_winners": ["nominal_bond", "gold", "cash"],
        "expected_losers": ["equity_cyclical"],
        "historical_fact": (
            "2001: 纳斯达克从5000跌到1100，科技股崩盘。"
            "S&P500 跌13%。10Y国债收益率从6.5%降到5%（债券涨）。"
            "黄金开始从底部回升（260→280）。"
            "降息力度大但信贷意愿弱——企业在去杠杆。"
        ),
    },

    # ── 2008 金融危机 ────────────────────────────────────────────
    "2008Q4 雷曼倒闭后": {
        "macro": MacroContext(
            gdp_growth_actual=-8.4,   # 2008Q4 年化 GDP
            gdp_growth_expected=2.0,
            cpi_actual=-0.4,          # 2008年底 CPI 转负
            cpi_expected=2.0,
            fed_funds_rate=0.25,      # 已降到零
            treasury_10y=2.2,
            treasury_2y=0.8,
            credit_growth=-5.0,       # 信贷崩溃
            total_debt_to_gdp=350,
            vix=60,                   # VIX 历史极值
            unemployment_rate=7.2,    # 还在上升中
            fiscal_deficit_to_gdp=10.0,  # TARP 等救市
            snapshot_date="2008-12-01",
        ),
        "expected_quadrant": "growth_down_inflation_down",
        "expected_winners": ["nominal_bond", "gold", "cash"],
        "expected_losers": ["equity_cyclical", "commodity"],
        "historical_fact": (
            "2008Q4: 雷曼倒闭后全球金融系统濒临崩溃。"
            "S&P500 跌38%。长期国债涨33%（30Y Treasury）。"
            "黄金涨5%（避险），原油从147跌到32（需求崩溃）。"
            "VIX达89。美联储利率降到0，启动QE。"
            "典型通缩去杠杆: 达利欧的'丑陋去杠杆'标本。"
        ),
    },

    # ── 2013 QE 退出恐慌 ────────────────────────────────────────
    "2013 Taper Tantrum": {
        "macro": MacroContext(
            gdp_growth_actual=1.8,
            gdp_growth_expected=2.5,
            cpi_actual=1.5,
            cpi_expected=2.0,
            fed_funds_rate=0.25,
            treasury_10y=2.7,       # 从1.6%急升到2.7%
            treasury_2y=0.4,
            credit_growth=3.0,
            total_debt_to_gdp=330,
            vix=17,
            unemployment_rate=7.3,
            fiscal_deficit_to_gdp=4.1,
            snapshot_date="2013-09-01",
        ),
        "expected_quadrant": "growth_down_inflation_down",
        "expected_winners": ["equity_cyclical", "cash"],
        "expected_losers": ["nominal_bond", "gold"],
        "historical_fact": (
            "2013: 伯南克暗示缩减QE，10Y收益率从1.6%暴涨到2.7%（债券大亏）。"
            "但S&P500 全年涨30%（企业盈利强+估值扩张）。"
            "黄金暴跌28%（1700→1200），因通缩担忧→通胀不来。"
            "链可能会出错: 宏观看起来弱，但股票靠QE估值扩张。"
        ),
    },

    # ── 2022 美联储暴力加息 ──────────────────────────────────────
    "2022Q3 暴力加息进行中": {
        "macro": MacroContext(
            gdp_growth_actual=-0.6,   # 技术性衰退
            gdp_growth_expected=2.0,
            cpi_actual=8.3,
            cpi_expected=2.5,
            fed_funds_rate=3.25,      # 正在快速加息
            treasury_10y=3.7,
            treasury_2y=4.2,          # 倒挂
            credit_growth=11.0,       # 信贷仍在扩张（滞后）
            total_debt_to_gdp=350,
            vix=27,
            sp500_earnings_yield=5.2,
            unemployment_rate=3.5,    # 劳动力市场极紧
            fiscal_deficit_to_gdp=5.5,
            snapshot_date="2022-09-01",
        ),
        "expected_quadrant": "growth_down_inflation_up",
        "expected_winners": ["commodity", "gold", "cash"],
        "expected_losers": ["nominal_bond", "equity_cyclical"],
        "historical_fact": (
            "2022: 40年来最严重的通胀（CPI 9.1%峰值）。"
            "S&P500 跌19%，纳斯达克跌33%。"
            "长期国债 TLT 跌31%（史上最惨）。"
            "60/40组合遭遇1937年以来最差表现。"
            "原油涨70%+，黄金持平。现金是唯一避风港。"
        ),
    },

    # ── 2020Q2 COVID 崩盘后 QE 无限 ──────────────────────────────
    "2020Q2 COVID后无限QE": {
        "macro": MacroContext(
            gdp_growth_actual=-31.2,   # 2020Q2 年化 GDP
            gdp_growth_expected=2.0,
            cpi_actual=0.1,
            cpi_expected=2.0,
            fed_funds_rate=0.25,
            treasury_10y=0.7,
            treasury_2y=0.15,
            credit_growth=12.0,        # 政府信用扩张
            total_debt_to_gdp=380,
            vix=34,
            unemployment_rate=13.0,
            fiscal_deficit_to_gdp=16.0,  # 天量财政刺激
            snapshot_date="2020-06-01",
        ),
        "expected_quadrant": "growth_down_inflation_down",
        "expected_winners": ["nominal_bond", "gold", "equity_cyclical"],
        "expected_losers": ["commodity"],
        "historical_fact": (
            "2020Q2: GDP 年化跌31%，但美联储无限QE+国会3万亿财政刺激。"
            "S&P500 从3月低点反弹40%（V型反转）。"
            "黄金涨25%（从1500到1900）。长期国债涨。"
            "原油一度跌到负值，后来V型反弹。"
            "关键: 政策空间充足→'漂亮去杠杆'→风险资产快速修复。"
        ),
    },
}


def main():
    for name, scenario in HISTORICAL_SCENARIOS.items():
        macro = scenario["macro"]
        expected_q = scenario["expected_quadrant"]
        expected_w = scenario["expected_winners"]
        expected_l = scenario["expected_losers"]
        fact = scenario["historical_fact"]

        chain = evaluate(macro)

        print(f"\n{'=' * 70}")
        print(f"  {name}  ({macro.snapshot_date})")
        print(f"{'=' * 70}")

        # 象限判断
        actual_q = chain.regime.quadrant if chain.regime else "N/A"
        q_match = "✓" if actual_q == expected_q else "✗"
        print(f"\n  象限: {actual_q}  {q_match}  (预期: {expected_q})")

        if chain.regime:
            print(f"  短期周期: {chain.regime.short_cycle_phase}")
            print(f"  长期周期: {chain.regime.long_cycle_phase}")
            print(f"  置信度: {chain.regime.confidence:.0%}")

        # 押注检验
        ow = [t.asset_type for t in chain.active_tilts if t.direction == "overweight"]
        uw = [t.asset_type for t in chain.active_tilts if t.direction == "underweight"]

        print(f"\n  超配: {', '.join(ow) if ow else '无'}")
        print(f"  低配: {', '.join(uw) if uw else '无'}")

        # 对比预期
        w_hits = [w for w in expected_w if w in ow]
        w_miss = [w for w in expected_w if w not in ow]
        l_hits = [l for l in expected_l if l in uw]
        l_miss = [l for l in expected_l if l not in uw]

        print(f"\n  赢家命中: {w_hits if w_hits else '无'}  未命中: {w_miss if w_miss else '无'}")
        print(f"  输家命中: {l_hits if l_hits else '无'}  未命中: {l_miss if l_miss else '无'}")

        score = len(w_hits) + len(l_hits)
        total = len(expected_w) + len(expected_l)
        print(f"  得分: {score}/{total}")

        # 尾部风险
        if chain.tail_risk and chain.tail_risk.risks:
            print(f"\n  尾部风险 ({chain.tail_risk.severity}):")
            for r in chain.tail_risk.risks:
                print(f"    ⚠ {r}")

        # 政策路径
        if chain.policy_path and chain.policy_path.likely_tools:
            print(f"\n  政策工具: {', '.join(chain.policy_path.likely_tools)}")

        # 对冲
        if chain.hedge_specs:
            print(f"\n  对冲: {len(chain.hedge_specs)} 个保护情景")

        # 历史事实
        print(f"\n  ── 历史事实 ──")
        for line in fact.split("。"):
            line = line.strip()
            if line:
                print(f"    {line}。")

    # 总分统计
    print(f"\n\n{'=' * 70}")
    print(f"  总分统计")
    print(f"{'=' * 70}")
    total_score = 0
    total_possible = 0
    for name, scenario in HISTORICAL_SCENARIOS.items():
        macro = scenario["macro"]
        chain = evaluate(macro)
        ow = [t.asset_type for t in chain.active_tilts if t.direction == "overweight"]
        uw = [t.asset_type for t in chain.active_tilts if t.direction == "underweight"]
        expected_w = scenario["expected_winners"]
        expected_l = scenario["expected_losers"]
        hits = len([w for w in expected_w if w in ow]) + len([l for l in expected_l if l in uw])
        possible = len(expected_w) + len(expected_l)
        total_score += hits
        total_possible += possible
        actual_q = chain.regime.quadrant if chain.regime else "N/A"
        q_ok = "✓" if actual_q == scenario["expected_quadrant"] else "✗"
        print(f"  {q_ok} {name}: {hits}/{possible}")

    print(f"\n  总计: {total_score}/{total_possible} ({total_score/total_possible:.0%})")


if __name__ == "__main__":
    main()
