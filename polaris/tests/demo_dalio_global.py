"""
达利欧因果链 — 全球宏观历史验证
================================

四个经济体（美国、日本、欧洲、中国）在关键年份的宏观数据回测。
检验链的判断是否符合历史事实。

数据来源: FRED, World Bank, BOJ, ECB, PBOC, BIS
标注 [est] 的为合理估计值。
"""

from polaris.chains.dalio import MacroContext, evaluate, format_dalio


GLOBAL_SCENARIOS = {

    # ══════════════════════════════════════════════════════════════════
    #  美国 (US)
    # ══════════════════════════════════════════════════════════════════

    "US 1973 石油危机": {
        "macro": MacroContext(
            gdp_growth_actual=5.6,
            gdp_growth_expected=4.0,
            cpi_actual=6.2,
            cpi_expected=3.5,
            fed_funds_rate=10.5,
            credit_growth=12.0,       # [est] 信贷仍在扩张
            total_debt_to_gdp=155,    # [est]
            unemployment_rate=4.9,
            snapshot_date="1973-12-01",
        ),
        "expected_quadrant": "growth_up_inflation_up",
        "expected_winners": ["commodity", "gold"],
        "expected_losers": ["nominal_bond", "equity_cyclical"],
        "fact": (
            "1973: 阿拉伯石油禁运，原油价格翻4倍。S&P500跌17%。"
            "黄金从65涨到112美元/oz。大宗商品暴涨。通胀急升。"
        ),
    },

    "US 1974 滞胀": {
        "macro": MacroContext(
            gdp_growth_actual=-0.5,
            gdp_growth_expected=3.0,
            cpi_actual=11.0,
            cpi_expected=4.0,
            fed_funds_rate=10.5,
            credit_growth=-2.0,
            total_debt_to_gdp=150,    # [est]
            unemployment_rate=7.2,
            snapshot_date="1974-12-01",
        ),
        "expected_quadrant": "growth_down_inflation_up",
        "expected_winners": ["commodity", "gold"],
        "expected_losers": ["equity_cyclical", "nominal_bond"],
        "fact": (
            "1974: 石油禁运余波，S&P500跌26%。10Y国债大亏。"
            "黄金从112涨到183$/oz。典型滞胀——增长崩溃+通胀失控。"
        ),
    },

    "US 1980 二次石油危机+沃尔克上台": {
        "macro": MacroContext(
            gdp_growth_actual=-0.3,
            gdp_growth_expected=2.5,
            cpi_actual=13.5,
            cpi_expected=8.0,
            fed_funds_rate=18.0,
            credit_growth=2.0,        # [est] 利率太高，信贷收缩
            total_debt_to_gdp=140,    # [est]
            unemployment_rate=7.2,
            snapshot_date="1980-12-01",
        ),
        "expected_quadrant": "growth_down_inflation_up",
        "expected_winners": ["gold", "commodity", "cash"],
        "expected_losers": ["nominal_bond", "equity_cyclical"],
        "fact": (
            "1980: 伊朗革命引发二次石油危机，CPI达13.5%。"
            "沃尔克将FFR推到20%。黄金从500飙到850$/oz（1月峰值）。"
            "S&P500全年涨26%（下半年反弹），但实际回报被通胀侵蚀。"
        ),
    },

    "US 1982 沃尔克紧缩末期": {
        "macro": MacroContext(
            gdp_growth_actual=-1.8,
            gdp_growth_expected=2.0,
            cpi_actual=6.2,
            cpi_expected=8.0,
            fed_funds_rate=12.0,
            credit_growth=-1.0,
            total_debt_to_gdp=130,    # [est]
            unemployment_rate=10.8,
            snapshot_date="1982-09-01",
        ),
        "expected_quadrant": "growth_down_inflation_down",
        "expected_winners": ["nominal_bond", "gold"],
        "expected_losers": ["equity_cyclical", "commodity"],
        "fact": (
            "1982: 沃尔克暴力加息到20%后开始降息，通胀从14%→6%。"
            "史上最大债券牛市起点。S&P500在8月触底后启动40年大牛市。"
        ),
    },

    "US 1987 黑色星期一": {
        "macro": MacroContext(
            gdp_growth_actual=3.5,
            gdp_growth_expected=3.0,
            cpi_actual=3.6,
            cpi_expected=3.0,
            fed_funds_rate=6.75,
            credit_growth=8.0,        # [est]
            total_debt_to_gdp=180,    # [est]
            unemployment_rate=6.2,
            snapshot_date="1987-10-19",
        ),
        "expected_quadrant": "growth_up_inflation_up",
        "expected_winners": ["nominal_bond", "cash"],
        "expected_losers": ["equity_cyclical"],
        "fact": (
            "1987.10.19: 道指单日暴跌22.6%（黑色星期一）。"
            "但经济基本面健康，美联储迅速注入流动性，衰退未发生。"
            "全年S&P500仍涨2%。国债因避险买盘上涨。"
        ),
    },

    "US 1990 储贷危机+海湾战争": {
        "macro": MacroContext(
            gdp_growth_actual=1.9,
            gdp_growth_expected=3.0,
            cpi_actual=5.4,
            cpi_expected=4.0,
            fed_funds_rate=8.0,
            credit_growth=2.0,        # [est] 储贷危机后信贷紧缩
            total_debt_to_gdp=190,    # [est]
            unemployment_rate=5.6,
            snapshot_date="1990-10-01",
        ),
        "expected_quadrant": "growth_down_inflation_up",
        "expected_winners": ["gold", "nominal_bond", "cash"],
        "expected_losers": ["equity_cyclical"],
        "fact": (
            "1990: 伊拉克入侵科威特→油价翻倍。储贷危机蔓延。"
            "S&P500跌6%。美联储年内降息5次。黄金小幅上涨。"
        ),
    },

    "US 1994 格林斯潘意外加息": {
        "macro": MacroContext(
            gdp_growth_actual=4.0,
            gdp_growth_expected=3.0,
            cpi_actual=2.6,
            cpi_expected=3.0,
            fed_funds_rate=5.5,       # 从3%加到5.5%
            credit_growth=5.0,        # [est]
            total_debt_to_gdp=185,    # [est]
            unemployment_rate=6.1,
            snapshot_date="1994-12-01",
        ),
        "expected_quadrant": "growth_up_inflation_down",
        "expected_winners": ["cash"],
        "expected_losers": ["nominal_bond"],
        "fact": (
            "1994: 格林斯潘意外加息300bp（3%→6%），债券大屠杀。"
            "长期国债跌超10%。S&P500持平（+1%）。墨西哥比索危机。"
            "橘郡因利率衍生品破产。这是'债券市场噩梦年'。"
        ),
    },

    "US 1997 互联网繁荣中期": {
        "macro": MacroContext(
            gdp_growth_actual=4.5,
            gdp_growth_expected=3.0,
            cpi_actual=2.3,
            cpi_expected=2.5,
            fed_funds_rate=5.5,
            credit_growth=8.0,        # [est]
            total_debt_to_gdp=180,    # [est]
            unemployment_rate=4.9,
            snapshot_date="1997-06-01",
        ),
        "expected_quadrant": "growth_up_inflation_down",
        "expected_winners": ["equity_cyclical", "nominal_bond"],
        "expected_losers": ["gold", "commodity"],
        "fact": (
            "1997: 互联网革命推动生产率爆炸。S&P500涨33%。"
            "黄金从380跌到290。亚洲金融危机冲击新兴市场。"
            "典型金发姑娘：增长强、通胀低、财政盈余。"
        ),
    },

    "US 2000 互联网泡沫见顶": {
        "macro": MacroContext(
            gdp_growth_actual=4.1,
            gdp_growth_expected=3.5,
            cpi_actual=3.4,
            cpi_expected=2.5,
            fed_funds_rate=6.5,       # 加到6.5%
            credit_growth=9.0,        # [est]
            total_debt_to_gdp=195,    # [est]
            unemployment_rate=4.0,
            snapshot_date="2000-03-10",
        ),
        "expected_quadrant": "growth_up_inflation_up",
        "expected_winners": ["cash", "commodity"],
        "expected_losers": ["equity_cyclical"],
        "fact": (
            "2000: 纳斯达克3月10日见顶5048点。FFR升至6.5%。"
            "纳斯达克全年跌39%。价值股跑赢成长股。大宗商品上涨。"
            "REITs全年涨26%——最佳避风港。"
        ),
    },

    "US 2001 互联网泡沫破裂+911": {
        "macro": MacroContext(
            gdp_growth_actual=1.0,
            gdp_growth_expected=3.5,
            cpi_actual=2.8,
            cpi_expected=2.5,
            fed_funds_rate=3.5,       # 格林斯潘已开始降息
            credit_growth=1.0,
            total_debt_to_gdp=190,    # [est]
            unemployment_rate=5.7,
            snapshot_date="2001-09-01",
        ),
        "expected_quadrant": "growth_down_inflation_up",
        "expected_winners": ["nominal_bond", "gold", "cash"],
        "expected_losers": ["equity_cyclical"],
        "fact": (
            "2001: 纳斯达克从5000跌到1100。911恐袭。S&P500跌13%。"
            "10Y收益率从6.5%降到5%（债券涨）。黄金底部回升260→280。"
        ),
    },

    "US 2003 伊拉克战争+复苏起点": {
        "macro": MacroContext(
            gdp_growth_actual=2.9,
            gdp_growth_expected=2.5,
            cpi_actual=2.3,
            cpi_expected=2.0,
            fed_funds_rate=1.0,       # 格林斯潘降到1%
            credit_growth=6.0,        # [est] 房贷开始加速
            total_debt_to_gdp=210,    # [est]
            unemployment_rate=6.0,
            snapshot_date="2003-06-01",
        ),
        "expected_quadrant": "growth_up_inflation_down",
        "expected_winners": ["equity_cyclical", "commodity", "gold"],
        "expected_losers": ["cash"],
        "fact": (
            "2003: 伊拉克战争3月开打。S&P500涨26%（从底部反弹）。"
            "黄金涨20%（美元走弱）。FFR只有1%→流动性泛滥开始。"
            "房地产牛市和次贷的种子在这里种下。"
        ),
    },

    "US 2007 次贷危机前夜": {
        "macro": MacroContext(
            gdp_growth_actual=2.0,
            gdp_growth_expected=2.5,
            cpi_actual=2.9,
            cpi_expected=2.0,
            fed_funds_rate=5.25,
            credit_growth=10.0,       # [est] 次贷全盛期
            total_debt_to_gdp=340,    # [est] 杠杆率高企
            unemployment_rate=4.6,
            snapshot_date="2007-06-01",
        ),
        "expected_quadrant": "growth_down_inflation_up",
        "expected_winners": ["commodity", "gold"],
        "expected_losers": ["nominal_bond", "equity_cyclical"],
        "fact": (
            "2007: 次贷危机2月开始浮现（汇丰减记）。"
            "S&P500涨3.5%（10月见顶1565）。原油涨57%。黄金涨31%。"
            "全年信用利差急剧扩大。8月BNP Paribas冻结基金。"
        ),
    },

    "US 2008Q4 雷曼倒闭": {
        "macro": MacroContext(
            gdp_growth_actual=-8.4,   # Q4年化
            gdp_growth_expected=2.0,
            cpi_actual=-0.4,          # 年底CPI转负
            cpi_expected=2.0,
            fed_funds_rate=0.25,
            credit_growth=-5.0,       # 信贷崩溃
            total_debt_to_gdp=350,
            unemployment_rate=7.2,    # 还在上升中
            snapshot_date="2008-12-01",
        ),
        "expected_quadrant": "growth_down_inflation_down",
        "expected_winners": ["nominal_bond", "gold", "cash"],
        "expected_losers": ["equity_cyclical", "commodity"],
        "fact": (
            "2008Q4: 雷曼倒闭→全球金融系统濒临崩溃。S&P500跌38%。"
            "长期国债涨33%。原油从147跌到32。VIX达89。"
            "美联储降到0+QE1启动。达利欧'丑陋去杠杆'标本。"
        ),
    },

    "US 2009 大衰退谷底+复苏": {
        "macro": MacroContext(
            gdp_growth_actual=-2.5,   # 全年
            gdp_growth_expected=0.0,
            cpi_actual=-0.3,
            cpi_expected=1.5,
            fed_funds_rate=0.25,
            credit_growth=-3.0,       # [est]
            total_debt_to_gdp=360,    # [est]
            unemployment_rate=10.0,   # 10月峰值
            snapshot_date="2009-03-09",
        ),
        "expected_quadrant": "growth_down_inflation_down",
        "expected_winners": ["equity_cyclical", "nominal_bond", "gold"],
        "expected_losers": ["cash"],
        "fact": (
            "2009.3.9: S&P500触底666点，之后开始11年大牛市。"
            "全年S&P500涨23%。黄金涨24%。长期国债小幅亏损。"
            "失业率10月达10%。ARRA刺激法案2月通过。"
        ),
    },

    "US 2013 Taper Tantrum": {
        "macro": MacroContext(
            gdp_growth_actual=1.8,
            gdp_growth_expected=2.5,
            cpi_actual=1.5,
            cpi_expected=2.0,
            fed_funds_rate=0.25,
            credit_growth=3.0,
            total_debt_to_gdp=330,
            unemployment_rate=7.3,
            snapshot_date="2013-09-01",
        ),
        "expected_quadrant": "growth_down_inflation_down",
        "expected_winners": ["equity_cyclical", "cash"],
        "expected_losers": ["nominal_bond", "gold"],
        "fact": (
            "2013: 伯南克暗示缩减QE→10Y从1.6%飙到2.7%（债券大亏）。"
            "S&P500涨30%（盈利+估值扩张）。黄金暴跌28%（1700→1200）。"
            "QE主导的'估值扩张牛市'——基本面弱但股票涨。"
        ),
    },

    "US 2015 美联储首次加息": {
        "macro": MacroContext(
            gdp_growth_actual=2.9,
            gdp_growth_expected=2.5,
            cpi_actual=0.1,
            cpi_expected=2.0,
            fed_funds_rate=0.5,       # 12月首次加息
            credit_growth=5.0,        # [est]
            total_debt_to_gdp=330,    # [est]
            unemployment_rate=5.0,
            snapshot_date="2015-12-15",
        ),
        "expected_quadrant": "growth_up_inflation_down",
        "expected_winners": ["equity_cyclical", "cash"],
        "expected_losers": ["commodity", "gold"],
        "fact": (
            "2015: 美联储12月首次加息（0→0.25%），结束7年零利率。"
            "S&P500持平（-0.7%）。原油暴跌35%。黄金跌10%。"
            "中国股灾+人民币贬值引发全球恐慌（8月）。"
        ),
    },

    "US 2018 美联储缩表+加息尾声": {
        "macro": MacroContext(
            gdp_growth_actual=2.9,
            gdp_growth_expected=2.5,
            cpi_actual=2.4,
            cpi_expected=2.0,
            fed_funds_rate=2.5,       # 12月加到2.5%
            credit_growth=4.0,        # [est]
            total_debt_to_gdp=335,    # [est]
            unemployment_rate=3.7,
            snapshot_date="2018-12-24",
        ),
        "expected_quadrant": "growth_up_inflation_up",
        "expected_winners": ["cash"],
        "expected_losers": ["equity_cyclical", "nominal_bond", "commodity"],
        "fact": (
            "2018: 美联储加息4次+缩表。12月S&P500跌20%（圣诞大屠杀）。"
            "全年S&P500跌6%。贸易战升级。全球几乎所有资产负回报。"
            "现金是唯一正回报资产——'万物皆跌'年。"
        ),
    },

    "US 2020Q2 COVID后无限QE": {
        "macro": MacroContext(
            gdp_growth_actual=-31.2,   # Q2年化
            gdp_growth_expected=2.0,
            cpi_actual=0.1,
            cpi_expected=2.0,
            fed_funds_rate=0.25,
            credit_growth=12.0,        # 政府信用扩张
            total_debt_to_gdp=380,
            unemployment_rate=13.0,
            snapshot_date="2020-06-01",
        ),
        "expected_quadrant": "growth_down_inflation_down",
        "expected_winners": ["nominal_bond", "gold", "equity_cyclical"],
        "expected_losers": ["commodity"],
        "fact": (
            "2020Q2: GDP年化跌31%。美联储无限QE+国会3万亿财政刺激。"
            "S&P500从3月低点反弹40%（V型反转）。黄金涨25%。"
            "原油一度跌到负值。政策空间充足→'漂亮去杠杆'。"
        ),
    },

    "US 2022Q3 暴力加息": {
        "macro": MacroContext(
            gdp_growth_actual=-0.6,   # 技术性衰退
            gdp_growth_expected=2.0,
            cpi_actual=8.3,
            cpi_expected=2.5,
            fed_funds_rate=3.25,
            credit_growth=11.0,
            total_debt_to_gdp=350,
            unemployment_rate=3.5,
            snapshot_date="2022-09-01",
        ),
        "expected_quadrant": "growth_down_inflation_up",
        "expected_winners": ["commodity", "cash"],
        "expected_losers": ["nominal_bond", "equity_cyclical"],
        "fact": (
            "2022: 40年来最严重通胀（CPI峰值9.1%）。S&P500跌19%。"
            "TLT跌31%（史上最惨）。60/40组合1937年以来最差。"
            "原油涨70%。现金是唯一避风港。"
        ),
    },

    # ══════════════════════════════════════════════════════════════════
    #  日本 (Japan)
    # ══════════════════════════════════════════════════════════════════

    "JP 1989 资产泡沫顶峰": {
        "macro": MacroContext(
            gdp_growth_actual=5.4,
            gdp_growth_expected=4.0,
            cpi_actual=2.3,
            cpi_expected=1.5,
            fed_funds_rate=4.25,      # BOJ贴现率（年中开始加息）
            credit_growth=12.0,       # [est] 疯狂信贷扩张
            total_debt_to_gdp=230,    # [est]
            unemployment_rate=2.3,
            snapshot_date="1989-12-29",
        ),
        "expected_quadrant": "growth_up_inflation_up",
        "expected_winners": ["equity_cyclical"],
        "expected_losers": ["cash"],
        "fact": (
            "1989.12.29: 日经指数触38957历史高点。地产/股市双泡沫。"
            "BOJ年中开始加息（2.5%→4.25%）。信贷增速超10%。"
            "泡沫最后的疯狂——之后日本进入失去的三十年。"
        ),
    },

    "JP 1990 泡沫破裂": {
        "macro": MacroContext(
            gdp_growth_actual=4.9,
            gdp_growth_expected=4.0,
            cpi_actual=3.1,
            cpi_expected=2.0,
            fed_funds_rate=6.0,       # BOJ贴现率加到6%
            credit_growth=8.0,        # [est] 开始收缩
            total_debt_to_gdp=235,    # [est]
            unemployment_rate=2.1,
            snapshot_date="1990-10-01",
        ),
        "expected_quadrant": "growth_up_inflation_up",
        "expected_winners": ["cash", "gold"],
        "expected_losers": ["equity_cyclical"],
        "fact": (
            "1990: BOJ连续加息到6%刺破泡沫。日经从38957跌到23849（-39%）。"
            "地产价格也开始下跌。但GDP还没反映（滞后）。"
            "现金和短债是最佳资产——利率高+资产暴跌。"
        ),
    },

    "JP 1995 失去的十年+阪神地震": {
        "macro": MacroContext(
            gdp_growth_actual=1.9,
            gdp_growth_expected=2.5,
            cpi_actual=-0.1,          # 通缩开始
            cpi_expected=1.0,
            fed_funds_rate=0.5,       # BOJ call rate
            credit_growth=0.5,        # [est] 银行坏账缠身
            total_debt_to_gdp=280,    # [est] 政府疯狂发债
            unemployment_rate=3.2,
            snapshot_date="1995-06-01",
        ),
        "expected_quadrant": "growth_down_inflation_down",
        "expected_winners": ["nominal_bond"],
        "expected_losers": ["equity_cyclical", "commodity"],
        "fact": (
            "1995: 阪神大地震+日元升值到80。通缩正式开始。"
            "日经全年跌0.7%。日本国债收益率暴跌（债券大牛市）。"
            "银行坏账问题恶化，政府开始大规模财政刺激。"
        ),
    },

    "JP 2001 量化宽松起点": {
        "macro": MacroContext(
            gdp_growth_actual=0.4,
            gdp_growth_expected=1.5,
            cpi_actual=-0.7,
            cpi_expected=0.0,
            fed_funds_rate=0.25,      # BOJ call rate→QE目标改为准备金
            credit_growth=-2.0,       # [est] 信贷持续收缩
            total_debt_to_gdp=310,    # [est] 政府债务已超GDP 150%
            unemployment_rate=5.0,
            snapshot_date="2001-03-19",
        ),
        "expected_quadrant": "growth_down_inflation_down",
        "expected_winners": ["nominal_bond", "cash"],
        "expected_losers": ["equity_cyclical"],
        "fact": (
            "2001.3.19: BOJ启动全球首个QE——目标从利率改为准备金量。"
            "日经跌24%。JGB收益率极低（1.3%）但继续下探。"
            "全球互联网泡沫破裂+日本结构性通缩。典型流动性陷阱。"
        ),
    },

    "JP 2013 安倍经济学": {
        "macro": MacroContext(
            gdp_growth_actual=2.0,
            gdp_growth_expected=1.0,
            cpi_actual=0.4,
            cpi_expected=0.0,         # 十几年通缩，没人信能通胀
            fed_funds_rate=0.1,       # BOJ call rate
            credit_growth=2.0,        # [est]
            total_debt_to_gdp=400,    # [est] 政府债务/GDP超200%
            unemployment_rate=4.0,
            snapshot_date="2013-04-04",
        ),
        "expected_quadrant": "growth_up_inflation_up",
        "expected_winners": ["equity_cyclical"],
        "expected_losers": ["nominal_bond", "cash"],
        "fact": (
            "2013.4.4: 黑田东彦启动'量化质化宽松'——每年购债60-70万亿日元。"
            "日经涨57%（全球最佳）。日元贬值22%。CPI从-0.1%→+1.6%。"
            "安倍三支箭：超级QE+财政刺激+结构改革。"
        ),
    },

    # ══════════════════════════════════════════════════════════════════
    #  欧洲 (Europe / ECB)
    # ══════════════════════════════════════════════════════════════════

    "EU 2010 欧债危机开始": {
        "macro": MacroContext(
            gdp_growth_actual=2.1,
            gdp_growth_expected=1.0,
            cpi_actual=1.6,
            cpi_expected=2.0,
            fed_funds_rate=1.0,       # ECB主要再融资利率
            credit_growth=1.0,        # [est] 银行去杠杆
            total_debt_to_gdp=280,    # [est] 欧元区整体
            unemployment_rate=10.1,
            snapshot_date="2010-05-01",
        ),
        "expected_quadrant": "growth_up_inflation_down",
        "expected_winners": ["nominal_bond", "gold"],
        "expected_losers": ["equity_cyclical"],
        "fact": (
            "2010.5: 希腊被迫求助→EU/IMF 1100亿欧元纾困。"
            "欧元区核心国经济复苏但PIIGS债务危机爆发。"
            "德国国债大涨（避险）。黄金涨30%。PIIGS国债暴跌。"
        ),
    },

    "EU 2012 德拉吉 Whatever It Takes": {
        "macro": MacroContext(
            gdp_growth_actual=-0.9,
            gdp_growth_expected=0.5,
            cpi_actual=2.5,
            cpi_expected=2.0,
            fed_funds_rate=0.75,      # ECB主要再融资利率
            credit_growth=-1.0,       # [est] 信贷紧缩
            total_debt_to_gdp=290,    # [est]
            unemployment_rate=11.4,
            snapshot_date="2012-07-26",
        ),
        "expected_quadrant": "growth_down_inflation_up",
        "expected_winners": ["nominal_bond", "gold"],
        "expected_losers": ["equity_cyclical"],
        "fact": (
            "2012.7.26: 德拉吉说'ECB will do whatever it takes'。"
            "PIIGS利差在讲话后暴跌（信心恢复）。全年欧股涨14%。"
            "西班牙10Y从7.5%→5%。欧元区险被拆散——口头干预救了欧元。"
        ),
    },

    "EU 2015 ECB启动QE": {
        "macro": MacroContext(
            gdp_growth_actual=2.0,
            gdp_growth_expected=1.0,
            cpi_actual=0.0,           # 通缩边缘
            cpi_expected=2.0,         # ECB目标
            fed_funds_rate=0.05,      # ECB主要再融资利率
            credit_growth=0.5,        # [est]
            total_debt_to_gdp=290,    # [est]
            unemployment_rate=10.9,
            snapshot_date="2015-03-09",
        ),
        "expected_quadrant": "growth_up_inflation_down",
        "expected_winners": ["equity_cyclical", "nominal_bond"],
        "expected_losers": ["cash"],
        "fact": (
            "2015.3.9: ECB正式启动QE——每月购买600亿欧元资产。"
            "欧洲斯托克600涨8%。德国10Y一度跌到0.05%。"
            "欧元对美元跌12%。希腊危机再爆但被遏制。"
        ),
    },

    "EU 2022 能源危机+暴力加息": {
        "macro": MacroContext(
            gdp_growth_actual=3.4,
            gdp_growth_expected=2.5,
            cpi_actual=8.4,
            cpi_expected=2.0,
            fed_funds_rate=2.5,       # ECB deposit rate年末
            credit_growth=6.0,        # [est]
            total_debt_to_gdp=290,    # [est]
            unemployment_rate=6.7,
            snapshot_date="2022-10-01",
        ),
        "expected_quadrant": "growth_up_inflation_up",
        "expected_winners": ["commodity", "cash"],
        "expected_losers": ["nominal_bond", "equity_cyclical"],
        "fact": (
            "2022: 俄乌战争→天然气价格暴涨10倍。CPI峰值10.6%（10月）。"
            "ECB从-0.5%暴力加息到2.5%。欧洲斯托克600跌13%。"
            "欧洲国债全面大跌。能源股是唯一赢家。"
        ),
    },

    # ══════════════════════════════════════════════════════════════════
    #  中国 (China)
    # ══════════════════════════════════════════════════════════════════

    "CN 2008 四万亿刺激": {
        "macro": MacroContext(
            gdp_growth_actual=9.7,
            gdp_growth_expected=10.0,
            cpi_actual=5.9,
            cpi_expected=3.0,
            fed_funds_rate=5.31,      # PBOC 1年期贷款基准利率年初→年末降到5.31%
            credit_growth=19.0,       # [est] 信贷爆发
            total_debt_to_gdp=150,    # [est]
            unemployment_rate=4.2,    # 官方
            snapshot_date="2008-11-09",
        ),
        "expected_quadrant": "growth_down_inflation_up",
        "expected_winners": ["commodity", "gold"],
        "expected_losers": ["equity_cyclical"],
        "fact": (
            "2008.11.9: 国务院宣布4万亿刺激计划。沪指全年跌65%。"
            "GDP从峰值15%跌到6%（Q4）。但信贷暴增14.6万亿。"
            "四万亿种下了地方债务和产能过剩的种子。"
        ),
    },

    "CN 2015 股灾+汇改": {
        "macro": MacroContext(
            gdp_growth_actual=6.9,
            gdp_growth_expected=7.0,
            cpi_actual=1.4,
            cpi_expected=3.0,
            fed_funds_rate=4.35,      # PBOC 1年期贷款基准利率
            credit_growth=15.0,       # [est] 影子银行膨胀
            total_debt_to_gdp=250,    # [est]
            unemployment_rate=4.1,    # 官方
            snapshot_date="2015-08-11",
        ),
        "expected_quadrant": "growth_down_inflation_down",
        "expected_winners": ["nominal_bond", "cash"],
        "expected_losers": ["equity_cyclical"],
        "fact": (
            "2015.6-8: 沪指从5178暴跌到2927（-43%）。杠杆牛爆破。"
            "8.11汇改——人民币一次性贬值1.86%引发全球恐慌。"
            "国家队入场救市。外汇储备消耗近万亿美元。"
        ),
    },

    "CN 2020 COVID冲击+快速恢复": {
        "macro": MacroContext(
            gdp_growth_actual=2.2,
            gdp_growth_expected=6.0,
            cpi_actual=2.5,           # 全年平均（猪周期推高食品）
            cpi_expected=3.0,
            fed_funds_rate=3.85,      # PBOC LPR 1年
            credit_growth=13.0,       # [est] 社融大幅扩张
            total_debt_to_gdp=280,    # [est]
            unemployment_rate=5.2,    # 调查失业率
            snapshot_date="2020-03-01",
        ),
        "expected_quadrant": "growth_down_inflation_down",
        "expected_winners": ["nominal_bond", "equity_cyclical"],
        "expected_losers": ["commodity"],
        "fact": (
            "2020: 中国最先遭受COVID冲击，Q1 GDP跌6.8%（建国以来首次负增长）。"
            "但V型复苏全球最快。沪深300全年涨27%。"
            "中国国债全年涨（利率从3.1%降到2.6%）。出口Q3后强劲反弹。"
        ),
    },

    "CN 2022 房地产危机+动态清零": {
        "macro": MacroContext(
            gdp_growth_actual=3.0,
            gdp_growth_expected=5.5,
            cpi_actual=2.0,
            cpi_expected=3.0,
            fed_funds_rate=3.65,      # PBOC LPR 1年
            credit_growth=10.0,       # [est] 但房贷断崖下跌
            total_debt_to_gdp=300,    # [est] 私人部门超250%
            unemployment_rate=5.5,    # 调查（青年失业率超20%未公布）
            snapshot_date="2022-10-01",
        ),
        "expected_quadrant": "growth_down_inflation_down",
        "expected_winners": ["cash", "nominal_bond"],
        "expected_losers": ["equity_cyclical", "commodity"],
        "fact": (
            "2022: 恒大/碧桂园等房企债务违约。动态清零重创经济。"
            "沪深300跌22%。恒生指数跌15%。中国国债小幅上涨。"
            "青年失业率超20%。人民币跌破7.2。外资加速撤离。"
        ),
    },
}


# 国别历史百分位（近似值，基于 1980-2020 长期数据）
# 用于百分位归一化，让因果引擎适应不同国家的"正常"水平
COUNTRY_CONTEXT = {
    "US": dict(hist_rate_median=3.0, hist_rate_p25=1.0, hist_rate_p75=5.5,
               hist_unemployment_median=5.5, hist_gdp_median=2.5,
               hist_credit_growth_median=5.0, hist_cpi_median=2.5),
    "JP": dict(hist_rate_median=1.0, hist_rate_p25=0.1, hist_rate_p75=3.0,
               hist_unemployment_median=3.5, hist_gdp_median=1.5,
               hist_credit_growth_median=2.0, hist_cpi_median=0.5),
    "EU": dict(hist_rate_median=1.5, hist_rate_p25=0.0, hist_rate_p75=3.5,
               hist_unemployment_median=9.0, hist_gdp_median=1.5,
               hist_credit_growth_median=4.0, hist_cpi_median=2.0),
    "CN": dict(hist_rate_median=5.0, hist_rate_p25=4.0, hist_rate_p75=6.0,
               hist_unemployment_median=4.0, hist_gdp_median=7.0,
               hist_credit_growth_median=14.0, hist_cpi_median=2.5),
}


def _inject_country_context(macro, name: str):
    """给 MacroContext 注入国别历史百分位 + country profile。"""
    prefix = name[:2]
    ctx = COUNTRY_CONTEXT.get(prefix)
    if ctx:
        for k, v in ctx.items():
            setattr(macro, k, v)
    # 设置 country code 让因果引擎加载对应 profile
    macro.country = prefix


def main():
    total_score = 0
    total_possible = 0
    results = []

    for name, scenario in GLOBAL_SCENARIOS.items():
        macro = scenario["macro"]
        _inject_country_context(macro, name)
        expected_w = scenario["expected_winners"]
        expected_l = scenario["expected_losers"]
        expected_q = scenario["expected_quadrant"]
        fact = scenario["fact"]

        chain = evaluate(macro)

        print(f"\n{'=' * 70}")
        print(f"  {name}  ({macro.snapshot_date})")
        print(f"{'=' * 70}")

        # 象限判断
        actual_q = chain.regime.quadrant if chain.regime else "N/A"
        q_match = "O" if actual_q == expected_q else "X"
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
        total_score += score
        total_possible += total
        print(f"  得分: {score}/{total}")

        # 尾部风险
        if chain.tail_risk and chain.tail_risk.risks:
            print(f"\n  尾部风险 ({chain.tail_risk.severity}):")
            for r in chain.tail_risk.risks:
                print(f"    ! {r}")

        # 政策路径
        if chain.policy_path and chain.policy_path.likely_tools:
            print(f"\n  政策工具: {', '.join(chain.policy_path.likely_tools)}")

        # 对冲
        if chain.hedge_specs:
            print(f"\n  对冲: {len(chain.hedge_specs)} 个保护情景")

        # 历史事实
        print(f"\n  -- 历史事实 --")
        for line in fact.split("。"):
            line = line.strip()
            if line:
                print(f"    {line}。")

        results.append((name, score, total, q_match))

    # ── 总分统计 ──
    print(f"\n\n{'=' * 70}")
    print(f"  总分统计 ({len(GLOBAL_SCENARIOS)} 个全球宏观场景)")
    print(f"{'=' * 70}")

    # 按经济体分组统计
    groups = {"US": [], "JP": [], "EU": [], "CN": []}
    for name, score, total, q_ok in results:
        prefix = name[:2]
        if prefix in groups:
            groups[prefix].append((name, score, total, q_ok))

    for region, items in groups.items():
        region_score = sum(s for _, s, _, _ in items)
        region_total = sum(t for _, _, t, _ in items)
        region_name = {"US": "美国", "JP": "日本", "EU": "欧洲", "CN": "中国"}[region]
        print(f"\n  --- {region_name} ---")
        for name, score, total, q_ok in items:
            marker = "O" if q_ok == "O" else "X"
            print(f"  {marker} {name}: {score}/{total}")
        if region_total > 0:
            print(f"  小计: {region_score}/{region_total} ({region_score/region_total:.0%})")

    print(f"\n  {'=' * 40}")
    pct = total_score / total_possible if total_possible > 0 else 0
    print(f"  总计: {total_score}/{total_possible} ({pct:.0%})")

    q_correct = sum(1 for _, _, _, q in results if q == "O")
    print(f"  象限命中: {q_correct}/{len(results)} ({q_correct/len(results):.0%})")


if __name__ == "__main__":
    main()
