# Axion 特征目录

> 维护所有特征的定义、计算逻辑、数据来源和优先级。
> 新增特征先在此表登记，确认后再实现代码。

---

## L0 公司域 — 截面特征（单期快照）

### 盈利能力

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.company.gross_margin` | (revenue - cost_of_revenue) / revenue | financial_line_items | P0 | 已实现 |
| `l0.company.net_margin` | net_income / revenue | financial_line_items | P0 | 已实现 |
| `l0.company.operating_margin` | operating_income / revenue | financial_line_items | P0 | 已实现 |
| `l0.company.roic` | NOPAT / invested_capital | financial_line_items | P1 | 待实现 |
| `l0.company.roe` | net_income / shareholders_equity | financial_line_items | P1 | 待实现 |
| `l0.company.roe_without_leverage` | net_income / total_assets，不含杠杆的资产回报率。巴菲特偏好低负债下的高 ROE，此指标剥离杠杆后看真实盈利能力 | financial_line_items | P0 | 待实现 |
| `l0.company.free_cash_flow_margin` | (operating_cash_flow - capex) / revenue | financial_line_items | P0 | 待实现 |
| `l0.company.sga_to_revenue` | sga_expense / revenue，费用控制能力 | financial_line_items | P1 | 待实现 |
| `l0.company.rnd_to_revenue` | rnd_expense / revenue，研发投入强度 | financial_line_items | P1 | 待实现 |
| `l0.company.incremental_roic` | Δoperating_income / Δinvested_capital（跨期），新投入资本的回报率。巴菲特核心指标——公司每多花一块钱能多赚多少，比静态 ROIC 更重要 | financial_line_items 跨期 | P0 | 待实现 |

### 所有者盈余（Owner Earnings）

> 巴菲特最重要的概念：股东真正能拿走的钱 = 净利润 + 折旧摊销 - 维持性资本开支。
> 与 FCF 的区别：FCF 包含增长性 CapEx，Owner Earnings 只扣维持性 CapEx（用 D&A 近似）。

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.company.owner_earnings` | net_income + depreciation_amortization - capex，所有者盈余绝对值（百万） | financial_line_items | P0 | 待实现 |
| `l0.company.owner_earnings_margin` | owner_earnings / revenue，所有者盈余率 | financial_line_items | P0 | 待实现 |
| `l0.company.owner_earnings_to_net_income` | owner_earnings / net_income，>1 说明折旧大于 capex（轻资本），<1 说明 capex 吞噬利润 | financial_line_items | P0 | 待实现 |

### 资本轻重（CapEx Intensity）

> 巴菲特偏好"不需要持续大量投入就能维持盈利"的生意。
> See's Candies 之所以好：赚的钱几乎不需要再投回去。

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.company.capex_to_revenue` | capex / revenue，资本开支强度。<5% 轻资本，>15% 重资本 | financial_line_items | P0 | 待实现 |
| `l0.company.capex_to_ocf` | capex / operating_cash_flow，经营现金流中被 capex 吃掉的比例。>80% 说明赚的钱都要投回去 | financial_line_items | P0 | 待实现 |
| `l0.company.depreciation_to_capex` | depreciation / capex，>1 说明在消化存量资产，<1 说明在加速扩张 | financial_line_items | P1 | 待实现 |

### 现金流质量

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.company.ocf_to_net_income` | operating_cash_flow / net_income，利润真实性 | financial_line_items | P0 | 已实现 |
| `l0.company.ocf_growth_vs_ni_growth` | OCF 增速 - 净利润增速，背离=危险信号 | financial_line_items 跨期 | P0 | 待实现 |

### 盈利质量（Earnings Quality）

> 巴菲特反复强调：利润可以操纵，现金流不能。
> 应计项异常、应收膨胀、存货积压是三大造假/恶化信号。

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.company.accruals_ratio` | (net_income - operating_cash_flow) / total_assets，应计项比率。高应计 = 利润中现金含量低 = 质量差 | financial_line_items | P0 | 待实现 |
| `l0.company.receivables_growth_vs_revenue` | 应收增速 - 收入增速（跨期），>0 说明收入靠赊账堆出来的 | financial_line_items 跨期 | P0 | 待实现 |
| `l0.company.inventory_growth_vs_revenue` | 存货增速 - 收入增速（跨期），>0 说明产品卖不动在积压 | financial_line_items 跨期 | P0 | 待实现 |
| `l0.company.goodwill_to_assets` | goodwill / total_assets，商誉占比。高商誉 = 过去高价收购多 = 减值风险 | financial_line_items | P1 | 待实现 |

### 资本配置（Capital Allocation）

> 管理层拿到利润后怎么花——回购、分红、收购还是囤现金？
> 巴菲特说：判断管理层质量，看他有钱时怎么花。

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.company.dividend_payout_ratio` | dividends_paid / net_income，分红慷慨度 | financial_line_items | P1 | 待实现 |
| `l0.company.buyback_to_net_income` | share_repurchase / net_income，回购力度 | financial_line_items | P1 | 待实现 |
| `l0.company.shareholder_yield` | (dividends + buybacks) / net_income，股东综合回报率 | financial_line_items | P0 | 待实现 |
| `l0.company.retention_rate` | 1 - dividend_payout_ratio，留存比例 | financial_line_items | P2 | 待实现 |
| `l0.company.retained_earnings_roic` | 留存利润再投资回报 = revenue_growth / retention_rate（粗估），巴菲特看留存利润是否创造了对应价值 | financial_line_items 跨期 | P1 | 待实现 |

### 收入质量

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.company.recurring_revenue_pct` | is_recurring=True 的 segment 数量占比 | downstream_segments | P1 | 已实现 |
| `l0.company.top_customer_concentration` | 最大客户 revenue_pct | downstream_segments | P0 | 已实现 |
| `l0.company.top3_customer_concentration` | 前 3 大客户 revenue_pct 之和 | downstream_segments | P1 | 待实现 |
| `l0.company.revenue_type_diversity` | 不同 revenue_type 的数量，类型越多越分散 | downstream_segments | P2 | 待实现 |
| `l0.company.backlog_coverage` | sum(backlog) / total_revenue，积压订单覆盖率 | downstream_segments | P0 | 待实现 |

### 供应链

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.company.sole_source_pct` | is_sole_source=True 的供应商占比 | upstream_segments | P0 | 已实现 |
| `l0.company.supplier_geo_concentration` | 供应商最集中地区的占比（按 geographic_location 分组） | upstream_segments | P0 | 待实现 |
| `l0.company.purchase_obligation_to_revenue` | sum(purchase_obligation) / revenue，预付义务负担 | upstream_segments + financial_line_items | P1 | 待实现 |
| `l0.company.long_lead_time_pct` | lead_time 含 "12 months" 或 "exceeding" 的供应商占比 | upstream_segments | P1 | 待实现 |

### 地域

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.company.top_region_concentration` | 最大地区 revenue_share | geographic_revenues | P0 | 已实现 |
| `l0.company.geo_diversity` | 1 - HHI(revenue_share)，地域多样性 | geographic_revenues | P1 | 待实现 |
| `l0.company.china_revenue_share` | region 含 "China" / "中国" 的 revenue_share 之和 | geographic_revenues | P0 | 待实现 |

### 资本结构与偿债能力

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.company.total_debt_count` | 债务工具数量 | debt_obligations | P2 | 已实现 |
| `l0.company.total_debt_principal` | sum(principal)，总债务规模（百万） | debt_obligations | P0 | 待实现 |
| `l0.company.current_debt_pct` | is_current=True 的本金 / 总本金，短期债务占比 | debt_obligations | P0 | 待实现 |
| `l0.company.weighted_avg_interest_rate` | 加权平均利率 = sum(principal × rate) / sum(principal) | debt_obligations | P1 | 待实现 |
| `l0.company.debt_maturity_within_1y` | maturity_date < 1 年后的本金合计 | debt_obligations | P1 | 待实现 |
| `l0.company.debt_to_equity` | total_debt / shareholders_equity，杠杆率。巴菲特喜欢 <0.5 的公司 | debt_obligations + financial_line_items | P0 | 待实现 |
| `l0.company.debt_to_owner_earnings` | total_debt / owner_earnings，用所有者盈余还清债务需要几年。巴菲特标准 <3 年 | debt_obligations + financial_line_items | P0 | 待实现 |
| `l0.company.interest_coverage` | operating_income / interest_expense，利息保障倍数。<3 危险，>10 非常安全 | financial_line_items | P0 | 待实现 |
| `l0.company.current_ratio` | current_assets / current_liabilities，流动比率 | financial_line_items | P1 | 待实现 |

### 管理层

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.company.ceo_pay_ratio` | CEO 的 pay_ratio 字段 | executive_compensations | P1 | 已实现 |
| `l0.company.exec_stock_award_pct` | 高管平均 stock_awards / total_comp，激励对齐度 | executive_compensations | P1 | 待实现 |
| `l0.company.exec_total_comp_to_net_income` | 管理层总薪酬 / net_income | executive_compensations + financial_line_items | P2 | 待实现 |
| `l0.company.mgmt_ownership_pct` | 管理层合计 percent_of_class | stock_ownership | P0 | 待实现 |
| `l0.company.top5_ownership_concentration` | 前 5 大持股人 percent_of_class 之和 | stock_ownership | P1 | 待实现 |
| `l0.company.narrative_count` | 管理层叙事/承诺总数 | company_narratives | P2 | 待实现 |
| `l0.company.narrative_fulfillment_rate` | status=delivered / (delivered + missed + abandoned) | company_narratives | P0 | 待实现 |

### 风险

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.company.litigation_count` | status in (pending, ongoing) 的诉讼数 | litigations | P0 | 已实现 |
| `l0.company.litigation_accrued_total` | sum(accrued_amount)，已计提诉讼金额（百万） | litigations | P1 | 待实现 |
| `l0.company.litigation_claimed_to_accrued` | sum(claimed_amount) / sum(accrued_amount)，计提充分度 | litigations | P2 | 待实现 |
| `l0.company.related_party_tx_count` | 关联交易笔数 | related_party_transactions | P1 | 待实现 |
| `l0.company.related_party_ongoing_count` | is_ongoing=True 的持续性关联交易数 | related_party_transactions | P1 | 待实现 |
| `l0.company.related_party_amount_to_revenue` | sum(amount) / revenue，关联交易占收入比 | related_party_transactions + financial_line_items | P0 | 待实现 |

### 经营

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.company.operational_issue_count` | 经营议题数量 | operational_issues | P1 | 已实现 |
| `l0.company.risk_issue_pct` | risk 字段非空的议题占比，风险密度 | operational_issues | P1 | 待实现 |
| `l0.company.guidance_issue_pct` | guidance 字段非空的议题占比，前瞻覆盖度 | operational_issues | P2 | 待实现 |

### 债务周期健康度（Dalio）

> 达利欧认为债务周期是驱动经济的核心引擎。
> 公司层面，关键是：债务增速是否超过收入增速？利息负担是否在吞噬现金流？
> 当债务增长快于偿债能力时，就处于周期晚期，脆弱性极高。

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.company.debt_service_burden` | interest_expense / operating_cash_flow，偿债负担率。>0.3 说明经营现金流大量用于还息，达利欧视为高风险 | financial_line_items | P0 | 待实现 |
| `l0.company.net_debt_to_ebitda` | (total_debt - cash) / EBITDA，净负债倍数。达利欧的杠杆核心指标，>4x 进入危险区 | financial_line_items + debt_obligations | P0 | 待实现 |
| `l0.company.cash_to_short_term_debt` | cash / current_debt，现金缓冲比。<1 说明短期债务无法用现金覆盖，流动性危机风险 | financial_line_items + debt_obligations | P0 | 待实现 |
| `l0.company.debt_growth_vs_revenue_growth` | 总债务增速 - 收入增速（跨期），>0 说明借钱速度超过赚钱速度，达利欧的经典预警信号 | financial_line_items + debt_obligations 跨期 | P0 | 待实现 |
| `l0.company.debt_growth_vs_ocf_growth` | 总债务增速 - 经营现金流增速（跨期），>0 说明债务扩张快于现金流产出，偿债能力恶化 | financial_line_items + debt_obligations 跨期 | P0 | 待实现 |
| `l0.company.interest_to_revenue` | interest_expense / revenue，利息成本占收入比。达利欧关注利息负担何时开始挤压利润空间 | financial_line_items | P1 | 待实现 |

### 反身性与杠杆动态（Soros）

> 索罗斯的反身性理论：市场认知影响基本面，基本面又反过来影响认知，形成正反馈循环。
> 公司层面体现为：股价上涨 → 融资能力增强 → 扩张加速 → 业绩改善 → 股价继续涨（泡沫）。
> 反之：股价下跌 → 融资困难 → 收缩 → 业绩恶化 → 继续跌（崩溃）。
> 关键是识别这种自我强化循环的方向和强度。

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.company.leverage_acceleration` | 债务增速的变化率（二阶导），d(debt_growth)/dt。>0 说明杠杆在加速扩张，索罗斯关注加速阶段的不稳定性 | financial_line_items + debt_obligations 多期 | P0 | 待实现 |
| `l0.company.equity_issuance_to_capex` | 股权融资额 / capex，依赖股权融资扩张的程度。高比例说明公司扩张依赖市场情绪（反身性通道） | financial_line_items | P1 | 待实现 |
| `l0.company.debt_issuance_to_capex` | 新增借款 / capex，依赖债务融资扩张的程度。高比例 + 利率上升 = 反身性反转风险 | financial_line_items + debt_obligations | P1 | 待实现 |
| `l0.company.goodwill_growth_vs_revenue_growth` | 商誉增速 - 收入增速（跨期），>0 说明在高价收购但收入没跟上，泡沫期的典型特征 | financial_line_items 跨期 | P1 | 待实现 |
| `l0.company.share_dilution_rate` | (shares_t - shares_t-1) / shares_t-1，股份稀释率。持续稀释说明公司在用股权"货币"融资，依赖股价维持扩张 | financial_line_items 跨期 | P0 | 待实现 |
| `l0.company.financing_dependency` | (股权融资 + 新增借款) / operating_cash_flow，外部融资依赖度。>1 说明公司自身造血不够，完全靠外部输血，极度脆弱 | financial_line_items + debt_obligations | P0 | 待实现 |

---

## L0 公司域 — 趋势特征（跨期变化）

> 需要 ComputeContext 支持查前一期数据。

### 增长与变化

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.company.revenue_growth_yoy` | (revenue_t - revenue_t-1) / revenue_t-1 | financial_line_items 跨期 | P0 | 待实现 |
| `l0.company.gross_margin_delta` | gross_margin_t - gross_margin_t-1 | financial_line_items 跨期 | P0 | 待实现 |
| `l0.company.net_margin_delta` | net_margin_t - net_margin_t-1 | financial_line_items 跨期 | P0 | 待实现 |
| `l0.company.ocf_growth_yoy` | (OCF_t - OCF_t-1) / OCF_t-1 | financial_line_items 跨期 | P0 | 待实现 |
| `l0.company.owner_earnings_growth_yoy` | 所有者盈余增速 | financial_line_items 跨期 | P0 | 待实现 |
| `l0.company.china_revenue_share_delta` | 中国收入占比变化 | geographic_revenues 跨期 | P1 | 待实现 |
| `l0.company.sole_source_pct_delta` | 独家供应占比变化 | upstream_segments 跨期 | P2 | 待实现 |
| `l0.company.top_customer_concentration_delta` | 最大客户占比变化 | downstream_segments 跨期 | P1 | 待实现 |

### 稳定性与可预测性（Predictability）

> 巴菲特核心原则：他只买"能看懂未来十年"的公司。
> 不是利润高就行，要稳。波动大=不可预测=不值得买。
> 需要 3 期以上数据才有意义。

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.company.gross_margin_stability` | 近 N 期毛利率的标准差，越小越好。稳定的毛利率 = 定价权 | financial_line_items 多期 | P0 | 待实现 |
| `l0.company.net_margin_stability` | 近 N 期净利率的标准差 | financial_line_items 多期 | P0 | 待实现 |
| `l0.company.revenue_growth_stability` | 近 N 期收入增速的标准差，低波动 = 可预测 | financial_line_items 多期 | P1 | 待实现 |
| `l0.company.ocf_margin_stability` | 近 N 期经营现金流率的标准差 | financial_line_items 多期 | P1 | 待实现 |
| `l0.company.consecutive_margin_expansion` | 毛利率连续扩张的期数。连续 3 期以上 = 定价权在增强 | financial_line_items 多期 | P0 | 待实现 |
| `l0.company.consecutive_revenue_growth` | 收入连续正增长的期数 | financial_line_items 多期 | P1 | 待实现 |
| `l0.company.consecutive_positive_fcf` | 自由现金流连续为正的期数。巴菲特不买烧钱公司 | financial_line_items 多期 | P0 | 待实现 |

---

## L0 其他域

### 行业域

> 待 Anchor Industry 域管线。当前可从多家公司的 Company 域数据间接推导。

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.industry.avg_gross_margin` | 同行业公司的平均毛利率 | 跨公司 financial_line_items | P2 | 待实现 |
| `l0.industry.avg_revenue_growth` | 同行业公司的平均收入增速 | 跨公司 financial_line_items | P2 | 待实现 |

### 周期域 — 基础

> 来源：外部市场数据

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.cycle.fed_funds_rate` | 联邦基金利率 | 外部 API | P2 | 待实现 |
| `l0.cycle.yield_curve_slope` | 10Y - 2Y 国债收益率差 | 外部 API | P2 | 待实现 |
| `l0.cycle.credit_spread` | 高收益债利差 | 外部 API | P2 | 待实现 |

### 周期域 — 债务周期位置（Dalio）

> 达利欧的框架：经济由短期债务周期（5-8 年）和长期债务周期（50-75 年）驱动。
> 判断当前处于周期的哪个位置，决定应该进攻还是防守。
> 关键信号：实际利率、信贷增速、货币供应、债务/收入比。

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.cycle.real_interest_rate` | fed_funds_rate - CPI_yoy，实际利率。<0 = 宽松刺激期，>2% = 紧缩抑制期 | 外部 API | P1 | 待实现 |
| `l0.cycle.m2_growth_yoy` | M2 货币供应量同比增速。达利欧：印钞是长期债务周期末期的标志性动作 | 外部 API | P1 | 待实现 |
| `l0.cycle.corporate_debt_to_gdp` | 企业部门总债务 / GDP，企业杠杆率。历史高位 = 周期晚期 | 外部 API | P1 | 待实现 |
| `l0.cycle.credit_growth_vs_gdp` | 信贷增速 - 名义 GDP 增速。>0 = 信贷驱动（泡沫积累），<0 = 去杠杆 | 外部 API | P1 | 待实现 |
| `l0.cycle.cpi_yoy` | CPI 同比，通胀水平。达利欧：通胀是决定央行行为的核心变量 | 外部 API | P1 | 待实现 |
| `l0.cycle.dollar_index_yoy` | 美元指数同比变化。达利欧高度关注货币贬值作为债务周期末期信号 | 外部 API | P2 | 待实现 |

### 周期域 — 市场情绪与反身性（Soros）

> 索罗斯：市场不是被动反映现实，而是主动塑造现实。
> 当市场共识过于一致时，就是反转的前兆。
> 关键是捕捉"认知与现实的偏差"以及偏差的自我强化/修正。

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.cycle.vix_level` | VIX 恐慌指数，市场隐含波动率。极低 = 过度自满（索罗斯：不稳定性在稳定中积累） | 外部 API | P1 | 待实现 |
| `l0.cycle.vix_percentile` | 当前 VIX 在近 2 年分布中的百分位，标准化后更有信号意义 | 外部 API | P1 | 待实现 |
| `l0.cycle.margin_debt_growth` | 保证金贷款余额同比增速。加速增长 = 市场杠杆堆积，索罗斯的泡沫指标 | 外部 API | P1 | 待实现 |
| `l0.cycle.put_call_ratio` | 看跌/看涨期权比率。极端值 = 市场情绪一边倒，反身性反转信号 | 外部 API | P2 | 待实现 |
| `l0.cycle.credit_spread_momentum` | 信用利差的 3 个月变化方向。利差走阔 = 风险厌恶上升，市场从贪婪转向恐惧 | 外部 API | P1 | 待实现 |
| `l0.cycle.yield_curve_slope_momentum` | 收益率曲线斜率的 3 个月变化。从倒挂到恢复常态 = 衰退即将兑现 | 外部 API | P2 | 待实现 |

### 政府域

> 待 Anchor Policy 域管线

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.gov.directive_count` | 相关政策指令数量 | PolicyDirective.sectors_json 匹配行业 | P2 | 待实现 |
| `l0.gov.promote_vs_restrict_ratio` | promote 类指令 / restrict 类指令 | PolicyDirective.force_direction | P2 | 待实现 |

### 技术域

> 待 Anchor Technology 域管线

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l0.tech.patent_count` | 公司持有的活跃专利数 | PatentRight.legal_status=active | P2 | 待实现 |
| `l0.tech.patent_commercial_count` | 专利商业化事件数 | PatentCommercial | P2 | 待实现 |

---

## L1 传导特征（跨域组合）

### 技术 → 公司

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l1.tech_to_company.patent_barrier` | 公司专利数 × 商业化率 | l0.tech.patent_count + l0.tech.patent_commercial_count | P2 | 待实现 |

### 行业 → 公司

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l1.industry_to_company.margin_vs_peers` | 公司毛利率 - 行业平均毛利率 | l0.company.gross_margin - l0.industry.avg_gross_margin | P2 | 待实现 |
| `l1.industry_to_company.growth_vs_peers` | 公司收入增速 - 行业平均增速 | l0.company.revenue_growth_yoy - l0.industry.avg_revenue_growth | P2 | 待实现 |

### 政府 → 公司

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l1.gov_to_company.export_control_risk` | 受限地区收入占比 × 管制严格度 | l0.company.china_revenue_share × 管制指数 | P0 | 待实现 |

### 周期 → 资本 → 公司

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l1.cycle_to_company.funding_stress` | (current_debt_pct × debt_principal) × f(利率环境) | l0.company + l0.cycle | P2 | 待实现 |

### 地缘 → 公司

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l1.geo_to_company.supply_chain_geo_risk` | 供应商地理集中度 × 该地区地缘紧张度 | l0.company.supplier_geo_concentration × 地缘指数 | P0 | 待实现 |
| `l1.geo_to_company.revenue_geo_risk` | 高风险地区收入占比 × 地缘紧张度 | l0.company.china_revenue_share × 地缘指数 | P1 | 待实现 |

### 周期 → 公司：债务周期脆弱性（Dalio）

> 达利欧：同样的公司，在周期不同阶段表现完全不同。
> 高杠杆公司在宽松期如鱼得水，在紧缩期率先崩溃。
> 必须将公司的财务状况放在宏观周期的背景下评估。

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l1.cycle_to_company.rate_sensitivity` | debt_service_burden × real_interest_rate，利率敏感度。高负债 + 高实际利率 = 极度脆弱 | l0.company.debt_service_burden × l0.cycle.real_interest_rate | P0 | 待实现 |
| `l1.cycle_to_company.credit_cycle_exposure` | net_debt_to_ebitda × credit_spread，信用周期暴露度。高杠杆 + 利差走阔 = 再融资困难 | l0.company.net_debt_to_ebitda × l0.cycle.credit_spread | P0 | 待实现 |
| `l1.cycle_to_company.deleveraging_pressure` | debt_growth_vs_revenue_growth + credit_growth_vs_gdp，公司去杠杆压力。两者都 >0 = 公司和宏观都在加杠杆（达利欧的泡沫信号） | l0.company + l0.cycle | P1 | 待实现 |
| `l1.cycle_to_company.liquidity_stress` | (1 - cash_to_short_term_debt) × real_interest_rate，流动性压力。现金不足 + 利率高 = 短期偿债危机 | l0.company.cash_to_short_term_debt × l0.cycle | P1 | 待实现 |

### 周期 → 公司：反身性暴露度（Soros）

> 索罗斯：你必须判断一家公司是反身性循环的受益者还是受害者。
> 在牛市泡沫期靠融资扩张的公司，一旦情绪反转，跌得最惨。
> 关键是识别哪些公司的基本面"依赖"市场情绪维持。

| 特征名 | 计算逻辑 | 数据来源 | 优先级 | 状态 |
|--------|---------|---------|--------|------|
| `l1.cycle_to_company.reflexivity_exposure` | financing_dependency × (1 / vix_level)，反身性暴露度。高融资依赖 + 低波动（自满期） = 一旦情绪反转，受冲击最大 | l0.company.financing_dependency × l0.cycle.vix_level | P0 | 待实现 |
| `l1.cycle_to_company.leverage_cycle_risk` | leverage_acceleration × margin_debt_growth，杠杆周期风险。公司加杠杆 + 市场加杠杆 = 系统性泡沫中的高危个体 | l0.company.leverage_acceleration × l0.cycle.margin_debt_growth | P0 | 待实现 |
| `l1.cycle_to_company.bubble_fragility` | share_dilution_rate × (100 - vix_percentile)，泡沫脆弱度。持续稀释股份 + 市场极度自满 = 最脆弱的反身性受益者 | l0.company.share_dilution_rate × l0.cycle.vix_percentile | P1 | 待实现 |

---

## 统计

| 层级 | 已实现 | 待实现 | 合计 |
|------|--------|--------|------|
| L0 截面（盈利） | 3 | 7 | 10 |
| L0 截面（所有者盈余） | 0 | 3 | 3 |
| L0 截面（资本轻重） | 0 | 3 | 3 |
| L0 截面（现金流） | 1 | 1 | 2 |
| L0 截面（盈利质量） | 0 | 4 | 4 |
| L0 截面（资本配置） | 0 | 5 | 5 |
| L0 截面（收入质量） | 2 | 3 | 5 |
| L0 截面（供应链） | 1 | 3 | 4 |
| L0 截面（地域） | 1 | 2 | 3 |
| L0 截面（资本结构） | 1 | 8 | 9 |
| L0 截面（管理层） | 1 | 6 | 7 |
| L0 截面（风险） | 1 | 5 | 6 |
| L0 截面（经营） | 1 | 2 | 3 |
| L0 截面（债务周期·Dalio） | 0 | 6 | 6 |
| L0 截面（反身性·Soros） | 0 | 6 | 6 |
| L0 趋势（增长） | 0 | 8 | 8 |
| L0 趋势（稳定性） | 0 | 7 | 7 |
| L0 其他域（行业/政府/技术） | 0 | 7 | 7 |
| L0 周期域（基础） | 0 | 3 | 3 |
| L0 周期域（债务周期·Dalio） | 0 | 6 | 6 |
| L0 周期域（市场情绪·Soros） | 0 | 6 | 6 |
| L1 传导（原有） | 0 | 6 | 6 |
| L1 传导（债务周期·Dalio） | 0 | 4 | 4 |
| L1 传导（反身性·Soros） | 0 | 3 | 3 |
| **合计** | **12** | **117** | **129** |

---

## 附录 A. 原始数据需求 — financial_line_items

> 反推所有特征所需的 `item_key`。
> Anchor 的 `financial_line_items` 是长表结构（每行一个科目），`item_key` 是科目标识。
> 🔴 = 当前 DB 中未出现，需要 Anchor 补充提取。

### A.1 必需 item_key 清单

| item_key | 中文 | 依赖此字段的特征 | DB 状态 |
|----------|------|-----------------|---------|
| `revenue` | 营业收入 | gross_margin, net_margin, operating_margin, free_cash_flow_margin, sga_to_revenue, rnd_to_revenue, capex_to_revenue, owner_earnings_margin, interest_to_revenue, revenue_growth_yoy, revenue_growth_stability, consecutive_revenue_growth, receivables_growth_vs_revenue, inventory_growth_vs_revenue, related_party_amount_to_revenue, purchase_obligation_to_revenue, debt_growth_vs_revenue_growth, goodwill_growth_vs_revenue_growth | 🔴 |
| `cost_of_revenue` | 营业成本 | gross_margin | 🔴 |
| `net_income` | 净利润 | net_margin, roe, roe_without_leverage, owner_earnings, owner_earnings_margin, owner_earnings_to_net_income, ocf_to_net_income, accruals_ratio, dividend_payout_ratio, buyback_to_net_income, shareholder_yield, exec_total_comp_to_net_income, net_margin_delta | ✅ |
| `operating_income` | 营业利润 | operating_margin, roic, incremental_roic, interest_coverage | ✅ |
| `operating_cash_flow` | 经营活动现金流 | free_cash_flow_margin, ocf_to_net_income, ocf_growth_vs_ni_growth, capex_to_ocf, accruals_ratio, debt_service_burden, financing_dependency, ocf_growth_yoy, ocf_margin_stability, debt_growth_vs_ocf_growth, consecutive_positive_fcf | 🔴 |
| `capital_expenditures` | 资本开支 | free_cash_flow_margin, owner_earnings, capex_to_revenue, capex_to_ocf, depreciation_to_capex, equity_issuance_to_capex, debt_issuance_to_capex, consecutive_positive_fcf | 🔴 |
| `depreciation_amortization` | 折旧摊销 | owner_earnings, depreciation_to_capex, EBITDA 计算 | 🔴 |
| `shareholders_equity` | 股东权益 | roe, debt_to_equity | 🔴 |
| `total_assets` | 总资产 | roe_without_leverage, accruals_ratio, goodwill_to_assets | 🔴 |
| `interest_expense` | 利息费用 | interest_coverage, debt_service_burden, interest_to_revenue | 🔴 |
| `current_assets` | 流动资产 | current_ratio | 🔴 |
| `current_liabilities` | 流动负债 | current_ratio | 🔴 |
| `goodwill` | 商誉 | goodwill_to_assets, goodwill_growth_vs_revenue_growth | 🔴 |
| `accounts_receivable` | 应收账款 | receivables_growth_vs_revenue | 🔴 |
| `inventory` | 存货 | inventory_growth_vs_revenue | 🔴 |
| `cash_and_equivalents` | 现金及等价物 | net_debt_to_ebitda, cash_to_short_term_debt | 🔴 |
| `dividends_paid` | 已付股利 | dividend_payout_ratio, shareholder_yield | 🔴 |
| `share_repurchase` | 股票回购 | buyback_to_net_income, shareholder_yield | 🔴 |
| `sga_expense` | 销售及管理费用 | sga_to_revenue | 🔴 |
| `rnd_expense` | 研发费用 | rnd_to_revenue | 🔴 |
| `total_debt` | 总有息负债 | debt_to_equity, debt_to_owner_earnings, net_debt_to_ebitda, debt_growth_vs_revenue_growth, debt_growth_vs_ocf_growth, leverage_acceleration | ✅ |
| `proceeds_from_stock_issuance` | 股权融资收入 | equity_issuance_to_capex, financing_dependency | 🔴 |
| `proceeds_from_debt_issuance` | 新增借款 | debt_issuance_to_capex, financing_dependency | 🔴 |
| `basic_weighted_average_shares` | 基本加权平均股数 | share_dilution_rate | ✅ |

### A.2 计算公式（需多个 item_key 组合）

> 部分特征依赖中间量，以下列出中间量的计算方式。

| 中间量 | 公式 | 用途 |
|--------|------|------|
| **EBITDA** | `operating_income + depreciation_amortization` | net_debt_to_ebitda |
| **NOPAT** | `operating_income × (1 - tax_rate)`，tax_rate 近似 = `income_tax_expense_total / income_before_tax_total` | roic |
| **invested_capital** | `total_assets - current_liabilities + current_debt`（或 `shareholders_equity + total_debt - cash_and_equivalents`） | roic, incremental_roic |
| **owner_earnings** | `net_income + depreciation_amortization - capital_expenditures` | owner_earnings 系列特征 |
| **free_cash_flow** | `operating_cash_flow - capital_expenditures` | free_cash_flow_margin, consecutive_positive_fcf |
| **current_debt** | `SUM(principal) WHERE is_current=True` from debt_obligations | cash_to_short_term_debt, current_debt_pct |
| **total_debt_principal** | `SUM(principal)` from debt_obligations | 可与 item_key `total_debt` 交叉验证 |

---

## 附录 B. 原始数据需求 — Anchor 其他表

> 各表的字段使用情况。列出每张表中被特征计算实际引用的字段。

### B.1 debt_obligations

| 字段 | 类型 | 依赖此字段的特征 |
|------|------|-----------------|
| `principal` | FLOAT | total_debt_principal, current_debt_pct, weighted_avg_interest_rate, debt_maturity_within_1y, debt_to_equity, debt_to_owner_earnings, net_debt_to_ebitda, cash_to_short_term_debt, debt_growth_vs_revenue_growth, debt_growth_vs_ocf_growth, leverage_acceleration, funding_stress |
| `interest_rate` | FLOAT | weighted_avg_interest_rate |
| `maturity_date` | DATE | debt_maturity_within_1y |
| `is_current` | BOOLEAN | current_debt_pct, cash_to_short_term_debt, funding_stress |

### B.2 downstream_segments

| 字段 | 类型 | 依赖此字段的特征 |
|------|------|-----------------|
| `revenue_pct` | FLOAT | top_customer_concentration, top3_customer_concentration, top_customer_concentration_delta |
| `is_recurring` | BOOLEAN | recurring_revenue_pct |
| `revenue_type` | VARCHAR | revenue_type_diversity |
| `backlog` | FLOAT | backlog_coverage |

### B.3 upstream_segments

| 字段 | 类型 | 依赖此字段的特征 |
|------|------|-----------------|
| `is_sole_source` | BOOLEAN | sole_source_pct, sole_source_pct_delta |
| `geographic_location` | VARCHAR | supplier_geo_concentration, supply_chain_geo_risk |
| `purchase_obligation` | FLOAT | purchase_obligation_to_revenue |
| `lead_time` | VARCHAR | long_lead_time_pct |

### B.4 geographic_revenues

| 字段 | 类型 | 依赖此字段的特征 |
|------|------|-----------------|
| `region` | VARCHAR | top_region_concentration, china_revenue_share, geo_diversity |
| `revenue_share` | FLOAT | top_region_concentration, china_revenue_share, geo_diversity, china_revenue_share_delta, revenue_geo_risk |

### B.5 executive_compensations

| 字段 | 类型 | 依赖此字段的特征 |
|------|------|-----------------|
| `pay_ratio` | FLOAT | ceo_pay_ratio |
| `stock_awards` | FLOAT | exec_stock_award_pct |
| `total_comp` | FLOAT | exec_stock_award_pct, exec_total_comp_to_net_income |

### B.6 stock_ownership

| 字段 | 类型 | 依赖此字段的特征 |
|------|------|-----------------|
| `title` | VARCHAR | mgmt_ownership_pct（用于判断是否为管理层） |
| `percent_of_class` | FLOAT | mgmt_ownership_pct, top5_ownership_concentration |

### B.7 company_narratives

| 字段 | 类型 | 依赖此字段的特征 |
|------|------|-----------------|
| `status` | VARCHAR | narrative_count, narrative_fulfillment_rate（delivered / missed / abandoned） |

### B.8 litigations

| 字段 | 类型 | 依赖此字段的特征 |
|------|------|-----------------|
| `status` | VARCHAR | litigation_count（pending / ongoing） |
| `accrued_amount` | FLOAT | litigation_accrued_total, litigation_claimed_to_accrued |
| `claimed_amount` | FLOAT | litigation_claimed_to_accrued |

### B.9 related_party_transactions

| 字段 | 类型 | 依赖此字段的特征 |
|------|------|-----------------|
| `amount` | FLOAT | related_party_amount_to_revenue |
| `is_ongoing` | BOOLEAN | related_party_ongoing_count |

### B.10 operational_issues

| 字段 | 类型 | 依赖此字段的特征 |
|------|------|-----------------|
| `risk` | VARCHAR | risk_issue_pct（非空占比） |
| `guidance` | VARCHAR | guidance_issue_pct（非空占比） |

---

## 附录 C. 原始数据需求 — 外部市场数据

> 周期域特征需要外部 API 提供的宏观/市场数据。
> 候选数据源：FRED API（美联储数据）、Yahoo Finance、Alpha Vantage。

### C.1 宏观经济数据（Dalio 债务周期）

| 数据字段 | 含义 | FRED Series ID | 依赖此字段的特征 |
|----------|------|---------------|-----------------|
| `fed_funds_rate` | 联邦基金有效利率 | `DFF` | fed_funds_rate, real_interest_rate |
| `cpi_yoy` | CPI 同比 | `CPIAUCSL`（需计算 YoY） | cpi_yoy, real_interest_rate |
| `m2_money_supply` | M2 货币供应量 | `M2SL`（需计算 YoY） | m2_growth_yoy |
| `corporate_debt_total` | 非金融企业总债务 | `BCNSDODNS` | corporate_debt_to_gdp |
| `nominal_gdp` | 名义 GDP | `GDP` | corporate_debt_to_gdp, credit_growth_vs_gdp |
| `total_credit` | 全社会信贷总额 | `TCMDO`（需计算 YoY） | credit_growth_vs_gdp |
| `dollar_index` | 美元指数 | `DTWEXBGS`（需计算 YoY） | dollar_index_yoy |
| `treasury_10y` | 10 年期国债收益率 | `DGS10` | yield_curve_slope |
| `treasury_2y` | 2 年期国债收益率 | `DGS2` | yield_curve_slope |
| `high_yield_spread` | 高收益债利差 | `BAMLH0A0HYM2` | credit_spread, credit_spread_momentum |

### C.2 市场情绪数据（Soros 反身性）

| 数据字段 | 含义 | 数据源 | 依赖此字段的特征 |
|----------|------|--------|-----------------|
| `vix` | VIX 恐慌指数 | CBOE / Yahoo Finance `^VIX` | vix_level, vix_percentile, reflexivity_exposure, bubble_fragility |
| `margin_debt` | 保证金贷款余额 | FINRA（月度发布） | margin_debt_growth, leverage_cycle_risk |
| `put_call_ratio` | 看跌/看涨期权比率 | CBOE | put_call_ratio |

### C.3 其他域外部数据

| 数据字段 | 含义 | 数据源 | 依赖此字段的特征 |
|----------|------|--------|-----------------|
| `policy_directive_sectors` | 政策指令涉及行业 | Anchor PolicyDirective.sectors_json | directive_count, promote_vs_restrict_ratio |
| `policy_force_direction` | 政策力量方向 | Anchor PolicyDirective.force_direction | promote_vs_restrict_ratio |
| `patent_legal_status` | 专利法律状态 | Anchor PatentRight.legal_status | patent_count |
| `patent_commercial_events` | 专利商业化事件 | Anchor PatentCommercial | patent_commercial_count |
| `geopolitical_tension_index` | 地缘紧张度指数 | GPR Index / 自建 | supply_chain_geo_risk, revenue_geo_risk |
| `export_control_strictness` | 出口管制严格度 | BIS Entity List / 自建 | export_control_risk |

---

## 附录 D. 特征 → 原始数据 完整依赖矩阵

> 每个特征需要的全部原始字段和完整计算步骤。
> fli = financial_line_items, do = debt_obligations, 其他表用全名。

### D.1 盈利能力

| 特征 | 原始字段 | 完整计算 |
|------|---------|---------|
| `gross_margin` | fli: `revenue`, `cost_of_revenue` | `(revenue - cost_of_revenue) / revenue` |
| `net_margin` | fli: `net_income`, `revenue` | `net_income / revenue` |
| `operating_margin` | fli: `operating_income`, `revenue` | `operating_income / revenue` |
| `roic` | fli: `operating_income`, `income_tax_expense_total`, `income_before_tax_total`, `total_assets`, `current_liabilities`, `cash_and_equivalents`, `total_debt` | `NOPAT / invested_capital`，其中 NOPAT = `operating_income × (1 - income_tax_expense_total/income_before_tax_total)`，invested_capital = `shareholders_equity + total_debt - cash_and_equivalents` |
| `roe` | fli: `net_income`, `shareholders_equity` | `net_income / shareholders_equity` |
| `roe_without_leverage` | fli: `net_income`, `total_assets` | `net_income / total_assets` |
| `free_cash_flow_margin` | fli: `operating_cash_flow`, `capital_expenditures`, `revenue` | `(operating_cash_flow - capital_expenditures) / revenue` |
| `sga_to_revenue` | fli: `sga_expense`, `revenue` | `sga_expense / revenue` |
| `rnd_to_revenue` | fli: `rnd_expense`, `revenue` | `rnd_expense / revenue` |
| `incremental_roic` | fli 跨期: `operating_income_t`, `operating_income_t-1`, invested_capital_t, invested_capital_t-1 | `(operating_income_t - operating_income_t-1) / (invested_capital_t - invested_capital_t-1)` |

### D.2 所有者盈余

| 特征 | 原始字段 | 完整计算 |
|------|---------|---------|
| `owner_earnings` | fli: `net_income`, `depreciation_amortization`, `capital_expenditures` | `net_income + depreciation_amortization - capital_expenditures` |
| `owner_earnings_margin` | fli: `net_income`, `depreciation_amortization`, `capital_expenditures`, `revenue` | `owner_earnings / revenue` |
| `owner_earnings_to_net_income` | fli: `net_income`, `depreciation_amortization`, `capital_expenditures` | `owner_earnings / net_income` |

### D.3 资本轻重

| 特征 | 原始字段 | 完整计算 |
|------|---------|---------|
| `capex_to_revenue` | fli: `capital_expenditures`, `revenue` | `capital_expenditures / revenue` |
| `capex_to_ocf` | fli: `capital_expenditures`, `operating_cash_flow` | `capital_expenditures / operating_cash_flow` |
| `depreciation_to_capex` | fli: `depreciation_amortization`, `capital_expenditures` | `depreciation_amortization / capital_expenditures` |

### D.4 现金流质量

| 特征 | 原始字段 | 完整计算 |
|------|---------|---------|
| `ocf_to_net_income` | fli: `operating_cash_flow`, `net_income` | `operating_cash_flow / net_income` |
| `ocf_growth_vs_ni_growth` | fli 跨期: `operating_cash_flow_t/t-1`, `net_income_t/t-1` | `(ocf_t/ocf_t-1 - 1) - (ni_t/ni_t-1 - 1)` |

### D.5 盈利质量

| 特征 | 原始字段 | 完整计算 |
|------|---------|---------|
| `accruals_ratio` | fli: `net_income`, `operating_cash_flow`, `total_assets` | `(net_income - operating_cash_flow) / total_assets` |
| `receivables_growth_vs_revenue` | fli 跨期: `accounts_receivable_t/t-1`, `revenue_t/t-1` | `(ar_t/ar_t-1 - 1) - (rev_t/rev_t-1 - 1)` |
| `inventory_growth_vs_revenue` | fli 跨期: `inventory_t/t-1`, `revenue_t/t-1` | `(inv_t/inv_t-1 - 1) - (rev_t/rev_t-1 - 1)` |
| `goodwill_to_assets` | fli: `goodwill`, `total_assets` | `goodwill / total_assets` |

### D.6 资本配置

| 特征 | 原始字段 | 完整计算 |
|------|---------|---------|
| `dividend_payout_ratio` | fli: `dividends_paid`, `net_income` | `abs(dividends_paid) / net_income`（dividends_paid 通常为负值） |
| `buyback_to_net_income` | fli: `share_repurchase`, `net_income` | `abs(share_repurchase) / net_income` |
| `shareholder_yield` | fli: `dividends_paid`, `share_repurchase`, `net_income` | `(abs(dividends_paid) + abs(share_repurchase)) / net_income` |
| `retention_rate` | fli: `dividends_paid`, `net_income` | `1 - abs(dividends_paid) / net_income` |
| `retained_earnings_roic` | fli 跨期: `revenue_t/t-1`, `dividends_paid`, `net_income` | `revenue_growth / retention_rate` |

### D.7 收入质量

| 特征 | 原始字段 | 完整计算 |
|------|---------|---------|
| `recurring_revenue_pct` | downstream_segments: `is_recurring` | `COUNT(is_recurring=True) / COUNT(*)` |
| `top_customer_concentration` | downstream_segments: `revenue_pct` | `MAX(revenue_pct)` |
| `top3_customer_concentration` | downstream_segments: `revenue_pct` | `SUM(TOP 3 revenue_pct)` |
| `revenue_type_diversity` | downstream_segments: `revenue_type` | `COUNT(DISTINCT revenue_type)` |
| `backlog_coverage` | downstream_segments: `backlog` + fli: `revenue` | `SUM(backlog) / revenue` |

### D.8 供应链

| 特征 | 原始字段 | 完整计算 |
|------|---------|---------|
| `sole_source_pct` | upstream_segments: `is_sole_source` | `COUNT(is_sole_source=True) / COUNT(*)` |
| `supplier_geo_concentration` | upstream_segments: `geographic_location` | `MAX(COUNT per location / total)` |
| `purchase_obligation_to_revenue` | upstream_segments: `purchase_obligation` + fli: `revenue` | `SUM(purchase_obligation) / revenue` |
| `long_lead_time_pct` | upstream_segments: `lead_time` | `COUNT(lead_time含"12 months"或"exceeding") / COUNT(*)` |

### D.9 地域

| 特征 | 原始字段 | 完整计算 |
|------|---------|---------|
| `top_region_concentration` | geographic_revenues: `region`, `revenue_share` | `MAX(revenue_share)` |
| `geo_diversity` | geographic_revenues: `revenue_share` | `1 - SUM(revenue_share²)`（1 - HHI） |
| `china_revenue_share` | geographic_revenues: `region`, `revenue_share` | `SUM(revenue_share WHERE region LIKE '%China%' OR region LIKE '%中国%')` |

### D.10 资本结构与偿债

| 特征 | 原始字段 | 完整计算 |
|------|---------|---------|
| `total_debt_count` | do: 全表 | `COUNT(*)` |
| `total_debt_principal` | do: `principal` | `SUM(principal)` |
| `current_debt_pct` | do: `principal`, `is_current` | `SUM(principal WHERE is_current) / SUM(principal)` |
| `weighted_avg_interest_rate` | do: `principal`, `interest_rate` | `SUM(principal × interest_rate) / SUM(principal)` |
| `debt_maturity_within_1y` | do: `principal`, `maturity_date` | `SUM(principal WHERE maturity_date < NOW + 1year)` |
| `debt_to_equity` | do: `principal` + fli: `shareholders_equity` | `SUM(principal) / shareholders_equity` |
| `debt_to_owner_earnings` | do: `principal` + fli: `net_income`, `depreciation_amortization`, `capital_expenditures` | `SUM(principal) / owner_earnings` |
| `interest_coverage` | fli: `operating_income`, `interest_expense` | `operating_income / interest_expense` |
| `current_ratio` | fli: `current_assets`, `current_liabilities` | `current_assets / current_liabilities` |

### D.11 管理层

| 特征 | 原始字段 | 完整计算 |
|------|---------|---------|
| `ceo_pay_ratio` | executive_compensations: `pay_ratio` | `pay_ratio WHERE role_type='CEO'`（直接读） |
| `exec_stock_award_pct` | executive_compensations: `stock_awards`, `total_comp` | `AVG(stock_awards / total_comp)` |
| `exec_total_comp_to_net_income` | executive_compensations: `total_comp` + fli: `net_income` | `SUM(total_comp) / net_income` |
| `mgmt_ownership_pct` | stock_ownership: `title`, `percent_of_class` | `SUM(percent_of_class WHERE title IS NOT NULL)`（有 title = 管理层） |
| `top5_ownership_concentration` | stock_ownership: `percent_of_class` | `SUM(TOP 5 percent_of_class)` |
| `narrative_count` | company_narratives: 全表 | `COUNT(*)` |
| `narrative_fulfillment_rate` | company_narratives: `status` | `COUNT(status='delivered') / COUNT(status IN ('delivered','missed','abandoned'))` |

### D.12 风险

| 特征 | 原始字段 | 完整计算 |
|------|---------|---------|
| `litigation_count` | litigations: `status` | `COUNT(status IN ('pending','ongoing'))` |
| `litigation_accrued_total` | litigations: `accrued_amount`, `status` | `SUM(accrued_amount WHERE status IN ('pending','ongoing'))` |
| `litigation_claimed_to_accrued` | litigations: `claimed_amount`, `accrued_amount` | `SUM(claimed_amount) / SUM(accrued_amount)` |
| `related_party_tx_count` | related_party_transactions: 全表 | `COUNT(*)` |
| `related_party_ongoing_count` | related_party_transactions: `is_ongoing` | `COUNT(is_ongoing=True)` |
| `related_party_amount_to_revenue` | related_party_transactions: `amount` + fli: `revenue` | `SUM(amount) / revenue` |

### D.13 经营

| 特征 | 原始字段 | 完整计算 |
|------|---------|---------|
| `operational_issue_count` | operational_issues: 全表 | `COUNT(*)` |
| `risk_issue_pct` | operational_issues: `risk` | `COUNT(risk IS NOT NULL) / COUNT(*)` |
| `guidance_issue_pct` | operational_issues: `guidance` | `COUNT(guidance IS NOT NULL) / COUNT(*)` |

### D.14 债务周期健康度（Dalio）

| 特征 | 原始字段 | 完整计算 |
|------|---------|---------|
| `debt_service_burden` | fli: `interest_expense`, `operating_cash_flow` | `interest_expense / operating_cash_flow` |
| `net_debt_to_ebitda` | fli: `operating_income`, `depreciation_amortization`, `cash_and_equivalents` + do: `principal` | `(SUM(principal) - cash_and_equivalents) / (operating_income + depreciation_amortization)` |
| `cash_to_short_term_debt` | fli: `cash_and_equivalents` + do: `principal`, `is_current` | `cash_and_equivalents / SUM(principal WHERE is_current)` |
| `debt_growth_vs_revenue_growth` | fli 跨期: `revenue_t/t-1` + do 跨期: `SUM(principal)_t/t-1` | `(debt_t/debt_t-1 - 1) - (rev_t/rev_t-1 - 1)` |
| `debt_growth_vs_ocf_growth` | fli 跨期: `operating_cash_flow_t/t-1` + do 跨期: `SUM(principal)_t/t-1` | `(debt_t/debt_t-1 - 1) - (ocf_t/ocf_t-1 - 1)` |
| `interest_to_revenue` | fli: `interest_expense`, `revenue` | `interest_expense / revenue` |

### D.15 反身性与杠杆动态（Soros）

| 特征 | 原始字段 | 完整计算 |
|------|---------|---------|
| `leverage_acceleration` | do 多期: `SUM(principal)_t/t-1/t-2` | debt_growth_t = debt_t/debt_t-1 - 1; debt_growth_t-1 = debt_t-1/debt_t-2 - 1; `debt_growth_t - debt_growth_t-1`（增速的增速） |
| `equity_issuance_to_capex` | fli: `proceeds_from_stock_issuance`, `capital_expenditures` | `proceeds_from_stock_issuance / capital_expenditures` |
| `debt_issuance_to_capex` | fli: `proceeds_from_debt_issuance`, `capital_expenditures` | `proceeds_from_debt_issuance / capital_expenditures` |
| `goodwill_growth_vs_revenue_growth` | fli 跨期: `goodwill_t/t-1`, `revenue_t/t-1` | `(gw_t/gw_t-1 - 1) - (rev_t/rev_t-1 - 1)` |
| `share_dilution_rate` | fli 跨期: `basic_weighted_average_shares_t/t-1` | `(shares_t - shares_t-1) / shares_t-1` |
| `financing_dependency` | fli: `proceeds_from_stock_issuance`, `proceeds_from_debt_issuance`, `operating_cash_flow` | `(proceeds_from_stock_issuance + proceeds_from_debt_issuance) / operating_cash_flow` |

### D.16 趋势特征（增长）

| 特征 | 原始字段 | 完整计算 |
|------|---------|---------|
| `revenue_growth_yoy` | fli 跨期: `revenue_t/t-1` | `(revenue_t - revenue_t-1) / revenue_t-1` |
| `gross_margin_delta` | fli 跨期: 同 gross_margin 字段 | `gross_margin_t - gross_margin_t-1` |
| `net_margin_delta` | fli 跨期: 同 net_margin 字段 | `net_margin_t - net_margin_t-1` |
| `ocf_growth_yoy` | fli 跨期: `operating_cash_flow_t/t-1` | `(ocf_t - ocf_t-1) / ocf_t-1` |
| `owner_earnings_growth_yoy` | fli 跨期: owner_earnings 计算字段 | `(oe_t - oe_t-1) / oe_t-1` |
| `china_revenue_share_delta` | geographic_revenues 跨期: `region`, `revenue_share` | `china_share_t - china_share_t-1` |
| `sole_source_pct_delta` | upstream_segments 跨期: `is_sole_source` | `sole_source_pct_t - sole_source_pct_t-1` |
| `top_customer_concentration_delta` | downstream_segments 跨期: `revenue_pct` | `max_pct_t - max_pct_t-1` |

### D.17 趋势特征（稳定性）

> 以下均需 N ≥ 3 期数据。`std()` = 标准差，`consecutive()` = 连续计数。

| 特征 | 原始字段 | 完整计算 |
|------|---------|---------|
| `gross_margin_stability` | fli 多期: `revenue`, `cost_of_revenue` | `std([gross_margin_period_1, ..., gross_margin_period_n])` |
| `net_margin_stability` | fli 多期: `net_income`, `revenue` | `std([net_margin_period_1, ..., net_margin_period_n])` |
| `revenue_growth_stability` | fli 多期: `revenue` | `std([rev_growth_1, ..., rev_growth_n])` |
| `ocf_margin_stability` | fli 多期: `operating_cash_flow`, `revenue` | `std([ocf/rev_period_1, ..., ocf/rev_period_n])` |
| `consecutive_margin_expansion` | fli 多期: `revenue`, `cost_of_revenue` | 从最近期向前数，毛利率连续递增的期数 |
| `consecutive_revenue_growth` | fli 多期: `revenue` | 从最近期向前数，收入连续正增长的期数 |
| `consecutive_positive_fcf` | fli 多期: `operating_cash_flow`, `capital_expenditures` | 从最近期向前数，FCF 连续为正的期数 |

### D.18 周期域（外部数据）

| 特征 | 原始字段 | 完整计算 |
|------|---------|---------|
| `fed_funds_rate` | FRED: `DFF` | 直接读取 |
| `yield_curve_slope` | FRED: `DGS10`, `DGS2` | `DGS10 - DGS2` |
| `credit_spread` | FRED: `BAMLH0A0HYM2` | 直接读取 |
| `real_interest_rate` | FRED: `DFF`, `CPIAUCSL` | `fed_funds_rate - cpi_yoy` |
| `m2_growth_yoy` | FRED: `M2SL` | `(M2_t - M2_t-12m) / M2_t-12m` |
| `corporate_debt_to_gdp` | FRED: `BCNSDODNS`, `GDP` | `BCNSDODNS / GDP` |
| `credit_growth_vs_gdp` | FRED: `TCMDO`, `GDP` | `credit_growth_yoy - nominal_gdp_growth_yoy` |
| `cpi_yoy` | FRED: `CPIAUCSL` | `(CPI_t - CPI_t-12m) / CPI_t-12m` |
| `dollar_index_yoy` | FRED: `DTWEXBGS` | `(DXY_t - DXY_t-12m) / DXY_t-12m` |
| `vix_level` | CBOE: `^VIX` | 直接读取 |
| `vix_percentile` | CBOE: `^VIX` 近 2 年历史 | `percentile_rank(vix_now, vix_2y_history)` |
| `margin_debt_growth` | FINRA margin statistics | `(margin_debt_t - margin_debt_t-12m) / margin_debt_t-12m` |
| `put_call_ratio` | CBOE daily P/C ratio | 直接读取 |
| `credit_spread_momentum` | FRED: `BAMLH0A0HYM2` 近 3 个月 | `spread_now - spread_3m_ago` |
| `yield_curve_slope_momentum` | FRED: `DGS10`, `DGS2` 近 3 个月 | `slope_now - slope_3m_ago` |

### D.19 L1 传导特征

> L1 特征不直接读原始数据，而是组合 L0 特征。此处列出依赖的 L0 特征。

| 特征 | 依赖的 L0 特征 | 完整计算 |
|------|---------------|---------|
| `patent_barrier` | `l0.tech.patent_count`, `l0.tech.patent_commercial_count` | `patent_count × (patent_commercial_count / patent_count)` |
| `margin_vs_peers` | `l0.company.gross_margin`, `l0.industry.avg_gross_margin` | `gross_margin - avg_gross_margin` |
| `growth_vs_peers` | `l0.company.revenue_growth_yoy`, `l0.industry.avg_revenue_growth` | `revenue_growth - avg_revenue_growth` |
| `export_control_risk` | `l0.company.china_revenue_share` + 外部管制指数 | `china_revenue_share × export_control_strictness` |
| `funding_stress` | `l0.company.current_debt_pct`, `l0.company.total_debt_principal` + `l0.cycle.fed_funds_rate` | `current_debt_pct × total_debt_principal × fed_funds_rate` |
| `supply_chain_geo_risk` | `l0.company.supplier_geo_concentration` + 外部地缘指数 | `supplier_geo_concentration × geopolitical_tension_index` |
| `revenue_geo_risk` | `l0.company.china_revenue_share` + 外部地缘指数 | `china_revenue_share × geopolitical_tension_index` |
| `rate_sensitivity` | `l0.company.debt_service_burden`, `l0.cycle.real_interest_rate` | `debt_service_burden × real_interest_rate` |
| `credit_cycle_exposure` | `l0.company.net_debt_to_ebitda`, `l0.cycle.credit_spread` | `net_debt_to_ebitda × credit_spread` |
| `deleveraging_pressure` | `l0.company.debt_growth_vs_revenue_growth`, `l0.cycle.credit_growth_vs_gdp` | `debt_growth_vs_revenue_growth + credit_growth_vs_gdp` |
| `liquidity_stress` | `l0.company.cash_to_short_term_debt`, `l0.cycle.real_interest_rate` | `(1 - cash_to_short_term_debt) × real_interest_rate` |
| `reflexivity_exposure` | `l0.company.financing_dependency`, `l0.cycle.vix_level` | `financing_dependency × (1 / vix_level)` |
| `leverage_cycle_risk` | `l0.company.leverage_acceleration`, `l0.cycle.margin_debt_growth` | `leverage_acceleration × margin_debt_growth` |
| `bubble_fragility` | `l0.company.share_dilution_rate`, `l0.cycle.vix_percentile` | `share_dilution_rate × (100 - vix_percentile)` |
