# Anchor 输出契约 — Polaris 数据需求

> 本文档定义 Polaris 对 Anchor 提取数据的全部需求。
> Anchor 应以此为标准，确保输出表和字段覆盖以下清单。
>
> 格式：**特征 → 计算方式 → 源数据**
>
> 源数据标注：
> - `[现有]` = Anchor 表已存在（字段是否齐全需确认）
> - `[新增]` = 需要 Anchor 新建表或新增提取逻辑
> - `[外部]` = 来自外部市场数据 API，非 Anchor 职责

---

## 通用数据标准

### 期间粒度

| 数据类型 | 粒度 | 说明 |
|---------|------|------|
| 财务报表 (financial_line_items) | **季度** | 10-Q / 中报。Anchor 按季度提取，Polaris 可自行聚合为年度 |
| 经营数据 (segments, issues 等) | **随报表期** | 跟随财报发布节奏（季度或年度） |
| 管理层指引 (management_guidance) | **随披露** | 电话会 / 投资者日，不定期 |
| 分析师预期 (analyst_estimates) | **季度** | 按目标财报期 |
| 内部人交易 (insider_transactions) | **逐笔** | 按交易日期 |
| 高管变动 (executive_changes) | **逐笔** | 按事件日期 |

### 历史深度要求

> Anchor 应确保每家公司至少有 **20 个季度（5 年）** 的 financial_line_items 数据。
> 不足 20 期的公司，Polaris 会在稳定性特征上标注"数据不足，无法评估"。

| 用途 | 最少期数 | 粒度 | 适用特征举例 |
|------|---------|------|------------|
| 单期计算（比率、占比） | 1 期 | 季度 | gross_margin, debt_to_equity, owner_earnings |
| 同比增速 (YoY) | 5 期（当期 + 前 4 同期） | 季度 | revenue_growth_yoy, receivables_growth_vs_revenue |
| 稳定性（标准差） | **20 期（5 年 × 4 季）** | 季度 | gross_margin_stability, roe_stability, 所有 `_stability` 系列 |
| 连续性（consecutive） | 全部可用期 | 季度 | consecutive_revenue_growth, consecutive_positive_fcf |
| 加速度（二阶导） | 12 期（3 年 × 4 季） | 季度 | leverage_acceleration, gap_trend |
| 增量 ROIC | 8 期（2 年 × 4 季） | 季度 | incremental_roic（需跨期差值） |
| 管理层指引兑现 | 不限 | 随披露 | narrative_fulfillment_rate（累计所有历史） |

### 外部数据源清单

> 以下数据 **不属于 Anchor 职责**，由 Polaris 自建 `external_data` 模块从外部 API 获取。
> 列出完整清单以明确系统边界。

| 数据 | 变量名 | 源 | 频率 | 用于 |
|------|--------|-----|------|------|
| 10Y 国债收益率 | `DGS10` | FRED API | 日 | 巴菲特 DCF 折现率 + 达利欧象限 |
| GDP Nowcast | `GDPNOW` | FRED API (Atlanta Fed) | 周 | 达利欧·实际增长 |
| GDP 历史 | `GDP` | FRED API | 季 | 达利欧·预期增长（10 年均值） |
| CPI YoY | `CPIAUCSL` | FRED API | 月 | 达利欧·实际通胀 |
| 10Y 盈亏平衡通胀 | `T10YIE` | FRED API | 日 | 达利欧·预期通胀 |
| 3M T-Bill | `DTB3` | FRED API | 日 | 达利欧·现金回报 |
| S&P 500 盈利收益率 | Shiller CAPE 倒数 | Shiller 数据 | 月 | 达利欧·ERP 计算 |
| VIX | `^VIX` | CBOE / Yahoo Finance | 日 | 达利欧象限 + 索罗斯情绪 |
| HY 信用利差 | `BAMLH0A0HYM2` | FRED API | 日 | 索罗斯·情绪 |
| FINRA 保证金贷款 | margin debt | FINRA | 月 | 索罗斯·情绪 |
| Put/Call Ratio | P/C ratio | CBOE | 日 | 索罗斯·情绪 |
| 个股价格 | per ticker | Yahoo Finance | 日 | 索罗斯·反向 DCF |
| 个股做空数据 | short interest | 交易所 | 双周 | 索罗斯·情绪 |
| 资产价格 (VTI/TLT/IEF/GLD/DBC) | per ETF | Yahoo Finance | 日 | 达利欧·风险平价波动率 |

### 跨期查询约定

> Polaris 的特征计算分为**单期**和**跨期**两类。
> Anchor 数据库须支持按 `company_id` 查询多期数据（不限制单一 period）。

- **单期特征**：只需当期数据。Polaris 传入 `(company_id, period)`。
- **跨期特征**：需要 N 期历史。Polaris 传入 `(company_id, period, n_periods)` 或直接查全部可用期后在内存筛选。
- **外部数据特征**：不经过 Anchor，Polaris 直连外部 API。

> Polaris 代码层面：`ComputeContext` 需支持 `get_financial_line_items(n_periods=20)` 等多期查询方法。
> 当前单期实现是 v0.1 遗留，须在 v0.2 中扩展。

---

## 目录

1. [巴菲特·商业模式质量](#1-巴菲特商业模式质量)
2. [巴菲特·护城河](#2-巴菲特护城河)
3. [巴菲特·所有者盈余与资本轻重](#3-巴菲特所有者盈余与资本轻重)
4. [巴菲特·盈利质量](#4-巴菲特盈利质量)
5. [巴菲特·资本配置](#5-巴菲特资本配置)
6. [巴菲特·管理层品格](#6-巴菲特管理层品格)
7. [巴菲特·可预测性](#7-巴菲特可预测性)
8. [巴菲特·财务安全](#8-巴菲特财务安全)
9. [巴菲特·内在价值估算](#9-巴菲特内在价值估算)
10. [达利欧·环境引擎（象限判定）](#10-达利欧环境引擎象限判定)
11. [达利欧·风险平价引擎](#11-达利欧风险平价引擎)
12. [达利欧·公司级周期脆弱度](#12-达利欧公司级周期脆弱度)
13. [索罗斯·预期偏差引擎（反身性核心）](#13-索罗斯预期偏差引擎反身性核心)
14. [索罗斯·反身性强度](#14-索罗斯反身性强度)
15. [索罗斯·市场情绪环境](#15-索罗斯市场情绪环境)
16. [索罗斯·综合输出](#16-索罗斯综合输出)
17. [新增表定义](#17-新增表定义)
18. [financial_line_items 必需 item_key 清单](#18-financial_line_items-必需-item_key-清单)

---

## 1. 巴菲特·商业模式质量

> 收入是否可预测？来源是否集中？客户粘性如何？

| 特征 | 计算方式 | 源数据 |
|------|---------|--------|
| `recurring_revenue_pct` | 经常性收入金额 / 总收入（按金额加权，非 segment 数量） | `[现有]` downstream_segments: `is_recurring`, `revenue_amount` 或 `revenue_pct` |
| `top_customer_concentration` | 最大客户收入占比 | `[现有]` downstream_segments: `revenue_pct` |
| `top3_customer_concentration` | 前 3 大客户收入占比之和 | `[现有]` downstream_segments: `revenue_pct` |
| `revenue_type_diversity` | 不同收入类型的数量 | `[现有]` downstream_segments: `revenue_type` |
| `backlog_coverage` | 积压订单总额 / 年收入 | `[现有]` downstream_segments: `backlog` + financial_line_items: `revenue` |
| `contract_duration_avg` | 客户合同的平均时长（月） | `[新增]` downstream_segments 新增字段: `contract_duration_months` |
| `customer_retention_rate` | 客户留存率 = 续约客户数 / 上期客户数 | `[新增]` downstream_segments 新增字段: `is_new_customer`, `is_churned`；或新增表 `customer_retention` |
| `switching_cost_indicator` | 客户是否存在高转换成本（集成深度、数据迁移成本、定制程度） | `[新增]` downstream_segments 新增字段: `switching_cost_level`（high/medium/low，从财报/电话会提取） |

---

## 2. 巴菲特·护城河

> 竞争优势是否持久？定价权如何？竞对能否用钱砸进来？

### 2.1 定价权

| 特征 | 计算方式 | 源数据 |
|------|---------|--------|
| `price_increase_history` | 近 N 期提价次数和平均提价幅度 | `[新增]` **pricing_actions** 表: `product_or_segment`, `price_change_pct`, `effective_date`, `announced_in`（来源文章） |
| `price_volume_response` | 提价后销量变化（提价幅度 vs 销量变化的比率） | `[新增]` pricing_actions 表: `volume_impact_pct`（提价后销量变化百分比，从财报/电话会提取） |
| `gross_margin_after_price_change` | 提价前后毛利率变化 | `[新增]` pricing_actions: `effective_date` + `[现有]` financial_line_items 跨期: `revenue`, `cost_of_revenue` |
| `gross_margin` | (revenue - cost_of_revenue) / revenue | `[现有]` financial_line_items: `revenue`, `cost_of_revenue` |
| `gross_margin_stability` | 近 N 期毛利率标准差（低 = 定价权稳定） | `[现有]` financial_line_items 多期: `revenue`, `cost_of_revenue` |
| `operating_margin` | operating_income / revenue | `[现有]` financial_line_items: `operating_income`, `revenue` |

### 2.2 竞对分析（十亿美金测试）

| 特征 | 计算方式 | 源数据 |
|------|---------|--------|
| `competitor_capex_intensity` | 主要竞对的资本开支 / 收入（竞对在砸多少钱进攻这个市场） | `[新增]` **competitor_relations** 表: `competitor_company_id`, `market_overlap_description`；竞对公司的 financial_line_items: `capital_expenditures`, `revenue` |
| `competitor_market_share_trend` | 竞对市占率变化方向（扩张/收缩/稳定） | `[新增]` **market_share_data** 表: `company_id`, `market_segment`, `period`, `share_pct`, `source`（从行业报告/财报提取） |
| `new_entrant_count` | 近 N 年新进入者数量 | `[新增]` market_share_data 中新出现的 company_id 数量 |
| `new_entrant_funding` | 新进入者的融资总额 | `[新增]` competitor_relations + 外部融资数据，或从行业报告提取 |
| `market_share_stability` | 公司市占率近 N 期标准差 | `[新增]` market_share_data 多期 |
| `margin_vs_peers` | 公司毛利率 - 行业平均毛利率 | `[现有]` 本公司 financial_line_items + 竞对公司 financial_line_items（需 competitor_relations 确定同业） |
| `growth_vs_peers` | 公司收入增速 - 行业平均增速 | 同上 |

### 2.3 护城河动态

| 特征 | 计算方式 | 源数据 |
|------|---------|--------|
| `gross_margin_delta` | 毛利率同比变化 | `[现有]` financial_line_items 跨期 |
| `consecutive_margin_expansion` | 毛利率连续扩张期数 | `[现有]` financial_line_items 多期 |
| `incremental_roic` | Δ operating_income / Δ invested_capital | `[现有]` financial_line_items 跨期: `operating_income`, `total_assets`, `current_liabilities`, `cash_and_equivalents`, `total_debt` |

---

## 3. 巴菲特·所有者盈余与资本轻重

> 股东真正能拿走多少钱？生意需要持续砸钱维持吗？

| 特征 | 计算方式 | 源数据 |
|------|---------|--------|
| `owner_earnings` | net_income + depreciation_amortization - capital_expenditures | `[现有]` financial_line_items: `net_income`, `depreciation_amortization`, `capital_expenditures` |
| `owner_earnings_margin` | owner_earnings / revenue | 同上 + `revenue` |
| `owner_earnings_to_net_income` | owner_earnings / net_income（>1 轻资本，<1 重资本） | 同上 |
| `owner_earnings_growth_yoy` | 所有者盈余同比增速 | 同上，跨期 |
| `capex_to_revenue` | capital_expenditures / revenue | `[现有]` financial_line_items: `capital_expenditures`, `revenue` |
| `capex_to_ocf` | capital_expenditures / operating_cash_flow | `[现有]` financial_line_items: `capital_expenditures`, `operating_cash_flow` |
| `depreciation_to_capex` | depreciation / capex（>1 消化存量，<1 加速扩张） | `[现有]` financial_line_items: `depreciation_amortization`, `capital_expenditures` |
| `maintenance_capex_ratio` | 维持性资本开支 / 总资本开支 | `[新增]` financial_line_items 新增 item_key: `maintenance_capex`（从财报/电话会提取管理层披露的维持性 vs 增长性 capex 拆分）；若无披露，用 depreciation_amortization 近似 |

---

## 4. 巴菲特·盈利质量

> 利润是真的吗？现金流能不能背书？有没有造假信号？

| 特征 | 计算方式 | 源数据 |
|------|---------|--------|
| `ocf_to_net_income` | operating_cash_flow / net_income | `[现有]` financial_line_items: `operating_cash_flow`, `net_income` |
| `ocf_growth_vs_ni_growth` | OCF 增速 - 净利润增速（背离 = 危险） | `[现有]` financial_line_items 跨期 |
| `accruals_ratio` | (net_income - operating_cash_flow) / total_assets | `[现有]` financial_line_items: `net_income`, `operating_cash_flow`, `total_assets` |
| `receivables_growth_vs_revenue` | 应收增速 - 收入增速（>0 = 赊账堆收入） | `[现有]` financial_line_items 跨期: `accounts_receivable`, `revenue` |
| `inventory_growth_vs_revenue` | 存货增速 - 收入增速（>0 = 卖不动） | `[现有]` financial_line_items 跨期: `inventory`, `revenue` |
| `goodwill_to_assets` | goodwill / total_assets | `[现有]` financial_line_items: `goodwill`, `total_assets` |
| `related_party_amount_to_revenue` | 关联交易总额 / 收入 | `[现有]` related_party_transactions: `amount` + financial_line_items: `revenue` |
| `related_party_ongoing_count` | 持续性关联交易数量 | `[现有]` related_party_transactions: `is_ongoing` |
| `audit_opinion_type` | 审计意见类型（标准无保留 / 保留 / 否定） | `[新增]` **audit_opinions** 表: `period`, `opinion_type`, `emphasis_matters`（从年报提取） |

---

## 5. 巴菲特·资本配置

> 管理层赚到钱后怎么花？回购、分红、收购还是囤现金？

| 特征 | 计算方式 | 源数据 |
|------|---------|--------|
| `shareholder_yield` | (dividends + buybacks) / net_income | `[现有]` financial_line_items: `dividends_paid`, `share_repurchase`, `net_income` |
| `dividend_payout_ratio` | abs(dividends_paid) / net_income | `[现有]` financial_line_items: `dividends_paid`, `net_income` |
| `buyback_to_net_income` | abs(share_repurchase) / net_income | `[现有]` financial_line_items: `share_repurchase`, `net_income` |
| `retained_earnings_roic` | incremental_roic（留存利润的再投资回报直接看增量 ROIC） | `[现有]` financial_line_items 跨期（同 incremental_roic） |
| `acquisition_spend_to_ocf` | 收购支出 / 经营现金流（大额收购是否超出造血能力） | `[现有]` financial_line_items: `acquisitions_net`（需确认 item_key）, `operating_cash_flow` |
| `goodwill_growth_vs_revenue_growth` | 商誉增速 - 收入增速（>0 = 高价收购但收入没跟上） | `[现有]` financial_line_items 跨期: `goodwill`, `revenue` |
| `share_count_trend` | 股本数量同比变化（持续减少 = 回购，持续增加 = 稀释） | `[现有]` financial_line_items 跨期: `basic_weighted_average_shares` |

---

## 6. 巴菲特·管理层品格

> 管理层是否诚实？是否对齐股东利益？

### 6.1 承诺兑现

| 特征 | 计算方式 | 源数据 |
|------|---------|--------|
| `narrative_fulfillment_rate` | status=delivered / (delivered + missed + abandoned) | `[现有]` company_narratives: `status` |
| `narrative_count` | 管理层叙事/承诺总数 | `[现有]` company_narratives |

### 6.2 问题回避度（诚实性检验）

> 逻辑：Anchor 从外部信息源提取"公司面临的已知问题清单"，从致股东信/财报电话会提取"管理层主动提及的问题清单"。Polaris 比对两个清单，未被提及的问题越多，回避度越高。

| 特征 | 计算方式 | 源数据 |
|------|---------|--------|
| `issue_acknowledgment_rate` | 管理层提及的问题数 / 全部已知问题数 | `[新增]` **known_issues** 表 + **management_acknowledgments** 表（见 Section 17.4 / 17.5） |
| `issue_avoidance_severity` | 未被提及问题的严重程度加权分 | `[新增]` known_issues: `severity`；management_acknowledgments 做关联比对 |
| `problem_response_quality` | 管理层对已承认问题的回应质量（正面面对/轻描淡写/甩锅） | `[新增]` management_acknowledgments: `response_quality`（forthright / downplay / deflect，从致股东信/电话会提取） |

### 6.3 利益对齐

| 特征 | 计算方式 | 源数据 |
|------|---------|--------|
| `mgmt_ownership_pct` | 管理层合计持股比例 | `[现有]` stock_ownership: `title`, `percent_of_class` |
| `ceo_pay_ratio` | CEO 薪酬 / 员工中位数 | `[现有]` executive_compensations: `pay_ratio` |
| `exec_stock_award_pct` | 高管平均 stock_awards / total_comp | `[现有]` executive_compensations: `stock_awards`, `total_comp` |
| `insider_selling_vs_buying` | 内部人净卖出 / 净买入（过去 12 月） | `[新增]` **insider_transactions** 表: `person`, `transaction_type`（buy/sell）, `shares`, `price`, `date` |
| `mgmt_turnover_rate` | 高管团队离职率（过去 24 月） | `[新增]` **executive_changes** 表: `person`, `title`, `change_type`（joined/departed/promoted）, `date`, `reason` |

---

## 7. 巴菲特·可预测性

> 关键指标是否长期稳定？能看懂未来十年吗？

| 特征 | 计算方式 | 源数据 |
|------|---------|--------|
| `gross_margin_stability` | 近 N 期毛利率标准差 | `[现有]` financial_line_items 多期: `revenue`, `cost_of_revenue` |
| `net_margin_stability` | 近 N 期净利率标准差 | `[现有]` financial_line_items 多期: `net_income`, `revenue` |
| `revenue_growth_stability` | 近 N 期收入增速标准差 | `[现有]` financial_line_items 多期: `revenue` |
| `ocf_margin_stability` | 近 N 期经营现金流率标准差 | `[现有]` financial_line_items 多期: `operating_cash_flow`, `revenue` |
| `consecutive_revenue_growth` | 收入连续正增长的期数 | `[现有]` financial_line_items 多期 |
| `consecutive_positive_fcf` | 自由现金流连续为正的期数 | `[现有]` financial_line_items 多期: `operating_cash_flow`, `capital_expenditures` |
| `revenue_growth_yoy` | 收入同比增速 | `[现有]` financial_line_items 跨期: `revenue` |
| `roe` | net_income / shareholders_equity | `[现有]` financial_line_items: `net_income`, `shareholders_equity` |
| `roe_stability` | 近 N 期 ROE 标准差 | `[现有]` financial_line_items 多期 |

---

## 8. 巴菲特·财务安全

> 债务可控吗？有没有偿债压力？

| 特征 | 计算方式 | 源数据 |
|------|---------|--------|
| `debt_to_equity` | total_debt / shareholders_equity | `[现有]` financial_line_items: `total_debt`, `shareholders_equity` |
| `debt_to_owner_earnings` | total_debt / owner_earnings（几年能还清） | `[现有]` financial_line_items + debt_obligations |
| `interest_coverage` | operating_income / interest_expense | `[现有]` financial_line_items: `operating_income`, `interest_expense` |
| `current_ratio` | current_assets / current_liabilities | `[现有]` financial_line_items: `current_assets`, `current_liabilities` |
| `net_margin` | net_income / revenue | `[现有]` financial_line_items: `net_income`, `revenue` |
| `free_cash_flow_margin` | (operating_cash_flow - capex) / revenue | `[现有]` financial_line_items: `operating_cash_flow`, `capital_expenditures`, `revenue` |

---

## 9. 巴菲特·内在价值估算

> 前置条件：公司必须通过巴菲特流派的过滤标准（ROE 稳定、现金流稳定、护城河成立、管理层品格合格）。
> 未通过过滤的公司不计算内在价值，标注"无法估值"。
> 无 guidance 的公司也标注"无法估值"。
>
> 核心公式：内在价值 = Σ 未来 owner_earnings / (1 + 折现率)^t
> 折现率 = 美国长期国债收益率（10Y Treasury）`[外部]`

### 9.1 估值计算路径

> Polaris 根据 Anchor 提取到的 guidance 类型，选择可用的计算路径。
> 有什么值用什么值算，算不出来标"无法估值"。

**路径 A：CapEx guidance + 增量 ROIC（最优路径）**

管理层给出了再投资计划和预期回报（或 Polaris 用历史 incremental_roic 替代）。

```
留存利润 = owner_earnings × (1 - payout_ratio)
增量收益 = 留存利润 × incremental_roic（guided 或 historical）
下期 owner_earnings = 当期 owner_earnings + 增量收益
逐年滚动 N 年，按国债利率折现
永续价值 = 第 N 年 owner_earnings / 折现率（假设零增长永续）
```

| 所需数据 | 来源 |
|---------|------|
| 当期 owner_earnings | Polaris 计算（net_income + D&A - capex） |
| payout_ratio | Polaris 计算（dividends + buybacks / net_income） |
| guided_capex 或 guided_reinvestment | `[新增]` management_guidance: `metric=capex`, `value` |
| guided_roic 或 historical incremental_roic | `[新增]` management_guidance: `metric=roic_target`；或 Polaris 从历史 financial_line_items 计算 |
| 折现率 | `[外部]` 10Y Treasury yield |

**路径 B：收入增速 guidance + 利润率稳定假设**

管理层给出了收入增速预期，利润率由过滤条件保证稳定。

```
下期 revenue = 当期 revenue × (1 + guided_revenue_growth)
下期 owner_earnings = 下期 revenue × 当期 owner_earnings_margin
  （利润率稳定假设——已通过过滤器验证）
逐年滚动 N 年，按国债利率折现
```

| 所需数据 | 来源 |
|---------|------|
| guided_revenue_growth | `[新增]` management_guidance: `metric=revenue_growth`, `value` |
| 当期 owner_earnings_margin | Polaris 计算 |
| 折现率 | `[外部]` 10Y Treasury yield |

**路径 C：EPS guidance + 所有者盈余调整**

管理层给出了 EPS 目标。

```
下期 net_income = guided_eps × shares_outstanding
下期 owner_earnings = 下期 net_income × 历史 (owner_earnings / net_income) 比率
逐年滚动或单期估算
```

| 所需数据 | 来源 |
|---------|------|
| guided_eps | `[新增]` management_guidance: `metric=eps`, `value` |
| shares_outstanding | `[现有]` financial_line_items: `basic_weighted_average_shares` |
| 历史 OE/NI 比率 | Polaris 计算 |

**路径 D：利润率 guidance + 历史收入增速**

管理层给出了利润率目标，收入增速用历史均值。

```
下期 revenue = 当期 revenue × (1 + 历史平均收入增速)
下期 owner_earnings = 下期 revenue × guided_margin
  （需从 guided operating_margin 或 net_margin 换算）
```

| 所需数据 | 来源 |
|---------|------|
| guided_operating_margin 或 guided_net_margin | `[新增]` management_guidance: `metric=operating_margin`, `value` |
| 历史收入增速 | Polaris 从 financial_line_items 多期计算 |

### 9.2 路径选择优先级

```
有 capex + ROIC guidance  → 路径 A（最直接，最巴菲特）
有 revenue_growth guidance → 路径 B
有 EPS guidance           → 路径 C
有 margin guidance         → 路径 D
以上都没有               → 标"无法估值"
```

多条路径可用时，Polaris 可交叉验证：如果路径 A 和路径 B 算出的内在价值差异 > 30%，标注"估值分歧大，需人工审查"。

### 9.3 Polaris 输出

| 输出字段 | 说明 |
|---------|------|
| `intrinsic_value` | 内在价值（每股，或总市值） |
| `valuation_path` | 使用了哪条计算路径（A/B/C/D） |
| `key_assumptions` | 关键假设（增速、利润率、折现率、预测年限） |
| `valuation_status` | valued / unvaluable / divergent（多路径分歧大） |
| `sensitivity` | 关键假设变动 ±1 个百分点对内在价值的影响 |

> 注：Polaris 只输出内在价值。当前股价获取和安全边际计算由 Axion 负责。

---

## 10. 达利欧·环境引擎（象限判定）

> 达利欧的核心：资产价格由四个变量驱动——增长、通胀、折现率、风险溢价。
> Polaris 用这四个变量的"预期差"判定当前处于哪个象限，并给出估值信号。

### 10.1 输入向量

| 变量 | 量化指标 | 计算方式 | 源数据 |
|------|---------|---------|--------|
| 实际增长 | `actual_growth` | GDP Nowcast 或 Manufacturing PMI | `[外部]` FRED: `GDPNOW`（亚特兰大联储）或 `MANEMP` / ISM PMI |
| 预期增长 | `expected_growth` | 过去 10 年 GDP 增长均值（滚动） | `[外部]` FRED: `GDP`，Polaris 计算 10 年移动平均 |
| 实际通胀 | `actual_inflation` | CPI YoY 或 PCE YoY | `[外部]` FRED: `CPIAUCSL` 或 `PCEPI` |
| 预期通胀 | `expected_inflation` | 10 年盈亏平衡通胀率（TIPS 利差） | `[外部]` FRED: `T10YIE` |
| 折现率 | `discount_rate` | 10 年期国债收益率 | `[外部]` FRED: `DGS10` |
| 现金回报 | `cash_return` | 3 个月国库券收益率 | `[外部]` FRED: `DTB3` |
| 资产收益率 | `equity_yield` | S&P 500 盈利收益率（E/P） | `[外部]` S&P earnings data / Shiller CAPE 倒数 |
| 波动率 | `vix` | VIX 恐慌指数 | `[外部]` CBOE: `^VIX` |

### 10.2 象限判定逻辑

```
growth_gap = actual_growth - expected_growth
inflation_gap = actual_inflation - expected_inflation

QUAD_1_GOLDILOCKS:           growth_gap > 0, inflation_gap <= 0
QUAD_2_INFLATIONARY_GROWTH:  growth_gap > 0, inflation_gap > 0
QUAD_3_STAGFLATION:          growth_gap <= 0, inflation_gap > 0
QUAD_4_DEFLATIONARY_RECESSION: growth_gap <= 0, inflation_gap <= 0
```

### 10.3 估值信号

```
equity_risk_premium = equity_yield - discount_rate

规则 1（泡沫信号）:
  discount_rate > 历史 75 分位 AND erp < 历史 25 分位
  → SELL_RISK_ASSETS

规则 2（恐慌机会）:
  erp > 历史 90 分位 AND vix > 30
  → BUY_THE_FEAR

其他 → HOLD
```

> 注："历史分位"由 Polaris 基于滚动窗口（如 20 年）计算，非硬编码阈值。

### 10.4 Polaris 输出

| 输出字段 | 说明 |
|---------|------|
| `quadrant` | 当前象限（QUAD_1 / QUAD_2 / QUAD_3 / QUAD_4） |
| `growth_gap` | 增长预期差（实际 - 预期） |
| `inflation_gap` | 通胀预期差（实际 - 预期） |
| `equity_risk_premium` | 股权风险溢价 |
| `erp_percentile` | ERP 在历史分布中的百分位 |
| `discount_rate_percentile` | 折现率在历史分布中的百分位 |
| `valuation_signal` | SELL_RISK_ASSETS / BUY_THE_FEAR / HOLD |
| `discount_rate_slope` | 折现率 3 个月变化斜率（用于 Axion 判断是否暴力加息） |

---

## 11. 达利欧·风险平价引擎

> 核心原理：按波动率倒数分配权重，使每类资产对组合的风险贡献相等。
> Polaris 输出基础权重和波动率矩阵，Axion 负责杠杆调节、再平衡执行。

### 11.1 资产池

| 资产桶 | 代表标的 | 角色 |
|--------|---------|------|
| 股票 | VTI（美国全市场） | 增长受益 |
| 长期国债 | TLT（20 年+国债） | 通缩/衰退受益 |
| 中期国债 | IEF（7-10 年国债） | 温和环境稳定器 |
| 黄金 | GLD | 通胀/地缘避险 |
| 大宗商品 | DBC | 通胀受益 |

### 11.2 波动率与相关性计算

| 特征 | 计算方式 | 源数据 |
|------|---------|--------|
| `asset_volatility` | 每类资产的年化波动率 = 日收益率标准差 × √252（252 日滚动窗口） | `[外部]` 价格时间序列: Yahoo Finance / Alpha Vantage |
| `correlation_matrix` | 资产间滚动相关系数矩阵（252 日窗口） | 同上 |
| `risk_parity_weights` | 权重 ∝ 1/σ（波动率倒数），归一化至总和 = 1 | Polaris 计算 |

### 11.3 风险平价权重计算

```
对每类资产 i:
  raw_weight_i = 1 / volatility_i
归一化:
  weight_i = raw_weight_i / Σ raw_weight_j
```

### 11.4 Polaris 输出

| 输出字段 | 说明 |
|---------|------|
| `risk_parity_weights` | 五类资产的风险平价基础权重 |
| `asset_volatilities` | 每类资产当前年化波动率 |
| `correlation_matrix` | 5×5 资产相关性矩阵 |
| `portfolio_volatility` | 按当前权重计算的组合年化波动率 |

> 注：Axion 负责根据象限做动态偏移（tilts）、杠杆调节、再平衡触发和执行。

---

## 12. 达利欧·公司级周期脆弱度

> 将公司按主行业归入资产桶，评估其在当前周期环境下的脆弱程度。

### 12.1 公司→资产桶映射

| 行业分类 | 资产桶 | 逻辑 |
|---------|--------|------|
| 科技、消费、医疗、金融 | 股票（VTI） | 增长敏感 |
| 能源、矿业、农业 | 大宗商品（DBC） | 通胀/商品周期敏感 |
| 公用事业、地产（REIT） | 长债（TLT） | 利率敏感 |
| 贵金属相关 | 黄金（GLD） | 避险属性 |

> 映射依据：`[现有]` company_profiles: `industry` 字段。Polaris 维护行业→资产桶的映射表。

### 12.2 公司级脆弱度特征

> 公司的财务特征决定了它在所属资产桶内的脆弱度。

| 特征 | 计算方式 | 源数据 |
|------|---------|--------|
| `debt_service_burden` | interest_expense / operating_cash_flow | `[现有]` financial_line_items: `interest_expense`, `operating_cash_flow` |
| `net_debt_to_ebitda` | (total_debt - cash) / EBITDA | `[现有]` financial_line_items: `total_debt`, `cash_and_equivalents`, `operating_income`, `depreciation_amortization` |
| `debt_growth_vs_revenue_growth` | 总债务增速 - 收入增速 | `[现有]` financial_line_items 跨期 + debt_obligations 跨期 |
| `current_debt_pct` | 短期债务本金 / 总本金 | `[现有]` debt_obligations: `principal`, `is_current` |
| `weighted_avg_interest_rate` | 加权平均利率 | `[现有]` debt_obligations: `principal`, `interest_rate` |
| `floating_rate_debt_pct` | 浮动利率债务本金 / 总本金 | `[新增]` debt_obligations 新增字段: `is_floating_rate` |
| `refinancing_wall` | 未来 2 年到期债务 / 经营现金流 | `[现有]` debt_obligations: `principal`, `maturity_date` + financial_line_items: `operating_cash_flow` |
| `cash_to_short_term_debt` | cash / current_debt | `[现有]` financial_line_items: `cash_and_equivalents` + debt_obligations: `principal`, `is_current` |
| `interest_to_revenue` | interest_expense / revenue | `[现有]` financial_line_items: `interest_expense`, `revenue` |

### 12.3 脆弱度与象限的交叉

> Polaris 输出公司脆弱度评分时，考虑当前象限环境。

```
QUAD_3（滞胀）下:
  高 debt_service_burden + 高 floating_rate_debt_pct = 极度脆弱
  （利率高 + 通胀侵蚀利润 + 浮动利率放大成本）

QUAD_4（通缩衰退）下:
  高 refinancing_wall + 低 cash_to_short_term_debt = 极度脆弱
  （需要再融资但信用市场冻结）

QUAD_1（金发姑娘）下:
  大多数公司安全，高杠杆公司反而受益（借钱便宜、增长好）

QUAD_2（通胀增长）下:
  大宗商品桶公司受益，长债桶公司承压
```

### 12.4 Polaris 输出

| 输出字段 | 说明 |
|---------|------|
| `asset_bucket` | 公司所属的资产桶 |
| `bucket_volatility` | 所属桶的当前波动率 |
| `vulnerability_score` | 公司在当前象限下的脆弱度评分（基于 12.2 特征 + 12.3 交叉逻辑） |
| `vulnerability_drivers` | 脆弱度的 top-3 驱动因子 |

---

## 13. 索罗斯·预期偏差引擎（反身性核心）

> 索罗斯的核心：市场价格包含了一套预期（共识），当现实与预期出现裂痕时，就是机会。
> Polaris 通过反向 DCF 从股价反推市场隐含预期，然后跟基本面现实比对，找出偏差。
>
> 与巴菲特模块的关系：
> - 巴菲特 = 正向 DCF（从基本面算内在价值）
> - 索罗斯 = 反向 DCF（从股价反推隐含预期）+ 偏差分析
> 两者共享 owner_earnings、折现率等基础数据。

### 13.1 反向 DCF：市场在定价什么预期？

```
已知: current_price, current_owner_earnings, discount_rate, payout_ratio
求解: implied_growth_rate

使得: DCF(owner_earnings, implied_growth_rate, discount_rate) = current_price

即: Σ [OE × (1+g)^t × (1-payout)] / (1+r)^t + 永续价值 = current_price
```

| 所需数据 | 来源 |
|---------|------|
| `current_price` | `[外部]` Yahoo Finance / Alpha Vantage（个股价格） |
| `current_owner_earnings` | 巴菲特模块计算（Section 3） |
| `discount_rate` | `[外部]` FRED: `DGS10`（10Y 国债） |
| `payout_ratio` | 巴菲特模块计算（Section 5） |
| `shares_outstanding` | `[现有]` financial_line_items: `basic_weighted_average_shares` |

### 13.2 预期偏差计算

```
implied_growth_rate = 反向 DCF 求解结果（市场认为的增速）
actual_growth_rate  = 历史收入/OE 增速（Anchor 数据）
guided_growth_rate  = 管理层 guidance（如有）

expectation_gap = implied_growth_rate - actual_growth_rate
  正值 = 市场过度乐观（定价了比现实更高的增速）
  负值 = 市场过度悲观（定价了比现实更低的增速）

gap_trend = 本期 expectation_gap - 上期 expectation_gap
  正值 = 偏差在扩大（反身性在加强）
  负值 = 偏差在收窄（共识正在修正）
```

### 13.3 分析师一致预期偏差

> 除了反向 DCF 的隐含偏差，还可以直接用分析师预期 vs 实际业绩的 surprise。

| 特征 | 计算方式 | 源数据 |
|------|---------|--------|
| `analyst_surprise_pct` | (actual - consensus_estimate) / consensus_estimate | `[新增]` analyst_estimates: `consensus_estimate`, `actual` |
| `consecutive_beats` | 连续超预期的季度数 | `[新增]` analyst_estimates 多期 |
| `consecutive_misses` | 连续低于预期的季度数 | 同上 |
| `estimate_revision_direction` | 分析师上调/下调预期的方向和幅度 | `[新增]` analyst_estimates: 同一 target_period 不同 estimate_date 的变化 |

---

## 14. 索罗斯·反身性强度

> 偏差是否在自我强化？公司的基本面是否依赖市场情绪维持？

### 14.1 融资依赖（反身性通道）

> 如果公司靠发股/借钱维持扩张，那股价上涨→融资能力增强→业绩改善→股价继续涨（正反馈）。
> 一旦情绪反转，这个循环会反向崩溃。

| 特征 | 计算方式 | 源数据 |
|------|---------|--------|
| `financing_dependency` | (股权融资 + 新增借款) / operating_cash_flow | `[现有]` financial_line_items: `proceeds_from_stock_issuance`, `proceeds_from_debt_issuance`, `operating_cash_flow` |
| `share_dilution_rate` | (shares_t - shares_t-1) / shares_t-1 | `[现有]` financial_line_items 跨期: `basic_weighted_average_shares` |
| `equity_issuance_to_capex` | 股权融资 / capex（用股票融的钱占扩张投入的多少） | `[现有]` financial_line_items: `proceeds_from_stock_issuance`, `capital_expenditures` |
| `cash_burn_rate` | -free_cash_flow / cash（现金消耗速度） | `[现有]` financial_line_items: `operating_cash_flow`, `capital_expenditures`, `cash_and_equivalents` |
| `secondary_offering_count` | 近 N 年增发次数 | `[新增]` equity_offerings 表 |
| `goodwill_growth_vs_revenue_growth` | 商誉增速 - 收入增速（高价收购但收入没跟上 = 泡沫信号） | `[现有]` financial_line_items 跨期: `goodwill`, `revenue` |

### 14.2 杠杆动态（加速度）

> 索罗斯关注的不是杠杆高不高，而是杠杆在加速还是减速（二阶导）。
> 加速加杠杆 = 不稳定性在积累。

| 特征 | 计算方式 | 源数据 |
|------|---------|--------|
| `leverage_acceleration` | 债务增速的变化率（debt_growth_t - debt_growth_t-1） | `[现有]` debt_obligations 多期: `principal` |
| `debt_issuance_to_capex` | 新增借款 / capex | `[现有]` financial_line_items: `proceeds_from_debt_issuance`, `capital_expenditures` |

---

## 15. 索罗斯·市场情绪环境

> 市场整体处于自满还是恐慌？用于判断反身性循环处于哪个阶段。

| 特征 | 计算方式 | 源数据 |
|------|---------|--------|
| `vix_level` | VIX 恐慌指数 | `[外部]` CBOE: `^VIX` |
| `vix_percentile` | VIX 在近 2 年分布的百分位 | `[外部]` VIX 历史数据 |
| `margin_debt_growth` | 保证金贷款余额同比增速（市场杠杆水平） | `[外部]` FINRA margin statistics |
| `put_call_ratio` | 看跌/看涨期权比率 | `[外部]` CBOE |
| `credit_spread_momentum` | 信用利差 3 个月变化方向 | `[外部]` FRED: `BAMLH0A0HYM2` |
| `short_interest_ratio` | 做空股数 / 日均成交量（做空拥挤度） | `[外部]` 交易所数据 |

---

## 16. 索罗斯·综合输出

> 合并预期偏差 + 反身性强度 + 市场情绪，输出公司级的反身性评估。

### 16.1 反身性阶段判定

```
阶段 1 — 潜伏期:
  expectation_gap ≈ 0 或略负（市场还没注意到）
  financing_dependency 低
  margin_debt_growth 温和
  → Polaris 标注: "中性/潜伏"

阶段 2 — 自我强化:
  expectation_gap > 0 且 gap_trend > 0（偏差在扩大）
  financing_dependency 上升（公司在利用高估值融资扩张）
  margin_debt_growth 上升但未达历史极值
  → Polaris 标注: "正向反身性·加强中"

阶段 3 — 脆弱/反转临界:
  expectation_gap 极大（implied_growth >> actual_growth）
  financing_dependency 高（基本面依赖市场情绪）
  margin_debt_growth 达历史高分位 或 开始下降
  consecutive_misses > 0（开始低于预期）
  → Polaris 标注: "正向反身性·脆弱，可能反转"

反向同理:
  expectation_gap 极度负值 + 连续超预期 + VIX 高分位
  → Polaris 标注: "负向反身性·过度悲观，可能反弹"
```

### 16.2 Polaris 输出

| 输出字段 | 说明 |
|---------|------|
| `implied_growth_rate` | 当前股价隐含的增长率 |
| `actual_growth_rate` | 基本面实际增长率 |
| `expectation_gap` | 隐含增速 - 实际增速 |
| `gap_trend` | 偏差变化方向（扩大/收窄） |
| `reflexivity_phase` | 当前反身性阶段（中性 / 正向加强 / 正向脆弱 / 负向过度） |
| `financing_dependency` | 融资依赖度 |
| `leverage_acceleration` | 杠杆加速度 |
| `vulnerability_if_reversal` | 如果共识反转，基本面受冲击程度（基于 financing_dependency + cash_burn_rate） |
| `key_narrative` | 当前支撑股价的核心叙事（来自 Anchor 提取，人工审查） |
| `narrative_cracks` | 叙事裂痕信号（consecutive_misses、guidance 下调等） |

> 注：Axion 负责监控实时价格行为（暴跌、成交量异常等），结合 Polaris 的反身性阶段判定做交易决策。

---

## 17. 新增表定义

> Anchor 需要新建的表。每张表列出字段、类型、提取来源。

### 17.1 pricing_actions — 定价行为记录

> 提取来源：财报电话会、年报、新闻稿

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | |
| `company_id` | INTEGER FK | 关联 company_profiles |
| `period` | TEXT | 所属财报期 |
| `product_or_segment` | TEXT | 涉及的产品/服务/业务线 |
| `price_change_pct` | FLOAT | 价格变化百分比（+涨价 / -降价） |
| `volume_impact_pct` | FLOAT | 提价后销量变化百分比（可空，不一定披露） |
| `effective_date` | DATE | 生效日期 |
| `raw_post_id` | INTEGER FK | 来源文章 |

### 17.2 competitor_relations — 竞对关系

> 提取来源：年报竞争分析章节、行业报告、财报电话会

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | |
| `company_id` | INTEGER FK | 本公司 |
| `competitor_name` | TEXT | 竞对名称（待归一化为 company_id） |
| `competitor_company_id` | INTEGER FK | 竞对公司（如已入库）。可空 |
| `market_segment` | TEXT | 竞争所在的细分市场 |
| `relationship_type` | TEXT | direct_competitor / indirect_competitor / potential_entrant |
| `raw_post_id` | INTEGER FK | 来源文章 |

### 17.3 market_share_data — 市占率数据

> 提取来源：行业报告、财报中引用的第三方数据、新闻

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | |
| `company_id` | INTEGER FK | |
| `market_segment` | TEXT | 细分市场 |
| `period` | TEXT | 时间段 |
| `share_pct` | FLOAT | 市占率百分比 |
| `source_description` | TEXT | 数据来源描述（如"IDC Q3 2025 report"） |
| `raw_post_id` | INTEGER FK | 来源文章 |

### 17.4 known_issues — 已知问题清单

> 提取来源：分析师报告、新闻报道、诉讼记录、财务数据异常
> 每条记录 = 一个外部可识别的公司问题

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | |
| `company_id` | INTEGER FK | |
| `period` | TEXT | 问题出现的时间段 |
| `issue_description` | TEXT | 问题描述 |
| `issue_category` | TEXT | financial / operational / legal / reputational / regulatory |
| `severity` | TEXT | critical / major / minor |
| `source_type` | TEXT | analyst_report / news / litigation / financial_anomaly |
| `raw_post_id` | INTEGER FK | 来源文章 |

### 17.5 management_acknowledgments — 管理层问题回应

> 提取来源：致股东信、财报电话会 Q&A
> 每条记录 = 管理层对某个问题的主动提及或回应

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | |
| `company_id` | INTEGER FK | |
| `period` | TEXT | |
| `known_issue_id` | INTEGER FK | 关联 known_issues（可空，如果管理层主动提及的问题不在已知清单中） |
| `issue_description` | TEXT | 管理层描述的问题（用于无法关联 known_issue 时） |
| `response_quality` | TEXT | forthright / downplay / deflect / deny |
| `has_action_plan` | BOOLEAN | 是否给出了具体改进措施 |
| `raw_post_id` | INTEGER FK | 来源文章（致股东信/电话会） |

### 17.6 insider_transactions — 内部人交易

> 提取来源：SEC Form 4 / 港交所权益披露

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | |
| `company_id` | INTEGER FK | |
| `person_name` | TEXT | |
| `title` | TEXT | CEO / CFO / Director / ... |
| `transaction_type` | TEXT | buy / sell / option_exercise |
| `shares` | INTEGER | 交易股数 |
| `price_per_share` | FLOAT | 交易价格 |
| `transaction_date` | DATE | |
| `raw_post_id` | INTEGER FK | |

### 17.7 executive_changes — 高管变动

> 提取来源：公司公告、新闻

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | |
| `company_id` | INTEGER FK | |
| `person_name` | TEXT | |
| `title` | TEXT | |
| `change_type` | TEXT | joined / departed / promoted / demoted |
| `date` | DATE | |
| `reason` | TEXT | 可空，离职原因 |
| `raw_post_id` | INTEGER FK | |

### 17.8 audit_opinions — 审计意见

> 提取来源：年报审计报告页

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | |
| `company_id` | INTEGER FK | |
| `period` | TEXT | 财年 |
| `opinion_type` | TEXT | unqualified / qualified / adverse / disclaimer |
| `auditor_name` | TEXT | 审计师事务所 |
| `emphasis_matters` | TEXT | 强调事项段内容（可空） |
| `raw_post_id` | INTEGER FK | |

### 17.9 analyst_estimates — 分析师预期

> 提取来源：分析师报告、Bloomberg/FactSet（如可接入）

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | |
| `company_id` | INTEGER FK | |
| `period` | TEXT | 预测的目标期 |
| `metric` | TEXT | eps / revenue / ebitda |
| `consensus_estimate` | FLOAT | 一致预期值 |
| `actual` | FLOAT | 实际值（财报发布后填入） |
| `surprise_pct` | FLOAT | (actual - estimate) / estimate |
| `estimate_date` | DATE | 预期数据的截取日期 |

### 17.10 equity_offerings — 股权融资事件

> 提取来源：公司公告、SEC 文件

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | |
| `company_id` | INTEGER FK | |
| `date` | DATE | |
| `offering_type` | TEXT | ipo / secondary / follow_on / atm / convertible |
| `shares_offered` | INTEGER | |
| `price_per_share` | FLOAT | |
| `total_proceeds` | FLOAT | |
| `raw_post_id` | INTEGER FK | |

### 17.11 management_guidance — 管理层前瞻指引

> 提取来源：财报电话会、年报、投资者日演讲
> 每条记录 = 管理层对某个指标的一条前瞻指引
> 同一份电话会可能产出多条 guidance（收入增速、利润率、capex 等各一条）

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | |
| `company_id` | INTEGER FK | |
| `source_period` | TEXT | 发布 guidance 的财报期（如 FY2025Q3） |
| `target_period` | TEXT | guidance 指向的目标期（如 FY2026 或 FY2026Q1） |
| `metric` | TEXT | 指标类型（见下方枚举） |
| `value_low` | FLOAT | 指引下限（如给区间 10%-15%，此处填 0.10） |
| `value_high` | FLOAT | 指引上限（如给区间 10%-15%，此处填 0.15；如给单一值，同 value_low） |
| `unit` | TEXT | pct / absolute / per_share |
| `confidence_language` | TEXT | 管理层用词的确定性（expect / target / aspire / preliminary） |
| `verbatim` | TEXT | 原文引用（用于溯源和审计） |
| `raw_post_id` | INTEGER FK | 来源文章 |

**metric 枚举值：**

| metric 值 | 含义 | Polaris 估值路径 |
|-----------|------|--------------|
| `revenue` | 收入绝对值 | 路径 B |
| `revenue_growth` | 收入增速 | 路径 B |
| `operating_margin` | 营业利润率 | 路径 D |
| `net_margin` | 净利率 | 路径 D |
| `eps` | 每股收益 | 路径 C |
| `capex` | 资本开支 | 路径 A |
| `roic_target` | 再投资回报率目标 | 路径 A |
| `free_cash_flow` | 自由现金流 | 辅助验证 |
| `gross_margin` | 毛利率 | 辅助验证 |
| `tax_rate` | 有效税率 | 辅助计算 |
| `share_repurchase` | 回购计划金额 | 资本配置分析 |
| `dividend` | 分红计划 | 资本配置分析 |
| `other` | 其他定量指引 | 人工审查 |

---

## 18. financial_line_items 必需 item_key 清单

> Anchor 的 financial_line_items 是长表（每行一个科目）。以下是 Polaris 全部特征所需的 item_key。
> Anchor 须确保提取逻辑覆盖这些科目。

| item_key | 中文 | 依赖此字段的 Polaris 特征 |
|----------|------|----------------------|
| `revenue` | 营业收入 | gross_margin, net_margin, operating_margin, free_cash_flow_margin, capex_to_revenue, owner_earnings_margin, interest_to_revenue, revenue_growth_yoy, 多个 _vs_revenue 系列, 多个 stability 系列 |
| `cost_of_revenue` | 营业成本 | gross_margin, gross_margin_stability, consecutive_margin_expansion |
| `net_income` | 净利润 | net_margin, roe, owner_earnings 系列, ocf_to_net_income, accruals_ratio, dividend_payout_ratio, buyback_to_net_income, shareholder_yield |
| `operating_income` | 营业利润 | operating_margin, roic, incremental_roic, interest_coverage, net_debt_to_ebitda（算 EBITDA） |
| `operating_cash_flow` | 经营活动现金流 | ocf_to_net_income, free_cash_flow_margin, capex_to_ocf, accruals_ratio, debt_service_burden, financing_dependency, cash_burn_rate, 多个 _growth 系列 |
| `capital_expenditures` | 资本开支 | capex_to_revenue, capex_to_ocf, depreciation_to_capex, owner_earnings 系列, free_cash_flow_margin, equity_issuance_to_capex, debt_issuance_to_capex |
| `depreciation_amortization` | 折旧摊销 | owner_earnings 系列, depreciation_to_capex, net_debt_to_ebitda（算 EBITDA） |
| `shareholders_equity` | 股东权益 | roe, debt_to_equity |
| `total_assets` | 总资产 | accruals_ratio, goodwill_to_assets |
| `total_debt` | 总有息负债 | debt_to_equity, debt_to_owner_earnings, net_debt_to_ebitda, 多个 debt_growth 系列 |
| `interest_expense` | 利息费用 | interest_coverage, debt_service_burden, interest_to_revenue |
| `current_assets` | 流动资产 | current_ratio |
| `current_liabilities` | 流动负债 | current_ratio |
| `cash_and_equivalents` | 现金及等价物 | net_debt_to_ebitda, cash_to_short_term_debt, cash_burn_rate |
| `goodwill` | 商誉 | goodwill_to_assets, goodwill_growth_vs_revenue_growth |
| `accounts_receivable` | 应收账款 | receivables_growth_vs_revenue |
| `inventory` | 存货 | inventory_growth_vs_revenue |
| `dividends_paid` | 已付股利 | dividend_payout_ratio, shareholder_yield |
| `share_repurchase` | 股票回购 | buyback_to_net_income, shareholder_yield |
| `proceeds_from_stock_issuance` | 股权融资收入 | equity_issuance_to_capex, financing_dependency |
| `proceeds_from_debt_issuance` | 新增借款 | debt_issuance_to_capex, financing_dependency |
| `basic_weighted_average_shares` | 加权平均股数 | share_dilution_rate, share_count_trend |
| `sga_expense` | 销售及管理费用 | sga_to_revenue |
| `rnd_expense` | 研发费用 | rnd_to_revenue |
| `acquisitions_net` | 收购净支出 | acquisition_spend_to_ocf |
| `maintenance_capex` | 维持性资本开支 | maintenance_capex_ratio（可能无法提取，用 D&A 近似） |

### 18.1 现有 Anchor 表字段确认清单

> 以下字段已被 Polaris 特征引用，需确认 Anchor 提取逻辑已覆盖。

| Anchor 表 | 字段 | 被引用的 Polaris 特征 |
|-----------|------|-------------------|
| downstream_segments | `is_recurring` | recurring_revenue_pct |
| downstream_segments | `revenue_pct` | top_customer_concentration, top3_customer_concentration |
| downstream_segments | `revenue_type` | revenue_type_diversity |
| downstream_segments | `backlog` | backlog_coverage |
| upstream_segments | `is_sole_source` | sole_source_pct |
| upstream_segments | `geographic_location` | supplier_geo_concentration |
| upstream_segments | `purchase_obligation` | purchase_obligation_to_revenue |
| upstream_segments | `lead_time` | long_lead_time_pct |
| geographic_revenues | `region` | top_region_concentration, china_revenue_share |
| geographic_revenues | `revenue_share` | top_region_concentration, geo_diversity, china_revenue_share |
| debt_obligations | `principal` | total_debt_principal, current_debt_pct, weighted_avg_interest_rate, debt_maturity_within_1y |
| debt_obligations | `interest_rate` | weighted_avg_interest_rate |
| debt_obligations | `maturity_date` | debt_maturity_within_1y, refinancing_wall |
| debt_obligations | `is_current` | current_debt_pct, cash_to_short_term_debt |
| executive_compensations | `pay_ratio` | ceo_pay_ratio |
| executive_compensations | `stock_awards` | exec_stock_award_pct |
| executive_compensations | `total_comp` | exec_stock_award_pct |
| stock_ownership | `title` | mgmt_ownership_pct |
| stock_ownership | `percent_of_class` | mgmt_ownership_pct, top5_ownership_concentration |
| company_narratives | `status` | narrative_fulfillment_rate, narrative_count |
| litigations | `status` | litigation_count |
| litigations | `accrued_amount` | litigation_accrued_total |
| litigations | `claimed_amount` | litigation_claimed_to_accrued |
| related_party_transactions | `amount` | related_party_amount_to_revenue |
| related_party_transactions | `is_ongoing` | related_party_ongoing_count |
| operational_issues | `risk` | risk_issue_pct |
| operational_issues | `guidance` | guidance_issue_pct |

---

## 统计

| 类别 | 新增表数 | 新增字段数（现有表） | 特征总数 |
|------|---------|-------------------|---------|
| 巴菲特·过滤+评分 | 7（pricing_actions, competitor_relations, market_share_data, known_issues, management_acknowledgments, insider_transactions, executive_changes） | 3（downstream_segments: contract_duration_months, switching_cost_level, is_new_customer/is_churned） | ~60 |
| 巴菲特·内在价值 | 1（management_guidance） | 0 | 5（intrinsic_value + 4 个路径输出） |
| 达利欧·环境引擎 | 0 | 0 | 8 输入 + 8 输出（全部 `[外部]` 市场数据） |
| 达利欧·风险平价 | 0 | 0 | 4 输出（全部 `[外部]` 价格数据计算） |
| 达利欧·公司脆弱度 | 0 | 1（debt_obligations: is_floating_rate） | ~10（`[现有]` Anchor 数据） |
| 索罗斯·预期偏差 | 1（analyst_estimates） | 0 | ~6（反向 DCF + 分析师偏差） |
| 索罗斯·反身性强度 | 1（equity_offerings） | 0 | ~8（融资依赖 + 杠杆动态） |
| 索罗斯·市场情绪 | 0 | 0 | 6（全部 `[外部]` 市场数据） |
| 索罗斯·综合输出 | 0 | 0 | 10 输出（反身性阶段 + 偏差 + 叙事） |
| 审计 | 1（audit_opinions） | 0 | 1 |
| **合计** | **11 张新表** | **~4 个新字段** | **~117** |

---

> 文档状态：v0.4（2026-03-16）
> v0.4：新增「通用数据标准」——期间粒度、历史深度（20 季度最低要求）、外部数据源完整清单（14 项）、跨期查询约定
> v0.3：重写索罗斯模块——预期偏差引擎（反向 DCF）+ 反身性强度 + 市场情绪 + 综合输出（四阶段判定）；重写达利欧模块——环境引擎（四变量象限）+ 风险平价引擎 + 公司级脆弱度
> v0.2：新增 Section 9 巴菲特·内在价值估算（4 条计算路径 + management_guidance 表）
> 待后续对齐：Anchor 确认每张表的提取可行性和 item_key 命名
