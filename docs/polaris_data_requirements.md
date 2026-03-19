# Polaris 数据需求 — 巴菲特因果链

> 基于 26 家公司实战验证的最终版本数据需求。
> 目标：Anchor 提供结构化数据，Polaris 消费并评估。

---

## 一、数据总览

Polaris 消费 **25 张表**，分四大类：

| 类别 | 表数 | 用途 |
|------|------|------|
| 财务数据 | 3 | 盈余能力、利润分配、DCF |
| 业务结构 | 6 | 护城河、生意画像、可预测性 |
| 治理与诚信 | 9 | 管理层评估、诚信检测 |
| 市场与竞争 | 7 | 护城河、风险评估 |

---

## 二、逐表需求

### A. 财务数据（来源：10-K/年报/财报）

#### 1. financial_line_items ⭐ 最核心
- **用途**：盈余能力、利润分配、DCF、可预测性
- **必需字段**：item_key, value, period
- **必需 item_key**（26 个）：

| item_key | 中文 | 用于哪个模块 |
|----------|------|------------|
| revenue | 收入 | 全部 |
| cost_of_revenue | 营业成本 | 护城河（毛利率） |
| operating_income | 营业利润 | ROIC、盈余 |
| net_income | 净利润 | 盈余、分配 |
| operating_cash_flow | 经营现金流 | 盈余（利润真实性） |
| capital_expenditures | 资本支出 | 盈余（主动投资） |
| depreciation_amortization | 折旧摊销 | OE 计算 |
| shareholders_equity | 股东权益 | ROE、D/E、资本结构 |
| total_assets | 总资产 | 商誉比率、权益比率 |
| interest_expense | 利息支出 | 利息覆盖率 |
| goodwill | 商誉 | 商誉/资产比 |
| accounts_receivable | 应收账款 | 应收增速 |
| inventory | 存货 | 存货增速 |
| cash_and_equivalents | 现金 | 净债务、IC 计算 |
| total_debt | 总债务 | D/E、净债务/EBITDA |
| current_assets | 流动资产 | 流动比率 |
| current_liabilities | 流动负债 | IC 计算 |
| dividends_paid | 分红 | 利润分配 |
| share_repurchase | 回购 | 利润分配、稀释率 |
| sga_expense | 销售管理费 | 毛利率计算 |
| rnd_expense | 研发费用 | 盈余（主动投资） |
| basic_weighted_average_shares | 加权平均股数 | 稀释率 |
| income_tax_expense_total | 所得税 | 税率 |
| income_before_tax_total | 税前利润 | 税率 |
| proceeds_from_stock_issuance | 股权融资 | 稀释检测 |
| proceeds_from_debt_issuance | 新增借债 | 借新还旧检测 |

- **时间范围**：至少 4 期（年度），8 期更好（覆盖完整周期）
- **数据来源**：10-K / 年报 / 财报 PDF → LLM 提取
- **时效性**：年报发布后更新（通常 Q1 发布上一年年报）
- **Anchor 现状**：✅ FinancialLineItem 表已有，提取管线已实现

#### 2. debt_obligations
- **用途**：债务结构分析（当前链条未深度使用，预留）
- **Anchor 现状**：✅ DebtObligation 表已有

#### 3. management_guidance
- **用途**：DCF 估值的增长率输入
- **必需字段**：metric（revenue_growth / operating_margin / eps / capex / roic_target）, value
- **数据来源**：财报电话会、投资者日、年报展望
- **时效性**：每季度更新
- **Anchor 现状**：✅ ManagementGuidance 表已有

---

### B. 业务结构（来源：10-K 业务描述/投资者演示）

#### 4. downstream_segments ⭐ 非常重要
- **用途**：生意画像、护城河（转换成本/生态锁定）、风险（客户集中）、可预测性
- **必需字段**：

| 字段 | 用途 | 重要性 |
|------|------|--------|
| customer_name | 业务线/客户名称 | ⭐ 必需 |
| revenue_pct | 收入占比 | ⭐ 必需 |
| product_category | 产品品类 | ⭐ 必需（驱动生意画像） |
| revenue_type | 收入类型 | ⭐ 必需（subscription/transaction_fee/ad_revenue/license/recurring） |
| is_recurring | 是否经常性收入 | 重要 |
| switching_cost_level | 转换成本高低 | 重要（high/medium/low） |
| contract_duration | 合同期限 | 重要（multi-year/5-year/1-year） |
| product_criticality | 产品关键性 | 重要（high = 出事代价大） |
| segment_gross_margin | **业务线毛利率**（新增） | 重要（分业务线同行对比） |

- **product_category 标准值**：beverage, liquor, tobacco, food, grocery, consumer_electronics, cloud_infrastructure, operating_system, insurance, banking, payment, healthcare, pharma, gaming, social_media, commodity, industrial_equipment, pipeline
- **数据来源**：10-K "Business" 章节、投资者演示、分析师报告
- **时效性**：年度更新
- **Anchor 现状**：✅ DownstreamSegment 表已有。**缺 segment_gross_margin 字段，需新增**

#### 5. upstream_segments
- **用途**：供应链集中风险
- **必需字段**：supplier_name, is_sole_source, geographic_location
- **Anchor 现状**：✅ 已有

#### 6. geographic_revenues
- **用途**：地缘风险、地理集中度
- **必需字段**：region, revenue_share
- **Anchor 现状**：✅ 已有

#### 7. peer_financials ⭐ 重要
- **用途**：护城河（同行对比）、定价权推断
- **必需字段**：

| 字段 | 用途 |
|------|------|
| peer_name | 同行公司名 |
| metric | gross_margin / operating_margin / net_margin / revenue |
| value | 指标值 |
| period | 时期 |
| **segment** | **对应哪条业务线**（新增，分业务线对比用） |

- **数据来源**：同行公司财报、行业报告
- **时效性**：年度更新
- **Anchor 现状**：⚠️ **目前无独立表**。CompetitorRelation 存竞争关系但无财务指标。**需新增 peer_financials 表或扩展 CompetitorRelation**

#### 8. brand_signals
- **用途**：品牌护城河（信任默选）
- **必需字段**：signal_type（viral_praise/organic_mention/pr_crisis/kol_attack/quality_incident）, sentiment_score
- **数据来源**：社交媒体情绪分析
- **时效性**：实时/周级
- **Anchor 现状**：⚠️ **无此表。预留，优先级低**

#### 9. non_financial_kpis
- **用途**：数据网络效应检测（MAU/DAU/数据量）
- **Anchor 现状**：✅ 已有

---

### C. 治理与诚信（来源：Proxy/年报/新闻）

#### 10. audit_opinions
- **用途**：诚信硬证据（非标审计意见 = 一票否决）
- **必需字段**：opinion_type（unqualified/qualified/adverse/disclaimer）, emphasis_matters
- **数据来源**：年报审计报告页
- **Anchor 现状**：✅ 已有

#### 11. known_issues ⭐ 重要
- **用途**：诚信检测（第三方发现的问题）
- **必需字段**：issue_description, severity（critical/high/medium/low）, source_type（news/analyst/regulatory/financial）
- **数据来源**：新闻、分析师报告、监管公告
- **时效性**：实时/周级
- **Anchor 现状**：✅ 已有

#### 12. management_acknowledgments ⭐ 重要
- **用途**：诚信差集（管理层承认了什么）
- **必需字段**：issue_description, response_quality（strong/adequate/defensive）, has_action_plan（bool）
- **数据来源**：财报电话会、致股东信、投资者日
- **时效性**：季度更新
- **Anchor 现状**：✅ 已有

#### 13. company_narratives
- **用途**：管理层人格（言）+ 叙事兑现率
- **必需字段**：narrative, status（delivered/missed/in_progress）
- **数据来源**：致股东信、投资者日、战略发布会
- **时效性**：年度/半年度
- **Anchor 现状**：✅ CompanyNarrative 已有（有 capital_required, promised_outcome, deadline 等字段）

#### 14. executive_changes
- **用途**：管理层稳定性、继任信号、关键人风险
- **必需字段**：name, title, change_type（joined/departed）
- **数据来源**：SEC Form 8-K、新闻公告
- **Anchor 现状**：✅ ExecutiveChange 已有

#### 15. stock_ownership
- **用途**：管理层持股（利益绑定 vs 权力控制）
- **必需字段**：name, title, percent_of_class
- **Anchor 现状**：✅ StockOwnership 已有

#### 16. executive_compensations
- **用途**：CEO Pay Ratio（薪酬失控检测）
- **必需字段**：name, role_type, pay_ratio, stock_awards, total_comp
- **Anchor 现状**：✅ ExecutiveCompensation 已有

#### 17. litigations
- **用途**：诉讼风险
- **必需字段**：status（pending/ongoing/resolved）, claimed_amount, description
- **Anchor 现状**：✅ Litigation 已有

#### 18. related_party_transactions
- **用途**：利益输送风险
- **Anchor 现状**：✅ 已有

---

### D. 市场与竞争（来源：行业报告/新闻/SEC）

#### 19. pricing_actions ⭐ 重要
- **用途**：涨价测试（护城河定价权）
- **必需字段**：price_change_pct, product_or_segment, effective_date, volume_impact_pct
- **可选字段**：price_vs_peers（higher/lower/similar）
- **数据来源**：公司公告、新闻、分析师报告
- **Anchor 现状**：✅ PricingAction 已有

#### 20. market_share_data ⭐ 重要
- **用途**：护城河验证（份额下滑 = 护城河瓦解）
- **必需字段**：period, share, source
- **可选字段**：market_segment
- **数据来源**：行业报告（IDC/Gartner/Statista）、公司披露
- **时效性**：年度/半年度
- **Anchor 现状**：✅ MarketShareData 已有

#### 21. competitive_dynamics ⭐ 非常重要
- **用途**：护城河（竞品进攻/低谷存活/专利/监管）、风险（政策/技术颠覆）
- **必需字段**：

| 字段 | 用途 |
|------|------|
| competitor_name | 竞争对手名 |
| event_type | 事件类型（见下表） |
| event_description | 事件描述 |
| outcome_description | 结果描述 |
| outcome_market_share_change | 份额变化 |

- **event_type 标准值**：

| event_type | 护城河模块怎么用 |
|-----------|----------------|
| price_war | 涨价测试路由、低谷存活 |
| new_entry | 有效规模（新进入者失败）、网络效应 |
| exit | 有效规模（玩家退出） |
| product_launch | 竞品进攻防守 |
| patent_challenge | 专利护城河（挑战被击退 = 正面） |
| patent_expiration | 专利到期（护城河有时间限制） |
| regulatory_change | 监管风险、牌照壁垒 |
| industry_downturn | 低谷存活测试 |
| migration_tool | 数据迁移壁垒（竞品降低门槛 = 负面） |

- **数据来源**：新闻、行业报告、分析师研究
- **时效性**：实时/月级
- **Anchor 现状**：⚠️ **CompetitorRelation 表结构不完全匹配**。有 competitive_dynamics 的概念但字段可能不全。需核实并补全 event_type 和 outcome 字段

#### 22-25. 其他表
- **insider_transactions** — 内幕交易（SEC Form 4），Anchor ✅ 已有
- **equity_offerings** — 股权发行，Anchor ✅ 已有
- **analyst_estimates** — 分析师预估，Anchor ✅ 已有
- **operational_issues** — 运营问题，Anchor ✅ 已有

---

### E. 市场数据（外部 API）

#### DCF 估值需要的外部数据

| 数据 | 用途 | 来源 | 时效性 |
|------|------|------|--------|
| 当前股价 | 安全边际 | StockQuote / yfinance | 日级 |
| 流通股数 | 每股内在价值 | StockQuote / 10-K | 季度 |
| 10Y 国债利率 | 无风险利率（折现率） | MacroIndicator / FRED | 日级 |

- **Anchor 现状**：✅ StockQuote + MacroIndicator 表已有

---

## 三、数据缺口汇总

| 优先级 | 缺什么 | 改动量 | 说明 |
|--------|--------|--------|------|
| 🔴 P0 | peer_financials 表 | 新建表 + 提取管线 | 护城河同行对比的核心数据 |
| 🔴 P0 | downstream_segments 加 segment_gross_margin | 加字段 | 分业务线对比 |
| 🟡 P1 | competitive_dynamics 字段补全 | 对齐字段 | event_type 标准值、outcome 字段 |
| 🟡 P1 | company_narratives 加 status 字段 | 加字段 | 叙事兑现率需要 delivered/missed/in_progress |
| 🟢 P2 | brand_signals 表 | 新建表 | 品牌舆情，可后做 |

---

## 四、数据刷新策略

| 数据类别 | 刷新频率 | 触发条件 |
|---------|---------|---------|
| 财务数据 | 年度 | 年报/10-K 发布后 |
| 业务结构 | 年度 | 年报发布后 |
| 同行财务 | 年度 | 对标公司年报发布后 |
| 治理数据 | 季度 | 财报电话会后 |
| 竞争动态 | 实时/月级 | 新闻/行业事件触发 |
| 市场数据 | 日级 | 交易日自动更新 |
| 叙事兑现 | 半年度 | 年中/年末复盘 |
