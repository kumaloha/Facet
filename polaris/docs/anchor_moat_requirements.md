# Anchor 数据需求 — 护城河检测

> Polaris 因果链·护城河检测的完整数据需求。
> 每张表标明: 已有/新增/扩展。

---

## 检测流程（决策树）

```
Step 0: 伪护城河排除
  → 亏损+烧钱+稀释？→ 是 → 无护城河，终止

Step 1: 涨价测试（路由: 品牌 vs 成本优势）
  → 涨价 + 份额不跌 + 比竞品贵 → 品牌定价权
  → 涨价 + 份额不跌 + 比竞品便宜 → 成本优势型定价
  → 涨价 + 份额不跌 + 不知道 → 定价能力待定
  → 涨价 + 份额跌了 → 无定价权

Step 2: 竞品进攻测试
  → 全部失败 → 护城河强
  → 有胜有败 → 护城河在削弱
  → 竞品成功 → 护城河弱

Step 3-7: 五大类检测（见下方数据需求）

Step 8: 深度判定
  → 行为证据 + 多类叠加 → 极深
  → 行为证据 + 单类     → 深
  → 结构证据 + 多类     → 深
  → 结构证据 + 单类     → 浅
  → 仅间接信号          → 数据不足
```

---

## 数据需求总表

### 1. pricing_actions `[已定义，需扩展]`

> 涨价测试的核心数据

| 字段 | 类型 | 说明 | 状态 |
|------|------|------|------|
| `id` | INTEGER PK | | 已有 |
| `company_id` | INTEGER FK | | 已有 |
| `period` | TEXT | 所属财报期 | 已有 |
| `product_or_segment` | TEXT | 涉及产品 | 已有 |
| `price_change_pct` | FLOAT | 价格变化 % | 已有 |
| `volume_impact_pct` | FLOAT | 提价后销量变化 | 已有 |
| `price_vs_peers` | TEXT | 涨价后相对竞品价格 (higher/lower/similar) | **新增** |
| `effective_date` | DATE | | 已有 |
| `raw_post_id` | INTEGER FK | | 已有 |

### 2. market_share_data `[已定义，无变化]`

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | |
| `company_id` | INTEGER FK | |
| `market_segment` | TEXT | 细分市场（支持城市级用于本地网络效应） |
| `period` | TEXT | |
| `share` | FLOAT | 市占率 |
| `source` | TEXT | 数据来源 |
| `raw_post_id` | INTEGER FK | |

### 3. competitive_dynamics `[新增]`

> 竞品进攻、行业低谷、反定位等事件

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | |
| `company_id` | INTEGER FK | 受影响的公司 |
| `competitor_name` | TEXT | 发起动作的竞对/新进入者 |
| `event_type` | TEXT | 见枚举 |
| `event_description` | TEXT | 事件描述 |
| `estimated_investment` | FLOAT | 竞对投入估算（可空） |
| `outcome_description` | TEXT | 结果描述 |
| `outcome_market_share_change` | FLOAT | 份额变化（可空） |
| `event_date` | DATE | |
| `raw_post_id` | INTEGER FK | |

**event_type 枚举：**

| 值 | 用于检测 | 例子 |
|----|---------|------|
| `new_entry` | 网络效应、有效规模 | DeepSeek 进入 |
| `product_launch` | 网络效应、know-how | 飞聊挑战微信 |
| `price_war` | 成本优势·低谷存活 | Sam's Club 降价 |
| `migration_tool` | 转换成本·数据迁移削弱 | 竞品推一键迁移 |
| `talent_poaching` | know-how 流失 | 挖核心工程师 |
| `capacity_expansion` | 成本优势·规模 | 竞对建新厂 |
| `exit` | 有效规模 | 竞对退出 |
| `regulatory_change` | 牌照壁垒 | 新发牌照 |
| `industry_downturn` | 成本优势·低谷存活 | 铜价暴跌 |
| `patent_challenge` | 专利 | 专利被挑战 |
| `patent_expiration` | 专利 | 专利到期 |

### 4. brand_signals `[新增]`

> 品牌·信任默选的检测数据

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | |
| `company_id` | INTEGER FK | |
| `signal_type` | TEXT | viral_praise / organic_mention / pr_crisis / kol_attack / quality_incident / sentiment_shift |
| `platform` | TEXT | weibo / xiaohongshu / twitter / douyin / news |
| `description` | TEXT | 事件描述 |
| `sentiment_score` | FLOAT | -1 到 +1（可空） |
| `reach_estimate` | INTEGER | 影响人数估算（可空） |
| `event_date` | DATE | |
| `raw_post_id` | INTEGER FK | |

### 5. peer_financials `[新增]`

> 同行对比的数据。一行 = 一个同行的一个指标。

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | |
| `company_id` | INTEGER FK | 本公司 |
| `peer_name` | TEXT | 同行名称 |
| `peer_company_id` | INTEGER FK | 同行 company_id（如已入库，可空） |
| `metric` | TEXT | gross_margin / operating_margin / net_margin / revenue |
| `value` | FLOAT | 指标值 |
| `period` | TEXT | |
| `source` | TEXT | 数据来源 |

### 6. downstream_segments `[已有，需扩展]`

| 新增字段 | 类型 | 用于检测 |
|---------|------|---------|
| `switching_cost_level` | TEXT (high/medium/low) | 转换成本·系统嵌入 |
| `product_criticality` | TEXT (high/medium/low) | 转换成本·风险不对称 |
| `cost_share_pct` | FLOAT | 转换成本·风险不对称（产品在客户总成本中占比） |
| `renewal_rate` | FLOAT | 转换成本前置信号 |
| `is_dual_sourcing` | BOOLEAN | 转换成本前置信号 |

---

## 数据来源映射

| 护城河类型 | 需要的表 | 数据从哪来 |
|-----------|---------|-----------|
| 品牌·定价权 | pricing_actions + market_share_data | 财报电话会、行业报告 |
| 品牌·信任默选 | brand_signals | 社交媒体、新闻 |
| 专利/IP | competitive_dynamics (patent_*) | 专利局公开数据、诉讼记录 |
| 牌照/特许经营权 | competitive_dynamics (regulatory_change) | 政策新闻、监管公告 |
| 商业秘密/know-how | competitive_dynamics + executive_changes | 行业新闻、人事变动 |
| 独占资源 | company_profile (扩展) | 行业报告 |
| 系统嵌入 | downstream_segments + competitive_dynamics | 财报、客户反馈 |
| 数据迁移 | downstream_segments + competitive_dynamics | 产品动态 |
| 学习成本 | downstream_segments (扩展) | 产品分析 |
| 生态锁定 | downstream_segments + competitive_dynamics | 财报、竞品动态 |
| 合同约束 | downstream_segments | 财报 |
| 风险不对称 | downstream_segments (扩展) | 行业分析 |
| 直接/双边网络效应 | competitive_dynamics | 行业新闻 |
| 数据网络效应 | non_financial_kpis | 公司披露 |
| 本地网络效应 | market_share_data (城市级) | 第三方数据 |
| 成本优势·同行对比 | peer_financials | 同行财报 |
| 成本优势·低谷存活 | competitive_dynamics (industry_downturn) | 行业新闻、财报 |
| 成本优势·规模经济 | peer_financials (revenue) | 同行财报 |
| 成本优势·反定位 | competitive_dynamics | 竞对公告 |
| 地理优势 | 待定 (product_logistics) | 行业分析 |
| 独占低成本资源 | 待定 (resource_data) | 行业报告 |
| 有效规模·自然垄断 | competitive_dynamics (new_entry/exit) | 行业新闻 |
| 有效规模·利基 | market_share_data | 行业报告 |

---

## 新增表汇总

| 表名 | 用途 | 优先级 |
|------|------|--------|
| `competitive_dynamics` | 竞品进攻、行业低谷、反定位、进入退出 | P0 — 最核心 |
| `peer_financials` | 同行财务对比 | P0 — 成本优势检测必需 |
| `brand_signals` | 品牌舆情信号 | P1 — 品牌信任检测 |

## 已有表扩展

| 表名 | 新增字段 | 优先级 |
|------|---------|--------|
| `pricing_actions` | `price_vs_peers` | P0 — 涨价测试路由 |
| `downstream_segments` | `switching_cost_level`, `product_criticality`, `cost_share_pct` | P1 |
| `downstream_segments` | `renewal_rate`, `is_dual_sourcing` | P2 — 前置信号 |
