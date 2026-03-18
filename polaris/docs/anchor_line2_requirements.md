# Anchor 数据需求 — 线 2 人和环境（诚信/管理层人格/风险）

---

## A. 诚信检测

### 已有表，需填充

| 表名 | 当前状态 | 用途 |
|------|---------|------|
| `audit_opinions` | 已定义，空 | 硬证据：审计非标一票否决 |
| `known_issues` | 已定义，空 | 第三方发现的已知问题 |
| `management_acknowledgments` | 已定义，空 | 管理层承认的问题（做差集） |

### 诚信检测对这三张表的具体需求

**audit_opinions** — 无变化，按现有定义填充即可

**known_issues** — 提取来源需扩展：
- 现有来源：分析师报告、新闻报道、诉讼记录
- 新增来源：**财报数据异常**（Polaris 自动检测，不需要 Anchor）
- 关键字段：`issue_category`（financial/operational/legal/reputational/regulatory）, `severity`（critical/major/minor）

**management_acknowledgments** — 提取来源明确：
- 致股东信（最重要，年度一次）
- 财报电话会 Q&A
- 投资者日发言
- 关键字段：`response_quality`（forthright/downplay/deflect/deny）, `has_action_plan`

### 差集匹配的局限

> 当前 Polaris 用关键词匹配 known_issues vs management_acknowledgments。
> 精度不够（"供应链瓶颈" vs "供应链挑战" 可能匹配不上）。
>
> 建议：Anchor 在提取 management_acknowledgments 时，直接标注 `known_issue_id`（关联到具体的 known_issue），
> 这样 Polaris 不需要做文本匹配，直接看哪些 known_issue 没有对应的 acknowledgment。

---

## B. 管理层人格

### 已有表，需填充

| 表名 | 用途 |
|------|------|
| `company_narratives` | 承诺/愿景，算兑现率 |
| `executive_compensations` | CEO 薪酬比 |
| `stock_ownership` | 管理层持股 |
| `executive_changes` | 高管变动（文化信号） |

### 新增表

#### executive_history — 管理层个人履历 `[新增]`

> 提取来源：新闻、采访、LinkedIn、百科、公司公告
> 每条记录 = 一段任职经历

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | |
| `company_id` | INTEGER FK | 当前任职公司（关联用） |
| `person_name` | TEXT | |
| `current_title` | TEXT | 当前职位（CEO/CFO/创始人） |
| `prior_company` | TEXT | 之前任职公司 |
| `prior_role` | TEXT | 之前职位 |
| `prior_period_start` | TEXT | 任职开始 |
| `prior_period_end` | TEXT | 任职结束 |
| `outcome` | TEXT | failed / acquired / successful / ongoing / resigned |
| `event_description` | TEXT | 关键事件描述（失败原因、成功经历等） |
| `raw_post_id` | INTEGER FK | 来源 |

**Polaris 用法：**
- 找当前 CEO/创始人的 `outcome=failed` 记录
- 有失败后又有成功 → "经历过失败又站起来"
- 多次失败无成功 → 不一定是好信号

#### mission_statements — 使命/愿景跨期记录 `[新增]`

> 提取来源：致股东信、年报、公司官网
> 每期一条，用于跨期对比是否动摇

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | |
| `company_id` | INTEGER FK | |
| `period` | TEXT | 哪一年的 |
| `mission` | TEXT | 使命/愿景原文 |
| `core_values` | TEXT | 核心价值观（可空） |
| `strategic_focus` | TEXT | 战略重点 |
| `raw_post_id` | INTEGER FK | |

**Polaris 用法：**
- 跨期对比 `mission` 是否变化
- 频繁变 → 没有坚持
- 稳定不变 → 有信念

### 已有表扩展

| 表名 | 新增字段 | 用途 |
|------|---------|------|
| `company_narratives` | `category` (mission/promise/strategy) | 区分使命和具体承诺 |

---

## C. 风险评估

### 已有表，需填充

| 表名 | 用途 |
|------|------|
| `geographic_revenues` | 地缘政治风险（地区+收入占比） |
| `competitive_dynamics` | 监管变化、技术颠覆事件 |
| `downstream_segments` | 客户集中度 |
| `upstream_segments` | 供应链 sole source |

### 新增表

#### country_risk_ratings — 国家/地区风险评级 `[新增]`

> 来源：Damodaran 国家风险数据、世界银行、标普主权评级
> 替代代码中硬编码的高风险地区名单

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | |
| `country_code` | TEXT | ISO 2 字母代码 |
| `country_name` | TEXT | |
| `risk_level` | TEXT | catastrophic / high / medium / low |
| `erp` | FLOAT | 股权风险溢价（同时供 DCF 使用） |
| `source` | TEXT | 数据来源 |
| `updated_at` | DATE | |

**Polaris 用法：**
- 替代硬编码的 HIGH_RISK_REGIONS / MEDIUM_RISK_REGIONS
- `erp` 字段同时供 DCF 折现率使用，替代硬编码的 MARKET_ERP

---

## 新增表汇总

| 表名 | 用途 | 优先级 |
|------|------|--------|
| `executive_history` | 管理层个人履历（失败后站起来） | P1 |
| `mission_statements` | 使命/愿景跨期对比 | P1 |
| `country_risk_ratings` | 国家风险评级 + ERP | P1 |

## 已有表需填充（优先级排序）

| 表名 | 优先级 | 理由 |
|------|--------|------|
| `known_issues` | **P0** | 诚信检测核心：差集的一半 |
| `management_acknowledgments` | **P0** | 诚信检测核心：差集的另一半 |
| `audit_opinions` | P0 | 硬证据一票否决 |
| `executive_changes` | P1 | 文化信号、关键人变动 |
| `company_narratives` | P1 | 兑现率（已有结构，需持续填充） |

## 已有表扩展

| 表名 | 变化 | 优先级 |
|------|------|--------|
| `company_narratives` | 新增 `category` 字段 | P2 |
| `management_acknowledgments` | 建议 Anchor 直接标注 `known_issue_id` 避免文本匹配 | P1 |
