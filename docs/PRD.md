# Facet — 产品需求文档 (PRD)

> 版本：v1.0
> 更新：2026-03-16
> 合并自：Anchor v9.0 + Polaris v0.4 + Axion v0.1

---

## 目录

**产品篇**

1. [产品定位与愿景](#1-产品定位与愿景)
2. [三层架构](#2-三层架构)
3. [用户画像](#3-用户画像)
4. [核心使用场景](#4-核心使用场景)

**Layer 1: Anchor — 信息提取**

5. [Anchor 功能概览](#5-anchor-功能概览)
6. [Anchor 系统架构](#6-anchor-系统架构)
7. [数据模型](#7-数据模型)
    - [7a. Company 域 13 张表完整 Schema](#7a-company-域-13-张表完整-schema)
    - [7b. 表间关系](#7b-表间关系)
8. [域开关机制](#8-域开关机制)
9. [Company 专用提取管线](#9-company-专用提取管线)
10. [三条链路设计](#10-三条链路设计)
11. [内容路由逻辑](#11-内容路由逻辑)
12. [监控流水线](#12-监控流水线)

**Layer 2: Polaris — 认知模型**

13. [Polaris 两层架构](#13-polaris-两层架构)
14. [七域与传导](#14-七域与传导)
15. [L0 原始特征](#15-l0-原始特征)
16. [L1 传导特征](#16-l1-传导特征)
17. [L2 汇总特征](#17-l2-汇总特征)
18. [三流派评分框架](#18-三流派评分框架)
19. [V1 规则模型](#19-v1-规则模型)
20. [Anchor 输入映射](#20-anchor-输入映射)

**Layer 3: Axion — 量化投资**

21. [Axion 核心功能](#21-axion-核心功能)

**工程**

22. [配置与环境](#22-配置与环境)
23. [技术栈](#23-技术栈)
24. [文件结构](#24-文件结构)

**附录**

25. [系统局限与边界](#25-系统局限与边界)
26. [路线图](#26-路线图)
- [附录 A. 政策分析框架（IPOCC）](#附录-a-政策分析框架ipocc)
- [附录 B. 公司分析框架](#附录-b-公司分析框架)

---

# 产品篇

## 1. 产品定位与愿景

**Facet = 信息提取 → 认知模型 → 量化投资**

三层流水线，把非结构化信息变成投资决策：

- **Anchor（信息提取）**：从财报、政策文件、技术论文中提取域专用结构化数据
- **Polaris（认知模型）**：编码产业认知，输出三大投资流派各自的评分和信号
- **Axion（量化投资）**：估值、仓位、执行——把评分变成钱

核心设计原则：
- **特征层编码领域知识**——七个域之间的传导关系是人工定义的，深度模型无法自动提取
- **模型层以三大流派为评分框架**——巴菲特看公司本身，达利欧看公司与周期的关系，索罗斯看市场认知与现实的偏差
- **在任何市场阶段，真正驱动价格的核心变量只有 1-2 个**——系统的任务是识别当前的「主要矛盾」

**历史演进**：
- v1-v5：观点提取 + 事实验证（七实体 DAG）
- v6：Top-down 提取 + 多模式
- v7：Anchor/Polaris/Axion 三层分离
- v8：通用 ExtractionNode/Edge + 统一 2-call 管线
- v9：域专用数据结构 — 移除 Node/Edge，company 域 13 张表专用管线
- **v1.0（本版本）：三层合并为 Facet，统一 PRD**

---

## 2. 三层架构

```
文章/文件 → [Anchor 信息提取] → 域专用结构化表（per-article 提取）
                                      ↓
         市场数据 → [Polaris 认知模型] → 三流派多维度评分
                                      ↓
         当前定价 → [Axion 量化投资] → 投资决策
```

| 层 | 输入 | 输出 | 状态 |
|----|------|------|------|
| **Anchor** | URL / 文件 | 域专用结构化表 | Company 域可用 |
| **Polaris** | Anchor 表 + 市场数据 | 三流派评分 + 归因 | V1 规则评分可用 |
| **Axion** | Polaris 评分 + 实时行情 | 投资决策 + 仓位 | PRD 阶段 |

**层间约定**：
- Anchor 做 **per-article 提取**（每篇文章独立，不跨文章归一化）
- Polaris 做 **跨期归一化**（同一 company_id 多个 period → 时间序列特征）和 **跨公司比较**
- Axion 做 **估值和决策**（安全边际、仓位、执行）
- 关联键：`company_id` + `period` + `raw_post_id`

---

## 3. 用户画像

### 3.1 产业投资者（首要用户）

**痛点**：需要从 10-K/年报中快速提取公司经营数据、商业模式、管理层叙事。

**Facet 的解法**：
- Anchor 自动提取 13 张表：经营议题、叙事、三表、上下游、地域收入、KPI、债务、诉讼、薪酬、持股、关联交易
- Polaris 三流派评分：巴菲特（内在价值）、达利欧（周期定位）、索罗斯（反身性）
- Axion 估值+决策：DCF、仓位管理、风险预算

### 3.2 政策研究员 / 宏观分析师 / 期货交易员

暂时禁用，待后续域专用管线实现后启用。

---

## 4. 核心使用场景

### 场景 A：公司财报分析（已启用）

```
1. Anchor 采集 RawPost
2. 通用判断：domain=公司, nature=一手信息 → content_mode=company
3. Company 专用提取管线：LLM 一次调用提取 13 张表
4. Polaris 特征计算 → 三流派评分
5. Axion 估值 + 决策（待实现）
```

### 场景 B：非公司内容（暂时禁用）

域开关检查 → 域已禁用，跳过提取。

---

# Layer 1: Anchor — 信息提取

Anchor 负责**数字化**——从非结构化文本中提取**域专用结构化数据**。每篇文章独立提取，不做跨文章归一化（跨期归一化归 Polaris）。

当前只有 **company 域**已实现专用管线（13 张表），其他 5 域暂时禁用。

## 5. Anchor 功能概览

| 功能 | 说明 | 状态 |
|------|------|------|
| 智能采集 | Twitter/X、微博、YouTube、Bilibili、Truth Social、通用 Web | 可用 |
| 订阅监控 | sources.yaml 批量监控 | 可用 |
| 内容质量过滤 | 付费墙、视频时长、文章字数 | 可用 |
| 2D 内容分类 | content_domain × content_nature → content_mode | 可用 |
| **Company 专用提取** | **13 张表结构化提取** | **已启用** |
| 域开关 | 按域启用/禁用提取 | 可用 |
| 事实核查 | 网络检索验证 | 暂停（待域专用验证） |
| 作者档案 | 信誉等级 1-5 | 可用 |

---

## 6. Anchor 系统架构

```
输入 URL / 文档内容
        │
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  采集层（anchor/collect/）                                          │
│  输出：RawPost 存入 DB                                              │
└──────────────────────────────┬───────────────────────────────────┘
                               │
           ┌───────────────────▼──────────────────────┐
           │    通用判断（前置）                           │
           │    → content_domain / content_nature        │
           │    → resolve_content_mode() → 6 种模式       │
           └────────────────────┬─────────────────────┘
                                │
              ┌─────────────────▼─────────────────────┐
              │     域开关检查                            │
              │     settings.is_domain_enabled(mode)    │
              │                                         │
              │     ✅ company → Company 专用管线          │
              │     ❌ 其他 5 域 → 跳过提取               │
              └────────────────────┬──────────────────┘
                                   │ (company only)
              ┌────────────────────▼──────────────────┐
              │     Company 专用提取管线                  │
              │     extract_company_compute()           │
              │     → LLM 一次调用提取 13 张表            │
              │     extract_company_write()             │
              │     → 写入 company_profiles + 12 张表    │
              └───────────────────────────────────────┘
```

---

## 7. 数据模型

### 7.0 基础设施表

| 表名 | 描述 |
|------|------|
| `authors` | 观点作者档案 |
| `raw_posts` | 原始帖子/文档 |
| `monitored_sources` | 监控源 |
| `author_groups` | 跨平台作者实体 |
| `topics` | 话题标签 |
| `post_quality_assessments` | 单篇内容质量评估 |
| `author_stats` | 作者综合统计 |

---

### 7a. Company 域 13 张表完整 Schema

#### `company_profiles` — 公司档案（最小标识）

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | |
| name | VARCHAR INDEX | 公司名 "NVIDIA Corporation" |
| ticker | VARCHAR UNIQUE INDEX | 股票代码 "NVDA" / "600519.SH" |
| market | VARCHAR | us\|cn_a\|cn_h\|hk\|jp |
| industry | VARCHAR | 所属行业 |
| summary | VARCHAR | 一句话商业模式 |
| created_at | DATETIME | |
| updated_at | DATETIME | |

#### `operational_issues` — 经营议题

来源：CEO致股东信 / MD&A。每行 = 一个经营议题，四个维度（表现/归因/风险/指引）。

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | |
| company_id | INTEGER FK→company_profiles | |
| period | VARCHAR INDEX | "FY2025" / "2025Q4" |
| raw_post_id | INTEGER FK→raw_posts | 溯源 |
| topic | VARCHAR | 议题名 ≤30字 |
| performance | TEXT | 表现（定性，不含财务数字）≤200字 |
| attribution | TEXT | 归因 ≤200字 |
| risk | TEXT | 风险 ≤200字 |
| guidance | TEXT | 指引 ≤200字 |
| created_at | DATETIME | |

#### `company_narratives` — 叙事（管理层承诺）

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | |
| company_id | INTEGER FK | |
| raw_post_id | INTEGER FK | |
| narrative | TEXT | 故事/战略承诺 ≤300字 |
| capital_required | FLOAT | 资金量 |
| capital_unit | VARCHAR | 亿美元\|亿人民币 |
| promised_outcome | TEXT | 承诺结果 ≤200字 |
| deadline | DATE | 承诺时间 |
| status | VARCHAR | announced\|in_progress\|delivered\|missed\|abandoned |
| reported_at | DATE | |
| created_at | DATETIME | |

#### `financial_statements` — 财务报表头

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | |
| company_id | INTEGER FK | |
| period | VARCHAR INDEX | "FY2025" |
| period_type | VARCHAR | quarterly\|annual |
| statement_type | VARCHAR INDEX | income\|balance_sheet\|cashflow\|equity\|tax_detail\|sbc_detail |
| currency | VARCHAR | CNY\|USD\|HKD |
| reported_at | DATE | |
| raw_post_id | INTEGER FK | |
| created_at | DATETIME | |

#### `financial_line_items` — 财务科目（长表）

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | |
| statement_id | INTEGER FK→financial_statements | |
| item_key | VARCHAR INDEX | 标准化键 "revenue" / "operating_income" |
| item_label | VARCHAR | 原始标签 "营业收入" |
| value | FLOAT | 数值 |
| parent_key | VARCHAR | 父科目键（层级结构） |
| ordinal | INTEGER | 排列顺序 |
| note | VARCHAR | 备注 |

#### `downstream_segments` — 下游（客户/收入流）

每期每客户一行，segment 可选(null=公司级)。

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | |
| company_id | INTEGER FK | |
| period | VARCHAR INDEX | |
| segment | VARCHAR | 业务线（null=公司级） |
| customer_name | VARCHAR | 客户名或收入流名 |
| customer_type | VARCHAR | direct\|indirect\|channel\|OEM\|distributor |
| products | VARCHAR | 卖给该客户的产品/服务 |
| channels | VARCHAR | 销售渠道 |
| revenue | FLOAT | 收入（百万美元） |
| revenue_pct | FLOAT | 占总收入百分比 |
| growth_yoy | VARCHAR | 同比增速 |
| backlog | FLOAT | 积压订单（百万美元） |
| backlog_note | VARCHAR | 积压说明 |
| pricing_model | VARCHAR | per-unit\|per-user/month\|usage-based\|混合 |
| contract_duration | VARCHAR | one-time\|1-year\|multi-year |
| revenue_type | VARCHAR | product_sale\|subscription\|license\|royalty\|service\|NRE\|cloud_service |
| is_recurring | BOOLEAN | 是否经常性收入 |
| recognition_method | VARCHAR | point_in_time\|over_time |
| description | TEXT | |
| raw_post_id | INTEGER FK | |
| created_at | DATETIME | |

#### `upstream_segments` — 上游（供应商）

每期每供应商一行，segment 可选(null=公司级)。

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | |
| company_id | INTEGER FK | |
| period | VARCHAR INDEX | |
| segment | VARCHAR | 业务线（null=公司级） |
| supplier_name | VARCHAR | 供应商名 |
| supply_type | VARCHAR | foundry\|memory\|assembly_test\|substrate\|component\|contract_mfg\|software\|logistics |
| material_or_service | VARCHAR | 供应内容 |
| process_node | VARCHAR | 制程节点 |
| geographic_location | VARCHAR | 所在地 |
| is_sole_source | BOOLEAN | 独家供应 |
| purchase_obligation | FLOAT | 采购义务（百万美元） |
| lead_time | VARCHAR | 交货周期 |
| contract_type | VARCHAR | 长期合约\|purchase_order\|prepaid\|non-cancellable |
| prepaid_amount | FLOAT | 预付金额 |
| concentration_risk | VARCHAR | 集中度风险 |
| description | TEXT | |
| raw_post_id | INTEGER FK | |
| created_at | DATETIME | |

#### `geographic_revenues` — 地域收入

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | |
| company_id | INTEGER FK | |
| period | VARCHAR INDEX | |
| region | VARCHAR | 地域名 |
| revenue | FLOAT | 收入（百万） |
| revenue_share | FLOAT | 占比（0-1） |
| growth_yoy | VARCHAR | 同比增速 |
| note | VARCHAR | |
| raw_post_id | INTEGER FK | |
| created_at | DATETIME | |

#### `non_financial_kpis` — 非财务KPI

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | |
| company_id | INTEGER FK | |
| period | VARCHAR INDEX | |
| kpi_name | VARCHAR | 指标名 |
| kpi_value | VARCHAR | 值（文本，兼容非数值） |
| kpi_unit | VARCHAR | 单位 |
| yoy_change | VARCHAR | 同比变化 |
| category | VARCHAR | workforce\|customer\|product\|esg\|operational |
| note | VARCHAR | |
| raw_post_id | INTEGER FK | |
| created_at | DATETIME | |

#### `debt_obligations` — 债务/义务明细

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | |
| company_id | INTEGER FK | |
| period | VARCHAR INDEX | |
| instrument_name | VARCHAR | 债务工具名 |
| debt_type | VARCHAR | bond\|loan\|lease\|convertible\|credit_facility |
| principal | FLOAT | 本金（百万） |
| currency | VARCHAR | |
| interest_rate | FLOAT | 年利率 |
| maturity_date | DATE | 到期日 |
| is_secured | BOOLEAN | 有担保 |
| is_current | BOOLEAN | 一年内到期 |
| note | VARCHAR | |
| raw_post_id | INTEGER FK | |
| created_at | DATETIME | |

#### `litigations` — 诉讼/或有事项

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | |
| company_id | INTEGER FK | |
| case_name | VARCHAR | 案件名 |
| case_type | VARCHAR | lawsuit\|regulatory\|patent\|antitrust\|environmental\|tax\|other |
| status | VARCHAR | pending\|settled\|dismissed\|ongoing\|appealed |
| counterparty | VARCHAR | 对方 |
| filed_at | DATE | |
| claimed_amount | FLOAT | 索赔金额（百万） |
| accrued_amount | FLOAT | 已计提（百万） |
| currency | VARCHAR | |
| description | TEXT | |
| resolution | TEXT | |
| resolved_at | DATE | |
| raw_post_id | INTEGER FK | |
| created_at | DATETIME | |

#### `executive_compensations` — 管理层/董事薪酬

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | |
| company_id | INTEGER FK | |
| period | VARCHAR INDEX | |
| role_type | VARCHAR | executive\|director |
| name | VARCHAR | |
| title | VARCHAR | CEO\|CFO\|Independent Director |
| base_salary | FLOAT | 基本工资（董事: fees_earned_cash） |
| bonus | FLOAT | |
| stock_awards | FLOAT | |
| option_awards | FLOAT | |
| non_equity_incentive | FLOAT | |
| other_comp | FLOAT | |
| total_comp | FLOAT | |
| currency | VARCHAR | |
| pay_ratio | FLOAT | CEO Pay Ratio（仅 CEO） |
| median_employee_comp | FLOAT | 员工中位数（仅 CEO） |
| raw_post_id | INTEGER FK | |
| created_at | DATETIME | |

#### `stock_ownership` — 持股信息

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | |
| company_id | INTEGER FK | |
| period | VARCHAR INDEX | |
| name | VARCHAR | 持有人 |
| title | VARCHAR | 职位 |
| shares_beneficially_owned | INTEGER | 受益持股数 |
| percent_of_class | FLOAT | 持股比例（0-100） |
| raw_post_id | INTEGER FK | |
| created_at | DATETIME | |

#### `related_party_transactions` — 关联交易

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | |
| company_id | INTEGER FK | |
| period | VARCHAR INDEX | |
| related_party | VARCHAR | 关联方 |
| relationship | VARCHAR | director\|officer\|major_shareholder\|subsidiary\|affiliate\|family |
| transaction_type | VARCHAR | sale\|purchase\|lease\|loan\|guarantee\|service\|license\|other |
| amount | FLOAT | 金额（百万） |
| currency | VARCHAR | |
| terms | TEXT | 交易条件 |
| is_ongoing | BOOLEAN | 持续性交易 |
| description | TEXT | |
| raw_post_id | INTEGER FK | |
| created_at | DATETIME | |

---

### 7b. 表间关系

```
company_profiles (1)
    ├──→ operational_issues (N)     — company_id + period
    ├──→ company_narratives (N)     — company_id
    ├──→ financial_statements (N)   — company_id + period
    │       └──→ financial_line_items (N) — statement_id
    ├──→ downstream_segments (N)    — company_id + period
    ├──→ upstream_segments (N)      — company_id + period
    ├──→ geographic_revenues (N)    — company_id + period
    ├──→ non_financial_kpis (N)     — company_id + period
    ├──→ debt_obligations (N)       — company_id + period
    ├──→ litigations (N)            — company_id
    ├──→ executive_compensations (N)— company_id + period
    ├──→ stock_ownership (N)        — company_id + period
    └──→ related_party_transactions (N) — company_id + period

raw_posts (1)
    └──→ 所有业务表 (N)              — raw_post_id（溯源）
```

**核心关联键**：
- `company_id` — 唯一标识公司（通过 ticker 去重）
- `period` — 报告期（"FY2025" / "2025Q4"）
- `raw_post_id` — 溯源到原始文档

**跨期查询模式**（供 Polaris 参考）：
```sql
-- 获取某公司所有期的经营议题
SELECT * FROM operational_issues
WHERE company_id = ? ORDER BY period;

-- 获取某期的全部下游客户
SELECT * FROM downstream_segments
WHERE company_id = ? AND period = 'FY2025';

-- 跨公司比较管理层薪酬
SELECT cp.name, ec.* FROM executive_compensations ec
JOIN company_profiles cp ON ec.company_id = cp.id
WHERE ec.period = 'FY2025' AND ec.role_type = 'executive';
```

---

## 8. 域开关机制

### 配置

```python
enabled_domains: dict[str, bool] = {
    "company": True,      # ✅ 已实现专用管线
    "policy": False,      # ❌ 待实现
    "industry": False,    # ❌ 待实现
    "technology": False,  # ❌ 待实现
    "futures": False,     # ❌ 待实现
    "expert": False,      # ❌ 待实现
}
```

### 行为

| 场景 | 行为 |
|------|------|
| 域已启用 | 路由到专用提取管线 |
| 域已禁用 | 跳过提取，返回 `skip_reason="域已禁用"` |
| 未知域 | 默认禁用 |

### 检查点

域开关在三处生效：
1. **Extractor.extract()** — 单条提取入口
2. **ConcurrentBatchRunner._process_one()** — 批量提取
3. **run_url.py** — CLI 展示

---

## 9. Company 专用提取管线

### 架构

```
extract_company_compute(content, platform, author, today)
  │
  ├── LLM 一次调用：
  │   输入: 公司财报/年报全文
  │   输出: CompanyExtractionResult（13 张表数据 + 公司信息 + 摘要）
  │
  └── 返回: CompanyComputeResult（无 DB 操作）

extract_company_write(raw_post, session, compute_result)
  │
  ├── get_or_create_company(ticker) → CompanyProfile
  ├── 写入 12 张业务表
  ├── 更新 RawPost.content_summary
  │
  └── 返回: {is_relevant_content, table_counts, summary, company_name, company_ticker}
```

### Pydantic Schema

```python
class CompanyExtractionResult(BaseModel):
    is_relevant_content: bool
    company: CompanyProfile       # {name, ticker, market, industry, summary}
    period: str                   # "FY2025"
    operational_issues: list[...]
    narratives: list[...]
    downstream_segments: list[...]
    upstream_segments: list[...]
    geographic_revenues: list[...]
    non_financial_kpis: list[...]
    debt_obligations: list[...]
    litigations: list[...]
    executive_compensations: list[...]
    stock_ownership: list[...]
    related_party_transactions: list[...]
    summary: str | None
    one_liner: str | None
```

### 与旧架构的区别

| 维度 | v8 Node/Edge | v9 Company 专用 |
|------|-------------|----------------|
| 数据模型 | 2 张通用表 | 13 张专用表 |
| LLM 调用 | 2-call（节点+边） | 1-call（全量提取） |
| 输出 | ExtractionNode + ExtractionEdge | 13 张表各自的行 |
| 关联键 | raw_post_id | company_id + period + raw_post_id |
| 跨期查询 | 不支持 | 原生支持（period 字段） |

---

## 10. 三条链路设计

### 内容提取

```
URL
 → process_url()           — 采集 RawPost
 → assess_post()           — 通用判断（确定 content_mode）
 → 域开关检查
 → Extractor.extract()     — 路由到专用管线
 → 返回 {table_counts, summary}（company 域）
```

### 通用判断

2D 分类（content_domain × content_nature）→ content_mode。

### 事实验证

暂停。company 域跳过验证（使用专用表，无 ExtractionNode）。待后续域专用验证管线实现。

---

## 11. 内容路由逻辑

```python
def resolve_content_mode(domain, nature, content_type=None) -> str:
    if nature == "第三方分析": return "expert"
    if domain == "政策": return "policy"
    if domain == "产业": return "industry"
    if domain == "技术": return "technology"
    if domain == "期货": return "futures"
    if domain == "公司": return "company"
    return "expert"
```

| domain | nature | content_mode | 状态 |
|--------|--------|-------------|------|
| 公司 | 一手信息 | company | **已启用** |
| 政策 | 一手信息 | policy | 禁用 |
| 产业 | 一手信息 | industry | 禁用 |
| 技术 | 一手信息 | technology | 禁用 |
| 期货 | 一手信息 | futures | 禁用 |
| 任意 | 第三方分析 | expert | 禁用 |

---

## 12. 监控流水线

`anchor monitor` 依然可用，但非 company 域的内容会在提取阶段跳过。

---

# Layer 2: Polaris — 认知模型

Polaris 是一台决策机器：输入特征，输出三大投资流派各自的评分和买入信号。

三流派：**巴菲特**（内在价值）、**达利欧**（周期定位）、**索罗斯**（反身性）。

## 13. Polaris 两层架构

```
┌─────────────────────────────────────────────────────────────┐
│                      模型层 (Model)                          │
│                                                             │
│  输入: L2 公司级特征向量                                       │
│  输出: 多维度评分 + 综合判断                                    │
│  V1: 规则权重（经验拍）  V2+: 市场结果反向校正权重               │
├─────────────────────────────────────────────────────────────┤
│                      特征层 (Feature)                        │
│                                                             │
│  L2  汇总特征    多条传导链在同一公司上的合力                     │
│  L1  传导特征    跨域组合特征（人工编码的领域知识）                │
│  L0  原始特征    从 Anchor 各域表 + 市场数据直接计算              │
│                                                             │
│  七域: 公司 | 行业 | 政府 | 周期 | 地缘 | 资本 | 技术           │
└─────────────────────────────────────────────────────────────┘
         ↑                              ↑
   Anchor 结构化表                  外部市场数据
```

### 特征层：分级计算

| 层级 | 名称 | 内容 | Alpha 来源 |
|------|------|------|-----------|
| **L0** | 原始特征 | 从 Anchor 表或市场数据直接计算的单域特征 | Anchor 提取质量 |
| **L1** | 传导特征 | 跨域组合特征——一个域的变化如何影响另一个域 | **产业认知** |
| **L2** | 汇总特征 | 同一公司身上多条传导链的合力 | 特征选择与加权 |

**L1 是 Polaris 的核心价值**——这些传导关系是领域专家才知道的因果推理，不是从数据中能自动学出来的。

### 模型层：评分机器

| 版本 | 方法 | 特点 |
|------|------|------|
| **V1** | 规则 + 经验权重 | 先跑通端到端，快速迭代 |
| V2 | 权重校正 | 用市场结果信号反向调整特征权重 |
| V3+ | 学习 | 引入梯度更新，自动发现特征重要性 |

接口始终不变：**特征向量进，多维评分出。**

---

## 14. 七域与传导

七个特征域，以及它们之间的影响关系：

```
地缘 ──→ 一切（制裁/关税/冲突改变游戏规则）
  │
  ├──→ 政府 ──→ 行业（产业政策/监管/补贴）
  │     │  ↑       │
  │     │  │       └──→ 公司（行业红利/衰退影响每家公司）
  │     │  │              ↑
  │     │  周期 ──→ 资本 ─┘（融资环境/估值水位/一级涌入）
  │     │            │
  │     └──→ 公司     └──→ 行业（资本催熟/泡沫/出清）
  │
  └──→ 技术 ──→ 行业（开源技术/行业标准改变格局）
         └──→ 公司（专利/自研技术构成壁垒）
```

### 传导方向汇总

| 源域 | → 目标域 | 传导机制示例 |
|------|---------|------------|
| **技术** | → 行业 | AI 扩大云计算 TAM；开源模型压缩 SaaS 毛利 |
| **技术** | → 公司 | 专利壁垒；自研芯片降本 |
| **行业** | → 公司 | 电商渗透率提升，所有电商公司受益；行业价格战压缩利润 |
| **政府** | → 行业 | 芯片补贴法案利好半导体；环保标准淘汰落后产能 |
| **政府** | → 公司 | 反垄断针对特定公司；出口管制限制特定公司收入 |
| **周期** | → 资本 | 降息周期→流动性宽松→估值扩张 |
| **周期** | → 政府 | 衰退→财政刺激；过热→货币紧缩 |
| **资本** | → 行业 | VC 涌入 AI→竞争加剧；资本退潮→出清 |
| **资本** | → 公司 | 融资环境影响扩张能力；二级市场情绪影响定价 |
| **地缘** | → 一切 | 中美脱钩重构供应链；战争冲击能源和粮食 |

**每条传导 = 一个 L1 特征的定义**。特征随着认知积累逐条增加。

---

## 15. L0 原始特征

从 Anchor 各域表或外部市场数据**直接计算**的单域特征。不涉及跨域推理。

### 15.1 公司域（Anchor Company 13 张表）

| 类别 | 特征示例 | 数据来源 |
|------|---------|---------|
| 盈利能力 | 毛利率、净利率、ROE、ROIC | financial_line_items |
| 盈利趋势 | 毛利率 YoY 变化、收入增速 | financial_line_items 跨 period |
| 收入质量 | 经常性收入占比、客户集中度 | downstream_segments |
| 供应链 | 供应商集中度、独家供应占比 | upstream_segments |
| 资本结构 | 有息负债/EBITDA、流动比率 | debt_obligations + financial_line_items |
| 现金流 | 经营现金流/净利润、自由现金流率 | financial_line_items |
| 管理层 | 承诺兑现率、CEO Pay Ratio、管理层持股比例 | company_narratives + executive_compensations + stock_ownership |
| 风险 | 诉讼计提/净利润、关联交易金额/收入 | litigations + related_party_transactions |
| 地域 | 单一市场收入占比、高风险地区敞口 | geographic_revenues |
| 运营 | 员工增速、客户数增速 | non_financial_kpis 跨 period |

### 15.2 行业域

待 Anchor Industry 域管线实现。当前可从 Company 域间接推导部分行业特征（如同行业多家公司的平均毛利率）。

### 15.3 政府域

待 Anchor Policy 域管线实现。特征设计参考[附录 A：IPOCC 框架](#附录-a-政策分析框架ipocc)。

### 15.4 周期域

来源：外部市场数据。如利率水平、收益率曲线斜率、信用利差、PMI 等。

### 15.5 地缘域

来源：待定。如制裁清单、关税税率、冲突指数等。

### 15.6 资本域

来源：外部市场数据。如一级融资额、二级市场估值分位、资金流向等。

### 15.7 技术域

待 Anchor Technology 域管线实现。

---

## 16. L1 传导特征

**跨域组合特征**——编码「A 域的变化如何影响 B 域」的领域知识。每条传导链是一个 L1 特征的计算公式。

### 16.1 技术 → 公司

| 特征 | 计算逻辑 | 说明 |
|------|---------|------|
| 技术壁垒分 | 公司专利数量 × 专利质量（引用/商业化） | 待 Anchor PatentRight/PatentCommercial |

### 16.2 技术 → 行业

| 特征 | 计算逻辑 | 说明 |
|------|---------|------|
| 行业颠覆风险分 | 开源替代方案的成熟度 × 行业毛利率 | 高毛利+成熟开源替代=高颠覆风险 |

### 16.3 行业 → 公司

| 特征 | 计算逻辑 | 说明 |
|------|---------|------|
| 行业顺风分 | 行业收入增速 × 公司在行业中的份额弹性 | 行业增长时，份额稳定的公司自然受益 |

### 16.4 政府 → 行业

| 特征 | 计算逻辑 | 说明 |
|------|---------|------|
| 政策利好分 | 政策力度 × 执行力 × 行业覆盖度 | 待 Anchor Policy 域。参考 IPOCC 框架 |

### 16.5 政府 → 公司

| 特征 | 计算逻辑 | 说明 |
|------|---------|------|
| 出口管制风险分 | 受限地区收入占比 × 管制严格度 | geographic_revenues 高风险地区敞口 |

### 16.6 周期 → 资本

| 特征 | 计算逻辑 | 说明 |
|------|---------|------|
| 流动性环境分 | f(利率变化方向, 收益率曲线斜率, 信用利差) | 外部市场数据 |

### 16.7 资本 → 公司

| 特征 | 计算逻辑 | 说明 |
|------|---------|------|
| 融资依赖风险分 | 公司现金消耗速度 × 流动性环境分的反向 | 烧钱公司在紧缩周期更脆弱 |

### 16.8 地缘 → 公司

| 特征 | 计算逻辑 | 说明 |
|------|---------|------|
| 供应链重构敞口 | 上游供应商地缘集中度 × 地缘紧张度 | upstream_segments 地理位置 |

---

## 17. L2 汇总特征

**多条传导链在同一公司身上的合力**。将所有 L0 和 L1 特征汇聚到公司级别。

```
公司 X 的 L2 特征向量 = [
    # L0 公司内生特征
    毛利率趋势, 收入质量, 资本结构, 现金流质量, 管理层可信度, ...

    # L1 外部传导特征
    行业顺风分, 政策利好分, 出口管制风险分, 技术壁垒分,
    流动性环境分, 融资依赖风险分, 供应链重构敞口, ...
]
```

L2 向量就是模型层的输入。

---

## 18. 三流派评分框架

```
┌────────────────────────────────────────────────────────────┐
│                  三流派评分输出                                │
│                                                            │
│  巴菲特·内在价值 (50%)                                      │
│  「这门生意本身好不好？值不值得永久持有？」                      │
│                                                            │
│  达利欧·周期定位 (30%)                                      │
│  「当前债务周期下，这家公司是安全的还是脆弱的？」                │
│                                                            │
│  索罗斯·反身性   (20%)                                      │
│  「市场认知与现实之间，是否存在可利用的偏差？」                  │
│                                                            │
│  → 综合评分 = 加权合成 → 买入 / 观望 / 回避                   │
│  → 归因：每个流派 top-5 驱动特征                              │
└────────────────────────────────────────────────────────────┘
```

### 18.1 巴菲特流派 — 内在价值

> 看**公司本身**。不关心股价波动，只关心这门生意的内在品质。

| 子维度 | 核心问题 | 主要输入特征 |
|--------|---------|-------------|
| **商业模式** | 收入可预测吗？定价权强吗？ | recurring_revenue_pct, top_customer_concentration, operating_margin |
| **护城河** | 竞争优势持久吗？在扩大还是收窄？ | gross_margin + stability, incremental_roic, margin_vs_peers |
| **所有者盈余** | 股东真正能拿走多少钱？ | owner_earnings, owner_earnings_margin, capex_to_ocf |
| **盈利质量** | 利润是真的吗？现金流能背书？ | ocf_to_net_income, accruals_ratio, receivables_growth_vs_revenue |
| **资本配置** | 管理层有钱时怎么花？ | shareholder_yield, retained_earnings_roic, buyback_to_net_income |
| **管理层** | 值得信任吗？利益对齐吗？ | narrative_fulfillment_rate, mgmt_ownership_pct, ceo_pay_ratio |
| **可预测性** | 能看懂未来十年吗？ | gross_margin_stability, consecutive_revenue_growth, consecutive_positive_fcf |

**巴菲特高分特征**：高毛利 + 毛利稳定 + 轻资本 + 现金流充沛 + 管理层诚信 + 低债务。

### 18.2 达利欧流派 — 周期定位

> 看**公司与周期的关系**。同样的公司，在周期不同阶段表现完全不同。

| 子维度 | 核心问题 | 主要输入特征 |
|--------|---------|-------------|
| **宏观周期位置** | 我们在债务周期的哪个阶段？ | real_interest_rate, credit_growth_vs_gdp, m2_growth_yoy, yield_curve_slope |
| **公司债务健康** | 杠杆率合理吗？偿债能力够吗？ | debt_service_burden, net_debt_to_ebitda, debt_growth_vs_revenue_growth |
| **利率敏感性** | 利率变化对这家公司冲击多大？ | rate_sensitivity, weighted_avg_interest_rate, current_debt_pct |
| **流动性缓冲** | 短期偿债有安全垫吗？ | cash_to_short_term_debt, current_ratio, liquidity_stress |

**达利欧高分特征**：低杠杆 + 充裕现金 + 低利率敏感 + 宏观处于早/中期扩张阶段。

### 18.3 索罗斯流派 — 反身性

> 看**市场认知与现实的偏差**。市场不是被动反映现实，而是主动塑造现实。

| 子维度 | 核心问题 | 主要输入特征 |
|--------|---------|-------------|
| **反身性循环** | 自我强化的反馈在什么方向？ | reflexivity_exposure, goodwill_growth_vs_revenue_growth |
| **杠杆动态** | 杠杆在加速还是减速？ | leverage_acceleration, leverage_cycle_risk |
| **市场情绪** | 市场是自满还是恐慌？ | vix_percentile, margin_debt_growth, put_call_ratio |
| **融资依赖** | 基本面多大程度依赖市场情绪维持？ | financing_dependency, share_dilution_rate, bubble_fragility |

**索罗斯高分特征**：低融资依赖 + 自造血能力强 + 市场情绪非极端 + 不处于泡沫反身性循环中。

### 18.4 三者关系

```
巴菲特：这家公司好不好？     （选标的）
达利欧：现在买安全吗？       （选时机）
索罗斯：市场错了吗？         （选赔率）

理想状态：巴菲特高分 + 达利欧安全 + 索罗斯发现偏差 = 最佳买入机会
危险信号：巴菲特高分但达利欧低分 = 好公司但时机不对
反身性机会：索罗斯高分但巴菲特中性 = 市场恐慌中的错杀
```

---

## 19. V1 规则模型

V1 纯规则，经验拍权重，先跑通端到端。

### 19.1 单流派评分

每个流派内部：特征 → 规则 → 分数。

示例（巴菲特流派）：
```
毛利率 > 40%  → +2       (护城河)
毛利率 < 20%  → -2       (无定价权)
经营现金流/净利润 > 0.8 → +2  (利润有现金背书)
经营现金流/净利润 < 0.5 → -2  (利润可能不真实)
经常性收入 > 50% → +2     (收入可预测)
最大客户 > 30%  → -1.5    (客户集中风险)
...
求和 → 归一化到 1-10 → 转化为信号（≥7 买入 / 4-7 观望 / <4 回避）
```

### 19.2 综合评分

三流派加权求和：
```
综合分 = 0.50 × 巴菲特 + 0.30 × 达利欧 + 0.20 × 索罗斯
```

权重可按市场环境动态调整：
- 紧缩/危机周期：提高达利欧权重（周期主导一切）
- 泡沫/极端情绪期：提高索罗斯权重（反身性主导定价）
- 正常市场：维持默认（内在价值主导选择）

### 19.3 归因输出

每次评分同时输出：
- 每个流派的 top-5 驱动特征及其贡献
- 综合判断中权重最大的流派
- 三个流派各自的买入/观望/回避信号
- 与上期评分的变化归因

---

## 20. Anchor 输入映射

### 20.1 Company 域 13 张表（已可用）

| Anchor 表 | Polaris 特征用途 |
|----------|--------------|
| company_profiles | 公司标识、行业归属、跨期关联键 |
| operational_issues | L0 经营状态特征（议题数量、风险密度） |
| company_narratives | L0 管理层特征（承诺兑现率、资本承诺规模） |
| financial_statements + financial_line_items | L0 财务特征（毛利率、ROE、现金流、负债率等全部三表指标） |
| downstream_segments | L0 收入质量特征（经常性占比、客户集中度、定价模型） |
| upstream_segments | L0 供应链特征（供应商集中度、独家供应）+ L1 地缘传导 |
| geographic_revenues | L0 地域特征 + L1 地缘传导（高风险地区敞口） |
| non_financial_kpis | L0 运营特征（员工增速、客户增速） |
| debt_obligations | L0 资本结构特征 |
| litigations | L0 法律风险特征 |
| executive_compensations + stock_ownership | L0 管理层特征（Pay Ratio、持股比例） |
| related_party_transactions | L0 造假信号特征 |

### 20.2 其他域（待 Anchor 管线）

| Anchor 域 | 表 | Polaris 特征用途 | 状态 |
|-----------|---|--------------|------|
| Policy | PolicyDocument/Directive/Link | L0 政策特征 → L1 政府→行业/公司传导 | 待管线 |
| Technology | TechInsight/PatentRight/PatentCommercial | L0 技术特征 → L1 技术→行业/公司传导 | 待管线 |
| Industry | 待设计 | L0 行业特征 | 待设计 |

### 20.3 基础设施表

| Anchor 表 | 用途 |
|----------|------|
| raw_posts | 溯源（raw_post_id 关联原文） |
| authors + author_stats | 信源可信度加权 |

---

# Layer 3: Axion — 量化投资

Axion 承接 Polaris 评分，负责估值、仓位和执行。

核心设计原则：**市场定价 = 基本面 + 预期差。Axion 的核心任务是量化预期差，并在风险可控的前提下捕捉定价偏离。**

## 21. Axion 核心功能

### 21.1 估值模型

承接 Polaris 分析框架中超出认知模型边界的部分：

**预期差（Pricing Gap）**：
- 市场已经定价了多少？
- 当前定价隐含了什么假设？
- 假设与 Polaris 传导链预测的偏离度

**估值安全边际**：
- DCF 估值（基于 Polaris 输出的增长/利润率/WACC）
- 相对估值（PE/PB/PS + 行业比较）
- 赔率分析（上行空间 vs 下行风险）

**风险与证伪条件**：
- 系统性风险（宏观/政策/流动性）
- 公司特有风险（竞争/技术替代/管理层）
- 证伪条件清单（什么情况下论点不成立）

### 21.2 仓位管理

- **Kelly 准则**：基于胜率和赔率计算最优仓位
- **相关性控制**：避免同质化持仓（同一传导链上的多个标的）
- **风险预算**：按资产类别/行业/因子分配风险额度

### 21.3 执行与监控

- **信号触发**：Polaris 预测信号 + 估值偏离度 → 交易信号
- **止损/止盈**：基于证伪条件和风险预算动态调整
- **组合再平衡**：定期检查持仓与目标权重的偏离

### 21.4 收益归因

- **来源分解**：哪些预测信号贡献了收益？
- **传导链追溯**：收益/亏损可以追溯到 Polaris 的哪条传导链？
- **反馈回路**：投资结果作为 Polaris 学习层的 label 信号

### 21.5 数据模型（待设计）

- `valuations` — 估值计算结果（DCF + 相对估值 + 隐含假设）
- `positions` — 持仓记录（标的、方向、仓位、入场理由）
- `risk_budgets` — 风险预算分配
- `trade_signals` — 交易信号记录（来源、强度、触发条件）
- `pnl_attribution` — 收益归因（按信号来源/传导链/因子分解）
- `falsification_checks` — 证伪条件监控

---

# 工程

## 22. 配置与环境

```bash
# 必需
DATABASE_URL=sqlite+aiosqlite:///./data/facet.db
LLM_PROVIDER=anthropic
LLM_API_KEY=...

# 可选
SERPER_API_KEY=...
TWITTER_BEARER_TOKEN=...
FRED_API_KEY=...
```

---

## 23. 技术栈

| 层次 | 技术 |
|------|------|
| 语言 | Python 3.12+ |
| LLM | Anthropic Claude / OpenAI 兼容接口 |
| ORM | SQLModel（SQLAlchemy 2.0 异步） |
| 数据库 | SQLite（开发）/ PostgreSQL（生产） |
| 采集 | Twitter API / Weibo / BeautifulSoup / Jina Reader |
| 特征计算 | Pandas / NumPy |
| 异步 | asyncio + aiosqlite |

---

## 24. 文件结构

```
facet/
├── pyproject.toml                     # 顶层项目配置
├── docker-compose.yml
├── docs/PRD.md                        # 本文档
│
├── anchor/                            # Layer 1: 信息提取
│   ├── config.py                      # 配置 + enabled_domains 域开关
│   ├── models.py                      # 13 张 company 表 + 基础设施表
│   ├── llm_client.py                  # LLM 统一接口
│   ├── cli.py                         # Click CLI
│   ├── extract/
│   │   ├── router.py                  # Extractor 门面 — 域开关 + 路由
│   │   ├── schemas/company.py         # Company LLM 输出 Pydantic schema
│   │   └── pipelines/company.py       # Company 专用管线
│   ├── chains/
│   │   ├── content_extraction.py      # 内容提取链路
│   │   ├── general_assessment.py      # 通用判断
│   │   └── fact_verification.py       # 事实验证
│   ├── collect/                       # 数据采集
│   ├── monitor/                       # 订阅监控
│   ├── verify/                        # 验证工具
│   └── database/session.py            # 异步 DB Session
│
├── polaris/                             # Layer 2: 认知模型
│   ├── config.py                      # 配置
│   ├── cli.py                         # Click CLI
│   ├── db/
│   │   ├── session.py                 # DB Session
│   │   └── anchor.py                  # 读取 Anchor DB
│   ├── features/
│   │   ├── registry.py                # 特征注册
│   │   ├── pipeline.py                # 特征计算管线
│   │   └── l0/, l1/, l2/              # 分层特征
│   └── scoring/
│       ├── scorer.py                  # 评分主入口
│       ├── dimensions.py              # 维度定义
│       ├── rules.py                   # V1 规则
│       └── v1/                        # 三流派实现
│           ├── buffett.py
│           ├── dalio.py
│           └── soros.py
│
└── axion/                           # Layer 3: 量化投资（PRD 阶段）
```

---

# 附录

## 25. 系统局限与边界

### Anchor
- **仅 company 域已实现**：其他 5 域暂时禁用
- **事实验证暂停**：待域专用验证管线实现
- **每篇文章单域**：一篇内容只进入一个域的提取管线

### Polaris
- **L1 传导特征是人工编码的**：系统不自动发现传导关系
- **V1 规则权重是经验拍的**：初期无学习能力
- **依赖 Anchor 提取质量**：L0 特征质量上限 = Anchor 提取精度
- **当前仅 Company 域可用**：其他 6 域的 L0 特征需等待 Anchor 管线或外部数据接入

### Axion
- **不做产业分析**：产业理解属于 Polaris
- **不做高频交易**：面向中低频策略（持仓周期 > 1 天）
- **估值模型假设**：DCF 等方法本质是主观判断的结构化表达

---

## 26. 路线图

### 已完成

- [x] Anchor Company 专用管线：13 张表一次 LLM 调用提取
- [x] Anchor 域开关机制：enabled_domains 按域启用/禁用
- [x] Polaris V1 规则评分（巴菲特/达利欧/索罗斯）
- [x] Polaris 特征计算管线（L0 公司域）
- [x] 三项目合并为 Facet monorepo
- [x] **统一 PRD**

### 近期

- [ ] Anchor Policy 域专用管线（PolicyDocument + PolicyDirective + PolicyLink）
- [ ] Anchor Technology 域专用管线（TechInsight + PatentRight + PatentCommercial）
- [ ] Polaris L1 传导特征第一批（地缘→公司、政府→公司、行业→公司）
- [ ] Axion 数据模型设计 + Polaris 输出接口契约
- [ ] 统一 Config / DB / 构建系统（工程债务清理）

### 中期

- [ ] 外部市场数据接入（周期域、资本域 L0 特征）
- [ ] Axion 估值引擎（DCF + 相对估值 + 预期差量化）
- [ ] Axion 仓位管理（Kelly + 风险预算）
- [ ] Anchor Expert 域专用管线
- [ ] Anchor Industry / Futures 域专用管线

### 远期

- [ ] Polaris V2 模型：市场结果反向校正权重
- [ ] Axion 回测框架 + 收益归因
- [ ] Axion 实盘执行（券商 API 对接）
- [ ] Polaris V3 模型：梯度更新特征权重

---

## 附录 A. 政策分析框架（IPOCC）

> 供政府域 L0/L1 特征设计参考。

政策不是简单的文本，而是一种结构化的力量。

### A.1 决策者目标函数

政策永远是在多重目标之间的权衡取舍：

| 目标 | 内容 |
|------|------|
| 政治生存 | 执政合法性、选举/权力稳固 |
| 经济增长 | GDP、就业、收入 |
| 金融稳定 | 防止危机、债务可持续 |
| 社会稳定 | 贫富差距、社会矛盾 |
| 外部安全 | 地缘、汇率、资本流动 |

关键洞察：**读懂决策者当前最怕什么，比读懂政策文本更重要。**

### A.2 IPOCC 五维度

| 维度 | 核心问题 |
|------|---------|
| **I**ntent — 意图 | 政策真正想解决什么问题？ |
| **P**olicy — 工具 | 用什么手段？货币/财政/监管/产业？ |
| **O**utcome — 目标 | 官方宣称的结果是什么？ |
| **C**apacity — 执行力 | 有没有能力真正落地？ |
| **C**onstraint — 约束 | 什么在限制这个政策的空间？ |

### A.3 政策工具分类

| 类别 | 工具 |
|------|------|
| 货币政策 | 利率/准备金率/公开市场操作、前瞻指引/量化宽松收紧、汇率干预 |
| 财政政策 | 支出扩张（基建/补贴/转移支付）、税收调整、债务融资方式 |
| 监管/结构性 | 行业准入/反垄断、土地/户籍/劳动力改革、金融监管、环境标准 |
| 产业政策 | 补贴与保护、国家主导投资、技术管制与标准制定 |

### A.4 执行力评估

执行链条：中央意图 → 部委细则 → 地方执行 → 市场主体响应。每一环节可能出现利益不一致、能力不足、政策打架。

可信度信号：有没有配套资金和机制？过去类似政策执行历史如何？有没有明确问责机制？

### A.5 传导路径与时间分解

同一政策在三个窗口效果可能方向相反：

| 阶段 | 特征 |
|------|------|
| 短期（0-6 月） | 预期和情绪驱动，市场往往过度反应 |
| 中期（6 月-3 年） | 实际资金和资源配置开始改变 |
| 长期（3 年+） | 结构性变化，产业格局重塑 |

### A.6 反身性

政策-市场反馈环：经济数据恶化 → 宽松预期 → 市场上涨 → 金融条件改善 → 数据好转 → 收紧预期 → 循环。

地缘政治 = 跨国政策分析：框架同构——动机（国家利益/国内政治）、工具（关税/制裁/军事/技术管制）、约束（经济相互依存/盟友体系）、传导（供应链/能源/货币/资本流动）。

### A.7 六步工作流

```
Step 1  动机 (Trigger)        这个政策是被什么逼出来的？
Step 2  目标函数 (Objective)   决策者真正在意什么、害怕什么？
Step 3  工具匹配 (Tool-Fit)    政策工具能不能解决这个问题？
Step 4  执行力 (Execution)     能不能真正落地？谁会抵制？
Step 5  传导路径 (Transmission) 短中长期分别影响什么行业/公司？
─────── Polaris 边界 ──────────────────────────────────────
Step 6  预期差 (Pricing Gap)   市场已经定价了多少？（Axion 负责）
```

---

## 附录 B. 公司分析框架

> 供公司域 L0 特征设计和模型评分维度参考。

公司分析回答：**这家公司是什么、能走多远、当前状态如何**。

### B.1 商业模式

核心三问：
1. **为谁创造价值？** — 客户是谁，痛点是什么
2. **用什么方式创造？** — 产品/服务/交付方式
3. **怎么把价值变成钱？** — 收入模式、定价权

收入模式质量：

| 模式 | 可预测性 | 典型例子 |
|------|---------|---------|
| 订阅/SaaS | 最高 | Salesforce、Netflix |
| 耗材/复购 | 高 | 打印机墨盒、医疗耗材 |
| 平台抽佣 | 中高 | 美团、Visa |
| 项目制 | 低 | 建筑、咨询 |
| 大宗商品 | 最低 | 钢铁、煤炭 |

### B.2 竞争优势（护城河）

五类护城河（巴菲特/晨星体系）：

| 护城河类型 | 核心判断 |
|-----------|---------|
| 无形资产 | 品牌溢价能否转化为定价权？专利/牌照壁垒多高？ |
| 转换成本 | 客户换掉你的代价有多高？ |
| 网络效应 | 用户越多产品越有价值？已跨越临界点？ |
| 成本优势 | 规模经济/流程领先/地理优势？ |
| 有效规模 | 市场足够小，进入者无利可图？ |

护城河动态：扩大 → 稳定 → 收窄 → 突破，这比静态判断更重要。

### B.3 行业结构

- **天花板**：TAM 多大？当前渗透率？增长来自行业扩张还是抢份额？
- **竞争格局**：垄断/双寡头（定价权强）→ 寡头竞争 → 充分竞争 → 过度竞争（价格战）
- **行业周期**：导入期 → 成长期 → 成熟期 → 衰退期

### B.4 财务健康

三表核心逻辑：

| 报表 | 核心问题 | 关键指标 |
|------|---------|---------|
| 利润表 | 赚不赚钱？ | 毛利率趋势、费用率变化、净利率 |
| 资产负债表 | 安不安全？ | 有息负债/EBITDA、流动比率、商誉占比、应收周转 |
| 现金流表 | 利润真实吗？ | 经营现金流 vs 净利润（长期背离=质量差）、自由现金流 |

核心指标：ROE/ROIC（资本回报）、自由现金流率（盈利真实性）、毛利率趋势（定价权）

造假信号：利润增长但经营现金流不增长、应收增速远超收入、存货异常积累、关联交易高且不透明

### B.5 管理层

- **能力**：过去重大决策质量、承诺兑现率、资本配置历史
- **利益对齐**：持股比例、薪酬挂钩方式、历史套现记录
- **品格**：信息披露透明度、面对失败的态度

### B.6 七步工作流

```
Step 1  行业值不值得看？       天花板、竞争格局、周期位置
Step 2  有没有护城河？         五类护城河 + 动态判断
Step 3  商业模式可持续吗？     收入质量、利润率、资本回报
Step 4  财务数据真实健康吗？    三表交叉验证、造假信号排查
Step 5  管理层值得信任吗？     能力 + 品格 + 利益对齐
─────── Polaris 边界 ──────────────────────────────────────
Step 6  估值有多少安全边际？    DCF + 相对估值 + 赔率（Axion 负责）
Step 7  主要风险和证伪条件？    系统性 + 公司特有风险（Axion 负责）
```

---

> 文档状态：v1.0（2026-03-16），三层统一 PRD
