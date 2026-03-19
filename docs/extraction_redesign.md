# 提取管线重构 — 按文件类型的 MapReduce

## 核心理念

**旧**：一个大 prompt 试图从任何文档中提取所有 20+ 张表
**新**：每种文件类型一套 focused prompt，只提取该文件该出的数据

## 文件类型总表

| # | 文件类型 | 来源 | 提取表数 | 优先级 | 频率 |
|---|---------|------|---------|--------|------|
| 1 | **annual_report** (10-K) | SEC EDGAR | 8 | P0 | 年度 |
| 2 | **proxy** (DEF 14A) | SEC EDGAR | 4 | P0 | 年度 |
| 3 | **earnings_call** | IR 网站/转录 | 5 | P0 | 季度 |
| 4 | **prospectus** (S-1/F-1) | SEC EDGAR | 7 | P0 | 一次性 |
| 5 | **competitive_intel** | 第三方报告 | 4 | P1 | 季度 |
| 6 | **investor_day** | IR 网站 | 4 | P1 | 不定期 |
| 7 | **event_filing** (8-K) | SEC EDGAR | 4 | P1 | 事件驱动 |
| 8 | **news** | RSS/社交 | 4 | P2 | 实时 |

**数据获取路径建议**：
1. 新公司首次建档：先拉 **招股书** → 建立行业结构基线
2. 日常更新：**10-K** + **电话会** + **Proxy** 每年/每季度
3. 实时监控：**新闻** + **8-K** 事件驱动

## 文件类型 → 提取目标

### 1. annual_report（10-K / 年报）
**来源**：SEC EDGAR / 交易所公告
**提取内容**：
- financial_line_items — 三表全量财务数据
- downstream_segments — 业务线/客户（含 product_category, segment_gross_margin）
- upstream_segments — 供应链
- geographic_revenues — 地域收入
- debt_obligations — 债务明细
- litigations — 诉讼
- audit_opinions — 审计意见
- operational_issues — MD&A 经营议题

**MapReduce 策略**：
- Map：按章节拆分（Business / Risk Factors / MD&A / Financials / Legal）
- Reduce：按表合并去重

---

### 2. proxy（DEF 14A / 委托声明书）
**来源**：SEC EDGAR
**提取内容**：
- executive_compensations — 高管薪酬 + CEO Pay Ratio
- stock_ownership — 管理层/大股东持股
- related_party_transactions — 关联交易
- executive_changes — 董事会变动

**MapReduce 策略**：
- 通常不需要拆分（Proxy 篇幅相对短）
- 单次提取

---

### 3. earnings_call（财报电话会记录）
**来源**：IR 网站 / Seeking Alpha / 音频转录
**提取内容**：
- management_guidance — 前瞻指引（收入增速/margin/capex/EPS）
- company_narratives — 管理层战略承诺（含 status 兑现状态）
- management_acknowledgments — 管理层对问题的回应（含 response_quality）
- known_issues — 管理层/分析师提到的问题
- pricing_actions — 定价变动

**MapReduce 策略**：
- Map：拆为 prepared_remarks + Q&A
- Reduce：合并，Q&A 里的 acknowledgments 优先级更高

---

### 4. investor_day（投资者日 / Capital Markets Day）
**来源**：IR 网站 / 演示文稿
**提取内容**：
- company_narratives — 长期战略叙事
- downstream_segments — 业务线深潜（可能有 segment_gross_margin）
- non_financial_kpis — 运营指标
- management_guidance — 长期目标

---

### 5. competitive_intel（行业报告 / 分析师研究）
**来源**：第三方报告 / 新闻
**提取内容**：
- competitive_dynamics — 竞争事件（price_war/new_entry/exit/patent_challenge/...）
- peer_financials — 同行财务指标（含 segment 分业务线）
- market_share_data — 市占率
- known_issues — 第三方发现的问题

---

### 6. event_filing（8-K / 临时公告）
**来源**：SEC EDGAR / 交易所公告
**提取内容**：
- executive_changes — 高管变动
- pricing_actions — 定价行为
- competitive_dynamics — 重大竞争事件
- equity_offerings — 股权发行

---

### 7. prospectus（招股书 S-1 / F-1 / 424B）
**来源**：SEC EDGAR（免费，`sec-edgar-downloader` 或 `edgartools`）
**提取内容**：
- downstream_segments — 最详细的业务线拆分（含 product_category, revenue_type）
- competitive_dynamics — 完整竞争格局（公司自述竞争优势和威胁）
- peer_financials — 行业对标数据（招股书常引用行业数据）
- market_share_data — TAM/SAM/市占率
- upstream_segments — 供应链依赖
- known_issues — 风险因素（IPO 时法律要求最全面的风险披露）
- company_narratives — 商业模式和战略叙事

**为什么招股书比年报好**：
- 年报假设读者已经了解公司，招股书假设读者完全不了解 → 解释更完整
- IPO 时法律合规要求最严格，风险披露最全面
- 行业结构、市场规模、竞争格局的描述比年报详细得多
- 一次性数据源：只提取一次（IPO 时），之后用年报增量更新

**MapReduce 策略**：
- Map：按章节拆分（Business / Risk Factors / Competition / Industry / Use of Proceeds）
- Reduce：按表合并
- 招股书通常很长（100-300 页），必须分段

**获取方式**：SEC EDGAR 免费 API
```python
# 用 edgartools
from edgar import Company
company = Company("AAPL")
filings = company.get_filings(form="S-1")
```

---

### 8. news（新闻 / 社交媒体）
**来源**：RSS / Twitter / 微博
**提取内容**：
- competitive_dynamics — 竞争事件
- known_issues — 负面新闻
- pricing_actions — 定价变动
- brand_signals — 品牌舆情（预留）

---

## 代码架构

```
anchor/extract/pipelines/
├── _base.py            # 通用工具（call_llm, parse_json, chunking）
├── _mapreduce.py       # MapReduce 框架（chunk → map → reduce）
├── annual_report.py    # 10-K 年报提取
├── proxy.py            # DEF 14A 提取
├── earnings_call.py    # 电话会提取
├── investor_day.py     # 投资者日提取
├── competitive_intel.py # 行业报告提取
├── event_filing.py     # 8-K 提取
├── news.py             # 新闻提取
└── company.py          # [保留] 向后兼容入口，路由到具体管线
```

## 每个管线的接口

```python
@dataclass
class ExtractionResult:
    """每个管线的统一输出。"""
    company_ticker: str
    period: str
    tables: dict[str, list[dict]]  # table_name → rows

async def extract(content: str, doc_type: str, metadata: dict) -> ExtractionResult:
    """统一入口：根据 doc_type 路由到对应管线。"""
```

## MapReduce 框架

```python
async def map_reduce_extract(
    content: str,
    chunk_fn: Callable,           # 怎么切（按章节/按段落/不切）
    map_prompt: str,              # Map 阶段的 prompt
    reduce_fn: Callable,          # 怎么合并
    max_tokens: int = 8192,
) -> dict:
    chunks = chunk_fn(content)
    map_results = await asyncio.gather(*[
        call_llm(map_prompt, chunk) for chunk in chunks
    ])
    return reduce_fn(map_results)
```
