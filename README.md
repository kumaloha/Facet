# Facet

**Facet** = 信息提取 → 认知模型 → 量化投资

```
Anchor (信息提取) → Polaris (认知模型) → Axion (量化投资)
```

## 架构

| 模块 | 职责 | 状态 |
|------|------|------|
| **anchor/** | SEC 财报结构化提取（10-K/DEF 14A → 19 张表） | 可用 |
| **polaris/** | 认知模型 + 评分引擎（Buffett/Dalio/Soros） | 可用 |
| **axion/** | 量化投资 | PRD 阶段 |

## 快速开始

```bash
# 安装
pip install -e anchor/ -e polaris/

# 建表
python -m anchor.database.session

# 提取 NVIDIA 最近 5 年 10-K
python -m anchor backfill NVDA --years 5

# 查看提取结果
python -m anchor company-sources NVDA
```

## 文档

统一产品文档见 [`docs/PRD.md`](docs/PRD.md)。

## 数据流

```
SEC EDGAR / Web → Anchor 提取 → data/facet.db → Polaris 认知模型 → Axion 量化投资
```
