# Facet

**Facet** = 信息提取 → 认知模型 → 投资工具

```
Anchor (信息提取) → Axion (认知模型) → Polaris (投资工具)
```

## 架构

| 模块 | 职责 | 状态 |
|------|------|------|
| **anchor/** | SEC 财报结构化提取（10-K/DEF 14A → 19 张表） | 可用 |
| **axion/** | 认知模型 + 评分引擎（Buffett/Dalio/Soros） | 可用 |
| **polaris/** | 投资工具 | PRD 阶段 |

## 快速开始

```bash
# 安装
pip install -e anchor/ -e axion/

# 建表
python -m anchor.database.session

# 提取 NVIDIA 最近 5 年 10-K
python -m anchor backfill NVDA --years 5

# 查看提取结果
python -m anchor company-sources NVDA
```

## 数据流

```
SEC EDGAR / Web → Anchor 提取 → data/facet.db → Axion 认知模型 → Polaris 投资决策
```
