"""
pipelines/causal.py — 因果链提取管线
====================================
从任意文章中提取因果关系（变量 + 因果链），写入 causal_variables / causal_links。

架构：
  extract_causal_compute(content, platform, author, today) → CausalComputeResult
  extract_causal_write(raw_post, session, compute_result) → dict
"""

from __future__ import annotations

from dataclasses import dataclass

from loguru import logger
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from anchor.extract.pipelines._base import call_llm, parse_json
from anchor.extract.schemas.causal import (
    CausalExtractionResult,
    ExtractedCausalLink,
    ExtractedCausalVariable,
)
from anchor.models import CausalLink, CausalVariable, RawPost, _utcnow

_MAX_TOKENS = 4096

# ── LLM 提示词 ──────────────────────────────────────────────────────────

SYSTEM_CAUSAL = """\
你是一个因果关系提取专家。从文章中识别出因果关系链。

## 核心要求：mechanism 和 conditions 必须分离

- **mechanism**：结构性解释——"为什么 A 一般会导致 B"。这是**永恒成立的因果逻辑**，不含时间、不含当前情况。
- **conditions**：当前触发条件——文章描述的具体情境，说明这条因果关系**此刻为什么被激活**。

示例：
  ✅ mechanism: "商业地产是私募信贷的主要抵押品，地产价值下降直接导致信贷资产质量恶化"
     conditions: "2026年商场写字楼出租率持续走低"
  ❌ mechanism: "商场写字楼出租困难导致私募信贷坏账飙升引发市场忧虑"（混在一起了）

## 输出格式

返回 JSON：

```json
{
  "variables": [
    {
      "name": "规范化英文ID，snake_case，如 tsmc_advanced_node_price",
      "domain": "company|industry|policy|cycle|geopolitics|capital|technology",
      "description": "中文描述，≤50字",
      "observable": true/false
    }
  ],
  "links": [
    {
      "cause": "变量name（必须在 variables 中出现或在【已有变量】中存在）",
      "effect": "变量name",
      "mechanism": "结构性因果机制，≤80字。只写【为什么 A 一般会导致 B】，不含时间和当前情况",
      "magnitude": "量级估计，如 '涨价10% → 毛利损失2-3pp'，没有则 null",
      "lag": "时滞估计，如 '1-2季度'，没有则 null",
      "conditions": "当前触发条件，≤80字。文章描述的具体情境，没有则 null"
    }
  ]
}
```

## 提取规则

1. **只提取文章中明确表述或强烈暗示的因果关系**，不要凭空推测
2. **变量必须具体**——"宏观经济" 太宽泛，"美联储基准利率" 才是变量
3. **mechanism 只写结构性逻辑**——十年后依然成立的那部分。当前情况放 conditions
4. **量级和时滞**——文章提到就填，没提到就 null
5. **如果文章没有任何因果关系，返回空数组**
6. **只输出 JSON，不要输出其他文字**
"""


def _build_user_message(
    content: str,
    existing_variables: list[str],
) -> str:
    parts = []
    if existing_variables:
        var_list = ", ".join(existing_variables[:200])
        parts.append(f"## 已有变量（优先复用，避免重复）\n{var_list}\n")
    parts.append(f"## 文章内容\n{content[:30000]}")
    return "\n".join(parts)


# ── Compute ──────────────────────────────────────────────────────────────


@dataclass
class CausalComputeResult:
    data: CausalExtractionResult | None
    skip_reason: str | None = None


async def extract_causal_compute(
    content: str,
    existing_variables: list[str] | None = None,
) -> CausalComputeResult:
    """纯 LLM 调用，不涉及 DB。"""
    if not content or len(content.strip()) < 200:
        return CausalComputeResult(data=None, skip_reason="content_too_short")

    user_msg = _build_user_message(content, existing_variables or [])
    raw = await call_llm(SYSTEM_CAUSAL, user_msg, _MAX_TOKENS)
    if raw is None:
        return CausalComputeResult(data=None, skip_reason="llm_failed")

    parsed = parse_json(raw, CausalExtractionResult, "causal")
    if parsed is None:
        return CausalComputeResult(data=None, skip_reason="parse_failed")

    if not parsed.links:
        return CausalComputeResult(data=None, skip_reason="no_causal_links")

    return CausalComputeResult(data=parsed)


# ── Write ────────────────────────────────────────────────────────────────


async def extract_causal_write(
    raw_post: RawPost,
    session: AsyncSession,
    compute_result: CausalComputeResult,
) -> dict:
    """将因果链写入 DB。返回 {variables: int, links: int}。"""
    if compute_result.data is None:
        return {"variables": 0, "links": 0, "skip_reason": compute_result.skip_reason}

    data = compute_result.data
    now = _utcnow()

    # 1. 写入 / 复用变量
    var_map: dict[str, int] = {}  # name → id
    for v in data.variables:
        var_map[v.name] = await _get_or_create_variable(session, v, now)
    await session.flush()

    # 2. 写入因果链
    link_count = 0
    for link in data.links:
        cause_id = var_map.get(link.cause)
        effect_id = var_map.get(link.effect)

        # 变量可能引用了已有变量（不在本次 extraction 中）
        if cause_id is None:
            cause_id = await _resolve_variable_id(session, link.cause)
        if effect_id is None:
            effect_id = await _resolve_variable_id(session, link.effect)

        if cause_id is None or effect_id is None:
            logger.warning(f"Skipping link: {link.cause} → {link.effect} (variable not found)")
            continue

        session.add(CausalLink(
            cause_id=cause_id,
            effect_id=effect_id,
            mechanism=link.mechanism,
            magnitude=link.magnitude,
            lag=link.lag,
            conditions=link.conditions,
            confidence=0.5,
            raw_post_id=raw_post.id,
            created_at=now,
            updated_at=now,
        ))
        link_count += 1

    await session.flush()
    return {"variables": len(var_map), "links": link_count}


async def _get_or_create_variable(
    session: AsyncSession,
    v: ExtractedCausalVariable,
    now,
) -> int:
    """查找已有变量或创建新变量，返回 id。"""
    stmt = select(CausalVariable).where(CausalVariable.name == v.name)
    result = await session.exec(stmt)
    existing = result.first()
    if existing:
        return existing.id

    new_var = CausalVariable(
        name=v.name,
        domain=v.domain,
        description=v.description,
        observable=v.observable,
        created_at=now,
    )
    session.add(new_var)
    await session.flush()
    return new_var.id


async def _resolve_variable_id(session: AsyncSession, name: str) -> int | None:
    """按 name 查找已有变量。"""
    stmt = select(CausalVariable.id).where(CausalVariable.name == name)
    result = await session.exec(stmt)
    row = result.first()
    return row if row else None
