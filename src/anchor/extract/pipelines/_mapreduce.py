"""
MapReduce 提取框架
==================
Map: 文档拆段 → 每段独立提取
Reduce: 合并去重

每个管线只需定义:
  1. 怎么切（chunk_fn）
  2. 每段提什么（section_prompts）
  3. 怎么合（reduce_fn）
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Callable, Optional

from loguru import logger

import json
import re

from anchor.extract.pipelines._base import call_llm
from anchor.config import settings


# ── 模型 context window 查表 ─────────────────────────────────────────
# model_name (或前缀) → 最大 context tokens
_MODEL_CONTEXT: dict[str, int] = {
    # Qwen (DashScope)
    "qwen-plus":       131072,
    "qwen-max":        131072,
    "qwen-turbo":      131072,
    "qwen-long":       10000000,  # 1000万，但实际受限于费用
    "qwen3":           131072,
    # OpenAI
    "gpt-4o":          128000,
    "gpt-4o-mini":     128000,
    "gpt-4-turbo":     128000,
    "gpt-4":           8192,
    "o3-mini":         128000,
    # Anthropic
    "claude-opus":     200000,
    "claude-sonnet":   200000,
    "claude-haiku":    200000,
    # DeepSeek
    "deepseek-chat":   128000,
    "deepseek-reasoner": 128000,
    # Local (Ollama)
    "qwen3.5":         131072,
    "llama3":          8192,
}

# 英文 SEC filing 的 chars/token 比率（保守估计）
_CHARS_PER_TOKEN = 2.5


def estimate_max_chunk_chars(
    max_output_tokens: int = 12000,
    system_prompt_chars: int = 2000,
) -> int:
    """根据当前模型的 context window 动态计算最大输入 chunk 大小。

    公式:
      context_tokens = system_prompt_tokens + input_tokens + output_tokens
      max_input_tokens = context_tokens - system_prompt_tokens - output_tokens
      max_chunk_chars = max_input_tokens * chars_per_token

    保留 10% 余量防止 tokenizer 估算偏差。
    """
    model_name = settings.effective_llm_model

    # 查表：先精确匹配，再前缀匹配
    context_tokens = _MODEL_CONTEXT.get(model_name)
    if context_tokens is None:
        for prefix, ctx in _MODEL_CONTEXT.items():
            if model_name.startswith(prefix):
                context_tokens = ctx
                break

    if context_tokens is None:
        # 未知模型，保守估计 32K
        context_tokens = 32768
        logger.warning(
            f"[MapReduce] 未知模型 '{model_name}' 的 context window，"
            f"保守估计 {context_tokens} tokens"
        )

    system_prompt_tokens = int(system_prompt_chars / _CHARS_PER_TOKEN)
    available_tokens = context_tokens - system_prompt_tokens - max_output_tokens
    # 保留 10% 余量
    available_tokens = int(available_tokens * 0.9)
    max_chars = int(available_tokens * _CHARS_PER_TOKEN)

    # 下限 20K，上限 500K
    max_chars = max(20000, min(max_chars, 500000))

    logger.debug(
        f"[MapReduce] 动态切片: model={model_name} context={context_tokens} "
        f"→ max_chunk={max_chars:,} chars"
    )
    return max_chars


def _parse_json_raw(raw: str) -> dict | None:
    """从 LLM 返回的文本中提取 JSON dict。宽容处理。"""
    s = raw.strip()
    # 去掉 ```json ... ```
    if s.startswith("```"):
        s = re.sub(r"^```\w*\n?", "", s)
        s = re.sub(r"\n?```\s*$", "", s)
    # 提取最外层 { ... }
    start = s.find("{")
    end = s.rfind("}")
    if start == -1 or end == -1:
        return None
    try:
        return json.loads(s[start:end + 1])
    except json.JSONDecodeError:
        # 尝试修复常见问题（末尾多余逗号）
        fixed = re.sub(r",\s*([}\]])", r"\1", s[start:end + 1])
        try:
            return json.loads(fixed)
        except json.JSONDecodeError:
            logger.error(f"[MapReduce] JSON 解析失败: {s[start:start+200]}...")
            return None


@dataclass
class ChunkMeta:
    """一个文档片段。"""
    section_name: str       # 段名（如 "Business", "Risk Factors"）
    content: str            # 段内容
    prompt_key: str = ""    # 对应哪个 prompt（不填则用 section_name）


@dataclass
class ExtractionResult:
    """管线统一输出。"""
    company_ticker: str = ""
    company_name: str = ""
    period: str = ""
    tables: dict[str, list[dict]] = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)


def default_chunker(content: str, max_chunk: int | None = None) -> list[ChunkMeta]:
    """默认切分：按长度切，不做语义分段。max_chunk 不传则动态计算。"""
    if max_chunk is None:
        max_chunk = estimate_max_chunk_chars()

    if len(content) <= max_chunk:
        return [ChunkMeta(section_name="full", content=content)]

    chunks = []
    for i in range(0, len(content), max_chunk):
        chunk = content[i:i + max_chunk]
        chunks.append(ChunkMeta(
            section_name=f"part_{i // max_chunk + 1}",
            content=chunk,
        ))
    return chunks


def dedup_rows(existing: list[dict], new_rows: list[dict], key_field: str) -> list[dict]:
    """按 key_field 去重，返回 new_rows 中不重复的条目。"""
    seen = set()
    for row in existing:
        k = row.get(key_field, "")
        if k:
            seen.add(str(k).lower().strip())
    result = []
    for row in new_rows:
        k = str(row.get(key_field, "")).lower().strip()
        if k and k not in seen:
            seen.add(k)
            result.append(row)
    return result


def merge_table_results(
    base: dict[str, list[dict]],
    other: dict[str, list[dict]],
    dedup_keys: dict[str, str] | None = None,
) -> dict[str, list[dict]]:
    """合并两个 table dict，按 dedup_keys 去重。

    dedup_keys: {table_name: key_field}，有的表按 key 去重，没有的直接追加。
    """
    dedup_keys = dedup_keys or {}
    for table_name, rows in other.items():
        if table_name not in base:
            base[table_name] = []
        key = dedup_keys.get(table_name)
        if key:
            base[table_name].extend(dedup_rows(base[table_name], rows, key))
        else:
            base[table_name].extend(rows)
    return base


async def map_reduce_extract(
    chunks: list[ChunkMeta],
    section_prompts: dict[str, str],
    dedup_keys: dict[str, str] | None = None,
    max_tokens: int = 8192,
    concurrency: int = 10,
) -> dict[str, list[dict]]:
    """MapReduce 提取。

    Args:
        chunks: 文档片段列表
        section_prompts: {prompt_key: system_prompt}
        dedup_keys: {table_name: dedup_field}
        max_tokens: LLM 最大输出 tokens
        concurrency: 并发数

    Returns:
        合并后的 {table_name: [row_dicts]}
    """
    sem = asyncio.Semaphore(concurrency)

    # ── Pre-filter：跳过明显无内容的子段 ──
    # risk_factors 子段如果没有诉讼/竞对关键词，跳过 LLM 调用
    _RISK_KEYWORDS = re.compile(
        r"(?i)(litigation|lawsuit|legal\s+proceed|case\s+no|court|settle|"
        r"competi|antitrust|patent\s+(infring|challeng|expir)|"
        r"recall|sanction|penalty|fine\b|injunction)",
    )

    def _should_skip(chunk: ChunkMeta) -> bool:
        """判断子段是否可以跳过 LLM 调用。"""
        key = chunk.prompt_key or chunk.section_name
        # 只对 risk_factors 的子段做 pre-filter
        # business 和 mda 不能跳过（一定有内容）
        if key == "risk_factors":
            # 检查是否有诉讼/竞对相关关键词
            if not _RISK_KEYWORDS.search(chunk.content):
                logger.info(
                    f"[MapReduce] 跳过 {chunk.section_name}: "
                    f"无诉讼/竞对关键词 ({len(chunk.content):,} chars)"
                )
                return True
        return False

    async def _extract_one(chunk: ChunkMeta) -> dict[str, list[dict]]:
        prompt_key = chunk.prompt_key or chunk.section_name
        system_prompt = section_prompts.get(prompt_key)
        if not system_prompt:
            # 没有对应 prompt，用 default（如果有）
            system_prompt = section_prompts.get("default")
        if not system_prompt:
            logger.warning(f"[MapReduce] 无 prompt for section '{prompt_key}'，跳过")
            return {}

        # Pre-filter
        if _should_skip(chunk):
            return {}

        user_msg = f"## 文档内容（{chunk.section_name} 段）\n\n{chunk.content}"

        async with sem:
            resp = await call_llm(system_prompt, user_msg, max_tokens=max_tokens)

        if not resp:
            return {}

        data = _parse_json_raw(resp)
        if not data:
            # 空返回重试一次（换一种提示方式）
            logger.warning(
                f"[MapReduce] {chunk.section_name} 首次返回空，重试"
            )
            retry_msg = (
                f"## 文档内容（{chunk.section_name} 段）\n\n"
                f"{chunk.content}\n\n"
                f"---\n请仔细阅读以上文档，提取所有可用信息。"
                f"即使信息不完整也要输出，不允许返回空 []。"
            )
            async with sem:
                resp = await call_llm(system_prompt, retry_msg, max_tokens=max_tokens)
            if resp:
                data = _parse_json_raw(resp)
            if not data:
                logger.warning(f"[MapReduce] {chunk.section_name} 重试仍为空")
                return {}

        # 提取所有 list 类型的字段作为 table
        tables = {}
        for key, val in data.items():
            if isinstance(val, list) and val:
                tables[key] = val
            elif isinstance(val, dict) and key not in ("company",):
                # 单个 dict 包装成 list
                tables[key] = [val]
        return tables

    # Map
    logger.info(f"[MapReduce] 开始提取 {len(chunks)} 个段")
    tasks = [_extract_one(chunk) for chunk in chunks]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Reduce
    merged: dict[str, list[dict]] = {}
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"[MapReduce] 段 {chunks[i].section_name} 提取失败: {result}")
            continue
        merged = merge_table_results(merged, result, dedup_keys)

    total_rows = sum(len(rows) for rows in merged.values())
    logger.info(f"[MapReduce] 提取完成: {len(merged)} 张表, {total_rows} 行数据")
    return merged
