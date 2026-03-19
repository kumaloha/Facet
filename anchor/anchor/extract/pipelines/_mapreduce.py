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


def default_chunker(content: str, max_chunk: int = 80000) -> list[ChunkMeta]:
    """默认切分：按长度切，不做语义分段。"""
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
    concurrency: int = 3,
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

    async def _extract_one(chunk: ChunkMeta) -> dict[str, list[dict]]:
        prompt_key = chunk.prompt_key or chunk.section_name
        system_prompt = section_prompts.get(prompt_key)
        if not system_prompt:
            # 没有对应 prompt，用 default（如果有）
            system_prompt = section_prompts.get("default")
        if not system_prompt:
            logger.warning(f"[MapReduce] 无 prompt for section '{prompt_key}'，跳过")
            return {}

        user_msg = f"## 文档内容（{chunk.section_name} 段）\n\n{chunk.content}"

        async with sem:
            resp = await call_llm(system_prompt, user_msg, max_tokens=max_tokens)

        if not resp:
            return {}

        data = _parse_json_raw(resp)
        if not data:
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
