"""
因果链 Pydantic Schemas — LLM 输出校验
======================================
从文章中提取因果关系：变量 + 因果链。
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class ExtractedCausalVariable(BaseModel):
    name: str                          # 规范化英文 ID "tsmc_advanced_node_price"
    domain: str = "company"            # company|industry|policy|cycle|geopolitics|capital|technology
    description: str = ""              # 人可读中文描述
    observable: bool = False           # 能否定量观测


class ExtractedCausalLink(BaseModel):
    cause: str                         # cause variable name（对应 ExtractedCausalVariable.name）
    effect: str                        # effect variable name
    mechanism: str                     # 因果机制
    magnitude: Optional[str] = None    # 量级估计
    lag: Optional[str] = None          # 时滞
    conditions: Optional[str] = None   # 边界条件


class CausalExtractionResult(BaseModel):
    """LLM 从一篇文章中提取的全部因果关系。"""
    variables: list[ExtractedCausalVariable] = []
    links: list[ExtractedCausalLink] = []
