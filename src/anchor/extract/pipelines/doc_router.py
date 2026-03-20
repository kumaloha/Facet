"""
文档类型路由
============
根据文档类型调用对应的提取管线。

用法:
    result = await extract_document(content, doc_type="annual_report", metadata={...})
    # XBRL-first: 传入 filing 对象自动提取 XBRL
    result = await extract_document(content, doc_type="annual_report", metadata={...}, filing=filing)
"""

from __future__ import annotations

import re

from loguru import logger

from anchor.extract.pipelines._mapreduce import ExtractionResult


def _clean_sec_text(content: str) -> str:
    """清理 SEC EDGAR document.text() 的装饰字符。

    EDGAR 输出包含大量 box-drawing 字符（│╰─ 等），这些字符
    在 LLM tokenizer 中占大量 token 但无信息量，会导致有效内容被截断。
    """
    # 移除 box-drawing / 装饰字符
    content = re.sub(r"[│╰╯╭╮─┌┐└┘├┤┬┴┼═║╔╗╚╝╠╣╦╩╬▪▫●○◦■□▶►▷▹◆◇★☆]+", "", content)
    # 压缩多余空格（保留换行结构）
    content = re.sub(r"[ \t]{3,}", "  ", content)
    # 压缩多余空行
    content = re.sub(r"\n{3,}", "\n\n", content)
    return content


async def extract_document(
    content: str,
    doc_type: str,
    metadata: dict | None = None,
    filing=None,
) -> ExtractionResult:
    """统一入口：根据文档类型路由到对应管线。

    Args:
        content: 文档全文
        doc_type: 文档类型
        metadata: 可选元数据
        filing: edgartools Filing 对象（可选，用于 XBRL 提取）

    doc_type:
      - annual_report: 10-K / 年报
      - proxy: DEF 14A / 委托声明
      - earnings_call: 财报电话会
      - prospectus: 招股书 S-1 / F-1
      - competitive_intel: 行业报告 / 分析师研究
      - news: 新闻（预留）
      - event_filing: 8-K（预留）
      - investor_day: 投资者日（预留）
    """
    metadata = metadata or {}
    content = _clean_sec_text(content)

    if doc_type == "annual_report":
        from anchor.extract.pipelines.annual_report import extract_annual_report
        from anchor.extract.pipelines.xbrl_extract import XBRLData, extract_xbrl

        # XBRL-first: 尝试从 filing 提取结构化数据
        xbrl_data: XBRLData | None = None
        if filing is not None:
            try:
                xbrl_data = extract_xbrl(filing)
                ticker = metadata.get("ticker", "?")
                if xbrl_data.has_xbrl:
                    logger.info(
                        f"[DocRouter] {ticker}: XBRL 提取成功 "
                        f"({len(xbrl_data.financial_line_items)} 财务科目)"
                    )
                else:
                    logger.warning(
                        f"[DocRouter] {ticker}: XBRL 不可用 ({xbrl_data.error})"
                    )
            except Exception as e:
                logger.warning(f"[DocRouter] XBRL 提取异常: {e}")
                xbrl_data = None

        return await extract_annual_report(content, metadata, xbrl_data=xbrl_data)

    elif doc_type == "proxy":
        from anchor.extract.pipelines.proxy import extract_proxy
        return await extract_proxy(content, metadata)

    elif doc_type == "earnings_call":
        from anchor.extract.pipelines.earnings_call import extract_earnings_call
        return await extract_earnings_call(content, metadata)

    elif doc_type == "prospectus":
        from anchor.extract.pipelines.prospectus import extract_prospectus
        return await extract_prospectus(content, metadata)

    elif doc_type == "competitive_intel":
        from anchor.extract.pipelines.competitive_intel import extract_competitive_intel
        return await extract_competitive_intel(content, metadata)

    else:
        logger.warning(f"[DocRouter] 未知文档类型: {doc_type}，跳过")
        return ExtractionResult(metadata={"error": f"unknown doc_type: {doc_type}"})
