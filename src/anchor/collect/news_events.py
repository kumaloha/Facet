"""
新闻事件采集
============

从RSS/Web抓取财经新闻 → LLM提取结构化事件 → 映射到五力框架

输出: 结构化事件列表，每个事件包含:
- 发生了什么
- 影响哪个Force
- 具体影响哪个行业/公司
- 对资产的含义
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class EconomicEvent:
    """结构化经济事件"""
    headline: str              # 一句话描述
    source: str                # 来源(RSS/web)
    date: str                  # YYYY-MM-DD

    # 五力映射
    force_id: str              # "f1a"/"f1b"/"f2"/"f3"/"f4"/"f5"
    force_impact: str          # "positive"/"negative"/"neutral"

    # 具体影响
    affected_sectors: list[str] = field(default_factory=list)    # ["XLK", "XLI"] GICS行业
    affected_entities: list[str] = field(default_factory=list)   # ["AAPL", "TSMC"] 具体公司
    transmission: str = ""     # 传导链描述: "关税→进口成本↑→电子制造业盈利↓"

    # 资产含义
    asset_implications: dict[str, str] = field(default_factory=dict)  # {"equity": "negative", "gold": "positive"}

    confidence: float = 0.5    # 0-1


# ━━ LLM 提取 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


_EXTRACTION_PROMPT = """分析以下财经新闻，提取经济事件信息。

新闻: {content}

请用JSON格式回答:
{{
    "headline": "一句话事件描述",
    "is_economic_event": true/false,
    "force": "f1a/f1b/f2/f3/f4/f5 (短债务/长债务/内部秩序/外部秩序/自然之力/技术)",
    "force_impact": "positive/negative/neutral",
    "affected_sectors": ["XLK", "XLI"],
    "affected_entities": ["AAPL", "TSMC"],
    "transmission": "事件→影响路径→最终效果",
    "asset_implications": {{"equity": "negative", "gold": "positive"}},
    "confidence": 0.8
}}

行业代码: XLK科技 XLV医疗 XLF金融 XLE能源 XLI工业 XLY可选消费 XLP必选消费 XLU公用事业 XLB材料 XLRE地产 XLC通信

如果不是经济相关事件，返回 {{"is_economic_event": false}}
只返回JSON，不要其他文字。"""


async def fetch_news_events(max_items: int = 20) -> list[EconomicEvent]:
    """从RSS源抓取最新新闻，LLM提取结构化事件。"""
    from anchor.collect.rss import RSSCollector

    collector = RSSCollector()
    posts = await collector.collect()

    # 取最新的max_items条
    posts = sorted(posts, key=lambda p: p.posted_at, reverse=True)[:max_items]

    # LLM批量提取事件
    events = []
    for post in posts:
        event = await _extract_event(post)
        if event:
            events.append(event)

    return events


async def _extract_event(post) -> EconomicEvent | None:
    """用LLM从新闻中提取结构化事件。"""
    from anchor.llm_client import chat_completion

    prompt = _EXTRACTION_PROMPT.format(content=post.content[:500])

    try:
        response = await chat_completion(prompt)
        data = json.loads(response.content)
        if not data.get("is_economic_event"):
            return None
        return EconomicEvent(
            headline=data["headline"],
            source=post.url if hasattr(post, "url") else "rss",
            date=(
                post.posted_at.strftime("%Y-%m-%d")
                if hasattr(post.posted_at, "strftime")
                else str(post.posted_at)
            ),
            force_id=data.get("force", ""),
            force_impact=data.get("force_impact", "neutral"),
            affected_sectors=data.get("affected_sectors", []),
            affected_entities=data.get("affected_entities", []),
            transmission=data.get("transmission", ""),
            asset_implications=data.get("asset_implications", {}),
            confidence=data.get("confidence", 0.5),
        )
    except Exception:
        return None


# ━━ 关键词 fallback（不需要 LLM） ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


# 关键词 → 五力映射
_KEYWORD_FORCE_MAP = {
    # F1a 短债务
    "interest rate": "f1a", "fed": "f1a", "rate hike": "f1a", "rate cut": "f1a",
    "利率": "f1a", "加息": "f1a", "降息": "f1a", "美联储": "f1a",
    "credit": "f1a", "lending": "f1a", "mortgage": "f1a",
    # F1b 长债务
    "national debt": "f1b", "treasury": "f1b", "deficit": "f1b",
    "国债": "f1b", "赤字": "f1b", "债务上限": "f1b",
    # F2 内部秩序
    "inequality": "f2", "protest": "f2", "election": "f2", "populism": "f2",
    "不平等": "f2", "抗议": "f2", "选举": "f2", "民粹": "f2",
    # F3 外部秩序
    "tariff": "f3", "sanction": "f3", "trade war": "f3", "military": "f3",
    "关税": "f3", "制裁": "f3", "贸易战": "f3", "军事": "f3",
    "china": "f3", "russia": "f3", "taiwan": "f3",
    # F4 自然之力
    "pandemic": "f4", "earthquake": "f4", "hurricane": "f4", "drought": "f4",
    "疫情": "f4", "地震": "f4", "飓风": "f4", "气候": "f4",
    "supply chain": "f4", "供应链": "f4", "shipping": "f4",
    # F5 技术
    "AI": "f5", "artificial intelligence": "f5", "productivity": "f5",
    "人工智能": "f5", "芯片": "f5", "semiconductor": "f5",
}

# 关键词 → 行业映射
_KEYWORD_SECTOR_MAP = {
    "semiconductor": ["XLK"], "芯片": ["XLK"], "chip": ["XLK"],
    "AI": ["XLK"], "tech": ["XLK"], "科技": ["XLK"],
    "oil": ["XLE"], "energy": ["XLE"], "石油": ["XLE"], "能源": ["XLE"],
    "bank": ["XLF"], "银行": ["XLF"], "financial": ["XLF"],
    "pharma": ["XLV"], "drug": ["XLV"], "医药": ["XLV"],
    "real estate": ["XLRE"], "housing": ["XLRE"], "房地产": ["XLRE"],
    "auto": ["XLY"], "汽车": ["XLY"],
    "manufacturing": ["XLI"], "制造": ["XLI"], "industrial": ["XLI"],
}


def extract_events_keyword(headlines: list[dict]) -> list[EconomicEvent]:
    """关键词提取（不需要LLM的fallback）。

    Args:
        headlines: [{"title": "...", "date": "...", "source": "..."}]
    """
    events = []
    for h in headlines:
        title = h.get("title", "").lower()

        # 匹配Force
        force = None
        for keyword, fid in _KEYWORD_FORCE_MAP.items():
            if keyword.lower() in title:
                force = fid
                break
        if not force:
            continue

        # 匹配行业
        sectors: list[str] = []
        for keyword, secs in _KEYWORD_SECTOR_MAP.items():
            if keyword.lower() in title:
                sectors.extend(secs)
        sectors = list(set(sectors))

        events.append(EconomicEvent(
            headline=h.get("title", ""),
            source=h.get("source", ""),
            date=h.get("date", ""),
            force_id=force,
            force_impact="negative",  # 默认负面（新闻多报忧）
            affected_sectors=sectors,
            affected_entities=[],
            transmission="",
            asset_implications={},
            confidence=0.3,  # 关键词匹配信心低
        ))

    return events
