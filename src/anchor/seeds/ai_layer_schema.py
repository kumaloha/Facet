"""
AI 产业链 LayerSchema 种子数据 — thin wrapper
==============================================
实际实现已移至 polaris.seeds.ai_layer_schema。

用法：
    python -m anchor.seeds.ai_layer_schema
    python -m polaris.seeds.ai_layer_schema   (推荐)
"""

from polaris.seeds.ai_layer_schema import seed, INDUSTRY_CHAIN, AI_LAYER_METRICS  # noqa: F401

if __name__ == "__main__":
    import asyncio
    asyncio.run(seed())
