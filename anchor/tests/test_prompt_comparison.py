"""
新旧 prompt 对比测试
====================
用同一段 NVDA 10-K 的 business section 分别测试旧 prompt（全量）和新 prompt（聚焦）。
对比：速度、输出量、提取质量。

用法：
    uv run python anchor/tests/test_prompt_comparison.py
"""

import asyncio
import json
import re
import sqlite3
import sys
import time

# 旧 prompt（从 git history df12b5f 提取，精简版）
OLD_SYSTEM = """\
你是一位资深基本面分析师。从公司财报/年报/Proxy Statement 中提取**非财务结构化信息**。

★ 重要：财务三表数据（利润表/资产负债表/现金流量表）由其他数据源提供，你**不需要提取**。
★ 你的任务是提取三表以外的所有定性和半定量信息。

## 输出格式
输出一个 JSON 对象。如果原文中没有某类信息，对应字段返回空数组 [] 或 null。

```json
{
  "is_relevant_content": true,
  "skip_reason": null,
  "company": {"name": "公司全名", "ticker": "股票代码", "market": "us", "industry": "行业", "summary": "商业模式"},
  "period": "FY2025",
  "operational_issues": [{"topic": "议题", "performance": "表现", "attribution": "归因", "risk": "风险", "guidance": "指引"}],
  "narratives": [{"narrative": "故事", "capital_required": null, "promised_outcome": "结果", "deadline": null}],
  "downstream_segments": [{"segment": null, "customer_name": "客户", "customer_type": "direct", "products": "产品", "revenue": null, "revenue_pct": null, "growth_yoy": null, "pricing_model": null, "switching_cost_level": null, "description": null}],
  "upstream_segments": [{"segment": null, "supplier_name": "供应商", "supply_type": "foundry", "material_or_service": "内容", "geographic_location": "所在地", "is_sole_source": false, "description": null}],
  "geographic_revenues": [{"region": "地域", "revenue": null, "revenue_share": null}],
  "non_financial_kpis": [{"kpi_name": "指标", "kpi_value": "值", "kpi_unit": "单位", "category": "workforce"}],
  "debt_obligations": [{"instrument_name": "债务", "debt_type": "bond", "principal": null, "interest_rate": null, "maturity_date": null}],
  "litigations": [{"case_name": "案件", "case_type": "lawsuit", "status": "pending", "description": "摘要"}],
  "executive_compensations": [{"name": "姓名", "title": "职位", "total_comp": null, "pay_ratio": null}],
  "stock_ownership": [{"name": "持有人", "shares_beneficially_owned": null, "percent_of_class": null}],
  "related_party_transactions": [{"related_party": "关联方", "transaction_type": "sale", "amount": null}],
  "pricing_actions": [{"product_or_segment": "产品", "price_change_pct": null}],
  "competitor_relations": [{"competitor_name": "竞对", "market_segment": "市场", "relationship_type": "direct_competitor"}],
  "market_share_data": [{"company_or_competitor": "公司", "market_segment": "市场", "share_pct": null}],
  "known_issues": [{"issue_description": "问题", "severity": "major"}],
  "management_acknowledgments": [{"issue_description": "问题", "response_quality": "forthright"}],
  "executive_changes": [{"person_name": "姓名", "change_type": "joined", "change_date": null}],
  "audit_opinion": {"opinion_type": "unqualified", "auditor_name": "事务所"},
  "management_guidance": [{"target_period": "FY2026", "metric": "revenue_growth", "value_low": null, "value_high": null, "verbatim": "原文"}],
  "inventory_provisions": [{"provision_amount": null}],
  "deferred_revenues": [{"total_deferred": null}],
  "revenue_recognition_policies": [{"category": "product", "policy": "确认方式"}],
  "purchase_obligation_summaries": [{"total_outstanding": null}],
  "asp_trends": [{"product_category": "产品", "trend": "趋势"}],
  "recurring_revenue_breakdowns": [{"recurring_pct": null}],
  "summary": "中文摘要",
  "one_liner": "一句话"
}
```

## 规则
1. 每个独立事实单独成条目
2. 无数据返回空数组
3. 只输出 JSON
★ summary 和 one_liner 必须中文。\
"""


def count_items(raw_json: str) -> dict[str, int]:
    """从 JSON 输出中统计各表的条目数。"""
    try:
        data = json.loads(raw_json)
    except json.JSONDecodeError:
        # 尝试修复常见 JSON 错误
        cleaned = raw_json.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r'^```\w*\n?', '', cleaned)
            cleaned = re.sub(r'\n?```$', '', cleaned)
        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError:
            return {"_parse_error": 1}

    counts = {}
    for key, val in data.items():
        if isinstance(val, list):
            counts[key] = len(val)
        elif isinstance(val, dict) and key not in ("company",):
            counts[key] = 1 if any(v for v in val.values()) else 0
    return counts


async def test_prompt(label: str, system: str, user_msg: str) -> dict:
    from anchor.extract.pipelines._base import call_llm

    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  System prompt: {len(system):,} chars")
    print(f"{'='*60}")

    t0 = time.monotonic()
    raw = await call_llm(system, user_msg, 8192)
    elapsed = time.monotonic() - t0

    if raw is None:
        print(f"  FAILED: None response ({elapsed:.1f}s)")
        return {"label": label, "time": elapsed, "ok": False}

    print(f"  Time: {elapsed:.1f}s")
    print(f"  Output: {len(raw):,} chars")

    counts = count_items(raw)
    if "_parse_error" in counts:
        print(f"  Parse FAILED")
        print(f"  Raw (first 500): {raw[:500]}")
    else:
        total = sum(counts.values())
        print(f"  Total items: {total}")
        for k, v in sorted(counts.items()):
            if v > 0:
                print(f"    {k}: {v}")

    return {"label": label, "time": elapsed, "output_len": len(raw), "counts": counts, "ok": True}


async def main():
    # 读取 NVDA 10-K
    conn = sqlite3.connect("data/anchor.db")
    content = conn.execute("SELECT content FROM raw_posts WHERE id = 1").fetchone()[0]
    conn.close()

    from anchor.extract.sec_10k_splitter import split_10k, get_sections_for_topic
    sections = split_10k(content)
    business_text = get_sections_for_topic("business", sections)

    print(f"Input: NVDA FY2026 10-K business section ({len(business_text):,} chars)")

    user_msg = f"""## 文章信息
平台：SEC EDGAR
作者：NVIDIA Corporation
日期：2026-02-25

## 文章内容

{business_text[:100000]}

## 提取任务

请从上述文章中提取结构化信息，严格按照 system prompt 指定的 JSON 格式输出。"""

    # Test 1: 旧 prompt（全量，20+ 表类型）
    r_old = await test_prompt("旧 prompt（全量 20+ 表类型）", OLD_SYSTEM, user_msg)

    # Test 2: 新 prompt（聚焦 business）
    from anchor.extract.pipelines.company import SYSTEM_PROMPTS
    r_new = await test_prompt("新 prompt（聚焦 business）", SYSTEM_PROMPTS["business"], user_msg)

    # 对比
    print(f"\n{'='*60}")
    print(f"  对比总结")
    print(f"{'='*60}")
    if r_old["ok"] and r_new["ok"]:
        speedup = r_old["time"] / r_new["time"] if r_new["time"] > 0 else 0
        print(f"  速度: 旧={r_old['time']:.1f}s  新={r_new['time']:.1f}s  加速={speedup:.1f}x")
        print(f"  输出: 旧={r_old['output_len']:,} chars  新={r_new['output_len']:,} chars")

        old_total = sum(r_old.get("counts", {}).values())
        new_total = sum(r_new.get("counts", {}).values())
        print(f"  条目: 旧={old_total}  新={new_total}")


if __name__ == "__main__":
    asyncio.run(main())
