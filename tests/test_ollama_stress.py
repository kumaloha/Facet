"""
Ollama 压力测试
===============
测试本地 Ollama 的最大 QPS 和稳定性。

用法：
    uv run python anchor/tests/test_ollama_stress.py
    uv run python anchor/tests/test_ollama_stress.py --max-concurrency 5
"""

import asyncio
import time
import sys

from anchor.llm_client import chat_completion

# 短 prompt，测纯吞吐
SYSTEM_SHORT = "你是一个 JSON 生成器。只输出 JSON，不要输出其他内容。"
USER_SHORT = '输出 {"status": "ok", "timestamp": 当前时间戳}'

# 中等 prompt，模拟真实提取
SYSTEM_MEDIUM = "你是一位资深基本面分析师。从以下文本中提取公司经营议题，输出 JSON。"
USER_MEDIUM = """
NVIDIA fiscal year 2026 revenue was $215.9 billion, up 65% from fiscal year 2025.
Data Center revenue was $190.4 billion, up 68%, driven by demand for Blackwell architecture.
Gaming revenue was $14.6 billion, up 41%. Professional Visualization revenue was $2.5 billion, up 70%.
Key risks include supply chain constraints, export controls on China (H20 product $4.5B charge),
and competition from open-source AI models. Management guided for continued growth with Rubin platform
expected to ship in FY2027 second half.

请提取经营议题，每个议题包含 topic, performance, attribution, risk, guidance 字段。
"""


async def single_call(label: str, system: str, user: str, max_tokens: int) -> dict:
    """执行单次 LLM 调用，返回 {label, ok, latency_ms, output_len, error}。"""
    t0 = time.monotonic()
    try:
        resp = await chat_completion(system=system, user=user, max_tokens=max_tokens)
        latency = (time.monotonic() - t0) * 1000
        if resp is None:
            return {"label": label, "ok": False, "latency_ms": latency, "output_len": 0, "error": "None response"}
        return {
            "label": label,
            "ok": True,
            "latency_ms": latency,
            "output_len": len(resp.content),
            "error": None,
            "input_tokens": resp.input_tokens,
            "output_tokens": resp.output_tokens,
        }
    except Exception as e:
        latency = (time.monotonic() - t0) * 1000
        return {"label": label, "ok": False, "latency_ms": latency, "output_len": 0, "error": str(e)[:200]}


async def run_concurrency_test(concurrency: int, system: str, user: str, max_tokens: int, n_calls: int) -> list[dict]:
    """以指定并发度发送 n_calls 次请求。"""
    sem = asyncio.Semaphore(concurrency)

    async def limited_call(i: int) -> dict:
        async with sem:
            return await single_call(f"c{concurrency}_#{i}", system, user, max_tokens)

    tasks = [limited_call(i) for i in range(n_calls)]
    return await asyncio.gather(*tasks)


def print_results(concurrency: int, results: list[dict], wall_time: float):
    ok = [r for r in results if r["ok"]]
    fail = [r for r in results if not r["ok"]]
    n = len(results)

    if ok:
        latencies = [r["latency_ms"] for r in ok]
        avg_lat = sum(latencies) / len(latencies)
        p50 = sorted(latencies)[len(latencies) // 2]
        p99 = sorted(latencies)[int(len(latencies) * 0.99)]
        qps = len(ok) / wall_time if wall_time > 0 else 0
    else:
        avg_lat = p50 = p99 = qps = 0

    print(f"  并发={concurrency}  总={n}  成功={len(ok)}  失败={len(fail)}  "
          f"QPS={qps:.2f}  avg={avg_lat:.0f}ms  p50={p50:.0f}ms  p99={p99:.0f}ms  "
          f"wall={wall_time:.1f}s")

    for r in fail[:3]:
        print(f"    FAIL: {r['error']}")


async def main(max_concurrency: int = 3):
    n_calls = 2  # 每个并发度测试的总调用次数（本地模型用小值）

    print("=" * 70)
    print("  Ollama 压力测试")
    print("=" * 70)

    # Phase 1: 短 prompt（测纯连接和响应速度）
    print("\n--- Phase 1: 短 prompt（测连接速度）---")
    for c in range(1, max_concurrency + 1):
        t0 = time.monotonic()
        results = await run_concurrency_test(c, SYSTEM_SHORT, USER_SHORT, 128, n_calls)
        wall = time.monotonic() - t0
        print_results(c, results, wall)

    # Phase 2: 中等 prompt（模拟真实提取）
    print("\n--- Phase 2: 中等 prompt（模拟提取）---")
    for c in range(1, max_concurrency + 1):
        t0 = time.monotonic()
        results = await run_concurrency_test(c, SYSTEM_MEDIUM, USER_MEDIUM, 2048, n_calls)
        wall = time.monotonic() - t0
        print_results(c, results, wall)

        # 如果全部失败，停止升级并发
        if all(not r["ok"] for r in results):
            print(f"  ⛔ 并发={c} 全部失败，停止测试")
            break

    # Phase 3: 大 prompt（测 context window 极限）
    print("\n--- Phase 3: 大 prompt（测 context window）---")
    large_user = USER_MEDIUM * 20  # 重复 20 次，模拟大输入
    for size_label, text in [("~5K", USER_MEDIUM * 5), ("~10K", USER_MEDIUM * 10), ("~20K", USER_MEDIUM * 20)]:
        t0 = time.monotonic()
        results = await run_concurrency_test(1, SYSTEM_MEDIUM, text, 2048, 2)
        wall = time.monotonic() - t0
        ok = [r for r in results if r["ok"]]
        fail = [r for r in results if not r["ok"]]
        input_tok = ok[0].get("input_tokens", "?") if ok else "?"
        print(f"  input≈{size_label} chars  input_tokens={input_tok}  "
              f"成功={len(ok)}  失败={len(fail)}  wall={wall:.1f}s")
        if fail:
            print(f"    FAIL: {fail[0]['error']}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    mc = int(sys.argv[sys.argv.index("--max-concurrency") + 1]) if "--max-concurrency" in sys.argv else 3
    asyncio.run(main(max_concurrency=mc))
