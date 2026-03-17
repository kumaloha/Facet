"""
anchor.cli — 统一 CLI 入口
===========================
安装后：  anchor run-url <url>
开发模式：python -m anchor run-url <url>
"""
from __future__ import annotations

import click

from anchor import __version__


def _load_env():
    """Load .env before any business import."""
    import os
    from pathlib import Path

    from dotenv import load_dotenv

    # 优先加载项目根目录 .env
    env_path = Path.cwd() / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()

    os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///./anchor.db")


@click.group()
@click.version_option(version=__version__, prog_name="anchor")
def main():
    """Anchor — 多模式信息提取与事实验证引擎"""
    _load_env()


@main.command("run-url")
@click.argument("target")
@click.option("--force", is_flag=True, help="强制重新抓取并覆盖已有记录")
@click.option("--fill-gaps", is_flag=True, help="增量模式：只提取缺失的表，跳过已有数据")
def run_url(target: str, force: bool, fill_gaps: bool):
    """分析单条 URL 或本地文件/目录"""
    from anchor.commands.run_url import run_url_command

    run_url_command(target, force=force, fill_gaps=fill_gaps)


@main.command()
@click.option("--dry-run", is_flag=True, help="仅预览新 URL，不执行分析")
@click.option("--source", default=None, metavar="NAME", help="仅处理名称含该字符串的来源")
@click.option("--limit", default=0, type=int, metavar="N", help="每个来源最多处理条数（0=不限）")
@click.option("--concurrency", default=5, type=int, metavar="N", help="并行提取 worker 数量")
@click.option("--since", default=None, metavar="YYYY-MM-DD", help="只抓此日期之后的文章")
@click.option("--force", "-f", is_flag=True, help="强制重新处理所有 URL")
def monitor(dry_run: bool, source: str | None, limit: int, concurrency: int, since: str | None, force: bool):
    """从 sources.yaml 批量拉取新文章并分析"""
    from anchor.commands.monitor import monitor_command

    monitor_command(
        dry_run=dry_run,
        source=source,
        limit=limit,
        concurrency=concurrency,
        since=since,
        force=force,
    )


@main.command("company-sources")
@click.argument("ticker", required=False, default=None)
@click.option("--name", default=None, metavar="NAME", help="按公司名称查找（模糊匹配）")
@click.option("--years", default=5, type=int, metavar="N", help="期望覆盖最近 N 年（默认5）")
def company_sources(ticker: str | None, name: str | None, years: int):
    """查询指定公司的全部输入源 URL 及年份覆盖"""
    if not ticker and not name:
        click.echo("请指定 ticker（如 NVDA）或 --name（如 --name 台积电）")
        raise SystemExit(1)
    from anchor.commands.company_sources import company_sources_command

    company_sources_command(ticker, name, years)


@main.command()
@click.argument("ticker")
@click.option("--years", default=5, type=int, metavar="N", help="拉取最近 N 年（默认5）")
@click.option("--fill-gaps", is_flag=True, help="增量模式：跳过已有数据的 topic")
def backfill(ticker: str, years: int, fill_gaps: bool):
    """自动从 SEC EDGAR 拉取历年 10-K 并提取"""
    from anchor.commands.backfill import backfill_command

    backfill_command(ticker, years, fill_gaps)


@main.command("market-update")
@click.option("--ticker", default=None, metavar="TICKER", help="只更新指定股票（不填则更新全部已跟踪公司）")
@click.option("--macro-only", is_flag=True, help="只更新宏观指标")
@click.option("--days", default=30, type=int, metavar="N", help="拉取最近 N 天数据（默认30）")
def market_update(ticker: str | None, macro_only: bool, days: int):
    """更新市场数据（个股行情 + 宏观指标）"""
    import asyncio

    from anchor.collect.market import market_update as _market_update

    result = asyncio.run(_market_update(ticker=ticker, macro_only=macro_only, days=days))
    click.echo(f"Done. Stocks: +{result['stocks']} rows, Macro: +{result['macro']} rows")


@main.command()
@click.option("--host", default="0.0.0.0", help="绑定地址")
@click.option("--port", default=8765, type=int, help="监听端口")
def serve(host: str, port: int):
    """启动 Web UI 服务"""
    from anchor.commands.serve import serve_command

    serve_command(host=host, port=port)
