"""
Axion CLI
=========
axion score --ticker NVDA --period FY2025
axion list-companies
axion list-features
"""

import json
from datetime import datetime, timezone

import click

from axion.db import anchor
from axion.db.session import create_tables, get_connection
from axion.features.pipeline import compute_features
from axion.scoring.scorer import CompanyAnalysis, format_report, score_company


@click.group()
def cli():
    """Axion — 产业模型"""
    pass


@cli.command()
@click.option("--ticker", required=True, help="股票代码，如 NVDA / 600519.SH")
@click.option("--period", default=None, help="报告期，如 FY2025。不填则用最新。")
def score(ticker: str, period: str | None):
    """对一家公司评分。"""
    # 1. 查公司
    company_id = anchor.resolve_company_id(ticker)
    if company_id is None:
        click.echo(f"Company not found: {ticker}")
        click.echo("Use 'axion list-companies' to see available companies.")
        return

    profile = anchor.get_company_profile(company_id)
    company_name = profile["name"] if profile else ticker

    # 2. 确定 period
    if period is None:
        periods = anchor.get_periods(company_id)
        if not periods:
            click.echo(f"No data periods found for {ticker}")
            return
        period = periods[-1]
        click.echo(f"Using latest period: {period}")

    # 3. 计算特征
    click.echo(f"Computing features for {company_name} ({ticker}) / {period}...")
    feature_results = compute_features(company_id, period)
    features = {name: fr.value for name, fr in feature_results.items()}

    click.echo(f"  Computed {len(features)} features")

    # 4. 评分
    result = score_company(
        company_id=company_id,
        company_name=company_name,
        ticker=ticker,
        period=period,
        features=features,
    )

    # 5. 输出报告
    click.echo(format_report(result))

    # 6. 输出特征明细
    if feature_results:
        click.echo("-" * 56)
        click.echo("  Features")
        click.echo("-" * 56)
        for name, fr in sorted(feature_results.items()):
            detail = f"  ({fr.detail})" if fr.detail else ""
            click.echo(f"  {name}: {fr.value:.4f}{detail}")
        click.echo("")

    # 7. 持久化
    create_tables()
    _save_results(result, features)
    click.echo("  Results saved to Axion DB.")


def _save_results(result: CompanyAnalysis, features: dict[str, float]):
    """将结果写入 Axion DB。"""
    from sqlalchemy import text as sa_text

    now = datetime.now(timezone.utc).isoformat()

    with get_connection() as conn:
        # 避免重复：先删除同 company_id + period 的旧数据
        conn.execute(
            sa_text("DELETE FROM feature_values WHERE company_id = :cid AND period = :p"),
            {"cid": result.company_id, "p": result.period},
        )
        conn.execute(
            sa_text("DELETE FROM company_scores WHERE company_id = :cid AND period = :p"),
            {"cid": result.company_id, "p": result.period},
        )

        # 写特征值
        for name, value in features.items():
            conn.execute(
                sa_text(
                    "INSERT INTO feature_values "
                    "(company_id, period, feature_name, value, computed_at) "
                    "VALUES (:cid, :p, :name, :val, :ts)"
                ),
                {"cid": result.company_id, "p": result.period, "name": name, "val": value, "ts": now},
            )

        # 写评分（三流派独立结果）
        school_scores = {}
        top_drivers = []

        for school_name, school_result in [
            ("buffett", result.buffett),
            ("dalio", result.dalio),
            ("soros", result.soros),
        ]:
            if school_result is None:
                continue
            ss = school_result.school_score
            school_scores[school_name] = {
                "score": ss.score,
                "raw_points": ss.raw_points,
                "signal": ss.signal,
                "drivers": [
                    {"rule": d.rule_name, "pts": d.contribution, "desc": d.description}
                    for d in ss.drivers
                ],
            }
            for d in ss.drivers:
                top_drivers.append(
                    {"school": school_name, "rule": d.rule_name, "pts": d.contribution}
                )

        if result.buffett:
            school_scores["buffett"]["filters_passed"] = result.buffett.filters_passed
            school_scores["buffett"]["valuation_status"] = result.buffett.valuation_status

        if result.soros:
            school_scores["soros"]["reflexivity_phase"] = result.soros.reflexivity_phase

        top_drivers.sort(key=lambda x: abs(x["pts"]), reverse=True)

        conn.execute(
            sa_text(
                "INSERT INTO company_scores "
                "(company_id, period, model_version, dimension_scores_json, "
                "composite_score, top_drivers_json, scored_at) "
                "VALUES (:cid, :p, :mv, :scores, :cs, :drivers, :ts)"
            ),
            {
                "cid": result.company_id,
                "p": result.period,
                "mv": result.model_version,
                "scores": json.dumps(school_scores, ensure_ascii=False),
                "cs": 0.0,
                "drivers": json.dumps(top_drivers[:10], ensure_ascii=False),
                "ts": now,
            },
        )
        conn.commit()


@cli.command("list-companies")
def list_companies():
    """列出 Anchor DB 中的公司。"""
    df = anchor.query_df(
        "SELECT id, ticker, name, market, industry "
        "FROM company_profiles ORDER BY ticker"
    )
    if df.empty:
        click.echo("No companies found in Anchor DB.")
        return
    for _, row in df.iterrows():
        click.echo(
            f"  {row['ticker']:<12s} {row['name']:<30s} "
            f"{row['market']:<6s} {row['industry'] or ''}"
        )


@cli.command("list-features")
def list_features():
    """列出已注册的特征。"""
    from axion.features.registry import get_features

    features = get_features()
    if not features:
        click.echo("No features registered.")
        return
    for f in sorted(features, key=lambda x: x.name):
        click.echo(f"  [{f.level.value}] {f.name:<45s} domain={f.domain}")


if __name__ == "__main__":
    cli()
