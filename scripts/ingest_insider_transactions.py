"""
SEC Form 4 内部人交易批量导入
==============================
从 SEC EDGAR 批量 TSV 下载 insider transactions，
只保留 S&P 500 公司，写入 insider_transactions 表。

数据源：https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/
每季度一个 zip（~8MB），内含 SUBMISSION.tsv + NONDERIV_TRANS.tsv + REPORTINGOWNER.tsv 等。

用法:
    python scripts/ingest_insider_transactions.py                    # 最近 4 个季度
    python scripts/ingest_insider_transactions.py --years 3          # 最近 3 年
    python scripts/ingest_insider_transactions.py --quarter 2025q3   # 指定季度
"""

from __future__ import annotations

import asyncio
import csv
import io
import sys
import tempfile
import zipfile
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

import httpx
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

SP500_CSV = Path(__file__).parent.parent / "data" / "sp500.csv"
BASE_URL = "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets"
HEADERS = {"User-Agent": "Facet Research facet@example.com", "Accept-Encoding": "gzip"}

# SEC transaction code → our transaction_type
CODE_MAP = {
    "P": "buy",
    "S": "sell",
    "G": "gift",
    "A": "award",
    "M": "exercise",
    "F": "tax",
    "C": "conversion",
    "X": "exercise",
    "D": "other",      # return to issuer
    "J": "other",
    "K": "other",
    "L": "other",
    "W": "other",
    "Z": "other",
    "U": "other",
}


def load_sp500_tickers() -> set[str]:
    tickers = set()
    with open(SP500_CSV) as f:
        for row in csv.DictReader(f):
            tickers.add(row["Symbol"].upper())
    return tickers


def _quarter_keys(n_years: int = 1) -> list[str]:
    """生成最近 n_years 年的季度 key，如 ['2025q1', '2025q2', ...]"""
    now = datetime.now()
    keys = []
    for y in range(now.year - n_years, now.year + 1):
        for q in range(1, 5):
            # 跳过未来季度
            quarter_end_month = q * 3
            if y == now.year and quarter_end_month > now.month:
                break
            keys.append(f"{y}q{q}")
    return keys


def download_quarter(client: httpx.Client, quarter: str) -> bytes | None:
    """下载一个季度的 zip 文件。"""
    url = f"{BASE_URL}/{quarter}_form345.zip"
    logger.info(f"[Form4] 下载 {url}")
    try:
        resp = client.get(url, follow_redirects=True, timeout=60)
        if resp.status_code == 200:
            logger.info(f"[Form4] {quarter}: {len(resp.content) / 1e6:.1f} MB")
            return resp.content
        else:
            logger.warning(f"[Form4] {quarter}: HTTP {resp.status_code}")
            return None
    except Exception as e:
        logger.error(f"[Form4] {quarter} 下载失败: {e}")
        return None


def _read_tsv(zf: zipfile.ZipFile, name: str) -> list[dict]:
    """从 zip 里读一个 TSV 文件。"""
    for f in zf.namelist():
        if f.upper().endswith(name.upper()):
            with zf.open(f) as fh:
                text = io.TextIOWrapper(fh, encoding="utf-8", errors="replace")
                reader = csv.DictReader(text, delimiter="\t")
                return list(reader)
    return []


def _safe_int(val: str | None) -> int | None:
    if not val:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _safe_float(val: str | None) -> float | None:
    if not val:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_date(val: str | None) -> date | None:
    """解析 SEC TSV 日期，格式可能是 '31-MAR-2025' 或 '2025-03-31'。"""
    if not val:
        return None
    val = val.strip()
    for fmt in ("%d-%b-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(val[:11], fmt).date()
        except (ValueError, TypeError):
            continue
    return None


def _parse_owner_relationship(rel: str | None) -> tuple[bool, bool, bool]:
    """解析 RPTOWNER_RELATIONSHIP 字段 → (is_director, is_officer, is_ten_pct_owner)。
    值可能是: Director, Officer, TenPercentOwner, Other, 或空。
    一个 accession 可能有多个 owner，每人只有一个 relationship。
    """
    if not rel:
        return False, False, False
    rel = rel.strip().lower()
    return ("director" in rel, "officer" in rel, "tenpercentowner" in rel)


def parse_quarter(zip_bytes: bytes, sp500_tickers: set[str]) -> list[dict]:
    """解析一个季度的 zip，返回 S&P 500 的交易记录。"""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        submissions = _read_tsv(zf, "SUBMISSION.tsv")
        owners = _read_tsv(zf, "REPORTINGOWNER.tsv")
        transactions = _read_tsv(zf, "NONDERIV_TRANS.tsv")

    if not submissions:
        logger.warning("[Form4] SUBMISSION.tsv 为空")
        return []

    # 按 accession_number 索引 submission 和 owner
    sub_by_acc: dict[str, dict] = {}
    for s in submissions:
        acc = s.get("ACCESSION_NUMBER", "")
        ticker = (s.get("ISSUERTRADINGSYMBOL") or "").upper().strip()
        if ticker in sp500_tickers:
            sub_by_acc[acc] = s

    logger.info(f"[Form4] {len(submissions)} submissions, {len(sub_by_acc)} 属于 S&P 500")

    # 按 accession_number 索引 owner（注意：一个 accession 可能有多个 owner）
    owner_by_acc: dict[str, dict] = {}
    for o in owners:
        acc = o.get("ACCESSION_NUMBER", "")
        if acc in sub_by_acc:
            # 保留第一个 owner（通常是主要 reporting person）
            if acc not in owner_by_acc:
                owner_by_acc[acc] = o

    # 解析交易
    results = []
    for t in transactions:
        acc = t.get("ACCESSION_NUMBER", "")
        if acc not in sub_by_acc:
            continue

        sub = sub_by_acc[acc]
        owner = owner_by_acc.get(acc, {})
        ticker = (sub.get("ISSUERTRADINGSYMBOL") or "").upper().strip()
        code = (t.get("TRANS_CODE") or "").upper().strip()
        trans_type = CODE_MAP.get(code, "other")
        shares = _safe_int(t.get("TRANS_SHARES"))
        price = _safe_float(t.get("TRANS_PRICEPERSHARE"))
        value = round(shares * price, 2) if shares and price else None

        # 解析 owner relationship
        is_dir, is_off, is_10pct = _parse_owner_relationship(
            owner.get("RPTOWNER_RELATIONSHIP")
        )

        # AFF10B5ONE 在 SUBMISSION 里表示是否 10b5-1 plan
        aff = (sub.get("AFF10B5ONE") or "").strip()
        is_10b5 = aff == "1" if aff else None

        # 用 accession_number + trans_sk 做唯一键
        trans_sk = t.get("NONDERIV_TRANS_SK", "")

        results.append({
            "ticker": ticker,
            "accession_number": acc,
            "_trans_sk": trans_sk,  # 仅用于去重，不写入 DB
            "person_name": (owner.get("RPTOWNERNAME") or "").strip(),
            "title": (owner.get("RPTOWNER_TITLE") or "").strip() or None,
            "is_director": is_dir,
            "is_officer": is_off,
            "is_ten_pct_owner": is_10pct,
            "transaction_type": trans_type,
            "transaction_code": code or None,
            "security_title": (t.get("SECURITY_TITLE") or "").strip() or None,
            "shares": shares,
            "price_per_share": price,
            "value": value,
            "acquired_disposed": (t.get("TRANS_ACQUIRED_DISP_CD") or "").strip() or None,
            "shares_post_transaction": _safe_int(t.get("SHRS_OWND_FOLWNG_TRANS")),
            "ownership_type": (t.get("DIRECT_INDIRECT_OWNERSHIP") or "").strip() or None,
            "is_10b5_1_plan": is_10b5,
            "transaction_date": _safe_date(t.get("TRANS_DATE")),
            "filing_date": _safe_date(sub.get("FILING_DATE")),
        })

    return results


async def write_transactions(rows: list[dict]):
    """写入 DB。"""
    from anchor.database.session import AsyncSessionLocal, create_tables
    from anchor.models import CompanyProfile, InsiderTransaction
    from sqlmodel import select

    await create_tables()

    async with AsyncSessionLocal() as session:
        # 建 ticker → company_id 映射
        result = await session.execute(
            select(CompanyProfile.id, CompanyProfile.ticker)
        )
        ticker_to_id = {r[1]: r[0] for r in result.all()}

        # 查已入库的 accession_numbers 去重（按 filing 级别去重）
        # 一个 accession_number = 一个 Form 4 filing，含多笔交易
        # 如果该 accession 已入库，跳过整个 filing 的所有交易
        existing_acc = set()
        result = await session.execute(
            select(InsiderTransaction.accession_number).where(
                InsiderTransaction.accession_number.isnot(None)
            )
        )
        existing_acc = {r[0] for r in result.all()}

        written = 0
        skipped = 0
        no_company = set()

        for row in rows:
            ticker = row.pop("ticker")
            row.pop("_trans_sk", None)  # 去掉内部字段
            company_id = ticker_to_id.get(ticker)
            if not company_id:
                no_company.add(ticker)
                continue

            acc = row.get("accession_number")
            if acc and acc in existing_acc:
                skipped += 1
                continue

            txn = InsiderTransaction(company_id=company_id, **row)
            session.add(txn)
            written += 1

            if written % 5000 == 0:
                await session.flush()

        await session.commit()

    logger.info(
        f"[Form4] 写入 {written:,} 条, 跳过重复 {skipped:,}, "
        f"无公司档案 {len(no_company)} tickers"
    )
    if no_company:
        logger.debug(f"[Form4] 无公司档案: {sorted(no_company)}")

    return written


async def main():
    args = sys.argv[1:]

    # 解析参数
    n_years = 1
    specific_quarter = None
    for i, arg in enumerate(args):
        if arg == "--years" and i + 1 < len(args):
            n_years = int(args[i + 1])
        if arg == "--quarter" and i + 1 < len(args):
            specific_quarter = args[i + 1]

    sp500 = load_sp500_tickers()
    logger.info(f"[Form4] S&P 500: {len(sp500)} tickers")

    if specific_quarter:
        quarters = [specific_quarter]
    else:
        quarters = _quarter_keys(n_years)

    logger.info(f"[Form4] 下载 {len(quarters)} 个季度: {quarters}")

    all_rows = []
    with httpx.Client(headers=HEADERS, timeout=60) as client:
        for q in quarters:
            data = download_quarter(client, q)
            if data:
                rows = parse_quarter(data, sp500)
                logger.info(f"[Form4] {q}: {len(rows)} 条 S&P 500 交易")
                all_rows.extend(rows)

    logger.info(f"[Form4] 共 {len(all_rows):,} 条交易待写入")

    if all_rows:
        written = await write_transactions(all_rows)
        logger.info(f"[Form4] 完成: {written:,} 条新增")
    else:
        logger.warning("[Form4] 无数据")


if __name__ == "__main__":
    asyncio.run(main())
