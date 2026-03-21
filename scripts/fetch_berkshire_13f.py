#!/usr/bin/env python3
"""
Fetch Berkshire Hathaway 13F holdings from SEC EDGAR (2006-2024).
Parses XML information tables, aggregates by issuer, maps CUSIP->ticker,
and outputs: data/berkshire_holdings.json
"""

import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError

HEADERS = {"User-Agent": "Facet Research project@facet.local"}
CIK = "1067983"
BASE = "https://data.sec.gov"

# ── CUSIP -> Ticker mapping ──
CUSIP_TO_TICKER = {
    # Core long-term holdings
    "025816109": "AXP",
    "191216100": "KO",
    "037833100": "AAPL",
    "060505104": "BAC",
    "166764100": "CVX",
    "674599105": "OXY",
    "500754106": "KHC",
    "615369105": "MCO",
    "949746101": "WFC",
    "742718109": "PG",
    "20825C104": "COP",
    # Banks & financials
    "902973304": "USB",
    "064058100": "BK",
    "38141G104": "GS",
    "46625H100": "JPM",
    "172967424": "C",
    "55261F104": "MTB",
    "87165B103": "SYF",
    "14040H105": "COF",
    "02005N100": "ALLY",
    "891027104": "GL",
    "37959E102": "GL",
    # Tech
    "459200101": "IBM",
    "92826C839": "V",
    "57636Q104": "MA",
    "92343E102": "VRSN",
    "023135106": "AMZN",
    "833445109": "SNOW",
    "67066G104": "NU",
    "40434L105": "HPQ",
    # Consumer
    "501044101": "KR",
    "931142103": "WMT",
    "22160K105": "COST",
    "478160104": "JNJ",
    "035229103": "BUD",
    "03524A108": "BUD",
    "423074101": "HNZ",
    "609207105": "MDLZ",
    "21036P108": "STZ",
    # Energy
    "718507105": "PSX",
    "867224107": "SU",
    # Healthcare
    "23918K108": "DVA",
    "881624209": "TEVA",
    "80105N105": "SNY",
    # Industrials
    "369604103": "GE",
    "37045V100": "GM",
    "244199105": "DE",
    # Telecom/Media
    "92343V104": "VZ",
    "16119P108": "CHTR",
    "82967N108": "SIRI",
    "531229102": "LSXMA",
    "531229888": "LSXMK",
    "872590104": "TMUS",
    "25490A101": "DTV",
    "92556H206": "PARA",
    "92556H107": "PARA",
    # Insurance
    "570535104": "MKL",
    "G0408V102": "AON",
    "H1467J104": "CB",
    "00440Q102": "CB",
    # Other
    "384637104": "GHC",
    "693483105": "PKX",
    "76131D103": "QSR",
    "862121100": "STOR",
    "86255N107": "STNE",
    "81752R100": "SRG",
    "74967X103": "RH",
    "00507V109": "ATVI",
    "150870103": "CE",
    "339750101": "FND",
    "92345Y106": "VRSK",
    "05454B104": "AXTA",
    "472319204": "JEF",
    "422806109": "HEI",
    "90384S303": "ULTA",
    "23331A109": "DHI",
    "62944T105": "NVR",
    "546347105": "LPX",
    "526057104": "LEN",
    "25754A201": "DPZ",
    "73278L105": "POOL",
    "844741108": "LUV",
    "247361702": "DAL",
    "910047109": "UAL",
    "02376R102": "AAL",
    # Pre-2013 era
    "029712106": "ASD",
    "03076C106": "AMP",
    "093671105": "HRB",
    "20030N200": "CMCSA",
    "200334100": "CDCO",
    "319963104": "FDC",
    "364730101": "GCI",
    "364760108": "GPS",
    "437076102": "HD",
    "462846106": "IRM",
    "529771107": "LXK",
    "548661107": "LOW",
    "654106103": "NKE",
    "689899102": "OSI",
    "71646E100": "PTR",
    "720279108": "PIR",
    "81211K100": "SEE",
    "81760N109": "SVM",
    "867914103": "STI",
    "903293405": "USG",
    "740189105": "PCP",
    "584404107": "MEG",
    "92553P201": "VIAB",
    "523768109": "LEE",
    "30231G102": "XOM",
    "25243Q205": "DEO",
    "G5960L103": "MDT",
    "571748102": "MMC",
    "531229409": "LBTYA",
    "530715106": "LBTYA",
    "951347105": "WSC",
    # Resolved from OpenFIGI and filing text
    "12189T104": "BNI",    # Burlington Northern Santa Fe
    "950817106": "WSC",    # Wesco Financial Corp (same ticker)
    "50075N104": "KFT",    # Kraft Foods (pre-KHC split)
    "907818108": "UNP",    # Union Pacific
    "939640108": "WPO",    # Washington Post Co
    "760759100": "RSG",    # Republic Services
    "210371100": "CEG",    # Constellation Energy
    "892893108": "TRV",    # Travelers Companies
    "902124106": "TYC",    # Tyco International
    "143130102": "KMX",    # CarMax
    "49773V107": "KFT",    # Kraft Foods (alt CUSIP)
    "260561105": "DJ",     # Dow Jones & Co
    "337738108": "FISV",   # Fiserv
    "62985Q101": "NLC",    # Nalco Holding
    "641069406": "NSRGY",  # Nestle ADR
    "91324P102": "UNH",    # UnitedHealth
    "126650100": "CVS",    # CVS Health
    "458140100": "INTC",   # Intel
    "959802109": "WU",     # Western Union
    "369550108": "GD",     # General Dynamics
    # Second batch of unmapped CUSIPs
    "718546104": "PSX",    # Phillips 66 (alt CUSIP)
    "25490A309": "DTV",    # DirecTV (Class A, alt CUSIP)
    "16117M305": "CHTR",   # Charter Communications (Class C)
    "00206R102": "T",      # AT&T
    "874039100": "TSM",    # Taiwan Semiconductor ADR
    "61166W101": "MON",    # Monsanto (acquired by Bayer 2018)
    "00287Y109": "ABBV",   # AbbVie
    "829933100": "SIRI",   # Sirius XM (alt CUSIP)
    "68389X105": "ORCL",   # Oracle
    "075887109": "BDX",    # Becton Dickinson
    "58933Y105": "MRK",    # Merck
    "530322106": "LMCA",   # Liberty Media
    "629377508": "NRG",    # NRG Energy (alt CUSIP)
    "531229607": "LSXMK",  # Liberty SiriusXM C (alt CUSIP)
    "110122108": "BMY",    # Bristol-Myers Squibb
    "167250109": "CBI",    # Chicago Bridge & Iron
    "911312106": "UPS",    # UPS
    "637071101": "NOV",    # National Oilwell Varco
    "256677105": "DG",     # Dollar General
    "94973V107": "WFC",    # Wells Fargo (alt CUSIP)
    "532187101": "LILA",
    "53228K108": "LILAK",
    "929566107": "WNC",
}


def fetch(url: str) -> bytes:
    req = Request(url, headers=HEADERS)
    time.sleep(0.12)
    with urlopen(req, timeout=30) as resp:
        return resp.read()


def fetch_json(url: str) -> dict:
    return json.loads(fetch(url))


def fetch_text(url: str) -> str:
    return fetch(url).decode("utf-8", errors="replace")


def get_13f_filings() -> list[tuple[str, str]]:
    """Return [(filing_date, accession)] for all 13F-HR filings."""
    results = []
    data = fetch_json(f"{BASE}/submissions/CIK{CIK.zfill(10)}.json")

    def extract(d):
        for form, date, acc in zip(d["form"], d["filingDate"], d["accessionNumber"]):
            if form == "13F-HR":
                results.append((date, acc))

    extract(data["filings"]["recent"])
    for f in data["filings"].get("files", []):
        extract(fetch_json(f"{BASE}/submissions/{f['name']}"))
    return results


def filing_date_to_quarter(date_str: str) -> str:
    y, m, _ = date_str.split("-")
    y, m = int(y), int(m)
    if m <= 3:
        return f"{y-1}-Q4"
    elif m <= 6:
        return f"{y}-Q1"
    elif m <= 9:
        return f"{y}-Q2"
    else:
        return f"{y}-Q3"


def find_info_table_url(accession: str) -> str | None:
    """Find the information table URL from the filing index page."""
    acc_no_dash = accession.replace("-", "")
    base = f"https://www.sec.gov/Archives/edgar/data/{CIK}/{acc_no_dash}"
    index_url = f"{base}/{accession}-index.htm"

    try:
        html = fetch_text(index_url)
    except Exception:
        return None

    # Collect all hrefs from the page
    all_hrefs = re.findall(r'href="(/Archives/[^"]+)"', html)

    # Strategy 1: Find raw XML info table (NOT xsl-rendered)
    # Look for .xml files that are NOT primary_doc and NOT in xsl directories
    for h in all_hrefs:
        if h.endswith(".xml") and "primary_doc" not in h and "/xsl" not in h:
            return f"https://www.sec.gov{h}"

    # Strategy 2: For older text-only filings, find the main document
    # (not the complete submission text file which has the accession in name)
    for h in all_hrefs:
        if h.endswith(".txt") and accession not in h.split("/")[-1]:
            return f"https://www.sec.gov{h}"

    # Strategy 3: Last resort - complete submission
    for h in all_hrefs:
        if h.endswith(".txt"):
            return f"https://www.sec.gov{h}"

    return None


def parse_xml_holdings(xml_text: str) -> dict[str, int]:
    """Parse 13F XML. Returns {cusip: total_value_in_thousands}."""
    holdings = {}
    xml_text = re.sub(r'xmlns="[^"]*"', '', xml_text)
    xml_text = re.sub(r'xmlns:[a-z]+="[^"]*"', '', xml_text)

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return holdings

    for info in root.iter("infoTable"):
        cusip_el = info.find("cusip")
        value_el = info.find("value")
        if cusip_el is not None and value_el is not None:
            cusip = cusip_el.text.strip()
            try:
                value = int(value_el.text.strip())
            except (ValueError, AttributeError):
                continue
            holdings[cusip] = holdings.get(cusip, 0) + value

    return holdings


def parse_text_holdings(text: str) -> dict[str, int]:
    """Parse older text-format 13F. Returns {cusip: total_value_in_thousands}.

    Handles continuation lines where the CUSIP is not repeated but additional
    sub-manager entries for the same holding appear with just value + shares.
    """
    holdings = {}
    last_cusip = None

    for line in text.split("\n"):
        # Format 1 (pre-2012): "025816 10 9  905,195  17,225,400"
        match = re.search(
            r'([A-Z0-9]{6})\s+(\d{2})\s+(\d)\s+([\d,]+)\s+([\d,]+)',
            line
        )
        if match:
            cusip = match.group(1) + match.group(2) + match.group(3)
            try:
                value = int(match.group(4).replace(",", ""))
            except ValueError:
                continue
            holdings[cusip] = holdings.get(cusip, 0) + value
            last_cusip = cusip
            continue

        # Format 2 (2012+): "025816109   112,951   1,952,142"
        match = re.search(
            r'([A-Z0-9]{9})\s+([\d,]+)\s+([\d,]+)\s',
            line
        )
        if match:
            cusip = match.group(1)
            if re.match(r'[A-Z0-9]{6}\d{2}[A-Z0-9]', cusip):
                try:
                    value = int(match.group(2).replace(",", ""))
                except ValueError:
                    continue
                holdings[cusip] = holdings.get(cusip, 0) + value
                last_cusip = cusip
                continue

        # Continuation line: no CUSIP, just value + shares (for same holding)
        # Pattern: lots of leading whitespace, then value, then shares
        if last_cusip:
            cont_match = re.match(
                r'\s{20,}([\d,]+)\s+([\d,]+)\s',
                line
            )
            if cont_match:
                try:
                    value = int(cont_match.group(1).replace(",", ""))
                except ValueError:
                    continue
                holdings[last_cusip] = holdings.get(last_cusip, 0) + value
                continue

        # Reset last_cusip on separator/header lines
        if '----' in line or '<PAGE>' in line or 'Column' in line:
            last_cusip = None

    return holdings


def main():
    out_path = Path(__file__).parent.parent / "data" / "berkshire_holdings.json"

    print("Fetching filing list from EDGAR...")
    all_filings = get_13f_filings()

    target = []
    seen = set()
    for date, acc in all_filings:
        q = filing_date_to_quarter(date)
        year = int(q.split("-")[0])
        if 2006 <= year <= 2024 and q not in seen:
            target.append((date, acc, q))
            seen.add(q)

    target.sort(key=lambda x: x[2])
    print(f"Processing {len(target)} quarters")

    all_holdings = {}
    unknown_cusips: dict[str, dict] = {}

    for i, (date, acc, quarter) in enumerate(target):
        print(f"  [{i+1}/{len(target)}] {quarter} (filed {date})")

        url = find_info_table_url(acc)
        if not url:
            print(f"    WARN: no info table URL")
            continue

        try:
            content = fetch_text(url)
        except Exception as e:
            print(f"    WARN: fetch error: {e}")
            continue

        raw = parse_xml_holdings(content)
        if not raw:
            raw = parse_text_holdings(content)

        if not raw:
            print(f"    WARN: no holdings parsed")
            continue

        total = sum(raw.values())
        ticker_vals: dict[str, int] = {}
        for cusip, value in raw.items():
            ticker = CUSIP_TO_TICKER.get(cusip)
            if ticker:
                ticker_vals[ticker] = ticker_vals.get(ticker, 0) + value
            else:
                w = value / total if total else 0
                if w >= 0.003:
                    if cusip not in unknown_cusips:
                        unknown_cusips[cusip] = {"quarters": [], "max_w": 0}
                    unknown_cusips[cusip]["quarters"].append(quarter)
                    unknown_cusips[cusip]["max_w"] = max(unknown_cusips[cusip]["max_w"], w)

        weights = {}
        for ticker, value in ticker_vals.items():
            w = round(value / total, 4) if total else 0
            if w >= 0.005:
                weights[ticker] = w

        top = dict(sorted(weights.items(), key=lambda x: -x[1])[:20])
        all_holdings[quarter] = top
        cov = sum(top.values())
        print(f"    OK: {len(top)} tickers, coverage={cov:.1%}")

    if unknown_cusips:
        print(f"\nUnmapped CUSIPs (>= 0.3% weight):")
        for cusip, info in sorted(unknown_cusips.items(), key=lambda x: -x[1]["max_w"])[:20]:
            print(f"  {cusip}: max={info['max_w']:.1%}, n={len(info['quarters'])}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(all_holdings, f, indent=2, sort_keys=True)
    print(f"\nSaved {len(all_holdings)} quarters -> {out_path}")


if __name__ == "__main__":
    main()
