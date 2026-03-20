"""
Mock 端到端测试
===============
注入模拟数据到 ComputeContext._cache，绕过 Anchor DB，
验证 特征计算 → 规则评分 → 报告输出 的完整管线。

模拟两家公司：
- GoodCorp: 类巴菲特理想标的（高毛利、轻资本、稳定增长）
- BadCorp: 高杠杆烧钱公司（低毛利、重资本、依赖融资）
"""

import pandas as pd
import pytest

from polaris.features.types import ComputeContext, FeatureResult
from polaris.features.registry import get_features
from polaris.principles.pipeline import run_pipeline, format_decision

# 触发 feature/rule 注册
import polaris.features.l0.company  # noqa: F401
import polaris.features.l0.cross_period  # noqa: F401


# ── Mock 数据工厂 ──────────────────────────────────────────────────


def _fli(items: dict[str, float], period: str = "FY2025") -> pd.DataFrame:
    """构造 financial_line_items DataFrame。"""
    rows = []
    for i, (key, val) in enumerate(items.items()):
        rows.append({
            "id": i,
            "statement_id": 1,
            "item_key": key,
            "item_label": key,
            "value": val,
            "parent_key": None,
            "ordinal": i,
            "note": None,
            "period": period,
        })
    return pd.DataFrame(rows)


def _downstream(segments: list[dict]) -> pd.DataFrame:
    defaults = {
        "id": 0, "company_id": 1, "period": "FY2025", "segment": None,
        "customer_type": None, "products": None, "channels": None,
        "revenue": None, "growth_yoy": None, "backlog": None,
        "backlog_note": None, "pricing_model": None, "contract_duration": None,
        "recognition_method": None, "description": None,
        "raw_post_id": None, "created_at": "2025-01-01",
    }
    rows = []
    for i, s in enumerate(segments):
        row = {**defaults, "id": i, **s}
        rows.append(row)
    return pd.DataFrame(rows)


def _upstream(suppliers: list[dict]) -> pd.DataFrame:
    defaults = {
        "id": 0, "company_id": 1, "period": "FY2025", "segment": None,
        "supply_type": "component", "material_or_service": None,
        "process_node": None, "purchase_obligation": None,
        "contract_type": None, "prepaid_amount": None,
        "concentration_risk": None, "description": None,
        "raw_post_id": None, "created_at": "2025-01-01",
    }
    rows = []
    for i, s in enumerate(suppliers):
        row = {**defaults, "id": i, **s}
        rows.append(row)
    return pd.DataFrame(rows)


def _geo(regions: list[dict]) -> pd.DataFrame:
    defaults = {
        "id": 0, "company_id": 1, "period": "FY2025",
        "revenue": None, "growth_yoy": None, "note": None,
        "raw_post_id": None, "created_at": "2025-01-01",
    }
    rows = []
    for i, r in enumerate(regions):
        row = {**defaults, "id": i, **r}
        rows.append(row)
    return pd.DataFrame(rows)


def _debt(obligations: list[dict]) -> pd.DataFrame:
    defaults = {
        "id": 0, "company_id": 1, "period": "FY2025",
        "instrument_name": "Note", "debt_type": "unsecured",
        "currency": "USD", "interest_rate": None,
        "maturity_date": None, "is_secured": False,
        "note": None, "raw_post_id": None, "created_at": "2025-01-01",
    }
    rows = []
    for i, d in enumerate(obligations):
        row = {**defaults, "id": i, **d}
        rows.append(row)
    return pd.DataFrame(rows)


def _exec_comp(execs: list[dict]) -> pd.DataFrame:
    defaults = {
        "id": 0, "company_id": 1, "period": "FY2025",
        "role_type": "officer", "base_salary": None, "bonus": None,
        "option_awards": None, "non_equity_incentive": None,
        "other_comp": None, "currency": "USD",
        "median_employee_comp": None, "raw_post_id": None,
        "created_at": "2025-01-01",
    }
    rows = []
    for i, e in enumerate(execs):
        row = {**defaults, "id": i, **e}
        rows.append(row)
    return pd.DataFrame(rows)


def _ownership(owners: list[dict]) -> pd.DataFrame:
    defaults = {
        "id": 0, "company_id": 1, "period": "FY2025",
        "shares_beneficially_owned": None,
        "raw_post_id": None, "created_at": "2025-01-01",
    }
    rows = []
    for i, o in enumerate(owners):
        row = {**defaults, "id": i, **o}
        rows.append(row)
    return pd.DataFrame(rows)


def _narratives(items: list[dict]) -> pd.DataFrame:
    defaults = {
        "id": 0, "company_id": 1, "raw_post_id": None,
        "capital_required": None, "capital_unit": None,
        "promised_outcome": None, "deadline": None,
        "reported_at": None, "created_at": "2025-01-01",
    }
    rows = []
    for i, n in enumerate(items):
        row = {**defaults, "id": i, **n}
        rows.append(row)
    return pd.DataFrame(rows)


def _litigations(items: list[dict]) -> pd.DataFrame:
    defaults = {
        "id": 0, "company_id": 1, "case_name": "Case",
        "case_type": "civil", "counterparty": None,
        "filed_at": None, "currency": "USD",
        "description": None, "resolution": None, "resolved_at": None,
        "raw_post_id": None, "created_at": "2025-01-01",
    }
    rows = []
    for i, l in enumerate(items):
        row = {**defaults, "id": i, **l}
        rows.append(row)
    return pd.DataFrame(rows)


def _ops_issues(issues: list[dict]) -> pd.DataFrame:
    defaults = {
        "id": 0, "company_id": 1, "period": "FY2025",
        "raw_post_id": None, "performance": None, "attribution": None,
        "created_at": "2025-01-01",
    }
    rows = []
    for i, o in enumerate(issues):
        row = {**defaults, "id": i, **o}
        rows.append(row)
    return pd.DataFrame(rows)


def _rpt(txns: list[dict]) -> pd.DataFrame:
    defaults = {
        "id": 0, "company_id": 1, "period": "FY2025",
        "currency": "USD", "terms": None, "description": None,
        "raw_post_id": None, "created_at": "2025-01-01",
    }
    rows = []
    for i, t in enumerate(txns):
        row = {**defaults, "id": i, **t}
        rows.append(row)
    return pd.DataFrame(rows)


EMPTY = pd.DataFrame()


# ── GoodCorp: 巴菲特理想标的 ──────────────────────────────────────


def build_good_corp_context() -> ComputeContext:
    """模拟一家高质量公司：高毛利、轻资本、稳定增长、低负债。"""
    # 多期财务数据（FY2022-FY2025）
    fli_items_by_period = {
        "FY2022": {
            "revenue": 28_000, "cost_of_revenue": 9_800,
            "operating_income": 12_000, "net_income": 9_500,
            "operating_cash_flow": 11_000, "capital_expenditures": 1_200,
            "depreciation_amortization": 1_000,
            "shareholders_equity": 40_000, "total_assets": 55_000,
            "interest_expense": 300, "current_assets": 25_000,
            "current_liabilities": 10_000, "goodwill": 3_000,
            "accounts_receivable": 4_000, "inventory": 2_000,
            "cash_and_equivalents": 12_000, "total_debt": 5_000,
            "dividends_paid": -2_000, "share_repurchase": -3_000,
            "sga_expense": 3_000, "rnd_expense": 4_000,
            "basic_weighted_average_shares": 1_000,
            "income_tax_expense_total": 2_500, "income_before_tax_total": 12_000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        "FY2023": {
            "revenue": 32_000, "cost_of_revenue": 11_200,
            "operating_income": 14_000, "net_income": 11_000,
            "operating_cash_flow": 13_000, "capital_expenditures": 1_400,
            "depreciation_amortization": 1_100,
            "shareholders_equity": 45_000, "total_assets": 60_000,
            "interest_expense": 280, "current_assets": 28_000,
            "current_liabilities": 11_000, "goodwill": 3_200,
            "accounts_receivable": 4_500, "inventory": 2_100,
            "cash_and_equivalents": 14_000, "total_debt": 4_500,
            "dividends_paid": -2_500, "share_repurchase": -3_500,
            "sga_expense": 3_200, "rnd_expense": 4_500,
            "basic_weighted_average_shares": 980,
            "income_tax_expense_total": 3_000, "income_before_tax_total": 14_000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        "FY2024": {
            "revenue": 37_000, "cost_of_revenue": 12_600,
            "operating_income": 17_000, "net_income": 13_500,
            "operating_cash_flow": 16_000, "capital_expenditures": 1_600,
            "depreciation_amortization": 1_200,
            "shareholders_equity": 52_000, "total_assets": 68_000,
            "interest_expense": 250, "current_assets": 32_000,
            "current_liabilities": 12_000, "goodwill": 3_300,
            "accounts_receivable": 5_000, "inventory": 2_200,
            "cash_and_equivalents": 17_000, "total_debt": 4_000,
            "dividends_paid": -3_000, "share_repurchase": -4_000,
            "sga_expense": 3_400, "rnd_expense": 5_000,
            "basic_weighted_average_shares": 960,
            "income_tax_expense_total": 3_500, "income_before_tax_total": 17_000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
        "FY2025": {
            "revenue": 43_000, "cost_of_revenue": 14_200,
            "operating_income": 20_000, "net_income": 16_000,
            "operating_cash_flow": 19_000, "capital_expenditures": 1_800,
            "depreciation_amortization": 1_400,
            "shareholders_equity": 60_000, "total_assets": 78_000,
            "interest_expense": 220, "current_assets": 38_000,
            "current_liabilities": 13_000, "goodwill": 3_500,
            "accounts_receivable": 5_500, "inventory": 2_400,
            "cash_and_equivalents": 20_000, "total_debt": 3_500,
            "dividends_paid": -3_500, "share_repurchase": -5_000,
            "sga_expense": 3_600, "rnd_expense": 5_500,
            "basic_weighted_average_shares": 940,
            "income_tax_expense_total": 4_000, "income_before_tax_total": 20_000,
            "proceeds_from_stock_issuance": 0, "proceeds_from_debt_issuance": 0,
        },
    }

    # 构造多期 fli 合并 DataFrame
    all_fli = pd.concat(
        [_fli(items, period) for period, items in fli_items_by_period.items()],
        ignore_index=True,
    )

    # 多期 debt
    all_debt = pd.concat([
        _debt([
            {"principal": 3000, "is_current": False, "interest_rate": 0.035, "period": "FY2022"},
            {"principal": 2000, "is_current": True, "interest_rate": 0.03, "period": "FY2022"},
        ]),
        _debt([
            {"principal": 2500, "is_current": False, "interest_rate": 0.035, "period": "FY2023"},
            {"principal": 2000, "is_current": True, "interest_rate": 0.03, "period": "FY2023"},
        ]),
        _debt([
            {"principal": 2000, "is_current": False, "interest_rate": 0.035, "period": "FY2024"},
            {"principal": 2000, "is_current": True, "interest_rate": 0.03, "period": "FY2024"},
        ]),
        _debt([
            {"principal": 2000, "is_current": False, "interest_rate": 0.035, "period": "FY2025"},
            {"principal": 1500, "is_current": True, "interest_rate": 0.03, "period": "FY2025"},
        ]),
    ], ignore_index=True)

    ctx = ComputeContext(company_id=1, period="FY2025")
    ctx._cache = {
        # 单期
        "financial_line_items": _fli(fli_items_by_period["FY2025"]),
        "downstream_segments": _downstream([
            {"customer_name": "Enterprise A", "revenue_pct": 0.15, "is_recurring": True, "revenue_type": "license", "backlog": 5000},
            {"customer_name": "Enterprise B", "revenue_pct": 0.12, "is_recurring": True, "revenue_type": "subscription", "backlog": 3000},
            {"customer_name": "Enterprise C", "revenue_pct": 0.08, "is_recurring": True, "revenue_type": "subscription"},
            {"customer_name": "Government D", "revenue_pct": 0.06, "is_recurring": False, "revenue_type": "project"},
            {"customer_name": "Others", "revenue_pct": 0.59, "is_recurring": True, "revenue_type": "license"},
        ]),
        "upstream_segments": _upstream([
            {"supplier_name": "TSMC", "is_sole_source": True, "geographic_location": "Taiwan", "lead_time": "6 months"},
            {"supplier_name": "Samsung", "is_sole_source": False, "geographic_location": "South Korea"},
            {"supplier_name": "Intel", "is_sole_source": False, "geographic_location": "United States"},
            {"supplier_name": "ASML", "is_sole_source": True, "geographic_location": "Netherlands"},
        ]),
        "geographic_revenues": _geo([
            {"region": "United States", "revenue_share": 0.45},
            {"region": "China", "revenue_share": 0.20},
            {"region": "Europe", "revenue_share": 0.18},
            {"region": "Japan", "revenue_share": 0.10},
            {"region": "Other", "revenue_share": 0.07},
        ]),
        "debt_obligations": _debt([
            {"principal": 2000, "is_current": False, "interest_rate": 0.035},
            {"principal": 1500, "is_current": True, "interest_rate": 0.03},
        ]),
        "executive_compensations": _exec_comp([
            {"name": "CEO", "title": "CEO", "role_type": "CEO", "pay_ratio": 120.0, "stock_awards": 8_000, "total_comp": 10_000},
            {"name": "CFO", "title": "CFO", "role_type": "officer", "pay_ratio": None, "stock_awards": 3_000, "total_comp": 4_500},
        ]),
        "stock_ownership": _ownership([
            {"name": "CEO", "title": "CEO", "percent_of_class": 8.0},
            {"name": "CFO", "title": "CFO", "percent_of_class": 2.0},
            {"name": "Vanguard", "title": None, "percent_of_class": 7.5},
            {"name": "BlackRock", "title": None, "percent_of_class": 6.0},
        ]),
        "company_narratives": _narratives([
            {"narrative": "Expand cloud", "status": "delivered"},
            {"narrative": "Enter new market", "status": "delivered"},
            {"narrative": "Reduce costs", "status": "delivered"},
            {"narrative": "Hire 1000 engineers", "status": "missed"},
            {"narrative": "Launch product X", "status": "delivered"},
        ]),
        "litigations": _litigations([
            {"status": "resolved", "accrued_amount": 50, "claimed_amount": 100},
        ]),
        "operational_issues": _ops_issues([
            {"topic": "Supply shortage", "risk": "moderate", "guidance": "Improving"},
            {"topic": "Demand growth", "risk": None, "guidance": "Strong"},
            {"topic": "Margin pressure", "risk": "low", "guidance": None},
        ]),
        "related_party_transactions": _rpt([
            {"related_party": "Board member firm", "relationship": "director",
             "transaction_type": "consulting", "amount": 50, "is_ongoing": False},
        ]),
        "non_financial_kpis": EMPTY,
        # 多期
        "financial_line_items_all": all_fli,
        "debt_obligations_all": all_debt,
        # 不存在的表（返回空）
        "pricing_actions": EMPTY,
        "market_share_data": EMPTY,
        "audit_opinions": EMPTY,
        "known_issues": EMPTY,
        "insider_transactions": EMPTY,
        "executive_changes": EMPTY,
        "equity_offerings": EMPTY,
        "analyst_estimates": EMPTY,
        "management_guidance": EMPTY,
    }
    return ctx


# ── BadCorp: 高杠杆烧钱公司 ───────────────────────────────────────


def build_bad_corp_context() -> ComputeContext:
    """模拟一家差公司：低毛利、高负债、管理层不诚信。"""
    fli = {
        "revenue": 10_000, "cost_of_revenue": 8_500,
        "operating_income": 500, "net_income": -200,
        "operating_cash_flow": 300, "capital_expenditures": 2_000,
        "depreciation_amortization": 800,
        "shareholders_equity": 5_000, "total_assets": 25_000,
        "interest_expense": 1_200, "current_assets": 6_000,
        "current_liabilities": 8_000, "goodwill": 8_000,
        "accounts_receivable": 3_000, "inventory": 4_000,
        "cash_and_equivalents": 1_500, "total_debt": 15_000,
        "dividends_paid": 0, "share_repurchase": 0,
        "sga_expense": 2_500, "rnd_expense": 500,
        "basic_weighted_average_shares": 500,
        "income_tax_expense_total": 100, "income_before_tax_total": -100,
        "proceeds_from_stock_issuance": 3_000, "proceeds_from_debt_issuance": 5_000,
    }

    ctx = ComputeContext(company_id=2, period="FY2025")
    ctx._cache = {
        "financial_line_items": _fli(fli),
        "downstream_segments": _downstream([
            {"customer_name": "MegaCorp", "revenue_pct": 0.55, "is_recurring": False, "revenue_type": "project"},
            {"customer_name": "Others", "revenue_pct": 0.45, "is_recurring": False, "revenue_type": "project"},
        ]),
        "upstream_segments": _upstream([
            {"supplier_name": "Sole A", "is_sole_source": True, "geographic_location": "China"},
            {"supplier_name": "Sole B", "is_sole_source": True, "geographic_location": "China"},
            {"supplier_name": "C", "is_sole_source": False, "geographic_location": "India"},
        ]),
        "geographic_revenues": _geo([
            {"region": "China", "revenue_share": 0.75},
            {"region": "United States", "revenue_share": 0.25},
        ]),
        "debt_obligations": _debt([
            {"principal": 8000, "is_current": True, "interest_rate": 0.08},
            {"principal": 4000, "is_current": True, "interest_rate": 0.09},
            {"principal": 3000, "is_current": False, "interest_rate": 0.07},
        ]),
        "executive_compensations": _exec_comp([
            {"name": "CEO", "title": "CEO", "role_type": "CEO", "pay_ratio": 500.0,
             "stock_awards": 1_000, "total_comp": 15_000},
        ]),
        "stock_ownership": _ownership([
            {"name": "CEO", "title": "CEO", "percent_of_class": 0.5},
            {"name": "PE Fund", "title": None, "percent_of_class": 45.0},
        ]),
        "company_narratives": _narratives([
            {"narrative": "Turn around", "status": "missed"},
            {"narrative": "Cut debt", "status": "missed"},
            {"narrative": "New product", "status": "abandoned"},
            {"narrative": "Expansion", "status": "delivered"},
        ]),
        "litigations": _litigations([
            {"status": "pending", "accrued_amount": 500, "claimed_amount": 2000},
            {"status": "ongoing", "accrued_amount": 300, "claimed_amount": 1000},
            {"status": "pending", "accrued_amount": 200, "claimed_amount": 800},
        ]),
        "operational_issues": _ops_issues([
            {"topic": f"Issue {i}", "risk": "high", "guidance": None}
            for i in range(15)
        ]),
        "related_party_transactions": _rpt([
            {"related_party": "CEO's company", "relationship": "officer",
             "transaction_type": "lease", "amount": 2_000, "is_ongoing": True},
            {"related_party": "Board LLC", "relationship": "director",
             "transaction_type": "consulting", "amount": 500, "is_ongoing": True},
        ]),
        "non_financial_kpis": EMPTY,
        "financial_line_items_all": _fli(fli),  # 只有 1 期
        "debt_obligations_all": _debt([
            {"principal": 8000, "is_current": True, "interest_rate": 0.08},
            {"principal": 4000, "is_current": True, "interest_rate": 0.09},
            {"principal": 3000, "is_current": False, "interest_rate": 0.07},
        ]),
        "pricing_actions": EMPTY,
        "market_share_data": EMPTY,
        "audit_opinions": EMPTY,
        "known_issues": EMPTY,
        "insider_transactions": EMPTY,
        "executive_changes": EMPTY,
        "equity_offerings": EMPTY,
        "analyst_estimates": EMPTY,
        "management_guidance": EMPTY,
    }
    return ctx


# ── 特征计算（绕过 pipeline，直接调 registry）──────────────────────

def compute_all_features(ctx: ComputeContext) -> dict[str, FeatureResult]:
    from polaris.features.types import FeatureLevel
    results = {}
    for level in (FeatureLevel.L0, FeatureLevel.L1, FeatureLevel.L2):
        for feat in get_features(level=level):
            try:
                result = feat.compute_fn(ctx)
            except Exception:
                continue
            if result is not None:
                ctx.features[feat.name] = result.value
                results[feat.name] = result
    return results


# ══════════════════════════════════════════════════════════════════════
# Tests
# ══════════════════════════════════════════════════════════════════════


class TestGoodCorp:
    """GoodCorp 应该在巴菲特流派得高分。"""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.ctx = build_good_corp_context()
        self.feature_results = compute_all_features(self.ctx)
        self.features = {n: r.value for n, r in self.feature_results.items()}
        self.score = run_pipeline(1, "GoodCorp", "GOOD", "FY2025", self.features)

    def test_feature_count(self):
        """应计算出大量特征（>30）。"""
        assert len(self.features) > 30, f"Only {len(self.features)} features computed"

    def test_key_financial_features(self):
        """核心财务特征都有值。"""
        expected = [
            "l0.company.gross_margin",
            "l0.company.net_margin",
            "l0.company.operating_margin",
            "l0.company.ocf_to_net_income",
            "l0.company.owner_earnings",
            "l0.company.capex_to_revenue",
            "l0.company.debt_to_equity",
            "l0.company.interest_coverage",
            "l0.company.current_ratio",
        ]
        missing = [f for f in expected if f not in self.features]
        assert not missing, f"Missing features: {missing}"

    def test_gross_margin_correct(self):
        gm = self.features["l0.company.gross_margin"]
        assert abs(gm - (43000 - 14200) / 43000) < 0.001

    def test_owner_earnings_positive(self):
        oe = self.features["l0.company.owner_earnings"]
        assert oe > 0  # 16000 + 1400 - 1800 = 15600

    def test_capex_light(self):
        capex = self.features["l0.company.capex_to_revenue"]
        assert capex < 0.05  # 1800/43000 ≈ 4.2%

    def test_buffett_high_score(self):
        """巴菲特流派得分应 >= 7。"""
        bs = self.score.buffett.school_score
        assert bs.score >= 7.0, f"Buffett score {bs.score} < 7.0"

    def test_buffett_signal(self):
        assert self.score.buffett.school_score.signal == "值得持有"

    def test_dalio_safe(self):
        """达利欧流派不应标为脆弱。"""
        ds = self.score.dalio.school_score
        assert ds.signal != "脆弱", f"Dalio signal is 脆弱 at {ds.score}"

    def test_soros_not_risky(self):
        """索罗斯流派不应标为风险。"""
        ss = self.score.soros.school_score
        assert ss.signal != "风险", f"Soros signal is 风险 at {ss.score}"

    def test_stability_features(self):
        """多期数据应产出稳定性特征。"""
        stability_feats = [k for k in self.features if "stability" in k]
        assert len(stability_feats) > 0, "No stability features computed"

    def test_report_format(self):
        """报告应包含三个流派。"""
        report = format_decision(self.score)
        assert "巴菲特" in report
        assert "达利欧" in report
        assert "索罗斯" in report
        assert "值得持有" in report


class TestBadCorp:
    """BadCorp 应该三个流派都得低分。"""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.ctx = build_bad_corp_context()
        self.feature_results = compute_all_features(self.ctx)
        self.features = {n: r.value for n, r in self.feature_results.items()}
        self.score = run_pipeline(2, "BadCorp", "BAD", "FY2025", self.features)

    def test_feature_count(self):
        assert len(self.features) > 20

    def test_low_gross_margin(self):
        gm = self.features["l0.company.gross_margin"]
        assert gm < 0.20  # (10000-8500)/10000 = 15%

    def test_high_leverage(self):
        de = self.features["l0.company.debt_to_equity"]
        assert de > 2.0  # 15000/5000 = 3.0

    def test_financing_dependency_high(self):
        fd = self.features["l0.company.financing_dependency"]
        assert fd > 1.0  # (3000+5000)/300 >> 1

    def test_buffett_low_score(self):
        bs = self.score.buffett.school_score
        assert bs.score <= 4.0, f"Buffett score {bs.score} > 4.0 for BadCorp"

    def test_buffett_filter_fails(self):
        assert not self.score.buffett.filters_passed

    def test_dalio_fragile(self):
        ds = self.score.dalio.school_score
        assert ds.score <= 5.0, f"Dalio score {ds.score} > 5.0 for BadCorp"

    def test_soros_risky(self):
        ss = self.score.soros.school_score
        assert ss.score <= 5.0, f"Soros score {ss.score} > 5.0 for BadCorp"

    def test_litigation_detected(self):
        assert self.features["l0.company.litigation_count"] == 3.0

    def test_related_party_high(self):
        rpt = self.features.get("l0.company.related_party_amount_to_revenue")
        assert rpt is not None and rpt > 0.10  # 2500/10000 = 25%


class TestScoreContrast:
    """GoodCorp vs BadCorp：好公司应在所有流派都高于差公司。"""

    @pytest.fixture(autouse=True)
    def setup(self):
        ctx_good = build_good_corp_context()
        fr_good = compute_all_features(ctx_good)
        f_good = {n: r.value for n, r in fr_good.items()}
        self.good = run_pipeline(1, "GoodCorp", "GOOD", "FY2025", f_good)

        ctx_bad = build_bad_corp_context()
        fr_bad = compute_all_features(ctx_bad)
        f_bad = {n: r.value for n, r in fr_bad.items()}
        self.bad = run_pipeline(2, "BadCorp", "BAD", "FY2025", f_bad)

    def test_buffett_contrast(self):
        assert self.good.buffett.school_score.score > self.bad.buffett.school_score.score

    def test_dalio_contrast(self):
        assert self.good.dalio.school_score.score > self.bad.dalio.school_score.score

    def test_soros_contrast(self):
        assert self.good.soros.school_score.score > self.bad.soros.school_score.score


# ── 直接运行 ──────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  GoodCorp")
    print("=" * 60)
    ctx = build_good_corp_context()
    fr = compute_all_features(ctx)
    features = {n: r.value for n, r in fr.items()}
    print(f"\n  Features computed: {len(features)}")
    for name, val in sorted(features.items()):
        print(f"    {name}: {val:.4f}")

    result = run_pipeline(1, "GoodCorp", "GOOD", "FY2025", features)
    print(format_decision(result))

    print("\n" + "=" * 60)
    print("  BadCorp")
    print("=" * 60)
    ctx = build_bad_corp_context()
    fr = compute_all_features(ctx)
    features = {n: r.value for n, r in fr.items()}
    print(f"\n  Features computed: {len(features)}")
    for name, val in sorted(features.items()):
        print(f"    {name}: {val:.4f}")

    result = run_pipeline(2, "BadCorp", "BAD", "FY2025", features)
    print(format_decision(result))
