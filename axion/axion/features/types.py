"""
特征类型定义
"""

from dataclasses import dataclass, field
from enum import Enum

import pandas as pd

from axion.db import anchor


class FeatureLevel(str, Enum):
    L0 = "l0"
    L1 = "l1"
    L2 = "l2"


@dataclass
class ComputeContext:
    """特征计算上下文——传递给每个特征函数。"""

    company_id: int
    period: str
    features: dict[str, float] = field(default_factory=dict)

    # 缓存的 Anchor 数据（按需加载）
    _cache: dict[str, pd.DataFrame] = field(default_factory=dict, repr=False)

    # ── 单期查询 ────────────────────────────────────────────────

    def get_financial_line_items(self) -> pd.DataFrame:
        key = "financial_line_items"
        if key not in self._cache:
            self._cache[key] = anchor.get_financial_line_items(
                self.company_id, self.period
            )
        return self._cache[key]

    def get_downstream_segments(self) -> pd.DataFrame:
        key = "downstream_segments"
        if key not in self._cache:
            self._cache[key] = anchor.get_downstream_segments(
                self.company_id, self.period
            )
        return self._cache[key]

    def get_upstream_segments(self) -> pd.DataFrame:
        key = "upstream_segments"
        if key not in self._cache:
            self._cache[key] = anchor.get_upstream_segments(
                self.company_id, self.period
            )
        return self._cache[key]

    def get_geographic_revenues(self) -> pd.DataFrame:
        key = "geographic_revenues"
        if key not in self._cache:
            self._cache[key] = anchor.get_geographic_revenues(
                self.company_id, self.period
            )
        return self._cache[key]

    def get_operational_issues(self) -> pd.DataFrame:
        key = "operational_issues"
        if key not in self._cache:
            self._cache[key] = anchor.get_operational_issues(
                self.company_id, self.period
            )
        return self._cache[key]

    def get_debt_obligations(self) -> pd.DataFrame:
        key = "debt_obligations"
        if key not in self._cache:
            self._cache[key] = anchor.get_debt_obligations(
                self.company_id, self.period
            )
        return self._cache[key]

    def get_executive_compensations(self) -> pd.DataFrame:
        key = "executive_compensations"
        if key not in self._cache:
            self._cache[key] = anchor.get_executive_compensations(
                self.company_id, self.period
            )
        return self._cache[key]

    def get_stock_ownership(self) -> pd.DataFrame:
        key = "stock_ownership"
        if key not in self._cache:
            self._cache[key] = anchor.get_stock_ownership(
                self.company_id, self.period
            )
        return self._cache[key]

    def get_company_narratives(self) -> pd.DataFrame:
        key = "company_narratives"
        if key not in self._cache:
            self._cache[key] = anchor.get_company_narratives(self.company_id)
        return self._cache[key]

    def get_litigations(self) -> pd.DataFrame:
        key = "litigations"
        if key not in self._cache:
            self._cache[key] = anchor.get_litigations(self.company_id)
        return self._cache[key]

    def get_related_party_transactions(self) -> pd.DataFrame:
        key = "related_party_transactions"
        if key not in self._cache:
            self._cache[key] = anchor.get_related_party_transactions(
                self.company_id, self.period
            )
        return self._cache[key]

    def get_non_financial_kpis(self) -> pd.DataFrame:
        key = "non_financial_kpis"
        if key not in self._cache:
            self._cache[key] = anchor.get_non_financial_kpis(
                self.company_id, self.period
            )
        return self._cache[key]

    # ── 多期查询（跨期特征用）────────────────────────────────

    def get_financial_line_items_history(
        self, n_periods: int | None = None
    ) -> pd.DataFrame:
        """获取当期及之前 N 期的财务科目。

        返回含 period 列的 DataFrame。
        n_periods=None 返回当期及之前的所有可用期。
        n_periods=N 返回最近 N 期（含当期）。
        """
        key = "financial_line_items_all"
        if key not in self._cache:
            self._cache[key] = anchor.get_financial_line_items_all(self.company_id)

        df = self._cache[key]
        if df.empty:
            return df

        all_periods = sorted(df["period"].unique())
        periods = [p for p in all_periods if p <= self.period]
        if n_periods is not None:
            periods = periods[-n_periods:]

        return df[df["period"].isin(periods)]

    def get_debt_obligations_history(
        self, n_periods: int | None = None
    ) -> pd.DataFrame:
        key = "debt_obligations_all"
        if key not in self._cache:
            self._cache[key] = anchor.get_debt_obligations_all(self.company_id)

        df = self._cache[key]
        if df.empty:
            return df

        all_periods = sorted(df["period"].unique())
        periods = [p for p in all_periods if p <= self.period]
        if n_periods is not None:
            periods = periods[-n_periods:]

        return df[df["period"].isin(periods)]

    # ── 新增表查询（表不存在时返回空 DataFrame）─────────────

    def get_known_issues(self) -> pd.DataFrame:
        key = "known_issues"
        if key not in self._cache:
            self._cache[key] = anchor.get_known_issues(
                self.company_id, self.period
            )
        return self._cache[key]

    def get_management_acknowledgments(self) -> pd.DataFrame:
        key = "management_acknowledgments"
        if key not in self._cache:
            self._cache[key] = anchor.get_management_acknowledgments(
                self.company_id, self.period
            )
        return self._cache[key]

    def get_insider_transactions(self) -> pd.DataFrame:
        key = "insider_transactions"
        if key not in self._cache:
            self._cache[key] = anchor.get_insider_transactions(self.company_id)
        return self._cache[key]

    def get_executive_changes(self) -> pd.DataFrame:
        key = "executive_changes"
        if key not in self._cache:
            self._cache[key] = anchor.get_executive_changes(self.company_id)
        return self._cache[key]

    def get_audit_opinions(self) -> pd.DataFrame:
        key = "audit_opinions"
        if key not in self._cache:
            self._cache[key] = anchor.get_audit_opinions(
                self.company_id, self.period
            )
        return self._cache[key]

    def get_analyst_estimates(self) -> pd.DataFrame:
        key = "analyst_estimates"
        if key not in self._cache:
            self._cache[key] = anchor.get_analyst_estimates(self.company_id)
        return self._cache[key]

    def get_equity_offerings(self) -> pd.DataFrame:
        key = "equity_offerings"
        if key not in self._cache:
            self._cache[key] = anchor.get_equity_offerings(self.company_id)
        return self._cache[key]

    def get_management_guidance(self) -> pd.DataFrame:
        key = "management_guidance"
        if key not in self._cache:
            self._cache[key] = anchor.get_management_guidance(self.company_id)
        return self._cache[key]

    def get_pricing_actions(self) -> pd.DataFrame:
        key = "pricing_actions"
        if key not in self._cache:
            self._cache[key] = anchor.get_pricing_actions(self.company_id)
        return self._cache[key]

    def get_market_share_data(self) -> pd.DataFrame:
        key = "market_share_data"
        if key not in self._cache:
            self._cache[key] = anchor.get_market_share_data(self.company_id)
        return self._cache[key]


@dataclass
class FeatureResult:
    """单个特征的计算结果。"""

    value: float
    detail: str | None = None
