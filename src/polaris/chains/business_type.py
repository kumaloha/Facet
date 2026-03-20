"""
生意类型推断
============
"能不能看懂这个生意" — 巴菲特第一条。

不是给尺子加例外，而是让公司先声明该用哪些尺子量。
从 downstream_segments / financial_line_items / 跨期数据自动推断，
不需要手动贴标签。

四个维度:
  1. business_model: 收入靠什么驱动
  2. profit_volatility: 利润的可预测性
  3. capital_structure: 资本结构的特殊性
  4. moat_durability: 护城河能持续多久
"""

from __future__ import annotations

from dataclasses import dataclass

from polaris.features.types import ComputeContext


@dataclass
class BusinessType:
    """公司的生意画像，决定链条里该用哪些尺子。"""

    # ── 商业模式: 怎么赚钱（不是为什么能赚，壁垒在护城河模块里）──
    # product: 卖产品（硬件/消费品）— 苹果卖手机、可口可乐卖饮料、茅台卖酒
    # platform: 平台抽佣/撮合 — 腾讯、阿里、亚马逊
    # commodity: 卖大宗商品 — OXY 卖油、紫金卖金/铜
    # spread: 赚利差/浮存金 — 银行吸存放贷、保险收保费投资
    # license: 收许可费/专利费 — 高通收 5G 专利税
    # franchise: 收加盟费 — 达美乐、麦当劳
    # subscription: 卖订阅/SaaS — 甲骨文、Adobe
    # service: 卖服务 — 达维塔做透析、UNH 做保险+医疗服务
    business_model: str = "unknown"

    # ── 利润特征: 利润随什么波动 ──
    # stable: 稳定可预测（消费品、保险、公用事业）
    # cyclical: 随外部价格/周期波动（石油、矿业）
    # growth: 高增长期，利润率在快速变化（科技、电商）
    profit_volatility: str = "unknown"

    # ── 资本结构 ──
    # normal: 正常权益，D/E 有意义
    # negative_equity: 回购/收购导致权益为负或极低，D/E 无意义
    # financial: 银行/保险/支付，利息是经营成本不是偿债
    capital_structure: str = "normal"

    # ── 护城河持久性 ──
    # perpetual: 品牌、成瘾品、基础设施 — 永续
    # expiring: 专利、技术代际 — 有过期日
    # needs_reinvestment: 效率/规模优势 — 需要持续投入维护
    # none: 没有结构性护城河
    moat_durability: str = "unknown"

    # 推断依据（给人看的）
    reasoning: list[str] = None

    def __post_init__(self):
        if self.reasoning is None:
            self.reasoning = []


# ── 收入驱动型分类表 ──
_BRAND_CATEGORIES = {"liquor", "beverage", "tobacco", "alcohol", "beer", "wine", "coffee"}
_PLATFORM_CATEGORIES = {"social_media", "gaming"}  # 有 transaction_fee/ad_revenue 的另外检测
_COMMODITY_CATEGORIES = {"commodity"}
_SPREAD_CATEGORIES = {"banking", "insurance", "payment"}
_FRANCHISE_REVENUE_TYPES = {"franchise"}
_SUBSCRIPTION_REVENUE_TYPES = {"saas", "license", "subscription"}
_INFRA_CATEGORIES = {"utility", "electricity", "pipeline", "telecom", "cloud_infrastructure",
                     "operating_system"}

_PERPETUAL_CATEGORIES = _BRAND_CATEGORIES | {"healthcare", "pharma", "grocery", "food"} | _INFRA_CATEGORIES
_EXPIRING_SIGNALS = {"patent"}  # 从 competitive_dynamics 检测


def infer_business_type(ctx: ComputeContext, overrides: dict | None = None) -> BusinessType:
    """从已有数据推断生意类型。

    overrides: 可选的显式声明，覆盖自动推断。
    用于自动推断不够准确时（如苹果的品牌 vs 订阅区分）。
    只需要声明推断错误的维度，其他维度仍然自动推断。
    """
    bt = BusinessType()
    overrides = overrides or {}

    ds = ctx.get_downstream_segments()
    fli = ctx.get_financial_line_items()

    # ── 收集产品类型和收入类型 ──
    categories = set()
    revenue_types = set()
    if not ds.empty:
        if "product_category" in ds.columns:
            categories = set(ds["product_category"].dropna().str.lower().unique())
        if "revenue_type" in ds.columns:
            revenue_types = set(ds["revenue_type"].dropna().str.lower().unique())

    # ── 1. 收入驱动 ──
    # 按最大收入来源的 product_category 判断，而不是看所有类别的并集
    # 避免辅助收入（如腾讯的 payment 占 30%）抢了主类型（gaming/social 占 52%）
    primary_cat = ""
    primary_rev_type = ""
    if not ds.empty and "revenue_pct" in ds.columns and "product_category" in ds.columns:
        top = ds.sort_values("revenue_pct", ascending=False).iloc[0]
        primary_cat = str(top.get("product_category", "")).lower()
        primary_rev_type = str(top.get("revenue_type", "")).lower()

    # 产品类: 卖实物产品（硬件/消费品/食品/酒）
    _PRODUCT_CATEGORIES = _BRAND_CATEGORIES | {"consumer_electronics", "grocery", "food"}
    # 服务类: 卖服务
    _SERVICE_CATEGORIES = {"healthcare", "pharma"}

    if primary_cat in _COMMODITY_CATEGORIES:
        bt.business_model = "commodity"
        bt.reasoning.append(f"卖大宗商品: {primary_cat}")
    elif primary_cat in _SPREAD_CATEGORIES and not (categories - _SPREAD_CATEGORIES):
        bt.business_model = "spread"
        bt.reasoning.append(f"赚利差/浮存金: {categories & _SPREAD_CATEGORIES}")
    elif primary_cat in _SERVICE_CATEGORIES:
        bt.business_model = "service"
        bt.reasoning.append(f"卖服务: {primary_cat}")
    elif primary_rev_type in ("saas", "license"):
        bt.business_model = "subscription"
        bt.reasoning.append(f"卖订阅/SaaS: {primary_rev_type}")
    elif primary_rev_type in ("transaction_fee", "ad_revenue") or (
            revenue_types & {"transaction_fee", "ad_revenue"}):
        bt.business_model = "platform"
        bt.reasoning.append(f"平台抽佣: {primary_rev_type or revenue_types & {'transaction_fee', 'ad_revenue'}}")
    elif primary_cat in _PRODUCT_CATEGORIES or categories & _PRODUCT_CATEGORIES:
        bt.business_model = "product"
        bt.reasoning.append(f"卖产品: {primary_cat or categories & _PRODUCT_CATEGORIES}")
    elif primary_cat in _INFRA_CATEGORIES:
        bt.business_model = "subscription"
        bt.reasoning.append(f"卖订阅（基础设施）: {primary_cat}")
    else:
        bt.business_model = "unknown"
        bt.reasoning.append(f"商业模式未识别 (primary_cat={primary_cat}, rev_type={primary_rev_type})")

    # 专利检测: 如果有专利事件且专利许可费是主要收入之一 → license
    cd = ctx.get_competitive_dynamics()
    has_patent = False
    has_patent_expiry = False
    if not cd.empty and "event_type" in cd.columns:
        if not cd[cd["event_type"] == "patent_challenge"].empty:
            has_patent = True
        if not cd[cd["event_type"] == "patent_expiration"].empty:
            has_patent_expiry = True
    # 只有当专利许可费占显著收入时才覆盖商业模式
    has_license_revenue = False
    if not ds.empty and "revenue_type" in ds.columns:
        license_rows = ds[ds["revenue_type"].str.lower() == "license"]
        if not license_rows.empty:
            license_pct = license_rows["revenue_pct"].sum() if "revenue_pct" in license_rows.columns else 0
            has_license_revenue = license_pct >= 0.15
    if has_patent and has_license_revenue:
        bt.business_model = "license"
        bt.reasoning.append(f"收专利许可费（占收入 {license_pct:.0%}+）")

    # 加盟检测
    if not ds.empty and "contract_duration" in ds.columns:
        durations = ds["contract_duration"].dropna().tolist()
        long_contracts = [d for d in durations if isinstance(d, str) and
                         any(x in d for x in ["5", "long", "10"])]
        if long_contracts and categories & {"food", "grocery"}:
            bt.business_model = "franchise"
            bt.reasoning.append("长期加盟合同 + 食品类 → 加盟模式")

    # ── 2. 利润模式 ──

    roe_vol = ctx.features.get("l0.company.roe_stability")
    ocf_vol = ctx.features.get("l0.company.ocf_margin_stability")

    if bt.business_model == "commodity":
        bt.profit_volatility = "cyclical"
        bt.reasoning.append("大宗商品 → 周期性利润")
    elif roe_vol is not None and roe_vol > 0.08 and ocf_vol is not None and ocf_vol < 0.03:
        bt.profit_volatility = "cyclical"
        bt.reasoning.append(f"ROE 波动大({roe_vol:.2f})但现金流稳({ocf_vol:.3f}) → 周期性稳定")
    elif (roe_vol is not None and roe_vol > 0.10
          and bt.capital_structure != "negative_equity"):
        # 负权益公司 ROE 波动大是因为分母小/不稳定，不是利润周期性
        bt.profit_volatility = "growth"
        bt.reasoning.append(f"ROE 波动大({roe_vol:.2f}) → 高增长/波动型")
    elif bt.business_model in ("brand", "spread", "franchise", "subscription"):
        bt.profit_volatility = "stable"
        bt.reasoning.append(f"{bt.business_model} → 稳定型利润")
    else:
        # 检查收入增速
        rev_growth = ctx.features.get("l0.company.revenue_growth_yoy")
        if rev_growth is not None and rev_growth > 0.15:
            bt.profit_volatility = "growth"
            bt.reasoning.append(f"收入增速 {rev_growth:.0%} → 增长型")
        else:
            bt.profit_volatility = "stable"
            bt.reasoning.append("默认稳定型")

    # ── 3. 资本结构 ──

    equity = None
    total_assets = None
    if not fli.empty:
        eq_rows = fli[fli["item_key"] == "shareholders_equity"]
        ta_rows = fli[fli["item_key"] == "total_assets"]
        if not eq_rows.empty:
            equity = float(eq_rows.iloc[0]["value"])
        if not ta_rows.empty:
            total_assets = float(ta_rows.iloc[0]["value"])

    if bt.business_model == "spread":
        bt.capital_structure = "financial"
        bt.reasoning.append("金融机构 → 利息是经营成本")
    elif equity is not None and equity <= 0:
        bt.capital_structure = "negative_equity"
        bt.reasoning.append(f"权益 {equity:,.0f} ≤ 0 → 回购/收购导致负权益")
    elif (equity is not None and total_assets is not None and total_assets > 0
          and equity / total_assets < 0.05):
        bt.capital_structure = "negative_equity"
        bt.reasoning.append(f"权益/总资产 = {equity/total_assets:.1%} < 5% → 极低权益")
    else:
        bt.capital_structure = "normal"

    # ── 4. 护城河持久性 ──
    # 根据收入驱动和主要品类判断，不是看所有品类的并集
    # 永续: 品牌/成瘾品/基础设施 — 品牌不会过期
    # 有期限: 专利/技术代际 — 有过期日
    # 需持续投入: 平台/效率/大宗商品 — 护城河会被侵蚀

    if bt.business_model == "product" and categories & _BRAND_CATEGORIES:
        bt.moat_durability = "perpetual"
        bt.reasoning.append("品牌消费品 → 永续护城河")
    elif bt.business_model == "product":
        bt.moat_durability = "needs_reinvestment"
        bt.reasoning.append("产品型 → 需持续创新维护（硬件/电子）")
    elif bt.business_model == "spread" and primary_cat in ("insurance", "banking"):
        bt.moat_durability = "perpetual"
        bt.reasoning.append("金融牌照 → 永续护城河")
    elif bt.business_model == "subscription" and primary_cat in _INFRA_CATEGORIES:
        bt.moat_durability = "perpetual"
        bt.reasoning.append("数字基础设施 → 永续护城河（转换成本极高）")
    elif bt.business_model == "franchise":
        bt.moat_durability = "perpetual"
        bt.reasoning.append("加盟模式 → 永续护城河（品牌+合同）")
    elif has_patent_expiry:
        bt.moat_durability = "expiring"
        bt.reasoning.append("专利有过期日 → 有期限护城河")
    elif has_patent:
        bt.moat_durability = "expiring"
        bt.reasoning.append("专利驱动 → 有期限护城河（需持续创新）")
    elif bt.business_model == "commodity":
        bt.moat_durability = "needs_reinvestment"
        bt.reasoning.append("大宗商品 → 低成本优势需持续投入")
    elif bt.business_model == "platform":
        bt.moat_durability = "needs_reinvestment"
        bt.reasoning.append("平台型 → 护城河需持续投入维护")
    elif bt.business_model == "efficiency":
        bt.moat_durability = "none"
        bt.reasoning.append("效率型 → 无结构性护城河")
    else:
        bt.moat_durability = "unknown"

    # ── 显式覆盖 ──
    # 自动推断不完美时，数据层可以声明具体维度
    for field in ("business_model", "profit_volatility", "capital_structure", "moat_durability"):
        if field in overrides:
            old = getattr(bt, field)
            new = overrides[field]
            setattr(bt, field, new)
            bt.reasoning.append(f"[覆盖] {field}: {old} → {new}")

    return bt


def format_business_type(bt: BusinessType) -> str:
    labels = {
        "business_model": {
            "product": "卖产品", "platform": "平台抽佣", "commodity": "卖大宗商品",
            "spread": "赚利差/浮存金", "license": "收许可费", "franchise": "收加盟费",
            "subscription": "卖订阅/SaaS", "service": "卖服务",
            "efficiency": "靠效率低价卖", "unknown": "未识别",
        },
        "profit_volatility": {
            "stable": "稳定可预测", "cyclical": "随周期波动", "growth": "高增长变化中",
            "unknown": "未识别",
        },
        "capital_structure": {
            "normal": "正常", "negative_equity": "负/极低权益（回购）",
            "financial": "金融机构",
        },
        "moat_durability": {
            "perpetual": "永续", "expiring": "有期限", "needs_reinvestment": "需持续投入",
            "none": "无结构性护城河", "unknown": "未识别",
        },
    }
    lines = ["", "  生意画像"]
    lines.append("  ════════════════════════════════════════════════")
    lines.append(f"  商业模式: {labels['business_model'].get(bt.business_model, bt.business_model)}")
    lines.append(f"  利润特征: {labels['profit_volatility'].get(bt.profit_volatility, bt.profit_volatility)}")
    lines.append(f"  资本结构: {labels['capital_structure'].get(bt.capital_structure, bt.capital_structure)}")
    lines.append(f"  护城河持久性: {labels['moat_durability'].get(bt.moat_durability, bt.moat_durability)}")
    if bt.reasoning:
        lines.append(f"  推断依据:")
        for r in bt.reasoning:
            lines.append(f"    · {r}")
    lines.append("")
    return "\n".join(lines)
