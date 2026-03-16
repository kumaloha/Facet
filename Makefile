.PHONY: install anchor axion db

# ── 安装 ──────────────────────────────────────────────────
install:
	pip install -e anchor/ -e axion/

# ── 数据库 ────────────────────────────────────────────────
db:
	python -m anchor.database.session

# ── Anchor 快捷命令 ──────────────────────────────────────
anchor-run:
	python -m anchor run-url $(URL)

anchor-backfill:
	python -m anchor backfill $(TICKER) --years $(or $(YEARS),5) --fill-gaps

anchor-sources:
	python -m anchor company-sources $(TICKER)

# ── Axion 快捷命令 ──────────────────────────────────────
axion-score:
	python -m axion score $(TICKER)

axion-features:
	python -m axion features $(TICKER)
