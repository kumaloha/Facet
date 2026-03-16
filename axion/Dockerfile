FROM python:3.12-slim

WORKDIR /app

# 系统依赖（psycopg2 编译需要）
RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# 安装 Python 依赖
COPY pyproject.toml .
RUN pip install --no-cache-dir . 2>/dev/null || true

# 复制源码并安装
COPY . .
RUN pip install --no-cache-dir ".[pg]"

# 数据目录
RUN mkdir -p /app/data

ENTRYPOINT ["axion"]
