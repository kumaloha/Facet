FROM python:3.12-slim

WORKDIR /app

COPY anchor/pyproject.toml anchor/pyproject.toml
COPY polaris/pyproject.toml polaris/pyproject.toml

RUN pip install --no-cache-dir -e anchor/ -e polaris/

COPY anchor/ anchor/
COPY polaris/ polaris/
COPY axion/ axion/

RUN mkdir -p data

CMD ["python", "-m", "anchor", "--help"]
