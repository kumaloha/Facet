FROM python:3.12-slim

WORKDIR /app

COPY anchor/pyproject.toml anchor/pyproject.toml
COPY axion/pyproject.toml axion/pyproject.toml

RUN pip install --no-cache-dir -e anchor/ -e axion/

COPY anchor/ anchor/
COPY axion/ axion/
COPY polaris/ polaris/

RUN mkdir -p data

CMD ["python", "-m", "anchor", "--help"]
