FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml pyproject.toml

RUN pip install --no-cache-dir -e .

COPY src/ src/
COPY scripts/ scripts/
COPY sources.yaml sources.yaml

RUN mkdir -p data

ENV PYTHONPATH=/app/src

CMD ["python", "-m", "anchor", "--help"]
