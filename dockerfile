FROM python:3.12-slim

WORKDIR /app

# Copier les fichiers de dépendances
COPY pyproject.toml uv.lock ./

# Installer uv et les dépendances
RUN pip install --no-cache-dir uv && \
    uv sync --frozen --no-dev

# Copier le code source
COPY config/ ./config/
COPY src/ ./src/
COPY sql/ ./sql/
COPY data/ ./data/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Utiliser le Python du virtualenv créé par uv
CMD ["/app/.venv/bin/python", "-m", "src.main"]
