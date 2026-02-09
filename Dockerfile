FROM python:3.12-slim@sha256:9e01bf1ae5db7649a236da7be1e94ffbbbdd7a93f867dd0d8d5720d9e1f89fab

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.5.21@sha256:87d729f94c87d0aade077260d07d899848f027b7ef37e734dca711c7ceffd3cf /uv /uvx /bin/

RUN apt-get update && apt-get install -y --no-install-recommends curl \
  && rm -rf /var/lib/apt/lists/*

# Install dependencies in a separate layer (faster rebuilds)
# (Requires BuildKit; Docker Desktop + modern Docker supports this by default)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project

# Copy app code
COPY . /app

# Install the project itself
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked

RUN useradd -m -u 10001 appuser
USER appuser

EXPOSE 8501
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["uv", "run", "streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]
