# The pipeline task: the package, its locked dependencies, dbt and the dbt project.
FROM python:3.13.9-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.12.3 /uv /usr/local/bin/uv

ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    DBT_PROJECT_DIR=/app/dbt \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --group pipeline --no-install-project
COPY src ./src
COPY dbt ./dbt
RUN uv sync --locked --no-dev --group pipeline \
    && useradd --system --uid 10001 pipeline \
    && chown -R pipeline /app/dbt

USER pipeline
ENTRYPOINT ["/app/.venv/bin/python", "-m", "spotgrid_lakehouse.pipeline"]
CMD ["run", "--mode", "manual"]
