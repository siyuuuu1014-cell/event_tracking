FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN python -m pip install --no-cache-dir . && useradd --system --create-home analytics && mkdir /app/logs && chown analytics /app/logs
USER analytics
ENV ANALYTICS_LOG_DIR=/app/logs
CMD ["python", "-m", "uvicorn", "ipaklink_analytics.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]

