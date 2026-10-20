FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.txt README.md ./
COPY app ./app
COPY dashboard ./dashboard
RUN pip install --no-cache-dir -r requirements.txt
RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /app
USER 10001
EXPOSE 8000
CMD ["python", "-m", "app", "api"]
