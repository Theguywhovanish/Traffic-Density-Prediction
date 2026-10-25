FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY pyproject.toml requirements-stack.txt /app/
COPY src /app/src
RUN pip install --no-cache-dir -e . -r requirements-stack.txt

COPY stack/dashboard.html /app/stack/dashboard.html
COPY artifacts/model.joblib /app/artifacts/model.joblib

# Raw PEMS04 data are bind-mounted at runtime, not duplicated into the image.
CMD ["python", "-m", "traffic_density.deploy", "producer"]
