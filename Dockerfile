# syntax=docker/dockerfile:1.7
FROM python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203 AS python-base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY requirements-runtime.lock ./
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    pip install --require-hashes -r requirements-runtime.lock

FROM python-base AS tooling

FROM python-base AS capture-base

ENV LIBVA_DRIVER_NAME=iHD \
    TZ=America/Los_Angeles

RUN DEBIAN_FRONTEND=noninteractive apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg intel-media-va-driver tzdata \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

FROM capture-base AS runtime-app

COPY parking_spot_monitor ./parking_spot_monitor
COPY src ./src
COPY main.py config.yaml.example ./
RUN python -m compileall -q /app/parking_spot_monitor /app/src

HEALTHCHECK --interval=30s --timeout=5s --start-period=45s --retries=3 \
    CMD python -m parking_spot_monitor.healthcheck --health-file /data/health.json --max-age-seconds 120

CMD ["python", "-m", "parking_spot_monitor", "--config", "/config/config.yaml"]

FROM capture-base AS runtime-detector

ENV YOLO_CONFIG_DIR=/data/ultralytics

COPY requirements-detector.lock ./
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    pip install --require-hashes -r requirements-detector.lock

COPY parking_spot_monitor ./parking_spot_monitor
COPY src ./src
COPY main.py config.yaml.example ./
RUN python -m compileall -q /app/parking_spot_monitor /app/src

HEALTHCHECK --interval=30s --timeout=5s --start-period=45s --retries=3 \
    CMD python -m parking_spot_monitor.healthcheck --health-file /data/health.json --max-age-seconds 120

CMD ["python", "-m", "parking_spot_monitor", "--config", "/config/config.yaml"]
