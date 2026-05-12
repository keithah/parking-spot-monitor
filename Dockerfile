FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LIBVA_DRIVER_NAME=iHD \
    TZ=America/Los_Angeles

WORKDIR /app

RUN DEBIAN_FRONTEND=noninteractive apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg intel-media-va-driver tzdata vainfo \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY parking_spot_monitor ./parking_spot_monitor
COPY main.py config.yaml.example ./

CMD ["python", "-m", "parking_spot_monitor", "--config", "/config/config.yaml"]
