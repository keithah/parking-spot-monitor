# Resource-hardening pre-change baseline

This redaction-safe baseline is retained in Git so the final audit's provenance does not depend on an ignored deployment artifact.

- Captured at: `2026-07-29T08:41:03-07:00`
- Implementation baseline commit: `d9274c23e63d0e7c708411d5aaaf413c286dcf80`
- Scope: one deployment resource sample plus aggregate structured-log levels from the preceding 24 hours
- Safety: no environment values, raw log lines, health or outbox payloads, camera frames, or snapshots were retained

## Healthy service metadata

`docker compose ps` reported one `parking-spot-monitor:local` service container, up for approximately 11 hours and healthy.

## Point resource sample

Command:

```sh
docker stats --no-stream "$(docker compose ps -q parking-spot-monitor)"
```

Redacted result:

```text
CPU=63.07%
memory=362.2 MiB / 4 GiB
network_io=307 MB / 11.8 MB
block_io=14.2 GB / 5.7 GB
pids=13
```

This is one instantaneous sample, not a trend or steady-state estimate.

## Process threads

Command:

```sh
docker exec "$(docker compose ps -q parking-spot-monitor)" \
  sh -c "awk '/^Threads:/ {print \$2}' /proc/1/status"
```

Result: `13`.

## Durable artifact metadata

`stat -c '%s bytes' data/matrix-outbox.json` reported `1,565,331 bytes`. Only file metadata was read; the payload was not opened or copied.

One deployed call to `VehicleHistoryArchive(Path("/data/vehicle-history")).health_snapshot()` was timed with `time.perf_counter()` and took `59.083 ms`. The health payload was neither printed nor retained.

## Aggregate structured-log levels

The source was `docker compose logs --since 24h --no-log-prefix parking-spot-monitor`, parsed in memory with only aggregate counts retained:

```text
window=24h
INFO=13117
WARNING=31
ERROR=0
parsed_structured_lines=13148
unparsed_lines=8
```

The eight unparsed lines were not inspected or captured. These are observed counts for the available Docker log window, not an estimate.
