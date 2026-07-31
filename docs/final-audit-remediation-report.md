# Final audit remediation report

This report is the acceptance record for the 2026-07-30 whole-project maintainability and resource audit. It lets a future operator verify the code gate, deploy the reviewed image with the existing bind mounts, compare equal observation windows, and roll back without relying on private session context.

## Acceptance status

Code, static, dependency-lock, Compose-render, image-build, deployment, health, and graceful-stop evidence is recorded below. The final image is healthy and rollback-ready. The live windows were short and not workload-matched, so this report does not claim lower production resource use, no increase in peak RSS, or final performance acceptance.

Compatibility invariants:

- Existing YAML, CLI commands and exit codes, Matrix command text, alert content, occupancy thresholds, and the single-service Compose topology are unchanged.
- Runtime state, decision memory, vehicle history, corrections, and Matrix outbox retain their schema versions. New outbox retry/derivative fields are optional schema-version-1 metadata with legacy defaults.
- Occupancy transition behavior remains conservative. No backend change, archive index, database, broker, journal, event bus, or unbounded queue was introduced.
- Verification and detector benchmarks run serially; `pytest-xdist` is not used.
- Evidence in this report contains no resolved credentials, config bodies, health/outbox/decision payloads, raw logs, Matrix responses, or image bytes.

## Verification gate

All final runtime code-gate commands were run serially from source boundary `f07f02884444a50c75ae7b6afd8b1a65aa320d83`. The transactional deployment helper and its tests continued through `ac6c228d51a0939cd66dc2ddb8217c6687fbace8`; later changes are documentation/evidence only.

| Gate | Command | Result |
| --- | --- | --- |
| Focused final structural/deployment set | Deployment, decomposition, Docker, and closeout `pytest` command | `114 passed` |
| Bytecode/static import gate | `python3 -m compileall -q parking_spot_monitor src scripts tests` | Exit `0` |
| Complete regression suite | `python3 -m pytest -q` | `1,691 passed in 69.66s` at final documentation boundary `bf3ff12` |
| Dependency locks | `python3 -I scripts/lock_dependencies.py --check` | Current; digest `8899d93ce13a7c521fe4367bf348b51d30a3f2a65242309e1e7ef7884f009e33` |
| Compose render | `docker compose config --no-interpolate` | Exit `0`; rendered evidence was not retained in the repository |
| Detector image build | `docker compose build --pull parking-spot-monitor` | Exit `0`; built `sha256:780eb5e056276c53bda2004a89a8810a92cedfee4b1ce5822f12376bb78762a8` |
| Documentation contracts | Documentation/deployment/structural `pytest` plus `git diff --check` | `114 passed`; diff check exit `0` |

## Finding disposition matrix

Each row names every commit in the task group. “Fixed” means the finding has a focused regression and is included in the serial full suite. “Benchmark-only” means the new tool cannot change production configuration. “Intentionally unchanged” names a bounded behavior whose removal would require a separately approved design.

| Group | Exact commits | Focused evidence and measured outcome | Compatibility | Disposition |
| --- | --- | --- | --- | --- |
| 1. Structural cleanup | `55bf585` | Module-decomposition and dependency lock contract/generation/validation suites pass. Test cases were split without loss; dead pass-throughs are absent. | Facade exports retained; no runtime schema change. | Fixed. |
| 2. Shared detector | `3aa13e6` | Detector adapter, detection, startup, and decomposition suites pass. Runtime and incident paths construct one retry-safe lazy backend. | Legacy path/image detector call shapes retained. | Fixed. |
| 3. Owner registry/snapshots | `bb078de`, `3358087` | Owner vehicle, owner cache, Matrix command, and startup suites pass. Reads have a 64 KiB pre-decode cap, stable last-known-good state, and required providers. | Permissive missing-file behavior retained. | Fixed. |
| 4. Shutdown/lifecycle | `d6991d1`, `068839b`, `524f9ec` | Runtime lifecycle, capture-loop, Matrix delivery, startup, Docker-contract, and cleanup-race suites pass. The live stop completed in `1.189s` without forced-kill or OOM evidence, then restarted healthy. | Immediate lifecycle compatibility method retained; same service topology. | Fixed and live stop verified. |
| 5. Reconnect/commands/logging | `d2473f3`, `ae62747`, `78cd4fe` | Runtime command, loop-health, logging, Matrix, startup, and decomposition suites pass. Reconnect math is capped; fetch is capacity-one; INFO is summarized every 900 seconds. | Alert retry policy and operator transition/error records retained. | Fixed. |
| 6. Outbox scheduling | `9112c1a`, `85218ac` | Outbox persistence and Matrix delivery suites pass for restart eligibility, ordering, jitter/cap validation, summary caching, and exact phase write counts. | Optional fields remain schema version 1 and legacy records default safely. | Fixed. Complete atomic full-file publication after each durable network phase is intentionally unchanged because restart semantics preclude removing it without a journal/database migration. |
| 7. Decision memory | `2cddf43`, `bfc8104`, `c46d821`, `05747d7`, `9f0ee7d`, `2e81575` | Decision store, operator memory, runtime/startup, config, and persistence suites pass. Immediate records and 300-second/50-record checkpoints survive reconciliation, constructor, fsync, and cadence faults. | JSON shape/version and compatibility append APIs retained. | Fixed. |
| 8. Archive reuse/cache | `e01d35b` | Vehicle-history and runtime-health-cache suites pass. Profile-summary closed-session scans fell from three to one; revision/TTL cache behavior is unchanged. | Estimate fields, wrong-match/merge semantics, streamed health fields unchanged. | Fixed duplicate scan. Streamed full reconciliation and on-demand analytics directory scans are intentionally unchanged: there is no recent-session hot-path consumer and the existing cache already provides revision/TTL reuse. |
| 9. JPEG/derivatives/recovery | `cd86d1b`, `2933706`, `095e56a`, `7dceee4`, `8ff36d7`, `e696470`, `de19962`, `36d34a6`, `ac59a0d`, `128586d`, `bd15c04`, `1aa74cb`, `ef6474a`, `80ec895`, `5f9766b`, `c63c490` | Vehicle-history, Matrix delivery, image-budget, operator-cockpit, startup, ownership-race, recovery, and decomposition suites pass. Full JPEG encode is removed; derivatives are restart-reused; indexed recovery is bounded. | Session and outbox schema versions retained; optional derivative metadata is backward-readable. | Fixed within the documented cooperative single-writer directory boundary. |
| 10. Ultralytics/backend spike | `d7484ae`, `405d349`, `0544d00`, `f4240bd`, `c6dfa53`, `b67f99c` | Benchmark (`44`), Task 10 focused (`239`), decomposition (`35`), and full (`1,638`) acceptance suites pass. State, corpus, model, output, timeout, mutation, and provenance boundaries are covered. | Production `detection.model` remains `/models/yolov8n.pt`; no fallback/config change. | Benchmark-only. A switch requires all parity gates, at least 15% p95 or peak-RSS improvement, and a separate approved deployment. |
| 11. Strict re-audit and residual remediation | `1b02fbd`, `728b141`, `d6420cb`, `f07842c`, `35951ef` | Full suite `1,665`; exact split-domain collection parity `797/797`; compile, locks, Compose, and diff checks passed at that boundary. | Durable schemas, operator bind mounts, `.pt` production backend, and single-service topology retained. | Initial residual reconciliation, cache, lookup, bounded-replay, recovery, benchmark-I/O, JPEG-I/O, and test-monolith findings fixed. |
| 12. Final adversarial follow-up | `cb22d51`, `cfccc72`, `88b78e2`, `01036ac`, `95192d1`, `9d003ce`, `bffdbcf`, `ba9e575`, `bed302f`, `af1e7af`, `600a051`, `f07f028` | Full suite `1,691`; largest test module `896` lines; decision-memory writer races, correction compaction concurrency, non-finite outbox recovery, bounded recovery blocking, and single-traversal owner-cache behavior have deterministic regressions. | Durable schema versions and public command/config contracts retained. Read-only summary audits moved to bounded structured logging. | Runtime adversarial findings fixed at source boundary `f07f028`. |
| 13. Transactional operations | `2584075`, `043c42d`, `b318a1a`, `ac6c228` | Backup, upgrade, stale-health, rollback, and restore failure-injection tests pass. The runbook shrank from 956 to 638 lines and invokes one checked-in helper. A live quiesced backup proved exact running-image selection, external bind support, root-owned data capture, cleanup, and restart. | Single Compose service and host bind layout retained. | Operational findings fixed; final recovery evidence is recorded below. |

## Design constraints checked again

- Outbox text, upload, and image phases still publish their result atomically before advancing. One complete file publication per durable phase is the accepted bounded cost.
- Immediate decision records cover transition, alert, correction/feedback, command outcome, and lifecycle decisions. Routine diagnostic time/count settings are checkpoint triggers under successful persistence, not absolute crash-loss bounds.
- Matrix command fetching is background-only. Cursor persistence, archive mutation, detector work, result application, and replies remain on the capture thread.
- The canonical JPEG is independently owned; the mutable source cannot change it. Matrix and operator callers share bounded decoding, and retryable Matrix records retain the exact upload derivative.
- Vehicle-history JSON shapes are unchanged. Profile summary reuse removes only a confirmed duplicate scan; archive health remains streamed and revision/TTL cached.
- Production uses one shared lazy `.pt` detector. ONNX and TorchScript are evidence-only.

## Baseline and earlier rollout evidence

The tracked, redaction-safe [pre-change baseline](resource-hardening-prechange-baseline.md) was captured at `2026-07-29T08:41:03-07:00` from commit `d9274c2`. It includes the commands and retained aggregate metadata. It was a point sample plus a 24-hour aggregate log count, not a controlled benchmark.

| Measurement | Pre-change baseline | Earlier resource-hardening rollout |
| --- | ---: | ---: |
| Docker CPU | `63.07%` point sample | `0.00%` in two point samples |
| Docker memory | `362.2 MiB` | `577.5 MiB`, then `624.4 MiB` |
| PIDs / process threads | `13` / `13` | `14` / `14` |
| Matrix outbox bytes | `1,565,331` | `1,614,450` |
| Vehicle-health snapshot | `59.083 ms` | `63.968 ms` |
| Docker block I/O | `14.2 GB / 5.7 GB` after about 11 hours | `319 MB / 5.68 MB` after about 7 minutes |
| Aggregate INFO | `13,117` observed over 24 hours | `33` over 192 seconds, projected `14,850/day` |

Those windows had different ages and workloads. The earlier deployment proved health and rollback readiness but did not prove a resource improvement; memory was higher in the two new point samples. The final comparison below must use equal windows and disclose transition, inference, Matrix-outage, escalation, and outbox-depth differences.

## Final deployment record

The table contains only aggregate or metadata evidence. It omits resolved configuration, model contents, payloads, raw logs, and secrets.

| Item | Final value |
| --- | --- |
| Reviewed runtime source commit | `f07f02884444a50c75ae7b6afd8b1a65aa320d83`; deployment helper source `ac6c228d51a0939cd66dc2ddb8217c6687fbace8` |
| Built image ID/digest | `sha256:780eb5e056276c53bda2004a89a8810a92cedfee4b1ce5822f12376bb78762a8`; immutable local release tag `release-final-f07f028-20260730` |
| Compose recreation | `2.06s`; existing operator bind mounts retained |
| Final container started at | `2026-07-30T23:58:41.856629336Z` after the explicit graceful-stop proof |
| Observation window | Five samples 10 seconds apart, followed by one point five seconds after the graceful restart; no equal-window or detector-heavy-phase claim is made |
| Healthcheck / restart count | Compose healthy; explicit in-container healthcheck passed; successful-frame timestamp and health mtime both newer than `StartedAt`; restart count `0` |
| Rollback image | Immediate predecessor `rollback-pre-final-ac6c228-20260730T235642Z -> sha256:2e6624ada3f196372a863c7899bb688cad5034bf17d5c7a8ef29007b72e75227` |
| Protected backup reference | `/home/keith/backups/parking-spot-monitor/final-ac6c228-20260730T235642Z`; created `2026-07-30T23:56:54.422705+00:00`; directory `0700`; all files including `.env` `0600`; complete quiesced `/data` archive `1,186,641,920 B`, SHA-256 `b08e17fef9d212daafad1e8ca8a5bbcf684ceaaeb85c9ea15b847045e40184dc`; safe archive, approved-model record, consistency manifests, and exact rollback tag/ID verified |
| Config/model continuity | Existing operator config, data, and read-only model mounts retained and validated; backend remains `.pt` |
| Graceful-stop result | Final-image proof `1.07s`, exit `0`, OOM `false`, no forced-kill evidence; final restart returned healthy with fresh-frame evidence |
| Durable lifecycle evidence | Final artifact sizes were outbox `1,187,100 B`, decision memory `94,721 B`, and health `1,869 B`; no Matrix readback was performed, while restart idempotence remains covered by tests |

## Comparable resource result

The available observations are recorded honestly below. They are not equal-duration or workload-matched. Docker network/block totals reset at recreation, and count/size differences may reflect retention or durable lifecycle work rather than steady-state efficiency.

| Measurement | Baseline/comparator | Final observed result | Workload note |
| --- | ---: | ---: | --- |
| CPU samples/distribution | Original `63.07%`; prior residual series included `0–48.38%` | Final warm series `0.00%`, `9.31%`, `0.00%`, `0.00%`, `0.00%`; post-restart `0.00%` | Samples did not establish a distribution or peak |
| RSS / peak RSS | Original `362.2 MiB`; earlier first-hardened `577.5–624.4 MiB`; prior residual series `480.6–574.9 MiB` | Final warm series `350.1`, `372.1`, `343.2`, `343.2`, `343.4 MiB`; post-restart `355.4 MiB` | Workload and process age were not matched; none is peak RSS |
| PIDs / threads | Original `13 / 13`; immediately prior `16 / 15` | Docker PIDs `16`; Python threads `15` plus one `docker-init` | Stable process/thread topology; workload was not matched |
| Block-write delta | Original `5.7 GB` after about 11 hours; prior container lifetime was not comparable | Final short series changed from `7.07 MB / 3.24 MB` to `7.07 MB / 5.07 MB`; post-restart point `0 B / 2.68 MB` | Counters changed across restart; no steady-state reduction claim |
| Capture duration/cadence | Not measured before the prior residual deployment | Prior residual image: six captures averaged `1.176850s`, maximum `1.194097s`; not repeated for the final image | No comparative improvement claim |
| Matrix-outage command duration | Prior controlled trace: `60s -> 120s -> success/reset` | No live Matrix outage induced | Retry persistence/backoff is supported by the controlled trace and serial tests, not this live window |
| INFO / WARNING / ERROR | Original `13,117 / 31 / 0` over 24h | Prior residual short window INFO `26`; final window not recounted | The short prior window included startup and cannot be extrapolated |
| Lifecycle event-name counts | Not collected for the baseline | Not recounted for the residual deployment | Lifecycle persistence and restart idempotence remain covered by the serial suite |
| Outbox writes / retry latency | Original size `1,565,331 B`; pre-residual `1,173,077 B`; live retry latency not measured | Final `1,187,100 B`; no live outage retry | Size reflects retained content and lifecycle work, not write amplification |
| Decision-memory writes | Pre-residual size `96,975 B`; write count not instrumented | Final `94,721 B`; write count not instrumented | Bounded retention/content changed; size is not a write-rate metric |
| JPEG encode attempts / strategies | Immediately prior `4,124` files and `0` derivatives; encode attempts not instrumented | Prior residual deployment had `4,126` files; final count and derivative count were not repeated | Tests prove one source decode, one streaming committed-destination hash, and derivative retry reuse |
| Health artifact bytes | Immediately prior `1,866 B` | Final `1,869 B` | Compact snapshot size only; health-scan timing was not repeated |
| Graceful shutdown latency | No comparable baseline | Final-image proof `1.07s`, exit zero, OOM false | Same container restarted healthy with zero restarts and a fresh successful frame |

Schema compatibility, redaction safety, service health, graceful shutdown, rollback readiness, and the unchanged `.pt` backend are verified. The short live window did not provide workload-matched transition coverage, peak RSS, steady-state block-write deltas, or a live Matrix outage. The correct final classification is therefore: **healthy and rollback-ready; production resource acceptance pending**. Extend the observation before claiming no missed transitions, no peak-RSS increase, or a measurable steady-state improvement.

## Upgrade, observation, and rollback decision

Before recreation, retain a protected copy of configuration, authenticated model/checksum, complete data tree including hidden recovery metadata, and the old image under an immutable rollback tag. Validate the new image against the same read-only config/model and read-write data mounts. After recreation, require Compose health, the explicit healthcheck, zero unexplained restarts, aggregate-only logs, and the graceful-stop/restart proof.

Rollback if health cannot stabilize, restart count grows, peak RSS increases under a comparable window, transition evidence regresses, durable delivery/recovery fails, or the data mount reports persistent recovery/durability errors. Restore the matched image, config, model, and checksum through the deployment runbook; keep the existing `/data` bind mount unless the protected backup must be restored as a deliberate recovery action.

## Log interpretation

Routine success is no longer represented by one INFO line per frame. Use `runtime-loop-summary` for bounded interval counts and DEBUG only for short targeted investigations. Transition, non-empty command, lifecycle, warning, and error records retain their operational levels. Treat repeated `snapshot-retention-durability-uncertain`, `snapshot-retention-failed`, decision checkpoint failures, outbox retry growth, or health staleness as action signals; retain event names, counts, durations, and exception types only.
