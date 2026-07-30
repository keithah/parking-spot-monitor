# Task 9 report — canonical JPEG artifacts and durable upload derivatives

## Result

Vehicle-history sessions now publish one validated canonical full JPEG without re-encoding, then decode that canonical artifact once to create only the crop. Matrix and operator resizing share one bounded JPEG decode lifecycle. Matrix outbox records persist the exact selected upload derivative and immutable info before network I/O, so retries and restarts reuse the same bytes. Owned deletion transitions are durably indexed before rename, recovered at startup and the next applicable operation, and report namespace deletion separately from uncertain parent-directory durability.

## TDD evidence

- Canonical-publication RED began with three `NotImplementedError` failures; after final review, GREEN proves independent reflink preference, bounded-copy fallback, unchanged bytes/mode, no source-inode chmod/fchmod, and temporary cleanup.
- Vehicle integration preserves source bytes without re-encoding the full frame while retaining an independently owned canonical inode plus clamped crop dimensions and pixels.
- Shared-decoder RED proved Matrix/operator paths ignored the common helper; GREEN preserves Matrix `snapshot_resize_failed`, operator `resize failed`, prior error-type logging, draft-before-load ordering, RGB conversion, and deterministic opened/converted image closure.
- Durable-derivative RED failed on absent `upload_derivative_path` metadata; GREEN persists path plus immutable mimetype/size/width/height, reuses exact bytes after restart, regenerates legacy metadata before upload, rejects external/symlink-capable paths, protects retryable artifacts, and prunes the terminal raw/derivative pair.
- A source-replacement race RED published a post-validation replacement; GREEN binds descriptor validation and temporary bytes with stable evidence and SHA-256 before atomic publication.
- Full-suite RED exposed three canonical root-glob collisions from a root-level `.upload.jpg` suffix. Derivatives now live in `snapshots/.upload-derivatives/<canonical-name>.jpg`; a regression proves production `occupancy-*.jpg` enumeration sees only canonical snapshots.

## Implementation

- Added `jpeg_artifacts.py` with `JpegPublication`, durable FICLONE/1 MiB descriptor-copy publication, file and directory fsync, post-replace failure semantics, safe temporary cleanup, and source-stability checks.
- Added `JpegDecodeError`, `DecodedRgbJpeg`, and `open_decoded_rgb_jpeg()` with bounded Pillow draft, positive dimensions, JPEG validation, RGB conversion, and deterministic closure.
- Vehicle-history full-frame paths are canonical publications; only crops are encoded. Session schema and image-reference fields are unchanged.
- Added immutable `MatrixUploadDerivative`; derivatives are atomically written mode 0600, validated through deterministic local paths and bounded metadata, and read from an `O_NOFOLLOW` regular-file descriptor with exact size checks.
- New outbox records persist derivative metadata before returning from enqueue; legacy records regenerate and durably update optional schema-v1 metadata before upload. Duplicate event enqueue returns the existing record without changing selected bytes.
- Retryable retention protects both raw and derivative paths. When a terminal raw snapshot is later pruned, its derivative and empty private derivative directory are removed with it.
- Matrix/operator byte budgets, upload/send result schema, public session/outbox schema versions, and caller-specific error mappings remain unchanged.

## Verification

- Required focused suite: 162 passed in 5.80 seconds.
- Matrix/operator/outbox/decomposition compatibility suite: 295 passed.
- Fresh full serial suite: 1,494 passed in 27.58 seconds.
- `python3 -m compileall -q parking_spot_monitor src scripts tests`, `git diff --check`, repository artifact checks, and 31 structural/module-cap tests passed.
- Caps: `jpeg_artifacts.py` 325/350, `matrix_snapshots.py` 422/430, `operator_cockpit_snapshots.py` 430/460, `vehicle_history_images.py` 127 lines, and `outbox.py` 450/450.

## Scope notes

No session or outbox schema-version change was introduced. `vehicle_history_sessions.py` and `vehicle_history_maintenance_utils.py` required no behavior edit: existing session references already point at the independently owned canonical path.

## Fix Round 1

All six final-review findings were remediated with new failing regressions before production changes:

- Duplicate snapshot enqueue publication is serialized by short-lived per-root/event locks; unrelated event keys remain independent. The canonical record identity is established once, and the selected derivative is attached atomically without changing the record or transaction identity.
- Persisted derivatives now require a SHA-256 digest and are revalidated as actual JPEG bytes with exact size and dimensions. Stable descriptor signatures detect mutation during reads.
- All configured-root artifact operations are owned by a rooted descriptor-relative storage module. Every parent and final read is opened without following symlinks; persisted raw and derivative paths must exactly match deterministic configured-root paths.
- The generic outbox metadata mutator was removed. A typed attach-if-absent operation validates transport evidence, rejects terminal/conflicting mutation, persists atomically, and returns detached state.
- Retention deletes the derivative before its canonical raw file. A transient derivative deletion failure leaves the raw retryable, while final symlinks are safely unlinked without following them.
- Matrix derivative types and validation moved out of generic JPEG helpers. Snapshot naming, rooted storage, Matrix derivative validation, and outbox snapshot lifecycle now have explicit dependency boundaries and strict module caps.

Final evidence: focused 157 passed; fresh full serial suite 1,505 passed in 28.44 seconds; compile and `git diff --check` clean. Caps are delivery 619/650, outbox snapshot lifecycle 237/300, rooted storage 290/320, derivative validation 191/400, naming 43/80, snapshots 412/430, outbox 447/450, and typed outbox attachment 56/80.

## Fix Round 2

- Canonical JPEG validation and every byte-producing fallback now share one source descriptor. SHA-256 evidence and stable descriptor signatures bracket validation and publication; reflink and bounded copy never reopen the source pathname.
- Every independent publication strategy hashes the temporary artifact against validated evidence before fsync/replace, preserving source mode, durability, and cleanup semantics.
- Deterministic regressions cover pathname replace-away/PNG-copy/original-restore, mutation during descriptor validation, and mutation during both reflink and copy publication. Publication either uses the validated bytes or rejects without a destination/temp artifact; unvalidated replacement bytes are never published.
- Pillow decompression-bomb errors and warnings from retained snapshot metadata inspection now map to `snapshot_metadata_failed`, and the copied retained snapshot is deleted before the Matrix error returns.

Final Fix Round 2 evidence: exact security/error-contract 7 passed, combined Task 9 375 passed, fresh full serial 1,511 passed in 29.65 seconds, compileall and diff checks clean. Caps remain clean at JPEG artifacts 310/350 and Matrix snapshots 417/430.

## Fix Round 3

- Generic hardlink publication was removed because capture owns only atomic pathname replacement, not an enforceable shared source lock. The safe default now tries copy-on-write reflink and falls back to a bounded descriptor copy; both preserve the no-reencode optimization while returning an inode independent from later source writes.
- Source evidence now includes `ctime_ns` alongside device, inode, size, and `mtime_ns`, plus SHA-256. Same-length mutation/restore with spoofed mtime is rejected, and the source plus temporary digest/signature are checked again after file fsync before replacement.
- Tests prove no successful default publication can later change through a writable source path, no hardlink is attempted, source mode is preserved, pathname replacements cannot supply publication bytes, and post-fsync temporary corruption is cleaned without a destination.
- Operator latest/Who validation promotes Pillow decompression-bomb warnings to errors, catches both warning and error classes at the established boundary, returns the existing invalid-JPEG result, and logs only redacted exception types.
- The tracked implementation plan now documents the corrected `Literal["reflink", "copy"]` strategy and immutable publication contract.

Final Fix Round 3 evidence: exact rereview 7 passed, combined Task 9 450 passed, fresh full serial 1,519 passed in 29.01 seconds, compileall/diff/artifact checks clean. Caps remain clean at JPEG artifacts 291/350 and operator snapshot handling 439/460.

## Fix Round 4

- Final Matrix upload selection now captures bounded retained JPEG evidence through one configured-root descriptor walk. Every parent and the final file use `O_NOFOLLOW`; the final descriptor is opened once, must be a regular file no larger than 32 MiB, and supplies the exact immutable bytes, dimensions, size, mimetype, and SHA-256 used by upload selection.
- Stable device/inode/size/mtime/ctime evidence and a second descriptor digest bracket exact reading and JPEG/decompression-bomb validation. Same-inode mutation is rejected, while a final-path swap to an external symlink cannot disclose the external payload. Resizing decodes only the captured bytes and under-budget metadata is derived from those same bytes rather than stale `MatrixSnapshot.info`.
- Snapshot enqueue and upload preparation now share the same per-root/event publication lock. Upload preparation refreshes the persisted record by ID after acquiring the lock, so a drain cannot mistake a visible base record for a legacy record before initial derivative attachment. Unrelated event keys remain concurrent, and concurrent legacy preparation performs one derivative generation and one immutable attachment.
- The archive documentation now describes independent reflink/copy ownership without implying hardlink behavior. Documentation contract fixtures generate real JPEG evidence and therefore exercise the same strict upload boundary as production.

Final Fix Round 4 evidence: exact security/concurrency 6 passed, Matrix/outbox 200 passed, combined Task 9 compatibility 640 passed in 19.04 seconds, structural 34 passed, and fresh full serial 1,525 passed in 30.50 seconds. Compileall, diff, and repository artifact checks are clean. Caps remain clean at JPEG artifacts 327/350, rooted snapshot storage 318/320, Matrix snapshots 430/430, and outbox snapshot lifecycle 247/300.

## Fix Round 5

- Canonical publication now retains the validated temporary descriptor across `os.replace`. The committed destination is opened with `O_NOFOLLOW`, must be a regular JPEG whose stable signature and SHA-256 match the original source evidence, and must have the same device/inode identity as the held validated temporary descriptor.
- The destination pathname is rebound to the held committed descriptor both before and after parent-directory fsync. A direct symlink destination parent is rejected before temporary or destination creation. Final symlink swaps are inspected through an `O_PATH|O_NOFOLLOW` identity descriptor and removed without following their target.
- Verification mismatch cleanup compares the current pathname identity to the held committed identity before unlinking. A concurrent replacement with an unrelated file is therefore preserved, while an unvalidated regular file or symlink moved from the private temporary name is removed and publication fails.
- Matrix derivative publication and restart validation now reuse the canonical `jpeg_bytes_dimensions()` validator, eliminating the duplicate Pillow/error-mapping implementation. `RootedJpegEvidence` retains only the exact immutable upload bytes and media info; its redundant, unused SHA-256 field was removed while descriptor stability still uses an internal digest.

Final Fix Round 5 evidence: exact committed-evidence/cleanup/consolidation 6 passed, canonical publication 16 passed, focused Matrix/outbox/vehicle/structural 324 passed, combined Task 9 compatibility 645 passed in 19.32 seconds, structural 34 passed, and fresh full serial 1,530 passed in 29.39 seconds. Dependency lock validation, compileall, diff, and repository artifact checks are clean. Caps remain clean at JPEG artifacts 349/350, descriptor binding 45/80, rooted snapshot storage 316/320, derivative validation 167/400, and Matrix snapshots 430/430.

## Fix Round 6

- Canonical publication now owns its destination through one retained `RootedDirectoryOwner`. Absolute paths are walked from `/`, relative paths from the captured working-directory root, every component is opened with `O_DIRECTORY|O_NOFOLLOW`, `..` is rejected, and missing components are created and fsynced descriptor-relatively.
- Temporary creation, reflink/copy, validation, atomic replacement, committed identity checks, cleanup, and directory fsync all use the retained parent descriptor plus safe basenames. A final no-symlink rewalk proves that the displayed path still names the held parent; parent replacement fails publication and cleans only the original held directory.
- `JpegPublication` and retained Matrix publication now carry typed `(dev, ino)` ownership. Vehicle crop failure and Matrix metadata failure delete only the exact published inode, preserving an adversarial pathname replacement.
- Regressions cover direct and intermediate symlink ancestors, parent-directory replacement, absolute and working-directory-relative nested creation, preexisting destination replacement, committed identity reporting, and replacement-safe vehicle/Matrix failure cleanup.

Final Fix Round 6 evidence: exact security/ownership/decomposition 42 passed, vehicle/Matrix/decomposition 271 passed, and fresh full serial 1,536 passed in 41.88 seconds. Dependency locks are current; compileall and `git diff --check` are clean. Caps remain clean at descriptor ownership 132/180, JPEG artifacts 346/350, rooted Matrix snapshot storage 320/320, Matrix snapshots 430/430, and vehicle image handling 131 lines.

## Fix Round 7

- Vehicle-history crop decoding now consumes the canonical publication through an `O_NOFOLLOW` descriptor whose `(dev, ino)` matches `JpegPublication.identity`. The final basename and complete no-symlink parent walk are verified before and after consumption, so a valid attacker JPEG cannot be cropped through a replaced path or root.
- Matrix retained evidence accepts the exact publication identity and validates it on the single bounded read descriptor. Rooted Matrix directory contexts verify their held root against a fresh no-symlink walk before and after work, rejecting root rename/replacement while final-file swaps fail descriptor stability or identity checks.
- Identity cleanup is now rename-first: the target is atomically moved to a random same-directory quarantine through the retained dirfd, inspected there, and deleted only on an exact identity match. A mismatch is restored with no-clobber hardlink-plus-unlink semantics. Generic, vehicle, and Matrix delete-boundary races preserve unrelated replacements without quarantine residue.
- Retained Matrix copy publication moved into cohesive `matrix_retained_publication.py`. It opens the source once with `O_NOFOLLOW`, rejects non-regular, empty, or over-32-MiB input before destination creation, copies no more than the preflight size, detects short/growing input, and compares source and temporary SHA-256 plus stable signatures after fsync.
- The tracked Task 9 plan now records `JpegPublication.identity` and explains its descriptor-binding and cleanup-ownership contract.

Final Fix Round 7 evidence: exact new regressions 9 passed, vehicle/Matrix/decomposition 280 passed, and fresh full serial 1,545 passed in 45.74 seconds. Dependency locks are current; compileall and `git diff --check` are clean; `/proc/self/fd` stayed at 4 across 100 publication/read/delete cycles. Caps remain clean at descriptor ownership 178/180, JPEG artifacts 348/350, retained Matrix publication 127/180, rooted Matrix storage 277/320, Matrix snapshots 430/430, and vehicle image handling 133 lines.

## Fix Round 8

- Canonical publication now enforces the shared 32 MiB JPEG ceiling before hashing or destination-directory creation. Copy fallback writes exactly the preflight size, rejects short reads, probes one byte for growth without writing it, and revalidates source/temp evidence; reflink artifacts must also retain the exact validated size.
- Vehicle full-frame and crop capture is one rooted transaction. Archive, images, full-frame, and crop owners remain open throughout; the full JPEG publishes through the held full owner, decode consumes its exact identity descriptor, and the crop stages, fsyncs, replaces, and returns a typed identity through the held crop owner. Every owner and final identity is checked before return; root/final swaps clean only owned artifacts through held dirfds.
- Cleanup now performs a strict regular-file and expected-identity precheck, so stable directories, symlinks, and unrelated files never move. A race after precheck is contained by random same-directory quarantine and no-clobber hardlink/unlink restore.
- If a post-rename blocker prevents immediate restore, both unrelated files remain intact. The next cleanup attempt runs an exact-name recovery scan capped at 256 entries before checking ownership; `recover_quarantined_path()` exposes the same bounded recovery explicitly. Tests prove blocker removal plus a normal cleanup clears the quarantine without deleting the recovered mismatch.
- JPEG decode lifecycle and identity cleanup were separated into cohesive capped modules; public imports remain compatible. The tracked Task 9 plan documents the byte ceiling, rooted vehicle transaction, and deferred recovery behavior.

Final Fix Round 8 evidence: exact new regressions 6 passed, vehicle/Matrix/image-budget/decomposition 314 passed, and fresh full serial 1,551 passed in 34.93 seconds. Dependency locks are current; compileall, structural, diff, and artifact checks are clean; `/proc/self/fd` stayed at 4 across 100 rooted publication/read/crop/delete cycles. Caps remain clean at descriptor ownership 168/180, identity cleanup 98/120, JPEG publication 272/350, JPEG decoding 133/180, vehicle transaction 179/220, retained Matrix publication 128/180, rooted Matrix storage 277/320, and Matrix snapshots 430/430.

## Fix Round 9

- Canonical publication, retained publication, and rooted JPEG evidence now share one exact-size descriptor primitive. It reads only the captured size in 1 MiB chunks, rejects short input, probes exactly one byte for growth, always compares the post-read descriptor signature, resets the offset, and optionally hashes, captures, or copies the same bounded bytes.
- A valid JPEG padded to the exact 32 MiB ceiling still publishes. A source expanded to 40 MiB during validation and a continuously growing rooted source both reject after consuming no more than their captured size plus the single probe byte; retained-copy destinations never receive growth bytes.
- Deferred cleanup recovery now recognizes only `.<escaped safe basename>.<16 lowercase hex>.quarantine`, sorts exact candidates deterministically within the 256-entry scan cap, and leaves uppercase, wrong-length, nonhex, and manually suffixed variations untouched.
- Recovery is idempotent after an interrupted hardlink/unlink restore: when target and quarantine already identify the same no-follow regular inode, it removes only the quarantine and fsyncs. A transient unlink failure remains retryable and the next recovery completes it.
- The tracked Task 9 file map names the actual bounded-I/O, decode, cleanup, retained-publication, rooted-storage, and derivative modules. Its interface contract now lists the implemented derivative APIs and removes the nonexistent `prepare_upload_derivative` declaration.

Final Fix Round 9 evidence: exact growth/recovery/mutation regressions 14 passed, vehicle/Matrix/decomposition 297 passed, structural 34 passed, and final post-edit full serial 1,562 passed in 34.34 seconds. Compileall and `git diff --check` are clean. Caps remain clean at exact descriptor I/O 74/120, cleanup recovery 131/150, JPEG publication 250/350, retained publication 97/180, and rooted storage 268/320.

## Fix Round 10

- Rooted Matrix artifact validators now reject both `.` and `..` for directories and filenames. Publish/delete traversal regressions prove a parent-owned artifact remains untouched.
- Cleanup never unlinks the predictable checked quarantine pathname. A cohesive disposal owner atomically moves it to a fresh cryptographically random name, binds a no-follow identity descriptor, verifies a regular file and the expected inode, rechecks the randomized pathname, and restores raced mismatches through no-clobber hardlink semantics.
- The disposal module and deployment guide state the honest threat boundary: Linux has no inode-conditional unlink, so the minimized final randomized-name stat/unlink window requires application-owned directories without noncooperating writers and is not claimed safe for hostile shared directories.
- Canonical source and artifact JPEG validation captures exactly the preflight bytes through the shared bounded helper and passes only `BytesIO` to Pillow. The same capture supplies both payload and SHA-256 evidence; a second bounded digest retains mutation detection without another allocation. Live mutable descriptors are never exposed to Pillow.
- Recovery scanning now uses `islice`, so caps of 0, 1, and 256 consume exactly 0, 1, and 256 iterator entries rather than one extra.

Final Fix Round 10 evidence: exact new/adjacent 10 passed, vehicle/Matrix/decomposition 305 passed, Task 9 compatibility 397 passed, dependency-lock/structural 107 passed, and final post-edit full serial 1,570 passed in 110.18 seconds. Compileall, diff, and repository artifact checks are clean; `/proc/self/fd` stayed 4 to 4 across 100 retained publication/read/disposal cycles. Caps are descriptor I/O 74/120, cleanup 103/150, disposal 115/120, JPEG publication 267/350, and rooted storage 274/320.

## Fix Round 11

- Random disposal names now have the exact recoverable grammar `.<safe basename>.<16 lowercase hex>.dispose`. The same capped scan consumes at most 256 combined disposal/quarantine entries, processes disposals first, deterministically restores an absent target no-clobber, and finishes a same-inode target/disposal unlink.
- A failed disposal unlink remains one disposal-only or target-plus-disposal state. Later cleanup detects and retries that state before any fresh quarantine/disposal work, so three persistent failures do not multiply names or hardlinks; successful recovery removes all residue while preserving the original bytes.
- Commit `1aa74cb` made newly created `.upload-derivatives` entries durable before child use. The final verified-identity cache and restart behavior are recorded in Fix Round 12 below.
- `delete_owned_artifact()` and `delete_upload_derivative()` now return immutable `OwnedArtifactDeleteResult(status="deleted"|"missing"|"failed", bytes_deleted=N)`. Retention treats deleted and confirmed missing as success, stops before raw deletion on derivative failure, increments failed telemetry for typed failure, and reports retained/pruned counts from actual outcomes rather than ambiguous zero bytes.
- The tracked file map, interfaces, deployment threat/recovery documentation, and public recovery docstring now describe the implemented crash and durability contracts.

Fix Round 11 landed as `1aa74cb`. Its then-current verification was 1,575 serial tests; the authoritative current evidence is the Fix Round 12 closeout below.

## Fix Round 12

- Commit `ef6474a` writes a bounded, versioned `.owned-disposals.json` manifest durably before every owned source-to-disposal transition. Its at-most-256 entries bind exact disposal and recovery basenames to the expected device/inode identity, and its 256 KiB input ceiling bounds parsing. A crash before rename leaves a safely reconcilable stale record; a crash after rename leaves directly reachable work.
- Manifest recovery runs before the bounded legacy directory scan, and indexed candidates cannot be reprocessed through the identity-less legacy path. This prevents first-256 decoy starvation for current transitions and rejects a disposal inode replaced after the crash. Matrix startup/retention covers the snapshot root and `.upload-derivatives`; runtime startup and the next vehicle capture cover `occupied-full` and `occupied-crops`.
- Disposal unlink and parent-directory fsync are separate outcomes. Once unlink succeeds, callers receive `OwnedArtifactDeleteResult(status="deleted", durable=False)` if fsync fails; they do not retry an absent target as a normal failure. Retention counts the namespace deletion, emits `snapshot-retention-durability-uncertain`, and leaves the manifest entry for later reconciliation.
- Derivative directory durability now uses a thread-safe, process-local cache capped at 256 verified root/child identity tuples. The parent root is fsynced before first child use in each process, cache insertion occurs only after successful fsync, a failed fsync is retried, and restart or inode replacement requires fresh verification.
- Recovery indexing, legacy recovery, directory durability, and retention logging live in `owned_disposal_manifest.py`, `owned_file_recovery.py`, `owned_directory_durability.py`, and `matrix_retention_logging.py`, with explicit 180/180/60/80 line caps.
- Deployment documentation identifies the hidden manifest, startup/next-operation recovery owners, uncertainty telemetry, and the cooperative-directory threat boundary.

Final Fix Round 12 evidence: exact new recovery/durability regressions 8 passed; canonical Task 9 suite 218 passed; broad Matrix/vehicle/startup/decomposition suite 466 passed; post-closeout structural/docs/startup/identity checks 37 passed; and the final fresh serial suite passed 1,583 tests in 36.40 seconds. Python compileall, dependency-lock digest `8899d93ce13a7c521fe4367bf348b51d30a3f2a65242309e1e7ef7884f009e33`, and `git diff --check` were clean. Final caps are manifest 154/180, recovery 141/180, directory durability 23/60, retention logging 53/80, descriptor binding 178/180, cleanup 81/150, disposal 199/200, rooted storage 311/320, Matrix snapshots 414/430, and vehicle image handling 210/220.

## Fix Round 14

- Disposal producers now hold one per-directory reentrant manifest transaction continuously across durable record publication, source-to-disposal rename, identity decisions, unlink, and reconciliation. Recovery holds the same transaction across manifest read, existence/error classification, recovery, and forget, eliminating the interval where a recovery thread could classify a recorded-but-not-yet-renamed disposal as completed and erase its index.
- The process-local lock registry is keyed by held directory device/inode identity, reference-counted, and removed when idle. Nested record/read/forget operations reuse an `RLock` without deadlock; work in unrelated owned directories remains concurrent. The documented deployment model assigns each owned directory to one service process, so no persistent cross-process lock artifact is required.
- Manifest existence checks now classify only `FileNotFoundError` as absent. Permission, EIO, and other transient stat errors retain the manifest record and report pending recovery rather than falsely forgetting work.
- Deterministic thread barriers prove recovery cannot pass a producer paused after manifest publication and before rename, the manifest survives until actual recovery, nested transaction APIs complete, and a blocked directory does not serialize unrelated cleanup. A dedicated EIO regression proves the exact manifest entry remains pending.

Fix Round 14 raises only the manifest module cap from 180 to 200 lines for the per-directory transaction registry and public context; recovery remains capped at 180 and disposal at 200.

Final Fix Round 14 evidence: strict RED produced three expected failures with the unrelated-directory concurrency control already passing; GREEN passed all four interleaving, EIO, reentrancy, and independence regressions. The canonical Task 9 suite passed 223 tests, the Matrix/startup/vehicle/decomposition gate passed 472, dependency-lock/docs verification passed 82, and the final fresh serial suite passed 1,587 tests in 35.02 seconds. Compileall, current dependency locks, `git diff --check`, and artifact-residue checks passed; 100 disposal cycles kept `/proc/self/fd` at 4 to 4 and left the manifest lock registry empty. Final affected caps are manifest 189/200, recovery 151/180, and disposal 198/200.

## Fix Round 15

- Post-rename identity binding and pathname checks now distinguish conclusive absence/nonregular/mismatch from transient `open`, `fstat`, or `stat` failures. `FileNotFoundError` remains a conclusive missing observation; other `OSError` subclasses propagate to the transaction boundary and return `pending`.
- The producer performs no restore and no manifest removal after a transient identity observation failure. The recovery path applies the same classification and retains the exact indexed state until the observation succeeds.
- Deterministic regressions inject EIO at the first no-follow disposal bind and at the subsequent same-inode pathname check. Both transitions remain indexed and absent at the recovery name behind 300 decoys, then restore the original bytes and remove disposal/manifest residue when the fault clears.

Final Fix Round 15 evidence: strict RED produced the two expected manifest-loss/early-restore failures; GREEN passed both EIO regressions and 26 adjacent ownership/disposal/recovery tests. The canonical Task 9 suite passed 225 tests, the Matrix/startup/vehicle/decomposition gate passed 474, dependency-lock/docs verification passed 116, and the final fresh serial suite passed 1,589 tests in 34.64 seconds. Compileall, current dependency locks, `git diff --check`, and artifact-residue checks passed; 100 disposal cycles kept `/proc/self/fd` at 4 to 4. The disposal module remains at its 200-line cap.
