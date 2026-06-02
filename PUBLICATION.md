# Public repository workflow

This public repository was initialized from a clean snapshot, not from the original local development history.

The private local repository may contain ignored operator files such as `.env`, `config.yaml`, raw camera frames, and GSD runtime state. Do not push that local history directly. Publish updates by preparing a clean snapshot that excludes:

- `.env` and `.env.*`
- `config.yaml`
- `.gsd/` runtime state
- `.agents/` local agent skills
- raw camera frames and live-proof Docker logs
- cache directories

Before pushing, scan the snapshot for real RTSP URLs, Matrix access tokens, Authorization headers, and raw live-proof logs.

The public branch remote in this workspace may have its normal push URL disabled to protect private local history. When publishing a clean branch, push the current branch explicitly to the GitHub repository URL or through an authenticated GitHub CLI session; do not rewrite the disabled remote to point at private history. After pushing, fetch the public branch and confirm the local branch is no longer ahead before treating publication as complete.

Deployment from this repository is a local Docker Compose operation. Use `docker compose up -d --build parking-spot-monitor` for code, Dockerfile, or dependency changes so the `parking-spot-monitor:local` image is rebuilt before the container is recreated. Use `docker compose restart parking-spot-monitor` only for config or environment changes that do not require a new image. Post-deploy smoke should check `docker compose ps` and recent structured logs for `startup-ready`, `capture-frame-written`, `detection-frame-processed`, and the relevant Matrix delivery or command-poll events.
