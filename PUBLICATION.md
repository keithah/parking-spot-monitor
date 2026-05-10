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
