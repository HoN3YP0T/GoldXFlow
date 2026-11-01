# Deployment templates (Phase 17)

- `systemd/`: Linux VPS units (live ingestion, dashboard, alert check timer, daily backup timer) and an env-file
  example for secrets. A **non-US** host is required: Binance REST answers HTTP 451 to US hosts.
- `windows/install_tasks.ps1`: the Windows trading PC (MT5 + bridge). It registers scheduled tasks that start at logon
  and restart on failure. Order routing starts in the **demo** profile.

Secrets come from the environment (systemd `EnvironmentFile`, Windows user environment variables) or the MT5 Common
Files secret file, never from the config. Procedures: `docs/RUNBOOK.md`. Readiness status:
`docs/PRODUCTION_READINESS.md`.

These templates have not been installed on a real VPS or PC from the build environment. Adjust the paths and the
user, then run each step in the runbook once by hand before relying on the service.
