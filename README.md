# GivEVC OCPPv2 Portal

A self-hosted OCPP 2.0 management portal for GivEnergy EV chargers. Provides a multi-user web interface, a public REST API, real-time charger state via Server-Sent Events, scheduling with automatic DST correction, RFID tag management, firmware updates, and a full admin backend — all running as a single Docker container.

---

## Table of Contents

- [Features](#features)
- [Screenshots](#screenshots)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Charger Onboarding](#charger-onboarding)
- [User Guide](#user-guide)
- [Account Management](#account-management)
- [Admin Guide](#admin-guide)
- [Public API](#public-api)
- [Demo Mode](#demo-mode)
- [Reverse Proxy Setup](#reverse-proxy-setup)
- [Home Assistant Add-on](#home-assistant-add-on)
- [Security Notes](#security-notes)

---

## Features

- **Multi-user** — each user manages their own chargers independently
- **Real-time dashboard** — live power, current, voltage, charge mode, session energy via SSE
- **Charging control** — start/stop, current limit (6–32 A), session energy cap
- **Charge modes** — Eco (Solar), Hybrid, and Grid modes
- **Time-based scheduling** — daily or per-day-of-week schedules with automatic BST/GMT correction
- **RFID tag management** — add, enable/disable, set expiry, rename tags
- **Session history** — per-session kWh, meter start/stop, timestamps
- **Energy statistics** — today, this month, custom date range
- **Firmware updates** — browse available versions, install OTA
- **Two-factor authentication** — TOTP (Google Authenticator, Authy, etc.)
- **Public REST API** — Bearer-token API aligned with the GivEnergy Cloud API v1 spec
- **Interactive API docs** — built-in Swagger UI at `/api/v1/docs`
- **Admin backend** — user management, charger assignment, portal settings, statistics
- **Demo mode** — simulated charger with seeded data for evaluation

---

## Screenshots

### Desktop

| Area | Screenshot |
|------|------------|
| Overview | ![Desktop overview dashboard](docs/screenshots/overview-desktop.png) |
| Charging controls | ![Desktop charging controls](docs/screenshots/control-desktop.png) |
| Charge modes | ![Desktop charge mode settings](docs/screenshots/modes-desktop.png) |
| Charging schedule | ![Desktop charging schedule](docs/screenshots/schedule-desktop.png) |
| RFID tags | ![Desktop RFID tag management](docs/screenshots/id-tags-desktop.png) |
| Power statistics | ![Desktop power statistics](docs/screenshots/statistics-power-desktop.png) |
| Energy statistics | ![Desktop energy statistics](docs/screenshots/statistics-energy-desktop.png) |
| Activity logs | ![Desktop activity logs](docs/screenshots/logs-desktop.png) |
| Error log | ![Desktop error log](docs/screenshots/errors-desktop.png) |
| Charger account management | ![Desktop charger account management](docs/screenshots/account-chargers-desktop.png) |
| Account security | ![Desktop account security](docs/screenshots/account-security-desktop.png) |
| API keys | ![Desktop API key management](docs/screenshots/api-keys-desktop.png) |

### Mobile

| Area | Screenshot |
|------|------------|
| Overview | ![Mobile overview dashboard](docs/screenshots/overview-mobile.png) |
| Charging controls | ![Mobile charging controls](docs/screenshots/control-mobile.png) |
| Charge modes | ![Mobile charge mode settings](docs/screenshots/modes-mobile.png) |
| Charging schedule | ![Mobile charging schedule](docs/screenshots/schedule-mobile.png) |
| RFID tags | ![Mobile RFID tag management](docs/screenshots/id-tags-mobile.png) |
| Power statistics | ![Mobile power statistics](docs/screenshots/statistics-power-mobile.png) |
| Energy statistics | ![Mobile energy statistics](docs/screenshots/statistics-energy-mobile.png) |
| Activity logs | ![Mobile activity logs](docs/screenshots/logs-mobile.png) |
| Error log | ![Mobile error log](docs/screenshots/errors-mobile.png) |
| Charger account management | ![Mobile charger account management](docs/screenshots/account-chargers-mobile.png) |
| Account security | ![Mobile account security](docs/screenshots/account-security-mobile.png) |
| API keys | ![Mobile API key management](docs/screenshots/api-keys-mobile.png) |

---

## Architecture

Three services run in a single asyncio loop inside one container:

| Service | Default Port | Purpose |
|---------|-------------|---------|
| OCPP WebSocket server | **7655** | Charger connections (ws:// or wss://) |
| Firmware transfer server | **9688** | Firmware file downloads |
| Web UI / API server | **8099** | Browser UI, REST API, SSE |

State is persisted in SQLite at `/data/givevcocppv2.db`. Charger snapshots, charging sessions, and meter readings are all stored there.

---

## Quick Start

### Docker Compose

```yaml
services:
  givevc-ocppv2:
    image: ghcr.io/djbenson/givevc-ocppv2:latest
    container_name: givevc-ocppv2
    restart: unless-stopped
    ports:
      - "0.0.0.0:7655:7655"   # OCPP — expose publicly so chargers can connect
      - "127.0.0.1:9688:9688" # Firmware — localhost only
      - "127.0.0.1:8099:8099" # Web UI — localhost only, put behind reverse proxy
    environment:
      TZ: "Europe/London"
      PUBLIC_OCPP_BASE_URL: "wss://your-domain.com"
      SMTP_HOST: "smtp.example.com"
      SMTP_PORT: "587"
      SMTP_USERNAME: "user@example.com"
      SMTP_PASSWORD: "password"
      SMTP_FROM: "GivEVC <noreply@example.com>"
      SMTP_TLS: "true"
    volumes:
      - app-data:/data

volumes:
  app-data:
```

On first run, navigate to `http://localhost:8099` (or your proxy URL). The first account registered is automatically granted **Admin** role.

---

## Configuration

All configuration is via environment variables.

### Ports and Networking

| Variable | Default | Description |
|----------|---------|-------------|
| `OCPP_PORT` | `7655` | OCPP WebSocket listen port |
| `FIRMWARE_PORT` | `9688` | Firmware transfer server port |
| `INGRESS_PORT` | `8099` | Web UI and API port |
| `PUBLIC_OCPP_BASE_URL` | _(auto)_ | Public OCPP base URL shown to users during onboarding (e.g. `wss://charger.example.com`). Detected from the request origin if not set. |
| `PUBLIC_FIRMWARE_HOST` | _(auto)_ | Public hostname for firmware downloads |
| `PUBLIC_FIRMWARE_PORT` | `9688` | Public port for firmware downloads |

### Email (SMTP)

Email is required for password reset and optional for new-account email verification.

| Variable | Default | Description |
|----------|---------|-------------|
| `SMTP_HOST` | _(none)_ | SMTP server hostname |
| `SMTP_PORT` | `587` | SMTP port |
| `SMTP_USERNAME` | _(none)_ | SMTP login username |
| `SMTP_PASSWORD` | _(none)_ | SMTP login password |
| `SMTP_FROM` | _(none)_ | From address, e.g. `GivEVC <noreply@example.com>` |
| `SMTP_TLS` | `true` | Enable STARTTLS |

### Firmware

| Variable | Default | Description |
|----------|---------|-------------|
| `FIRMWARE_ROOT` | `/data/firmware` | Directory for cached firmware files |
| `FIRMWARE_MANIFEST_URL` | GitHub raw URL | JSON manifest listing available firmware versions |

### General

| Variable | Default | Description |
|----------|---------|-------------|
| `TZ` | `Europe/London` | Server timezone — used for schedule localisation |
| `DEBUG_LOGGING` | `false` | Enable verbose OCPP frame logging |

---

## Charger Onboarding

Onboarding links a physical charger to a user account using a one-time password exchange.

### Step 1 — Create an Onboarding Session

Go to **Account → Chargers** and click **Add Charger**. The portal generates:

- A unique **Charge Point ID** — the charger's identity on the OCPP server
- A one-time **OCPP Password** — shown once, never stored in plaintext
- The **OCPP Endpoint URL** — where the charger should connect

> The password is shown once only. If you lose it, cancel the session and create a new one. Sessions expire after 30 minutes.

### Step 2 — Configure the Charger

In the charger's OCPP settings (via the GivEnergy app or local web interface):

| Setting | Value |
|---------|-------|
| OCPP version | OCPP 1.6 JSON |
| Central system URL | The endpoint URL from Step 1 (e.g. `wss://charger.example.com/`) |
| Password | The OCPP Password from Step 1 |

> **Note:** GivEnergy chargers do not have a user-configurable Charge Point Identity — the charger's serial number is used automatically. The portal accepts the connection and links it to your account based on the OCPP password.

### Step 3 — Charger Connects

When the charger connects and authenticates, it is automatically assigned to your account. The dashboard becomes active.

### Multiple Chargers

Each user can onboard multiple chargers. Use **Account → Chargers** to switch the active charger. Admins can also assign unadopted chargers to users directly.

---

## User Guide

### Overview Dashboard

The dashboard shows real-time state pushed via Server-Sent Events:

- **Charger Status** — OCPP status, vehicle plug state, live power gauge, charge mode pill
- **Energy Usage** — current/last session kWh, today, this month, all-time total
- **Live Readings** — power (kW), current (A), voltage (V)
- **Charger Info** — model, firmware version, serial number, local IP, OCPP status

### Starting and Stopping a Charge

Use the **play/stop button** in the charger status panel. Charging can also be initiated via RFID tag or the public API.

### Charge Modes

| Mode | Label | Description |
|------|-------|-------------|
| `SuperEco` | Solar Mode | Prioritises solar generation |
| `Eco` | Hybrid Mode | Balances solar and grid |
| `Boost` | Grid Mode | Maximum power from grid |

Click the mode badge on the dashboard or go to **Settings → Modes** to change. A confirmation message appears below the mode cards when the change is applied.

### Settings — General

| Setting | Description |
|---------|-------------|
| Current Limit | Maximum charge current, 6–32 A |
| Plug and Go | Auto-start when a vehicle is plugged in |
| Max Energy Per Session | Optional kWh cap; charging stops when reached |
| Front Panel LEDs | Enable or disable the charger's status LEDs |
| Local Modbus Control | Enable for external energy management systems |
| Randomised Delay | Adds a random 10–30 minute delay before charging starts |
| Suspended State Timeout | How long to wait in SuspendedEV before stopping |
| DNO Fuse Rating | Maximum import capacity, 40–100 A |

### Settings — Schedules

Create time-based charging windows sent to the charger as OCPP `SetChargingProfile`.

**To create a schedule:**
1. Go to **Settings → Schedules** and click **New Schedule**
2. Set name, start time, end time, days of week, and current limit (A)
3. Enable the schedule to push it immediately

Only one schedule is active at a time. Enabling a new one disables the previous.

**DST Correction:**
Schedules are stored in local time (HH:MM + days) and converted to UTC only at push time. When the UK transitions between GMT and BST (last Sunday of March and October at 01:01 UTC), the portal automatically re-pushes the active schedule with recalculated UTC offsets. Chargers that are offline at the transition receive the correction on next reconnect.

### Settings — RFID Tags

Manage which RFID cards or fobs are authorised to start a charge.

| Action | Description |
|--------|-------------|
| Add Tag | Enter UID, optional alias and expiry date |
| Enable / Disable | Temporarily block a tag without deleting it |
| Delete | Permanently remove a tag |

Tags are synchronised to the charger via `SendLocalList`.

### Statistics

**Power** — hourly power samples over a configurable date range, displayed as a chart.

**Energy** — daily and monthly kWh totals over a configurable date range.

### Logs

**Activity Log** — every command sent to the charger: timestamp, user, action, result, and source (Portal / API / System).

**OCPP Frames** — the last 100 raw OCPP message pairs, useful for diagnostics.

### Firmware Updates

Go to **Settings → Firmware** to view the installed version, browse available releases from the manifest, and install an update OTA via the built-in firmware transfer server.

---

## Account Management

### Account → Chargers

Add chargers via onboarding, switch the active charger, or remove chargers from your account. When multiple chargers are registered, the charger status block in the sidebar becomes a dropdown allowing quick switching between chargers without leaving the current page. Deleting a charger immediately disconnects it from the OCPP server.

### Account → Security

**Change Password** — requires current password.

**Two-Factor Authentication (TOTP):**

1. Click **Set up 2FA**
2. Scan the QR code with an authenticator app (Google Authenticator, Authy, 1Password, etc.)
3. Enter the 6-digit code to confirm

Once enabled, every login requires a TOTP code after password entry. To disable, you need both your current password and a valid OTP.

Admins can reset another user's 2FA from the admin panel.

**Delete Account** — permanently deletes your account, all chargers, charging history, schedules, RFID tags, API keys, and sessions. Requires confirmation. Immediately logs you out. Not available to the demo account.

### Account → API Keys

Create long-lived API keys for the public REST API or third-party integrations.

**Scopes:**

| Scope | Access |
|-------|--------|
| `read` | All GET endpoints |
| `write` | GET endpoints + POST command endpoints |

Keys are shown once on creation and hashed in storage. Revoke from this page at any time. Expiry is optional (set in days).

### Account → General

Set your preferred display **theme**: Light, Dark, or System default.

---

## Admin Guide

The admin panel is available to accounts with the Admin role (the first registered account).

### User Accounts

Search users by email. Available actions:

| Action | Description |
|--------|-------------|
| Enable / Disable | Block or restore login access |
| Reset 2FA | Clear the user's TOTP; they can re-enrol |
| Bypass Email Verification | Mark a new account as verified |
| Delete Account | Permanently removes user, chargers, and all data |

### Chargers

**Unadopted Chargers** — chargers connected to the OCPP server but not yet assigned to any account. Search by serial number and assign to a user from here.

### Portal Settings

| Setting | Description |
|---------|-------------|
| Account Registration | Allow or prevent new sign-ups |
| Initial Email Validation | Require email OTP verification before first login |
| Public API | Enable or disable `/api/v1/` globally |
| SMTP Settings | View SMTP status; send a test email |
| Demo Mode | Enable or disable the simulated demo charger |

### Statistics

Real-time portal statistics (demo account excluded):

| Stat | Description |
|------|-------------|
| Users — Total | Non-demo accounts |
| Users — Active | Verified and not disabled |
| Users — Pending | Awaiting email verification |
| Users — Disabled | Blocked accounts |
| Chargers — Total | Non-deleted chargers |
| Chargers — Adopted | With a charge point ID assigned |
| Chargers — Pending | Created but not yet adopted |
| Active Onboarding Sessions | Unexpired pending onboarding sessions |
| Charging Sessions (total) | All recorded sessions |
| Charging Sessions (active) | Sessions without a stop time |
| Meter Samples | Total meter readings stored |
| Schedules Pending DST Correction | Offline chargers awaiting re-push |
| Active API Keys | Not revoked and not expired |
| Active Browser Sessions | Currently logged-in sessions |
| Database Size | SQLite file size |
| Last DST Correction Run | Timestamp of last automatic correction job |

**Force Re-push All Schedules** — immediately re-pushes the active schedule to all online chargers (max 10 concurrent). Offline chargers are flagged and corrected on next reconnect. Results show how many succeeded, failed, and were flagged offline.

**Purge Orphaned Data** — removes charging sessions, meter readings, and charger state snapshots that belong to deleted or unrecognised chargers. Requires confirmation. Reports counts of each table purged.

### Updates

When running as a standalone Docker container, the portal checks GitHub for new releases every 6 hours. When an update is available, an amber banner appears at the bottom of the sidebar; clicking it takes you to Admin → Statistics → Updates where you can see the latest version and a link to the release notes.

The update channel can be set to **Stable** (default) or **Beta** (includes pre-releases). Changing the channel triggers an immediate re-check.

> **Note:** The portal cannot update itself. To apply an update, pull the new image and recreate the container:
> ```bash
> docker compose pull && docker compose up -d
> ```
> For hands-off automatic updates, [Watchtower](https://containrrr.dev/watchtower/) can monitor the image and restart the container when a new version is published.

This feature is disabled when running as a Home Assistant add-on — HA's own update mechanism handles add-on version management.

---

## Public API

The public REST API is aligned with the GivEnergy Cloud API v1 specification.

| | |
|--|--|
| **Base URL** | `/api/v1/` |
| **Interactive docs** | `/api/v1/docs` (Swagger UI with light/dark/system theme) |
| **OpenAPI spec** | `/api/v1/openapi.yaml` |
| **Auth** | Bearer token (API key) |

### Authentication

Create an API key in **Account → API Keys**. Keys have the prefix `gevc_`.

```
Authorization: Bearer gevc_xxxxxxxxxxxxxxxxxx
```

### Endpoints

#### Chargers

```
GET  /api/v1/ev-charger
GET  /api/v1/ev-charger/{uuid}
GET  /api/v1/ev-charger/{uuid}/schedules
GET  /api/v1/ev-charger/{uuid}/id-tags
GET  /api/v1/ev-charger/{uuid}/charging-sessions
GET  /api/v1/ev-charger/{uuid}/meter-data
GET  /api/v1/ev-charger/{uuid}/commands
GET  /api/v1/ev-charger/{uuid}/commands/{command_id}
POST /api/v1/ev-charger/{uuid}/commands/{command_id}   (write scope)
```

**Pagination:** `?page=N` — responses include `links` (first/prev/next/last) and `meta` (current_page, last_page, per_page, total).

**Filtering — charging sessions:** `start_time`, `end_time` (ISO 8601)

**Filtering — meter data:** `start_time`, `end_time`, `measurands` (comma-separated), `meter_id`

### Commands Reference

Commands marked with ✦ are **not available in the GivEnergy Cloud API** and are unique to this portal.

| Command | Body Fields | Description |
|---------|------------|-------------|
| `start-charge` | _(none)_ | Start a session |
| `stop-charge` | _(none)_ | Stop the active session |
| `change-mode` | `mode`: `Eco` \| `SuperEco` \| `Boost` \| `ModbusSlave` | Set charge mode |
| `adjust-charge-power-limit` | `limit`: 6–32 | Set max current (amps) |
| `set-session-energy-limit` ✦ | `limit`: 0.1–250 | Cap session energy (kWh); omit to remove cap |
| `set-plug-and-go` ✦ | `enabled`: boolean | Auto-start on plug-in |
| `unlock-connector` | _(none)_ | Release a stuck connector |
| `restart-charger` | `hard_reset`: boolean | Restart (soft or hard) |
| `perform-factory-reset` | _(none)_ | Factory reset |
| `read-cp-voltage-and-duty-cycle` | _(none)_ | Read CP signal |
| `change-randomised-delay-duration` ✦ | `duration`: 600–1800 | Random start delay (seconds) |
| `adjust-suspended-state-wait-timeout` ✦ | `value`: 0–43200 | Suspend timeout (seconds) |
| `enable-front-panel-led` ✦ | `value`: boolean | Toggle LEDs |
| `enable-local-control` ✦ | `value`: boolean | Toggle Modbus control |
| `set-max-import-capacity` ✦ | `value`: 40–100 | DNO fuse rating (amps) |
| `set-schedule` ✦ | `name`, `schedule_id`?, `periods`: [{start_time, end_time, day_of_week[], current_a}] | Create/update schedule |
| `set-active-schedule` ✦ | `schedule_id`? | Activate schedule (omit to disable all) |
| `delete-charging-profile` ✦ | `schedule_id`? | Delete schedule (omit to delete all) |
| `add-id-tags` ✦ | `id_tags`: [{id, alias?, expiry_date?}] | Add RFID tags |
| `delete-id-tags` ✦ | `id_tags`: [id, ...] | Remove RFID tags |
| `rename-id-tag` ✦ | `tag_id`, `alias` | Update tag alias |

### Response Codes

| Code | Meaning |
|------|---------|
| 200 | Success |
| 401 | Missing or invalid API key |
| 403 | Write scope required |
| 404 | Charger or resource not found |
| 422 | Invalid parameters |
| 503 | Charger offline or command rejected |

### curl Examples

```bash
# List chargers
curl -s -H "Authorization: Bearer gevc_xxxx" \
  https://your-domain.com/api/v1/ev-charger | jq

# Start a charge
curl -s -X POST \
  -H "Authorization: Bearer gevc_xxxx" \
  https://your-domain.com/api/v1/ev-charger/EVC-123456/commands/start-charge | jq

# Change to Solar (Eco) mode
curl -s -X POST \
  -H "Authorization: Bearer gevc_xxxx" \
  -H "Content-Type: application/json" \
  -d '{"mode": "Eco"}' \
  https://your-domain.com/api/v1/ev-charger/EVC-123456/commands/change-mode | jq

# Set current limit to 16 A
curl -s -X POST \
  -H "Authorization: Bearer gevc_xxxx" \
  -H "Content-Type: application/json" \
  -d '{"limit": 16}' \
  https://your-domain.com/api/v1/ev-charger/EVC-123456/commands/adjust-charge-power-limit | jq

# List recent charging sessions
curl -s -H "Authorization: Bearer gevc_xxxx" \
  "https://your-domain.com/api/v1/ev-charger/EVC-123456/charging-sessions?page=1" | jq

# Get meter data filtered by measurand
curl -s -H "Authorization: Bearer gevc_xxxx" \
  "https://your-domain.com/api/v1/ev-charger/EVC-123456/meter-data?measurands=Energy.Active.Import.Register" | jq
```

---

## Demo Mode

Demo mode starts a simulated GivEnergy EV charger so the portal can be evaluated without physical hardware.

**Enable:** Admin → Portal Settings → Demo Mode → toggle on.

**Demo credentials:**
```
Email:    demo.user@givevcdemo.local
Password: p@ssw0rd123
```

The demo account is pre-seeded with:
- A simulated charger (`demo-charger-001`)
- 92 days of historical charging sessions and meter readings
- An example Octopus Go charging schedule (00:30–05:30)
- Sample RFID tags

The demo account cannot change its password, enable 2FA, delete its charger, or create onboarding sessions. All other features work normally including the public API (create an API key and test it).

---

## Reverse Proxy Setup

The web UI and API should be placed behind a TLS-terminating reverse proxy. The OCPP WebSocket port must be publicly reachable for chargers to connect.

### Caddy example

```caddyfile
# Web UI and API
your-domain.com {
    reverse_proxy localhost:8099
}

# OCPP WebSocket (separate subdomain)
ocpp.your-domain.com {
    reverse_proxy localhost:7655
}
```

Set `PUBLIC_OCPP_BASE_URL=wss://ocpp.your-domain.com` so onboarding shows users the correct endpoint.

If the OCPP endpoint is on the same domain as the web UI (e.g. at `/ocpp/`), configure your proxy to forward that path to port 7655 and set `PUBLIC_OCPP_BASE_URL=wss://your-domain.com/ocpp`.

---

## Home Assistant Add-on

The portal is available as a Home Assistant add-on. Install from the repository and configure via the add-on options panel — the options map directly to the environment variables above. The web UI is accessible via the HA ingress (no extra port forwarding required).

---

## Security Notes

- **Passwords** are hashed with PBKDF2-SHA256 (260,000 iterations)
- **Session tokens** are hashed in the database; cookies are HttpOnly, Secure, SameSite=Strict with a 14-day expiry
- **API keys** are shown once on creation and hashed in storage; scoped to read or write
- **OCPP passwords** are one-time use and hashed; prevent charger hijacking
- **Sensitive fields** are encrypted at rest using Fernet symmetric encryption; the key is stored in `/data/auth_secret.key`
- **Email OTP codes** expire after 10 minutes; 30-second resend cooldown
- **Password reset tokens** expire after 15 minutes
- The **web UI port (8099)** binds to `127.0.0.1` by default — do not expose directly to the internet
- The **OCPP port (7655)** must be reachable by chargers; restrict to known IP ranges at the firewall if possible
