# Multi-User Portal and Public API Plan

This file is the durable working plan for converting the current single-charger portal into a hosted multi-user service. Update task statuses here as work completes so the project can be resumed after context compaction.

Last updated: 2026-04-17.

Status legend:

- `[ ]` Not started
- `[~]` In progress
- `[x]` Complete
- `[!]` Blocked or needs decision

## Current Baseline

- The app is a single-process aiohttp service. This is acceptable for the current deployment target: one service instance handling many users and many chargers.
- Browser authentication, users, roles, sessions, chargers, onboarding sessions, API-key records, coordinator state, and per-charger state snapshots are now stored in SQLite.
- Legacy `/data/state.json` is imported into SQLite on first startup when no SQLite coordinator state exists.
- Multiple chargers can connect concurrently to one service instance and are keyed by OCPP charge point identity.
- Commands require an explicit `charge_point_id`; the previous implicit active-session command fallback has been removed.
- Unadopted chargers can connect as passive/unmanaged sessions for admin assignment, but cannot receive outbound OCPP commands.
- Adopted chargers are commandable only when their charge point identity is owned by an account.
- Portal state and `/api/chargers` both use per-charger snapshots plus the live connection registry, reducing stale active-state drift.
- Schedule management is wired to OCPP for enabled schedules using `SetChargingProfile`, `ClearChargingProfile`, and accepted-response gating.
- RFID tag management is wired to OCPP using `GetLocalListVersion` and `SendLocalList` differential updates before persisting portal state.
- Firmware updates use the central firmware cache/repository and track transfer/apply/reboot progress using firmware-server transfer events, `FirmwareStatusNotification`, and post-reboot firmware version checks.
- Remaining single-instance caveat: live OCPP websocket sessions are still in memory in this process. Horizontal scaling would require sticky routing or a distributed command/session layer.
- The current OCPP charger identity still comes from the websocket route / charge point id. Secure self-service ownership claim validation is not complete yet.
- Home Assistant add-on storage now maps `addon_config:rw` and uses `/config` for `auth.db`, `auth_secret.key`, legacy state import, and firmware cache in HA mode. Native Docker continues to use `/data`.

## External API Reference

Target API style should follow GivEnergy Cloud API v1 where practical:

- Base path style: `/v1/...`
- Authentication: `Authorization: Bearer {API_KEY}`
- Responses: successful responses wrap payloads in a `data` property.
- Errors should use conventional HTTP codes and JSON error bodies.
- EV charger endpoints include charger listing, charger details, meter data, supported commands, command data, send command, and charging sessions.
- EV charger command examples include `start-charge`, `stop-charge`, `set-plug-and-go`, `set-session-energy-limit`, `set-schedule`, and `change-mode`.

Reference checked: https://givenergy.cloud/docs/api/v1, last updated in the document as 2026-04-17.

## Security Position

Do not allow automatic ownership from a random charger connection alone. OCPP 1.6J identity is not proof of ownership.

Database design must assume the database can be copied or inspected:

- Store only password hashes for user passwords.
- Store only hashes of browser session tokens, API keys, OCPP claim tokens, and generated charger connection passwords.
- Store TOTP seeds encrypted with an application secret outside the database; prefer `AUTH_SECRET` in hosted deployments.
- Display generated secrets once at creation time, then discard plaintext server-side.
- Do not store recoverable onboarding passwords, API keys, or charger connection secrets for later display.
- Keep personally identifiable account data minimal; email and display name are retained because they are needed for login and the portal UI.

Use a claim-token onboarding flow, preferably using the charger's OCPP password field if the charger sends that value as HTTP Basic Authentication during the websocket upgrade:

1. Authenticated user creates a charger onboarding claim in the portal.
2. Server generates:
   - A random one-time claim token.
   - A strong one-time OCPP password.
   - An expected OCPP charge point id or onboarding websocket URL segment.
   - An expiry window, for example 15 minutes.
3. User configures the charger OCPP URL to the generated endpoint and enters the generated password in the charger's local OCPP password field.
4. First charger connection to that endpoint is placed into `pending_claimed` state.
5. Server validates:
   - Claim token exists, is unexpired, and unused.
   - The websocket upgrade includes the generated password if the charger supports OCPP Basic Auth.
   - Incoming charge point id matches the expected id if supplied.
   - No existing charger already owns that charge point id unless explicitly re-claimed by an owner/admin.
6. Server binds charger to the user account and consumes the claim token.
7. After binding, future connections must use the charger-specific endpoint and the stored charger connection secret.

If the charger password is OCPP Basic Auth, treat it as a connection secret rather than a portal password:

- The server should store only a hash of the generated charger connection secret.
- The charger should send it on every websocket connection attempt.
- The username is likely the OCPP charge point identity and the password is the configured OCPP password, but this must be verified against real charger traffic.
- Reverse proxies must pass the `Authorization` header through to the app for websocket upgrades.
- The portal should support rotating the charger connection secret, with an overlap window so users can update the charger without downtime.

Implementation note:

- The current `OcppServer._handle_websocket()` does not inspect `Authorization`.
- aiohttp can validate the request headers before `WebSocketResponse.prepare()`, so this can be implemented before accepting a charger session.
- If the charger does not send Basic Auth, fall back to a claim token in the OCPP websocket path and/or physical proof.

Recommended additional proof options:

- Display a random verification code in the portal and require the user to set the charger charge point id temporarily to include it.
- Ask the user to press a physical action during the claim window, for example plug/unplug or restart, and detect the expected OCPP event.
- For managed deployments, require admin approval for first claim.

Avoid relying on serial number alone. It is useful metadata, but not a secret.

## Target Data Model

- `[x]` Add persistent storage abstraction.
  - Start with SQLite for local/self-hosted deployments.
  - Import existing `/data/state.json` into SQLite coordinator state.
  - Schema migrations are implemented inside `AuthStore`.
- `[x]` Add `users`.
  - Fields: id, email, password hash, display name, role, created_at, updated_at, disabled_at.
  - Initial role scheme: `Admin` and `User`.
  - First registered account becomes `Admin`; later accounts default to `User`.
  - Password hashing: Argon2id, bcrypt, or PBKDF2-HMAC with per-password salt; never raw SHA.
- `[~]` Add `sessions`.
  - HTTP-only secure cookies for browser UI sessions.
  - Store only a hash of the session token in the database.
  - Remaining: CSRF protection for cookie-authenticated mutating requests.
- `[x]` Add `api_keys`.
  - Fields: id, user_id, name, key_prefix, key_hash, scopes, last_used_at, created_at, expires_at, revoked_at.
  - Store only a hash of the key.
  - Show the full API key only once at creation.
  - Expiry options: 30 days, 90 days default, 180 days, 1 year, no expiry.
- `[~]` Add `chargers`.
  - Current fields: id/uuid, user_id, charge_point_id, display_name, created_at, deleted_at.
  - Metadata such as manufacturer, model, serial, firmware, status, connected/last-seen currently comes from per-charger snapshots/live registry.
  - Remaining: decide whether to denormalize metadata onto `chargers` or keep snapshots as source of truth.
- `[~]` Add `charger_claims`.
  - Fields: id, user_id, token_hash, generated_password_hash, expires_at, consumed_at, claimed_charger_id.
  - Generated OCPP endpoint/password are one-time display only; database stores hashes, status, and expiry metadata.
  - Current implementation creates/cancels pending onboarding sessions and displays the generated endpoint/password once.
  - Remaining: consume claims from websocket connections and bind chargers automatically.
- `[ ]` Add charger connection credentials.
  - Fields: charger_id, credential_prefix, credential_hash, created_at, rotated_at, revoked_at.
  - Used to authenticate future OCPP websocket reconnects.
- `[~]` Add per-charger state storage.
  - Current: `charger_state_snapshots` stores one JSON snapshot per charge point id, including meter latest values, transaction fields, configuration snapshot, firmware state, schedules, ID tags, action logs, and OCPP frame logs.
  - Remaining: split high-value/history-heavy data into relational tables where needed:
    - Meter history / data points.
    - Transactions / charging sessions.
    - Action/audit logs with actor/source metadata.
    - OCPP frame logs with retention.

## Architecture Refactor

- `[x]` Remove single-active command routing.
  - OCPP commands require explicit `charge_point_id`.
  - The OCPP server holds a per-charge-point session registry.
  - Duplicate sessions for the same charge point id are replaced deterministically.
- `[x]` Remove single-charger runtime config.
  - Removed `ADOPT_FIRST_CHARGER` and `EXPECTED_CHARGE_POINT_ID` from runtime and deployment config.
  - Chargers without a charge point identity are rejected.
- `[~]` Replace single global `OcppCoordinator` with a multi-charger runtime model.
  - Current: one coordinator owns a per-charge-point session registry and per-charger snapshots.
  - Current: `coordinator.data` remains as a selected/dashboard compatibility mirror, not as the command-routing source of truth.
  - Remaining: decide whether to keep this model or fully split into per-charger runtime objects.
- `[~]` Split state concerns.
  - `ChargerRuntimeState`: live in-memory connection/session values.
  - `ChargerRepository`: persisted charger/account data.
  - `ActionLogService`: portal/API/source-aware action log writes.
- `[x]` Make existing portal command calls charger-scoped.
  - Browser routes should load a selected charger from user context.
  - API routes should use `charger_uuid` path params and authorize ownership.
- `[~]` Make SSE per-user/per-charger.
  - Current: SSE connections are authenticated and each event recomputes state for the connected user.
  - Current: per-charger snapshot updates wake SSE clients.
  - Remaining: queue fan-out is still broad; optimize to targeted per-user/per-charger queues if needed.
- `[ ]` Add background cleanup jobs.
  - Expired claims.
  - Revoked/expired API keys.
  - Old logs/frames.
  - Stale sessions.
  - Temporary firmware download fragments.
- `[x]` Centralize firmware repository/cache.
  - Firmware downloads are shared by filename under the central firmware root.
  - Existing valid files are reused after checksum/size validation.
  - Corrupt/temp downloads are removed before replacement or after failure.
- `[x]` Add firmware progress lifecycle.
  - Transfer progress is shown in a modal.
  - Completed transfers transition to applying/restarting instead of remaining stuck at `Downloading`.
  - OCPP firmware status notifications and post-reboot firmware version are used to mark installed/failed.
  - Closing the modal suppresses reopening for the same transfer across refreshes.

## Authentication and Portal UI

- `[x]` Add registration page.
  - Email, password, confirm password.
  - Registration notice shows whether the next account will be `Admin` or `User`.
  - Optional email verification can be added later, but design schema for it.
- `[x]` Add login/logout.
  - Secure HTTP-only cookie session.
  - SameSite=Lax or Strict depending on deployment path.
- `[x]` Add account security controls.
  - Change password after current-password verification.
  - Enable/disable authenticator app OTP.
  - Login prompts for OTP only after a valid password when 2FA is enabled.
- `[x]` Add account page.
  - Profile.
  - Password change.
  - API key management.
- `[x]` Add admin page, visible only to users with role `Admin`.
  - Add `/admin` route and sidebar navigation entry only for admin users.
  - Protect all admin browser/API endpoints server-side with role checks, not only UI hiding.
- `[x]` Add admin unadopted charger management.
  - List all currently connected charge points that are not allocated to a user account.
  - Allow an admin to assign an unadopted charger to an account by searching for the account email.
  - Record the assignment as a portal action log event; actor-id audit metadata follows the audit service work.
- `[x]` Add admin account enable/disable controls.
  - Search for an account by email.
  - Show current enabled/disabled state.
  - Allow disable or enable of another account.
  - Do not allow the currently logged-in admin account to be disabled or otherwise edited through this tool.
  - Disabled accounts must not be able to log in, create sessions, use API keys, or control chargers.
- `[x]` Add admin 2FA reset controls.
  - Search for an account by email.
  - Allow an admin to clear another user's TOTP credentials and pending setup state.
  - Do not allow the currently logged-in admin account to reset its own 2FA through this tool.
  - After reset, the target user can log in with email and password and re-enable 2FA from their account page.
- `[~]` Add charger onboarding UI.
  - Create claim.
  - Show generated OCPP URL.
  - Show pending sessions.
  - Allow cancel/retry.
  - Remaining: expiry countdown and connected/claimed state transitions.
- `[x]` Add charger switcher.
  - Portal persists and switches a user's active charger.
  - Dashboard state follows the selected charger.
- `[x]` Gate existing portal actions behind ownership checks.

## API Key Management

- `[x]` Add UI to create API key.
  - Name.
  - Optional expiry.
  - Scope selection.
  - One-time display of generated token.
- `[x]` Add UI to revoke API key.
- `[x]` Add UI to show key prefix, last used timestamp, scopes, expiry.
- `[ ]` Add middleware for `Authorization: Bearer`.
  - Hash supplied key and compare safely.
  - Attach actor context: user id, scopes, source=`API`.
  - Update `last_used_at`.
- `[ ]` Add basic rate limiting.
  - Suggested initial global default: 300 requests/minute per API key, matching GivEnergy-style docs.
  - Add headers: `X-RateLimit-Limit`, `X-RateLimit-Remaining`.

## API Shape

Use `/v1` as the public API prefix. Keep existing `/api/...` portal routes as internal browser endpoints initially.

### Account / Auth

- `[ ]` `GET /v1/account`
  - Scope: `account:read`
  - Returns current actor/user profile.

### EV Chargers

- `[ ]` `GET /v1/ev-charger`
  - Scope: `ev-charger:read`
  - Return chargers owned by the authenticated user.
- `[ ]` `GET /v1/ev-charger/{charger_uuid}`
  - Scope: `ev-charger:read`
  - Return charger info and latest state.
- `[ ]` `GET /v1/ev-charger/{charger_uuid}/data-points`
  - Scope: `ev-charger:read`
  - Query params: date/range/page/pageSize as needed.
  - Return meter data in GivEnergy-like response envelope.
- `[ ]` `GET /v1/ev-charger/{charger_uuid}/commands`
  - Scope: `ev-charger:control`
  - Return supported commands.
- `[ ]` `GET /v1/ev-charger/{charger_uuid}/commands/{command}`
  - Scope: `ev-charger:control`
  - Return current command data, validation hints, current state.
- `[ ]` `POST /v1/ev-charger/{charger_uuid}/commands/{command}`
  - Scope: `ev-charger:control`
  - Send command to charger or local service.
  - Log response with source `API`.
- `[ ]` `GET /v1/ev-charger/{charger_uuid}/charging-sessions`
  - Scope: `ev-charger:read`
  - Return persisted transactions/sessions.

### Initial Command Mapping

- `[ ]` `start-charge`
  - Existing backend: `async_start_charging()`.
  - Must require charger connected and car plugged in.
- `[ ]` `stop-charge`
  - Existing backend: `async_stop_charging()`.
  - Must require active/open transaction id.
- `[ ]` `set-plug-and-go`
  - Existing backend: `async_set_plug_and_go(enabled)`.
- `[ ]` `set-session-energy-limit`
  - Existing backend: `async_set_max_energy_per_session(kwh)`.
- `[ ]` `change-mode`
  - Existing backend: `async_set_charge_mode(mode)`.
- `[ ]` `set-max-current`
  - Existing backend: `async_change_configuration(current_limit_key, value)`.
- `[ ]` `set-schedule`
  - Backend exists: `async_save_charging_schedule()`.
  - Enabled schedules are sent with `SetChargingProfile`; disabled schedules persist locally only.
- `[ ]` `set-active-schedule`
  - Backend exists: `async_set_charging_schedule_enabled()`.
  - Enabling one schedule disables the other portal schedules and sends the active profile.
  - Disabling/deleting an active schedule sends `ClearChargingProfile`.
- `[ ]` `unlock-connector`
  - Existing backend: `async_unlock_connector()`.
- `[ ]` `restart-charger`
  - Existing backend: `async_reset("Soft")`.
- `[ ]` `factory-reset`
  - Existing backend: `async_reset("Hard")`.
- `[ ]` `install-firmware`
  - Existing backend: `async_install_firmware_file(filename)`.
  - Central cache and modal lifecycle are implemented for portal use.

### RFID / Local Authorization Commands

- `[ ]` `list-id-tags`
  - Existing portal backend/state exists.
  - Public API endpoint not implemented.
- `[ ]` `create-or-update-id-tag`
  - Backend exists: `async_save_rfid_tag()`.
  - Sends `SendLocalList` differential update before persisting.
- `[ ]` `enable-disable-id-tag`
  - Backend exists: `async_set_rfid_tag_enabled()`.
  - Sends `SendLocalList` differential update before persisting.
- `[ ]` `delete-id-tag`
  - Backend exists: `async_delete_rfid_tag()`.
  - Sends remove entry via `SendLocalList` before persisting.

## Response and Error Contract

- `[ ]` Add response helpers.
  - Success: `{ "data": ... }`.
  - Validation error: `422` with `{ "message": "...", "errors": { ... } }`.
  - Auth error: `401`.
  - Authorization/ownership error: `403`.
  - Missing charger/resource: `404`.
  - Charger command rejected/offline: `400` or `503` depending on condition.
- `[ ]` Normalize command responses to GivEnergy-style data.
  - Include `code`, `success`, `message`, `data`, and optional `error`.
- `[x]` Preserve real OCPP response payloads in portal action logs.
  - Remaining: retain raw response separately from user-visible response when relational audit logs are added.

## Logging and Auditing

- `[~]` Extend action log source.
  - Current UI logs use `via = "Portal"`.
  - New API logs use `via = "API"`.
  - Future integrations can add more sources.
- `[ ]` Store actor id and API key id on action logs where applicable.
- `[~]` Keep user-visible log response clean, but retain raw response in persisted audit data.
  - Portal UI hides raw JSON response noise.
  - Remaining: separate raw audit payload from display response.
- `[ ]` Add audit events for login, logout, API key create/revoke, charger claim create/consume/fail.

## Migration Strategy

- `[x]` Add SQLite alongside current JSON persistence.
- `[~]` On first startup with no database:
  - Create schema.
  - Existing `/data/state.json` is imported into `coordinator_state`.
  - Remaining: migrate old single-charger state into a real charger/account record when upgrading existing deployments.
- `[ ]` Keep a backup copy of migrated JSON.
  - Current import removes the legacy JSON after saving to SQLite.
- `[x]` Add setup wizard for first admin user.
  - First registration becomes `Admin`; if no users exist, registration is shown directly.
- `[x]` Remove direct writes to `/data/state.json`.
  - Runtime coordinator persistence now writes through `AuthStore` / SQLite.
- `[x]` Home Assistant add-on persistence.
  - HA add-on mode maps `addon_config:rw`, mounted at `/config`.
  - HA add-on mode uses `/config` for SQLite, auth secret, legacy state import, and firmware cache.
  - Native Docker remains on `/data`.
  - Startup copies legacy `/data/auth.db`, `/data/auth_secret.key`, `/data/state.json`, and `/data/firmware` into `/config` when `/config` is empty.

## Test Plan

- `[ ]` Unit tests for password hashing and API key hashing.
- `[ ]` Unit tests for ownership checks.
- `[ ]` Unit tests for claim-token lifecycle.
- `[ ]` Unit tests for charger registry routing.
- `[ ]` Unit tests for API response envelopes and error codes.
- `[ ]` Integration tests for:
  - User registration/login/logout.
  - API key creation/revocation.
  - Claim charger flow.
  - OCPP connection binding to claimed charger.
  - API start/stop commands.
  - API logs using `via = "API"`.
- `[ ]` Regression tests for existing portal settings and charge button behavior.

## Suggested Implementation Phases

### Phase 1: Persistence Foundation

- `[x]` Add SQLite dependency and database module.
- `[x]` Add schema migrations.
- `[~]` Add repositories for users, chargers, API keys, claims, logs.
  - Users, sessions, chargers, onboarding sessions, API keys, coordinator state, and charger snapshots exist.
  - Remaining: relational audit/action log repository and historical meter/session repositories.
- `[~]` Add JSON-to-SQLite migration.
  - Coordinator state import exists.
  - Remaining: full one-time migration into user/charger ownership records for legacy deployments.
- `[ ]` Add an explicit upgrade path for existing single-user installations.
  - Create first admin/setup flow.
  - Adopt/migrate the existing connected charger into that account.
  - Preserve old schedules, tags, settings, energy totals, logs, and firmware selection.

Acceptance:

- Existing app starts with migrated state.
- Existing portal still controls the current charger.
- State survives restarts from SQLite.

### Phase 2: Browser Authentication

- `[x]` Add registration/setup flow.
- `[x]` Add login/logout/session middleware.
- `[x]` Protect portal routes and `/api/...` browser endpoints.
- `[x]` Add charger ownership checks to existing portal APIs.
- `[x]` Add admin-only route/API guard helper.
- `[~]` Add disabled-account enforcement in login, session, and API-key authentication.
  - Browser login/session enforcement exists.
  - Remaining: API-key authentication is not implemented yet.

Acceptance:

- Anonymous users cannot see or control charger state.
- Logged-in user can control only their charger.
- Disabled users cannot create or use sessions.

### Phase 2A: Admin Portal Framework

- `[x]` Add admin sidebar item and routed `/admin` page for users with role `Admin`.
- `[x]` Add admin account search by email endpoint.
- `[x]` Add admin enable/disable account endpoint and UI.
- `[x]` Add admin reset 2FA endpoint and UI.
- `[x]` Add unadopted connected charger listing endpoint and UI.
- `[x]` Add assign unadopted charger to account endpoint and UI.
- `[~]` Add audit logs for admin account changes, 2FA resets, and charger assignment.

Acceptance:

- Non-admin users cannot see or call admin features.
- Admin users can find another account by email and enable/disable it.
- Admin users can reset another account's 2FA.
- Admin users cannot disable, enable, or reset 2FA for their own currently logged-in account through the admin tools.
- Admin users can view connected unadopted charge points and assign one to a user account by email.

### Phase 3: Charger Onboarding

- `[x]` Add claim-token creation endpoint and UI.
- `[x]` Add pending onboarding cancellation UI.
- `[x]` Add linked charger delete endpoint and UI.
- `[ ]` Add OCPP claim endpoint or token-aware websocket path.
- `[ ]` Bind first valid charger connection to the claim owner.
- `[ ]` Prevent duplicate/random claims.
- `[ ]` Add claim audit logs.

Acceptance:

- A new user can onboard a charger without admin database edits.
- A random charger cannot overwrite another user's existing charger.

### Phase 4: Multi-Charger Runtime

- `[x]` Add per-charge-point OCPP session registry.
- `[x]` Route outbound OCPP commands by explicit charge point id.
- `[x]` Remove implicit active-session command fallback.
- `[x]` Remove single-charger acceptance config.
- `[x]` Route OCPP sessions to adopted/unadopted state.
- `[~]` Refactor current coordinator to be per-charger.
  - Current architecture supports many chargers in one coordinator with per-charger snapshots.
  - Remaining optional cleanup: split into `ChargerRuntimeState` objects/repositories if needed.
- `[~]` Make UI state/SSE charger-scoped.
  - Current: authenticated SSE recomputes state per user and per active charger.
  - Remaining: targeted queue fan-out rather than broadcast-and-recompute.
- `[x]` Add charger switcher.

Acceptance:

- Two users/chargers can connect concurrently without state bleeding.
- User A cannot receive User B's SSE or command responses.

### Phase 4A: Portal OCPP Feature Wiring

- `[x]` Wire schedule management to OCPP.
  - Save/enable sends `SetChargingProfile`.
  - Disable/delete active schedule sends `ClearChargingProfile`.
  - Local schedule state persists only after accepted OCPP response.
- `[x]` Wire RFID/local authorization management to OCPP.
  - Add/update/enable/disable/delete uses `GetLocalListVersion` and `SendLocalList`.
  - Local ID tag state persists only after accepted OCPP response.
- `[x]` Improve firmware update lifecycle.
  - Central firmware cache is used.
  - Transfer modal follows transfer, apply, restart, and installed/failed state.
  - Reconnected firmware version is checked against the requested target.
- `[~]` Add user-facing reconciliation for failed writes.
  - Current: failed OCPP writes prevent local persistence and surface an error.
  - Remaining: richer per-item status/history for schedules, tags, and firmware attempts.

### Phase 5: API Keys

- `[x]` Add API key management UI.
- `[ ]` Add bearer auth middleware.
- `[ ]` Add scopes and rate limiting.
- `[ ]` Add API audit logging.

Acceptance:

- User can create/revoke an API key.
- Revoked keys stop working.
- API actions log as `via = "API"`.

### Phase 6: GivEnergy-Style Public API

- `[ ]` Implement `/v1/account`.
- `[ ]` Implement EV charger list/detail endpoints.
- `[ ]` Implement data-points and charging-sessions endpoints.
- `[ ]` Implement commands list/read/send endpoints.
- `[ ]` Add OpenAPI document for this portal API.

Acceptance:

- Basic API client can list chargers, read state, and send start/stop/change-mode commands.
- Responses use GivEnergy-style envelopes.

## Remaining Work Summary

Highest priority:

- `[ ]` Implement secure self-service charger claim consumption.
  - Validate generated onboarding password/token during websocket upgrade.
  - Bind the first valid charger connection to the claim owner.
  - Prevent random/unowned chargers from self-assigning to accounts.
  - Add claim consumed/failed audit events.
- `[ ]` Add persistent charger connection credentials.
  - Store only hashes.
  - Authenticate future OCPP reconnects after adoption.
  - Add rotation/revocation flow.
- `[ ]` Implement public API bearer authentication.
  - Validate `Authorization: Bearer`.
  - Enforce read/write scopes.
  - Reject disabled users and revoked/expired keys.
  - Update `last_used_at`.
- `[ ]` Implement `/v1` GivEnergy-style API endpoints and response envelopes.
- `[ ]` Add API rate limiting and rate-limit headers.
- `[ ]` Add CSRF protection for cookie-authenticated portal mutations.

Portal/OCPP feature follow-up:

- `[ ]` Expose schedule management through `/v1` API commands.
- `[ ]` Expose RFID/local authorization management through `/v1` API commands.
- `[ ]` Add explicit reconciliation/status display for schedules and RFID tags when an OCPP write fails.
- `[ ]` Decide whether firmware update records should be retained as history beyond the current per-charger snapshot.

Data and audit cleanup:

- `[ ]` Split historical/high-volume per-charger data out of JSON snapshots.
  - Meter history/data points.
  - Charging sessions/transactions.
  - Action logs with actor id/source/API key id.
  - OCPP frame logs with retention.
- `[ ]` Add background cleanup jobs.
  - Expired onboarding claims.
  - Expired/revoked API keys if physical cleanup is desired.
  - Old action logs/OCPP frames.
  - Stale sessions.
  - Temporary firmware download fragments.
- `[ ]` Complete legacy upgrade flow from old single-user installs.
  - Create/require first admin.
  - Bind existing charger to that admin.
  - Preserve existing state into per-charger/account records.
  - Keep a backup copy of imported JSON rather than deleting it.

Quality and hardening:

- `[ ]` Add automated tests for auth, ownership, onboarding, routing, public API envelopes, and portal regressions.
- `[ ]` Decide whether the current one-coordinator/per-charger-snapshot model is sufficient, or whether to fully split into per-charger runtime objects.
- `[ ]` Add OpenAPI documentation for `/v1`.

## Open Decisions

- `[!]` Should registration be open to the internet, invite-only, or first-admin-only plus invites?
- `[!]` Should charger onboarding require physical proof, admin approval, or token-only claim?
- `[!]` Should one user own many chargers from day one, or one charger first with schema allowing many?
- `[!]` Should API key scopes mimic GivEnergy names exactly, for example `api:ev-charger:read`, or use shorter internal names?
- `[!]` Should the public API intentionally match GivEnergy endpoint paths exactly where possible, or only response/command semantics?
- `[!]` What is the expected deployment mode behind proxy: same domain for portal and OCPP websocket, or separate hostnames?

## Resume Notes

When resuming:

1. Open this file first.
2. Pick the first incomplete task in the current phase.
3. Update its status to `[~]` before editing.
4. Implement and verify.
5. Update completed tasks to `[x]`.
6. Add any discovered follow-up tasks under the relevant phase.
