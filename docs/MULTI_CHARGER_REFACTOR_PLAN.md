# Multi-Charger Coordinator Refactor Plan

**Branch:** `multi-charger-coordinator`  
**Status:** Not started — plan only

## Problem

The coordinator has a single global `self.data: ChargerState` slot. Only the one charger that occupies this slot gets full feature support (live SSE, firmware updates, OCPP commands, transaction tracking). Every other user's charger is classified as "passive" and loses these features. This is incorrect for a multi-tenant system where each user should be able to use all features with their own charger.

## Target Model

- `_charger_states: dict[str, ChargerState]` — one live entry per adopted charger, keyed by charge_point_id
- Every adopted charger that connects gets full lifecycle: SSE updates, firmware, commands
- `self.data` becomes a property shim returning `_charger_states[_primary_charge_point_id]`
- `_primary_charge_point_id: str | None` — coordinator's "default" charger for legacy fallbacks
- `_live_firmware_state` removed — firmware state lives directly in `_charger_states[cpid]`
- `_is_selected_charge_point` removed — all routing by explicit charge_point_id
- server.py: all adopted chargers go through `async_connection_opened` (no more passive branch)
- `_state_for_user` in main.py: simplified to `coordinator.state_for_charge_point(cpid)`

## Implementation Steps

### Step 1 — `coordinator.py` `__init__` restructure
- Add `self._charger_states: dict[str, ChargerState] = {}`
- Add `self._primary_charge_point_id: str | None = None`
- Remove `self.data = ChargerState()` bare assignment
- Remove `self._live_firmware_state: dict[str, ChargerState] = {}`
- Add `self.data` as a property:
  ```python
  @property
  def data(self) -> ChargerState:
      if self._primary_charge_point_id and self._primary_charge_point_id in self._charger_states:
          return self._charger_states[self._primary_charge_point_id]
      if not hasattr(self, "_null_state"):
          self._null_state = ChargerState()
      return self._null_state
  ```

### Step 2 — `state_for_charge_point` rewrite
```python
def state_for_charge_point(self, charge_point_id: str | None) -> ChargerState:
    cpid = str(charge_point_id or "").strip()
    if cpid and cpid in self._charger_states:
        return self._charger_states[cpid]
    state = _state_from_snapshot(self.charger_snapshot_for(cpid) or {})
    if cpid:
        state.charge_point_id = cpid
    return state
```
Remove `_is_selected_charge_point` delegation.

### Step 3 — `_persist_charge_point_state` rewrite
Remove dual-branch (active vs passive). Always save to `charger_state_snapshots` via `_save_charger_snapshot`. Remove `_save()` calls.

### Step 4 — `_notify` update
Accept optional `charge_point_id` param. `_push_sse()` + persist specific charger if `persist=True`.

### Step 5 — `async_connection_opened` rewrite
Seed `_charger_states[cpid]` from DB snapshot if not present. Set connected fields on state. Set `_primary_charge_point_id = cpid` if not set. **Remove `async_passive_connection_opened`.**

### Step 6 — `async_connection_closed` rewrite
Use `removed.get("charge_point_id")` to target the correct `_charger_states` entry. **Remove `async_passive_connection_closed`.**

### Step 7 — Collapse all dual-branch inbound OCPP handlers
Each has an `if not _is_selected_charge_point` passive branch and an active branch. Collapse to single path using `state_for_charge_point`:
- `async_record_boot` (most complex — 3 sub-paths in passive branch)
- `async_record_heartbeat`
- `async_record_status`
- `async_start_transaction_from_charger`
- `async_stop_transaction_from_charger`
- `async_record_meter_values`
- `async_record_firmware_status`

Pattern for each:
```python
state = self.state_for_charge_point(charge_point_id)
# ... mutate state fields ...
self._persist_charge_point_state(charge_point_id, state, persist=True)
```

### Step 8 — Remove `_live_firmware_state` entirely
- `__init__`: remove declaration
- `charger_snapshot_for`: remove the `_live_firmware_state.get()` branch
- `async_record_boot`: remove `_live_firmware_state` references
- `_start_firmware_update_session`: remove `if not _is_selected: _live_firmware_state[cpid] = state`
- `record_firmware_transfer_event`: remove `_live_firmware_state.pop()`

### Step 9 — `_save_active_charger_snapshot` rewrite
Read from `_charger_states[cpid]` instead of `self.data`.

### Step 10 — `_charge_point_id_for_firmware_event` rewrite
Iterate `_charger_states` instead of `self.data` references.

### Step 11 — `load()` rewrite with migration
```python
def load(self):
    # 1. Load legacy coordinator_state single row → migrate to charger_state_snapshots
    raw = self._state_store.load_coordinator_state(self._state_path)
    if raw:
        cpid = str(raw.get("charge_point_id") or "").strip()
        if cpid:
            state = ChargerState()
            for field in _PERSIST_FIELDS:
                if field in raw:
                    setattr(state, field, raw[field])
            self._charger_states[cpid] = state
            self._primary_charge_point_id = cpid
            self._save_charger_snapshot(cpid, _state_to_dict(state))
    # 2. Load all charger_state_snapshots rows
    for cpid in self._state_store.list_all_adopted_charge_point_ids():
        if cpid not in self._charger_states:
            snapshot = self._state_store.load_charger_state(cpid)
            if snapshot:
                state = _state_from_snapshot(snapshot)
                state.charge_point_id = cpid
                self._charger_states[cpid] = state
```
Remove `_save()` — replaced by `_save_charger_snapshot` calls.

### Step 12 — Remaining `self.data` direct mutations
Replace `self.data.charge_point_id` fallbacks in helper methods with `self._primary_charge_point_id`:
- `_safe_reset`
- `async_save_charging_schedule`
- `async_dst_correction`
- `async_force_repush_all_schedules`

### Step 13 — `async_select_active_charge_point` rewrite
Set `_primary_charge_point_id`. Seed `_charger_states[cpid]` from snapshot if not present.

### Step 14 — `server.py` simplification
```python
# Before:
selected_active = bool(adopted and candidate_id and candidate_id == active_charge_point_id)

# After:
selected_active = adopted  # every adopted charger gets full lifecycle
```
Remove the `elif stateful:` → `async_passive_connection_opened` branch.  
Close dispatch: remove the `candidate_id == self.coordinator.data.charge_point_id` guard — just `if session.stateful: await coordinator.async_connection_closed(...)`.

### Step 15 — `main.py` `_state_for_user` simplification
Remove the 3-branch logic (active coordinator charger / snapshot-from-DB / disconnected).  
Single path:
```python
def _state_for_user(user: AuthUser) -> dict:
    active = _active_charger_for_user(user.id)
    if active is None:
        return _disconnected_state_for_charger()
    selected_charge_point_id = str(active.get("charge_point_id") or "")
    live_connection = _live_connection_for_charge_point(selected_charge_point_id)
    session_stats = auth_store.get_session_stats(selected_charge_point_id) if selected_charge_point_id else {}
    charger_state = coordinator.state_for_charge_point(selected_charge_point_id)
    state = _state_to_dict(charger_state)
    state["charge_point_id"] = selected_charge_point_id
    state.update(_merge_live_session_into_stats(state, session_stats))
    return _apply_live_connection_to_state(state, live_connection)
```

### Step 16 — `auth_store.py` new method
Add `list_all_adopted_charge_point_ids() -> list[str]`:
```python
def list_all_adopted_charge_point_ids(self) -> list[str]:
    with self._connect() as conn:
        rows = conn.execute(
            "SELECT DISTINCT charge_point_id FROM charger_state_snapshots WHERE charge_point_id IS NOT NULL"
        ).fetchall()
    return [row["charge_point_id"] for row in rows]
```

## What NOT to Change

- `_ocpp_callers` dict — already per-charger, works correctly
- `_connected_charge_points` dict — already per-charger
- `charge_point_can_receive_commands` authorizer callback
- `demo_simulator.py` — connects via OCPP WebSocket, no direct `coordinator.data` access
- Public method signatures on `OcppCoordinator`

## Key Risk Areas

1. **`async_record_boot` collapse** — most complex method; test with both primary and non-primary charger
2. **`async_connection_closed`** — must target correct dict entry via session's charge_point_id, not blindly clear self.data
3. **`self.data` shim** — log warning if accessed when `_primary_charge_point_id` is None
4. **`charger_snapshot_for` after `_live_firmware_state` removal** — callers must use `state_for_charge_point` for live chargers
5. **`_push_sse` payload** — main.py SSE loop discards queue value; changing payload content is safe

## DB Migration

No schema changes needed. `charger_state_snapshots` already exists. Legacy `coordinator_state` single row is read once at startup and migrated; the table is not deleted (kept for one release as rollback option).
