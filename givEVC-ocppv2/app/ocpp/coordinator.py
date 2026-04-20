"""Standalone OCPP coordinator — no Home Assistant dependency."""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import itertools
import json
import logging
import re
import socket
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import aiohttp

from .state import ChargerState

_LOGGER = logging.getLogger(__name__)

CP_READING_PATTERN = re.compile(
    r"CP_Voltage:(?P<voltage>\d+(?:\.\d+)?)V,CP_Duty:(?P<duty>\d+(?:\.\d+)?)%"
)

MAX_STORED_OCPP_FRAMES = 500
MAX_STORED_ACTION_LOGS = 500
DEFAULT_REMOTE_ID_TAG = "HA-REMOTE"
DEFAULT_EVSE_MAX_CURRENT = 32.0
FIRMWARE_INSTALLING_TIMEOUT = timedelta(minutes=10)
DST_SCHEDULE_PUSH_CONCURRENCY = 10
_tx_counter = itertools.count(1)
CHARGE_START_STATUSES = {"Available", "Preparing"}
CHARGE_STOP_STATUSES = {"Charging", "SuspendedEVSE", "SuspendedEV"}
CHARGE_DISABLED_STATUSES = {"Finishing", "Reserved", "Unavailable", "Faulted"}

# Fields written to disk on every meaningful state change
_PERSIST_FIELDS = (
    "charge_point_id",
    "manufacturer",
    "model",
    "firmware_version",
    "firmware_status",
    "firmware_update_state",
    "firmware_update_target_file",
    "firmware_update_target_version",
    "firmware_update_previous_version",
    "firmware_update_started_at",
    "firmware_update_download_completed_at",
    "firmware_update_install_started_at",
    "firmware_update_expected_reconnect_by",
    "firmware_update_completed_at",
    "firmware_update_failure_reason",
    "charge_point_serial_number",
    "charge_box_serial_number",
    "last_boot_notification",
    "total_energy_kwh",
    "session_energy_kwh",
    "transaction_id",
    "transaction_id_tag",
    "transaction_meter_start_wh",
    "transaction_started_at",
    "transaction_ended_at",
    "rfid_tags",
    "charging_schedule",
    "action_log",
    "plug_and_go_enabled",
    "charge_mode",
    "local_modbus_enabled",
    "front_panel_leds_enabled",
    "current_limit_a",
    "max_energy_per_session_kwh",
    "selected_firmware_file",
)


def _assert_safe_url(url: str) -> None:
    """Raise ValueError if the URL resolves to a private or loopback address."""
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"Firmware URL must use http or https, got: {parsed.scheme!r}")
    hostname = parsed.hostname
    if not hostname:
        raise ValueError("Firmware URL has no hostname")
    try:
        infos = socket.getaddrinfo(hostname, None)
    except OSError as err:
        raise ValueError(f"Cannot resolve firmware URL hostname {hostname!r}: {err}") from err
    for _family, _type, _proto, _canonname, sockaddr in infos:
        addr = ipaddress.ip_address(sockaddr[0])
        if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved:
            raise ValueError(
                f"Firmware URL {url!r} resolves to a non-routable address ({addr}) — blocked to prevent SSRF"
            )


class OcppCoordinator:
    """Manage charger state, persistence, and SSE fan-out."""

    def __init__(
        self,
        listen_port: int,
        state_path: Path | None = None,
        state_store: Any | None = None,
        firmware_directory: Path | None = None,
        firmware_server_port: int = 9688,
        firmware_public_host: str | None = None,
        firmware_public_port: int | None = None,
        firmware_manifest_url: str | None = None,
        debug_logging: bool = False,
    ) -> None:
        self.listen_port = listen_port
        self._state_path = state_path
        self._state_store = state_store
        self.firmware_directory = firmware_directory
        self.firmware_server_port = firmware_server_port
        self.firmware_public_host = str(firmware_public_host).strip() if firmware_public_host else None
        self.firmware_public_port = firmware_public_port or firmware_server_port
        self.firmware_manifest_url = firmware_manifest_url
        self.debug_logging = debug_logging
        self._charger_states: dict[str, ChargerState] = {}
        self._primary_charge_point_id: str | None = None
        self._connected_charge_points: dict[str, dict[str, Any]] = {}
        self._sse_queues: list[asyncio.Queue] = []
        self._ocpp_callers: dict[str, Any] = {}
        self._charge_point_command_authorizer: Callable[[str | None], bool] | None = None
        self._firmware_download_locks: dict[str, asyncio.Lock] = {}
        self._active_firmware_charge_point_id: str | None = None

    @property
    def data(self) -> ChargerState:
        if self._primary_charge_point_id and self._primary_charge_point_id in self._charger_states:
            return self._charger_states[self._primary_charge_point_id]
        if not hasattr(self, "_null_state"):
            self._null_state = ChargerState()
        return self._null_state

    # ── Persistence ──────────────────────────────────────────────────────

    def load(self) -> None:
        """Restore persisted state at startup. Migrates legacy coordinator_state if present."""
        if self._state_store is None:
            return

        # 1. Migrate legacy single-row coordinator_state → charger_state_snapshots
        raw: dict[str, Any] | None = None
        try:
            raw = self._state_store.load_coordinator_state(self._state_path)
        except Exception:
            _LOGGER.exception("Failed to load legacy coordinator state")

        if raw:
            cpid = str(raw.get("charge_point_id") or "").strip()
            if cpid:
                try:
                    state = ChargerState()
                    for field in _PERSIST_FIELDS:
                        if field not in raw:
                            continue
                        value = raw[field]
                        if field in ("transaction_started_at", "transaction_ended_at") and value:
                            value = datetime.fromisoformat(value)
                        setattr(state, field, value)
                    state.charging_schedule = _normalise_schedule_list(state.charging_schedule)
                    state.rfid_tags = _normalise_rfid_tag_list(state.rfid_tags)
                    state.transaction_active = _is_charging_status(state.status)
                    state.charge_point_id = cpid
                    self._charger_states[cpid] = state
                    if not self._primary_charge_point_id:
                        self._primary_charge_point_id = cpid
                    self._save_charger_snapshot(cpid, _state_to_dict(state))
                    _LOGGER.info("Migrated legacy coordinator state for %s", cpid)
                except Exception:
                    _LOGGER.exception("Failed to migrate legacy coordinator state")

        # 2. Load all charger_state_snapshots rows
        lister = getattr(self._state_store, "list_all_adopted_charge_point_ids", None)
        if lister:
            try:
                for cpid in lister():
                    if cpid not in self._charger_states:
                        loader = getattr(self._state_store, "load_charger_state", None)
                        if loader:
                            snapshot = loader(cpid)
                            if snapshot:
                                state = _state_from_snapshot(snapshot)
                                state.charge_point_id = cpid
                                self._charger_states[cpid] = state
                if self._charger_states and not self._primary_charge_point_id:
                    self._primary_charge_point_id = next(iter(self._charger_states))
                _LOGGER.info("Loaded %d charger state(s) from snapshots", len(self._charger_states))
            except Exception:
                _LOGGER.exception("Failed to load charger state snapshots")

    def _save(self) -> None:
        """Legacy save — no-op; state is now written per-charger via _save_charger_snapshot."""

    # ── SSE fan-out ──────────────────────────────────────────────────────

    def add_sse_queue(self, q: asyncio.Queue) -> None:
        self._sse_queues.append(q)

    def remove_sse_queue(self, q: asyncio.Queue) -> None:
        try:
            self._sse_queues.remove(q)
        except ValueError:
            pass

    def _push_sse(self) -> None:
        """Wake all SSE clients; each re-fetches their own charger state."""
        for q in list(self._sse_queues):
            try:
                q.put_nowait("wake")
            except asyncio.QueueFull:
                pass

    def _notify(self, persist: bool = False, charge_point_id: str | None = None) -> None:
        self._push_sse()
        if persist:
            cpid = str(charge_point_id or self._primary_charge_point_id or "").strip()
            if cpid and cpid in self._charger_states:
                self._save_active_charger_snapshot(cpid)

    def charger_snapshot_for(self, charge_point_id: str | None) -> dict[str, Any] | None:
        charge_point_id = str(charge_point_id or "").strip()
        if not charge_point_id:
            return None
        # Return in-memory state if live (includes in-progress firmware transfer data)
        if charge_point_id in self._charger_states:
            snapshot = _state_to_dict(self._charger_states[charge_point_id])
            snapshot["charge_point_id"] = charge_point_id
            return snapshot
        if self._state_store is None:
            return None
        loader = getattr(self._state_store, "load_charger_state", None)
        if loader is None:
            return None
        return loader(charge_point_id)

    def state_for_charge_point(self, charge_point_id: str | None) -> ChargerState:
        cpid = str(charge_point_id or "").strip()
        if cpid and cpid in self._charger_states:
            return self._charger_states[cpid]
        state = _state_from_snapshot(self.charger_snapshot_for(cpid) or {})
        if cpid:
            state.charge_point_id = cpid
        return state

    def _persist_charge_point_state(
        self,
        charge_point_id: str | None,
        state: ChargerState,
        *,
        persist: bool = True,
        notify: bool = True,
    ) -> None:
        charge_point_id = str(charge_point_id or state.charge_point_id or "").strip()
        if persist and charge_point_id:
            snapshot = _state_to_dict(state)
            snapshot["charge_point_id"] = charge_point_id
            self._save_charger_snapshot(charge_point_id, snapshot)
        if notify:
            self._push_sse()

    def command_session_available(self, charge_point_id: str | None) -> bool:
        charge_point_id = str(charge_point_id or "").strip()
        caller = self._ocpp_callers.get(charge_point_id)
        websocket = getattr(caller, "websocket", None)
        return bool(caller is not None and not getattr(websocket, "closed", False))

    def _save_active_charger_snapshot(self, charge_point_id: str | None = None) -> None:
        cpid = str(charge_point_id or self._primary_charge_point_id or "").strip()
        if not cpid:
            return
        state = self._charger_states.get(cpid)
        if state is None:
            return
        snapshot = _state_to_dict(state)
        snapshot["charge_point_id"] = cpid
        self._save_charger_snapshot(cpid, snapshot)

    def _save_connected_charge_point_snapshot(self, session_id: str | None) -> None:
        if not session_id:
            return
        item = self._connected_charge_points.get(session_id)
        if not item:
            return
        charge_point_id = str(item.get("charge_point_id") or "").strip()
        if not charge_point_id:
            return
        snapshot = self.charger_snapshot_for(charge_point_id) or {}
        status = item.get("status")
        snapshot.update(
            {
                "connected": True,
                "connection_state": item.get("connection_state") or "connected",
                "charge_point_id": charge_point_id,
                "manufacturer": item.get("manufacturer"),
                "model": item.get("model"),
                "firmware_version": item.get("firmware"),
                "charge_point_serial_number": item.get("charge_point_serial_number")
                or item.get("serial"),
                "charge_box_serial_number": item.get("charge_box_serial_number"),
                "websocket_remote_address": item.get("remote_address"),
                "local_ip_address": item.get("local_ip_address") or item.get("remote_address"),
                "firmware_server_host": item.get("firmware_server_host") or item.get("local_ip_address"),
                "status": status,
                "error_code": item.get("error_code"),
                "vendor_error_code": item.get("vendor_error_code"),
                "car_plugged_in": _is_car_plugged_in_status(status),
                "last_seen": item.get("last_seen"),
            }
        )
        self._save_charger_snapshot(charge_point_id, snapshot)
        self._push_sse()

    def _save_charger_snapshot(self, charge_point_id: str, snapshot: dict[str, Any]) -> None:
        if self._state_store is None:
            return
        saver = getattr(self._state_store, "save_charger_state", None)
        if saver is None:
            return
        try:
            snapshot_to_save = dict(snapshot)
            snapshot_to_save.pop("firmware_transfer_progress", None)
            saver(charge_point_id, snapshot_to_save)
        except Exception:
            _LOGGER.exception("Failed to persist charger snapshot for %s", charge_point_id)

    # ── Connection lifecycle ─────────────────────────────────────────────

    def connected_charge_points(self) -> list[dict[str, Any]]:
        items = [dict(item) for item in self._connected_charge_points.values()]
        active_snapshot = self._active_state_charge_point()
        if active_snapshot:
            matched = False
            for item in items:
                if (
                    item.get("charge_point_id") == active_snapshot.get("charge_point_id")
                    or item.get("active")
                ):
                    item.update({key: value for key, value in active_snapshot.items() if value is not None})
                    matched = True
                    break
            if not matched:
                items.append(active_snapshot)
        return sorted(
            items,
            key=lambda value: str(value.get("last_seen") or ""),
            reverse=True,
        )

    def _active_state_charge_point(self) -> dict[str, Any] | None:
        if not self.data.connected or not self.data.charge_point_id:
            return None
        last_seen = self.data.last_seen.isoformat() if isinstance(self.data.last_seen, datetime) else self.data.last_seen
        return {
            "session_id": "active-state",
            "charge_point_id": self.data.charge_point_id,
            "manufacturer": self.data.manufacturer,
            "model": self.data.model,
            "firmware": self.data.firmware_version,
            "serial": self.data.charge_point_serial_number or self.data.charge_box_serial_number,
            "charge_point_serial_number": self.data.charge_point_serial_number,
            "charge_box_serial_number": self.data.charge_box_serial_number,
            "local_ip_address": self.data.local_ip_address,
            "remote_address": self.data.websocket_remote_address,
            "connection_state": self.data.connection_state,
            "status": self.data.status,
            "error_code": self.data.error_code,
            "vendor_error_code": self.data.vendor_error_code,
            "active": True,
            "last_seen": last_seen,
        }

    async def async_select_active_charge_point(self, charge_point_id: str | None) -> None:
        """Promote the given charger as the primary charger for legacy self.data access."""
        cpid = str(charge_point_id or "").strip() or None
        self._primary_charge_point_id = cpid

        for item in self._connected_charge_points.values():
            item["active"] = bool(cpid and item.get("charge_point_id") == cpid)

        if not cpid:
            _LOGGER.info("Cleared primary charger selection")
            self._notify(persist=True)
            return

        # Seed live state from DB snapshot if not already tracked
        if cpid not in self._charger_states:
            snapshot = self.charger_snapshot_for(cpid)
            state = _state_from_snapshot(snapshot or {})
            state.charge_point_id = cpid
            self._charger_states[cpid] = state

        # Update connection fields from current connected_charge_points entry if present
        selected = next(
            (item for item in self._connected_charge_points.values() if item.get("charge_point_id") == cpid),
            None,
        )
        state = self._charger_states[cpid]
        if selected:
            state.connected = True
            state.connection_state = str(selected.get("connection_state") or "connected")
            state.manufacturer = selected.get("manufacturer") or state.manufacturer
            state.model = selected.get("model") or state.model
            state.firmware_version = selected.get("firmware") or state.firmware_version
            state.charge_point_serial_number = selected.get("charge_point_serial_number") or selected.get("serial") or state.charge_point_serial_number
            state.charge_box_serial_number = selected.get("charge_box_serial_number") or state.charge_box_serial_number
            state.websocket_remote_address = selected.get("remote_address") or state.websocket_remote_address
            state.local_ip_address = selected.get("local_ip_address") or selected.get("remote_address") or state.local_ip_address
            state.status = selected.get("status") or state.status
            state.error_code = selected.get("error_code")
            state.vendor_error_code = selected.get("vendor_error_code")
            state.car_plugged_in = _is_car_plugged_in_status(state.status)
            state.transaction_active = _is_charging_status(state.status)
            state.last_seen = _parse_ocpp_ts(selected.get("last_seen")) or datetime.now(UTC)

        _LOGGER.info("Selected active charger: %s", cpid)
        self._notify(persist=True, charge_point_id=cpid)

    async def async_note_rejected_charge_point(self, candidate_id: str | None) -> None:
        _LOGGER.warning("Rejected charger connection: %s", candidate_id)

    async def async_connection_opened(
        self,
        session_id: str,
        charge_point_id: str | None,
        local_host: str | None,
        remote_host: str | None,
        firmware_server_host: str | None = None,
    ) -> None:
        now = datetime.now(UTC)
        cpid = str(charge_point_id or "").strip()
        advertised_firmware_host = self.resolve_firmware_server_host(firmware_server_host, local_host)

        # Seed state from DB snapshot if not already live
        if cpid and cpid not in self._charger_states:
            snapshot = self.charger_snapshot_for(cpid)
            state = _state_from_snapshot(snapshot or {})
            state.charge_point_id = cpid
            self._charger_states[cpid] = state

        if cpid and cpid in self._charger_states:
            state = self._charger_states[cpid]
            state.connected = True
            state.connection_state = "connected"
            state.charge_point_id = cpid
            if advertised_firmware_host:
                state.firmware_server_host = advertised_firmware_host
            state.websocket_remote_address = remote_host or None
            state.local_ip_address = remote_host or None
            state.last_seen = now

        if not self._primary_charge_point_id:
            self._primary_charge_point_id = cpid or None

        self._record_connected_charge_point(
            session_id,
            charge_point_id,
            local_host=local_host,
            remote_host=remote_host,
            firmware_server_host=advertised_firmware_host,
            active=True,
            last_seen=now,
        )
        _LOGGER.info("Charger connected: %s from %s", charge_point_id, remote_host)
        self._save_connected_charge_point_snapshot(session_id)
        self._notify()

    async def async_unmanaged_connection_opened(
        self,
        session_id: str,
        charge_point_id: str | None,
        local_host: str | None,
        remote_host: str | None,
        firmware_server_host: str | None = None,
    ) -> None:
        now = datetime.now(UTC)
        self._record_connected_charge_point(
            session_id,
            charge_point_id,
            local_host=local_host,
            remote_host=remote_host,
            firmware_server_host=self.resolve_firmware_server_host(firmware_server_host, local_host),
            active=False,
            last_seen=now,
        )
        _LOGGER.info("Unadopted charger connected: %s from %s", charge_point_id, remote_host)
        self._save_connected_charge_point_snapshot(session_id)

    async def async_passive_connection_opened(
        self,
        session_id: str,
        charge_point_id: str | None,
        local_host: str | None,
        remote_host: str | None,
        firmware_server_host: str | None = None,
    ) -> None:
        await self.async_connection_opened(
            session_id, charge_point_id, local_host, remote_host, firmware_server_host=firmware_server_host
        )

    async def async_connection_closed(self, session_id: str | None = None) -> None:
        removed = self._connected_charge_points.pop(session_id, None) if session_id else None
        cpid = str((removed or {}).get("charge_point_id") or "").strip() or None
        now = datetime.now(UTC)

        if cpid and cpid in self._charger_states:
            state = self._charger_states[cpid]
            state.connected = False
            state.connection_state = "disconnected"
            state.live_power_kw = None
            state.live_current_a = None
            state.live_voltage_v = None
            state.last_seen = now
            self._mark_firmware_installing_after_disconnect(state)
            self._persist_charge_point_state(cpid, state, persist=True, notify=False)

        _LOGGER.info("Charger disconnected: %s", cpid)
        self._notify()

    async def async_unmanaged_connection_closed(self, session_id: str) -> None:
        removed = self._connected_charge_points.pop(session_id, None)
        if removed:
            _LOGGER.info("Unadopted charger disconnected: %s", removed.get("charge_point_id"))
            if removed.get("charge_point_id"):
                snapshot = self.charger_snapshot_for(removed.get("charge_point_id")) or {}
                snapshot.update(
                    {
                        "connected": False,
                        "connection_state": "disconnected",
                        "last_seen": datetime.now(UTC).isoformat(),
                    }
                )
                self._save_charger_snapshot(str(removed["charge_point_id"]), snapshot)

    async def async_passive_connection_closed(self, session_id: str) -> None:
        await self.async_connection_closed(session_id)

    # ── OCPP message handlers ────────────────────────────────────────────

    async def async_record_boot(
        self, charge_point_id: str | None, payload: dict[str, Any]
    ) -> None:
        cpid = str(charge_point_id or "").strip()

        # Ensure we have a live state entry for this charger
        if cpid and cpid not in self._charger_states:
            snapshot = self.charger_snapshot_for(cpid)
            state = _state_from_snapshot(snapshot or {})
            state.charge_point_id = cpid
            self._charger_states[cpid] = state

        state = self.state_for_charge_point(charge_point_id)
        state.charge_point_id = cpid or state.charge_point_id
        state.manufacturer = payload.get("chargePointVendor")
        state.model = payload.get("chargePointModel")
        state.firmware_version = payload.get("firmwareVersion")
        state.charge_point_serial_number = payload.get("chargePointSerialNumber")
        state.charge_box_serial_number = payload.get("chargeBoxSerialNumber")
        state.last_boot_notification = payload
        state.last_seen = datetime.now(UTC)
        self._mark_firmware_result_from_observed_version(state)
        if self._active_firmware_charge_point_id == cpid:
            self._active_firmware_charge_point_id = None

        session_id = self._connected_session_key_for_charge_point(charge_point_id)
        active = cpid == self._primary_charge_point_id
        self._update_connected_charge_point_boot(session_id, charge_point_id, payload, active=active)
        if session_id:
            self._save_connected_charge_point_snapshot(session_id)

        _LOGGER.info("BootNotification from %s %s fw=%s", state.manufacturer, state.model, state.firmware_version)
        self._persist_charge_point_state(cpid, state, persist=True, notify=True)

        # Schedule GetConfiguration after a short delay so the charger has finished
        # processing the BootNotification response before we send outbound calls.
        asyncio.get_running_loop().call_later(
            2.0,
            lambda: asyncio.ensure_future(
                self._safe_refresh_configuration(cpid)
            ),
        )
        if cpid and self._get_dst_correction_pending(cpid):
            asyncio.get_running_loop().call_later(
                5.0,
                lambda: asyncio.ensure_future(
                    self._async_repush_schedule(
                        cpid,
                        "Schedule timezone correction (DST)",
                        via="System (DST correction)",
                    )
                ),
            )

    async def async_record_unmanaged_boot(
        self, session_id: str, charge_point_id: str | None, payload: dict[str, Any]
    ) -> None:
        self._update_connected_charge_point_boot(session_id, charge_point_id, payload, active=False)
        self._save_connected_charge_point_snapshot(session_id)
        _LOGGER.info(
            "BootNotification from unadopted charger %s %s fw=%s",
            payload.get("chargePointVendor"),
            payload.get("chargePointModel"),
            payload.get("firmwareVersion"),
        )

    async def async_record_unmanaged_heartbeat(self, session_id: str) -> None:
        self._touch_connected_charge_point(session_id)
        self._save_connected_charge_point_snapshot(session_id)

    async def async_record_unmanaged_status(self, session_id: str, payload: dict[str, Any]) -> None:
        self._touch_connected_charge_point(
            session_id,
            status=payload.get("status"),
            error_code=payload.get("errorCode"),
            vendor_error_code=payload.get("vendorErrorCode"),
        )
        self._save_connected_charge_point_snapshot(session_id)

    def _record_connected_charge_point(
        self,
        session_id: str,
        charge_point_id: str | None,
        *,
        local_host: str | None,
        remote_host: str | None,
        firmware_server_host: str | None,
        active: bool,
        last_seen: datetime,
    ) -> None:
        existing = self._connected_charge_points.get(session_id, {})
        self._connected_charge_points[session_id] = {
            **existing,
            "session_id": session_id,
            "charge_point_id": charge_point_id,
            "local_ip_address": local_host,
            "firmware_server_host": firmware_server_host,
            "remote_address": remote_host,
            "connection_state": "connected",
            "active": active,
            "last_seen": last_seen.isoformat(),
        }

    def resolve_firmware_server_host(
        self,
        request_host: str | None = None,
        local_host: str | None = None,
    ) -> str | None:
        for candidate in (self.firmware_public_host, request_host, local_host):
            host = str(candidate or "").strip()
            if host and host not in {"0.0.0.0", "::"}:
                return host
        return None

    def _update_connected_charge_point_boot(
        self,
        session_id: str | None,
        charge_point_id: str | None,
        payload: dict[str, Any],
        *,
        active: bool,
    ) -> None:
        key = session_id or self._connected_session_key(charge_point_id, active=active)
        if not key:
            return
        existing = self._connected_charge_points.get(key, {})
        self._connected_charge_points[key] = {
            **existing,
            "session_id": key,
            "charge_point_id": charge_point_id or existing.get("charge_point_id"),
            "manufacturer": payload.get("chargePointVendor"),
            "model": payload.get("chargePointModel"),
            "firmware": payload.get("firmwareVersion"),
            "serial": payload.get("chargePointSerialNumber") or payload.get("chargeBoxSerialNumber"),
            "charge_point_serial_number": payload.get("chargePointSerialNumber"),
            "charge_box_serial_number": payload.get("chargeBoxSerialNumber"),
            "active": active,
            "last_seen": datetime.now(UTC).isoformat(),
        }

    def _touch_connected_charge_point(self, session_id: str, **updates: Any) -> None:
        existing = self._connected_charge_points.get(session_id)
        if not existing:
            return
        existing.update(updates)
        existing["last_seen"] = datetime.now(UTC).isoformat()

    def _connected_session_key(self, charge_point_id: str | None, *, active: bool) -> str | None:
        for session_id, item in self._connected_charge_points.items():
            if item.get("active") == active and item.get("charge_point_id") == charge_point_id:
                return session_id
        return None

    def _connected_session_key_for_charge_point(self, charge_point_id: str | None) -> str | None:
        for session_id, item in self._connected_charge_points.items():
            if item.get("charge_point_id") == charge_point_id:
                return session_id
        return None

    def mark_connected_charge_point_adopted(self, charge_point_id: str | None, *, active: bool = False) -> None:
        session_id = self._connected_session_key_for_charge_point(charge_point_id)
        if not session_id:
            return
        self._touch_connected_charge_point(
            session_id,
            active=active,
            connection_state="connected",
        )
        self._save_connected_charge_point_snapshot(session_id)
        self._push_sse()

    def _is_selected_charge_point(self, charge_point_id: str | None) -> bool:
        if not charge_point_id:
            return True
        return str(charge_point_id) == str(self.data.charge_point_id or "")

    def _patch_charger_snapshot(self, charge_point_id: str | None, updates: dict[str, Any]) -> None:
        charge_point_id = str(charge_point_id or "").strip()
        if not charge_point_id:
            return
        snapshot = self.charger_snapshot_for(charge_point_id) or {"charge_point_id": charge_point_id}
        snapshot.update(updates)
        self._save_charger_snapshot(charge_point_id, snapshot)
        self._push_sse()

    async def async_record_heartbeat(self, charge_point_id: str | None = None) -> None:
        cpid = str(charge_point_id or "").strip()
        state = self.state_for_charge_point(cpid)
        state.last_heartbeat = datetime.now(UTC)
        state.last_seen = datetime.now(UTC)
        session_id = self._connected_session_key_for_charge_point(cpid)
        if session_id:
            self._touch_connected_charge_point(session_id)
            self._save_connected_charge_point_snapshot(session_id)
        self._persist_charge_point_state(cpid, state, persist=True, notify=True)

    async def async_record_status(self, charge_point_id: str | None, payload: dict[str, Any]) -> None:
        cpid = str(charge_point_id or "").strip()
        state = self.state_for_charge_point(cpid)
        previous_plugged_in = state.car_plugged_in
        state.status = payload.get("status")
        state.error_code = payload.get("errorCode")
        state.vendor_error_code = payload.get("vendorErrorCode")
        state.last_status_notification = payload
        state.last_seen = datetime.now(UTC)
        state.car_plugged_in = _is_car_plugged_in_status(state.status)
        state.transaction_active = _is_charging_status(state.status)

        session_id = self._connected_session_key_for_charge_point(cpid)
        if session_id:
            self._touch_connected_charge_point(
                session_id,
                status=state.status,
                error_code=state.error_code,
                vendor_error_code=state.vendor_error_code,
            )
            self._save_connected_charge_point_snapshot(session_id)

        _LOGGER.debug("StatusNotification charger=%s: %s / %s", cpid, state.status, state.error_code)
        self._persist_charge_point_state(cpid, state, persist=True, notify=True)

        if (
            state.plug_and_go_enabled
            and previous_plugged_in is False
            and state.car_plugged_in is True
            and not state.transaction_active
            and not state.plug_and_go_start_pending
        ):
            state.plug_and_go_start_pending = True
            state.plug_and_go_last_error = None
            self._persist_charge_point_state(cpid, state, persist=True, notify=True)
            asyncio.create_task(self._async_handle_plug_and_go_start(cpid))

    async def async_start_transaction_from_charger(
        self, charge_point_id: str | None, payload: dict[str, Any]
    ) -> int:
        tx_id = next(_tx_counter)
        cpid = str(charge_point_id or "").strip()
        state = self.state_for_charge_point(cpid)
        timestamp = _parse_ocpp_ts(payload.get("timestamp")) or datetime.now(UTC)
        state.transaction_id = tx_id
        state.transaction_active = _is_charging_status(state.status)
        state.plug_and_go_start_pending = False
        state.plug_and_go_last_error = None
        state.transaction_id_tag = payload.get("idTag")
        state.transaction_meter_start_wh = _safe_float(payload.get("meterStart"))
        state.transaction_started_at = timestamp
        state.transaction_ended_at = None
        state.session_energy_kwh = None
        state.last_seen = datetime.now(UTC)
        _LOGGER.info("Transaction started id=%s tag=%s charger=%s", tx_id, state.transaction_id_tag, cpid)
        self._persist_charge_point_state(cpid, state, persist=True, notify=True)
        recorder = getattr(self._state_store, "record_session_start", None)
        if recorder and cpid:
            recorder(cpid, state.transaction_id_tag, state.transaction_meter_start_wh, timestamp.isoformat())
        return tx_id

    async def async_stop_transaction_from_charger(
        self, charge_point_id: str | None, payload: dict[str, Any]
    ) -> None:
        cpid = str(charge_point_id or "").strip()
        state = self.state_for_charge_point(cpid)
        meter_stop = _safe_float(payload.get("meterStop"))
        if meter_stop is not None:
            state.total_energy_kwh = round(meter_stop / 1000, 2)
            if state.transaction_meter_start_wh is not None:
                state.session_energy_kwh = round((meter_stop - state.transaction_meter_start_wh) / 1000, 3)
        state.transaction_active = False
        state.transaction_id = payload.get("transactionId", state.transaction_id)
        state.plug_and_go_start_pending = False
        state.transaction_ended_at = _parse_ocpp_ts(payload.get("timestamp")) or datetime.now(UTC)
        state.last_seen = datetime.now(UTC)
        _LOGGER.info("Transaction stopped charger=%s energy=%.3f kWh", cpid, state.session_energy_kwh or 0)
        self._persist_charge_point_state(cpid, state, persist=True, notify=True)
        recorder = getattr(self._state_store, "record_session_stop", None)
        if recorder and cpid:
            recorder(cpid, payload.get("idTag"), meter_stop, state.transaction_ended_at.isoformat(), payload.get("reason"))

    async def async_record_meter_values(self, charge_point_id: str | None, payload: dict[str, Any]) -> None:
        cpid = str(charge_point_id or "").strip()
        flattened = self._flatten_meter_values_payload(payload)
        recorder = getattr(self._state_store, "record_meter_values", None)
        if recorder and cpid:
            recorder(cpid, flattened)
        state = self.state_for_charge_point(cpid)
        previous_total_kwh = state.total_energy_kwh
        state.last_meter_values = payload
        state.last_seen = datetime.now(UTC)
        self._restore_transaction_from_meter_values(payload, state=state)
        self._apply_meter_values_payload(payload, state=state, flattened=flattened)
        self._persist_charge_point_state(cpid, state, persist=state.total_energy_kwh != previous_total_kwh, notify=True)

    def _restore_transaction_from_meter_values(
        self, payload: dict[str, Any], *, state: ChargerState | None = None
    ) -> None:
        """Recover an open transaction id from MeterValues after reconnects."""
        state = state or self.data
        transaction_id = _coerce_int(payload.get("transactionId"))
        if transaction_id is None:
            return

        if state.transaction_id != transaction_id:
            state.transaction_id = transaction_id
        state.transaction_ended_at = None
        if state.transaction_started_at is None:
            meter_values = payload.get("meterValue") or []
            timestamp = None
            if meter_values:
                timestamp = _parse_ocpp_ts(meter_values[0].get("timestamp"))
            state.transaction_started_at = timestamp or datetime.now(UTC)
        state.transaction_active = _is_charging_status(state.status)

    def _apply_meter_values_payload(
        self, payload: dict[str, Any], *, state: ChargerState | None = None, flattened: list[dict] | None = None
    ) -> None:
        """Parse MeterValues using the same sample selection model as upstream."""
        state = state or self.data
        flattened_samples = flattened if flattened is not None else self._flatten_meter_values_payload(payload)
        state.meter_samples = flattened_samples
        state.parsed_meter_values = {
            sample["sample_key"]: {
                "timestamp": sample["timestamp"],
                "raw_value": sample["raw_value"],
                "numeric_value": sample["numeric_value"],
                "normalized_value": sample["normalized_value"],
                "unit": sample["unit"],
                "measurand": sample["measurand"],
                "phase": sample["phase"],
                "context": sample["context"],
                "location": sample["location"],
            }
            for sample in flattened_samples
        }

        meter_groups = self._group_meter_samples(flattened_samples)
        ev_meter_group = self._pick_givenergy_ev_meter_group(meter_groups)
        preferred_group = ev_meter_group or self._pick_preferred_meter_group(meter_groups, state=state)
        live_samples = preferred_group or flattened_samples
        power_delivery_expected = self._status_expects_power_delivery(state=state)

        power_sample = self._pick_preferred_sample(
            live_samples,
            measurand="Power.Active.Import",
            preferred_phases=("L1", None, "L1-N", "N"),
            preferred_locations=("Outlet", None, "Body", "Cable"),
            preferred_contexts=("Sample.Periodic", None, "Transaction.Begin"),
            prefer_positive=power_delivery_expected,
            prefer_non_negative=not power_delivery_expected,
        )
        if power_sample and power_sample["normalized_value"] is not None:
            state.live_power_kw = round(power_sample["normalized_value"] / 1000, 2)

        current_sample = self._pick_preferred_sample(
            live_samples,
            measurand="Current.Import",
            preferred_phases=("L1", "N", None, "L1-N"),
            preferred_locations=("Outlet", None, "Body", "Cable"),
            preferred_contexts=("Sample.Periodic", None, "Transaction.Begin"),
            prefer_positive=power_delivery_expected,
            prefer_non_negative=not power_delivery_expected,
        )
        if current_sample and current_sample["normalized_value"] is not None:
            state.live_current_a = round(current_sample["normalized_value"], 3)

        voltage_sample = self._pick_preferred_sample(
            live_samples,
            measurand="Voltage",
            preferred_phases=("L1-N", None, "L1", "N"),
            preferred_locations=("Outlet", None, "Body", "Cable"),
            preferred_contexts=("Sample.Periodic", None, "Transaction.Begin"),
            prefer_non_negative=True,
        )
        if voltage_sample and voltage_sample["normalized_value"] is not None:
            state.live_voltage_v = round(voltage_sample["normalized_value"], 1)

        for sample in flattened_samples:
            context = sample.get("context") or ""
            m = CP_READING_PATTERN.search(str(context))
            if m:
                state.cp_voltage_v = float(m.group("voltage"))
                state.cp_duty_cycle_percent = float(m.group("duty"))

        previous_total_wh = (
            state.total_energy_kwh * 1000
            if state.total_energy_kwh is not None
            else None
        )
        total_energy_samples = ev_meter_group or preferred_group or flattened_samples
        total_energy_sample = self._pick_total_energy_sample(
            total_energy_samples, previous_total_wh
        )
        if total_energy_sample and total_energy_sample["normalized_value"] is not None:
            total_wh = total_energy_sample["normalized_value"]
            state.total_energy_kwh = round(total_wh / 1000, 2)
            if (
                state.transaction_active
                and state.transaction_meter_start_wh is None
                and total_wh > 0
            ):
                state.transaction_meter_start_wh = total_wh
                state.session_energy_kwh = 0.0
            elif (
                state.transaction_meter_start_wh is not None
                and total_wh >= state.transaction_meter_start_wh
            ):
                state.session_energy_kwh = round(
                    (total_wh - state.transaction_meter_start_wh) / 1000, 3
                )

    def _flatten_meter_values_payload(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        flattened: list[dict[str, Any]] = []
        for group_index, meter_value in enumerate(payload.get("meterValue") or []):
            timestamp = meter_value.get("timestamp")
            for sample_index, sampled_value in enumerate(meter_value.get("sampledValue") or []):
                raw_value = sampled_value.get("value")
                numeric_value = _safe_float(raw_value)
                measurand = sampled_value.get("measurand", "Energy.Active.Import.Register")
                phase = sampled_value.get("phase")
                context = sampled_value.get("context")
                location = sampled_value.get("location")
                unit = sampled_value.get("unit")
                normalized_value = _normalise_meter_sample_value(measurand, unit, numeric_value)
                sample_key = "|".join(
                    [
                        measurand,
                        phase or "no_phase",
                        context or "no_context",
                        location or "no_location",
                        unit or "no_unit",
                        str(group_index),
                        str(sample_index),
                    ]
                )
                flattened.append(
                    {
                        "timestamp": timestamp,
                        "group_index": group_index,
                        "sample_index": sample_index,
                        "raw_value": raw_value,
                        "numeric_value": numeric_value,
                        "normalized_value": normalized_value,
                        "measurand": measurand,
                        "phase": phase,
                        "context": context,
                        "location": location,
                        "unit": unit,
                        "sample_key": sample_key,
                    }
                )
        return flattened

    def _pick_preferred_sample(
        self,
        samples: list[dict[str, Any]],
        *,
        measurand: str,
        preferred_phases: tuple[str | None, ...],
        preferred_locations: tuple[str | None, ...] = (None,),
        preferred_contexts: tuple[str | None, ...] = (None,),
        prefer_positive: bool = False,
        prefer_non_negative: bool = False,
    ) -> dict[str, Any] | None:
        candidates = [
            sample
            for sample in samples
            if sample["measurand"] == measurand
            and sample["normalized_value"] is not None
        ]
        if not candidates:
            return None

        if prefer_positive:
            positive_candidates = [
                sample for sample in candidates if sample["normalized_value"] > 0
            ]
            if positive_candidates:
                candidates = positive_candidates
        elif prefer_non_negative:
            non_negative_candidates = [
                sample for sample in candidates if sample["normalized_value"] >= 0
            ]
            if non_negative_candidates:
                candidates = non_negative_candidates

        phase_scores = {
            phase: len(preferred_phases) - index
            for index, phase in enumerate(preferred_phases)
        }
        location_scores = {
            location: len(preferred_locations) - index
            for index, location in enumerate(preferred_locations)
        }
        context_scores = {
            context: len(preferred_contexts) - index
            for index, context in enumerate(preferred_contexts)
        }

        def score(item: dict[str, Any]) -> tuple[int, int, int, float]:
            return (
                phase_scores.get(item.get("phase"), 0),
                location_scores.get(item.get("location"), 0),
                context_scores.get(item.get("context"), 0),
                item["normalized_value"],
            )

        return max(candidates, key=score)

    def _group_meter_samples(
        self, samples: list[dict[str, Any]]
    ) -> dict[int, list[dict[str, Any]]]:
        groups: dict[int, list[dict[str, Any]]] = {}
        for sample in samples:
            groups.setdefault(sample["group_index"], []).append(sample)
        return groups

    def _pick_givenergy_ev_meter_group(
        self, groups: dict[int, list[dict[str, Any]]]
    ) -> list[dict[str, Any]] | None:
        ev_group = groups.get(0)
        if not ev_group:
            return None
        seen_measurands = {sample["measurand"] for sample in ev_group}
        if "Power.Active.Import" in seen_measurands and "Voltage" in seen_measurands:
            return ev_group
        return None

    def _pick_preferred_meter_group(
        self, groups: dict[int, list[dict[str, Any]]], *, state: ChargerState | None = None
    ) -> list[dict[str, Any]] | None:
        state = state or self.data
        if not groups:
            return None

        power_delivery_expected = self._status_expects_power_delivery(state=state)

        def group_summary(
            samples: list[dict[str, Any]],
        ) -> tuple[float | None, float | None, float | None, float | None]:
            power_sample = self._pick_preferred_sample(
                samples,
                measurand="Power.Active.Import",
                preferred_phases=("L1", None, "L1-N", "N"),
                prefer_positive=power_delivery_expected,
                prefer_non_negative=not power_delivery_expected,
            )
            current_sample = self._pick_preferred_sample(
                samples,
                measurand="Current.Import",
                preferred_phases=("L1", "N", None, "L1-N"),
                prefer_positive=power_delivery_expected,
                prefer_non_negative=not power_delivery_expected,
            )
            voltage_sample = self._pick_preferred_sample(
                samples,
                measurand="Voltage",
                preferred_phases=("L1-N", None, "L1", "N"),
                prefer_non_negative=True,
            )
            energy_sample = self._pick_total_energy_sample(samples, None)
            return (
                power_sample["normalized_value"] if power_sample else None,
                current_sample["normalized_value"] if current_sample else None,
                voltage_sample["normalized_value"] if voltage_sample else None,
                energy_sample["normalized_value"] if energy_sample else None,
            )

        def score(
            item: tuple[int, list[dict[str, Any]]],
        ) -> tuple[int, int, int, int, int, float, float] | tuple[int, int, int, float, float]:
            _group_index, samples = item
            power, current, voltage, energy = group_summary(samples)
            within_current_limit = int(self._sample_within_current_limit(current, state=state))
            within_power_limit = int(
                self._sample_within_power_limit(power, current, voltage, state=state)
            )
            has_valid_voltage = int(voltage is not None and voltage > 100)
            non_negative_power = int(power is not None and power >= 0)
            charging_like = int(
                power is not None
                and current is not None
                and power > 100
                and current > 0.5
            )
            near_zero_idle = int(
                power is not None
                and current is not None
                and abs(power) <= 50
                and abs(current) <= 0.5
            )
            energy_score = energy or 0.0

            if power_delivery_expected:
                return (
                    within_current_limit,
                    within_power_limit,
                    charging_like,
                    has_valid_voltage,
                    non_negative_power,
                    power or float("-inf"),
                    energy_score,
                )

            return (
                near_zero_idle,
                has_valid_voltage,
                non_negative_power,
                -(abs(power) if power is not None else float("inf")),
                energy_score,
            )

        return max(groups.items(), key=score)[1]

    def _status_expects_power_delivery(self, *, state: ChargerState | None = None) -> bool:
        state = state or self.data
        if state.status is None:
            return state.transaction_active
        return state.status == "Charging"

    def _sample_within_current_limit(
        self, current: float | None, *, state: ChargerState | None = None
    ) -> bool:
        if current is None:
            return False
        state = state or self.data
        limit = state.current_limit_a or DEFAULT_EVSE_MAX_CURRENT
        return current <= (limit * 1.1)

    def _sample_within_power_limit(
        self,
        power_w: float | None,
        current_a: float | None,
        voltage_v: float | None,
        *,
        state: ChargerState | None = None,
    ) -> bool:
        if power_w is None:
            return False
        state = state or self.data
        limit = state.current_limit_a or DEFAULT_EVSE_MAX_CURRENT
        reference_voltage = voltage_v if voltage_v and voltage_v > 100 else 240.0
        max_power_w = limit * reference_voltage * 1.1
        if power_w <= max_power_w:
            return True
        if current_a is not None and current_a <= (limit * 1.1):
            return True
        return False

    def _pick_total_energy_sample(
        self, samples: list[dict[str, Any]], previous_total_wh: float | None
    ) -> dict[str, Any] | None:
        candidates = [
            sample
            for sample in samples
            if sample["measurand"] == "Energy.Active.Import.Register"
            and sample["normalized_value"] is not None
        ]
        if not candidates:
            return None

        non_zero = [sample for sample in candidates if sample["normalized_value"] > 0]
        pool = non_zero or candidates
        if previous_total_wh is None:
            return max(pool, key=lambda item: item["normalized_value"])

        non_decreasing = [
            sample for sample in pool if sample["normalized_value"] >= previous_total_wh
        ]
        if non_decreasing:
            return min(
                non_decreasing,
                key=lambda item: item["normalized_value"] - previous_total_wh,
            )
        return min(
            pool, key=lambda item: abs(item["normalized_value"] - previous_total_wh)
        )

    # ── Configuration refresh ────────────────────────────────────────────

    async def _safe_refresh_configuration(self, charge_point_id: str | None = None) -> None:
        try:
            await self.async_refresh_configuration(charge_point_id=charge_point_id)
        except Exception:
            _LOGGER.exception("GetConfiguration failed for charger %s", charge_point_id)

    async def _safe_reset(self, charge_point_id: str | None = None) -> None:
        try:
            target = charge_point_id or self._primary_charge_point_id
            if not self.charge_point_can_receive_commands(target):
                return
            result = await self._ocpp_call("Reset", {"type": "Hard"}, timeout=5, charge_point_id=target)
            _LOGGER.info("Reset response: %s", result)
        except RuntimeError:
            _LOGGER.info("Reset sent — no response before disconnect (normal)")

    async def async_refresh_configuration(self, charge_point_id: str | None = None) -> dict[str, Any]:
        """Send GetConfiguration to the charger and update local state from the response."""
        result = await self._ocpp_call("GetConfiguration", {}, timeout=60, charge_point_id=charge_point_id)
        state = self.state_for_charge_point(charge_point_id)
        config: dict[str, dict[str, Any]] = {}
        for item in result.get("configurationKey", []):
            key = item.get("key")
            if key:
                config[key] = item

        def _val(k: str) -> str | None:
            entry = config.get(k)
            return entry.get("value") if entry else None

        def _bool(k: str) -> bool | None:
            v = _val(k)
            return None if v is None else v.lower() in ("true", "1")

        def _float(k: str) -> float | None:
            v = _val(k)
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        reported_ip = str(_val("LocalIPAddress") or "").strip()
        state.local_ip_address = (
            reported_ip
            if reported_ip and reported_ip != "0.0.0.0"
            else state.websocket_remote_address
        )

        state.charge_mode = _val("EcoMode") or state.charge_mode
        state.front_panel_leds_enabled = _bool("FrontPanelLEDsEnabled")
        state.local_modbus_enabled     = _bool("EnableLocalModbus")

        # Current limit — ChargeRate takes priority; stored in tenths-of-amps on write
        # but reported back in real amps by GetConfiguration.
        EVSE_MIN, EVSE_MAX = 6.0, 32.0
        limit: float | None = None
        for key in ("ChargeRate", "MaxCurrent"):
            raw = _float(key)
            if raw is None:
                continue
            # Both keys report real amps in GetConfiguration responses
            if EVSE_MIN <= raw <= EVSE_MAX:
                limit = round(raw, 1)
                state.current_limit_key = key
                break
            _LOGGER.debug("Ignoring %s=%s — outside %s–%sA range", key, raw, EVSE_MIN, EVSE_MAX)
        if limit is not None:
            state.current_limit_a = limit

        imax = _float("Imax")
        if imax is not None:
            try:
                state.max_import_capacity_a = int(imax)
            except (TypeError, ValueError):
                pass

        rand_delay = _float("RandomisedDelayDuration")
        if rand_delay is not None:
            try:
                state.randomised_delay_s = int(rand_delay)
            except (TypeError, ValueError):
                pass

        cp_lower = _float("ChargingStateBCPVoltageLowerLimit")
        if cp_lower is not None:
            state.cp_voltage_lower_limit = round(cp_lower / 10, 1)

        cp_upper = _float("ChargingStateBCPVoltageHigherLimit")
        if cp_upper is not None:
            state.cp_voltage_upper_limit = round(cp_upper / 10, 1)

        suspev = _float("SuspevTime")
        if suspev is not None:
            try:
                state.suspend_timeout_s = int(suspev)
            except (TypeError, ValueError):
                pass

        _LOGGER.info(
            "GetConfiguration: mode=%s leds=%s modbus=%s current_limit=%sA (key=%s) imax=%s suspev=%s",
            state.charge_mode,
            state.front_panel_leds_enabled,
            state.local_modbus_enabled,
            state.current_limit_a,
            state.current_limit_key,
            state.max_import_capacity_a,
            state.suspend_timeout_s,
        )
        self._persist_charge_point_state(charge_point_id, state, persist=True)
        return result

    # ── Outbound OCPP commands ───────────────────────────────────────────

    async def async_change_configuration(
        self, key: str, value: Any, charge_point_id: str | None = None
    ) -> dict[str, Any]:
        # ChargeRate is written in tenths-of-amps but read back in real amps
        if key == "ChargeRate":
            value = round(float(value) * 10, 1)
        result = await self._ocpp_call(
            "ChangeConfiguration",
            {"key": key, "value": str(value)},
            charge_point_id=charge_point_id,
        )
        status = result.get("status", "")
        if status in ("Accepted", "RebootRequired"):
            _LOGGER.info("ChangeConfiguration %s=%s → %s", key, value, status)
            state = self.state_for_charge_point(charge_point_id)
            self._apply_config_key(key, value, state=state)
            self._persist_charge_point_state(charge_point_id, state, persist=True)
        if status == "RebootRequired":
            _LOGGER.info("RebootRequired after %s — scheduling Hard Reset", key)
            asyncio.get_running_loop().call_later(
                0.5, lambda: asyncio.ensure_future(self._safe_reset(charge_point_id))
            )
        return result

    def _apply_config_key(self, key: str, value: Any, *, state: ChargerState | None = None) -> None:
        state = state or self.data
        sv = str(value)
        if key == "EcoMode":
            state.charge_mode = sv or None
        elif key == "FrontPanelLEDsEnabled":
            state.front_panel_leds_enabled = sv.lower() in ("true", "1")
        elif key == "EnableLocalModbus":
            state.local_modbus_enabled = sv.lower() in ("true", "1")
        elif key == "MaxCurrent":
            try:
                state.current_limit_a = float(sv)
            except (TypeError, ValueError):
                pass
        elif key == "ChargeRate":
            try:
                # value is tenths-of-amps (already ×10 before sending); convert back
                state.current_limit_a = round(float(sv) / 10, 1)
            except (TypeError, ValueError):
                pass
        elif key == "Imax":
            try:
                state.max_import_capacity_a = int(float(sv))
            except (TypeError, ValueError):
                pass
        elif key == "MaxEnergyOnInvalidId":
            try:
                state.max_energy_per_session_kwh = max(0, round(float(sv) / 1000, 3))
            except (TypeError, ValueError):
                pass
        elif key == "SuspevTime":
            try:
                state.suspend_timeout_s = int(float(sv))
            except (TypeError, ValueError):
                pass
        elif key == "RandomisedDelayDuration":
            try:
                state.randomised_delay_s = int(float(sv))
            except (TypeError, ValueError):
                pass
        elif key == "ChargingStateBCPVoltageLowerLimit":
            try:
                state.cp_voltage_lower_limit = round(float(sv) / 10, 1)
            except (TypeError, ValueError):
                pass
        elif key == "ChargingStateBCPVoltageHigherLimit":
            try:
                state.cp_voltage_upper_limit = round(float(sv) / 10, 1)
            except (TypeError, ValueError):
                pass

    CHARGE_MODES = ("SuperEco", "Eco", "Boost")

    async def async_set_charge_mode(self, mode: str, charge_point_id: str | None = None) -> dict[str, Any]:
        if mode not in self.CHARGE_MODES:
            raise ValueError(f"Unknown charge mode: {mode}")
        result = await self._ocpp_call(
            "ChangeConfiguration",
            {"key": "EcoMode", "value": mode},
            charge_point_id=charge_point_id,
        )
        status = result.get("status", "")
        if status in ("Accepted", "RebootRequired"):
            state = self.state_for_charge_point(charge_point_id)
            state.charge_mode = mode
            self._persist_charge_point_state(charge_point_id, state, persist=True)
        return result

    async def async_set_plug_and_go(self, enabled: bool, charge_point_id: str | None = None) -> None:
        """Plug-and-go is local state only — no OCPP call needed."""
        state = self.state_for_charge_point(charge_point_id)
        state.plug_and_go_enabled = enabled
        if not enabled:
            state.plug_and_go_start_pending = False
        self._persist_charge_point_state(charge_point_id, state, persist=True)

    async def async_set_max_energy_per_session(self, kwh: float, charge_point_id: str | None = None) -> None:
        """Max energy threshold is local UI state, persisted across restarts."""
        state = self.state_for_charge_point(charge_point_id)
        state.max_energy_per_session_kwh = max(0, round(float(kwh), 3))
        self._persist_charge_point_state(charge_point_id, state, persist=True)

    async def async_save_charging_schedule(
        self, schedule: dict[str, Any], charge_point_id: str | None = None
    ) -> dict[str, Any]:
        """Create or update a schedule, applying it to the charger only when enabled."""
        state = self.state_for_charge_point(charge_point_id)
        existing = {str(item.get("id")): item for item in state.charging_schedule if item.get("id") is not None}
        schedule_id = str(schedule.get("id") or _next_schedule_id(existing))
        normalised = _normalise_schedule(schedule, schedule_id)
        previous_enabled = bool(existing.get(schedule_id, {}).get("enabled"))
        ocpp_response: dict[str, Any] | None = None

        if normalised["enabled"]:
            ocpp_response = await self._async_apply_charging_schedule(normalised, charge_point_id=charge_point_id)
            self._set_dst_correction_pending(str(charge_point_id or self._primary_charge_point_id or ""), False)
        elif previous_enabled:
            ocpp_response = await self._async_clear_charging_schedule(charge_point_id=charge_point_id)
            self._set_dst_correction_pending(str(charge_point_id or self._primary_charge_point_id or ""), False)

        updated: list[dict[str, Any]] = []
        replaced = False
        for item in state.charging_schedule:
            if str(item.get("id")) == schedule_id:
                updated.append(normalised)
                replaced = True
            else:
                updated.append(item)
        if not replaced:
            updated.append(normalised)

        if normalised["enabled"]:
            for item in updated:
                if str(item.get("id")) != schedule_id:
                    item["enabled"] = False

        state.charging_schedule = updated
        self._persist_charge_point_state(charge_point_id, state, persist=True)
        return _schedule_with_ocpp_response(normalised, ocpp_response)

    async def async_set_charging_schedule_enabled(
        self, schedule_id: str, enabled: bool, charge_point_id: str | None = None
    ) -> dict[str, Any]:
        """Enable or disable one schedule, ensuring only one active charger profile exists."""
        state = self.state_for_charge_point(charge_point_id)
        target: dict[str, Any] | None = None
        for item in state.charging_schedule:
            if str(item.get("id")) == str(schedule_id):
                target = item
                break
        if target is None:
            raise ValueError(f"Unknown schedule: {schedule_id}")

        already_enabled = bool(target.get("enabled"))
        enabled = bool(enabled)
        ocpp_response: dict[str, Any] | None = None
        if enabled:
            ocpp_response = await self._async_apply_charging_schedule(target, charge_point_id=charge_point_id)
            self._set_dst_correction_pending(str(charge_point_id or self._primary_charge_point_id or ""), False)
        elif already_enabled:
            ocpp_response = await self._async_clear_charging_schedule(charge_point_id=charge_point_id)
            self._set_dst_correction_pending(str(charge_point_id or self._primary_charge_point_id or ""), False)

        for item in state.charging_schedule:
            if str(item.get("id")) == str(schedule_id):
                item["enabled"] = enabled
                target = item
            elif enabled:
                item["enabled"] = False
        self._persist_charge_point_state(charge_point_id, state, persist=True)
        return _schedule_with_ocpp_response(target, ocpp_response)

    async def async_delete_charging_schedule(
        self, schedule_id: str, charge_point_id: str | None = None
    ) -> dict[str, Any] | None:
        state = self.state_for_charge_point(charge_point_id)
        target = next(
            (item for item in state.charging_schedule if str(item.get("id")) == str(schedule_id)),
            None,
        )
        if target is None:
            raise ValueError(f"Unknown schedule: {schedule_id}")

        ocpp_response: dict[str, Any] | None = None
        if target.get("enabled"):
            ocpp_response = await self._async_clear_charging_schedule(charge_point_id=charge_point_id)

        before = len(state.charging_schedule)
        state.charging_schedule = [
            item for item in state.charging_schedule
            if str(item.get("id")) != str(schedule_id)
        ]
        if len(state.charging_schedule) == before:
            raise ValueError(f"Unknown schedule: {schedule_id}")
        self._persist_charge_point_state(charge_point_id, state, persist=True)
        return ocpp_response

    def _active_schedule_for_charge_point(self, charge_point_id: str) -> dict[str, Any] | None:
        state = self.state_for_charge_point(charge_point_id)
        return next(
            (item for item in state.charging_schedule if item.get("enabled")),
            None,
        )

    def _is_charge_point_online(self, charge_point_id: str) -> bool:
        if any(
            item.get("charge_point_id") == charge_point_id
            for item in self._connected_charge_points.values()
        ):
            return True
        state = self._charger_states.get(charge_point_id)
        return bool(state and state.connected)

    def _set_dst_correction_pending(self, charge_point_id: str, pending: bool) -> None:
        snapshot = self.charger_snapshot_for(charge_point_id) or {}
        snapshot["dst_correction_pending"] = pending
        saver = getattr(self._state_store, "save_charger_state", None)
        if saver:
            saver(charge_point_id, snapshot)

    def _get_dst_correction_pending(self, charge_point_id: str) -> bool:
        snapshot = self.charger_snapshot_for(charge_point_id) or {}
        return bool(snapshot.get("dst_correction_pending"))

    async def _async_repush_schedule(
        self,
        charge_point_id: str,
        action_label: str,
        via: str = "System",
    ) -> bool:
        """Re-push the active schedule for one charger. Returns True on success."""
        schedule = self._active_schedule_for_charge_point(charge_point_id)
        if not schedule:
            self._set_dst_correction_pending(charge_point_id, False)
            return False
        try:
            result = await self._async_apply_charging_schedule(schedule, charge_point_id=charge_point_id)
            self._set_dst_correction_pending(charge_point_id, False)
            self.record_portal_action(
                action_label,
                f"Schedule '{schedule.get('name', schedule.get('id'))}' re-pushed successfully",
                response=result,
                success=True,
                charge_point_id=charge_point_id,
                user="System",
                via=via,
            )
            _LOGGER.info("%s: %s for %s", via, action_label, charge_point_id)
            return True
        except Exception as exc:
            self.record_portal_action(
                action_label,
                f"Failed to re-push schedule '{schedule.get('name', schedule.get('id'))}': {exc}",
                response=str(exc),
                success=False,
                charge_point_id=charge_point_id,
                user="System",
                via=via,
            )
            _LOGGER.warning("%s: %s failed for %s: %s", via, action_label, charge_point_id, exc)
            return False

    async def async_dst_correction(self, auth_store: Any | None = None) -> dict[str, Any]:
        """Apply DST schedule correction to all chargers with an active schedule."""
        lister = getattr(auth_store or self._state_store, "list_all_adopted_charge_point_ids", None)
        all_ids: list[str] = lister() if lister else list(self._charger_states.keys())

        online_ids = [cpid for cpid in all_ids if self._active_schedule_for_charge_point(cpid) and self._is_charge_point_online(cpid)]
        offline_ids = [cpid for cpid in all_ids if self._active_schedule_for_charge_point(cpid) and not self._is_charge_point_online(cpid)]

        for cpid in offline_ids:
            self._set_dst_correction_pending(cpid, True)
            _LOGGER.info("DST correction: charger %s offline, flagged for reconnect", cpid)

        sem = asyncio.Semaphore(DST_SCHEDULE_PUSH_CONCURRENCY)

        async def _push_one(cpid: str) -> bool:
            async with sem:
                return await self._async_repush_schedule(
                    cpid,
                    "Schedule timezone correction (DST)",
                    via="System (DST correction)",
                )

        results = await asyncio.gather(*[_push_one(cpid) for cpid in online_ids], return_exceptions=True)
        succeeded = sum(1 for r in results if r is True)
        failed = sum(1 for r in results if r is not True)

        setter = getattr(auth_store or self._state_store, "set_system_setting", None)
        if setter:
            setter("dst_last_run", datetime.now(UTC).isoformat())

        _LOGGER.info(
            "DST correction complete: %d online pushed (%d ok, %d failed), %d offline flagged",
            len(online_ids), succeeded, failed, len(offline_ids),
        )
        return {"online_pushed": len(online_ids), "succeeded": succeeded, "failed": failed, "offline_flagged": len(offline_ids)}

    async def async_force_repush_all_schedules(self, auth_store: Any | None = None) -> dict[str, Any]:
        """Force re-push active schedules to all chargers (online now, flag offline)."""
        lister = getattr(auth_store or self._state_store, "list_all_adopted_charge_point_ids", None)
        all_ids: list[str] = lister() if lister else list(self._charger_states.keys())

        online_ids = [cpid for cpid in all_ids if self._active_schedule_for_charge_point(cpid) and self._is_charge_point_online(cpid)]
        offline_ids = [cpid for cpid in all_ids if self._active_schedule_for_charge_point(cpid) and not self._is_charge_point_online(cpid)]

        for cpid in offline_ids:
            self._set_dst_correction_pending(cpid, True)

        sem = asyncio.Semaphore(DST_SCHEDULE_PUSH_CONCURRENCY)

        async def _push_one(cpid: str) -> bool:
            async with sem:
                return await self._async_repush_schedule(
                    cpid,
                    "Schedule force re-push",
                    via="Admin",
                )

        results = await asyncio.gather(*[_push_one(cpid) for cpid in online_ids], return_exceptions=True)
        succeeded = sum(1 for r in results if r is True)
        failed = sum(1 for r in results if r is not True)

        return {"online_pushed": len(online_ids), "succeeded": succeeded, "failed": failed, "offline_flagged": len(offline_ids)}

    async def _async_apply_charging_schedule(
        self, schedule: dict[str, Any], charge_point_id: str | None = None
    ) -> dict[str, Any]:
        payload = _build_charging_schedule_payload(schedule)
        result = await self._ocpp_call(
            "SetChargingProfile",
            payload,
            timeout=30,
            charge_point_id=charge_point_id,
        )
        _require_ocpp_status("SetChargingProfile", result, {"Accepted"})
        return {"request": payload, "response": result}

    async def _async_clear_charging_schedule(self, charge_point_id: str | None = None) -> dict[str, Any]:
        payload = {
            "connectorId": 0,
            "chargingProfilePurpose": "TxDefaultProfile",
            "stackLevel": 0,
        }
        result = await self._ocpp_call(
            "ClearChargingProfile",
            payload,
            timeout=30,
            charge_point_id=charge_point_id,
        )
        _require_ocpp_status("ClearChargingProfile", result, {"Accepted", "Unknown"})
        return {"request": payload, "response": result}

    async def async_save_rfid_tag(
        self, tag: dict[str, Any], charge_point_id: str | None = None
    ) -> dict[str, Any]:
        """Create or update an RFID tag on the charger before persisting locally."""
        state = self.state_for_charge_point(charge_point_id)
        original_raw = tag.get("original_id_tag")
        original_id_tag = str(original_raw or "").strip()
        normalised = _normalise_rfid_tag(tag)
        id_tag = normalised["id_tag"]
        has_original = bool(original_id_tag)
        original_key = original_id_tag.casefold()
        id_tag_key = id_tag.casefold()

        updated: list[dict[str, Any]] = []
        replaced = False
        previous: dict[str, Any] | None = None
        for item in state.rfid_tags:
            item_id = str(item.get("id_tag") or "").strip()
            item_key = item_id.casefold()
            if has_original and item_key == original_key:
                previous = _normalise_rfid_tag(item)
                updated.append(normalised)
                replaced = True
            elif item_key == id_tag_key:
                if not has_original:
                    previous = _normalise_rfid_tag(item)
                    updated.append(normalised)
                    replaced = True
                    continue
                raise ValueError(f"ID tag already exists: {id_tag}")
            else:
                updated.append(item)
        if not replaced:
            updated.append(normalised)

        ocpp_response: dict[str, Any] | None = None
        if _rfid_tag_requires_ocpp_update(previous, normalised, original_id_tag):
            entries: list[dict[str, Any]] = []
            if previous and previous["id_tag"].casefold() != id_tag_key:
                entries.append({"idTag": previous["id_tag"]})
            entries.append(_rfid_tag_local_authorization_entry(normalised))
            ocpp_response = await self._async_send_local_authorization_update(
                entries,
                charge_point_id=charge_point_id,
            )

        state.rfid_tags = _normalise_rfid_tag_list(updated)
        self._persist_charge_point_state(charge_point_id, state, persist=True)
        return _tag_with_ocpp_response(normalised, ocpp_response)

    async def async_set_rfid_tag_enabled(
        self, id_tag: str, enabled: bool, charge_point_id: str | None = None
    ) -> dict[str, Any]:
        state = self.state_for_charge_point(charge_point_id)
        target: dict[str, Any] | None = None
        for item in state.rfid_tags:
            if str(item.get("id_tag")) == str(id_tag):
                target = item
                break
        if target is None:
            raise ValueError(f"Unknown ID tag: {id_tag}")
        enabled = bool(enabled)
        ocpp_response: dict[str, Any] | None = None
        if bool(target.get("enabled", True)) != enabled:
            updated_target = dict(target)
            updated_target["enabled"] = enabled
            ocpp_response = await self._async_send_local_authorization_update(
                [_rfid_tag_local_authorization_entry(_normalise_rfid_tag(updated_target))],
                charge_point_id=charge_point_id,
            )

        for item in state.rfid_tags:
            if str(item.get("id_tag")) == str(id_tag):
                item["enabled"] = enabled
                target = item
                break
        self._persist_charge_point_state(charge_point_id, state, persist=True)
        return _tag_with_ocpp_response(target, ocpp_response)

    async def async_delete_rfid_tag(self, id_tag: str, charge_point_id: str | None = None) -> dict[str, Any]:
        state = self.state_for_charge_point(charge_point_id)
        before = len(state.rfid_tags)
        target = next(
            (item for item in state.rfid_tags if str(item.get("id_tag")) == str(id_tag)),
            None,
        )
        if target is None:
            raise ValueError(f"Unknown ID tag: {id_tag}")

        ocpp_response = await self._async_send_local_authorization_update(
            [{"idTag": str(target.get("id_tag"))}],
            charge_point_id=charge_point_id,
        )
        state.rfid_tags = [
            item for item in state.rfid_tags
            if str(item.get("id_tag")) != str(id_tag)
        ]
        if len(state.rfid_tags) == before:
            raise ValueError(f"Unknown ID tag: {id_tag}")
        self._persist_charge_point_state(charge_point_id, state, persist=True)
        return ocpp_response

    async def _async_send_local_authorization_update(
        self,
        entries: list[dict[str, Any]],
        charge_point_id: str | None = None,
    ) -> dict[str, Any]:
        if not entries:
            return {"status": "Accepted"}

        version_result = await self._ocpp_call(
            "GetLocalListVersion",
            {},
            timeout=30,
            charge_point_id=charge_point_id,
        )
        current_version = _coerce_int(version_result.get("listVersion") if isinstance(version_result, dict) else None)
        if current_version is None or current_version < 0:
            current_version = 0
        result = await self._ocpp_call(
            "SendLocalList",
            {
                "listVersion": current_version + 1,
                "updateType": "Differential",
                "localAuthorizationList": entries,
            },
            timeout=30,
            charge_point_id=charge_point_id,
        )
        _require_ocpp_status("SendLocalList", result, {"Accepted"})
        return result

    async def async_remote_start_transaction(
        self,
        id_tag: str | None = None,
        connector_id: int | None = None,
        charging_profile: dict[str, Any] | None = None,
        charge_point_id: str | None = None,
    ) -> dict[str, Any]:
        """Issue an OCPP RemoteStartTransaction command."""
        payload: dict[str, Any] = {"idTag": id_tag or DEFAULT_REMOTE_ID_TAG}
        if connector_id is not None:
            payload["connectorId"] = connector_id
        if charging_profile is not None:
            payload["chargingProfile"] = charging_profile
        result = await self._ocpp_call("RemoteStartTransaction", payload, charge_point_id=charge_point_id)
        _LOGGER.info("RemoteStartTransaction payload=%s → %s", payload, result.get("status"))
        return result

    async def async_start_charging(self, charge_point_id: str | None = None) -> dict[str, Any]:
        """Request an immediate charging session."""
        return await self.async_remote_start_transaction(connector_id=1, charge_point_id=charge_point_id)

    async def async_stop_charging(self, charge_point_id: str | None = None) -> dict[str, Any]:
        """Request the charger to stop the current transaction."""
        state = self.state_for_charge_point(charge_point_id)
        if state.transaction_id is None:
            raise RuntimeError("No active transaction id is available")
        result = await self._ocpp_call(
            "RemoteStopTransaction",
            {"transactionId": state.transaction_id},
            charge_point_id=charge_point_id,
        )
        _LOGGER.info("RemoteStopTransaction(%s) → %s", state.transaction_id, result.get("status"))
        return result

    def has_open_transaction(self, charge_point_id: str | None = None) -> bool:
        """Return whether a StartTransaction has been seen without a matching StopTransaction."""
        state = self.state_for_charge_point(charge_point_id)
        return state.transaction_id is not None and state.transaction_ended_at is None

    async def _async_handle_plug_and_go_start(self, charge_point_id: str | None = None) -> None:
        """Start charging after a real unplugged -> plugged status edge."""
        state = self.state_for_charge_point(charge_point_id)
        try:
            result = await self.async_start_charging(charge_point_id=charge_point_id)
            status = str(result.get("status", ""))
            if status and status != "Accepted":
                state.plug_and_go_last_error = status
            else:
                state.plug_and_go_last_error = None
            _LOGGER.info("Plug and Go remote start result: %s", result)
        except Exception as err:
            state.plug_and_go_last_error = str(err)
            _LOGGER.warning("Plug and Go failed to start charging: %s", err)
        finally:
            state.plug_and_go_start_pending = False
            self._persist_charge_point_state(charge_point_id, state, persist=True)

    async def async_read_cp_voltage(self, charge_point_id: str | None = None) -> dict[str, Any]:
        """Read CP voltage and duty cycle via GivEnergy vendor DataTransfer."""
        result = await self._ocpp_call(
            "DataTransfer",
            {"vendorId": "GivEnergy", "messageId": "Parameter", "data": "CP"},
            charge_point_id=charge_point_id,
        )
        status = str(result.get("status", ""))
        data = result.get("data")
        if status == "Accepted" and data:
            m = CP_READING_PATTERN.search(str(data))
            if m:
                state = self.state_for_charge_point(charge_point_id)
                state.cp_voltage_v = float(m.group("voltage"))
                state.cp_duty_cycle_percent = float(m.group("duty"))
                _LOGGER.info("CP reading: %.1fV / %.1f%%", state.cp_voltage_v, state.cp_duty_cycle_percent)
                self._persist_charge_point_state(charge_point_id, state, persist=True)
        return result

    async def async_trigger_meter_values(
        self, connector_id: int = 1, charge_point_id: str | None = None
    ) -> dict[str, Any]:
        """Ask the charger to send a MeterValues frame immediately."""
        return await self._ocpp_call(
            "TriggerMessage",
            {"requestedMessage": "MeterValues", "connectorId": connector_id},
            charge_point_id=charge_point_id,
        )

    async def async_unlock_connector(
        self, connector_id: int = 1, charge_point_id: str | None = None
    ) -> dict[str, Any]:
        result = await self._ocpp_call(
            "UnlockConnector",
            {"connectorId": connector_id},
            charge_point_id=charge_point_id,
        )
        _LOGGER.info("UnlockConnector → %s", result.get("status"))
        return result

    async def async_reset(self, reset_type: str = "Soft", charge_point_id: str | None = None) -> dict[str, Any]:
        result = await self._ocpp_call("Reset", {"type": reset_type}, charge_point_id=charge_point_id)
        _LOGGER.info("Reset(%s) → %s", reset_type, result.get("status"))
        return result

    # ── Firmware management ─────────────────────────────────────────────

    async def async_refresh_firmware_manifest(self, charge_point_id: str | None = None) -> None:
        """Fetch and parse the configured firmware manifest."""
        state = self.state_for_charge_point(charge_point_id)
        if not self.firmware_manifest_url:
            state.firmware_manifest_error = "No firmware manifest URL is configured"
            state.firmware_manifest_entries = {}
            self._refresh_available_firmware_files(state=state)
            self._persist_charge_point_state(charge_point_id, state, persist=True)
            return

        try:
            _assert_safe_url(self.firmware_manifest_url)
            async with aiohttp.ClientSession() as session:
                async with session.get(self.firmware_manifest_url, allow_redirects=False) as response:
                    if response.status != 200:
                        raise RuntimeError(f"Manifest request failed with HTTP {response.status}")
                    manifest = json.loads(await response.text())
        except Exception as err:
            state.firmware_manifest_error = str(err)
            state.firmware_manifest_entries = {}
            self._refresh_available_firmware_files(state=state)
            self._persist_charge_point_state(charge_point_id, state, persist=True)
            raise RuntimeError(
                f"Unable to load firmware manifest from {self.firmware_manifest_url}: {err}"
            ) from err

        state.firmware_manifest_entries = self._parse_firmware_manifest(manifest, state=state)
        state.firmware_manifest_error = None
        state.firmware_manifest_refreshed_at = datetime.now(UTC)
        self._refresh_available_firmware_files(state=state)
        self._persist_charge_point_state(charge_point_id, state, persist=True)

    def firmware_catalog(self, charge_point_id: str | None = None) -> dict[str, Any]:
        """Return firmware entries with action metadata for the UI."""
        state = self.state_for_charge_point(charge_point_id)
        self._refresh_available_firmware_files(state=state)
        entries = []
        for filename in state.available_firmware_files:
            entry = state.firmware_manifest_entries.get(filename, {})
            version = entry.get("version")
            entries.append({
                **entry,
                "action": self.firmware_action_for_version(version, charge_point_id=charge_point_id),
            })
        return {
            "current_version": state.firmware_version,
            "selected_firmware_file": state.selected_firmware_file,
            "manifest_error": state.firmware_manifest_error,
            "manifest_refreshed_at": (
                state.firmware_manifest_refreshed_at.isoformat()
                if state.firmware_manifest_refreshed_at
                else None
            ),
            "entries": entries,
        }

    def _parse_firmware_manifest(
        self, manifest: dict[str, Any], *, state: ChargerState | None = None
    ) -> dict[str, dict[str, Any]]:
        """Parse the upstream firmware manifest into filename-indexed entries."""
        models = manifest.get("models")
        if not isinstance(models, dict):
            raise RuntimeError("Firmware manifest does not contain a valid models map")

        preferred_model = self._derive_manifest_model_key(state=state)
        if preferred_model and isinstance(models.get(preferred_model), dict):
            selected_models = [(preferred_model, models[preferred_model])]
        else:
            selected_models = [
                (model_key, model_value)
                for model_key, model_value in models.items()
                if isinstance(model_value, dict)
            ]

        entries: dict[str, dict[str, Any]] = {}
        for model_key, model_data in selected_models:
            versions = model_data.get("versions")
            if not isinstance(versions, dict):
                continue
            for version, entry in versions.items():
                if not isinstance(entry, dict):
                    continue
                filename = entry.get("filename")
                url = entry.get("url")
                checksum_md5 = entry.get("checksum_md5")
                if not filename or not url or not checksum_md5:
                    continue
                normalized_filename = str(filename).strip()
                entries[normalized_filename] = {
                    "model": model_key,
                    "version": str(version).strip(),
                    "filename": normalized_filename,
                    "url": str(url).strip(),
                    "checksum_md5": str(checksum_md5).strip().lower(),
                    "size": _coerce_int(entry.get("size")),
                }

        if not entries:
            raise RuntimeError("Firmware manifest did not yield any usable firmware entries")
        return entries

    def _derive_manifest_model_key(self, *, state: ChargerState | None = None) -> str | None:
        """Infer the firmware manifest model key from the charger firmware string."""
        state = state or self.data
        version = state.firmware_version
        if not version:
            return None
        parts = str(version).strip().split("_")
        if len(parts) < 3:
            return None
        return "_".join(parts[:-1])

    def _refresh_available_firmware_files(self, *, state: ChargerState | None = None) -> None:
        state = state or self.data
        files = sorted(
            state.firmware_manifest_entries,
            key=lambda filename: _firmware_version_key(
                state.firmware_manifest_entries[filename].get("version")
            ),
        )
        state.available_firmware_files = files
        if state.selected_firmware_file not in files:
            state.selected_firmware_file = files[-1] if files else None

    def firmware_file_path(self, filename: str) -> Path:
        if self.firmware_directory is None:
            raise RuntimeError("Firmware directory is not configured")
        return self.firmware_directory / Path(filename).name

    async def _async_download_firmware_for_install(
        self, filename: str, *, state: ChargerState | None = None
    ) -> Path:
        state = state or self.data
        entry = state.firmware_manifest_entries.get(filename)
        if not entry:
            raise RuntimeError(f"No manifest entry was found for firmware file: {filename}")

        target_path = self.firmware_file_path(filename)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        lock_key = Path(filename).name
        lock = self._firmware_download_locks.setdefault(lock_key, asyncio.Lock())
        async with lock:
            if await self._async_firmware_file_matches_manifest(target_path, entry):
                _LOGGER.info("Using cached firmware file: %s", target_path)
                return target_path

            await asyncio.to_thread(_unlink_if_exists, target_path)
            try:
                await self._async_download_firmware(target_path, entry)
                if not await self._async_firmware_file_matches_manifest(target_path, entry):
                    raise RuntimeError(f"Downloaded firmware failed checksum validation: {filename}")
            except Exception:
                await asyncio.to_thread(_unlink_if_exists, target_path)
                raise
        return target_path

    async def _async_firmware_file_matches_manifest(
        self, path: Path, entry: dict[str, Any]
    ) -> bool:
        if not path.is_file():
            return False
        expected_size = entry.get("size")
        if expected_size is not None and path.stat().st_size != expected_size:
            return False
        expected_md5 = entry.get("checksum_md5")
        actual_md5 = await asyncio.to_thread(_compute_md5, path)
        return actual_md5 == expected_md5

    async def _async_download_firmware(self, target_path: Path, entry: dict[str, Any]) -> None:
        download_url = str(entry["url"])
        temp_path = target_path.with_suffix(f"{target_path.suffix}.download")
        if temp_path.exists():
            temp_path.unlink()

        try:
            _assert_safe_url(download_url)
            async with aiohttp.ClientSession() as session:
                async with session.get(download_url, allow_redirects=False) as response:
                    if response.status in {301, 302, 303, 307, 308}:
                        redirect_url = str(response.headers.get("Location", ""))
                        if not redirect_url:
                            raise RuntimeError("Firmware redirect had no Location header")
                        _assert_safe_url(redirect_url)
                        async with session.get(redirect_url, allow_redirects=False) as r2:
                            if r2.status != 200:
                                raise RuntimeError(f"Firmware download failed with HTTP {r2.status}")
                            data = await r2.read()
                    elif response.status != 200:
                        raise RuntimeError(f"Firmware download failed with HTTP {response.status}")
                    else:
                        data = await response.read()
        except Exception as err:
            raise RuntimeError(f"Unable to download firmware from {download_url}: {err}") from err

        await asyncio.to_thread(temp_path.write_bytes, data)
        try:
            if not await self._async_firmware_file_matches_manifest(temp_path, entry):
                raise RuntimeError(
                    f"Downloaded file checksum or size did not match manifest for {entry['filename']}"
                )
            await asyncio.to_thread(temp_path.replace, target_path)
        finally:
            if temp_path.exists():
                temp_path.unlink()

    async def async_install_firmware_file(
        self, filename: str, charge_point_id: str | None = None
    ) -> dict[str, Any]:
        """Download a manifest firmware file and send OCPP UpdateFirmware."""
        await self.async_refresh_firmware_manifest(charge_point_id=charge_point_id)
        state = self.state_for_charge_point(charge_point_id)
        filename = Path(str(filename)).name
        if filename not in state.available_firmware_files:
            raise RuntimeError(f"Unknown firmware file from manifest: {filename}")
        if not self.command_session_available(charge_point_id or state.charge_point_id):
            raise RuntimeError("No charger connected")
        if not state.firmware_server_host:
            raise RuntimeError("Unable to determine the firmware server host for the charger")

        state.selected_firmware_file = filename
        state.firmware_transfer_progress = {
            "active": True,
            "event": "preparing",
            "filename": filename,
            "percent": 0,
            "status": "Preparing firmware file",
            "updated_at": datetime.now(UTC).isoformat(),
        }
        self._persist_charge_point_state(charge_point_id, state, persist=True)
        await self._async_download_firmware_for_install(filename, state=state)

        retrieve_at = (datetime.now(UTC) + timedelta(seconds=60)).replace(microsecond=0)
        location = (
            f"ftp://{state.firmware_server_host}:{self.firmware_public_port}/"
            f"ChargerFirmware/{quote(filename)}"
        )
        result = await self.async_update_firmware(
            location=location,
            retrieve_date=retrieve_at.isoformat().replace("+00:00", "Z"),
            retries=1,
            retry_interval=60,
            charge_point_id=charge_point_id,
        )
        return result

    def record_firmware_transfer_event(self, event: dict[str, Any]) -> None:
        """Observe firmware-server events and expose transfer progress to the UI."""
        event_type = event.get("event")
        filename = event.get("filename") or event.get("requested_filename")
        if event_type == "file_sent" and filename:
            _LOGGER.info("Firmware file served from central cache: %s", Path(str(filename)).name)
        charge_point_id = self._charge_point_id_for_firmware_event(event)
        if not charge_point_id:
            return

        state = self.state_for_charge_point(charge_point_id)
        progress = dict(state.firmware_transfer_progress or {})
        now = datetime.now(UTC).isoformat()
        event_name = str(event_type or "unknown")
        _noisy_events = {"control_frame_sent", "control_frame_received", "chunk_sent"}
        if event_name in _noisy_events:
            _LOGGER.debug("Firmware transfer event: %s (update_state=%s)", event_name, state.firmware_update_state)
        else:
            _LOGGER.info("Firmware transfer event: %s (update_state=%s)", event_name, state.firmware_update_state)
        if state.firmware_update_state in {"Cancelled", "Installed", "Failed"} and event_name not in {"transfer_cancelled"}:
            _LOGGER.warning("Firmware event %s dropped — update state is %s", event_name, state.firmware_update_state)
            return
        _retry_transfer_events = {"connect", "request_received", "control_frame_sent", "control_frame_received", "chunk_sent", "checksum_missing", "checksum_ok", "file_sent", "disconnect", "download_started", "overlapping_request", "socket_timeout"}
        if state.firmware_update_state in {"Downloaded", "Installing"} and event_name in _retry_transfer_events:
            if event_name == "disconnect":
                self._mark_firmware_installing_after_disconnect(state, now=now)
                state.firmware_transfer_progress = dict(state.firmware_transfer_progress or {})
                self._persist_charge_point_state(charge_point_id, state, persist=False)
            return
        terminal_errors = {
            "file_not_found",
            "checksum_mismatch",
            "chunk_read_error",
            "client_error",
            "socket_timeout",
        }

        if event_name == "download_started":
            chunk_count = _to_int(event.get("chunk_count"), 0)
            progress = {
                "active": True,
                "event": event_name,
                "filename": Path(str(filename or "")).name or filename,
                "remote": event.get("remote"),
                "trace": event.get("trace"),
                "bytes_total": _to_int(event.get("filesize"), 0),
                "bytes_sent": 0,
                "chunk_size": _to_int(event.get("chunk_size"), 0),
                "chunks_total": chunk_count,
                "chunks_sent": 0,
                "percent": 0,
                "status": "Transfer started",
                "updated_at": now,
            }
        elif event_name == "request_received":
            progress.update(
                {
                    "active": True,
                    "event": event_name,
                    "filename": Path(str(filename or progress.get("filename") or "")).name,
                    "remote": event.get("remote") or progress.get("remote"),
                    "trace": event.get("trace") or progress.get("trace"),
                    "status": "Firmware request received",
                    "updated_at": now,
                }
            )
        elif event_name == "control_frame_sent":
            payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            if payload.get("packnum") or payload.get("filesize"):
                progress.update(
                    {
                        "active": True,
                        "event": event_name,
                        "remote": event.get("remote") or progress.get("remote"),
                        "trace": event.get("trace") or progress.get("trace"),
                        "bytes_total": _to_int(payload.get("filesize"), _to_int(progress.get("bytes_total"), 0)),
                        "chunks_total": _to_int(payload.get("packnum"), _to_int(progress.get("chunks_total"), 0)),
                        "status": "Transfer metadata sent",
                        "updated_at": now,
                    }
                )
        elif event_name == "control_frame_received":
            payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            if "packsn" in payload:
                packsn = _to_int(payload.get("packsn"), -1)
                chunks_sent = max(_to_int(progress.get("chunks_sent"), 0), packsn + 1)
                chunks_total = _to_int(progress.get("chunks_total"), 0)
                percent = round((chunks_sent / chunks_total) * 100, 1) if chunks_total > 0 else progress.get("percent")
                progress.update(
                    {
                        "active": True,
                        "event": event_name,
                        "remote": event.get("remote") or progress.get("remote"),
                        "trace": event.get("trace") or progress.get("trace"),
                        "chunks_sent": chunks_sent,
                        "percent": percent,
                        "status": "Transferring firmware",
                        "updated_at": now,
                    }
                )
        elif event_name == "chunk_sent":
            packsn = _to_int(event.get("packsn"), -1)
            chunks_sent = max(_to_int(progress.get("chunks_sent"), 0), packsn + 1)
            chunks_total = _to_int(progress.get("chunks_total"), 0)
            percent = round((chunks_sent / chunks_total) * 100, 1) if chunks_total > 0 else None
            progress.update(
                {
                    "active": True,
                    "event": event_name,
                    "remote": event.get("remote") or progress.get("remote"),
                    "trace": event.get("trace") or progress.get("trace"),
                    "chunks_sent": chunks_sent,
                    "bytes_sent": _to_int(progress.get("bytes_sent"), 0) + _to_int(event.get("bytes"), 0),
                    "percent": percent,
                    "status": "Transferring firmware",
                    "updated_at": now,
                }
            )
        elif event_name == "checksum_ok":
            self._mark_firmware_download_complete(state, filename or progress.get("filename"), now)
            progress = dict(state.firmware_transfer_progress or {})
        elif event_name == "file_sent":
            if progress.get("event") not in terminal_errors and progress.get("status") != "Transfer failed":
                self._mark_firmware_download_complete(state, filename or progress.get("filename"), now)
                progress = dict(state.firmware_transfer_progress or {})
        elif event_name == "disconnect" and (
            state.firmware_update_state in {"Downloaded", "Installing"}
            or (
                _to_int(progress.get("chunks_total"), 0) > 0
                and _to_int(progress.get("chunks_sent"), 0) >= _to_int(progress.get("chunks_total"), 0)
            )
            or float(progress.get("percent") or 0) >= 100
        ):
            if state.firmware_update_state not in {"Downloaded", "Installing"}:
                self._mark_firmware_download_complete(state, filename or progress.get("filename"), now)
            self._mark_firmware_installing_after_disconnect(state, now=now)
            progress = dict(state.firmware_transfer_progress or {})
        elif event_name == "checksum_missing":
            # This charger always closes FTP connections without sending checksum ACK —
            # whether it's a partial session or the final one. Treat as non-fatal; the
            # subsequent disconnect event drives the Downloaded → Installing transition.
            self._mark_firmware_download_complete(state, filename or progress.get("filename"), now)
            progress = dict(state.firmware_transfer_progress or {})
        elif event_name in terminal_errors:
            progress.update(
                {
                    "active": False,
                    "event": event_name,
                    "filename": filename or progress.get("filename"),
                    "error": event.get("error") or event_name.replace("_", " "),
                    "status": "Transfer failed",
                    "updated_at": now,
                }
            )
            state.firmware_update_state = "Failed"
            state.firmware_update_completed_at = datetime.now(UTC)
            state.firmware_update_failure_reason = str(progress.get("error") or event_name)
        elif event_name == "transfer_cancelled":
            state.firmware_update_state = "Cancelled"
            state.firmware_update_completed_at = datetime.now(UTC)
            state.firmware_update_expected_reconnect_by = None
            state.firmware_update_failure_reason = "Cancelled by user"
            progress.update(
                {
                    "active": False,
                    "event": event_name,
                    "filename": filename or progress.get("filename"),
                    "error": "Cancelled by user",
                    "status": "Firmware transfer cancelled",
                    "updated_at": now,
                }
            )
        else:
            progress.update(
                {
                    "event": event_name,
                    "filename": filename or progress.get("filename"),
                    "status": event_name.replace("_", " ").title(),
                    "updated_at": now,
                }
            )

        state.firmware_transfer_progress = progress
        terminal = state.firmware_update_state in {"Installed", "Failed", "Cancelled"}
        self._persist_charge_point_state(charge_point_id, state, persist=terminal)
        if terminal:
            self._active_firmware_charge_point_id = None

    def cancel_firmware_update(self, charge_point_id: str | None = None, reason: str = "Cancelled by user") -> dict[str, Any]:
        """Mark the active firmware transfer as cancelled."""
        state = self.state_for_charge_point(charge_point_id)
        progress = dict(state.firmware_transfer_progress or {})
        if state.firmware_update_state == "Cancelled":
            return {
                "ok": True,
                "status": "Cancelled",
                "firmware_transfer_progress": progress,
            }
        event_name = str(progress.get("event") or "")
        transferable_events = {
            "download_started",
            "request_received",
            "control_frame_sent",
            "control_frame_received",
            "chunk_sent",
            "ocpp_downloading",
        }
        if state.firmware_update_state not in {"Downloading"} and event_name not in transferable_events:
            raise RuntimeError("No active firmware file transfer to cancel")

        now = datetime.now(UTC)
        state.firmware_update_state = "Cancelled"
        state.firmware_update_completed_at = now
        state.firmware_update_expected_reconnect_by = None
        state.firmware_update_failure_reason = reason
        progress.update(
            {
                "active": False,
                "event": "transfer_cancelled",
                "filename": progress.get("filename") or state.firmware_update_target_file,
                "error": reason,
                "status": "Firmware transfer cancelled",
                "updated_at": now.isoformat(),
            }
        )
        state.firmware_transfer_progress = progress
        self._persist_charge_point_state(charge_point_id, state, persist=True)
        return {
            "ok": True,
            "status": "Cancelled",
            "firmware_transfer_progress": progress,
        }

    def _charge_point_id_for_firmware_event(self, event: dict[str, Any]) -> str | None:
        filename = event.get("filename") or event.get("requested_filename")
        filename_name = Path(str(filename or "")).name
        trace = event.get("trace")

        # Match by filename or trace against each live charger state
        for cpid, state in self._charger_states.items():
            active_names = {
                Path(str(value)).name
                for value in {state.firmware_update_target_file, state.selected_firmware_file}
                if value
            }
            if filename_name and filename_name in active_names:
                return cpid
            active_progress = state.firmware_transfer_progress or {}
            if trace and trace == active_progress.get("trace"):
                return cpid

        remote = str(event.get("remote") or "").strip()
        remote_host = remote.rsplit(":", 1)[0] if ":" in remote else remote
        if remote_host:
            for item in self._connected_charge_points.values():
                if remote_host in {
                    str(item.get("remote_address") or ""),
                    str(item.get("local_ip_address") or ""),
                }:
                    return str(item.get("charge_point_id") or "").strip() or None
            for cpid, state in self._charger_states.items():
                if remote_host in {
                    str(state.websocket_remote_address or ""),
                    str(state.local_ip_address or ""),
                }:
                    return cpid

        if self._active_firmware_charge_point_id:
            return self._active_firmware_charge_point_id
        return self._primary_charge_point_id

    async def async_update_firmware(
        self,
        location: str,
        retrieve_date: str,
        retries: int | None = None,
        retry_interval: int | None = None,
        charge_point_id: str | None = None,
    ) -> dict[str, Any]:
        """Issue an OCPP UpdateFirmware command."""
        state = self.state_for_charge_point(charge_point_id)
        if self._firmware_update_in_progress(state=state):
            raise RuntimeError("A firmware update is already in progress")

        payload: dict[str, Any] = {
            "location": location,
            "retrieveDate": retrieve_date,
        }
        if retries is not None:
            payload["retries"] = retries
        if retry_interval is not None:
            payload["retryInterval"] = retry_interval

        state.last_update_firmware_request = dict(payload)
        self._start_firmware_update_session(location, state=state)
        self._persist_charge_point_state(charge_point_id, state, persist=True)
        try:
            result = await self._ocpp_call("UpdateFirmware", payload, charge_point_id=charge_point_id)
        except Exception as err:
            err_str = str(err)
            is_timeout = "timed out" in err_str.lower()
            if is_timeout:
                # Charger didn't ACK the OCPP command in time but likely received it
                # and will start the FTP download. Keep state as Requested so FTP
                # transfer events are not blocked by the guard in record_firmware_transfer_event.
                state.firmware_transfer_progress = {
                    **(state.firmware_transfer_progress or {}),
                    "active": True,
                    "event": "update_requested",
                    "status": "Command sent — waiting for charger to begin download",
                    "updated_at": datetime.now(UTC).isoformat(),
                }
                self._persist_charge_point_state(charge_point_id, state, persist=True)
                return {"status": "Accepted", "note": "OCPP ack timed out; charger may still proceed"}
            state.firmware_update_state = "Failed"
            state.firmware_update_failure_reason = err_str
            state.firmware_transfer_progress = {
                **(state.firmware_transfer_progress or {}),
                "active": False,
                "event": "update_request_failed",
                "error": err_str,
                "status": "Update request failed",
                "updated_at": datetime.now(UTC).isoformat(),
            }
            self._persist_charge_point_state(charge_point_id, state, persist=True)
            raise
        _LOGGER.info("UpdateFirmware requested: %s", payload)
        self._persist_charge_point_state(charge_point_id, state, persist=True)
        return result

    def _firmware_update_in_progress(self, *, state: ChargerState | None = None) -> bool:
        state = state or self.data
        return state.firmware_update_state in {"Requested", "Downloading", "Downloaded", "Installing"}

    def _start_firmware_update_session(self, location: str, *, state: ChargerState | None = None) -> None:
        state = state or self.data
        cpid = str(state.charge_point_id or "").strip() or None
        self._active_firmware_charge_point_id = cpid
        target_file = state.selected_firmware_file or Path(location).name or None
        state.firmware_status = None
        state.firmware_update_state = "Requested"
        state.firmware_update_target_file = target_file
        state.firmware_update_target_version = _derive_firmware_version_from_filename(target_file)
        state.firmware_update_previous_version = state.firmware_version
        state.firmware_update_started_at = datetime.now(UTC)
        state.firmware_update_download_completed_at = None
        state.firmware_update_install_started_at = None
        state.firmware_update_expected_reconnect_by = None
        state.firmware_update_completed_at = None
        state.firmware_update_failure_reason = None
        state.firmware_transfer_progress = {
            "active": True,
            "event": "update_requested",
            "filename": target_file,
            "percent": 0,
            "status": "Waiting for charger download request",
            "updated_at": state.firmware_update_started_at.isoformat(),
        }

    def _mark_firmware_download_complete(
        self,
        state: ChargerState,
        filename: Any = None,
        now: str | None = None,
    ) -> None:
        updated_at = now or datetime.now(UTC).isoformat()
        state.firmware_update_state = "Downloaded"
        state.firmware_update_download_completed_at = state.firmware_update_download_completed_at or datetime.now(UTC)
        progress = dict(state.firmware_transfer_progress or {})
        progress.update(
            {
                "active": True,
                "event": "applying",
                "filename": Path(str(filename or progress.get("filename") or "")).name,
                "percent": 100,
                "status": "Transfer complete - charger is applying firmware and will restart",
                "updated_at": updated_at,
            }
        )
        state.firmware_transfer_progress = progress

    def _mark_firmware_installing_after_disconnect(
        self,
        state: ChargerState,
        *,
        now: str | None = None,
    ) -> None:
        if state.firmware_update_state not in {"Downloaded", "Installing"}:
            return
        updated_at = now or datetime.now(UTC).isoformat()
        current_time = datetime.now(UTC)
        state.firmware_update_state = "Installing"
        state.firmware_update_install_started_at = state.firmware_update_install_started_at or current_time
        state.firmware_update_expected_reconnect_by = current_time + FIRMWARE_INSTALLING_TIMEOUT
        progress = dict(state.firmware_transfer_progress or {})
        progress.update(
            {
                "active": True,
                "event": "restarting",
                "filename": progress.get("filename") or state.firmware_update_target_file,
                "percent": 100,
                "status": "Charger is restarting to apply firmware",
                "updated_at": updated_at,
            }
        )
        state.firmware_transfer_progress = progress

    def _mark_firmware_result_from_observed_version(self, state: ChargerState) -> None:
        if state.firmware_update_state not in {"Requested", "Downloading", "Downloaded", "Installing", "Installed"}:
            return
        target = state.firmware_update_target_version
        current = state.firmware_version
        _LOGGER.info(
            "Firmware version check: update_state=%s target=%s current=%s match=%s",
            state.firmware_update_state, target, current,
            _firmware_version_key(target) == _firmware_version_key(current) if target and current else "n/a",
        )
        if not target or not current:
            # No target version recorded — stuck from a timed-out request; clear it
            if state.firmware_update_state in {"Requested", "Downloading"}:
                state.firmware_update_state = "Failed"
                state.firmware_update_failure_reason = "Update request did not complete before charger rebooted"
                state.firmware_transfer_progress = {
                    **(state.firmware_transfer_progress or {}),
                    "active": False,
                    "event": "stale_cleared",
                    "status": "Update state cleared on reconnect",
                }
            return
        progress = dict(state.firmware_transfer_progress or {})
        if _firmware_version_key(target) == _firmware_version_key(current):
            state.firmware_update_state = "Installed"
            state.firmware_update_completed_at = datetime.now(UTC)
            state.firmware_update_failure_reason = None
            state.firmware_update_expected_reconnect_by = None
            progress.update(
                {
                    "active": False,
                    "event": "installed",
                    "filename": progress.get("filename") or state.firmware_update_target_file,
                    "percent": 100,
                    "status": f"Firmware installed: {current}",
                    "updated_at": state.firmware_update_completed_at.isoformat(),
                }
            )
            state.firmware_transfer_progress = progress
            return

        state.firmware_update_state = "Failed"
        state.firmware_update_completed_at = datetime.now(UTC)
        state.firmware_update_failure_reason = f"Charger restarted on {current}, expected {target}"
        state.firmware_update_expected_reconnect_by = None
        progress.update(
            {
                "active": False,
                "event": "version_mismatch",
                "filename": progress.get("filename") or state.firmware_update_target_file,
                "percent": 100,
                "error": state.firmware_update_failure_reason,
                "status": "Firmware update did not apply",
                "updated_at": state.firmware_update_completed_at.isoformat(),
            }
        )
        state.firmware_transfer_progress = progress

    def _apply_firmware_ocpp_status(self, state: ChargerState, status: Any) -> None:
        if status in (None, ""):
            return
        normalized = str(status).strip()
        now = datetime.now(UTC)
        progress = dict(state.firmware_transfer_progress or {})
        state.firmware_status = normalized

        if normalized == "Downloading":
            state.firmware_update_state = "Downloading"
            progress.update(
                {
                    "active": True,
                    "event": "ocpp_downloading",
                    "filename": progress.get("filename") or state.firmware_update_target_file,
                    "status": "Charger is downloading firmware",
                    "updated_at": now.isoformat(),
                }
            )
        elif normalized == "Downloaded":
            self._mark_firmware_download_complete(state, progress.get("filename") or state.firmware_update_target_file, now.isoformat())
            return
        elif normalized in {"Installing", "InstallScheduled"}:
            state.firmware_update_state = "Installing"
            state.firmware_update_install_started_at = state.firmware_update_install_started_at or now
            state.firmware_update_expected_reconnect_by = now + FIRMWARE_INSTALLING_TIMEOUT
            progress.update(
                {
                    "active": True,
                    "event": "ocpp_installing",
                    "filename": progress.get("filename") or state.firmware_update_target_file,
                    "percent": 100,
                    "status": "Charger is applying firmware and restarting",
                    "updated_at": now.isoformat(),
                }
            )
        elif normalized == "Installed":
            state.firmware_update_state = "Installed"
            state.firmware_update_completed_at = now
            state.firmware_update_failure_reason = None
            state.firmware_update_expected_reconnect_by = None
            progress.update(
                {
                    "active": False,
                    "event": "installed",
                    "filename": progress.get("filename") or state.firmware_update_target_file,
                    "percent": 100,
                    "status": "Firmware installed",
                    "updated_at": now.isoformat(),
                }
            )
        elif normalized in {"DownloadFailed", "InstallationFailed", "InvalidSignature", "SignatureVerifiedFailed"}:
            state.firmware_update_state = "Failed"
            state.firmware_update_completed_at = now
            state.firmware_update_failure_reason = normalized
            state.firmware_update_expected_reconnect_by = None
            progress.update(
                {
                    "active": False,
                    "event": "firmware_status_failed",
                    "filename": progress.get("filename") or state.firmware_update_target_file,
                    "percent": 100,
                    "error": normalized,
                    "status": "Firmware update failed",
                    "updated_at": now.isoformat(),
                }
            )
        else:
            progress.update(
                {
                    "event": "firmware_status",
                    "filename": progress.get("filename") or state.firmware_update_target_file,
                    "status": f"Charger status: {normalized}",
                    "updated_at": now.isoformat(),
                }
            )
        state.firmware_transfer_progress = progress

    def firmware_action_for_version(self, target_version: Any, charge_point_id: str | None = None) -> str:
        state = self.state_for_charge_point(charge_point_id)
        comparison = _compare_firmware_versions(target_version, state.firmware_version)
        if comparison < 0:
            return "Downgrade"
        if comparison > 0:
            return "Upgrade"
        return "Reinstall"

    def register_ocpp_caller(self, charge_point_id: str | None, caller: Any) -> None:
        charge_point_id = str(charge_point_id or "").strip()
        if not charge_point_id:
            return
        self._ocpp_callers[charge_point_id] = caller

    def unregister_ocpp_caller(self, charge_point_id: str | None, caller: Any | None = None) -> None:
        charge_point_id = str(charge_point_id or "").strip()
        if not charge_point_id:
            return
        existing = self._ocpp_callers.get(charge_point_id)
        if caller is None or existing is caller:
            self._ocpp_callers.pop(charge_point_id, None)

    def set_charge_point_command_authorizer(
        self,
        authorizer: Callable[[str | None], bool] | None,
    ) -> None:
        """Register a policy callback for outbound OCPP commands."""
        self._charge_point_command_authorizer = authorizer

    def charge_point_can_receive_commands(self, charge_point_id: str | None) -> bool:
        if self._charge_point_command_authorizer is None:
            return True
        return bool(self._charge_point_command_authorizer(charge_point_id))

    async def _ocpp_call(
        self,
        action: str,
        payload: dict[str, Any],
        timeout: int = 20,
        charge_point_id: str | None = None,
    ) -> dict[str, Any]:
        target_charge_point_id = charge_point_id
        target_key = str(target_charge_point_id or "").strip()
        if not target_key:
            raise RuntimeError("A charge point identity is required")
        caller = self._ocpp_callers.get(target_key)
        if caller is None:
            raise RuntimeError("No charger connected")
        if not self.charge_point_can_receive_commands(target_charge_point_id):
            raise RuntimeError("Charger is not adopted")
        return await caller.async_call(action, payload, timeout=timeout)

    async def async_record_firmware_status(self, charge_point_id: str | None, payload: dict[str, Any]) -> None:
        cpid = str(charge_point_id or "").strip()
        status = payload.get("status")
        state = self.state_for_charge_point(cpid)
        state.last_seen = datetime.now(UTC)
        self._apply_firmware_ocpp_status(state, status)
        _LOGGER.info("FirmwareStatusNotification charger=%s: %s", cpid, status)
        self._persist_charge_point_state(cpid, state, persist=state.firmware_update_state in {"Installed", "Failed"}, notify=True)

    async def async_record_diagnostics_status(self, charge_point_id: str | None, payload: dict[str, Any]) -> None:
        _LOGGER.info("DiagnosticsStatusNotification charger=%s: %s", charge_point_id, payload.get("status"))

    # ── OCPP frame logging ───────────────────────────────────────────────

    def record_portal_action(
        self,
        action: str,
        detail: str,
        response: Any = "Success",
        success: bool = True,
        charge_point_id: str | None = None,
        user: str = "You",
        via: str = "Portal",
    ) -> None:
        """Record a user-visible portal action for the Settings -> Logs view."""
        state = self.state_for_charge_point(charge_point_id)
        entry = {
            "ts": datetime.now(UTC).isoformat(),
            "user": user,
            "action": action,
            "detail": detail,
            "response": _log_response(response),
            "success": bool(success),
            "via": via,
        }
        state.action_log.append(entry)
        if len(state.action_log) > MAX_STORED_ACTION_LOGS:
            state.action_log = state.action_log[-MAX_STORED_ACTION_LOGS:]
        self._persist_charge_point_state(charge_point_id, state, persist=True)

    def clear_action_log(self, charge_point_id: str | None = None) -> int:
        """Clear the persisted portal action log."""
        state = self.state_for_charge_point(charge_point_id)
        count = len(state.action_log)
        state.action_log = []
        self._persist_charge_point_state(charge_point_id, state, persist=True)
        return count

    def record_ocpp_frame(self, **kwargs: Any) -> None:
        entry = {"ts": datetime.now(UTC).isoformat(), **kwargs}
        charge_point_id = kwargs.get("charge_point_id")
        state = self.state_for_charge_point(charge_point_id)
        state.ocpp_frame_history.append(entry)
        if len(state.ocpp_frame_history) > MAX_STORED_OCPP_FRAMES:
            state.ocpp_frame_history = state.ocpp_frame_history[-MAX_STORED_OCPP_FRAMES:]
        self._persist_charge_point_state(charge_point_id, state, persist=True, notify=False)

    def record_unsupported_ocpp_action(self, action: str, payload: Any) -> None:
        _LOGGER.warning("Unsupported OCPP action: %s payload=%s", action, payload)

    def record_authorize_exchange(
        self,
        req: Any,
        resp: Any,
        *,
        charge_point_id: str | None = None,
        session_id: str | None = None,
    ) -> None:
        del req, resp, charge_point_id, session_id
        pass

    def record_start_transaction_exchange(
        self,
        req: Any,
        resp: Any,
        *,
        charge_point_id: str | None = None,
        session_id: str | None = None,
    ) -> None:
        del req, resp, charge_point_id, session_id
        pass

    def record_stop_transaction_exchange(
        self,
        req: Any,
        resp: Any,
        *,
        charge_point_id: str | None = None,
        session_id: str | None = None,
    ) -> None:
        del req, resp, charge_point_id, session_id
        pass

    def record_call_error(self, **kwargs: Any) -> None:
        _LOGGER.warning("OCPP CALLERROR: %s", kwargs)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _coerce_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _compute_md5(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _unlink_if_exists(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def _derive_firmware_version_from_filename(filename: str | None) -> str | None:
    if not filename:
        return None
    stem = Path(filename).stem
    parts = stem.split("_")
    if len(parts) < 3:
        return None
    return "_".join(parts[-3:])


def _firmware_version_key(value: Any) -> tuple[int, ...]:
    if value is None:
        return ()
    tail = str(value).strip().split("_")[-1]
    parts = re.findall(r"\d+", tail)
    return tuple(int(part) for part in parts)


def _compare_firmware_versions(target: Any, current: Any) -> int:
    target_key = _firmware_version_key(target)
    current_key = _firmware_version_key(current)
    if not target_key or not current_key:
        return 0
    width = max(len(target_key), len(current_key))
    target_key = target_key + (0,) * (width - len(target_key))
    current_key = current_key + (0,) * (width - len(current_key))
    return (target_key > current_key) - (target_key < current_key)


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _is_car_plugged_in_status(status: str | None) -> bool | None:
    if status is None:
        return None
    return status in {
        "Preparing",
        "Charging",
        "SuspendedEVSE",
        "SuspendedEV",
        "Finishing",
    }


def _is_charging_status(status: str | None) -> bool:
    return status == "Charging"


def _normalise_meter_sample_value(
    measurand: str, unit: str | None, value: float | None
) -> float | None:
    if value is None:
        return None
    if measurand == "Power.Active.Import" and unit == "kW":
        return value * 1000
    if measurand == "Energy.Active.Import.Register" and unit == "kWh":
        return value * 1000
    return value


def _pick_total_energy_sample(
    samples_wh: list[float], previous_total_wh: float | None
) -> float | None:
    candidates = [sample for sample in samples_wh if sample is not None]
    if not candidates:
        return None

    non_zero = [sample for sample in candidates if sample > 0]
    pool = non_zero or candidates
    if previous_total_wh is None:
        return max(pool)

    non_decreasing = [sample for sample in pool if sample >= previous_total_wh]
    if non_decreasing:
        return min(non_decreasing, key=lambda sample: sample - previous_total_wh)
    return min(pool, key=lambda sample: abs(sample - previous_total_wh))


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_ocpp_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


SCHEDULE_DAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
SCHEDULE_DAY_INDEX = {day.lower(): index for index, day in enumerate(SCHEDULE_DAYS)}


def _next_schedule_id(existing: dict[str, dict[str, Any]]) -> str:
    numeric_ids = [
        int(schedule_id) for schedule_id in existing
        if str(schedule_id).isdigit()
    ]
    if numeric_ids:
        return str(max(numeric_ids) + 1)
    return str(int(datetime.now(UTC).timestamp() * 1000))


def _normalise_schedule(schedule: dict[str, Any], schedule_id: str) -> dict[str, Any]:
    name = str(schedule.get("name") or "Schedule").strip()[:60] or "Schedule"
    start = _normalise_time(schedule.get("start") or schedule.get("start_time") or "00:00")
    end = _normalise_time(schedule.get("end") or schedule.get("end_time") or "01:00")
    days = [
        day for day in SCHEDULE_DAYS
        if day in {str(value) for value in schedule.get("days", [])}
    ]
    if not days:
        days = list(SCHEDULE_DAYS)
    current_a = _coerce_int(schedule.get("current_a"))
    if current_a is None:
        current_a = 32
    current_a = max(6, min(32, current_a))
    return {
        "id": schedule_id,
        "name": name,
        "enabled": bool(schedule.get("enabled")),
        "start": start,
        "end": end,
        "days": days,
        "current_a": current_a,
    }


def _normalise_schedule_list(schedules: Any) -> list[dict[str, Any]]:
    if not isinstance(schedules, list):
        return []
    existing: dict[str, dict[str, Any]] = {}
    active_seen = False
    normalised: list[dict[str, Any]] = []
    for raw in schedules:
        if not isinstance(raw, dict):
            continue
        schedule_id = str(raw.get("id") or _next_schedule_id(existing))
        item = _normalise_schedule(raw, schedule_id)
        if item["enabled"]:
            if active_seen:
                item["enabled"] = False
            active_seen = True
        existing[schedule_id] = item
        normalised.append(item)
    return normalised


def _schedule_with_ocpp_response(
    schedule: dict[str, Any] | None, response: dict[str, Any] | None
) -> dict[str, Any]:
    result = dict(schedule or {})
    if response is not None:
        result["_ocpp_response"] = response
    return result


def _build_charging_schedule_payload(schedule: dict[str, Any]) -> dict[str, Any]:
    days = [
        str(day).lower()
        for day in schedule.get("days", [])
        if str(day).lower() in SCHEDULE_DAY_INDEX
    ]
    all_days_selected = not days or set(days) == set(SCHEDULE_DAY_INDEX)
    if all_days_selected:
        days = [day.lower() for day in SCHEDULE_DAYS]

    start_hours, start_minutes = _split_hhmm(schedule.get("start") or "00:00")
    end_hours, end_minutes = _split_hhmm(schedule.get("end") or "01:00")
    limit_a = max(6, min(32, _coerce_int(schedule.get("current_a")) or 32))
    local_tz = datetime.now().astimezone().tzinfo or UTC

    if all_days_selected:
        recurrency = "Daily"
        cycle_seconds = 86400
        now_utc = datetime.now(UTC)
        anchor = (now_utc - timedelta(days=now_utc.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        now_local = datetime.now(local_tz)
        local_day = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        local_start = local_day.replace(hour=start_hours, minute=start_minutes)
        local_end = local_day.replace(hour=end_hours, minute=end_minutes)
        if local_end <= local_start:
            local_end += timedelta(days=1)

        utc_start = local_start.astimezone(UTC)
        utc_end = local_end.astimezone(UTC)
        start_offset = utc_start.hour * 3600 + utc_start.minute * 60
        duration_seconds = int((utc_end - utc_start).total_seconds())
        intervals: list[tuple[int, int]] = []
        _add_circular_schedule_interval(intervals, start_offset, duration_seconds, cycle_seconds)
    else:
        recurrency = "Weekly"
        cycle_seconds = 604800
        now_utc = datetime.now(UTC)
        days_since_monday = now_utc.weekday()
        anchor = (now_utc - timedelta(days=days_since_monday)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        now_local = datetime.now(local_tz)
        local_week_start = (now_local - timedelta(days=now_local.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        intervals: list[tuple[int, int]] = []
        for day in sorted(set(days), key=lambda value: SCHEDULE_DAY_INDEX[value]):
            local_day = local_week_start + timedelta(days=SCHEDULE_DAY_INDEX[day])
            local_start = local_day.replace(hour=start_hours, minute=start_minutes)
            local_end = local_day.replace(hour=end_hours, minute=end_minutes)
            if local_end <= local_start:
                local_end += timedelta(days=1)

            utc_start = local_start.astimezone(UTC)
            utc_end = local_end.astimezone(UTC)
            start_offset = int((utc_start - anchor).total_seconds())
            duration_seconds = int((utc_end - utc_start).total_seconds())
            _add_circular_schedule_interval(
                intervals, start_offset, duration_seconds, cycle_seconds
            )

    periods = _schedule_periods_from_intervals(intervals, cycle_seconds, limit_a)
    profile = {
        "stackLevel": 0,
        "chargingProfilePurpose": "TxDefaultProfile",
        "chargingProfileKind": "Recurring",
        "recurrencyKind": recurrency,
        "chargingSchedule": {
            "chargingRateUnit": "A",
            "startSchedule": anchor.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "chargingSchedulePeriod": [
                {"startPeriod": str(start), "limit": str(limit)}
                for start, limit in periods
            ],
        },
        "chargingProfileId": 1,
    }
    return {"connectorId": 0, "csChargingProfiles": profile}


def _add_circular_schedule_interval(
    intervals: list[tuple[int, int]],
    start_offset: int,
    duration_seconds: int,
    cycle_seconds: int,
) -> None:
    if duration_seconds >= cycle_seconds:
        intervals.append((0, cycle_seconds))
        return

    start_offset %= cycle_seconds
    end_offset = (start_offset + duration_seconds) % cycle_seconds
    if start_offset < end_offset:
        intervals.append((start_offset, end_offset))
        return

    intervals.append((0, end_offset))
    intervals.append((start_offset, cycle_seconds))


def _schedule_periods_from_intervals(
    intervals: list[tuple[int, int]], cycle_seconds: int, limit_a: int
) -> list[tuple[int, int]]:
    if not intervals:
        return [(0, 0)]

    merged: list[tuple[int, int]] = []
    for start, end in sorted(intervals):
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))

    periods: list[tuple[int, int]] = []
    if merged[0][0] > 0:
        periods.append((0, 0))
    for start, end in merged:
        if start > 0:
            periods.append((start, limit_a))
        elif not periods:
            periods.append((0, limit_a))
        if end < cycle_seconds:
            periods.append((end, 0))

    deduped: list[tuple[int, int]] = []
    for start, limit in sorted(periods):
        if deduped and deduped[-1][0] == start:
            deduped[-1] = (start, limit)
        elif deduped and deduped[-1][1] == limit:
            continue
        else:
            deduped.append((start, limit))
    return deduped or [(0, 0)]


def _split_hhmm(value: Any) -> tuple[int, int]:
    normalised = _normalise_time(value)
    hours, minutes = normalised.split(":", 1)
    return int(hours), int(minutes)


def _require_ocpp_status(action: str, result: dict[str, Any], accepted: set[str]) -> None:
    status = str(result.get("status") or "").strip()
    if status in accepted:
        return
    response = json.dumps(result, default=str, separators=(",", ":"))
    raise RuntimeError(f"{action} returned {response}")


def _normalise_time(value: Any) -> str:
    match = re.fullmatch(r"(\d{1,2}):(\d{2})", str(value).strip())
    if not match:
        return "00:00"
    hours = max(0, min(23, int(match.group(1))))
    minutes = max(0, min(59, int(match.group(2))))
    return f"{hours:02d}:{minutes:02d}"


def _normalise_rfid_tag(tag: dict[str, Any]) -> dict[str, Any]:
    id_tag = str(tag.get("id_tag") or tag.get("idTag") or "").strip()
    if not id_tag:
        raise ValueError("ID tag is required")
    alias = str(tag.get("alias") or "").strip()
    expires_at = str(tag.get("expires_at") or tag.get("expiresAt") or "").strip()
    return {
        "id_tag": id_tag,
        "alias": alias or None,
        "expires_at": expires_at or None,
        "enabled": bool(tag.get("enabled", True)),
    }


def _tag_with_ocpp_response(
    tag: dict[str, Any] | None, response: dict[str, Any] | None
) -> dict[str, Any]:
    result = dict(tag or {})
    if response is not None:
        result["_ocpp_response"] = response
    return result


def _rfid_tag_requires_ocpp_update(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
    original_id_tag: str,
) -> bool:
    if previous is None:
        return True
    if original_id_tag and previous["id_tag"].casefold() != current["id_tag"].casefold():
        return True
    return (
        bool(previous.get("enabled", True)) != bool(current.get("enabled", True))
        or str(previous.get("expires_at") or "") != str(current.get("expires_at") or "")
    )


def _rfid_tag_local_authorization_entry(tag: dict[str, Any]) -> dict[str, Any]:
    id_tag_info: dict[str, Any] = {
        "status": "Accepted" if tag.get("enabled", True) else "Blocked"
    }
    expiry_date = _normalise_ocpp_datetime(tag.get("expires_at"))
    if expiry_date:
        id_tag_info["expiryDate"] = expiry_date
    return {
        "idTag": str(tag.get("id_tag") or ""),
        "idTagInfo": id_tag_info,
    }


def _normalise_ocpp_datetime(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return raw
    if parsed.tzinfo is None:
        local_tz = datetime.now().astimezone().tzinfo or UTC
        parsed = parsed.replace(tzinfo=local_tz)
    return parsed.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalise_rfid_tag_list(tags: Any) -> list[dict[str, Any]]:
    if not isinstance(tags, list):
        return []
    normalised: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in tags:
        if not isinstance(raw, dict):
            continue
        try:
            item = _normalise_rfid_tag(raw)
        except ValueError:
            continue
        if item["id_tag"] in seen:
            continue
        seen.add(item["id_tag"])
        normalised.append(item)
    return normalised


def _log_response(response: Any) -> Any:
    if response is None:
        return "Success"
    if isinstance(response, (str, int, float, bool)):
        return response
    try:
        json.dumps(response, default=str)
        return response
    except TypeError:
        return str(response)


_STATE_DATETIME_FIELDS = {
    "firmware_update_started_at",
    "firmware_update_download_completed_at",
    "firmware_update_install_started_at",
    "firmware_update_expected_reconnect_by",
    "firmware_update_completed_at",
    "firmware_manifest_refreshed_at",
    "last_seen",
    "last_heartbeat",
    "transaction_started_at",
    "transaction_ended_at",
}


def _state_from_snapshot(snapshot: dict[str, Any]) -> ChargerState:
    state = ChargerState()
    _apply_snapshot_to_state(state, snapshot)
    return state


def _apply_snapshot_to_state(state: ChargerState, snapshot: dict[str, Any]) -> None:
    for field_name in ChargerState.__dataclass_fields__:
        if field_name not in snapshot:
            continue
        value = snapshot[field_name]
        if field_name in _STATE_DATETIME_FIELDS and isinstance(value, str) and value:
            value = _parse_ocpp_ts(value)
        try:
            setattr(state, field_name, value)
        except (AttributeError, TypeError, ValueError):
            continue
    state.charging_schedule = _normalise_schedule_list(state.charging_schedule)
    state.rfid_tags = _normalise_rfid_tag_list(state.rfid_tags)
    state.transaction_active = _is_charging_status(state.status)


def _state_to_dict(s: ChargerState) -> dict[str, Any]:
    """Serialize the fields the UI cares about to a JSON-safe dict."""
    return {
        "connected": s.connected,
        "connection_state": s.connection_state,
        "charge_point_id": s.charge_point_id,
        "manufacturer": s.manufacturer,
        "model": s.model,
        "firmware_version": s.firmware_version,
        "firmware_status": s.firmware_status,
        "firmware_update_state": s.firmware_update_state,
        "firmware_update_target_file": s.firmware_update_target_file,
        "firmware_update_target_version": s.firmware_update_target_version,
        "firmware_update_previous_version": s.firmware_update_previous_version,
        "firmware_update_started_at": s.firmware_update_started_at.isoformat() if s.firmware_update_started_at else None,
        "firmware_update_download_completed_at": s.firmware_update_download_completed_at.isoformat() if s.firmware_update_download_completed_at else None,
        "firmware_update_install_started_at": s.firmware_update_install_started_at.isoformat() if s.firmware_update_install_started_at else None,
        "firmware_update_expected_reconnect_by": s.firmware_update_expected_reconnect_by.isoformat() if s.firmware_update_expected_reconnect_by else None,
        "firmware_update_completed_at": s.firmware_update_completed_at.isoformat() if s.firmware_update_completed_at else None,
        "firmware_update_failure_reason": s.firmware_update_failure_reason,
        "firmware_server_host": s.firmware_server_host,
        "firmware_manifest_error": s.firmware_manifest_error,
        "firmware_manifest_refreshed_at": s.firmware_manifest_refreshed_at.isoformat() if s.firmware_manifest_refreshed_at else None,
        "firmware_manifest_entries": s.firmware_manifest_entries,
        "selected_firmware_file": s.selected_firmware_file,
        "available_firmware_files": s.available_firmware_files,
        "last_update_firmware_request": s.last_update_firmware_request,
        "firmware_transfer_progress": s.firmware_transfer_progress,
        "charge_point_serial_number": s.charge_point_serial_number,
        "charge_box_serial_number": s.charge_box_serial_number,
        "websocket_remote_address": s.websocket_remote_address,
        "local_ip_address": s.local_ip_address,
        "status": s.status,
        "error_code": s.error_code,
        "vendor_error_code": s.vendor_error_code,
        "last_boot_notification": s.last_boot_notification,
        "last_status_notification": s.last_status_notification,
        "last_meter_values": s.last_meter_values,
        "car_plugged_in": s.car_plugged_in,
        "transaction_active": _is_charging_status(s.status),
        "transaction_open": s.transaction_id is not None and s.transaction_ended_at is None,
        "transaction_id": s.transaction_id,
        "transaction_id_tag": s.transaction_id_tag,
        "transaction_started_at": s.transaction_started_at.isoformat() if s.transaction_started_at else None,
        "transaction_ended_at": s.transaction_ended_at.isoformat() if s.transaction_ended_at else None,
        "live_power_kw": s.live_power_kw,
        "live_current_a": s.live_current_a,
        "live_voltage_v": s.live_voltage_v,
        "cp_voltage_v": s.cp_voltage_v,
        "cp_duty_cycle_percent": s.cp_duty_cycle_percent,
        "session_energy_kwh": s.session_energy_kwh,
        "total_energy_kwh": s.total_energy_kwh,
        "current_limit_a": s.current_limit_a,
        "current_limit_key": s.current_limit_key,
        "max_energy_per_session_kwh": s.max_energy_per_session_kwh,
        "max_import_capacity_a": s.max_import_capacity_a,
        "suspend_timeout_s": s.suspend_timeout_s,
        "charge_mode": s.charge_mode,
        "plug_and_go_enabled": s.plug_and_go_enabled,
        "plug_and_go_start_pending": s.plug_and_go_start_pending,
        "plug_and_go_last_error": s.plug_and_go_last_error,
        "local_modbus_enabled": s.local_modbus_enabled,
        "front_panel_leds_enabled": s.front_panel_leds_enabled,
        "cp_voltage_lower_limit": s.cp_voltage_lower_limit,
        "cp_voltage_upper_limit": s.cp_voltage_upper_limit,
        "randomised_delay_s": s.randomised_delay_s,
        "last_heartbeat": s.last_heartbeat.isoformat() if s.last_heartbeat else None,
        "last_seen": s.last_seen.isoformat() if s.last_seen else None,
        "charging_schedule": s.charging_schedule,
        "rfid_tags": s.rfid_tags,
        "action_log": s.action_log,
        "ocpp_frame_history": s.ocpp_frame_history,
    }
