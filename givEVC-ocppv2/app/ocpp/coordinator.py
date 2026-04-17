"""Standalone OCPP coordinator — no Home Assistant dependency."""

from __future__ import annotations

import asyncio
import hashlib
import itertools
import json
import logging
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

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


class OcppCoordinator:
    """Manage charger state, persistence, and SSE fan-out."""

    def __init__(
        self,
        listen_port: int,
        state_path: Path,
        firmware_directory: Path | None = None,
        firmware_server_port: int = 9688,
        firmware_manifest_url: str | None = None,
        adopt_first_charger: bool = True,
        expected_charge_point_id: str | None = None,
        debug_logging: bool = False,
    ) -> None:
        self.listen_port = listen_port
        self._state_path = state_path
        self.firmware_directory = firmware_directory
        self.firmware_server_port = firmware_server_port
        self.firmware_manifest_url = firmware_manifest_url
        self._adopt_first_charger = adopt_first_charger
        self._expected_charge_point_id = expected_charge_point_id
        self.debug_logging = debug_logging
        self.data = ChargerState()
        self._sse_queues: list[asyncio.Queue] = []
        self._ocpp_caller: Any = None
        self._firmware_cleanup_path: Path | None = None
        self._firmware_cleanup_task: asyncio.Task | None = None

    # ── Persistence ──────────────────────────────────────────────────────

    def load(self) -> None:
        """Restore persisted state from disk at startup."""
        if not self._state_path.exists():
            return
        try:
            raw = json.loads(self._state_path.read_text())
            for field in _PERSIST_FIELDS:
                if field not in raw:
                    continue
                value = raw[field]
                # Re-hydrate datetime strings
                if field in ("transaction_started_at", "transaction_ended_at") and value:
                    value = datetime.fromisoformat(value)
                setattr(self.data, field, value)
            self.data.charging_schedule = _normalise_schedule_list(self.data.charging_schedule)
            self.data.rfid_tags = _normalise_rfid_tag_list(self.data.rfid_tags)
            self.data.transaction_active = _is_charging_status(self.data.status)
            _LOGGER.info("Restored state from %s", self._state_path)
        except Exception:
            _LOGGER.exception("Failed to load persisted state — starting fresh")

    def _save(self) -> None:
        """Write persisted fields to disk synchronously (called from async context)."""
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            snapshot: dict[str, Any] = {}
            for field in _PERSIST_FIELDS:
                value = getattr(self.data, field)
                if isinstance(value, datetime):
                    value = value.isoformat()
                snapshot[field] = value
            self._state_path.write_text(json.dumps(snapshot, indent=2))
        except Exception:
            _LOGGER.exception("Failed to persist state")

    # ── SSE fan-out ──────────────────────────────────────────────────────

    def add_sse_queue(self, q: asyncio.Queue) -> None:
        self._sse_queues.append(q)

    def remove_sse_queue(self, q: asyncio.Queue) -> None:
        try:
            self._sse_queues.remove(q)
        except ValueError:
            pass

    def _push_sse(self) -> None:
        """Push current state to all connected SSE clients."""
        payload = json.dumps(_state_to_dict(self.data))
        for q in list(self._sse_queues):
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                pass

    def _notify(self, persist: bool = False) -> None:
        self._push_sse()
        if persist:
            self._save()

    # ── Connection lifecycle ─────────────────────────────────────────────

    def can_accept_charge_point(self, candidate_id: str | None) -> bool:
        if self._expected_charge_point_id:
            return candidate_id == self._expected_charge_point_id
        return self._adopt_first_charger or candidate_id == self.data.charge_point_id

    async def async_note_rejected_charge_point(self, candidate_id: str | None) -> None:
        _LOGGER.warning("Rejected charger connection: %s", candidate_id)

    async def async_connection_opened(
        self,
        charge_point_id: str | None,
        local_host: str | None,
        remote_host: str | None,
    ) -> None:
        self.data.connected = True
        self.data.connection_state = "connected"
        self.data.charge_point_id = charge_point_id
        if local_host and local_host not in {"0.0.0.0", "::"}:
            self.data.firmware_server_host = local_host
        self.data.websocket_remote_address = remote_host or None
        self.data.local_ip_address = remote_host or None
        self.data.last_seen = datetime.now(UTC)
        _LOGGER.info("Charger connected: %s from %s", charge_point_id, remote_host)
        self._notify()

    async def async_connection_closed(self) -> None:
        self.data.connected = False
        self.data.connection_state = "disconnected"
        self.data.live_power_kw = None
        self.data.live_current_a = None
        self.data.live_voltage_v = None
        self.data.last_seen = datetime.now(UTC)
        _LOGGER.info("Charger disconnected")
        self._notify(persist=True)

    # ── OCPP message handlers ────────────────────────────────────────────

    async def async_record_boot(
        self, charge_point_id: str | None, payload: dict[str, Any]
    ) -> None:
        self.data.charge_point_id = charge_point_id or self.data.charge_point_id
        self.data.manufacturer = payload.get("chargePointVendor")
        self.data.model = payload.get("chargePointModel")
        self.data.firmware_version = payload.get("firmwareVersion")
        self.data.charge_point_serial_number = payload.get("chargePointSerialNumber")
        self.data.charge_box_serial_number = payload.get("chargeBoxSerialNumber")
        self.data.last_boot_notification = payload
        self.data.last_seen = datetime.now(UTC)
        _LOGGER.info("BootNotification from %s %s fw=%s", self.data.manufacturer, self.data.model, self.data.firmware_version)
        self._notify(persist=True)
        # Schedule GetConfiguration after a short delay so the charger has finished
        # processing the BootNotification response before we send outbound calls.
        asyncio.get_running_loop().call_later(
            2.0, lambda: asyncio.ensure_future(self._safe_refresh_configuration())
        )

    async def async_record_heartbeat(self) -> None:
        self.data.last_heartbeat = datetime.now(UTC)
        self.data.last_seen = datetime.now(UTC)
        self._notify()

    async def async_record_status(self, payload: dict[str, Any]) -> None:
        previous_plugged_in = self.data.car_plugged_in
        self.data.status = payload.get("status")
        self.data.error_code = payload.get("errorCode")
        self.data.vendor_error_code = payload.get("vendorErrorCode")
        self.data.last_status_notification = payload
        self.data.last_seen = datetime.now(UTC)
        self.data.car_plugged_in = _is_car_plugged_in_status(self.data.status)
        self.data.transaction_active = _is_charging_status(self.data.status)
        _LOGGER.debug("StatusNotification: %s / %s", self.data.status, self.data.error_code)
        self._notify()

        if (
            self.data.plug_and_go_enabled
            and previous_plugged_in is False
            and self.data.car_plugged_in is True
            and not self.data.transaction_active
            and not self.data.plug_and_go_start_pending
        ):
            self.data.plug_and_go_start_pending = True
            self.data.plug_and_go_last_error = None
            self._notify()
            asyncio.create_task(self._async_handle_plug_and_go_start())

    async def async_start_transaction_from_charger(self, payload: dict[str, Any]) -> int:
        tx_id = next(_tx_counter)
        self.data.transaction_id = tx_id
        self.data.transaction_active = _is_charging_status(self.data.status)
        self.data.plug_and_go_start_pending = False
        self.data.plug_and_go_last_error = None
        self.data.transaction_id_tag = payload.get("idTag")
        self.data.transaction_meter_start_wh = _safe_float(payload.get("meterStart"))
        self.data.transaction_started_at = _parse_ocpp_ts(payload.get("timestamp"))
        self.data.transaction_ended_at = None
        self.data.session_energy_kwh = None
        self.data.last_seen = datetime.now(UTC)
        _LOGGER.info("Transaction started id=%s tag=%s", tx_id, self.data.transaction_id_tag)
        self._notify(persist=True)
        return tx_id

    async def async_stop_transaction_from_charger(self, payload: dict[str, Any]) -> None:
        meter_stop = _safe_float(payload.get("meterStop"))
        if meter_stop is not None and self.data.transaction_meter_start_wh is not None:
            self.data.session_energy_kwh = round(
                (meter_stop - self.data.transaction_meter_start_wh) / 1000, 3
            )
            self.data.total_energy_kwh = round(meter_stop / 1000, 2)
        self.data.transaction_active = False
        self.data.transaction_id = payload.get("transactionId", self.data.transaction_id)
        self.data.plug_and_go_start_pending = False
        self.data.transaction_ended_at = _parse_ocpp_ts(payload.get("timestamp")) or datetime.now(UTC)
        self.data.last_seen = datetime.now(UTC)
        _LOGGER.info("Transaction stopped energy=%.3f kWh", self.data.session_energy_kwh or 0)
        self._notify(persist=True)

    async def async_record_meter_values(self, payload: dict[str, Any]) -> None:
        previous_total_kwh = self.data.total_energy_kwh
        self.data.last_meter_values = payload
        self.data.last_seen = datetime.now(UTC)
        self._restore_transaction_from_meter_values(payload)
        self._apply_meter_values_payload(payload)
        self._notify(persist=self.data.total_energy_kwh != previous_total_kwh)

    def _restore_transaction_from_meter_values(self, payload: dict[str, Any]) -> None:
        """Recover an open transaction id from MeterValues after reconnects."""
        transaction_id = _coerce_int(payload.get("transactionId"))
        if transaction_id is None:
            return

        if self.data.transaction_id != transaction_id:
            self.data.transaction_id = transaction_id
        self.data.transaction_ended_at = None
        if self.data.transaction_started_at is None:
            meter_values = payload.get("meterValue") or []
            timestamp = None
            if meter_values:
                timestamp = _parse_ocpp_ts(meter_values[0].get("timestamp"))
            self.data.transaction_started_at = timestamp or datetime.now(UTC)
        self.data.transaction_active = _is_charging_status(self.data.status)

    def _apply_meter_values_payload(self, payload: dict[str, Any]) -> None:
        """Parse MeterValues using the same sample selection model as upstream."""
        flattened_samples = self._flatten_meter_values_payload(payload)
        self.data.meter_samples = flattened_samples
        self.data.parsed_meter_values = {
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
        preferred_group = ev_meter_group or self._pick_preferred_meter_group(meter_groups)
        live_samples = preferred_group or flattened_samples
        power_delivery_expected = self._status_expects_power_delivery()

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
            self.data.live_power_kw = round(power_sample["normalized_value"] / 1000, 2)

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
            self.data.live_current_a = round(current_sample["normalized_value"], 3)

        voltage_sample = self._pick_preferred_sample(
            live_samples,
            measurand="Voltage",
            preferred_phases=("L1-N", None, "L1", "N"),
            preferred_locations=("Outlet", None, "Body", "Cable"),
            preferred_contexts=("Sample.Periodic", None, "Transaction.Begin"),
            prefer_non_negative=True,
        )
        if voltage_sample and voltage_sample["normalized_value"] is not None:
            self.data.live_voltage_v = round(voltage_sample["normalized_value"], 1)

        for sample in flattened_samples:
            context = sample.get("context") or ""
            m = CP_READING_PATTERN.search(str(context))
            if m:
                self.data.cp_voltage_v = float(m.group("voltage"))
                self.data.cp_duty_cycle_percent = float(m.group("duty"))

        previous_total_wh = (
            self.data.total_energy_kwh * 1000
            if self.data.total_energy_kwh is not None
            else None
        )
        total_energy_samples = ev_meter_group or preferred_group or flattened_samples
        total_energy_sample = self._pick_total_energy_sample(
            total_energy_samples, previous_total_wh
        )
        if total_energy_sample and total_energy_sample["normalized_value"] is not None:
            total_wh = total_energy_sample["normalized_value"]
            self.data.total_energy_kwh = round(total_wh / 1000, 2)
            if (
                self.data.transaction_active
                and self.data.transaction_meter_start_wh is None
                and total_wh > 0
            ):
                self.data.transaction_meter_start_wh = total_wh
                self.data.session_energy_kwh = 0.0
            elif (
                self.data.transaction_meter_start_wh is not None
                and total_wh >= self.data.transaction_meter_start_wh
            ):
                self.data.session_energy_kwh = round(
                    (total_wh - self.data.transaction_meter_start_wh) / 1000, 3
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
        self, groups: dict[int, list[dict[str, Any]]]
    ) -> list[dict[str, Any]] | None:
        if not groups:
            return None

        power_delivery_expected = self._status_expects_power_delivery()

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
            within_current_limit = int(self._sample_within_current_limit(current))
            within_power_limit = int(
                self._sample_within_power_limit(power, current, voltage)
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

    def _status_expects_power_delivery(self) -> bool:
        if self.data.status is None:
            return self.data.transaction_active
        return self.data.status == "Charging"

    def _sample_within_current_limit(self, current: float | None) -> bool:
        if current is None:
            return False
        limit = self.data.current_limit_a or DEFAULT_EVSE_MAX_CURRENT
        return current <= (limit * 1.1)

    def _sample_within_power_limit(
        self,
        power_w: float | None,
        current_a: float | None,
        voltage_v: float | None,
    ) -> bool:
        if power_w is None:
            return False
        limit = self.data.current_limit_a or DEFAULT_EVSE_MAX_CURRENT
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

    async def _safe_refresh_configuration(self) -> None:
        try:
            await self.async_refresh_configuration()
        except Exception:
            _LOGGER.exception("GetConfiguration failed")

    async def _safe_reset(self) -> None:
        try:
            caller = getattr(self, "_ocpp_caller", None)
            if caller is None:
                return
            result = await caller.async_call("Reset", {"type": "Hard"}, timeout=5)
            _LOGGER.info("Reset response: %s", result)
        except RuntimeError:
            _LOGGER.info("Reset sent — no response before disconnect (normal)")

    async def async_refresh_configuration(self) -> dict[str, Any]:
        """Send GetConfiguration to the charger and update local state from the response."""
        result = await self._ocpp_call("GetConfiguration", {}, timeout=60)
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
        self.data.local_ip_address = (
            reported_ip
            if reported_ip and reported_ip != "0.0.0.0"
            else self.data.websocket_remote_address
        )

        self.data.charge_mode = _val("EcoMode") or self.data.charge_mode
        self.data.front_panel_leds_enabled = _bool("FrontPanelLEDsEnabled")
        self.data.local_modbus_enabled     = _bool("EnableLocalModbus")

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
                self.data.current_limit_key = key
                break
            _LOGGER.debug("Ignoring %s=%s — outside %s–%sA range", key, raw, EVSE_MIN, EVSE_MAX)
        if limit is not None:
            self.data.current_limit_a = limit

        imax = _float("Imax")
        if imax is not None:
            try:
                self.data.max_import_capacity_a = int(imax)
            except (TypeError, ValueError):
                pass

        rand_delay = _float("RandomisedDelayDuration")
        if rand_delay is not None:
            try:
                self.data.randomised_delay_s = int(rand_delay)
            except (TypeError, ValueError):
                pass

        cp_lower = _float("ChargingStateBCPVoltageLowerLimit")
        if cp_lower is not None:
            self.data.cp_voltage_lower_limit = round(cp_lower / 10, 1)

        cp_upper = _float("ChargingStateBCPVoltageHigherLimit")
        if cp_upper is not None:
            self.data.cp_voltage_upper_limit = round(cp_upper / 10, 1)

        suspev = _float("SuspevTime")
        if suspev is not None:
            try:
                self.data.suspend_timeout_s = int(suspev)
            except (TypeError, ValueError):
                pass

        _LOGGER.info(
            "GetConfiguration: mode=%s leds=%s modbus=%s current_limit=%sA (key=%s) imax=%s suspev=%s",
            self.data.charge_mode,
            self.data.front_panel_leds_enabled,
            self.data.local_modbus_enabled,
            self.data.current_limit_a,
            self.data.current_limit_key,
            self.data.max_import_capacity_a,
            self.data.suspend_timeout_s,
        )
        self._notify(persist=True)
        return result

    # ── Outbound OCPP commands ───────────────────────────────────────────

    async def async_change_configuration(self, key: str, value: Any) -> dict[str, Any]:
        # ChargeRate is written in tenths-of-amps but read back in real amps
        if key == "ChargeRate":
            value = round(float(value) * 10, 1)
        result = await self._ocpp_call("ChangeConfiguration", {"key": key, "value": str(value)})
        status = result.get("status", "")
        if status in ("Accepted", "RebootRequired"):
            _LOGGER.info("ChangeConfiguration %s=%s → %s", key, value, status)
            self._apply_config_key(key, value)
            self._notify(persist=True)
        if status == "RebootRequired":
            _LOGGER.info("RebootRequired after %s — scheduling Hard Reset", key)
            asyncio.get_running_loop().call_later(
                0.5, lambda: asyncio.ensure_future(self._safe_reset())
            )
        return result

    def _apply_config_key(self, key: str, value: Any) -> None:
        sv = str(value)
        if key == "EcoMode":
            self.data.charge_mode = sv or None
        elif key == "FrontPanelLEDsEnabled":
            self.data.front_panel_leds_enabled = sv.lower() in ("true", "1")
        elif key == "EnableLocalModbus":
            self.data.local_modbus_enabled = sv.lower() in ("true", "1")
        elif key == "MaxCurrent":
            try:
                self.data.current_limit_a = float(sv)
            except (TypeError, ValueError):
                pass
        elif key == "ChargeRate":
            try:
                # value is tenths-of-amps (already ×10 before sending); convert back
                self.data.current_limit_a = round(float(sv) / 10, 1)
            except (TypeError, ValueError):
                pass
        elif key == "Imax":
            try:
                self.data.max_import_capacity_a = int(float(sv))
            except (TypeError, ValueError):
                pass
        elif key == "MaxEnergyOnInvalidId":
            try:
                self.data.max_energy_per_session_kwh = max(0, round(float(sv) / 1000, 3))
            except (TypeError, ValueError):
                pass
        elif key == "SuspevTime":
            try:
                self.data.suspend_timeout_s = int(float(sv))
            except (TypeError, ValueError):
                pass
        elif key == "RandomisedDelayDuration":
            try:
                self.data.randomised_delay_s = int(float(sv))
            except (TypeError, ValueError):
                pass
        elif key == "ChargingStateBCPVoltageLowerLimit":
            try:
                self.data.cp_voltage_lower_limit = round(float(sv) / 10, 1)
            except (TypeError, ValueError):
                pass
        elif key == "ChargingStateBCPVoltageHigherLimit":
            try:
                self.data.cp_voltage_upper_limit = round(float(sv) / 10, 1)
            except (TypeError, ValueError):
                pass

    CHARGE_MODES = ("SuperEco", "Eco", "Boost")

    async def async_set_charge_mode(self, mode: str) -> dict[str, Any]:
        if mode not in self.CHARGE_MODES:
            raise ValueError(f"Unknown charge mode: {mode}")
        result = await self._ocpp_call("ChangeConfiguration", {"key": "EcoMode", "value": mode})
        status = result.get("status", "")
        if status in ("Accepted", "RebootRequired"):
            self.data.charge_mode = mode
            self._notify(persist=True)
        return result

    async def async_set_plug_and_go(self, enabled: bool) -> None:
        """Plug-and-go is local state only — no OCPP call needed."""
        self.data.plug_and_go_enabled = enabled
        if not enabled:
            self.data.plug_and_go_start_pending = False
        self._notify(persist=True)

    async def async_set_max_energy_per_session(self, kwh: float) -> None:
        """Max energy threshold is local UI state, persisted across restarts."""
        self.data.max_energy_per_session_kwh = max(0, round(float(kwh), 3))
        self._notify(persist=True)

    async def async_save_charging_schedule(self, schedule: dict[str, Any]) -> dict[str, Any]:
        """Create or update a local charging schedule without OCPP side effects."""
        existing = {str(item.get("id")): item for item in self.data.charging_schedule if item.get("id") is not None}
        schedule_id = str(schedule.get("id") or _next_schedule_id(existing))
        normalised = _normalise_schedule(schedule, schedule_id)

        updated: list[dict[str, Any]] = []
        replaced = False
        for item in self.data.charging_schedule:
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

        self.data.charging_schedule = updated
        self._notify(persist=True)
        return normalised

    async def async_set_charging_schedule_enabled(self, schedule_id: str, enabled: bool) -> dict[str, Any]:
        """Enable or disable one schedule, ensuring only one active schedule exists."""
        target: dict[str, Any] | None = None
        for item in self.data.charging_schedule:
            if str(item.get("id")) == str(schedule_id):
                item["enabled"] = bool(enabled)
                target = item
            elif enabled:
                item["enabled"] = False
        if target is None:
            raise ValueError(f"Unknown schedule: {schedule_id}")
        self._notify(persist=True)
        return target

    async def async_delete_charging_schedule(self, schedule_id: str) -> None:
        before = len(self.data.charging_schedule)
        self.data.charging_schedule = [
            item for item in self.data.charging_schedule
            if str(item.get("id")) != str(schedule_id)
        ]
        if len(self.data.charging_schedule) == before:
            raise ValueError(f"Unknown schedule: {schedule_id}")
        self._notify(persist=True)

    async def async_save_rfid_tag(self, tag: dict[str, Any]) -> dict[str, Any]:
        """Create or update a local RFID tag without OCPP side effects."""
        original_raw = tag.get("original_id_tag")
        original_id_tag = str(original_raw or "").strip()
        normalised = _normalise_rfid_tag(tag)
        id_tag = normalised["id_tag"]
        has_original = bool(original_id_tag)
        original_key = original_id_tag.casefold()
        id_tag_key = id_tag.casefold()

        updated: list[dict[str, Any]] = []
        replaced = False
        for item in self.data.rfid_tags:
            item_id = str(item.get("id_tag") or "").strip()
            item_key = item_id.casefold()
            if has_original and item_key == original_key:
                updated.append(normalised)
                replaced = True
            elif item_key == id_tag_key:
                raise ValueError(f"ID tag already exists: {id_tag}")
            else:
                updated.append(item)
        if not replaced:
            updated.append(normalised)

        self.data.rfid_tags = _normalise_rfid_tag_list(updated)
        self._notify(persist=True)
        return normalised

    async def async_set_rfid_tag_enabled(self, id_tag: str, enabled: bool) -> dict[str, Any]:
        target: dict[str, Any] | None = None
        for item in self.data.rfid_tags:
            if str(item.get("id_tag")) == str(id_tag):
                item["enabled"] = bool(enabled)
                target = item
                break
        if target is None:
            raise ValueError(f"Unknown ID tag: {id_tag}")
        self._notify(persist=True)
        return target

    async def async_delete_rfid_tag(self, id_tag: str) -> None:
        before = len(self.data.rfid_tags)
        self.data.rfid_tags = [
            item for item in self.data.rfid_tags
            if str(item.get("id_tag")) != str(id_tag)
        ]
        if len(self.data.rfid_tags) == before:
            raise ValueError(f"Unknown ID tag: {id_tag}")
        self._notify(persist=True)

    async def async_remote_start_transaction(
        self,
        id_tag: str | None = None,
        connector_id: int | None = None,
        charging_profile: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Issue an OCPP RemoteStartTransaction command."""
        payload: dict[str, Any] = {"idTag": id_tag or DEFAULT_REMOTE_ID_TAG}
        if connector_id is not None:
            payload["connectorId"] = connector_id
        if charging_profile is not None:
            payload["chargingProfile"] = charging_profile
        result = await self._ocpp_call("RemoteStartTransaction", payload)
        _LOGGER.info("RemoteStartTransaction payload=%s → %s", payload, result.get("status"))
        return result

    async def async_start_charging(self) -> dict[str, Any]:
        """Request an immediate charging session."""
        return await self.async_remote_start_transaction(connector_id=1)

    async def async_stop_charging(self) -> dict[str, Any]:
        """Request the charger to stop the current transaction."""
        if self.data.transaction_id is None:
            raise RuntimeError("No active transaction id is available")
        result = await self._ocpp_call("RemoteStopTransaction", {"transactionId": self.data.transaction_id})
        _LOGGER.info("RemoteStopTransaction(%s) → %s", self.data.transaction_id, result.get("status"))
        return result

    def has_open_transaction(self) -> bool:
        """Return whether a StartTransaction has been seen without a matching StopTransaction."""
        return self.data.transaction_id is not None and self.data.transaction_ended_at is None

    async def _async_handle_plug_and_go_start(self) -> None:
        """Start charging after a real unplugged -> plugged status edge."""
        try:
            result = await self.async_start_charging()
            status = str(result.get("status", ""))
            if status and status != "Accepted":
                self.data.plug_and_go_last_error = status
            else:
                self.data.plug_and_go_last_error = None
            _LOGGER.info("Plug and Go remote start result: %s", result)
        except Exception as err:
            self.data.plug_and_go_last_error = str(err)
            _LOGGER.warning("Plug and Go failed to start charging: %s", err)
        finally:
            self.data.plug_and_go_start_pending = False
            self._notify()

    async def async_read_cp_voltage(self) -> dict[str, Any]:
        """Read CP voltage and duty cycle via GivEnergy vendor DataTransfer."""
        result = await self._ocpp_call(
            "DataTransfer",
            {"vendorId": "GivEnergy", "messageId": "Parameter", "data": "CP"},
        )
        status = str(result.get("status", ""))
        data = result.get("data")
        if status == "Accepted" and data:
            m = CP_READING_PATTERN.search(str(data))
            if m:
                self.data.cp_voltage_v = float(m.group("voltage"))
                self.data.cp_duty_cycle_percent = float(m.group("duty"))
                _LOGGER.info("CP reading: %.1fV / %.1f%%", self.data.cp_voltage_v, self.data.cp_duty_cycle_percent)
                self._notify()
        return result

    async def async_trigger_meter_values(self, connector_id: int = 1) -> dict[str, Any]:
        """Ask the charger to send a MeterValues frame immediately."""
        return await self._ocpp_call(
            "TriggerMessage",
            {"requestedMessage": "MeterValues", "connectorId": connector_id},
        )

    async def async_unlock_connector(self, connector_id: int = 1) -> dict[str, Any]:
        result = await self._ocpp_call("UnlockConnector", {"connectorId": connector_id})
        _LOGGER.info("UnlockConnector → %s", result.get("status"))
        return result

    async def async_reset(self, reset_type: str = "Soft") -> dict[str, Any]:
        result = await self._ocpp_call("Reset", {"type": reset_type})
        _LOGGER.info("Reset(%s) → %s", reset_type, result.get("status"))
        return result

    # ── Firmware management ─────────────────────────────────────────────

    async def async_refresh_firmware_manifest(self) -> None:
        """Fetch and parse the configured firmware manifest."""
        if not self.firmware_manifest_url:
            self.data.firmware_manifest_error = "No firmware manifest URL is configured"
            self.data.firmware_manifest_entries = {}
            self._refresh_available_firmware_files()
            self._notify()
            return

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.firmware_manifest_url, allow_redirects=True) as response:
                    if response.status != 200:
                        raise RuntimeError(f"Manifest request failed with HTTP {response.status}")
                    manifest = json.loads(await response.text())
        except Exception as err:
            self.data.firmware_manifest_error = str(err)
            self.data.firmware_manifest_entries = {}
            self._refresh_available_firmware_files()
            self._notify()
            raise RuntimeError(
                f"Unable to load firmware manifest from {self.firmware_manifest_url}: {err}"
            ) from err

        self.data.firmware_manifest_entries = self._parse_firmware_manifest(manifest)
        self.data.firmware_manifest_error = None
        self.data.firmware_manifest_refreshed_at = datetime.now(UTC)
        self._refresh_available_firmware_files()
        self._notify()

    def firmware_catalog(self) -> dict[str, Any]:
        """Return firmware entries with action metadata for the UI."""
        self._refresh_available_firmware_files()
        entries = []
        for filename in self.data.available_firmware_files:
            entry = self.data.firmware_manifest_entries.get(filename, {})
            version = entry.get("version")
            entries.append({
                **entry,
                "action": self.firmware_action_for_version(version),
            })
        return {
            "current_version": self.data.firmware_version,
            "selected_firmware_file": self.data.selected_firmware_file,
            "manifest_error": self.data.firmware_manifest_error,
            "manifest_refreshed_at": (
                self.data.firmware_manifest_refreshed_at.isoformat()
                if self.data.firmware_manifest_refreshed_at
                else None
            ),
            "entries": entries,
        }

    def _parse_firmware_manifest(self, manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """Parse the upstream firmware manifest into filename-indexed entries."""
        models = manifest.get("models")
        if not isinstance(models, dict):
            raise RuntimeError("Firmware manifest does not contain a valid models map")

        preferred_model = self._derive_manifest_model_key()
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

    def _derive_manifest_model_key(self) -> str | None:
        """Infer the firmware manifest model key from the charger firmware string."""
        version = self.data.firmware_version
        if not version:
            return None
        parts = str(version).strip().split("_")
        if len(parts) < 3:
            return None
        return "_".join(parts[:-1])

    def _refresh_available_firmware_files(self) -> None:
        files = sorted(
            self.data.firmware_manifest_entries,
            key=lambda filename: _firmware_version_key(
                self.data.firmware_manifest_entries[filename].get("version")
            ),
        )
        self.data.available_firmware_files = files
        if self.data.selected_firmware_file not in files:
            self.data.selected_firmware_file = files[-1] if files else None

    def firmware_file_path(self, filename: str) -> Path:
        if self.firmware_directory is None:
            raise RuntimeError("Firmware directory is not configured")
        return self.firmware_directory / Path(filename).name

    async def _async_download_firmware_for_install(self, filename: str) -> Path:
        entry = self.data.firmware_manifest_entries.get(filename)
        if not entry:
            raise RuntimeError(f"No manifest entry was found for firmware file: {filename}")

        target_path = self.firmware_file_path(filename)
        target_path.parent.mkdir(parents=True, exist_ok=True)
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
            async with aiohttp.ClientSession() as session:
                async with session.get(download_url, allow_redirects=True) as response:
                    if response.status != 200:
                        raise RuntimeError(f"Firmware download failed with HTTP {response.status}")
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

    async def async_install_firmware_file(self, filename: str) -> dict[str, Any]:
        """Download a manifest firmware file and send OCPP UpdateFirmware."""
        await self.async_refresh_firmware_manifest()
        filename = Path(str(filename)).name
        if filename not in self.data.available_firmware_files:
            raise RuntimeError(f"Unknown firmware file from manifest: {filename}")
        if not self.data.connected:
            raise RuntimeError("No charger connected")
        if not self.data.firmware_server_host:
            raise RuntimeError("Unable to determine the firmware server host for the charger")

        self.data.selected_firmware_file = filename
        downloaded_path = await self._async_download_firmware_for_install(filename)

        retrieve_at = (datetime.now(UTC) + timedelta(seconds=60)).replace(microsecond=0)
        location = (
            f"ftp://{self.data.firmware_server_host}:{self.firmware_server_port}/"
            f"ChargerFirmware/{quote(filename)}"
        )
        try:
            result = await self.async_update_firmware(
                location=location,
                retrieve_date=retrieve_at.isoformat().replace("+00:00", "Z"),
                retries=1,
                retry_interval=60,
            )
        except Exception as err:
            if "timed out" in str(err).lower():
                self._schedule_firmware_file_cleanup(downloaded_path, delay_seconds=20 * 60)
            else:
                await asyncio.to_thread(_unlink_if_exists, downloaded_path)
            raise

        self._schedule_firmware_file_cleanup(downloaded_path, delay_seconds=20 * 60)
        return result

    def record_firmware_transfer_event(self, event: dict[str, Any]) -> None:
        """Observe firmware-server events and clean temporary firmware files."""
        event_type = event.get("event")
        filename = event.get("filename") or event.get("requested_filename")
        target_file = self.data.firmware_update_target_file or self.data.selected_firmware_file
        if event_type == "file_sent" and filename and filename == target_file:
            self._cleanup_firmware_file("served")

    def _schedule_firmware_file_cleanup(self, path: Path, delay_seconds: int) -> None:
        self._firmware_cleanup_path = path
        if self._firmware_cleanup_task is not None:
            self._firmware_cleanup_task.cancel()
        self._firmware_cleanup_task = asyncio.create_task(
            self._async_cleanup_firmware_file_later(delay_seconds)
        )

    async def _async_cleanup_firmware_file_later(self, delay_seconds: int) -> None:
        try:
            await asyncio.sleep(delay_seconds)
            self._cleanup_firmware_file("timeout")
        except asyncio.CancelledError:
            pass

    def _cleanup_firmware_file(self, reason: str) -> None:
        path = self._firmware_cleanup_path
        if self._firmware_cleanup_task is not None:
            self._firmware_cleanup_task.cancel()
            self._firmware_cleanup_task = None
        self._firmware_cleanup_path = None
        if path is None:
            return
        _unlink_if_exists(path)
        _LOGGER.info("Removed temporary firmware file after %s: %s", reason, path)

    async def async_update_firmware(
        self,
        location: str,
        retrieve_date: str,
        retries: int | None = None,
        retry_interval: int | None = None,
    ) -> dict[str, Any]:
        """Issue an OCPP UpdateFirmware command."""
        if self._firmware_update_in_progress():
            raise RuntimeError("A firmware update is already in progress")

        payload: dict[str, Any] = {
            "location": location,
            "retrieveDate": retrieve_date,
        }
        if retries is not None:
            payload["retries"] = retries
        if retry_interval is not None:
            payload["retryInterval"] = retry_interval

        self.data.last_update_firmware_request = dict(payload)
        self._start_firmware_update_session(location)
        result = await self._ocpp_call("UpdateFirmware", payload)
        _LOGGER.info("UpdateFirmware requested: %s", payload)
        self._notify(persist=True)
        return result

    def _firmware_update_in_progress(self) -> bool:
        return self.data.firmware_update_state in {"Requested", "Downloading", "Downloaded", "Installing"}

    def _start_firmware_update_session(self, location: str) -> None:
        target_file = self.data.selected_firmware_file or Path(location).name or None
        self.data.firmware_status = None
        self.data.firmware_update_state = "Requested"
        self.data.firmware_update_target_file = target_file
        self.data.firmware_update_target_version = _derive_firmware_version_from_filename(target_file)
        self.data.firmware_update_previous_version = self.data.firmware_version
        self.data.firmware_update_started_at = datetime.now(UTC)
        self.data.firmware_update_completed_at = None
        self.data.firmware_update_failure_reason = None

    def firmware_action_for_version(self, target_version: Any) -> str:
        comparison = _compare_firmware_versions(target_version, self.data.firmware_version)
        if comparison < 0:
            return "Downgrade"
        if comparison > 0:
            return "Upgrade"
        return "Reinstall"

    def set_ocpp_caller(self, caller: Any) -> None:
        """Register the active ChargePointSession so commands can be sent."""
        self._ocpp_caller = caller

    async def _ocpp_call(self, action: str, payload: dict[str, Any], timeout: int = 20) -> dict[str, Any]:
        caller = getattr(self, "_ocpp_caller", None)
        if caller is None:
            raise RuntimeError("No charger connected")
        return await caller.async_call(action, payload, timeout=timeout)

    async def async_record_firmware_status(self, payload: dict[str, Any]) -> None:
        status = payload.get("status")
        self.data.firmware_status = status
        if status in {"Downloading", "Downloaded", "Installing", "Installed"}:
            self.data.firmware_update_state = status
        elif status:
            self.data.firmware_update_state = "Failed"
            self.data.firmware_update_failure_reason = str(status)
        if status == "Installed":
            self.data.firmware_update_completed_at = datetime.now(UTC)
            self.data.firmware_update_failure_reason = None
        _LOGGER.info("FirmwareStatusNotification: %s", status)
        self._notify(persist=status in {"Installed", "Failed"})

    async def async_record_diagnostics_status(self, payload: dict[str, Any]) -> None:
        _LOGGER.info("DiagnosticsStatusNotification: %s", payload.get("status"))

    # ── OCPP frame logging ───────────────────────────────────────────────

    def record_portal_action(
        self,
        action: str,
        detail: str,
        response: Any = "Success",
        success: bool = True,
    ) -> None:
        """Record a user-visible portal action for the Settings -> Logs view."""
        entry = {
            "ts": datetime.now(UTC).isoformat(),
            "user": "You",
            "action": action,
            "detail": detail,
            "response": _log_response(response),
            "success": bool(success),
            "via": "Portal",
        }
        self.data.action_log.append(entry)
        if len(self.data.action_log) > MAX_STORED_ACTION_LOGS:
            self.data.action_log = self.data.action_log[-MAX_STORED_ACTION_LOGS:]
        self._notify(persist=True)

    def clear_action_log(self) -> int:
        """Clear the persisted portal action log."""
        count = len(self.data.action_log)
        self.data.action_log = []
        self._notify(persist=True)
        return count

    def record_ocpp_frame(self, **kwargs: Any) -> None:
        entry = {"ts": datetime.now(UTC).isoformat(), **kwargs}
        self.data.ocpp_frame_history.append(entry)
        if len(self.data.ocpp_frame_history) > MAX_STORED_OCPP_FRAMES:
            self.data.ocpp_frame_history.pop(0)

    def record_unsupported_ocpp_action(self, action: str, payload: Any) -> None:
        _LOGGER.warning("Unsupported OCPP action: %s payload=%s", action, payload)

    def record_authorize_exchange(self, req: Any, resp: Any) -> None:
        pass

    def record_start_transaction_exchange(self, req: Any, resp: Any) -> None:
        pass

    def record_stop_transaction_exchange(self, req: Any, resp: Any) -> None:
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
        "firmware_update_completed_at": s.firmware_update_completed_at.isoformat() if s.firmware_update_completed_at else None,
        "firmware_update_failure_reason": s.firmware_update_failure_reason,
        "firmware_manifest_error": s.firmware_manifest_error,
        "firmware_manifest_refreshed_at": s.firmware_manifest_refreshed_at.isoformat() if s.firmware_manifest_refreshed_at else None,
        "selected_firmware_file": s.selected_firmware_file,
        "charge_point_serial_number": s.charge_point_serial_number,
        "local_ip_address": s.local_ip_address,
        "status": s.status,
        "error_code": s.error_code,
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
    }
