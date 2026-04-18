"""Embedded demo charger simulator.

Runs as a background asyncio task inside the portal process.  Connects to the
local OCPP server as a fake GivEnergy charger and sits permanently connected so
the demo account always has a live charger to interact with.

The simulator sends a full GivEnergy-style MeterValues payload (groups 0-3) every
60 seconds while idle and every 30 seconds while charging.
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
from datetime import UTC, datetime
from urllib.parse import urlparse, urlunparse
from uuid import uuid4

import aiohttp

OCPP_SUBPROTOCOL = "ocpp1.6"
RECONNECT_DELAY_SECONDS = 10
_LOGGER = logging.getLogger("demo-simulator")


def _now_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _build_meter_values(
    total_energy_wh: float,
    ev_power_w: float,
    ev_current_a: float,
    ev_voltage_v: float,
    transaction_id: int | None,
) -> dict:
    """Build a full GivEnergy-style MeterValues payload with all four groups."""

    ts = _now_z()

    # Grid values: simulate mild import when charging, mild export otherwise
    grid_power_w = round(ev_power_w * 0.3 + 100.0, 1)
    grid_current_a = round(grid_power_w / max(ev_voltage_v, 1.0), 2)

    # PV1: simulate modest solar generation
    import math
    hour = datetime.now(UTC).hour
    solar_factor = max(0.0, math.sin(math.pi * (hour - 6) / 12)) if 6 <= hour <= 18 else 0.0
    pv1_power_w = round(solar_factor * 2800.0, 1)
    pv1_current_a = round(pv1_power_w / max(ev_voltage_v, 1.0), 2)
    pv1_energy_wh = round(total_energy_wh * 0.15, 0)  # rough proportional figure

    # PV2: smaller second string
    pv2_power_w = round(solar_factor * 1200.0, 1)
    pv2_current_a = round(pv2_power_w / max(ev_voltage_v, 1.0), 2)
    pv2_energy_wh = round(total_energy_wh * 0.07, 0)

    def _sv(value: str, measurand: str, unit: str, phase: str | None = None,
            context: str = "Sample.Periodic", location: str = "Outlet") -> dict:
        entry: dict = {
            "value": value,
            "measurand": measurand,
            "unit": unit,
            "context": context,
            "location": location,
        }
        if phase:
            entry["phase"] = phase
        return entry

    groups = [
        # Group 0 — EV Charger
        {
            "timestamp": ts,
            "sampledValue": [
                _sv(f"{total_energy_wh:.0f}", "Energy.Active.Import.Register", "Wh"),
                _sv(f"{ev_power_w:.1f}", "Power.Active.Import", "W", "L1"),
                _sv(f"{ev_current_a:.1f}", "Current.Import", "A", "L1"),
                _sv(f"{ev_voltage_v:.1f}", "Voltage", "V", "L1-N"),
            ],
        },
        # Group 1 — Grid Meter
        {
            "timestamp": ts,
            "sampledValue": [
                _sv(f"{grid_power_w:.1f}", "Power.Active.Import", "W", "L1"),
                _sv(f"{grid_current_a:.2f}", "Current.Import", "A", "L1"),
                _sv(f"{ev_voltage_v:.1f}", "Voltage", "V", "L1-N"),
            ],
        },
        # Group 2 — PV1
        {
            "timestamp": ts,
            "sampledValue": [
                _sv(f"{pv1_energy_wh:.0f}", "Energy.Active.Import.Register", "Wh"),
                _sv(f"{pv1_power_w:.1f}", "Power.Active.Import", "W", "L1"),
                _sv(f"{pv1_current_a:.2f}", "Current.Import", "A", "L1"),
                _sv(f"{ev_voltage_v:.1f}", "Voltage", "V", "L1-N"),
            ],
        },
        # Group 3 — PV2
        {
            "timestamp": ts,
            "sampledValue": [
                _sv(f"{pv2_energy_wh:.0f}", "Energy.Active.Import.Register", "Wh"),
                _sv(f"{pv2_power_w:.1f}", "Power.Active.Import", "W", "L1"),
                _sv(f"{pv2_current_a:.2f}", "Current.Import", "A", "L1"),
                _sv(f"{ev_voltage_v:.1f}", "Voltage", "V", "L1-N"),
            ],
        },
    ]

    payload: dict = {
        "connectorId": 1,
        "meterValue": groups,
    }
    if transaction_id is not None:
        payload["transactionId"] = transaction_id
    return payload


class DemoChargerSimulator:
    """Stateful fake GivEnergy charger that auto-reconnects on disconnect."""

    CHARGE_POINT_ID = "demo-charger-001"
    VENDOR = "GivEnergy"
    MODEL = "EV Charger"
    FIRMWARE = "3.3.1.0"
    SERIAL = "demo-charger-001"
    VOLTAGE_V = 230.0
    CURRENT_A = 32.0

    def __init__(self, upstream_base: str, password: str) -> None:
        self._upstream_base = upstream_base.rstrip("/")
        self._password = password
        self._pending: dict[str, asyncio.Future] = {}
        self._heartbeat_task: asyncio.Task | None = None
        self._meter_task: asyncio.Task | None = None
        self._heartbeat_interval = 300
        self._meter_idle_interval = 60
        self._meter_charging_interval = 30
        self._status = "Available"
        self._transaction_active = False
        self._transaction_id: int | None = None
        self._transaction_id_tag: str | None = None
        self._total_energy_wh = 8_000_000.0
        self._meter_start_wh = 0.0
        self._next_tx_id = 5000
        self._running = False

    async def run_forever(self) -> None:
        """Connect and reconnect until cancelled."""
        self._running = True
        while self._running:
            try:
                await self._connect_and_run()
            except asyncio.CancelledError:
                self._running = False
                break
            except Exception as exc:
                _LOGGER.warning("[%s] Connection error: %s — reconnecting in %ss",
                                self.CHARGE_POINT_ID, exc, RECONNECT_DELAY_SECONDS)
            if self._running:
                await asyncio.sleep(RECONNECT_DELAY_SECONDS)

    async def stop(self) -> None:
        self._running = False
        await self._cancel_background_tasks()

    async def _connect_and_run(self) -> None:
        url = f"{self._upstream_base}/{self.CHARGE_POINT_ID}"
        parsed = urlparse(url)
        authed_netloc = f"{self.CHARGE_POINT_ID}:{self._password}@{parsed.hostname}"
        if parsed.port:
            authed_netloc += f":{parsed.port}"
        authed_url = urlunparse(parsed._replace(netloc=authed_netloc))

        use_ssl: ssl.SSLContext | bool = (
            ssl.create_default_context() if url.startswith("wss://") else False
        )

        _LOGGER.info("[%s] Connecting to OCPP server", self.CHARGE_POINT_ID)
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                authed_url,
                protocols=(OCPP_SUBPROTOCOL,),
                ssl=use_ssl,
            ) as ws:
                _LOGGER.info("[%s] Connected", self.CHARGE_POINT_ID)
                recv_task = asyncio.create_task(self._receive_loop(ws))
                try:
                    await self._boot_sequence(ws)
                    self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws))
                    self._meter_task = asyncio.create_task(self._idle_meter_loop(ws))
                    await recv_task
                finally:
                    await self._cancel_background_tasks()
                    recv_task.cancel()
                    try:
                        await recv_task
                    except (asyncio.CancelledError, Exception):
                        pass

    async def _cancel_background_tasks(self) -> None:
        for task in (self._heartbeat_task, self._meter_task):
            if task is None:
                continue
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._heartbeat_task = None
        self._meter_task = None

    async def _boot_sequence(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        boot_resp = await self._call(ws, "BootNotification", {
            "chargePointVendor": self.VENDOR,
            "chargePointModel": self.MODEL,
            "firmwareVersion": self.FIRMWARE,
            "chargePointSerialNumber": self.SERIAL,
        })
        if isinstance(boot_resp, dict):
            interval = int(boot_resp.get("interval", self._heartbeat_interval))
            self._heartbeat_interval = max(interval, 30)

        await self._send_status(ws, 0, "Available")
        await self._send_status(ws, 1, "Preparing")
        _LOGGER.info("[%s] Boot complete", self.CHARGE_POINT_ID)

    async def _heartbeat_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            await self._call(ws, "Heartbeat", {})

    async def _idle_meter_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Send meter values while idle (no transaction)."""
        while not self._transaction_active:
            await asyncio.sleep(self._meter_idle_interval)
            if not self._transaction_active:
                await self._send_meter_values(ws)

    async def _charging_meter_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Send meter values while a transaction is active."""
        while self._transaction_active:
            await asyncio.sleep(self._meter_charging_interval)
            if not self._transaction_active:
                break
            delta_wh = self.CURRENT_A * self.VOLTAGE_V * (self._meter_charging_interval / 3600)
            self._total_energy_wh = round(self._total_energy_wh + delta_wh, 3)
            await self._send_meter_values(ws)

    async def _send_meter_values(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        charging = self._transaction_active
        ev_power = round(self.CURRENT_A * self.VOLTAGE_V, 1) if charging else 0.0
        ev_current = self.CURRENT_A if charging else 0.0
        payload = _build_meter_values(
            self._total_energy_wh,
            ev_power,
            ev_current,
            self.VOLTAGE_V,
            self._transaction_id if charging else None,
        )
        await self._call(ws, "MeterValues", payload)

    async def _send_status(self, ws: aiohttp.ClientWebSocketResponse, connector: int, status: str) -> None:
        if connector == 1:
            self._status = status
        await self._call(ws, "StatusNotification", {
            "connectorId": connector,
            "errorCode": "NoError",
            "status": status,
            "vendorErrorCode": "NoError",
        })

    async def _start_transaction(self, ws: aiohttp.ClientWebSocketResponse, id_tag: str) -> None:
        if self._transaction_active:
            return
        await self._send_status(ws, 1, "Preparing")
        self._transaction_active = True
        self._transaction_id_tag = id_tag
        self._transaction_id = self._next_tx_id
        self._next_tx_id += 1
        self._meter_start_wh = self._total_energy_wh

        resp = await self._call(ws, "StartTransaction", {
            "connectorId": 1,
            "idTag": id_tag,
            "meterStart": int(self._meter_start_wh),
            "timestamp": _now_z(),
        })
        if isinstance(resp, dict) and resp.get("transactionId") is not None:
            self._transaction_id = int(resp["transactionId"])

        await self._send_status(ws, 1, "Charging")

        if self._meter_task is not None:
            self._meter_task.cancel()
            try:
                await self._meter_task
            except (asyncio.CancelledError, Exception):
                pass
        self._meter_task = asyncio.create_task(self._charging_meter_loop(ws))

    async def _stop_transaction(self, ws: aiohttp.ClientWebSocketResponse, reason: str) -> None:
        if not self._transaction_active:
            return
        self._transaction_active = False

        if self._meter_task is not None:
            self._meter_task.cancel()
            try:
                await self._meter_task
            except (asyncio.CancelledError, Exception):
                pass
            self._meter_task = None

        await self._send_status(ws, 1, "Finishing")
        await self._call(ws, "StopTransaction", {
            "transactionId": self._transaction_id,
            "idTag": self._transaction_id_tag,
            "meterStop": int(self._total_energy_wh),
            "timestamp": _now_z(),
            "reason": reason,
        })
        self._transaction_id = None
        self._transaction_id_tag = None
        await self._send_status(ws, 1, "Preparing")
        self._meter_task = asyncio.create_task(self._idle_meter_loop(ws))

    def _configuration_entries(self) -> list[dict]:
        return [
            {"key": "ChargeRate", "readonly": False, "value": f"{self.CURRENT_A:.1f}"},
            {"key": "AuthorizeRemoteTxRequests", "readonly": True, "value": "false"},
            {"key": "EcoMode", "readonly": False, "value": "Boost"},
            {"key": "ConnectionTimeout", "readonly": False, "value": "60"},
            {"key": "HeartbeatInterval", "readonly": False, "value": str(self._heartbeat_interval)},
            {"key": "MeterValueSampleInterval", "readonly": False, "value": "60"},
            {"key": "NumberOfConnectors", "readonly": True, "value": "1"},
            {"key": "SupportedFeatureProfiles", "readonly": True,
             "value": "Core,Reservation,Smart Charging,Remote Trigger"},
            {"key": "Imax", "readonly": False, "value": "80"},
            {"key": "LocalAuthorizeOffline", "readonly": False, "value": "true"},
            {"key": "FrontPanelLEDsEnabled", "readonly": False, "value": "true"},
            {"key": "EnableLocalModbus", "readonly": False, "value": "true"},
            {"key": "SuspevTime", "readonly": False, "value": "0"},
            {"key": "RandomisedDelayDuration", "readonly": False, "value": "600"},
        ]

    async def _dispatch(self, ws: aiohttp.ClientWebSocketResponse, action: str, payload: dict) -> dict:
        if action == "GetConfiguration":
            all_keys = self._configuration_entries()
            requested: list[str] = payload.get("key", [])
            if requested:
                config_keys = [k for k in all_keys if k["key"] in requested]
                unknown_keys = [k for k in requested if k not in {e["key"] for e in all_keys}]
            else:
                config_keys = all_keys
                unknown_keys = []
            result: dict = {"configurationKey": config_keys}
            if unknown_keys:
                result["unknownKey"] = unknown_keys
            return result

        if action == "ChangeConfiguration":
            key, value = payload.get("key"), payload.get("value")
            if key == "ChargeRate":
                try:
                    amps = float(value)
                    if amps > 100:
                        amps /= 10.0
                    self.CURRENT_A = max(6.0, amps)  # type: ignore[assignment]
                except (TypeError, ValueError):
                    pass
            elif key == "HeartbeatInterval":
                try:
                    self._heartbeat_interval = max(int(value), 30)
                except (TypeError, ValueError):
                    pass
            return {"status": "Accepted"}

        if action == "RemoteStartTransaction":
            id_tag = str(payload.get("idTag") or "DEMO-TAG")
            asyncio.create_task(self._start_transaction(ws, id_tag))
            return {"status": "Accepted"}

        if action == "RemoteStopTransaction":
            asyncio.create_task(self._stop_transaction(ws, "Remote"))
            return {"status": "Accepted"}

        if action == "Reset":
            return {"status": "Accepted"}

        if action == "TriggerMessage":
            requested_msg = payload.get("requestedMessage")
            if requested_msg == "BootNotification":
                asyncio.create_task(self._call(ws, "BootNotification", {
                    "chargePointVendor": self.VENDOR,
                    "chargePointModel": self.MODEL,
                    "firmwareVersion": self.FIRMWARE,
                    "chargePointSerialNumber": self.SERIAL,
                }))
            elif requested_msg == "Heartbeat":
                asyncio.create_task(self._call(ws, "Heartbeat", {}))
            elif requested_msg == "StatusNotification":
                asyncio.create_task(self._send_status(ws, 1, self._status))
            elif requested_msg == "MeterValues":
                asyncio.create_task(self._send_meter_values(ws))
            return {"status": "Accepted"}

        if action == "UnlockConnector":
            return {"status": "Unlocked"}

        if action == "ChangeAvailability":
            operative = str(payload.get("type", "Operative")) == "Operative"
            asyncio.create_task(
                self._send_status(ws, 1, "Preparing" if operative else "Unavailable")
            )
            return {"status": "Accepted"}

        if action in ("SetChargingProfile", "ClearChargingProfile", "SendLocalList"):
            return {"status": "Accepted"}

        if action == "DataTransfer":
            if payload.get("vendorId") == "GivEnergy" and payload.get("messageId") == "Parameter":
                if payload.get("data") == "CP":
                    return {"status": "Accepted", "data": "CP_Voltage:6.0V,CP_Duty:53%"}
            return {"status": "Rejected"}

        _LOGGER.debug("[%s] Unhandled server CALL %s", self.CHARGE_POINT_ID, action)
        return {}

    async def _receive_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        async for msg in ws:
            if msg.type != aiohttp.WSMsgType.TEXT:
                if msg.type in {aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                    _LOGGER.info("[%s] Server closed connection", self.CHARGE_POINT_ID)
                    break
                continue
            try:
                frame = json.loads(msg.data)
            except json.JSONDecodeError:
                continue
            if not isinstance(frame, list) or len(frame) < 2:
                continue
            msg_type = frame[0]
            if msg_type == 3 and len(frame) == 3:
                future = self._pending.get(frame[1])
                if future and not future.done():
                    future.set_result(frame[2])
            elif msg_type == 4 and len(frame) >= 3:
                future = self._pending.get(frame[1])
                if future and not future.done():
                    future.set_result({})
            elif msg_type == 2 and len(frame) == 4:
                unique_id, action, payload = frame[1], frame[2], frame[3]
                result = await self._dispatch(ws, action, payload)
                await ws.send_str(json.dumps([3, unique_id, result]))

    async def _call(self, ws: aiohttp.ClientWebSocketResponse, action: str, payload: dict) -> dict:
        unique_id = uuid4().hex
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[unique_id] = future
        await ws.send_str(json.dumps([2, unique_id, action, payload]))
        try:
            return await asyncio.wait_for(future, timeout=30)
        except TimeoutError:
            return {}
        finally:
            self._pending.pop(unique_id, None)
