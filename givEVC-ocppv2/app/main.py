"""GivEVC OCPPv2 — unified entrypoint.

Runs three servers in the same asyncio event loop:
  • OCPP WebSocket server        (OCPP_PORT,     default 7655)
  • Firmware transfer server     (FIRMWARE_PORT, default 9688)
  • Web UI / API (aiohttp)       (INGRESS_PORT,  default 8099)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path

from aiohttp import http_exceptions, web

from ocpp.coordinator import (
    CHARGE_DISABLED_STATUSES,
    CHARGE_START_STATUSES,
    CHARGE_STOP_STATUSES,
    OcppCoordinator,
    _state_to_dict,
)
from ocpp.firmware_server import FirmwareTransferServer
from ocpp.server import OcppServer

logging.basicConfig(
    level=logging.DEBUG if os.environ.get("DEBUG_LOGGING", "").lower() in ("1", "true", "yes") else logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
_LOGGER = logging.getLogger(__name__)


class _TlsHandshakeNoiseFilter(logging.Filter):
    """Suppress HTTPS/TLS handshakes accidentally sent to plain HTTP ports."""

    def filter(self, record: logging.LogRecord) -> bool:
        exc_info = record.exc_info
        if not exc_info:
            return True
        exc = exc_info[1]
        if not isinstance(exc, http_exceptions.BadStatusLine):
            return True
        haystack = " ".join((
            repr(exc),
            str(exc),
            str(getattr(exc, "message", "")),
            str(getattr(exc, "args", "")),
        ))
        return not (
            "Invalid method encountered" in haystack
            and "x16" in haystack
            and "x03" in haystack
        )


logging.getLogger("aiohttp.server").addFilter(_TlsHandshakeNoiseFilter())

DEFAULT_FIRMWARE_MANIFEST_URL = (
    "https://raw.githubusercontent.com/DJBenson/giv-firmware/refs/heads/main/"
    "Firmware/EVC/manifest.json"
)

# ── Config from environment ────────────────────────────────────────────────────
OCPP_PORT     = int(os.environ.get("OCPP_PORT", 7655))
FIRMWARE_PORT = int(os.environ.get("FIRMWARE_PORT", 9688))
INGRESS_PORT  = int(os.environ.get("INGRESS_PORT", 8099))
DEBUG         = os.environ.get("DEBUG_LOGGING", "").lower() in ("1", "true", "yes")

ADOPT_FIRST    = os.environ.get("ADOPT_FIRST_CHARGER", "true").lower() not in ("0", "false", "no")
EXPECTED_CP_ID = os.environ.get("EXPECTED_CHARGE_POINT_ID") or None

DATA_DIR      = Path(os.environ.get("DATA_DIR", "/data"))
FIRMWARE_ROOT = Path(os.environ.get("FIRMWARE_ROOT", str(DATA_DIR / "firmware")))
STATE_PATH    = DATA_DIR / "state.json"
TEMPLATES     = Path(__file__).parent / "templates"
FIRMWARE_MANIFEST_URL = os.environ.get("FIRMWARE_MANIFEST_URL", DEFAULT_FIRMWARE_MANIFEST_URL)


# ── Web app ────────────────────────────────────────────────────────────────────

def build_web_app(coordinator: OcppCoordinator, firmware: FirmwareTransferServer) -> web.Application:
    app = web.Application()

    # ── Static UI ──────────────────────────────────────────────────────────
    async def index(request: web.Request) -> web.Response:
        return web.Response(content_type="text/html", text=(TEMPLATES / "index.html").read_text())

    # ── REST: current state snapshot ───────────────────────────────────────
    async def api_state(request: web.Request) -> web.Response:
        return web.Response(
            content_type="application/json",
            text=json.dumps(_state_to_dict(coordinator.data)),
        )

    # ── SSE: push state updates to the browser ─────────────────────────────
    async def api_events(request: web.Request) -> web.StreamResponse:
        resp = web.StreamResponse()
        resp.headers["Content-Type"] = "text/event-stream"
        resp.headers["Cache-Control"] = "no-cache"
        resp.headers["X-Accel-Buffering"] = "no"
        await resp.prepare(request)

        q: asyncio.Queue = asyncio.Queue(maxsize=20)
        coordinator.add_sse_queue(q)

        # Send current state immediately on connect
        await resp.write(f"data: {json.dumps(_state_to_dict(coordinator.data))}\n\n".encode())

        try:
            while True:
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=25)
                    await resp.write(f"data: {payload}\n\n".encode())
                except asyncio.TimeoutError:
                    # Keepalive comment so proxies don't close the connection
                    await resp.write(b": keepalive\n\n")
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        finally:
            coordinator.remove_sse_queue(q)

        return resp

    # ── REST: OCPP frame history ───────────────────────────────────────────
    async def api_ocpp_frames(request: web.Request) -> web.Response:
        frames = coordinator.data.ocpp_frame_history[-100:]
        return web.Response(
            content_type="application/json",
            text=json.dumps(frames, default=str),
        )

    # ── REST: firmware server status ───────────────────────────────────────
    async def api_firmware_status(request: web.Request) -> web.Response:
        return web.Response(
            content_type="application/json",
            text=json.dumps({
                "running": firmware.is_running,
                "root": str(firmware.root),
                "files": [f.name for f in firmware.root.glob("*.bin")] if firmware.root.exists() else [],
            }),
        )

    async def api_firmware_manifest(request: web.Request) -> web.Response:
        try:
            await coordinator.async_refresh_firmware_manifest()
            payload = coordinator.firmware_catalog()
            return web.Response(content_type="application/json", text=json.dumps(payload))
        except RuntimeError as exc:
            payload = coordinator.firmware_catalog()
            payload["error"] = str(exc)
            return web.Response(status=502, content_type="application/json", text=json.dumps(payload))

    async def api_firmware_install(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            filename = str(body["filename"])
        except Exception:
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "Expected {filename}"}))
        try:
            result = await coordinator.async_install_firmware_file(filename)
            coordinator.record_portal_action("Install Firmware", filename, result)
            return web.Response(content_type="application/json", text=json.dumps(result))
        except RuntimeError as exc:
            coordinator.record_portal_action("Install Firmware", filename, str(exc), False)
            return web.Response(status=503, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    # ── REST: settings actions ─────────────────────────────────────────
    async def api_change_config(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            key = str(body["key"])
            value = str(body["value"])
        except Exception:
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "Expected {key, value}"}))
        try:
            result = await coordinator.async_change_configuration(key, value)
            coordinator.record_portal_action("Change Configuration", f"{key}={value}", result)
            return web.Response(content_type="application/json", text=json.dumps(result))
        except RuntimeError as exc:
            coordinator.record_portal_action("Change Configuration", f"{key}={value}", str(exc), False)
            return web.Response(status=503, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_refresh_config(request: web.Request) -> web.Response:
        log = request.rel_url.query.get("log") == "1"
        try:
            result = await coordinator.async_refresh_configuration()
            if log:
                coordinator.record_portal_action("Read Charger Configuration", "GetConfiguration", result)
            return web.Response(content_type="application/json", text=json.dumps({"ok": True}))
        except RuntimeError as exc:
            if log:
                coordinator.record_portal_action("Read Charger Configuration", "GetConfiguration", str(exc), False)
            return web.Response(status=503, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_set_mode(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            mode = str(body["mode"])
        except Exception:
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "Expected {mode}"}))
        try:
            result = await coordinator.async_set_charge_mode(mode)
            coordinator.record_portal_action("Change Charge Mode", mode, result)
            return web.Response(content_type="application/json", text=json.dumps(result))
        except (RuntimeError, ValueError) as exc:
            coordinator.record_portal_action("Change Charge Mode", mode, str(exc), False)
            code = 400 if isinstance(exc, ValueError) else 503
            return web.Response(status=code, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_set_plug_and_go(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            enabled = bool(body["enabled"])
        except Exception:
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "Expected {enabled: bool}"}))
        await coordinator.async_set_plug_and_go(enabled)
        coordinator.record_portal_action("Set Plug and Go", "Enabled" if enabled else "Disabled")
        return web.Response(content_type="application/json",
                            text=json.dumps({"enabled": enabled}))

    async def api_set_max_energy(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            kwh = float(body["kwh"])
        except Exception:
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "Expected {kwh: number}"}))
        await coordinator.async_set_max_energy_per_session(kwh)
        coordinator.record_portal_action("Set Max Energy Per Session", f"{coordinator.data.max_energy_per_session_kwh:g} kWh")
        return web.Response(content_type="application/json",
                            text=json.dumps({"kwh": coordinator.data.max_energy_per_session_kwh}))

    async def api_save_schedule(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            schedule = body["schedule"]
        except Exception:
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "Expected {schedule}"}))
        try:
            result = await coordinator.async_save_charging_schedule(schedule)
            coordinator.record_portal_action("Save Schedule", _schedule_log_detail(result, "Saved"))
            return web.Response(content_type="application/json", text=json.dumps(result))
        except (RuntimeError, ValueError) as exc:
            coordinator.record_portal_action("Save Schedule", _schedule_log_detail(schedule, "Save failed"), str(exc), False)
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_set_schedule_enabled(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            schedule_id = str(body["id"])
            enabled = bool(body["enabled"])
        except Exception:
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "Expected {id, enabled}"}))
        try:
            result = await coordinator.async_set_charging_schedule_enabled(schedule_id, enabled)
            coordinator.record_portal_action(
                "Change Active Schedule",
                _schedule_log_detail(result, "Enabled" if enabled else "Disabled"),
            )
            return web.Response(content_type="application/json", text=json.dumps(result))
        except ValueError as exc:
            coordinator.record_portal_action("Change Active Schedule", _schedule_log_detail({"id": schedule_id}, "Change failed"), str(exc), False)
            return web.Response(status=404, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_delete_schedule(request: web.Request) -> web.Response:
        schedule_id = str(request.match_info["id"])
        schedule_detail = _schedule_log_detail(_find_schedule(coordinator, schedule_id), "Deleted")
        try:
            await coordinator.async_delete_charging_schedule(schedule_id)
            coordinator.record_portal_action("Delete Schedule", schedule_detail)
            return web.Response(content_type="application/json", text=json.dumps({"ok": True}))
        except ValueError as exc:
            coordinator.record_portal_action("Delete Schedule", schedule_detail, str(exc), False)
            return web.Response(status=404, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_save_rfid_tag(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            tag = body["tag"]
        except Exception:
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "Expected {tag}"}))
        try:
            result = await coordinator.async_save_rfid_tag(tag)
            coordinator.record_portal_action("Save ID Tag", _tag_log_detail(result.get("id_tag")))
            return web.Response(content_type="application/json", text=json.dumps(result))
        except ValueError as exc:
            coordinator.record_portal_action("Save ID Tag", _tag_log_detail(tag.get("id_tag")), str(exc), False)
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_set_rfid_tag_enabled(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            id_tag = str(body["id_tag"])
            enabled = bool(body["enabled"])
        except Exception:
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "Expected {id_tag, enabled}"}))
        try:
            result = await coordinator.async_set_rfid_tag_enabled(id_tag, enabled)
            coordinator.record_portal_action(
                "Change ID Tag State",
                f"{_tag_log_detail(id_tag)}: {'Enabled' if enabled else 'Disabled'}",
            )
            return web.Response(content_type="application/json", text=json.dumps(result))
        except ValueError as exc:
            coordinator.record_portal_action("Change ID Tag State", _tag_log_detail(id_tag), str(exc), False)
            return web.Response(status=404, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_delete_rfid_tag(request: web.Request) -> web.Response:
        id_tag = str(request.match_info["id_tag"])
        try:
            await coordinator.async_delete_rfid_tag(id_tag)
            coordinator.record_portal_action("Delete ID Tag", _tag_log_detail(id_tag))
            return web.Response(content_type="application/json", text=json.dumps({"ok": True}))
        except ValueError as exc:
            coordinator.record_portal_action("Delete ID Tag", _tag_log_detail(id_tag), str(exc), False)
            return web.Response(status=404, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_read_cp_voltage(request: web.Request) -> web.Response:
        try:
            result = await coordinator.async_read_cp_voltage()
            coordinator.record_portal_action("Read CP Voltage", "DataTransfer GetCPVoltage", result)
            return web.Response(content_type="application/json", text=json.dumps(result))
        except RuntimeError as exc:
            coordinator.record_portal_action("Read CP Voltage", "DataTransfer GetCPVoltage", str(exc), False)
            return web.Response(status=503, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_trigger_meter_values(request: web.Request) -> web.Response:
        log = request.rel_url.query.get("log") == "1"
        try:
            result = await coordinator.async_trigger_meter_values()
            if log:
                coordinator.record_portal_action("Trigger Meter Values", "TriggerMessage MeterValues", result)
            return web.Response(content_type="application/json", text=json.dumps(result))
        except RuntimeError as exc:
            if log:
                coordinator.record_portal_action("Trigger Meter Values", "TriggerMessage MeterValues", str(exc), False)
            return web.Response(status=503, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_unlock(request: web.Request) -> web.Response:
        try:
            result = await coordinator.async_unlock_connector()
            coordinator.record_portal_action("Unlock Charging Port", "UnlockConnector", result)
            return web.Response(content_type="application/json", text=json.dumps(result))
        except RuntimeError as exc:
            coordinator.record_portal_action("Unlock Charging Port", "UnlockConnector", str(exc), False)
            return web.Response(status=503, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_toggle_charging(request: web.Request) -> web.Response:
        status = coordinator.data.status
        has_open_transaction = coordinator.has_open_transaction()
        status_can_start = status in CHARGE_START_STATUSES
        status_can_stop = status in CHARGE_STOP_STATUSES
        can_stop = status_can_stop or (has_open_transaction and not status_can_start)
        can_start = status_can_start
        if not can_start and not can_stop:
            reason = f"Charging control is disabled while OCPP status is {status or 'unknown'}"
            if status in CHARGE_DISABLED_STATUSES:
                reason = f"Charging control is disabled while OCPP status is {status}"
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": reason}))
        stopping = status_can_stop or (has_open_transaction and not can_start)
        action = "Stop Charging" if stopping else "Start Charging"
        detail = "RemoteStopTransaction" if stopping else "RemoteStartTransaction"
        try:
            result = (
                await coordinator.async_stop_charging()
                if stopping
                else await coordinator.async_start_charging()
            )
            coordinator.record_portal_action(action, detail, result)
            payload = {"action": action, **result}
            return web.Response(content_type="application/json", text=json.dumps(payload))
        except RuntimeError as exc:
            coordinator.record_portal_action(action, detail, str(exc), False)
            return web.Response(status=503, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_reset(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            reset_type = str(body.get("type", "Soft"))
        except Exception:
            reset_type = "Soft"
        if reset_type not in ("Soft", "Hard"):
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "type must be Soft or Hard"}))
        try:
            result = await coordinator.async_reset(reset_type)
            coordinator.record_portal_action(f"{reset_type} Reset", reset_type, result)
            return web.Response(content_type="application/json", text=json.dumps(result))
        except RuntimeError as exc:
            coordinator.record_portal_action(f"{reset_type} Reset", reset_type, str(exc), False)
            return web.Response(status=503, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_clear_action_log(request: web.Request) -> web.Response:
        deleted = coordinator.clear_action_log()
        return web.Response(content_type="application/json",
                            text=json.dumps({"ok": True, "deleted": deleted}))

    # API routes first, catch-all last
    app.router.add_get("/api/state", api_state)
    app.router.add_get("/api/events", api_events)
    app.router.add_get("/api/ocpp/frames", api_ocpp_frames)
    app.router.add_get("/api/firmware/status", api_firmware_status)
    app.router.add_get("/api/firmware/manifest", api_firmware_manifest)
    app.router.add_post("/api/firmware/install", api_firmware_install)
    app.router.add_post("/api/settings/refresh", api_refresh_config)
    app.router.add_post("/api/settings/config", api_change_config)
    app.router.add_post("/api/settings/mode", api_set_mode)
    app.router.add_post("/api/settings/plug-and-go", api_set_plug_and_go)
    app.router.add_post("/api/settings/max-energy", api_set_max_energy)
    app.router.add_post("/api/settings/schedules", api_save_schedule)
    app.router.add_post("/api/settings/schedules/enabled", api_set_schedule_enabled)
    app.router.add_delete("/api/settings/schedules/{id}", api_delete_schedule)
    app.router.add_post("/api/settings/id-tags", api_save_rfid_tag)
    app.router.add_post("/api/settings/id-tags/enabled", api_set_rfid_tag_enabled)
    app.router.add_delete("/api/settings/id-tags/{id_tag}", api_delete_rfid_tag)
    app.router.add_post("/api/settings/read-cp-voltage", api_read_cp_voltage)
    app.router.add_post("/api/settings/trigger-meter-values", api_trigger_meter_values)
    app.router.add_post("/api/settings/unlock", api_unlock)
    app.router.add_post("/api/charging/toggle", api_toggle_charging)
    app.router.add_post("/api/settings/reset", api_reset)
    app.router.add_delete("/api/settings/logs", api_clear_action_log)
    app.router.add_get("/", index)
    app.router.add_get("/{path:.*}", index)

    return app


def _find_schedule(coordinator: OcppCoordinator, schedule_id: str) -> dict | None:
    for schedule in coordinator.data.charging_schedule:
        if str(schedule.get("id")) == str(schedule_id):
            return schedule
    return {"id": schedule_id}


def _schedule_log_detail(schedule: dict | None, action: str) -> str:
    schedule = schedule or {}
    label = str(schedule.get("name") or schedule.get("id") or "Unknown schedule")
    return f"{label}: {action}"


def _tag_log_detail(id_tag: object) -> str:
    value = str(id_tag or "").strip() or "Unknown"
    return f"Tag ID: {value}"


# ── Main ───────────────────────────────────────────────────────────────────────

async def main() -> None:
    _LOGGER.info(
        "Starting GivEVC OCPPv2 — OCPP:%s  firmware:%s  web:%s",
        OCPP_PORT, FIRMWARE_PORT, INGRESS_PORT,
    )

    coordinator = OcppCoordinator(
        listen_port=OCPP_PORT,
        state_path=STATE_PATH,
        firmware_directory=FIRMWARE_ROOT,
        firmware_server_port=FIRMWARE_PORT,
        firmware_manifest_url=FIRMWARE_MANIFEST_URL,
        adopt_first_charger=ADOPT_FIRST,
        expected_charge_point_id=EXPECTED_CP_ID,
        debug_logging=DEBUG,
    )
    coordinator.load()

    firmware = FirmwareTransferServer(root=FIRMWARE_ROOT)
    loop = asyncio.get_running_loop()

    def _firmware_event(event: dict) -> None:
        _LOGGER.debug("Firmware: %s", event)
        loop.call_soon_threadsafe(coordinator.record_firmware_transfer_event, event)

    firmware.set_event_callback(_firmware_event)

    ocpp_server = OcppServer(coordinator)
    await ocpp_server.start()
    await firmware.start(FIRMWARE_PORT)

    web_app = build_web_app(coordinator, firmware)
    runner = web.AppRunner(web_app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, host="0.0.0.0", port=INGRESS_PORT).start()
    _LOGGER.info("Web UI on 0.0.0.0:%s", INGRESS_PORT)

    try:
        await asyncio.Event().wait()
    finally:
        await ocpp_server.stop()
        await firmware.stop()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
