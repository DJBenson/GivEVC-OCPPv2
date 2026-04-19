"""Standalone OCPP WebSocket server — no Home Assistant dependency.

Adapted from the GivEnergy EVC OCPP integration's server.py.
"""

from __future__ import annotations

import base64
import logging
import os
from typing import TYPE_CHECKING

from aiohttp import web

from .charge_point import ChargePointSession
from .coordinator import OcppCoordinator

if TYPE_CHECKING:
    from auth_store import AuthStore

_LOGGER = logging.getLogger(__name__)

WEBSOCKET_SUBPROTOCOL = "ocpp1.6"
DEFAULT_LISTEN_HOST = "0.0.0.0"


class OcppServer:
    """Manage the inbound OCPP WebSocket listener."""

    def __init__(self, coordinator: OcppCoordinator, auth_store: AuthStore | None = None) -> None:
        self.coordinator = coordinator
        self.auth_store = auth_store
        self._app = web.Application()
        self._app.router.add_get("/", self._handle_websocket)
        self._app.router.add_get("/{charge_point_id:.*}", self._handle_websocket)
        self._runner: web.AppRunner | None = None
        self._site: web.BaseSite | None = None
        self._sessions: dict[str, ChargePointSession] = {}

    async def start(self) -> None:
        self._runner = web.AppRunner(self._app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(
            self._runner,
            host=DEFAULT_LISTEN_HOST,
            port=self.coordinator.listen_port,
        )
        await self._site.start()
        _LOGGER.info(
            "OCPP server listening on %s:%s",
            DEFAULT_LISTEN_HOST,
            self.coordinator.listen_port,
        )

    async def stop(self) -> None:
        for session in list(self._sessions.values()):
            await session.async_close()
        self._sessions.clear()
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
            self._site = None

    async def send_call(
        self,
        action: str,
        payload: dict,
        charge_point_id: str,
        timeout: int = 20,
    ) -> dict:
        if not charge_point_id:
            raise RuntimeError("A charge point identity is required")
        if not self.coordinator.charge_point_can_receive_commands(charge_point_id):
            raise RuntimeError("Charger is not adopted")
        session = self._sessions.get(charge_point_id)
        if session is None or session.websocket.closed:
            raise RuntimeError("Requested charger is not currently connected")
        return await session.async_call(action, payload, timeout=timeout)

    async def switch_active_charge_point(self, charge_point_id: str | None) -> bool:
        """Promote the selected charge point session without closing other sessions."""
        selected = self._sessions.get(charge_point_id or "")

        if selected is not None and not selected.websocket.closed:
            selected.stateful = True
            self.coordinator.register_ocpp_caller(charge_point_id, selected)
            await self.coordinator.async_select_active_charge_point(charge_point_id)
            return True

        await self.coordinator.async_select_active_charge_point(charge_point_id)
        return False

    async def kick_charge_point(self, charge_point_id: str) -> bool:
        """Close the active WebSocket session for a charge point, if connected."""
        session = self._sessions.get(charge_point_id)
        if session is None or session.websocket.closed:
            return False
        await session.async_close("Charger removed")
        return True

    async def promote_adopted_charge_point(self, charge_point_id: str | None, *, active: bool = False) -> bool:
        """Mark an already-connected charger as adopted without waiting for reconnect."""
        charge_point_id = str(charge_point_id or "").strip()
        if not charge_point_id:
            return False

        selected = self._sessions.get(charge_point_id)
        if selected is not None and not selected.websocket.closed:
            selected.stateful = True
            self.coordinator.register_ocpp_caller(charge_point_id, selected)
            if active:
                await self.coordinator.async_select_active_charge_point(charge_point_id)
            else:
                self.coordinator.mark_connected_charge_point_adopted(charge_point_id, active=False)
            return True

        if active:
            await self.coordinator.async_select_active_charge_point(charge_point_id)
        else:
            self.coordinator.mark_connected_charge_point_adopted(charge_point_id, active=False)
        return False

    async def _handle_websocket(self, request: web.Request) -> web.StreamResponse:
        candidate_id = request.match_info.get("charge_point_id", "").strip("/") or None
        local_host: str | None = None
        if request.transport is not None:
            sockname = request.transport.get_extra_info("sockname")
            if isinstance(sockname, tuple) and sockname:
                local_host = str(sockname[0])
        remote_host = request.remote or None
        firmware_server_host = _request_host_without_port(request)

        if not candidate_id:
            return web.Response(status=400, text="Charge point identity is required")

        origin = request.headers.get("Origin")
        if origin is not None:
            allowed = _request_host_without_port(request)
            origin_host = origin.split("://", 1)[-1].split("/", 1)[0].split(":", 1)[0]
            if origin_host != allowed:
                return web.Response(status=403, text="Forbidden")

        # Extract Basic Auth password (username field is ignored — charger uses its own ID)
        password: str | None = None
        auth_header = request.headers.get("Authorization", "")
        if auth_header.lower().startswith("basic "):
            try:
                decoded = base64.b64decode(auth_header[6:]).decode("utf-8", errors="replace")
                password = decoded.split(":", 1)[1] if ":" in decoded else decoded
            except Exception:
                pass

        active_charge_point_id = self.coordinator.data.charge_point_id
        adopted = bool(candidate_id and self.coordinator.charge_point_can_receive_commands(candidate_id))

        if self.auth_store is not None:
            if adopted:
                # Already-registered charger — verify its stored password
                if not password or not self.auth_store.verify_charger_password(candidate_id, password):
                    _LOGGER.warning("OCPP auth failed for adopted charger %s", candidate_id)
                    return web.Response(
                        status=401,
                        headers={"WWW-Authenticate": 'Basic realm="OCPP"'},
                        text="Unauthorized",
                    )
            else:
                # Unknown charger — attempt to claim via onboarding session
                if not password:
                    _LOGGER.warning("OCPP connection from %s rejected: no credentials", candidate_id)
                    return web.Response(
                        status=401,
                        headers={"WWW-Authenticate": 'Basic realm="OCPP"'},
                        text="Unauthorized",
                    )
                try:
                    charger = self.auth_store.claim_charger_by_password(candidate_id, password)
                except ValueError:
                    charger = None
                if charger is None:
                    _LOGGER.warning("OCPP claim failed for %s: no matching onboarding session", candidate_id)
                    return web.Response(
                        status=401,
                        headers={"WWW-Authenticate": 'Basic realm="OCPP"'},
                        text="Unauthorized",
                    )
                _LOGGER.info("Charger %s claimed by user %s via onboarding", candidate_id, charger.get("user_id"))
                adopted = True
                active_charge_point_id = self.coordinator.data.charge_point_id

        if self.auth_store is not None and adopted:
            self.auth_store.record_charger_online(candidate_id)

        stateful = adopted
        selected_active = bool(adopted and candidate_id and candidate_id == active_charge_point_id)

        session_key = candidate_id or ""
        if candidate_id:
            existing = self._sessions.get(candidate_id)
            if existing is not None and not existing.websocket.closed:
                await existing.async_close("Replacing existing OCPP session")

        websocket = web.WebSocketResponse(protocols=(WEBSOCKET_SUBPROTOCOL,), heartbeat=15)
        await websocket.prepare(request)

        if websocket.ws_protocol != WEBSOCKET_SUBPROTOCOL:
            _LOGGER.warning(
                "Charger connected without negotiating %s; continuing anyway",
                WEBSOCKET_SUBPROTOCOL,
            )

        session = ChargePointSession(websocket, self.coordinator, candidate_id, stateful=stateful)
        session_key = candidate_id or session.session_id
        self._sessions[session_key] = session
        if stateful:
            self.coordinator.register_ocpp_caller(candidate_id, session)

        if stateful and selected_active:
            await self.coordinator.async_connection_opened(
                session.session_id,
                candidate_id,
                local_host,
                remote_host,
                firmware_server_host=firmware_server_host,
            )
        elif stateful:
            await self.coordinator.async_passive_connection_opened(
                session.session_id,
                candidate_id,
                local_host,
                remote_host,
                firmware_server_host=firmware_server_host,
            )
        else:
            await self.coordinator.async_unmanaged_connection_opened(
                session.session_id,
                candidate_id,
                local_host,
                remote_host,
                firmware_server_host=firmware_server_host,
            )

        try:
            await session.run()
        finally:
            if self._sessions.get(session_key) is session:
                self._sessions.pop(session_key, None)
            if session.stateful:
                self.coordinator.unregister_ocpp_caller(candidate_id, session)
            if session.stateful and candidate_id == self.coordinator.data.charge_point_id:
                await self.coordinator.async_connection_closed(session.session_id)
            elif session.stateful:
                await self.coordinator.async_passive_connection_closed(session.session_id)
            else:
                await self.coordinator.async_unmanaged_connection_closed(session.session_id)
            if self.auth_store is not None and session.stateful and candidate_id:
                self.auth_store.record_charger_offline(candidate_id)

        return websocket


def _request_host_without_port(request: web.Request) -> str | None:
    configured = os.environ.get("PUBLIC_FIRMWARE_HOST")
    if configured:
        return configured.strip() or None

    raw = (
        request.headers.get("X-Forwarded-Host")
        or request.headers.get("Host")
        or request.host
        or ""
    )
    host = str(raw).split(",", 1)[0].strip()
    if not host:
        return None
    if host.startswith("["):
        return host[1:].split("]", 1)[0].strip() or None
    if host.count(":") == 1:
        return host.rsplit(":", 1)[0].strip() or None
    return host
