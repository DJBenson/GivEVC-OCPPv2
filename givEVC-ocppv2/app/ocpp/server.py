"""Standalone OCPP WebSocket server — no Home Assistant dependency.

Adapted from the GivEnergy EVC OCPP integration's server.py.
"""

from __future__ import annotations

import logging

from aiohttp import web

from .charge_point import ChargePointSession
from .coordinator import OcppCoordinator

_LOGGER = logging.getLogger(__name__)

WEBSOCKET_SUBPROTOCOL = "ocpp1.6"
DEFAULT_LISTEN_HOST = "0.0.0.0"


class OcppServer:
    """Manage the inbound OCPP WebSocket listener."""

    def __init__(self, coordinator: OcppCoordinator) -> None:
        self.coordinator = coordinator
        self._app = web.Application()
        self._app.router.add_get("/", self._handle_websocket)
        self._app.router.add_get("/{charge_point_id:.*}", self._handle_websocket)
        self._runner: web.AppRunner | None = None
        self._site: web.BaseSite | None = None
        self._session: ChargePointSession | None = None

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
        if self._session is not None:
            await self._session.async_close()
            self._session = None
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
            self._site = None

    async def send_call(
        self, action: str, payload: dict, timeout: int = 20
    ) -> dict:
        if self._session is None:
            raise RuntimeError("No charger is currently connected")
        return await self._session.async_call(action, payload, timeout=timeout)

    async def _handle_websocket(self, request: web.Request) -> web.StreamResponse:
        candidate_id = request.match_info.get("charge_point_id", "").strip("/") or None
        local_host: str | None = None
        if request.transport is not None:
            sockname = request.transport.get_extra_info("sockname")
            if isinstance(sockname, tuple) and sockname:
                local_host = str(sockname[0])
        remote_host = request.remote or None

        if not self.coordinator.can_accept_charge_point(candidate_id):
            await self.coordinator.async_note_rejected_charge_point(candidate_id)
            _LOGGER.warning("Rejected unexpected charger connection: %s", candidate_id)
            return web.Response(status=403, text="Unexpected charge point ID")

        if self._session is not None and not self._session.websocket.closed:
            if candidate_id and candidate_id != self.coordinator.data.charge_point_id:
                return web.Response(status=409, text="A different charger is active")
            await self._session.async_close("Replacing existing OCPP session")

        websocket = web.WebSocketResponse(protocols=(WEBSOCKET_SUBPROTOCOL,), heartbeat=15)
        await websocket.prepare(request)

        if websocket.ws_protocol != WEBSOCKET_SUBPROTOCOL:
            _LOGGER.warning(
                "Charger connected without negotiating %s; continuing anyway",
                WEBSOCKET_SUBPROTOCOL,
            )

        session = ChargePointSession(websocket, self.coordinator, candidate_id)
        self._session = session
        self.coordinator.set_ocpp_caller(session)
        await self.coordinator.async_connection_opened(candidate_id, local_host, remote_host)

        try:
            await session.run()
        finally:
            if self._session is session:
                self._session = None
                self.coordinator.set_ocpp_caller(None)
                await self.coordinator.async_connection_closed()

        return websocket
