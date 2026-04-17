"""Local chunked firmware transfer server — no Home Assistant dependency.

Adapted from the GivEnergy EVC OCPP integration's firmware_transfer_server.py.
HA-specific imports (hass, HomeAssistantError, async_add_executor_job) replaced
with stdlib asyncio equivalents.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import math
from pathlib import Path
import socket
import threading
from typing import Any, Callable

_LOGGER = logging.getLogger(__name__)

DEFAULT_LISTEN_HOST = "0.0.0.0"
SOCKET_TIMEOUT = 30
RECV_BUFFER = 4096
DEFAULT_CHUNK_SIZE = 4096
CONNECTION_COUNTER = itertools.count(1)
ACTIVE_REQUESTS_LOCK = threading.Lock()
ACTIVE_REQUESTS: dict[tuple[str, str], int] = {}


def _log_prefix(trace_label: str | None) -> str:
    return f"[{trace_label}] " if trace_label else ""


def _extract_buffered_json(buffer: bytes) -> tuple[dict | None, bytes, str | None]:
    if not buffer:
        return None, buffer, None

    text = buffer.decode("utf-8", errors="replace")
    stripped = text.lstrip()
    if not stripped:
        return None, b"", None

    leading_whitespace_len = len(text) - len(stripped)
    try:
        obj, end = json.JSONDecoder().raw_decode(stripped)
    except json.JSONDecodeError:
        return None, buffer, None

    raw_text = stripped[:end]
    consumed_bytes = len((text[:leading_whitespace_len] + raw_text).encode("utf-8"))
    remaining = buffer[consumed_bytes:]
    return obj, remaining, raw_text


class _JsonSocketConnection:
    def __init__(
        self,
        sock: socket.socket,
        *,
        event_callback: Callable[[dict[str, Any]], None],
        trace_label: str | None = None,
        remote: str,
    ) -> None:
        self.sock = sock
        self.trace_label = trace_label
        self.buffer = b""
        self._event_callback = event_callback
        self._remote = remote

    def recv_json(self) -> dict | None:
        prefix = _log_prefix(self.trace_label)
        while True:
            obj, remaining, raw_text = _extract_buffered_json(self.buffer)
            if obj is not None:
                self.buffer = remaining
                self._event_callback({
                    "event": "control_frame_received",
                    "remote": self._remote,
                    "trace": prefix.strip(),
                    "payload": obj,
                    "raw": raw_text,
                    "buffered_bytes": len(self.buffer),
                })
                return obj

            try:
                chunk = self.sock.recv(RECV_BUFFER)
            except socket.timeout:
                self._event_callback({
                    "event": "socket_timeout",
                    "remote": self._remote,
                    "trace": prefix.strip(),
                })
                return None

            if not chunk:
                return None

            self.buffer += chunk


class FirmwareTransferServer:
    """Serve firmware files using the GivEnergy chunked transfer protocol."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self._event_callback: Callable[[dict[str, Any]], None] | None = None
        self._thread: threading.Thread | None = None
        self._server_socket: socket.socket | None = None
        self._startup_complete = threading.Event()
        self._stop_event = threading.Event()
        self._startup_error: Exception | None = None
        self._port: int | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def set_event_callback(self, callback: Callable[[dict[str, Any]], None]) -> None:
        """Attach a sync callback for transfer events (called from the server thread)."""
        self._event_callback = callback

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    async def start(self, port: int) -> None:
        if self.is_running:
            return

        self._loop = asyncio.get_running_loop()
        self.root.mkdir(parents=True, exist_ok=True)
        self._startup_complete.clear()
        self._stop_event.clear()
        self._startup_error = None
        self._port = port
        self._thread = threading.Thread(
            target=self._run_server,
            args=(port,),
            name="givevc-firmware-transfer",
            daemon=True,
        )
        self._thread.start()

        await asyncio.get_running_loop().run_in_executor(
            None, self._startup_complete.wait, 5.0
        )

        if self._startup_error is not None:
            self._thread = None
            raise RuntimeError(
                f"Unable to start firmware transfer server: {self._startup_error}"
            ) from self._startup_error

        _LOGGER.info("Firmware transfer server listening on port %s", port)

    async def stop(self) -> None:
        self._stop_event.set()
        if self._server_socket is not None:
            try:
                self._server_socket.close()
            except OSError:
                pass
        if self._thread is not None:
            await asyncio.get_running_loop().run_in_executor(None, self._thread.join, 5.0)
        self._server_socket = None
        self._thread = None
        self._startup_complete.clear()
        self._startup_error = None
        self._port = None

    # ── Internal ─────────────────────────────────────────────────────────

    def _emit_event(self, event: dict[str, Any]) -> None:
        if self._event_callback is not None:
            try:
                self._event_callback(event)
            except Exception:
                pass

    def _send_json(
        self, sock: socket.socket, obj: dict[str, Any], *, remote: str, trace_label: str
    ) -> None:
        payload = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        sock.sendall(payload)
        self._emit_event({
            "event": "control_frame_sent",
            "remote": remote,
            "trace": trace_label,
            "payload": obj,
        })

    def _resolve_firmware_path(self, filename: str) -> Path | None:
        safe_name = Path(filename.lstrip("/\\")).as_posix()
        if ".." in safe_name.split("/"):
            return None
        for candidate in [self.root / safe_name, self.root / Path(safe_name).name]:
            if candidate.is_file():
                return candidate
        return None

    @staticmethod
    def _register_active_request(charger_ip: str, filename: str, session_id: int) -> int | None:
        key = (charger_ip, filename)
        with ACTIVE_REQUESTS_LOCK:
            previous = ACTIVE_REQUESTS.get(key)
            ACTIVE_REQUESTS[key] = session_id
        return previous

    @staticmethod
    def _unregister_active_request(charger_ip: str, filename: str, session_id: int) -> None:
        key = (charger_ip, filename)
        with ACTIVE_REQUESTS_LOCK:
            if ACTIVE_REQUESTS.get(key) == session_id:
                del ACTIVE_REQUESTS[key]

    def _handle_download(
        self,
        conn: _JsonSocketConnection,
        remote: str,
        request: dict[str, Any],
        *,
        trace_label: str,
    ) -> None:
        sock = conn.sock
        requested_filename = str(request.get("filename", ""))
        pack_len = max(1, int(request.get("packlen", DEFAULT_CHUNK_SIZE) or DEFAULT_CHUNK_SIZE))
        file_path = self._resolve_firmware_path(requested_filename)

        if file_path is None:
            self._emit_event({"event": "file_not_found", "remote": remote, "requested_filename": requested_filename, "trace": trace_label})
            self._send_json(sock, {"res": "File does not exist"}, remote=remote, trace_label=trace_label)
            return

        file_size = file_path.stat().st_size
        pack_num = math.ceil(file_size / pack_len)

        checksum = 0
        with file_path.open("rb") as f:
            while chunk := f.read(1024 * 1024):
                for byte in chunk:
                    checksum = (checksum + byte) & 0xFFFFFFFF

        self._emit_event({"event": "download_started", "remote": remote, "trace": trace_label, "filename": file_path.name, "filesize": file_size, "chunk_size": pack_len, "chunk_count": pack_num, "checksum": checksum})
        self._send_json(sock, {"res": "ok", "filesize": str(file_size), "packnum": str(pack_num), "checksum": str(checksum)}, remote=remote, trace_label=trace_label)

        bytes_sent = 0
        try:
            with file_path.open("rb") as f:
                while True:
                    result = conn.recv_json()
                    if result is None:
                        self._emit_event({"event": "checksum_missing", "remote": remote, "trace": trace_label, "filename": file_path.name})
                        return

                    if "checksum" in result:
                        charger_checksum = str(result.get("checksum"))
                        ok = charger_checksum == "ok"
                        self._emit_event({"event": "checksum_ok" if ok else "checksum_mismatch", "remote": remote, "trace": trace_label, "filename": file_path.name, "charger_checksum": charger_checksum, "server_checksum": str(checksum)})
                        return

                    if "packsn" not in result:
                        self._emit_event({"event": "unexpected_control_frame", "remote": remote, "trace": trace_label, "payload": result})
                        continue

                    try:
                        pack_sn = int(result["packsn"])
                    except (TypeError, ValueError):
                        self._emit_event({"event": "invalid_packsn", "remote": remote, "trace": trace_label, "value": result.get("packsn")})
                        continue

                    if pack_sn < 0 or pack_sn >= pack_num:
                        self._emit_event({"event": "out_of_range_packsn", "remote": remote, "trace": trace_label, "packsn": pack_sn, "max_packsn": pack_num - 1})
                        continue

                    offset = pack_sn * pack_len
                    chunk_size = min(pack_len, file_size - offset)
                    f.seek(offset)
                    data = f.read(chunk_size)
                    if len(data) != chunk_size:
                        self._emit_event({"event": "chunk_read_error", "remote": remote, "trace": trace_label, "packsn": pack_sn})
                        return

                    sock.sendall(data)
                    bytes_sent += len(data)
                    self._emit_event({"event": "chunk_sent", "remote": remote, "trace": trace_label, "packsn": pack_sn, "bytes": len(data)})
        except OSError as err:
            self._emit_event({"event": "client_error", "remote": remote, "trace": trace_label, "error": str(err)})
        finally:
            self._emit_event({"event": "file_sent", "remote": remote, "trace": trace_label, "filename": file_path.name, "bytes": bytes_sent})

    def _handle_upload(
        self,
        conn: _JsonSocketConnection,
        remote: str,
        request: dict[str, Any],
        *,
        trace_label: str,
    ) -> None:
        sock = conn.sock
        filename = Path(str(request.get("filename", "upload.bin"))).name
        pack_len = max(1, int(request.get("packlen", DEFAULT_CHUNK_SIZE) or DEFAULT_CHUNK_SIZE))
        pack_num = max(0, int(request.get("packnum", 0) or 0))
        expected_checksum = int(request.get("checksum", 0) or 0)
        save_dir = self.root / "uploads"
        save_dir.mkdir(parents=True, exist_ok=True)
        save_path = save_dir / filename

        self._emit_event({"event": "upload_started", "remote": remote, "trace": trace_label, "filename": filename, "chunk_size": pack_len, "chunk_count": pack_num})

        checksum = 0
        bytes_received = 0
        with save_path.open("wb") as out:
            for pack_sn in range(pack_num):
                data = b""
                while len(data) < pack_len:
                    try:
                        chunk = sock.recv(pack_len - len(data))
                    except socket.timeout:
                        self._emit_event({"event": "socket_timeout", "remote": remote, "trace": trace_label})
                        break
                    if not chunk:
                        break
                    data += chunk
                if not data:
                    break
                out.write(data)
                bytes_received += len(data)
                for byte in data:
                    checksum = (checksum + byte) & 0xFFFFFFFF
                self._send_json(sock, {"packsn": str(pack_sn)}, remote=remote, trace_label=trace_label)

        ok = checksum == expected_checksum
        self._send_json(sock, {"checksum": "ok" if ok else "false"}, remote=remote, trace_label=trace_label)
        self._emit_event({"event": "upload_checksum_ok" if ok else "upload_checksum_mismatch", "remote": remote, "trace": trace_label, "filename": filename, "bytes": bytes_received, "checksum": checksum, "expected_checksum": expected_checksum})

    def _handle_client(self, conn: socket.socket, addr: tuple[str, int]) -> None:
        session_id = next(CONNECTION_COUNTER)
        remote = f"{addr[0]}:{addr[1]}"
        trace_label = f"conn={session_id} peer={remote}"
        active_filename: str | None = None
        json_conn = _JsonSocketConnection(conn, event_callback=self._emit_event, trace_label=trace_label, remote=remote)
        self._emit_event({"event": "connect", "remote": remote, "trace": trace_label})
        conn.settimeout(SOCKET_TIMEOUT)

        try:
            request = json_conn.recv_json()
            if request is None:
                self._emit_event({"event": "request_missing", "remote": remote, "trace": trace_label})
                return

            requested_filename = str(request.get("filename", ""))
            active_filename = requested_filename
            upload = str(request.get("upload", "0"))
            self._emit_event({"event": "request_received", "remote": remote, "trace": trace_label, "filename": requested_filename, "upload": upload})

            if not requested_filename:
                self._send_json(conn, {"res": "Data format error"}, remote=remote, trace_label=trace_label)
                return

            previous_session_id = self._register_active_request(addr[0], requested_filename, session_id)
            if upload != "1" and previous_session_id is not None and previous_session_id != session_id:
                self._emit_event({"event": "overlapping_request", "remote": remote, "trace": trace_label, "filename": requested_filename, "previous_session_id": previous_session_id})

            if upload == "1":
                self._handle_upload(json_conn, remote, request, trace_label=trace_label)
            else:
                self._handle_download(json_conn, remote, request, trace_label=trace_label)

        except Exception as err:
            self._emit_event({"event": "client_error", "remote": remote, "trace": trace_label, "error": str(err)})
        finally:
            if active_filename:
                self._unregister_active_request(addr[0], active_filename, session_id)
            try:
                conn.close()
            except Exception:
                pass
            self._emit_event({"event": "disconnect", "remote": remote, "trace": trace_label})

    def _run_server(self, port: int) -> None:
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((DEFAULT_LISTEN_HOST, port))
            server.listen()
            server.settimeout(0.5)
            self._server_socket = server
            self._emit_event({"event": "server_started", "port": port, "root": str(self.root)})
            self._startup_complete.set()

            while not self._stop_event.is_set():
                try:
                    conn, addr = server.accept()
                except socket.timeout:
                    continue
                except OSError:
                    if self._stop_event.is_set():
                        break
                    raise

                threading.Thread(
                    target=self._handle_client,
                    args=(conn, addr),
                    name="givevc-firmware-client",
                    daemon=True,
                ).start()

        except Exception as err:
            self._startup_error = err
            self._emit_event({"event": "server_error", "port": self._port, "error": str(err)})
            self._startup_complete.set()
        finally:
            if self._server_socket is not None:
                try:
                    self._server_socket.close()
                except OSError:
                    pass
            if self._port is not None:
                self._emit_event({"event": "server_stopped", "port": self._port})
            self._server_socket = None
