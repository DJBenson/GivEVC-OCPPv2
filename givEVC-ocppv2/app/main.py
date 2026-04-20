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
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path

from aiohttp import http_exceptions, web

from auth_store import (
    AuthStore, AuthUser, ROLE_ADMIN,
    DEMO_EMAIL, DEMO_PASSWORD, DEMO_CHARGE_POINT_ID, SYSTEM_SETTING_DEMO_MODE,
    SYSTEM_SETTING_UPDATE_CHANNEL,
)
from demo_simulator import DemoChargerSimulator
from emailer import EmailSender
from ocpp.coordinator import (
    CHARGE_DISABLED_STATUSES,
    CHARGE_START_STATUSES,
    CHARGE_STOP_STATUSES,
    OcppCoordinator,
    _state_to_dict,
)
from ocpp.firmware_server import FirmwareTransferServer
from ocpp.server import OcppServer
from ocpp.state import ChargerState

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


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        _LOGGER.warning("Invalid %s=%r; using %s", name, raw, default)
        return default


def _env_str(name: str, default: str) -> str:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    return raw


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


# ── Config from environment ────────────────────────────────────────────────────
OCPP_PORT     = _env_int("OCPP_PORT", 7655)
FIRMWARE_PORT = _env_int("FIRMWARE_PORT", 9688)
INGRESS_PORT  = _env_int("INGRESS_PORT", 8099)
DEBUG         = os.environ.get("DEBUG_LOGGING", "").lower() in ("1", "true", "yes")

DATA_DIR      = Path(_env_str("DATA_DIR", "/data"))
FIRMWARE_ROOT = Path(_env_str("FIRMWARE_ROOT", str(DATA_DIR / "firmware")))
LEGACY_STATE_PATH = DATA_DIR / "state.json"
AUTH_DB_PATH  = DATA_DIR / "auth.db"
TEMPLATES     = Path(__file__).parent / "templates"

def _read_app_version() -> str:
    # In Docker: config.yaml is copied to /app/ alongside main.py
    # In development: config.yaml is one level up in givEVC-ocppv2/
    for config_path in (
        Path(__file__).parent / "config.yaml",
        Path(__file__).parent.parent / "config.yaml",
    ):
        if not config_path.exists():
            continue
        try:
            import yaml  # type: ignore
            data = yaml.safe_load(config_path.read_text())
            return str(data.get("version", "unknown"))
        except Exception:
            pass
        try:
            for line in config_path.read_text().splitlines():
                if line.startswith("version:"):
                    return line.split(":", 1)[1].strip().strip('"\'')
        except Exception:
            pass
    return "unknown"

APP_VERSION = _read_app_version()
FIRMWARE_MANIFEST_URL = _env_str("FIRMWARE_MANIFEST_URL", DEFAULT_FIRMWARE_MANIFEST_URL)
PUBLIC_OCPP_BASE_URL = os.environ.get("PUBLIC_OCPP_BASE_URL") or None
PUBLIC_FIRMWARE_HOST = os.environ.get("PUBLIC_FIRMWARE_HOST") or None
PUBLIC_FIRMWARE_PORT = _env_int("PUBLIC_FIRMWARE_PORT", FIRMWARE_PORT)
SESSION_COOKIE = "givevc_session"
SMTP_HOST = _env_str("SMTP_HOST", "")
SMTP_PORT = _env_int("SMTP_PORT", 587)
SMTP_USERNAME = _env_str("SMTP_USERNAME", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
SMTP_FROM = _env_str("SMTP_FROM", "")
SMTP_TLS = _env_bool("SMTP_TLS", True)


# ── Web app ────────────────────────────────────────────────────────────────────

def build_web_app(
    coordinator: OcppCoordinator,
    firmware: FirmwareTransferServer,
    ocpp_server: OcppServer | None = None,
    auth_store: AuthStore | None = None,
    demo_mode_callback=None,
) -> web.Application:
    auth_store = auth_store or AuthStore(AUTH_DB_PATH)
    email_sender = EmailSender(
        host=SMTP_HOST,
        port=SMTP_PORT,
        username=SMTP_USERNAME,
        password=SMTP_PASSWORD,
        sender=SMTP_FROM,
        tls=SMTP_TLS,
    )
    coordinator.set_charge_point_command_authorizer(
        lambda charge_point_id: bool(
            charge_point_id and auth_store.get_charger_by_charge_point_id(charge_point_id)
        )
    )

    @web.middleware
    async def auth_middleware(request: web.Request, handler):
        user = auth_store.get_user_for_session(request.cookies.get(SESSION_COOKIE))
        if user is not None:
            request["user"] = user

        if request.path == "/api/v1" or request.path.startswith("/api/v1/"):
            if not auth_store.is_public_api_enabled():
                return _json_response({"message": "Public API is disabled."}, status=503)

        if (request.path.startswith("/api/")
                and not request.path.startswith("/api/auth/")
                and not request.path.startswith("/api/v1/")):
            if user is None:
                return _json_response({"error": "Authentication required"}, status=401)

        try:
            return await handler(request)
        except web.HTTPNotFound:
            if request.path.startswith("/api/"):
                return _json_response({"message": "Not found."}, status=404)
            raise

    app = web.Application(middlewares=[auth_middleware])

    def _chargers_for_user(user_id: str) -> list[dict]:
        return _enrich_chargers_with_connection(auth_store.list_chargers(user_id), coordinator)

    def _active_charger_for_user(user_id: str) -> dict | None:
        charger = auth_store.get_active_charger(user_id)
        if charger is None:
            return None
        return _enrich_chargers_with_connection([charger], coordinator)[0]

    def _disconnected_state_for_charger(charger: dict | None = None) -> dict:
        state = _state_to_dict(ChargerState())
        if charger:
            state["charge_point_id"] = charger.get("charge_point_id")
            state["manufacturer"] = charger.get("manufacturer")
            state["model"] = charger.get("model") or charger.get("display_name")
            state["firmware_version"] = charger.get("firmware")
            state["charge_point_serial_number"] = charger.get("serial")
            state["charge_box_serial_number"] = charger.get("serial")
            state["websocket_remote_address"] = charger.get("remote_address")
            state["local_ip_address"] = charger.get("remote_address")
            state["connected"] = False
            state["connection_state"] = "disconnected"
        return state

    def _live_connection_for_charge_point(charge_point_id: str) -> dict | None:
        charge_point_id = str(charge_point_id or "").strip()
        if not charge_point_id:
            return None
        for item in coordinator.connected_charge_points():
            if str(item.get("charge_point_id") or "").strip() == charge_point_id:
                return item
        return None

    def _apply_live_connection_to_state(state: dict, connection: dict | None) -> dict:
        if not connection:
            state["connected"] = False
            state["connection_state"] = "disconnected"
            return state
        connection_state = str(connection.get("connection_state") or "connected")
        state["connected"] = connection_state == "connected"
        state["connection_state"] = connection_state
        state["charge_point_id"] = connection.get("charge_point_id") or state.get("charge_point_id")
        state["manufacturer"] = connection.get("manufacturer") or state.get("manufacturer")
        state["model"] = connection.get("model") or state.get("model")
        state["firmware_version"] = connection.get("firmware") or state.get("firmware_version")
        state["charge_point_serial_number"] = (
            connection.get("charge_point_serial_number")
            or connection.get("serial")
            or state.get("charge_point_serial_number")
        )
        state["charge_box_serial_number"] = (
            connection.get("charge_box_serial_number")
            or state.get("charge_box_serial_number")
        )
        state["websocket_remote_address"] = connection.get("remote_address") or state.get("websocket_remote_address")
        state["local_ip_address"] = connection.get("local_ip_address") or connection.get("remote_address") or state.get("local_ip_address")
        state["status"] = connection.get("status") or state.get("status")
        state["error_code"] = connection.get("error_code") or state.get("error_code")
        state["vendor_error_code"] = connection.get("vendor_error_code") or state.get("vendor_error_code")
        state["last_seen"] = connection.get("last_seen") or state.get("last_seen")
        return state

    def _email_verification_required_payload(email: str, error: str | None = None) -> dict:
        payload = {
            "authenticated": False,
            "email_verification_required": True,
            "email": email,
            "message": "Enter the 6 digit code sent to your email address.",
            "resend_after_seconds": 30,
        }
        if error:
            payload["error"] = error
        return payload

    def _send_email_verification_code(email: str, otp: str) -> dict[str, object]:
        return email_sender.send_verification_otp(email, otp)

    def _email_sender_unconfigured_error() -> web.Response | None:
        if email_sender.configured:
            return None
        return _json_response({"error": "SMTP is not configured"}, status=503)

    def _admin_email_settings_payload() -> dict[str, object]:
        return {
            "registration_enabled": auth_store.is_registration_enabled(),
            "initial_email_validation_enabled": auth_store.is_email_verification_enabled(),
            "public_api_enabled": auth_store.is_public_api_enabled(),
            "smtp_configured": email_sender.configured,
            "smtp_host": SMTP_HOST,
            "smtp_from": SMTP_FROM,
        }

    def _merge_live_session_into_stats(state: dict, session_stats: dict) -> dict:
        """Add in-progress session energy to today/month totals so stats update in real time."""
        live_kwh = state.get("session_energy_kwh") or 0.0
        transaction_active = bool(state.get("transaction_active") or state.get("transaction_id"))
        if not transaction_active or live_kwh <= 0:
            return session_stats
        merged = dict(session_stats)
        if merged.get("today_kwh") is not None:
            merged["today_kwh"] = round(merged["today_kwh"] + live_kwh, 3)
        else:
            merged["today_kwh"] = round(live_kwh, 3)
        if merged.get("month_kwh") is not None:
            merged["month_kwh"] = round(merged["month_kwh"] + live_kwh, 3)
        else:
            merged["month_kwh"] = round(live_kwh, 3)
        return merged

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

    def _owned_active_charger_error(request: web.Request) -> web.Response | None:
        user = _require_user(request)
        active = _active_charger_for_user(user.id)
        if active is None:
            return _json_response({"error": "No adopted charger is selected"}, status=403)

        selected_charge_point_id = str(active.get("charge_point_id") or "").strip()
        if not selected_charge_point_id:
            return _json_response({"error": "Selected charger has no charge point identity"}, status=403)

        owner = auth_store.get_charger_by_charge_point_id(selected_charge_point_id)
        if owner is None:
            return _json_response({"error": "Selected charger is not adopted"}, status=403)
        if owner.get("user_id") != user.id:
            return _json_response({"error": "Selected charger belongs to another account"}, status=403)
        return None

    def _active_charge_point_id_for_request(request: web.Request) -> str:
        user = _require_user(request)
        active = _active_charger_for_user(user.id)
        return str((active or {}).get("charge_point_id") or "").strip()

    def _active_state_for_request(request: web.Request) -> ChargerState:
        return coordinator.state_for_charge_point(_active_charge_point_id_for_request(request))

    # ── Static UI ──────────────────────────────────────────────────────────
    async def index(request: web.Request) -> web.Response:
        return web.Response(
            content_type="text/html",
            text=(TEMPLATES / "index.html").read_text(),
            headers={
                "Content-Security-Policy": (
                    "default-src 'self'; "
                    "script-src 'self' 'unsafe-inline'; "
                    "style-src 'self' 'unsafe-inline'; "
                    "img-src 'self' data:; "
                    "connect-src 'self'"
                ),
            },
        )

    # ── REST: current state snapshot ───────────────────────────────────────
    async def api_state(request: web.Request) -> web.Response:
        user = _require_user(request)
        return web.Response(
            content_type="application/json",
            text=json.dumps(_state_for_user(user)),
        )

    async def api_energy(request: web.Request) -> web.Response:
        user = _require_user(request)
        active = _active_charger_for_user(user.id)
        if active is None:
            return _json_response({"error": "No adopted charger is selected"}, status=403)
        charge_point_id = str(active.get("charge_point_id") or "").strip()
        if not charge_point_id:
            return _json_response({"error": "Selected charger has no charge point identity"}, status=403)

        data = auth_store.get_energy_buckets(
            user.id,
            charge_point_id,
            period=request.rel_url.query.get("period") or "daily",
            anchor_date=request.rel_url.query.get("date") or None,
        )
        if data is None:
            return _json_response({"error": "Selected charger is not available"}, status=404)
        data["charge_point_id"] = charge_point_id
        return _json_response({"energy": data})

    async def api_power(request: web.Request) -> web.Response:
        user = _require_user(request)
        active = _active_charger_for_user(user.id)
        if active is None:
            return _json_response({"error": "No adopted charger is selected"}, status=403)
        charge_point_id = str(active.get("charge_point_id") or "").strip()
        if not charge_point_id:
            return _json_response({"error": "Selected charger has no charge point identity"}, status=403)

        data = auth_store.get_power_samples(
            user.id,
            charge_point_id,
            anchor_date=request.rel_url.query.get("date") or None,
            period=request.rel_url.query.get("period") or "daily",
        )
        if data is None:
            return _json_response({"error": "Selected charger is not available"}, status=404)
        data["charge_point_id"] = charge_point_id
        return _json_response({"power": data})

    # ── Version ───────────────────────────────────────────────────────────
    async def api_version(request: web.Request) -> web.Response:
        return web.json_response({"version": APP_VERSION})

    # ── REST: browser authentication ──────────────────────────────────────
    async def api_auth_session(request: web.Request) -> web.Response:
        user = request.get("user")
        registration_role = auth_store.next_registration_role() if user is None else None
        payload = {
            "authenticated": user is not None,
            "user": _user_payload(user) if user else None,
            "registration_role": registration_role,
            "first_user_required": registration_role == ROLE_ADMIN,
            "registration_enabled": auth_store.is_registration_enabled(),
            "chargers": _chargers_for_user(user.id) if user else [],
            "onboarding_sessions": auth_store.list_onboarding_sessions(user.id) if user else [],
        }
        return _json_response(payload)

    async def api_auth_register(request: web.Request) -> web.Response:
        if not auth_store.is_registration_enabled():
            return _json_response({"error": "New account registration is currently disabled"}, status=403)
        try:
            body = await request.json()
            email = str(body.get("email", ""))
            password = str(body.get("password", ""))
            password_confirm = str(body.get("password_confirm", password))
            if password != password_confirm:
                raise ValueError("Passwords do not match")
            display_name = str(body.get("display_name", "")).strip() or None
            email_verification_enabled = auth_store.is_email_verification_enabled()
            if email_verification_enabled:
                smtp_error = _email_sender_unconfigured_error()
                if smtp_error:
                    return smtp_error
            user = auth_store.create_user(email, password, display_name)
            if not email_verification_enabled:
                auth_store.mark_user_email_verified(user.id)
                session_id, expires_at = auth_store.create_session(user.id)
                verified_user = auth_store.get_user_for_session(session_id)
                response = _json_response({
                    "authenticated": True,
                    "user": _user_payload(verified_user or user),
                    "chargers": [],
                    "onboarding_sessions": [],
                })
                _set_session_cookie(request, response, session_id, expires_at)
                return response
            verification = auth_store.create_email_verification_otp(user.id)
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=400)
        except Exception:
            _LOGGER.exception("Registration failed")
            return _json_response({"error": "Registration failed"}, status=500)

        try:
            _send_email_verification_code(user.email, verification["otp"])
            return _json_response(_email_verification_required_payload(user.email))
        except Exception:
            _LOGGER.exception("Email verification send failed for %s", user.email)
            return _json_response(
                _email_verification_required_payload(
                    user.email,
                    "Account created, but the verification email could not be sent. Check SMTP settings and resend the code.",
                )
            )

    async def api_auth_login(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            email = str(body.get("email", ""))
            password = str(body.get("password", ""))
            otp = str(body.get("otp", "")).strip()
        except Exception:
            return _json_response({"error": "Expected {email, password}"}, status=400)

        user = auth_store.authenticate_password(email, password)
        if user is None:
            return _json_response({"error": "Invalid email or password"}, status=401)
        if user.totp_enabled:
            if not otp:
                return _json_response({
                    "authenticated": False,
                    "otp_required": True,
                    "error": "Enter your authentication code",
                })
            if not auth_store.verify_user_totp(user.id, otp):
                return _json_response({"error": "Invalid authentication code", "otp_required": True}, status=401)

        if not user.email_verified_at:
            if not auth_store.is_email_verification_enabled():
                auth_store.mark_user_email_verified(user.id)
                user = auth_store.authenticate_password(email, password)
                if user is None:
                    return _json_response({"error": "Invalid email or password"}, status=401)
            else:
                try:
                    smtp_error = _email_sender_unconfigured_error()
                    if smtp_error:
                        return smtp_error
                    verification = auth_store.create_email_verification_otp(user.id)
                    _send_email_verification_code(user.email, verification["otp"])
                except Exception:
                    _LOGGER.exception("Email verification send failed for %s", user.email)
                return _json_response(_email_verification_required_payload(user.email))

        session_id, expires_at = auth_store.create_session(user.id)
        response = _json_response({
            "authenticated": True,
            "user": _user_payload(user),
            "chargers": _chargers_for_user(user.id),
            "onboarding_sessions": auth_store.list_onboarding_sessions(user.id),
        })
        _set_session_cookie(request, response, session_id, expires_at)
        return response

    async def api_auth_verify_email(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            email = str(body.get("email", ""))
            otp = str(body.get("otp", ""))
            user = auth_store.verify_email_otp(email, otp)
            session_id, expires_at = auth_store.create_session(user.id)
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=400)
        except Exception:
            _LOGGER.exception("Email verification failed")
            return _json_response({"error": "Email verification failed"}, status=500)

        response = _json_response({
            "authenticated": True,
            "user": _user_payload(user),
            "chargers": _chargers_for_user(user.id),
            "onboarding_sessions": auth_store.list_onboarding_sessions(user.id),
        })
        _set_session_cookie(request, response, session_id, expires_at)
        return response

    async def api_auth_resend_email_otp(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            email = str(body.get("email", ""))
            smtp_error = _email_sender_unconfigured_error()
            if smtp_error:
                return smtp_error
            verification = auth_store.resend_email_verification_otp(email)
            _send_email_verification_code(verification["email"], verification["otp"])
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=400)
        except Exception:
            _LOGGER.exception("Email verification resend failed")
            return _json_response({"error": "Verification email could not be sent"}, status=500)
        return _json_response({
            "ok": True,
            "message": "A new verification code has been sent.",
            "resend_after_seconds": 30,
        })

    async def api_auth_forgot_password(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            email = str(body.get("email", ""))
        except Exception:
            return _json_response({"error": "Expected {email}"}, status=400)
        _generic_ok = _json_response({
            "ok": True,
            "message": "If that email address is registered you will receive a password reset link shortly.",
        })
        if email.strip().lower() == DEMO_EMAIL.lower():
            return _generic_ok
        try:
            smtp_error = _email_sender_unconfigured_error()
            if smtp_error:
                return smtp_error
            token = auth_store.create_password_reset_token(email)
            if token:
                origin = str(request.url.origin())
                reset_url = f"{origin}/?reset={token}"
                await asyncio.get_running_loop().run_in_executor(
                    None, email_sender.send_password_reset, email.strip().lower(), reset_url
                )
        except Exception:
            _LOGGER.exception("Password reset request failed for %s", email)
        return _generic_ok

    async def api_auth_validate_reset_token(request: web.Request) -> web.Response:
        token = str(request.rel_url.query.get("token", "")).strip()
        if not token or not auth_store.validate_password_reset_token(token):
            return _json_response({"valid": False}, status=400)
        return _json_response({"valid": True})

    async def api_auth_reset_password(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            token = str(body.get("token", "")).strip()
            new_password = str(body.get("password", ""))
            password_confirm = str(body.get("password_confirm", new_password))
            if new_password != password_confirm:
                return _json_response({"error": "Passwords do not match"}, status=400)
            auth_store.reset_password_with_token(token, new_password)
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=400)
        except Exception:
            _LOGGER.exception("Password reset failed")
            return _json_response({"error": "Password reset failed"}, status=500)
        return _json_response({"ok": True})

    async def api_auth_logout(request: web.Request) -> web.Response:
        auth_store.delete_session(request.cookies.get(SESSION_COOKIE))
        response = _json_response({"ok": True})
        response.del_cookie(SESSION_COOKIE, path="/")
        return response

    async def api_account_security(request: web.Request) -> web.Response:
        user = _require_user(request)
        return _json_response({"security": auth_store.get_account_security(user.id)})

    async def api_account_theme(request: web.Request) -> web.Response:
        user = _require_user(request)
        try:
            body = await request.json()
            updated = auth_store.set_user_theme_preference(user.id, str(body.get("theme", "")))
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=400)
        except Exception:
            _LOGGER.exception("Theme preference update failed")
            return _json_response({"error": "Theme preference update failed"}, status=500)
        return _json_response({"ok": True, "user": _user_payload(updated)})

    async def api_account_password(request: web.Request) -> web.Response:
        user = _require_user(request)
        if user.is_demo:
            return _json_response({"error": "Password changes are disabled for the demo account"}, status=403)
        try:
            body = await request.json()
            current_password = str(body.get("current_password", ""))
            new_password = str(body.get("new_password", ""))
            new_password_confirm = str(body.get("new_password_confirm", new_password))
            if new_password != new_password_confirm:
                raise ValueError("Passwords do not match")
            auth_store.change_password(
                user.id,
                current_password,
                new_password,
                keep_session_token=request.cookies.get(SESSION_COOKIE),
            )
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=400)
        except Exception:
            _LOGGER.exception("Password change failed")
            return _json_response({"error": "Password change failed"}, status=500)
        return _json_response({"ok": True})

    async def api_account_2fa_setup(request: web.Request) -> web.Response:
        user = _require_user(request)
        if user.is_demo:
            return _json_response({"error": "2FA is disabled for the demo account"}, status=403)
        try:
            body = await request.json()
            setup = auth_store.create_totp_setup(user.id, str(body.get("current_password", "")))
            setup["qr_svg"] = _totp_qr_svg(setup["provisioning_uri"])
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=400)
        except Exception:
            _LOGGER.exception("2FA setup failed")
            return _json_response({"error": "2FA setup failed"}, status=500)
        return _json_response({"setup": setup})

    async def api_account_2fa_enable(request: web.Request) -> web.Response:
        user = _require_user(request)
        if user.is_demo:
            return _json_response({"error": "2FA is disabled for the demo account"}, status=403)
        try:
            body = await request.json()
            auth_store.enable_totp(user.id, str(body.get("otp", "")))
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=400)
        except Exception:
            _LOGGER.exception("2FA enable failed")
            return _json_response({"error": "2FA enable failed"}, status=500)
        return _json_response({"ok": True, "security": auth_store.get_account_security(user.id)})

    async def api_account_2fa_disable(request: web.Request) -> web.Response:
        user = _require_user(request)
        if user.is_demo:
            return _json_response({"error": "2FA is disabled for the demo account"}, status=403)
        try:
            body = await request.json()
            auth_store.disable_totp(
                user.id,
                str(body.get("current_password", "")),
                str(body.get("otp", "")),
            )
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=400)
        except Exception:
            _LOGGER.exception("2FA disable failed")
            return _json_response({"error": "2FA disable failed"}, status=500)
        return _json_response({"ok": True, "security": auth_store.get_account_security(user.id)})

    async def api_account_api_keys(request: web.Request) -> web.Response:
        user = _require_user(request)
        return _json_response({"keys": auth_store.list_api_keys(user.id)})

    async def api_account_create_api_key(request: web.Request) -> web.Response:
        user = _require_user(request)
        try:
            body = await request.json()
            created = auth_store.create_api_key(
                user.id,
                str(body.get("name", "")),
                str(body.get("scope", "")),
                str(body.get("expiry") or "90"),
            )
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=400)
        except Exception:
            _LOGGER.exception("API key creation failed")
            return _json_response({"error": "API key creation failed"}, status=500)
        return _json_response({"api_key": created["api_key"], "key": created["key"], "keys": auth_store.list_api_keys(user.id)}, status=201)

    async def api_account_revoke_api_key(request: web.Request) -> web.Response:
        user = _require_user(request)
        key_id = str(request.match_info["id"])
        if not auth_store.revoke_api_key(user.id, key_id):
            return _json_response({"error": "API key not found"}, status=404)
        return _json_response({"ok": True, "keys": auth_store.list_api_keys(user.id)})

    async def api_account_delete_self(request: web.Request) -> web.Response:
        user = _require_user(request)
        if user.is_demo:
            return _json_response({"error": "Account deletion is disabled for the demo account"}, status=403)
        auth_store.delete_user(user.id)
        response = _json_response({"ok": True})
        response.del_cookie(SESSION_COOKIE, path="/")
        return response

    # ── REST: first-pass multi-user charger onboarding ────────────────────
    async def api_list_chargers(request: web.Request) -> web.Response:
        user = _require_user(request)
        return _json_response({"chargers": _chargers_for_user(user.id)})

    async def api_delete_charger(request: web.Request) -> web.Response:
        user = _require_user(request)
        if user.is_demo:
            return _json_response({"error": "Charger deletion is disabled for the demo account"}, status=403)
        charger_id = str(request.match_info["id"])
        existing = next((c for c in auth_store.list_chargers(user.id) if c["id"] == charger_id), None)
        if not auth_store.delete_charger(user.id, charger_id):
            return _json_response({"error": "Charger not found"}, status=404)
        if ocpp_server is not None and existing and existing.get("charge_point_id"):
            asyncio.ensure_future(ocpp_server.kick_charge_point(existing["charge_point_id"]))
        return _json_response({"ok": True, "chargers": _chargers_for_user(user.id)})

    async def api_switch_charger(request: web.Request) -> web.Response:
        user = _require_user(request)
        charger_id = str(request.match_info["id"])
        try:
            result = auth_store.switch_active_charger(user.id, charger_id)
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=404)
        result["chargers"] = _enrich_chargers_with_connection(result["chargers"], coordinator)
        active = next((charger for charger in result["chargers"] if charger.get("active")), None)
        if ocpp_server is not None:
            await ocpp_server.switch_active_charge_point(active.get("charge_point_id") if active else None)
        return _json_response(result)

    async def api_list_onboarding(request: web.Request) -> web.Response:
        user = _require_user(request)
        return _json_response({
            "onboarding_sessions": auth_store.list_onboarding_sessions(user.id)
        })

    async def api_create_onboarding(request: web.Request) -> web.Response:
        user = _require_user(request)
        if user.is_demo:
            return _json_response(
                {"error": "Charger onboarding is not available in the demo account."},
                status=403,
            )
        session = auth_store.create_onboarding_session(
            user.id, _ocpp_public_origin(request)
        )
        return _json_response({"onboarding_session": session}, status=201)

    async def api_delete_onboarding(request: web.Request) -> web.Response:
        user = _require_user(request)
        onboarding_id = str(request.match_info["id"])
        if not auth_store.delete_onboarding_session(user.id, onboarding_id):
            return _json_response({"error": "Onboarding session not found"}, status=404)
        return _json_response({"ok": True})

    # ── REST: admin framework ─────────────────────────────────────────────
    async def api_admin_user_search(request: web.Request) -> web.Response:
        _require_admin(request)
        email = str(request.rel_url.query.get("email", "")).strip()
        if not email:
            return _json_response({"error": "email is required"}, status=400)
        try:
            target = auth_store.find_user_by_email(email)
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=400)
        if target is None:
            return _json_response({"error": "Account not found"}, status=404)
        return _json_response({"user": target})

    async def api_admin_user_suggestions(request: web.Request) -> web.Response:
        _require_admin(request)
        query = str(request.rel_url.query.get("q", "")).strip()
        return _json_response({"users": auth_store.search_users(query)})

    async def api_admin_set_user_disabled(request: web.Request) -> web.Response:
        admin = _require_admin(request)
        target_id = str(request.match_info["id"])
        if target_id == admin.id:
            return _json_response({"error": "The currently logged-in account cannot be edited here"}, status=400)
        try:
            body = await request.json()
            disabled = bool(body["disabled"])
            target = auth_store.set_user_disabled(target_id, disabled)
        except KeyError:
            return _json_response({"error": "Expected {disabled: bool}"}, status=400)
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=400)
        coordinator.record_portal_action(
            "Admin Account State",
            f"{target['email']}: {'Disabled' if disabled else 'Enabled'}",
        )
        return _json_response({"user": target})

    async def api_admin_reset_user_2fa(request: web.Request) -> web.Response:
        admin = _require_admin(request)
        target_id = str(request.match_info["id"])
        if target_id == admin.id:
            return _json_response({"error": "The currently logged-in account cannot be edited here"}, status=400)
        try:
            target = auth_store.reset_user_totp(target_id)
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=400)
        coordinator.record_portal_action("Admin Reset 2FA", target["email"])
        return _json_response({"user": target})

    async def api_admin_verify_user_email(request: web.Request) -> web.Response:
        _require_admin(request)
        target_id = str(request.match_info["id"])
        try:
            target = auth_store.mark_user_email_verified(target_id)
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=400)
        coordinator.record_portal_action("Admin Verify Email", target["email"])
        return _json_response({"user": target})

    async def api_admin_delete_user(request: web.Request) -> web.Response:
        admin = _require_admin(request)
        target_id = str(request.match_info["id"])
        if target_id == admin.id:
            return _json_response({"error": "The currently logged-in account cannot be deleted"}, status=400)
        try:
            target_row = auth_store.find_user_by_id(target_id)
            if target_row and target_row.get("is_demo"):
                return _json_response({"error": "The demo account cannot be deleted"}, status=400)
            deleted = auth_store.delete_user(target_id)
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=404)
        coordinator.record_portal_action("Admin Delete User", deleted["email"])
        return _json_response({"ok": True, "deleted_email": deleted["email"]})

    async def api_admin_email_settings(request: web.Request) -> web.Response:
        _require_admin(request)
        return _json_response({"email_settings": _admin_email_settings_payload()})

    async def api_admin_update_email_settings(request: web.Request) -> web.Response:
        _require_admin(request)
        try:
            body = await request.json()
            if "registration_enabled" in body:
                reg_enabled = bool(body.get("registration_enabled"))
                auth_store.set_registration_enabled(reg_enabled)
                coordinator.record_portal_action(
                    "Admin Registration",
                    "Enabled" if reg_enabled else "Disabled",
                )
            if "initial_email_validation_enabled" in body:
                enabled = bool(body.get("initial_email_validation_enabled"))
                auth_store.set_email_verification_enabled(enabled)
                coordinator.record_portal_action(
                    "Admin Email Validation",
                    "Enabled" if enabled else "Disabled",
                )
            if "public_api_enabled" in body:
                enabled = bool(body.get("public_api_enabled"))
                auth_store.set_public_api_enabled(enabled)
                coordinator.record_portal_action(
                    "Admin Public API",
                    "Enabled" if enabled else "Disabled",
                )
        except Exception:
            _LOGGER.exception("Admin settings update failed")
            return _json_response({"error": "Could not update settings"}, status=500)
        return _json_response({"email_settings": _admin_email_settings_payload()})

    async def api_admin_test_smtp(request: web.Request) -> web.Response:
        admin = _require_admin(request)
        if not email_sender.configured:
            return _json_response({"error": "SMTP is not configured"}, status=503)
        try:
            email_sender.send_test_email(admin.email)
        except Exception as exc:
            _LOGGER.exception("SMTP test failed")
            return _json_response({"error": f"SMTP test failed: {exc}"}, status=502)
        coordinator.record_portal_action("Admin SMTP Test", admin.email)
        return _json_response({"ok": True, "message": f"Test email sent to {admin.email}"})

    async def api_admin_unadopted_chargers(request: web.Request) -> web.Response:
        _require_admin(request)
        serial = str(request.rel_url.query.get("serial", "")).strip()
        return _json_response({
            "chargers": _unadopted_chargers(auth_store, coordinator, serial_query=serial),
            "serial": serial,
        })

    async def api_admin_assign_unadopted_charger(request: web.Request) -> web.Response:
        admin = _require_admin(request)
        charge_point_id = str(request.match_info["charge_point_id"])
        try:
            body = await request.json()
            email = str(body.get("email", "")).strip()
            if not email:
                raise ValueError("email is required")
            target = auth_store.find_user_by_email(email)
            if target is None:
                return _json_response({"error": "Account not found"}, status=404)
            if target["disabled"]:
                return _json_response({"error": "Cannot assign a charger to a disabled account"}, status=400)
            current = _unadopted_chargers(auth_store, coordinator)
            current_charger = next((item for item in current if item["charge_point_id"] == charge_point_id), None)
            if current_charger is None:
                return _json_response({"error": "Charger is not connected or is already assigned"}, status=400)
            charger = auth_store.assign_charger_to_user(
                str(target["id"]),
                charge_point_id,
                display_name=body.get("display_name") or _charger_display_name(current_charger) or charge_point_id,
            )
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=400)
        if ocpp_server is not None:
            target_active = auth_store.get_active_charger(str(target["id"]))
            is_target_active = bool(
                target_active
                and str(target_active.get("charge_point_id") or "") == charge_point_id
            )
            await ocpp_server.promote_adopted_charge_point(
                charge_point_id,
                active=bool(is_target_active and str(target["id"]) == admin.id),
            )
        coordinator.record_portal_action(
            "Admin Assign Charger",
            f"{charge_point_id}: {charger['owner_email']}",
        )
        return _json_response({
            "charger": charger,
            "chargers": _unadopted_chargers(auth_store, coordinator),
        })

    async def api_admin_demo_settings(request: web.Request) -> web.Response:
        _require_admin(request)
        return _json_response({"demo_mode_enabled": auth_store.is_demo_mode_enabled()})

    async def api_admin_set_demo_mode(request: web.Request) -> web.Response:
        _require_admin(request)
        try:
            body = await request.json()
            enabled = bool(body.get("enabled", True))
            auth_store.set_demo_mode_enabled(enabled)
            coordinator.record_portal_action(
                "Admin Demo Mode",
                "enabled" if enabled else "disabled",
            )
            if demo_mode_callback is not None:
                await demo_mode_callback(enabled)
        except Exception:
            _LOGGER.exception("Demo mode toggle failed")
            return _json_response({"error": "Could not update demo mode"}, status=500)
        return _json_response({"demo_mode_enabled": auth_store.is_demo_mode_enabled()})

    async def api_admin_stats(request: web.Request) -> web.Response:
        _require_admin(request)
        stats = auth_store.get_admin_stats()
        # Augment with DST info from coordinator snapshots
        lister = getattr(auth_store, "list_all_adopted_charge_point_ids", None)
        all_ids: list[str] = lister() if lister else []
        pending_count = sum(
            1 for cpid in all_ids
            if coordinator._get_dst_correction_pending(cpid)
        )
        stats["dst_correction_pending"] = pending_count
        stats["dst_last_run"] = auth_store.get_system_setting("dst_last_run")
        return _json_response(stats)

    async def api_admin_server_details(request: web.Request) -> web.Response:
        _require_admin(request)
        portal_url = str(request.url.origin())
        ocpp_url = _ocpp_public_origin(request)
        # Parse host and port from the OCPP URL
        ocpp_parts = ocpp_url.rsplit(":", 1)
        ocpp_port = ocpp_parts[1] if len(ocpp_parts) == 2 else str(OCPP_PORT)
        firmware_host = coordinator.resolve_firmware_server_host(
            request_host=request.headers.get("X-Forwarded-Host") or request.host,
        ) or request.host.split(":")[0]
        firmware_port = coordinator.firmware_public_port
        firmware_base_url = f"ftp://{firmware_host}:{firmware_port}/"
        return _json_response({
            "portal_url": portal_url,
            "ocpp_url": ocpp_url,
            "ocpp_port": int(ocpp_port) if ocpp_port.isdigit() else OCPP_PORT,
            "firmware_host": firmware_host,
            "firmware_port": firmware_port,
            "firmware_base_url": firmware_base_url,
        })

    async def api_admin_force_repush_schedules(request: web.Request) -> web.Response:
        _require_admin(request)
        result = await coordinator.async_force_repush_all_schedules(auth_store=auth_store)
        return _json_response(result)

    async def api_admin_update_check(request: web.Request) -> web.Response:
        _require_admin(request)
        return _json_response({
            **_update_info,
            "current_version": APP_VERSION,
            "update_notifications_enabled": not _RUNNING_UNDER_HA,
            "channel": auth_store.get_update_channel(),
        })

    async def api_admin_orphaned_data_counts(request: web.Request) -> web.Response:
        _require_admin(request)
        return _json_response(auth_store.count_orphaned_data())

    async def api_admin_purge_orphaned_data(request: web.Request) -> web.Response:
        _require_admin(request)
        result = auth_store.purge_orphaned_data()
        return _json_response({"ok": True, **result})

    async def api_admin_set_update_channel(request: web.Request) -> web.Response:
        _require_admin(request)
        try:
            body = await request.json()
            channel = str(body.get("channel", "stable"))
        except Exception:
            return _json_response({"error": "Invalid request body"}, status=400)
        try:
            auth_store.set_update_channel(channel)
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=422)
        if not _RUNNING_UNDER_HA:
            asyncio.ensure_future(_run_update_check(auth_store))
        return _json_response({"ok": True, "channel": channel})

    # ── SSE: push state updates to the browser ─────────────────────────────
    async def api_events(request: web.Request) -> web.StreamResponse:
        user = _require_user(request)
        resp = web.StreamResponse()
        resp.headers["Content-Type"] = "text/event-stream"
        resp.headers["Cache-Control"] = "no-cache"
        resp.headers["X-Accel-Buffering"] = "no"
        await resp.prepare(request)

        q: asyncio.Queue = asyncio.Queue(maxsize=20)
        coordinator.add_sse_queue(q)

        try:
            # Send current state immediately on connect
            await resp.write(f"data: {json.dumps(_state_for_user(user))}\n\n".encode())

            while True:
                try:
                    await asyncio.wait_for(q.get(), timeout=25)
                    await resp.write(f"data: {json.dumps(_state_for_user(user))}\n\n".encode())
                except asyncio.TimeoutError:
                    # Keepalive comment so proxies don't close the connection
                    await resp.write(b": keepalive\n\n")
        except (ConnectionResetError, ConnectionError, asyncio.CancelledError):
            pass
        finally:
            coordinator.remove_sse_queue(q)

        return resp

    # ── REST: OCPP frame history ───────────────────────────────────────────
    async def api_ocpp_frames(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        frames = _active_state_for_request(request).ocpp_frame_history[-100:]
        return web.Response(
            content_type="application/json",
            text=json.dumps(frames, default=str),
        )

    # ── REST: firmware server status ───────────────────────────────────────
    async def api_firmware_status(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        return web.Response(
            content_type="application/json",
            text=json.dumps({
                "running": firmware.is_running,
                "root": str(firmware.root),
                "files": [f.name for f in firmware.root.glob("*.bin")] if firmware.root.exists() else [],
            }),
        )

    async def api_firmware_manifest(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        charge_point_id = _active_charge_point_id_for_request(request)
        try:
            await coordinator.async_refresh_firmware_manifest(charge_point_id=charge_point_id)
            payload = coordinator.firmware_catalog(charge_point_id=charge_point_id)
            return web.Response(content_type="application/json", text=json.dumps(payload))
        except RuntimeError as exc:
            payload = coordinator.firmware_catalog(charge_point_id=charge_point_id)
            payload["error"] = str(exc)
            return web.Response(status=502, content_type="application/json", text=json.dumps(payload))

    async def api_firmware_install(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        try:
            body = await request.json()
            filename = str(body["filename"])
        except Exception:
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "Expected {filename}"}))
        try:
            charge_point_id = _active_charge_point_id_for_request(request)
            result = await coordinator.async_install_firmware_file(filename, charge_point_id=charge_point_id)
            coordinator.record_portal_action("Install Firmware", filename, result, charge_point_id=charge_point_id)
            return web.Response(content_type="application/json", text=json.dumps(result))
        except RuntimeError as exc:
            coordinator.record_portal_action("Install Firmware", filename, str(exc), False, charge_point_id=_active_charge_point_id_for_request(request))
            return web.Response(status=503, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_firmware_cancel(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        charge_point_id = _active_charge_point_id_for_request(request)
        state = coordinator.state_for_charge_point(charge_point_id)
        progress = state.firmware_transfer_progress or {}
        filename = progress.get("filename") or state.firmware_update_target_file
        remote = progress.get("remote")
        cancelled_transfers = firmware.cancel_download(str(filename or ""), str(remote or ""))
        try:
            result = coordinator.cancel_firmware_update(charge_point_id=charge_point_id)
            result["cancelled_transfers"] = cancelled_transfers
            coordinator.record_portal_action("Cancel Firmware", str(filename or "Firmware transfer"), result, charge_point_id=charge_point_id)
            return web.Response(content_type="application/json", text=json.dumps(result))
        except RuntimeError as exc:
            coordinator.record_portal_action("Cancel Firmware", str(filename or "Firmware transfer"), str(exc), False, charge_point_id=charge_point_id)
            return web.Response(status=409, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    # ── REST: settings actions ─────────────────────────────────────────
    async def api_change_config(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        try:
            body = await request.json()
            key = str(body["key"])
            value = str(body["value"])
        except Exception:
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "Expected {key, value}"}))
        try:
            charge_point_id = _active_charge_point_id_for_request(request)
            result = await coordinator.async_change_configuration(key, value, charge_point_id=charge_point_id)
            coordinator.record_portal_action("Change Configuration", f"{key}={value}", result, charge_point_id=charge_point_id)
            return web.Response(content_type="application/json", text=json.dumps(result))
        except RuntimeError as exc:
            coordinator.record_portal_action("Change Configuration", f"{key}={value}", str(exc), False, charge_point_id=_active_charge_point_id_for_request(request))
            return web.Response(status=503, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_refresh_config(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        log = request.rel_url.query.get("log") == "1"
        charge_point_id = _active_charge_point_id_for_request(request)
        try:
            result = await coordinator.async_refresh_configuration(charge_point_id=charge_point_id)
            if log:
                coordinator.record_portal_action("Read Charger Configuration", "GetConfiguration", result, charge_point_id=charge_point_id)
            return web.Response(content_type="application/json", text=json.dumps({"ok": True}))
        except RuntimeError as exc:
            if log:
                coordinator.record_portal_action("Read Charger Configuration", "GetConfiguration", str(exc), False, charge_point_id=charge_point_id)
            return web.Response(status=503, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_set_mode(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        try:
            body = await request.json()
            mode = str(body["mode"])
        except Exception:
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "Expected {mode}"}))
        try:
            charge_point_id = _active_charge_point_id_for_request(request)
            result = await coordinator.async_set_charge_mode(mode, charge_point_id=charge_point_id)
            coordinator.record_portal_action("Change Charge Mode", mode, result, charge_point_id=charge_point_id)
            return web.Response(content_type="application/json", text=json.dumps(result))
        except (RuntimeError, ValueError) as exc:
            coordinator.record_portal_action("Change Charge Mode", mode, str(exc), False, charge_point_id=_active_charge_point_id_for_request(request))
            code = 400 if isinstance(exc, ValueError) else 503
            return web.Response(status=code, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_set_plug_and_go(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        try:
            body = await request.json()
            enabled = bool(body["enabled"])
        except Exception:
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "Expected {enabled: bool}"}))
        charge_point_id = _active_charge_point_id_for_request(request)
        await coordinator.async_set_plug_and_go(enabled, charge_point_id=charge_point_id)
        coordinator.record_portal_action("Set Plug and Go", "Enabled" if enabled else "Disabled", charge_point_id=charge_point_id)
        return web.Response(content_type="application/json",
                            text=json.dumps({"enabled": enabled}))

    async def api_set_max_energy(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        try:
            body = await request.json()
            kwh = float(body["kwh"])
        except Exception:
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "Expected {kwh: number}"}))
        charge_point_id = _active_charge_point_id_for_request(request)
        await coordinator.async_set_max_energy_per_session(kwh, charge_point_id=charge_point_id)
        state = coordinator.state_for_charge_point(charge_point_id)
        coordinator.record_portal_action("Set Max Energy Per Session", f"{state.max_energy_per_session_kwh:g} kWh", charge_point_id=charge_point_id)
        return web.Response(content_type="application/json",
                            text=json.dumps({"kwh": state.max_energy_per_session_kwh}))

    async def api_save_schedule(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        try:
            body = await request.json()
            schedule = body["schedule"]
        except Exception:
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "Expected {schedule}"}))
        try:
            charge_point_id = _active_charge_point_id_for_request(request)
            result = await coordinator.async_save_charging_schedule(schedule, charge_point_id=charge_point_id)
            ocpp_response = _pop_ocpp_response(result)
            coordinator.record_portal_action(
                "Save Schedule",
                _schedule_log_detail(result, "Saved"),
                ocpp_response,
                charge_point_id=charge_point_id,
            )
            return web.Response(content_type="application/json", text=json.dumps(result))
        except (RuntimeError, ValueError) as exc:
            coordinator.record_portal_action("Save Schedule", _schedule_log_detail(schedule, "Save failed"), str(exc), False, charge_point_id=_active_charge_point_id_for_request(request))
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_set_schedule_enabled(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        try:
            body = await request.json()
            schedule_id = str(body["id"])
            enabled = bool(body["enabled"])
        except Exception:
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "Expected {id, enabled}"}))
        try:
            charge_point_id = _active_charge_point_id_for_request(request)
            result = await coordinator.async_set_charging_schedule_enabled(schedule_id, enabled, charge_point_id=charge_point_id)
            ocpp_response = _pop_ocpp_response(result)
            coordinator.record_portal_action(
                "Change Active Schedule",
                _schedule_log_detail(result, "Enabled" if enabled else "Disabled"),
                ocpp_response,
                charge_point_id=charge_point_id,
            )
            return web.Response(content_type="application/json", text=json.dumps(result))
        except (RuntimeError, ValueError) as exc:
            coordinator.record_portal_action("Change Active Schedule", _schedule_log_detail({"id": schedule_id}, "Change failed"), str(exc), False, charge_point_id=_active_charge_point_id_for_request(request))
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_delete_schedule(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        schedule_id = str(request.match_info["id"])
        charge_point_id = _active_charge_point_id_for_request(request)
        schedule_detail = _schedule_log_detail(_find_schedule(_active_state_for_request(request), schedule_id), "Deleted")
        try:
            ocpp_response = await coordinator.async_delete_charging_schedule(schedule_id, charge_point_id=charge_point_id)
            coordinator.record_portal_action("Delete Schedule", schedule_detail, ocpp_response, charge_point_id=charge_point_id)
            return web.Response(content_type="application/json", text=json.dumps({"ok": True}))
        except (RuntimeError, ValueError) as exc:
            coordinator.record_portal_action("Delete Schedule", schedule_detail, str(exc), False, charge_point_id=charge_point_id)
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_save_rfid_tag(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        try:
            body = await request.json()
            tag = body["tag"]
        except Exception:
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "Expected {tag}"}))
        try:
            charge_point_id = _active_charge_point_id_for_request(request)
            result = await coordinator.async_save_rfid_tag(tag, charge_point_id=charge_point_id)
            ocpp_response = _pop_ocpp_response(result)
            coordinator.record_portal_action("Save ID Tag", _tag_log_detail(result.get("id_tag")), ocpp_response, charge_point_id=charge_point_id)
            return web.Response(content_type="application/json", text=json.dumps(result))
        except (RuntimeError, ValueError) as exc:
            coordinator.record_portal_action("Save ID Tag", _tag_log_detail(tag.get("id_tag")), str(exc), False, charge_point_id=_active_charge_point_id_for_request(request))
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_set_rfid_tag_enabled(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        try:
            body = await request.json()
            id_tag = str(body["id_tag"])
            enabled = bool(body["enabled"])
        except Exception:
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "Expected {id_tag, enabled}"}))
        try:
            charge_point_id = _active_charge_point_id_for_request(request)
            result = await coordinator.async_set_rfid_tag_enabled(id_tag, enabled, charge_point_id=charge_point_id)
            ocpp_response = _pop_ocpp_response(result)
            coordinator.record_portal_action(
                "Change ID Tag State",
                f"{_tag_log_detail(id_tag)}: {'Enabled' if enabled else 'Disabled'}",
                ocpp_response,
                charge_point_id=charge_point_id,
            )
            return web.Response(content_type="application/json", text=json.dumps(result))
        except (RuntimeError, ValueError) as exc:
            coordinator.record_portal_action("Change ID Tag State", _tag_log_detail(id_tag), str(exc), False, charge_point_id=_active_charge_point_id_for_request(request))
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_delete_rfid_tag(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        id_tag = str(request.match_info["id_tag"])
        charge_point_id = _active_charge_point_id_for_request(request)
        try:
            result = await coordinator.async_delete_rfid_tag(id_tag, charge_point_id=charge_point_id)
            coordinator.record_portal_action("Delete ID Tag", _tag_log_detail(id_tag), result, charge_point_id=charge_point_id)
            return web.Response(content_type="application/json", text=json.dumps({"ok": True}))
        except (RuntimeError, ValueError) as exc:
            coordinator.record_portal_action("Delete ID Tag", _tag_log_detail(id_tag), str(exc), False, charge_point_id=charge_point_id)
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_read_cp_voltage(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        charge_point_id = _active_charge_point_id_for_request(request)
        try:
            result = await coordinator.async_read_cp_voltage(charge_point_id=charge_point_id)
            coordinator.record_portal_action("Read CP Voltage", "DataTransfer GetCPVoltage", result, charge_point_id=charge_point_id)
            return web.Response(content_type="application/json", text=json.dumps(result))
        except RuntimeError as exc:
            coordinator.record_portal_action("Read CP Voltage", "DataTransfer GetCPVoltage", str(exc), False, charge_point_id=charge_point_id)
            return web.Response(status=503, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_trigger_meter_values(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        log = request.rel_url.query.get("log") == "1"
        charge_point_id = _active_charge_point_id_for_request(request)
        try:
            result = await coordinator.async_trigger_meter_values(charge_point_id=charge_point_id)
            if log:
                coordinator.record_portal_action("Trigger Meter Values", "TriggerMessage MeterValues", result, charge_point_id=charge_point_id)
            return web.Response(content_type="application/json", text=json.dumps(result))
        except RuntimeError as exc:
            if log:
                coordinator.record_portal_action("Trigger Meter Values", "TriggerMessage MeterValues", str(exc), False, charge_point_id=charge_point_id)
            return web.Response(status=503, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_unlock(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        charge_point_id = _active_charge_point_id_for_request(request)
        try:
            result = await coordinator.async_unlock_connector(charge_point_id=charge_point_id)
            coordinator.record_portal_action("Unlock Charging Port", "UnlockConnector", result, charge_point_id=charge_point_id)
            return web.Response(content_type="application/json", text=json.dumps(result))
        except RuntimeError as exc:
            coordinator.record_portal_action("Unlock Charging Port", "UnlockConnector", str(exc), False, charge_point_id=charge_point_id)
            return web.Response(status=503, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_toggle_charging(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        charge_point_id = _active_charge_point_id_for_request(request)
        state = coordinator.state_for_charge_point(charge_point_id)
        status = state.status
        has_open_transaction = coordinator.has_open_transaction(charge_point_id=charge_point_id)
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
                await coordinator.async_stop_charging(charge_point_id=charge_point_id)
                if stopping
                else await coordinator.async_start_charging(charge_point_id=charge_point_id)
            )
            coordinator.record_portal_action(action, detail, result, charge_point_id=charge_point_id)
            payload = {"action": action, **result}
            return web.Response(content_type="application/json", text=json.dumps(payload))
        except RuntimeError as exc:
            coordinator.record_portal_action(action, detail, str(exc), False, charge_point_id=charge_point_id)
            return web.Response(status=503, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_reset(request: web.Request) -> web.Response:
        error = _owned_active_charger_error(request)
        if error:
            return error
        try:
            body = await request.json()
            reset_type = str(body.get("type", "Soft"))
        except Exception:
            reset_type = "Soft"
        if reset_type not in ("Soft", "Hard"):
            return web.Response(status=400, content_type="application/json",
                                text=json.dumps({"error": "type must be Soft or Hard"}))
        charge_point_id = _active_charge_point_id_for_request(request)
        try:
            result = await coordinator.async_reset(reset_type, charge_point_id=charge_point_id)
            coordinator.record_portal_action(f"{reset_type} Reset", reset_type, result, charge_point_id=charge_point_id)
            return web.Response(content_type="application/json", text=json.dumps(result))
        except RuntimeError as exc:
            coordinator.record_portal_action(f"{reset_type} Reset", reset_type, str(exc), False, charge_point_id=charge_point_id)
            return web.Response(status=503, content_type="application/json",
                                text=json.dumps({"error": str(exc)}))

    async def api_clear_action_log(request: web.Request) -> web.Response:
        _require_admin(request)
        error = _owned_active_charger_error(request)
        if error:
            return error
        deleted = coordinator.clear_action_log(charge_point_id=_active_charge_point_id_for_request(request))
        return web.Response(content_type="application/json",
                            text=json.dumps({"ok": True, "deleted": deleted}))

    # API routes first, catch-all last
    app.router.add_get("/api/version", api_version)
    app.router.add_get("/api/auth/session", api_auth_session)
    app.router.add_post("/api/auth/register", api_auth_register)
    app.router.add_post("/api/auth/login", api_auth_login)
    app.router.add_post("/api/auth/verify-email", api_auth_verify_email)
    app.router.add_post("/api/auth/resend-email-otp", api_auth_resend_email_otp)
    app.router.add_post("/api/auth/forgot-password", api_auth_forgot_password)
    app.router.add_get("/api/auth/reset-password", api_auth_validate_reset_token)
    app.router.add_post("/api/auth/reset-password", api_auth_reset_password)
    app.router.add_post("/api/auth/logout", api_auth_logout)
    app.router.add_get("/api/account/security", api_account_security)
    app.router.add_post("/api/account/theme", api_account_theme)
    app.router.add_post("/api/account/password", api_account_password)
    app.router.add_post("/api/account/2fa/setup", api_account_2fa_setup)
    app.router.add_post("/api/account/2fa/enable", api_account_2fa_enable)
    app.router.add_post("/api/account/2fa/disable", api_account_2fa_disable)
    app.router.add_get("/api/account/api-keys", api_account_api_keys)
    app.router.add_post("/api/account/api-keys", api_account_create_api_key)
    app.router.add_delete("/api/account/api-keys/{id}", api_account_revoke_api_key)
    app.router.add_delete("/api/account/self", api_account_delete_self)
    app.router.add_get("/api/chargers", api_list_chargers)
    app.router.add_delete("/api/chargers/{id}", api_delete_charger)
    app.router.add_post("/api/chargers/{id}/switch", api_switch_charger)
    app.router.add_get("/api/onboarding", api_list_onboarding)
    app.router.add_post("/api/onboarding", api_create_onboarding)
    app.router.add_delete("/api/onboarding/{id}", api_delete_onboarding)
    app.router.add_get("/api/admin/users/search", api_admin_user_search)
    app.router.add_get("/api/admin/users/suggest", api_admin_user_suggestions)
    app.router.add_post("/api/admin/users/{id}/disabled", api_admin_set_user_disabled)
    app.router.add_post("/api/admin/users/{id}/reset-2fa", api_admin_reset_user_2fa)
    app.router.add_post("/api/admin/users/{id}/verify-email", api_admin_verify_user_email)
    app.router.add_delete("/api/admin/users/{id}", api_admin_delete_user)
    app.router.add_get("/api/admin/email-settings", api_admin_email_settings)
    app.router.add_post("/api/admin/email-settings", api_admin_update_email_settings)
    app.router.add_post("/api/admin/email-settings/test-smtp", api_admin_test_smtp)
    app.router.add_get("/api/admin/unadopted-chargers", api_admin_unadopted_chargers)
    app.router.add_post("/api/admin/unadopted-chargers/{charge_point_id}/assign", api_admin_assign_unadopted_charger)
    app.router.add_get("/api/admin/demo", api_admin_demo_settings)
    app.router.add_post("/api/admin/demo", api_admin_set_demo_mode)
    app.router.add_get("/api/admin/stats", api_admin_stats)
    app.router.add_get("/api/admin/server-details", api_admin_server_details)
    app.router.add_post("/api/admin/schedules/force-repush", api_admin_force_repush_schedules)
    app.router.add_get("/api/admin/update-check", api_admin_update_check)
    app.router.add_post("/api/admin/update-channel", api_admin_set_update_channel)
    app.router.add_get("/api/admin/orphaned-data-counts", api_admin_orphaned_data_counts)
    app.router.add_post("/api/admin/purge-orphaned-data", api_admin_purge_orphaned_data)
    app.router.add_get("/api/state", api_state)
    app.router.add_get("/api/energy", api_energy)
    app.router.add_get("/api/power", api_power)
    app.router.add_get("/api/events", api_events)
    app.router.add_get("/api/ocpp/frames", api_ocpp_frames)
    app.router.add_get("/api/firmware/status", api_firmware_status)
    app.router.add_get("/api/firmware/manifest", api_firmware_manifest)
    app.router.add_post("/api/firmware/install", api_firmware_install)
    app.router.add_post("/api/firmware/cancel", api_firmware_cancel)
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

    # ── User API (Bearer token) ───────────────────────────────────────────
    async def user_api_list_chargers(request: web.Request) -> web.Response:
        principal = _require_api_key(request, auth_store)
        page = max(1, int(request.rel_url.query.get("page", 1)))
        per_page = 15
        all_chargers = auth_store.list_chargers(principal["user_id"])
        enriched = _enrich_chargers_with_connection(all_chargers, coordinator)
        total = len(enriched)
        start = (page - 1) * per_page
        page_items = enriched[start:start + per_page]
        data = [_charger_to_api(c) for c in page_items]
        base = str(request.url.origin()) + "/api/v1/ev-charger"
        last_page = max(1, -(-total // per_page))
        return _json_response({
            "data": data,
            "links": {
                "first": f"{base}?page=1",
                "last": f"{base}?page={last_page}",
                "prev": f"{base}?page={page - 1}" if page > 1 else None,
                "next": f"{base}?page={page + 1}" if page < last_page else None,
            },
            "meta": {
                "current_page": page,
                "last_page": last_page,
                "per_page": per_page,
                "total": total,
            },
        })

    async def user_api_get_charger(request: web.Request) -> web.Response:
        principal = _require_api_key(request, auth_store)
        charge_point_id = request.match_info["uuid"]
        charger = auth_store.get_charger_for_user(principal["user_id"], charge_point_id)
        if charger is None:
            return _json_response({"message": "Charger not found."}, status=404)
        enriched = _enrich_chargers_with_connection([charger], coordinator)[0]
        return _json_response({"data": _charger_to_api(enriched)})

    # ------------------------------------------------------------------ #
    # Command catalogue — GivEnergy-compatible slugs                      #
    # ------------------------------------------------------------------ #

    _COMMAND_CATALOGUE = [
        {
            "id": "start-charge",
            "label": "Start Charging",
            "description": "Start a charging session",
            "requires_write": True,
            "parameters": [],
        },
        {
            "id": "stop-charge",
            "label": "Stop Charging",
            "description": "Stop an active charging session",
            "requires_write": True,
            "parameters": [],
        },
        {
            "id": "change-mode",
            "label": "Change Charge Mode",
            "description": "Change the charge mode",
            "requires_write": True,
            "parameters": [
                {
                    "name": "mode",
                    "type": "string",
                    "required": True,
                    "options": ["Eco", "SuperEco", "Boost", "ModbusSlave"],
                }
            ],
        },
        {
            "id": "adjust-charge-power-limit",
            "label": "Set Charge Current Limit",
            "description": "Set the maximum charge current in amps (6–32)",
            "requires_write": True,
            "parameters": [
                {
                    "name": "limit",
                    "type": "number",
                    "required": True,
                    "min": 6,
                    "max": 32,
                }
            ],
        },
        {
            "id": "set-session-energy-limit",
            "label": "Set Session Energy Limit",
            "description": "Set the maximum energy per session in kWh (0 = unlimited)",
            "requires_write": True,
            "parameters": [
                {
                    "name": "limit",
                    "type": "number",
                    "required": False,
                    "min": 0.1,
                    "max": 250,
                }
            ],
        },
        {
            "id": "set-plug-and-go",
            "label": "Set Plug and Go",
            "description": "Enable or disable automatic charging when a vehicle is plugged in",
            "requires_write": True,
            "parameters": [
                {
                    "name": "enabled",
                    "type": "boolean",
                    "required": True,
                }
            ],
        },
        {
            "id": "unlock-connector",
            "label": "Unlock Charging Port",
            "description": "Unlock the charging connector",
            "requires_write": True,
            "parameters": [],
        },
        {
            "id": "restart-charger",
            "label": "Restart Charger",
            "description": "Restart the charger (soft or hard reset)",
            "requires_write": True,
            "parameters": [
                {
                    "name": "hard_reset",
                    "type": "boolean",
                    "required": False,
                    "default": False,
                }
            ],
        },
        {
            "id": "perform-factory-reset",
            "label": "Factory Reset",
            "description": "Perform a hard factory reset of the charger",
            "requires_write": True,
            "parameters": [],
        },
        {
            "id": "read-cp-voltage-and-duty-cycle",
            "label": "Read CP Voltage and Duty Cycle",
            "description": "Read the control pilot voltage and duty cycle",
            "requires_write": True,
            "parameters": [],
        },
        {
            "id": "change-randomised-delay-duration",
            "label": "Set Randomised Delay Duration",
            "description": "Set the randomised delay duration in seconds (600–1800)",
            "requires_write": True,
            "parameters": [
                {
                    "name": "duration",
                    "type": "number",
                    "required": True,
                    "min": 600,
                    "max": 1800,
                }
            ],
        },
        {
            "id": "adjust-suspended-state-wait-timeout",
            "label": "Set Suspended State Wait Timeout",
            "description": "Set how long the charger waits in suspended state before stopping (seconds, 0 = disabled)",
            "requires_write": True,
            "parameters": [
                {
                    "name": "value",
                    "type": "number",
                    "required": False,
                    "min": 0,
                    "max": 43200,
                }
            ],
        },
        {
            "id": "enable-front-panel-led",
            "label": "Enable Front Panel LED",
            "description": "Enable or disable the front panel LED",
            "requires_write": True,
            "parameters": [
                {
                    "name": "value",
                    "type": "boolean",
                    "required": True,
                }
            ],
        },
        {
            "id": "enable-local-control",
            "label": "Enable Local Control",
            "description": "Enable or disable local Modbus control",
            "requires_write": True,
            "parameters": [
                {
                    "name": "value",
                    "type": "boolean",
                    "required": True,
                }
            ],
        },
        {
            "id": "set-max-import-capacity",
            "label": "Set Max Import Capacity",
            "description": "Set the DNO fuse / max import capacity in amps (40–100)",
            "requires_write": True,
            "parameters": [
                {
                    "name": "value",
                    "type": "number",
                    "required": False,
                    "min": 40,
                    "max": 100,
                }
            ],
        },
        {
            "id": "set-schedule",
            "label": "Set Schedule",
            "description": "Create or update a charging schedule",
            "requires_write": True,
            "parameters": [
                {"name": "schedule_id", "type": "string", "required": False},
                {"name": "name", "type": "string", "required": True, "max_length": 60},
                {
                    "name": "periods",
                    "type": "array",
                    "required": True,
                    "items": {
                        "start_time": "HH:MM string (required)",
                        "end_time": "HH:MM string (required)",
                        "day_of_week": "array of Mon/Tue/Wed/Thu/Fri/Sat/Sun (required)",
                        "current_a": "integer 6–32 (required)",
                    },
                },
            ],
        },
        {
            "id": "set-active-schedule",
            "label": "Set Active Schedule",
            "description": "Enable a schedule by ID, or disable all schedules if no ID given",
            "requires_write": True,
            "parameters": [
                {"name": "schedule_id", "type": "string", "required": False},
            ],
        },
        {
            "id": "delete-charging-profile",
            "label": "Delete Charging Profile",
            "description": "Delete a schedule by ID, or delete all schedules if no ID given",
            "requires_write": True,
            "parameters": [
                {"name": "schedule_id", "type": "string", "required": False},
            ],
        },
        {
            "id": "add-id-tags",
            "label": "Add ID Tags",
            "description": "Add or update one or more RFID authorisation tags",
            "requires_write": True,
            "parameters": [
                {
                    "name": "id_tags",
                    "type": "array",
                    "required": True,
                    "items": {
                        "id": "string (required)",
                        "alias": "string (optional)",
                        "expiry_date": "ISO8601 datetime string (optional)",
                    },
                }
            ],
        },
        {
            "id": "delete-id-tags",
            "label": "Delete ID Tags",
            "description": "Delete one or more RFID authorisation tags",
            "requires_write": True,
            "parameters": [
                {
                    "name": "id_tags",
                    "type": "array",
                    "required": True,
                    "items": "string (id_tag value)",
                }
            ],
        },
        {
            "id": "rename-id-tag",
            "label": "Rename ID Tag",
            "description": "Update the alias of an existing RFID tag",
            "requires_write": True,
            "parameters": [
                {
                    "name": "tag_id",
                    "type": "string",
                    "required": True,
                },
                {
                    "name": "alias",
                    "type": "string",
                    "required": True,
                },
            ],
        },
    ]

    def _api_charger_for_principal(user_id: str, charge_point_id: str) -> dict | None:
        charger = auth_store.get_charger_for_user(user_id, charge_point_id)
        if charger is None:
            return None
        return _enrich_chargers_with_connection([charger], coordinator)[0]

    async def user_api_list_commands(request: web.Request) -> web.Response:
        principal = _require_api_key(request, auth_store)
        charge_point_id = request.match_info["uuid"]
        if _api_charger_for_principal(principal["user_id"], charge_point_id) is None:
            return _json_response({"message": "Charger not found."}, status=404)
        return _json_response({"data": _COMMAND_CATALOGUE})

    async def user_api_get_command(request: web.Request) -> web.Response:
        principal = _require_api_key(request, auth_store)
        charge_point_id = request.match_info["uuid"]
        command_id = request.match_info["command_id"]
        if _api_charger_for_principal(principal["user_id"], charge_point_id) is None:
            return _json_response({"message": "Charger not found."}, status=404)
        command = next((c for c in _COMMAND_CATALOGUE if c["id"] == command_id), None)
        if command is None:
            return _json_response({"message": "Command not found."}, status=404)
        snapshot = coordinator.charger_snapshot_for(charge_point_id) or {}
        # Return current state in GivEnergy Cloud API-compatible shape
        if command_id == "change-mode":
            current = snapshot.get("charge_mode") or ""
            modes = ["SuperEco", "Eco", "Boost", "ModbusSlave"]
            return _json_response({"data": [{"mode": m, "active": m == current} for m in modes]})
        if command_id == "set-plug-and-go":
            return _json_response({"data": {"value": bool(snapshot.get("plug_and_go_enabled"))}})
        if command_id == "enable-front-panel-led":
            return _json_response({"data": {"value": bool(snapshot.get("front_panel_leds_enabled"))}})
        if command_id == "enable-local-control":
            return _json_response({"data": {"value": bool(snapshot.get("local_modbus_enabled"))}})
        if command_id == "set-session-energy-limit":
            return _json_response({"data": {"value": snapshot.get("max_energy_per_session_kwh")}})
        if command_id == "set-max-import-capacity":
            return _json_response({"data": {"value": snapshot.get("max_import_capacity_a")}})
        if command_id == "adjust-charge-power-limit":
            return _json_response({"data": {"value": snapshot.get("current_limit_a")}})
        return _json_response({"data": command})

    async def user_api_run_command(request: web.Request) -> web.Response:
        principal = _require_api_key(request, auth_store)
        if principal.get("scope") != "write":
            return _json_response({"message": "Write scope required."}, status=403)
        charge_point_id = request.match_info["uuid"]
        command_id = request.match_info["command_id"]
        charger = _api_charger_for_principal(principal["user_id"], charge_point_id)
        if charger is None:
            return _json_response({"message": "Charger not found."}, status=404)
        command = next((c for c in _COMMAND_CATALOGUE if c["id"] == command_id), None)
        if command is None:
            return _json_response({"message": "Command not found."}, status=404)

        _log_kwargs = {"charge_point_id": charge_point_id, "user": "API", "via": "API"}

        if not charger.get("connection_state") == "connected":
            coordinator.record_portal_action(
                command["label"], "Charger is not online", "Rejected", success=False, **_log_kwargs
            )
            return _json_response({"message": "Charger is not online."}, status=503)

        try:
            body = await request.json()
        except Exception:
            body = {}

        try:
            if command_id == "start-charge":
                result = await coordinator.async_start_charging(charge_point_id=charge_point_id)
                coordinator.record_portal_action("Start Charging", "RemoteStartTransaction", result, **_log_kwargs)
            elif command_id == "stop-charge":
                result = await coordinator.async_stop_charging(charge_point_id=charge_point_id)
                coordinator.record_portal_action("Stop Charging", "RemoteStopTransaction", result, **_log_kwargs)
            elif command_id == "change-mode":
                mode = str(body.get("mode", ""))
                if not mode:
                    return _json_response({"message": "Parameter 'mode' is required."}, status=422)
                result = await coordinator.async_set_charge_mode(mode, charge_point_id=charge_point_id)
                coordinator.record_portal_action("Change Charge Mode", mode, result, **_log_kwargs)
            elif command_id == "adjust-charge-power-limit":
                limit = body.get("limit")
                if limit is None:
                    return _json_response({"message": "Parameter 'limit' is required."}, status=422)
                limit = float(limit)
                if not (6 <= limit <= 32):
                    return _json_response({"message": "Parameter 'limit' must be between 6 and 32."}, status=422)
                state = coordinator.state_for_charge_point(charge_point_id)
                key = state.current_limit_key or "MaxCurrent"
                result = await coordinator.async_change_configuration(key, limit, charge_point_id=charge_point_id)
                coordinator.record_portal_action("Set Charge Current Limit", f"{limit:g}A", result, **_log_kwargs)
            elif command_id == "set-session-energy-limit":
                limit = body.get("limit")
                kwh = float(limit) if limit is not None else 0.0
                await coordinator.async_set_max_energy_per_session(kwh, charge_point_id=charge_point_id)
                state = coordinator.state_for_charge_point(charge_point_id)
                result = {"limit": state.max_energy_per_session_kwh}
                coordinator.record_portal_action("Set Session Energy Limit", f"{state.max_energy_per_session_kwh:g} kWh", **_log_kwargs)
            elif command_id == "set-plug-and-go":
                enabled = body.get("enabled")
                if enabled is None:
                    return _json_response({"message": "Parameter 'enabled' is required."}, status=422)
                await coordinator.async_set_plug_and_go(bool(enabled), charge_point_id=charge_point_id)
                state = coordinator.state_for_charge_point(charge_point_id)
                result = {"enabled": state.plug_and_go_enabled}
                coordinator.record_portal_action("Set Plug and Go", str(enabled), result, **_log_kwargs)
            elif command_id == "unlock-connector":
                result = await coordinator.async_unlock_connector(charge_point_id=charge_point_id)
                coordinator.record_portal_action("Unlock Charging Port", "UnlockConnector", result, **_log_kwargs)
            elif command_id == "restart-charger":
                hard_reset = bool(body.get("hard_reset", False))
                reset_type = "Hard" if hard_reset else "Soft"
                result = await coordinator.async_reset(reset_type, charge_point_id=charge_point_id)
                coordinator.record_portal_action("Restart Charger", reset_type, result, **_log_kwargs)
            elif command_id == "perform-factory-reset":
                result = await coordinator.async_reset("Hard", charge_point_id=charge_point_id)
                coordinator.record_portal_action("Factory Reset", "Hard", result, **_log_kwargs)
            elif command_id == "read-cp-voltage-and-duty-cycle":
                result = await coordinator.async_read_cp_voltage(charge_point_id=charge_point_id)
                coordinator.record_portal_action("Read CP Voltage and Duty Cycle", "TriggerMessage", result, **_log_kwargs)
            elif command_id == "change-randomised-delay-duration":
                duration = body.get("duration")
                if duration is None:
                    return _json_response({"message": "Parameter 'duration' is required."}, status=422)
                duration = int(duration)
                if not (600 <= duration <= 1800):
                    return _json_response({"message": "Parameter 'duration' must be between 600 and 1800."}, status=422)
                result = await coordinator.async_change_configuration("RandomisedDelayDuration", duration, charge_point_id=charge_point_id)
                coordinator.record_portal_action("Set Randomised Delay Duration", f"{duration}s", result, **_log_kwargs)
            elif command_id == "adjust-suspended-state-wait-timeout":
                value = body.get("value", 0)
                value = int(value)
                if not (0 <= value <= 43200):
                    return _json_response({"message": "Parameter 'value' must be between 0 and 43200."}, status=422)
                result = await coordinator.async_change_configuration("SuspevTime", value, charge_point_id=charge_point_id)
                coordinator.record_portal_action("Set Suspended State Wait Timeout", f"{value}s", result, **_log_kwargs)
            elif command_id == "enable-front-panel-led":
                value = body.get("value")
                if value is None:
                    return _json_response({"message": "Parameter 'value' is required."}, status=422)
                bool_val = str(value).lower() not in ("false", "0", "no")
                bool_str = "true" if bool_val else "false"
                result = await coordinator.async_change_configuration("FrontPanelLEDsEnabled", bool_str, charge_point_id=charge_point_id)
                coordinator.record_portal_action("Enable Front Panel LED", bool_str, result, **_log_kwargs)
            elif command_id == "enable-local-control":
                value = body.get("value")
                if value is None:
                    return _json_response({"message": "Parameter 'value' is required."}, status=422)
                bool_val = str(value).lower() not in ("false", "0", "no")
                bool_str = "true" if bool_val else "false"
                result = await coordinator.async_change_configuration("EnableLocalModbus", bool_str, charge_point_id=charge_point_id)
                coordinator.record_portal_action("Enable Local Control", bool_str, result, **_log_kwargs)
            elif command_id == "set-max-import-capacity":
                value = body.get("value")
                if value is None:
                    return _json_response({"message": "Parameter 'value' is required."}, status=422)
                value = int(value)
                if not (40 <= value <= 100):
                    return _json_response({"message": "Parameter 'value' must be between 40 and 100."}, status=422)
                result = await coordinator.async_change_configuration("Imax", value, charge_point_id=charge_point_id)
                coordinator.record_portal_action("Set Max Import Capacity", f"{value}A", result, **_log_kwargs)
            elif command_id == "add-id-tags":
                raw_tags = body.get("id_tags")
                if not isinstance(raw_tags, list) or not raw_tags:
                    return _json_response({"message": "Parameter 'id_tags' must be a non-empty array."}, status=422)
                saved = []
                for entry in raw_tags:
                    if not isinstance(entry, dict) or not entry.get("id"):
                        return _json_response({"message": "Each id_tags entry must have an 'id' field."}, status=422)
                    tag = {
                        "id_tag": entry["id"],
                        "alias": entry.get("alias") or None,
                        "expires_at": entry.get("expiry_date") or None,
                        "enabled": True,
                    }
                    saved.append(await coordinator.async_save_rfid_tag(tag, charge_point_id=charge_point_id))
                result = {"id_tags": saved}
                coordinator.record_portal_action("Add ID Tags", f"{len(saved)} tag(s)", result, **_log_kwargs)
            elif command_id == "delete-id-tags":
                raw_tags = body.get("id_tags")
                if not isinstance(raw_tags, list) or not raw_tags:
                    return _json_response({"message": "Parameter 'id_tags' must be a non-empty array."}, status=422)
                deleted = []
                for id_tag in raw_tags:
                    if not isinstance(id_tag, str) or not id_tag.strip():
                        return _json_response({"message": "Each id_tags entry must be a non-empty string."}, status=422)
                    await coordinator.async_delete_rfid_tag(id_tag.strip(), charge_point_id=charge_point_id)
                    deleted.append(id_tag.strip())
                result = {"deleted": deleted}
                coordinator.record_portal_action("Delete ID Tags", f"{len(deleted)} tag(s)", result, **_log_kwargs)
            elif command_id == "set-active-schedule":
                schedule_id = body.get("schedule_id")
                if schedule_id is None:
                    # No schedule_id — disable all schedules
                    state = coordinator.state_for_charge_point(charge_point_id)
                    result = {}
                    for sched in state.charging_schedule:
                        if sched.get("enabled"):
                            result = await coordinator.async_set_charging_schedule_enabled(
                                str(sched["id"]), False, charge_point_id=charge_point_id
                            )
                    coordinator.record_portal_action("Set Active Schedule", "Disabled all", result, **_log_kwargs)
                else:
                    result = await coordinator.async_set_charging_schedule_enabled(
                        str(schedule_id), True, charge_point_id=charge_point_id
                    )
                    coordinator.record_portal_action("Set Active Schedule", str(schedule_id), result, **_log_kwargs)
            elif command_id == "delete-charging-profile":
                schedule_id = body.get("schedule_id")
                if schedule_id is None:
                    # No schedule_id — delete all schedules
                    state = coordinator.state_for_charge_point(charge_point_id)
                    deleted_ids = [str(s["id"]) for s in state.charging_schedule if s.get("id") is not None]
                    for sid in deleted_ids:
                        await coordinator.async_delete_charging_schedule(sid, charge_point_id=charge_point_id)
                    result = {"deleted": deleted_ids}
                    coordinator.record_portal_action("Delete Charging Profile", f"{len(deleted_ids)} schedule(s)", result, **_log_kwargs)
                else:
                    result = await coordinator.async_delete_charging_schedule(str(schedule_id), charge_point_id=charge_point_id)
                    coordinator.record_portal_action("Delete Charging Profile", str(schedule_id), result, **_log_kwargs)
            elif command_id == "set-schedule":
                name = body.get("name")
                if not name:
                    return _json_response({"message": "Parameter 'name' is required."}, status=422)
                periods = body.get("periods")
                if not isinstance(periods, list) or not periods:
                    return _json_response({"message": "Parameter 'periods' must be a non-empty array."}, status=422)
                schedule_id = body.get("schedule_id")
                if len(periods) == 1:
                    period = periods[0]
                    schedule = {
                        "id": schedule_id,
                        "name": str(name)[:60],
                        "enabled": False,
                        "start": period.get("start_time", "00:00"),
                        "end": period.get("end_time", "01:00"),
                        "days": period.get("day_of_week", []),
                        "current_a": period.get("current_a", 32),
                    }
                    result = await coordinator.async_save_charging_schedule(schedule, charge_point_id=charge_point_id)
                    coordinator.record_portal_action("Set Schedule", name, result, **_log_kwargs)
                else:
                    # Multiple periods — create one schedule per period, inheriting the name with a suffix
                    saved = []
                    for i, period in enumerate(periods):
                        schedule = {
                            "id": None,
                            "name": f"{str(name)[:55]} {i + 1}" if len(periods) > 1 else str(name)[:60],
                            "enabled": False,
                            "start": period.get("start_time", "00:00"),
                            "end": period.get("end_time", "01:00"),
                            "days": period.get("day_of_week", []),
                            "current_a": period.get("current_a", 32),
                        }
                        saved.append(await coordinator.async_save_charging_schedule(schedule, charge_point_id=charge_point_id))
                    result = {"schedules": saved}
                    coordinator.record_portal_action("Set Schedule", f"{name} ({len(saved)} periods)", result, **_log_kwargs)
            elif command_id == "rename-id-tag":
                tag_id = body.get("tag_id")
                alias = body.get("alias")
                if not tag_id:
                    return _json_response({"message": "Parameter 'tag_id' is required."}, status=422)
                if alias is None:
                    return _json_response({"message": "Parameter 'alias' is required."}, status=422)
                state = coordinator.state_for_charge_point(charge_point_id)
                existing = next(
                    (t for t in state.rfid_tags if str(t.get("id_tag")) == str(tag_id)),
                    None,
                )
                if existing is None:
                    return _json_response({"message": "ID tag not found."}, status=404)
                tag = dict(existing)
                tag["alias"] = str(alias).strip() or None
                result = await coordinator.async_save_rfid_tag(tag, charge_point_id=charge_point_id)
                coordinator.record_portal_action("Rename ID Tag", f"{tag_id} → {alias}", result, **_log_kwargs)
            else:
                return _json_response({"message": "Command not implemented."}, status=501)
        except (RuntimeError, ValueError) as exc:
            coordinator.record_portal_action(command["label"], "Command failed", str(exc), success=False, **_log_kwargs)
            return _json_response({"message": str(exc)}, status=503)

        return _json_response({"data": {"command": command_id, "result": result}})

    async def user_api_list_charging_sessions(request: web.Request) -> web.Response:
        principal = _require_api_key(request, auth_store)
        charge_point_id = request.match_info["uuid"]
        page = max(1, int(request.rel_url.query.get("page", 1)))
        per_page = 15
        start_time = request.rel_url.query.get("start_time") or None
        end_time = request.rel_url.query.get("end_time") or None
        sessions, total = auth_store.list_charging_sessions(
            principal["user_id"], charge_point_id,
            start_time=start_time, end_time=end_time,
            page=page, per_page=per_page,
        )
        if total == 0 and auth_store.get_charger_for_user(principal["user_id"], charge_point_id) is None:
            return _json_response({"message": "Charger not found."}, status=404)
        base = str(request.url.origin()) + f"/api/v1/ev-charger/{charge_point_id}/charging-sessions"
        last_page = max(1, -(-total // per_page))
        return _json_response({
            "data": sessions,
            "links": {
                "first": f"{base}?page=1",
                "last": f"{base}?page={last_page}",
                "prev": f"{base}?page={page - 1}" if page > 1 else None,
                "next": f"{base}?page={page + 1}" if page < last_page else None,
            },
            "meta": {
                "current_page": page,
                "last_page": last_page,
                "per_page": per_page,
                "total": total,
            },
        })

    async def user_api_list_meter_data(request: web.Request) -> web.Response:
        # Numeric measurand IDs used by GivEnergy Cloud API → OCPP measurand strings
        _MEASURAND_ID_MAP = {
            "4": "Energy.Active.Import.Register",
            "1": "Power.Active.Import",
            "2": "Current.Import",
            "3": "Voltage",
        }
        principal = _require_api_key(request, auth_store)
        charge_point_id = request.match_info["uuid"]
        page = max(1, int(request.rel_url.query.get("page", 1)))
        per_page = 15
        start_time = request.rel_url.query.get("start_time") or None
        end_time = request.rel_url.query.get("end_time") or None
        # Support both ?measurands=X,Y and ?measurands[]=X&measurands[]=Y
        measurands_list = request.rel_url.query.getall("measurands[]", [])
        if not measurands_list:
            measurands_param = request.rel_url.query.get("measurands") or None
            measurands_list = [m.strip() for m in measurands_param.split(",")] if measurands_param else []
        # Map numeric IDs to measurand strings
        measurands = [_MEASURAND_ID_MAP.get(m, m) for m in measurands_list] if measurands_list else None
        meter_id_param = request.rel_url.query.get("meter_id") or request.rel_url.query.get("meter_ids[]")
        meter_id = int(meter_id_param) if meter_id_param is not None and str(meter_id_param).isdigit() else None
        readings, total = auth_store.list_meter_readings(
            principal["user_id"], charge_point_id,
            start_time=start_time, end_time=end_time,
            measurands=measurands, group_index=meter_id,
            page=page, per_page=per_page,
        )
        if total == 0 and auth_store.get_charger_for_user(principal["user_id"], charge_point_id) is None:
            return _json_response({"message": "Charger not found."}, status=404)
        # Group flat rows by timestamp+meter_id into GivEnergy-compatible shape:
        # {"start_time": ..., "end_time": ..., "measurements": [{"measurand": ..., "value": ...}]}
        from collections import OrderedDict
        grouped: OrderedDict = OrderedDict()
        for row in readings:
            key = (row.get("timestamp"), row.get("meter_id"))
            if key not in grouped:
                grouped[key] = {"start_time": row.get("timestamp"), "end_time": row.get("timestamp"), "measurements": []}
            grouped[key]["measurements"].append({
                "measurand": row.get("measurand"),
                "phase": row.get("phase"),
                "unit": row.get("unit"),
                "value": row.get("normalized_value"),
            })
        data = list(grouped.values())
        base = str(request.url.origin()) + f"/api/v1/ev-charger/{charge_point_id}/meter-data"
        last_page = max(1, -(-total // per_page))
        return _json_response({
            "data": data,
            "links": {
                "first": f"{base}?page=1",
                "last": f"{base}?page={last_page}",
                "prev": f"{base}?page={page - 1}" if page > 1 else None,
                "next": f"{base}?page={page + 1}" if page < last_page else None,
            },
            "meta": {
                "current_page": page,
                "last_page": last_page,
                "per_page": per_page,
                "total": total,
            },
        })

    async def user_api_list_schedules(request: web.Request) -> web.Response:
        principal = _require_api_key(request, auth_store)
        charge_point_id = request.match_info["uuid"]
        if _api_charger_for_principal(principal["user_id"], charge_point_id) is None:
            return _json_response({"message": "Charger not found."}, status=404)
        state = coordinator.state_for_charge_point(charge_point_id)
        schedules = [
            {
                "id": s.get("id"),
                "name": s.get("name"),
                "enabled": s.get("enabled", False),
                "start": s.get("start"),
                "end": s.get("end"),
                "days": s.get("days", []),
                "current_a": s.get("current_a"),
            }
            for s in (state.charging_schedule or [])
        ]
        return _json_response({"data": schedules})

    app.router.add_get("/api/v1/ev-charger", user_api_list_chargers)
    app.router.add_get("/api/v1/ev-charger/{uuid}", user_api_get_charger)
    app.router.add_get("/api/v1/ev-charger/{uuid}/commands", user_api_list_commands)
    app.router.add_get("/api/v1/ev-charger/{uuid}/commands/{command_id}", user_api_get_command)
    app.router.add_post("/api/v1/ev-charger/{uuid}/commands/{command_id}", user_api_run_command)
    app.router.add_get("/api/v1/ev-charger/{uuid}/charging-sessions", user_api_list_charging_sessions)
    app.router.add_get("/api/v1/ev-charger/{uuid}/meter-data", user_api_list_meter_data)
    app.router.add_get("/api/v1/ev-charger/{uuid}/schedules", user_api_list_schedules)

    async def user_api_list_id_tags(request: web.Request) -> web.Response:
        principal = _require_api_key(request, auth_store)
        charge_point_id = request.match_info["uuid"]
        if _api_charger_for_principal(principal["user_id"], charge_point_id) is None:
            return _json_response({"message": "Charger not found."}, status=404)
        state = coordinator.state_for_charge_point(charge_point_id)
        tags = [
            {
                "id": t.get("id_tag"),
                "alias": t.get("alias"),
                "expires_at": t.get("expires_at"),
                "enabled": t.get("enabled", True),
            }
            for t in (state.rfid_tags or [])
        ]
        return _json_response({"data": tags})

    app.router.add_get("/api/v1/ev-charger/{uuid}/id-tags", user_api_list_id_tags)

    async def api_v1_openapi_yaml(request: web.Request) -> web.Response:
        path = Path(__file__).parent / "templates" / "openapi.yaml"
        return web.Response(text=path.read_text(), content_type="application/yaml")

    async def api_v1_docs(request: web.Request) -> web.Response:
        path = Path(__file__).parent / "templates" / "api-docs.html"
        return web.Response(text=path.read_text(), content_type="text/html")

    app.router.add_get("/api/v1/openapi.yaml", api_v1_openapi_yaml)
    app.router.add_get("/api/v1/docs", api_v1_docs)

    async def _api_not_found(request: web.Request) -> web.Response:
        return _json_response({"message": "Not found."}, status=404)

    app.router.add_route("*", "/api/{path:.*}", _api_not_found)

    app.router.add_get("/", index)
    app.router.add_get("/{path:.*}", index)

    return app


def _json_response(payload: dict, status: int = 200) -> web.Response:
    return web.Response(
        status=status,
        content_type="application/json",
        text=json.dumps(payload, default=str),
    )


def _require_api_key(request: web.Request, auth_store: AuthStore) -> dict:
    auth = request.headers.get("Authorization", "")
    if not auth.lower().startswith("bearer "):
        raise web.HTTPUnauthorized(
            content_type="application/json",
            text=json.dumps({"message": "Unauthenticated."}),
        )
    token = auth[7:].strip()
    principal = auth_store.validate_api_key(token)
    if principal is None:
        raise web.HTTPUnauthorized(
            content_type="application/json",
            text=json.dumps({"message": "Unauthenticated."}),
        )
    return principal


def _charger_to_api(charger: dict) -> dict:
    online = str(charger.get("connection_state") or "").lower() == "connected"
    power_kw = charger.get("live_power_kw")
    return {
        "uuid": charger.get("charge_point_id"),
        "serial_number": charger.get("serial"),
        "type": charger.get("manufacturer"),
        "alias": charger.get("display_name"),
        "online": online,
        "went_offline_at": None if online else charger.get("went_offline_at"),
        "status": charger.get("status"),
        "power_now": {"value": power_kw if power_kw is not None else 0},
    }


def _require_user(request: web.Request) -> AuthUser:
    user = request.get("user")
    if user is None:
        raise web.HTTPUnauthorized(text="Authentication required")
    return user


def _require_admin(request: web.Request) -> AuthUser:
    user = _require_user(request)
    if user.role != ROLE_ADMIN:
        raise web.HTTPForbidden(text="Admin access required")
    return user


def _user_payload(user: AuthUser) -> dict[str, object]:
    return {
        "id": user.id,
        "email": user.email,
        "display_name": user.display_name,
        "role": user.role,
        "totp_enabled": user.totp_enabled,
        "disabled": bool(user.disabled_at),
        "theme_preference": user.theme_preference,
        "email_verified": bool(user.email_verified_at),
        "is_demo": user.is_demo,
    }


def _totp_qr_svg(provisioning_uri: str) -> str:
    try:
        import qrcode
        import qrcode.image.svg
    except ImportError as exc:
        _LOGGER.warning("qrcode package is unavailable; TOTP QR code disabled: %s", exc)
        return ""

    qr = qrcode.make(
        provisioning_uri,
        image_factory=qrcode.image.svg.SvgPathImage,
        box_size=8,
        border=2,
    )
    buffer = BytesIO()
    qr.save(buffer)
    return buffer.getvalue().decode("utf-8")


def _set_session_cookie(
    request: web.Request,
    response: web.Response,
    session_id: str,
    expires_at,
) -> None:
    del expires_at
    response.set_cookie(
        SESSION_COOKIE,
        session_id,
        max_age=14 * 24 * 60 * 60,
        httponly=True,
        secure=request.secure,
        samesite="Strict",
        path="/",
    )


def _ocpp_public_origin(request: web.Request) -> str:
    if PUBLIC_OCPP_BASE_URL:
        return PUBLIC_OCPP_BASE_URL.rstrip("/")
    # Derive from the incoming web request — honour X-Forwarded-Proto/Host from Caddy
    forwarded_proto = request.headers.get("X-Forwarded-Proto", "").strip()
    scheme = "wss" if (forwarded_proto == "https" or request.scheme == "https") else "ws"
    raw_host = (
        request.headers.get("X-Forwarded-Host")
        or request.headers.get("Host")
        or request.host
        or ""
    )
    host = str(raw_host).split(",", 1)[0].strip().split(":", 1)[0]
    return f"{scheme}://{host}:{OCPP_PORT}"


def _enrich_chargers_with_connection(
    chargers: list[dict],
    coordinator: OcppCoordinator,
) -> list[dict]:
    connected = {
        str(item.get("charge_point_id") or ""): item
        for item in coordinator.connected_charge_points()
        if item.get("charge_point_id")
    }
    enriched: list[dict] = []
    for charger in chargers:
        item = dict(charger)
        charge_point_id = str(item.get("charge_point_id") or "")
        snapshot = coordinator.charger_snapshot_for(charge_point_id)
        if snapshot:
            item.update({
                "manufacturer": snapshot.get("manufacturer"),
                "model": snapshot.get("model"),
                "serial": snapshot.get("charge_point_serial_number") or snapshot.get("charge_box_serial_number"),
                "firmware": snapshot.get("firmware_version"),
                "remote_address": snapshot.get("websocket_remote_address") or snapshot.get("local_ip_address"),
                "connection_state": snapshot.get("connection_state"),
                "status": snapshot.get("status"),
                "last_seen": snapshot.get("last_seen"),
                "live_power_kw": snapshot.get("live_power_kw"),
            })
        connection = connected.get(charge_point_id)
        if connection:
            item.update({
                "manufacturer": connection.get("manufacturer") or item.get("manufacturer"),
                "model": connection.get("model") or item.get("model"),
                "serial": connection.get("serial") or item.get("serial"),
                "firmware": connection.get("firmware") or item.get("firmware"),
                "remote_address": connection.get("remote_address") or item.get("remote_address"),
                "connection_state": connection.get("connection_state"),
                "status": connection.get("status") or item.get("status"),
                "last_seen": connection.get("last_seen"),
            })
        item["display_label"] = _charger_display_name(item)
        item["detail_label"] = _charger_detail_label(item)
        enriched.append(item)
    return enriched


def _charger_display_name(charger: dict) -> str:
    charge_point_id = str(charger.get("charge_point_id") or "").strip()
    display_name = str(charger.get("display_name") or "").strip()
    serial = str(charger.get("serial") or "").strip()
    manufacturer = str(charger.get("manufacturer") or "").strip()
    model = str(charger.get("model") or "").strip()
    if serial:
        return serial
    if manufacturer or model:
        return " ".join(part for part in (manufacturer, model) if part)
    if display_name and display_name != charge_point_id:
        return display_name
    return charge_point_id or "EV Charger"


def _charger_detail_label(charger: dict) -> str:
    parts = [
        charger.get("manufacturer"),
        charger.get("model"),
        charger.get("serial"),
    ]
    return " · ".join(str(part) for part in parts if part) or str(
        charger.get("display_name") or charger.get("charge_point_id") or "Pending charge point identity"
    )


def _unadopted_chargers(
    auth_store: AuthStore,
    coordinator: OcppCoordinator,
    *,
    serial_query: str = "",
    limit: int = 50,
) -> list[dict[str, object]]:
    query = serial_query.lower()
    chargers: list[dict[str, object]] = []
    for charger in coordinator.connected_charge_points():
        charge_point_id = str(charger.get("charge_point_id") or "").strip()
        if not charge_point_id:
            continue
        if auth_store.get_charger_by_charge_point_id(charge_point_id):
            continue
        searchable = " ".join(
            str(charger.get(key) or "")
            for key in ("serial", "charge_point_id", "manufacturer", "model", "remote_address")
        ).lower()
        if query and query not in searchable:
            continue
        chargers.append({
            "charge_point_id": charge_point_id,
            "manufacturer": charger.get("manufacturer"),
            "model": charger.get("model"),
            "serial": charger.get("serial"),
            "firmware": charger.get("firmware"),
            "remote_address": charger.get("remote_address"),
            "connection_state": charger.get("connection_state"),
            "last_seen": charger.get("last_seen"),
        })
        if len(chargers) >= limit:
            break
    return chargers


def _find_schedule(state: ChargerState, schedule_id: str) -> dict | None:
    for schedule in state.charging_schedule:
        if str(schedule.get("id")) == str(schedule_id):
            return schedule
    return {"id": schedule_id}


def _schedule_log_detail(schedule: dict | None, action: str) -> str:
    schedule = schedule or {}
    label = str(schedule.get("name") or schedule.get("id") or "Unknown schedule")
    return f"{label}: {action}"


def _pop_ocpp_response(payload: dict | None) -> object:
    if not isinstance(payload, dict):
        return "Success"
    return payload.pop("_ocpp_response", "Success")


def _tag_log_detail(id_tag: object) -> str:
    value = str(id_tag or "").strip() or "Unknown"
    return f"Tag ID: {value}"


# ── DST correction scheduler ───────────────────────────────────────────────────

def _next_uk_dst_transition(after: datetime) -> datetime:
    """Return the next UK DST transition after `after` (UTC): last Sun of March or October at 01:01 UTC."""
    from datetime import date as _date
    def last_sunday(year: int, month: int) -> _date:
        # Find last Sunday of the month
        d = _date(year, month, 31 if month in (1,3,5,7,8,10,12) else 30 if month in (4,6,9,11) else 28)
        return d - timedelta(days=d.weekday() + 1) if d.weekday() != 6 else d

    candidates = []
    for year in (after.year, after.year + 1):
        for month in (3, 10):
            d = last_sunday(year, month)
            t = datetime(d.year, d.month, d.day, 1, 1, 0, tzinfo=UTC)
            if t > after:
                candidates.append(t)
    return min(candidates)


async def _dst_correction_task(coordinator: object, auth_store: object) -> None:
    import logging
    logger = logging.getLogger(__name__)
    while True:
        next_transition = _next_uk_dst_transition(datetime.now(UTC))
        wait_seconds = (next_transition - datetime.now(UTC)).total_seconds()
        logger.info("DST correction: next run at %s (in %.0f hours)", next_transition.isoformat(), wait_seconds / 3600)
        await asyncio.sleep(wait_seconds)
        logger.info("DST correction: running now")
        try:
            await coordinator.async_dst_correction(auth_store=auth_store)
        except Exception as exc:
            logger.error("DST correction task failed: %s", exc)


# ── Update checker ────────────────────────────────────────────────────────────

_RUNNING_UNDER_HA = bool(os.environ.get("SUPERVISOR_TOKEN"))
_GITHUB_RELEASES_URL = "https://api.github.com/repos/DJBenson/GivEVC-OCPPv2/releases"
_UPDATE_CHECK_INTERVAL = 6 * 3600  # 6 hours

# In-memory cache — populated by the background task
_update_info: dict = {}


def _parse_version(v: str) -> tuple[int, ...]:
    import re
    return tuple(int(x) for x in re.findall(r"\d+", v))


async def _run_update_check(auth_store: AuthStore) -> None:
    global _update_info
    try:
        channel = auth_store.get_update_channel()
        import aiohttp as _aiohttp
        async with _aiohttp.ClientSession() as session:
            async with session.get(
                _GITHUB_RELEASES_URL,
                headers={"Accept": "application/vnd.github+json"},
                timeout=_aiohttp.ClientTimeout(total=10),
            ) as resp:
                releases = await resp.json()
        candidates = [
            r for r in releases
            if not r.get("draft")
            and (channel == "beta" or not r.get("prerelease"))
        ]
        if candidates:
            latest = candidates[0]
            tag = str(latest.get("tag_name", "")).lstrip("v")
            current = APP_VERSION.lstrip("v")
            is_newer = _parse_version(tag) > _parse_version(current)
            _update_info = {
                "latest_version": tag,
                "is_prerelease": latest.get("prerelease", False),
                "update_available": is_newer,
                "release_url": latest.get("html_url", ""),
                "channel": channel,
                "checked_at": datetime.now(UTC).isoformat(),
            }
        else:
            _update_info = {"update_available": False, "checked_at": datetime.now(UTC).isoformat()}
    except Exception as exc:
        _LOGGER.debug("Update check failed: %s", exc)
        _update_info = {"update_available": False, "error": str(exc), "checked_at": datetime.now(UTC).isoformat()}


async def _update_check_task(auth_store: AuthStore) -> None:
    while True:
        await _run_update_check(auth_store)
        await asyncio.sleep(_UPDATE_CHECK_INTERVAL)


# ── Main ───────────────────────────────────────────────────────────────────────

async def main() -> None:
    _LOGGER.info(
        "Starting GivEVC OCPPv2 — OCPP:%s  firmware:%s  web:%s",
        OCPP_PORT, FIRMWARE_PORT, INGRESS_PORT,
    )

    auth_store = AuthStore(AUTH_DB_PATH)
    auth_store.seed_startup_data()
    demo_mode_enabled = auth_store.is_demo_mode_enabled()
    _LOGGER.info(
        "Demo mode %s%s",
        "enabled" if demo_mode_enabled else "disabled",
        f" — demo account: {DEMO_EMAIL} / {DEMO_PASSWORD}" if demo_mode_enabled else "",
    )

    coordinator = OcppCoordinator(
        listen_port=OCPP_PORT,
        state_path=LEGACY_STATE_PATH,
        state_store=auth_store,
        firmware_directory=FIRMWARE_ROOT,
        firmware_server_port=FIRMWARE_PORT,
        firmware_public_host=PUBLIC_FIRMWARE_HOST,
        firmware_public_port=PUBLIC_FIRMWARE_PORT,
        firmware_manifest_url=FIRMWARE_MANIFEST_URL,
        debug_logging=DEBUG,
    )
    coordinator.load()

    firmware = FirmwareTransferServer(root=FIRMWARE_ROOT)
    loop = asyncio.get_running_loop()

    def _firmware_event(event: dict) -> None:
        _noisy = {"control_frame_sent", "control_frame_received", "chunk_sent"}
        _log = _LOGGER.debug if event.get("event") in _noisy else _LOGGER.info
        _log("Firmware event: %s", event.get("event"))
        try:
            loop.call_soon_threadsafe(coordinator.record_firmware_transfer_event, event)
        except Exception:
            _LOGGER.exception("Failed to dispatch firmware event to event loop")

    firmware.set_event_callback(_firmware_event)

    ocpp_scheme = "wss" if os.environ.get("OCPP_TLS", "").lower() in ("1", "true") else "ws"
    demo_upstream = f"{ocpp_scheme}://127.0.0.1:{OCPP_PORT}"
    demo_simulator = None
    demo_task = None

    async def apply_demo_runtime(enabled: bool) -> None:
        nonlocal demo_simulator, demo_task
        if enabled:
            if demo_task is not None and not demo_task.done():
                return
            demo_simulator = DemoChargerSimulator(demo_upstream, DEMO_PASSWORD)
            demo_task = asyncio.create_task(demo_simulator.run_forever(), name="demo-simulator")
            _LOGGER.info("Demo simulator started → %s/%s", demo_upstream, DEMO_CHARGE_POINT_ID)
            return

        if demo_simulator is not None:
            await demo_simulator.stop()
        if demo_task is not None:
            demo_task.cancel()
            try:
                await demo_task
            except (asyncio.CancelledError, Exception):
                pass
        demo_simulator = None
        demo_task = None

    ocpp_server = OcppServer(coordinator, auth_store=auth_store)
    web_app = build_web_app(coordinator, firmware, ocpp_server, auth_store, apply_demo_runtime)

    await ocpp_server.start()
    await firmware.start(FIRMWARE_PORT)

    runner = web.AppRunner(web_app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, host="0.0.0.0", port=INGRESS_PORT).start()
    _LOGGER.info("Web UI on 0.0.0.0:%s", INGRESS_PORT)

    if demo_mode_enabled:
        await apply_demo_runtime(True)

    dst_task = asyncio.create_task(_dst_correction_task(coordinator, auth_store), name="dst-correction")
    update_task = None
    if not _RUNNING_UNDER_HA:
        update_task = asyncio.create_task(_update_check_task(auth_store), name="update-check")

    try:
        await asyncio.Event().wait()
    finally:
        dst_task.cancel()
        try:
            await dst_task
        except asyncio.CancelledError:
            pass
        if update_task is not None:
            update_task.cancel()
            try:
                await update_task
            except asyncio.CancelledError:
                pass
        await apply_demo_runtime(False)
        await ocpp_server.stop()
        await firmware.stop()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
