"""SQLite-backed user, session, and onboarding storage."""

from __future__ import annotations

import base64
import hashlib
import re
import hmac
import json
import os
import random
import secrets
import sqlite3
import struct
import time
from dataclasses import dataclass
from calendar import monthrange
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from cryptography.fernet import Fernet
from urllib.parse import quote
from uuid import uuid4

PASSWORD_ITERATIONS = 260_000
SESSION_DAYS = 14
ONBOARDING_MINUTES = 30
EMAIL_OTP_MINUTES = 10
EMAIL_OTP_RESEND_SECONDS = 30
SCHEMA_VERSION = 18
DEMO_EMAIL = "demo.user@givevcdemo.local"
DEMO_PASSWORD = "p@ssw0rd123"
DEMO_CHARGE_POINT_ID = "demo-charger-001"
DEMO_DISPLAY_NAME = "Demo Charger"
SYSTEM_SETTING_DEMO_MODE = "demo_mode_enabled"
SYSTEM_SETTING_PUBLIC_API = "public_api_enabled"
PASSWORD_RESET_MINUTES = 15
COORDINATOR_STATE_ID = "global"
TOTP_ISSUER = "GivEVC Portal"
ROLE_USER = "User"
ROLE_ADMIN = "Admin"


@dataclass(frozen=True)
class AuthUser:
    id: str
    email: str
    display_name: str | None
    role: str = ROLE_USER
    totp_enabled: bool = False
    disabled_at: str | None = None
    theme_preference: str | None = None
    email_verified_at: str | None = None
    is_demo: bool = False


class AuthStore:
    """Small synchronous SQLite store for portal auth framework."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._secret_key = _load_secret_key(path)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute("PRAGMA foreign_keys = OFF")
            onboarding_columns = _table_columns(conn, "onboarding_sessions")
            self._create_schema(conn, include_onboarding=not onboarding_columns)
            if onboarding_columns and {
                "endpoint_path",
                "endpoint_url",
                "password_preview",
            }.intersection(onboarding_columns):
                self._migrate_insecure_onboarding_sessions(conn)

            if int(conn.execute("PRAGMA user_version").fetchone()[0] or 0) < 1:
                self._migrate_plaintext_session_tokens(conn)
                conn.execute("PRAGMA user_version = 1")
            if int(conn.execute("PRAGMA user_version").fetchone()[0] or 0) < 2:
                self._migrate_totp_columns(conn)
                conn.execute("PRAGMA user_version = 2")
            if int(conn.execute("PRAGMA user_version").fetchone()[0] or 0) < 3:
                self._migrate_role_column(conn)
                conn.execute("PRAGMA user_version = 3")
            if int(conn.execute("PRAGMA user_version").fetchone()[0] or 0) < 4:
                self._migrate_disabled_column(conn)
                conn.execute("PRAGMA user_version = 4")
            if int(conn.execute("PRAGMA user_version").fetchone()[0] or 0) < 5:
                self._migrate_active_charger_column(conn)
                conn.execute("PRAGMA user_version = 5")
            if int(conn.execute("PRAGMA user_version").fetchone()[0] or 0) < 6:
                self._migrate_api_keys_table(conn)
                conn.execute("PRAGMA user_version = 6")
            if int(conn.execute("PRAGMA user_version").fetchone()[0] or 0) < 7:
                self._migrate_api_key_expiry_nullable(conn)
                conn.execute("PRAGMA user_version = 7")
            if int(conn.execute("PRAGMA user_version").fetchone()[0] or 0) < 8:
                self._migrate_coordinator_state_table(conn)
                conn.execute("PRAGMA user_version = 8")
            if int(conn.execute("PRAGMA user_version").fetchone()[0] or 0) < 9:
                self._migrate_charger_state_snapshots_table(conn)
                conn.execute("PRAGMA user_version = 9")
            if int(conn.execute("PRAGMA user_version").fetchone()[0] or 0) < 10:
                self._migrate_theme_preference_column(conn)
                conn.execute("PRAGMA user_version = 10")
            if int(conn.execute("PRAGMA user_version").fetchone()[0] or 0) < 11:
                self._migrate_email_verification(conn)
                conn.execute("PRAGMA user_version = 11")
            if int(conn.execute("PRAGMA user_version").fetchone()[0] or 0) < 12:
                self._migrate_system_settings_table(conn)
                conn.execute("PRAGMA user_version = 12")
            if int(conn.execute("PRAGMA user_version").fetchone()[0] or 0) < 13:
                self._migrate_password_reset_table(conn)
                conn.execute("PRAGMA user_version = 13")
            if int(conn.execute("PRAGMA user_version").fetchone()[0] or 0) < 14:
                self._migrate_charger_ocpp_password(conn)
                conn.execute("PRAGMA user_version = 14")
            if int(conn.execute("PRAGMA user_version").fetchone()[0] or 0) < 15:
                self._migrate_charger_went_offline_at(conn)
                conn.execute("PRAGMA user_version = 15")
            if int(conn.execute("PRAGMA user_version").fetchone()[0] or 0) < 16:
                self._migrate_charging_sessions_table(conn)
                conn.execute("PRAGMA user_version = 16")
            if int(conn.execute("PRAGMA user_version").fetchone()[0] or 0) < 17:
                self._migrate_meter_readings_table(conn)
                conn.execute("PRAGMA user_version = 17")
            if int(conn.execute("PRAGMA user_version").fetchone()[0] or 0) < 18:
                self._migrate_is_demo_column(conn)
                conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            conn.execute("PRAGMA foreign_keys = ON")

    def _create_schema(self, conn: sqlite3.Connection, *, include_onboarding: bool = True) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                email TEXT NOT NULL UNIQUE COLLATE NOCASE,
                password_hash TEXT NOT NULL,
                display_name TEXT,
                role TEXT NOT NULL DEFAULT 'User',
                disabled_at TEXT,
                active_charger_id TEXT,
                theme_preference TEXT,
                email_verified_at TEXT,
                totp_secret_ciphertext TEXT,
                totp_enabled_at TEXT,
                totp_pending_secret_ciphertext TEXT,
                totp_pending_created_at TEXT,
                is_demo INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS chargers (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                charge_point_id TEXT,
                display_name TEXT NOT NULL,
                ocpp_password_hash TEXT,
                went_offline_at TEXT,
                created_at TEXT NOT NULL,
                deleted_at TEXT
            );
            """
        )
        self._create_api_keys_table(conn)
        self._create_email_verification_table(conn)
        self._create_system_settings_table(conn)
        self._create_coordinator_state_table(conn)
        self._create_charger_state_snapshots_table(conn)
        self._create_charging_sessions_table(conn)
        self._create_meter_readings_table(conn)
        if include_onboarding:
            self._create_onboarding_table(conn)

    def _create_onboarding_table(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS onboarding_sessions (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                claim_token_hash TEXT NOT NULL,
                ocpp_password_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                consumed_at TEXT
            );
            """
        )

    def _migrate_insecure_onboarding_sessions(self, conn: sqlite3.Connection) -> None:
        """Drop recoverable onboarding credentials from older development schemas."""
        conn.executescript(
            """
            DROP TABLE IF EXISTS onboarding_sessions_insecure_backup;
            ALTER TABLE onboarding_sessions RENAME TO onboarding_sessions_insecure_backup;
            """
        )
        self._create_onboarding_table(conn)
        conn.executescript(
            """
            INSERT INTO onboarding_sessions (
                id, user_id, claim_token_hash, ocpp_password_hash,
                status, created_at, expires_at, consumed_at
            )
            SELECT
                id, user_id, claim_token_hash, ocpp_password_hash,
                status, created_at, expires_at, consumed_at
            FROM onboarding_sessions_insecure_backup;
            DROP TABLE onboarding_sessions_insecure_backup;
            """
        )

    def _migrate_plaintext_session_tokens(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute("SELECT id FROM sessions").fetchall()
        for row in rows:
            token = row["id"]
            conn.execute(
                "UPDATE sessions SET id = ? WHERE id = ?",
                (_hash_secret(token), token),
            )

    def _migrate_totp_columns(self, conn: sqlite3.Connection) -> None:
        user_columns = _table_columns(conn, "users")
        for column, ddl in (
            ("totp_secret_ciphertext", "ALTER TABLE users ADD COLUMN totp_secret_ciphertext TEXT"),
            ("totp_enabled_at", "ALTER TABLE users ADD COLUMN totp_enabled_at TEXT"),
            ("totp_pending_secret_ciphertext", "ALTER TABLE users ADD COLUMN totp_pending_secret_ciphertext TEXT"),
            ("totp_pending_created_at", "ALTER TABLE users ADD COLUMN totp_pending_created_at TEXT"),
        ):
            if column not in user_columns:
                conn.execute(ddl)

    def _migrate_role_column(self, conn: sqlite3.Connection) -> None:
        user_columns = _table_columns(conn, "users")
        if "role" not in user_columns:
            conn.execute(f"ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT '{ROLE_USER}'")

        admin_count = conn.execute(
            "SELECT COUNT(*) FROM users WHERE role = ?",
            (ROLE_ADMIN,),
        ).fetchone()[0]
        if admin_count:
            return

        first_user = conn.execute(
            "SELECT id FROM users ORDER BY created_at ASC LIMIT 1"
        ).fetchone()
        if first_user:
            conn.execute(
                "UPDATE users SET role = ? WHERE id = ?",
                (ROLE_ADMIN, first_user["id"]),
            )

    def _migrate_disabled_column(self, conn: sqlite3.Connection) -> None:
        user_columns = _table_columns(conn, "users")
        if "disabled_at" not in user_columns:
            conn.execute("ALTER TABLE users ADD COLUMN disabled_at TEXT")

    def _migrate_theme_preference_column(self, conn: sqlite3.Connection) -> None:
        user_columns = _table_columns(conn, "users")
        if "theme_preference" not in user_columns:
            conn.execute("ALTER TABLE users ADD COLUMN theme_preference TEXT")

    def _migrate_email_verification(self, conn: sqlite3.Connection) -> None:
        user_columns = _table_columns(conn, "users")
        if "email_verified_at" not in user_columns:
            now = _now()
            conn.execute("ALTER TABLE users ADD COLUMN email_verified_at TEXT")
            conn.execute(
                "UPDATE users SET email_verified_at = ? WHERE email_verified_at IS NULL",
                (now,),
            )
        self._create_email_verification_table(conn)

    def _migrate_active_charger_column(self, conn: sqlite3.Connection) -> None:
        user_columns = _table_columns(conn, "users")
        if "active_charger_id" not in user_columns:
            conn.execute("ALTER TABLE users ADD COLUMN active_charger_id TEXT")

    def _migrate_api_keys_table(self, conn: sqlite3.Connection) -> None:
        self._create_api_keys_table(conn)

    def _create_email_verification_table(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS email_verification_otps (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                otp_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                consumed_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_email_verification_otps_user
                ON email_verification_otps(user_id, created_at);
            """
        )

    def _migrate_system_settings_table(self, conn: sqlite3.Connection) -> None:
        self._create_system_settings_table(conn)

    def _migrate_password_reset_table(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS password_reset_tokens (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                token_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                consumed_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_password_reset_tokens_user
                ON password_reset_tokens(user_id, created_at);
            """
        )

    def _migrate_charger_ocpp_password(self, conn: sqlite3.Connection) -> None:
        cols = _table_columns(conn, "chargers")
        if "ocpp_password_hash" not in cols:
            conn.execute("ALTER TABLE chargers ADD COLUMN ocpp_password_hash TEXT")

    def _migrate_charger_went_offline_at(self, conn: sqlite3.Connection) -> None:
        cols = _table_columns(conn, "chargers")
        if "went_offline_at" not in cols:
            conn.execute("ALTER TABLE chargers ADD COLUMN went_offline_at TEXT")

    def _create_charging_sessions_table(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS charging_sessions (
                id TEXT PRIMARY KEY,
                charge_point_id TEXT NOT NULL,
                started_by TEXT,
                meter_start REAL,
                started_at TEXT NOT NULL,
                stopped_by TEXT,
                meter_stop REAL,
                stopped_at TEXT,
                stop_reason TEXT,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_charging_sessions_charge_point_id
                ON charging_sessions (charge_point_id);
            """
        )

    def _migrate_charging_sessions_table(self, conn: sqlite3.Connection) -> None:
        self._create_charging_sessions_table(conn)

    def _create_meter_readings_table(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meter_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                charge_point_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                group_index INTEGER NOT NULL DEFAULT 0,
                measurand TEXT NOT NULL,
                phase TEXT,
                unit TEXT,
                raw_value TEXT,
                normalized_value REAL,
                context TEXT,
                location TEXT,
                recorded_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_meter_readings_charge_point_ts
                ON meter_readings (charge_point_id, timestamp);
            """
        )

    def _migrate_meter_readings_table(self, conn: sqlite3.Connection) -> None:
        self._create_meter_readings_table(conn)

    def _migrate_is_demo_column(self, conn: sqlite3.Connection) -> None:
        cols = _table_columns(conn, "users")
        if "is_demo" not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN is_demo INTEGER NOT NULL DEFAULT 0")

    def _create_system_settings_table(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS system_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )

    def _migrate_coordinator_state_table(self, conn: sqlite3.Connection) -> None:
        self._create_coordinator_state_table(conn)

    def _migrate_charger_state_snapshots_table(self, conn: sqlite3.Connection) -> None:
        self._create_charger_state_snapshots_table(conn)

    def _migrate_api_key_expiry_nullable(self, conn: sqlite3.Connection) -> None:
        api_key_columns = _table_columns(conn, "api_keys")
        if not api_key_columns:
            self._create_api_keys_table(conn)
            return
        expires_column = next(
            (row for row in conn.execute("PRAGMA table_info(api_keys)").fetchall() if row["name"] == "expires_at"),
            None,
        )
        if not expires_column or not expires_column["notnull"]:
            return
        conn.executescript(
            """
            DROP TABLE IF EXISTS api_keys_nullable_expiry_backup;
            ALTER TABLE api_keys RENAME TO api_keys_nullable_expiry_backup;
            DROP INDEX IF EXISTS idx_api_keys_user_id;
            """
        )
        self._create_api_keys_table(conn)
        conn.executescript(
            """
            INSERT INTO api_keys (
                id, user_id, name, scope, key_prefix, key_hash,
                created_at, expires_at, last_used_at, revoked_at
            )
            SELECT
                id, user_id, name, scope, key_prefix, key_hash,
                created_at, expires_at, last_used_at, revoked_at
            FROM api_keys_nullable_expiry_backup;
            DROP TABLE api_keys_nullable_expiry_backup;
            """
        )

    def _create_api_keys_table(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS api_keys (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                scope TEXT NOT NULL,
                key_prefix TEXT NOT NULL,
                key_hash TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                expires_at TEXT,
                last_used_at TEXT,
                revoked_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_api_keys_user_id ON api_keys(user_id);
            """
        )

    def _create_coordinator_state_table(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS coordinator_state (
                id TEXT PRIMARY KEY,
                snapshot_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )

    def _create_charger_state_snapshots_table(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS charger_state_snapshots (
                charge_point_id TEXT PRIMARY KEY,
                snapshot_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )

    def load_coordinator_state(self, legacy_state_path: Path | None = None) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT snapshot_json FROM coordinator_state WHERE id = ?",
                (COORDINATOR_STATE_ID,),
            ).fetchone()

        if row is not None:
            try:
                return json.loads(row["snapshot_json"])
            except json.JSONDecodeError:
                return None

        if legacy_state_path is None or not legacy_state_path.exists():
            return None

        try:
            snapshot = json.loads(legacy_state_path.read_text())
        except Exception:
            return None

        self.save_coordinator_state(snapshot)
        try:
            legacy_state_path.unlink()
        except OSError:
            pass
        return snapshot

    def save_coordinator_state(self, snapshot: dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO coordinator_state (id, snapshot_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    snapshot_json = excluded.snapshot_json,
                    updated_at = excluded.updated_at
                """,
                (
                    COORDINATOR_STATE_ID,
                    json.dumps(snapshot, separators=(",", ":"), default=str),
                    _now(),
                ),
            )

    def load_charger_state(self, charge_point_id: str | None) -> dict[str, Any] | None:
        charge_point_id = str(charge_point_id or "").strip()
        if not charge_point_id:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT snapshot_json
                FROM charger_state_snapshots
                WHERE charge_point_id = ?
                """,
                (charge_point_id,),
            ).fetchone()
        if row is None:
            return None
        try:
            return json.loads(row["snapshot_json"])
        except json.JSONDecodeError:
            return None

    def save_charger_state(self, charge_point_id: str | None, snapshot: dict[str, Any]) -> None:
        charge_point_id = str(charge_point_id or "").strip()
        if not charge_point_id:
            return
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO charger_state_snapshots (charge_point_id, snapshot_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(charge_point_id) DO UPDATE SET
                    snapshot_json = excluded.snapshot_json,
                    updated_at = excluded.updated_at
                """,
                (
                    charge_point_id,
                    json.dumps(snapshot, separators=(",", ":"), default=str),
                    _now(),
                ),
            )

    def list_charger_states(self, charge_point_ids: list[str]) -> dict[str, dict[str, Any]]:
        normalised_ids = [str(value).strip() for value in charge_point_ids if str(value or "").strip()]
        if not normalised_ids:
            return {}
        placeholders = ",".join("?" for _ in normalised_ids)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT charge_point_id, snapshot_json
                FROM charger_state_snapshots
                WHERE charge_point_id IN ({placeholders})
                """,
                normalised_ids,
            ).fetchall()
        snapshots: dict[str, dict[str, Any]] = {}
        for row in rows:
            try:
                snapshots[row["charge_point_id"]] = json.loads(row["snapshot_json"])
            except json.JSONDecodeError:
                continue
        return snapshots

    def create_user(self, email: str, password: str, display_name: str | None = None) -> AuthUser:
        email = _normalise_email(email)
        _validate_password(password)
        user_id = uuid4().hex
        now = _now()
        try:
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                role = ROLE_ADMIN if self._user_count(conn) == 0 else ROLE_USER
                conn.execute(
                    """
                    INSERT INTO users (id, email, password_hash, display_name, role, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (user_id, email, _hash_password(password), display_name or None, role, now, now),
                )
        except sqlite3.IntegrityError as err:
            raise ValueError("An account already exists for this email address") from err
        return AuthUser(user_id, email, display_name or None, role, False, None, None, None)

    def authenticate_user(self, email: str, password: str) -> AuthUser | None:
        return self.authenticate_password(email, password)

    def authenticate_password(self, email: str, password: str) -> AuthUser | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, email, password_hash, display_name, role, disabled_at, theme_preference,
                       email_verified_at, totp_enabled_at, is_demo
                FROM users
                WHERE email = ?
                """,
                (_normalise_email(email),),
            ).fetchone()
        if row is None or not _verify_password(password, row["password_hash"]):
            return None
        if row["disabled_at"]:
            return None
        return _user_from_row(row)

    def create_email_verification_otp(self, user_id: str) -> dict[str, str]:
        otp = f"{secrets.randbelow(1_000_000):06d}"
        now_dt = datetime.now(UTC)
        now = now_dt.isoformat()
        expires_at = (now_dt + timedelta(minutes=EMAIL_OTP_MINUTES)).isoformat()
        with self._connect() as conn:
            user = conn.execute(
                "SELECT id, email, email_verified_at, disabled_at FROM users WHERE id = ?",
                (user_id,),
            ).fetchone()
            if user is None or user["disabled_at"]:
                raise ValueError("Account is not available")
            if user["email_verified_at"]:
                raise ValueError("Email address is already verified")
            conn.execute(
                """
                UPDATE email_verification_otps
                SET consumed_at = ?
                WHERE user_id = ? AND consumed_at IS NULL
                """,
                (now, user_id),
            )
            conn.execute(
                """
                INSERT INTO email_verification_otps (id, user_id, otp_hash, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (uuid4().hex, user_id, _hash_email_otp(user_id, otp), now, expires_at),
            )
        return {"otp": otp, "email": user["email"], "expires_at": expires_at}

    def resend_email_verification_otp(self, email: str) -> dict[str, str]:
        email = _normalise_email(email)
        now_dt = datetime.now(UTC)
        now = now_dt.isoformat()
        with self._connect() as conn:
            user = conn.execute(
                """
                SELECT id, email, email_verified_at, disabled_at
                FROM users
                WHERE email = ?
                """,
                (email,),
            ).fetchone()
            if user is None or user["disabled_at"]:
                raise ValueError("Account is not available")
            if user["email_verified_at"]:
                raise ValueError("Email address is already verified")
            latest = conn.execute(
                """
                SELECT created_at
                FROM email_verification_otps
                WHERE user_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (user["id"],),
            ).fetchone()
            if latest:
                created_at = _parse_datetime(latest["created_at"])
                if created_at and (now_dt - created_at).total_seconds() < EMAIL_OTP_RESEND_SECONDS:
                    raise ValueError("Please wait before requesting another code")

            otp = f"{secrets.randbelow(1_000_000):06d}"
            expires_at = (now_dt + timedelta(minutes=EMAIL_OTP_MINUTES)).isoformat()
            conn.execute(
                """
                UPDATE email_verification_otps
                SET consumed_at = ?
                WHERE user_id = ? AND consumed_at IS NULL
                """,
                (now, user["id"]),
            )
            conn.execute(
                """
                INSERT INTO email_verification_otps (id, user_id, otp_hash, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (uuid4().hex, user["id"], _hash_email_otp(user["id"], otp), now, expires_at),
            )
        return {"otp": otp, "email": user["email"], "expires_at": expires_at}

    def verify_email_otp(self, email: str, otp: str) -> AuthUser:
        email = _normalise_email(email)
        normalised_otp = "".join(ch for ch in str(otp or "") if ch.isdigit())
        if len(normalised_otp) != 6:
            raise ValueError("Enter the 6 digit code")
        now = _now()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, email, display_name, role, disabled_at, theme_preference,
                       email_verified_at, totp_enabled_at, is_demo
                FROM users
                WHERE email = ?
                """,
                (email,),
            ).fetchone()
            if row is None or row["disabled_at"]:
                raise ValueError("Account is not available")
            if row["email_verified_at"]:
                return _user_from_row(row)
            otp_row = conn.execute(
                """
                SELECT id, otp_hash
                FROM email_verification_otps
                WHERE user_id = ? AND consumed_at IS NULL AND expires_at > ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (row["id"], now),
            ).fetchone()
            if otp_row is None or not hmac.compare_digest(
                otp_row["otp_hash"],
                _hash_email_otp(row["id"], normalised_otp),
            ):
                raise ValueError("Invalid or expired verification code")
            conn.execute(
                "UPDATE email_verification_otps SET consumed_at = ? WHERE id = ?",
                (now, otp_row["id"]),
            )
            conn.execute(
                "UPDATE users SET email_verified_at = ?, updated_at = ? WHERE id = ?",
                (now, now, row["id"]),
            )
            verified = conn.execute(
                """
                SELECT id, email, display_name, role, disabled_at, theme_preference,
                       email_verified_at, totp_enabled_at, is_demo
                FROM users
                WHERE id = ?
                """,
                (row["id"],),
            ).fetchone()
        return _user_from_row(verified)

    def is_email_verification_enabled(self) -> bool:
        value = self.get_system_setting("initial_email_validation_enabled")
        if value is None:
            return True
        return value.lower() not in {"0", "false", "no", "off"}

    def set_email_verification_enabled(self, enabled: bool) -> bool:
        self.set_system_setting("initial_email_validation_enabled", "true" if enabled else "false")
        return enabled

    def is_registration_enabled(self) -> bool:
        value = self.get_system_setting("registration_enabled")
        if value is None:
            return True
        return value.lower() not in {"0", "false", "no", "off"}

    def set_registration_enabled(self, enabled: bool) -> bool:
        self.set_system_setting("registration_enabled", "true" if enabled else "false")
        return enabled

    def is_public_api_enabled(self) -> bool:
        value = self.get_system_setting(SYSTEM_SETTING_PUBLIC_API)
        if value is None:
            return True
        return value.lower() not in {"0", "false", "no", "off"}

    def set_public_api_enabled(self, enabled: bool) -> bool:
        self.set_system_setting(SYSTEM_SETTING_PUBLIC_API, "true" if enabled else "false")
        return enabled

    def create_password_reset_token(self, email: str) -> str | None:
        """Create a password reset token for the given email.

        Returns the raw token, or None if no matching active account exists.
        Callers should send the email regardless to avoid user enumeration.
        """
        try:
            email = _normalise_email(email)
        except ValueError:
            return None
        token = secrets.token_urlsafe(32)
        now_dt = datetime.now(UTC)
        now = now_dt.isoformat()
        expires_at = (now_dt + timedelta(minutes=PASSWORD_RESET_MINUTES)).isoformat()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM users WHERE email = ? AND disabled_at IS NULL",
                (email,),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE password_reset_tokens SET consumed_at = ? WHERE user_id = ? AND consumed_at IS NULL",
                (now, row["id"]),
            )
            conn.execute(
                "INSERT INTO password_reset_tokens (id, user_id, token_hash, created_at, expires_at) VALUES (?, ?, ?, ?, ?)",
                (uuid4().hex, row["id"], _hash_secret(token), now, expires_at),
            )
        return token

    def validate_password_reset_token(self, token: str) -> bool:
        """Return True if the token exists, is unexpired, and has not been consumed."""
        now = _now()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM password_reset_tokens WHERE token_hash = ? AND consumed_at IS NULL AND expires_at > ?",
                (_hash_secret(token), now),
            ).fetchone()
        return row is not None

    def reset_password_with_token(self, token: str, new_password: str) -> None:
        """Validate token, update password, invalidate all sessions, consume token."""
        _validate_password(new_password)
        now = _now()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT pr.id AS reset_id, pr.user_id
                FROM password_reset_tokens pr
                WHERE pr.token_hash = ? AND pr.consumed_at IS NULL AND pr.expires_at > ?
                """,
                (_hash_secret(token), now),
            ).fetchone()
            if row is None:
                raise ValueError("Invalid or expired password reset link")
            conn.execute(
                "UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?",
                (_hash_password(new_password), now, row["user_id"]),
            )
            conn.execute(
                "DELETE FROM sessions WHERE user_id = ?",
                (row["user_id"],),
            )
            conn.execute(
                "UPDATE password_reset_tokens SET consumed_at = ? WHERE id = ?",
                (now, row["reset_id"]),
            )

    def get_system_setting(self, key: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value FROM system_settings WHERE key = ?",
                (key,),
            ).fetchone()
        return row["value"] if row else None

    def set_system_setting(self, key: str, value: str) -> None:
        now = _now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO system_settings (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (key, value, now),
            )

    def mark_user_email_verified(self, user_id: str) -> dict[str, Any]:
        now = _now()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM users WHERE id = ?",
                (user_id,),
            ).fetchone()
            if row is None:
                raise ValueError("User not found")
            conn.execute(
                """
                UPDATE email_verification_otps
                SET consumed_at = ?
                WHERE user_id = ? AND consumed_at IS NULL
                """,
                (now, user_id),
            )
            conn.execute(
                "UPDATE users SET email_verified_at = ?, updated_at = ? WHERE id = ?",
                (now, now, user_id),
            )
            updated = conn.execute(
                """
                SELECT id, email, display_name, role, disabled_at, email_verified_at,
                       totp_enabled_at, created_at, updated_at
                FROM users
                WHERE id = ?
                """,
                (user_id,),
            ).fetchone()
        return _admin_user_from_row(updated)

    def get_account_security(self, user_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT totp_enabled_at, totp_pending_created_at
                FROM users
                WHERE id = ?
                """,
                (user_id,),
            ).fetchone()
        if row is None:
            raise ValueError("User not found")
        return {
            "totp_enabled": bool(row["totp_enabled_at"]),
            "totp_pending": bool(row["totp_pending_created_at"]),
        }

    def change_password(
        self,
        user_id: str,
        current_password: str,
        new_password: str,
        keep_session_token: str | None = None,
    ) -> None:
        _validate_password(new_password)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT password_hash FROM users WHERE id = ?",
                (user_id,),
            ).fetchone()
            if row is None or not _verify_password(current_password, row["password_hash"]):
                raise ValueError("Current password is incorrect")
            now = _now()
            conn.execute(
                "UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?",
                (_hash_password(new_password), now, user_id),
            )
            if keep_session_token:
                conn.execute(
                    "DELETE FROM sessions WHERE user_id = ? AND id <> ?",
                    (user_id, _hash_secret(keep_session_token)),
                )

    def create_totp_setup(self, user_id: str, current_password: str) -> dict[str, str]:
        user = self._user_for_password_check(user_id, current_password)
        secret = _new_totp_secret()
        now = _now()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE users
                SET totp_pending_secret_ciphertext = ?,
                    totp_pending_created_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (_encrypt_secret(secret, self._secret_key), now, now, user_id),
            )
        return {
            "secret": secret,
            "provisioning_uri": _totp_provisioning_uri(user.email, secret),
        }

    def enable_totp(self, user_id: str, code: str) -> None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT totp_pending_secret_ciphertext
                FROM users
                WHERE id = ?
                """,
                (user_id,),
            ).fetchone()
            if row is None or not row["totp_pending_secret_ciphertext"]:
                raise ValueError("No pending 2FA setup")
            secret = _decrypt_secret(row["totp_pending_secret_ciphertext"], self._secret_key)
            if not _verify_totp(secret, code):
                raise ValueError("Invalid authentication code")
            now = _now()
            conn.execute(
                """
                UPDATE users
                SET totp_secret_ciphertext = ?,
                    totp_enabled_at = ?,
                    totp_pending_secret_ciphertext = NULL,
                    totp_pending_created_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (_encrypt_secret(secret, self._secret_key), now, now, user_id),
            )

    def disable_totp(self, user_id: str, current_password: str, code: str) -> None:
        self._user_for_password_check(user_id, current_password)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT totp_secret_ciphertext FROM users WHERE id = ?",
                (user_id,),
            ).fetchone()
            if row is None:
                raise ValueError("User not found")
            if row["totp_secret_ciphertext"]:
                secret = _decrypt_secret(row["totp_secret_ciphertext"], self._secret_key)
                if not _verify_totp(secret, code):
                    raise ValueError("Invalid authentication code")
            now = _now()
            conn.execute(
                """
                UPDATE users
                SET totp_secret_ciphertext = NULL,
                    totp_enabled_at = NULL,
                    totp_pending_secret_ciphertext = NULL,
                    totp_pending_created_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (now, user_id),
            )

    def user_requires_totp(self, user_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT totp_enabled_at FROM users WHERE id = ?",
                (user_id,),
            ).fetchone()
        return bool(row and row["totp_enabled_at"])

    def verify_user_totp(self, user_id: str, code: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT totp_secret_ciphertext FROM users WHERE id = ? AND totp_enabled_at IS NOT NULL",
                (user_id,),
            ).fetchone()
        if row is None or not row["totp_secret_ciphertext"]:
            return False
        secret = _decrypt_secret(row["totp_secret_ciphertext"], self._secret_key)
        return _verify_totp(secret, code)

    def _user_for_password_check(self, user_id: str, password: str) -> AuthUser:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, email, password_hash, display_name, role, disabled_at, theme_preference,
                       email_verified_at, totp_enabled_at, is_demo
                FROM users
                WHERE id = ?
                """,
                (user_id,),
            ).fetchone()
        if row is None or not _verify_password(password, row["password_hash"]):
            raise ValueError("Current password is incorrect")
        return _user_from_row(row)

    def next_registration_role(self) -> str:
        with self._connect() as conn:
            return ROLE_ADMIN if self._user_count(conn) == 0 else ROLE_USER

    def _user_count(self, conn: sqlite3.Connection) -> int:
        return int(conn.execute("SELECT COUNT(*) FROM users").fetchone()[0])

    def create_session(self, user_id: str) -> tuple[str, datetime]:
        session_token = secrets.token_urlsafe(48)
        now = datetime.now(UTC)
        expires_at = now + timedelta(days=SESSION_DAYS)
        with self._connect() as conn:
            user_row = conn.execute(
                "SELECT disabled_at, email_verified_at FROM users WHERE id = ?",
                (user_id,),
            ).fetchone()
            if user_row is None or user_row["disabled_at"]:
                raise ValueError("Account is disabled")
            if not user_row["email_verified_at"]:
                raise ValueError("Email verification required")
            conn.execute(
                "INSERT INTO sessions (id, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
                (_hash_secret(session_token), user_id, now.isoformat(), expires_at.isoformat()),
            )
        return session_token, expires_at

    def get_user_for_session(self, session_id: str | None) -> AuthUser | None:
        if not session_id:
            return None
        now = _now()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT u.id, u.email, u.display_name, u.role, u.disabled_at, u.theme_preference,
                       u.email_verified_at, u.totp_enabled_at, u.is_demo
                FROM sessions s
                JOIN users u ON u.id = s.user_id
                WHERE s.id = ? AND s.expires_at > ? AND u.disabled_at IS NULL
                """,
                (_hash_secret(session_id), now),
            ).fetchone()
        return _user_from_row(row) if row else None

    def delete_session(self, session_token: str | None) -> None:
        if not session_token:
            return
        with self._connect() as conn:
            conn.execute("DELETE FROM sessions WHERE id = ?", (_hash_secret(session_token),))

    def set_user_theme_preference(self, user_id: str, preference: str) -> AuthUser:
        preference = str(preference or "").strip().lower()
        if preference not in {"light", "dark", "system"}:
            raise ValueError("Invalid theme preference")
        now = _now()
        with self._connect() as conn:
            conn.execute(
                "UPDATE users SET theme_preference = ?, updated_at = ? WHERE id = ?",
                (preference, now, user_id),
            )
            row = conn.execute(
                """
                SELECT id, email, password_hash, display_name, role, disabled_at, theme_preference,
                       email_verified_at, totp_enabled_at, is_demo
                FROM users
                WHERE id = ?
                """,
                (user_id,),
            ).fetchone()
        if row is None:
            raise ValueError("User not found")
        return _user_from_row(row)

    def create_onboarding_session(self, user_id: str, origin: str) -> dict[str, Any]:
        ocpp_password = _new_ocpp_password()
        onboarding_id = uuid4().hex
        endpoint_url = origin.rstrip("/") + "/"
        now = datetime.now(UTC)
        expires_at = now + timedelta(minutes=ONBOARDING_MINUTES)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO onboarding_sessions (
                    id, user_id, claim_token_hash, ocpp_password_hash,
                    status, created_at, expires_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    onboarding_id,
                    user_id,
                    "",
                    _hash_secret(ocpp_password),
                    "pending",
                    now.isoformat(),
                    expires_at.isoformat(),
                ),
            )
        session = self.get_onboarding_session(user_id, onboarding_id) or {}
        session["endpoint_url"] = endpoint_url
        session["ocpp_password"] = ocpp_password
        session["credentials_available"] = True
        return session

    def list_onboarding_sessions(self, user_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, status, created_at, expires_at, consumed_at
                FROM onboarding_sessions
                WHERE user_id = ? AND consumed_at IS NULL
                ORDER BY created_at DESC
                """,
                (user_id,),
            ).fetchall()
        return [_onboarding_from_row(row) for row in rows]

    def get_onboarding_session(self, user_id: str, onboarding_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, status, created_at, expires_at, consumed_at
                FROM onboarding_sessions
                WHERE id = ? AND user_id = ?
                """,
                (onboarding_id, user_id),
            ).fetchone()
        return _onboarding_from_row(row) if row else None

    def delete_onboarding_session(self, user_id: str, onboarding_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM onboarding_sessions WHERE id = ? AND user_id = ?",
                (onboarding_id, user_id),
            )
        return cur.rowcount > 0

    def claim_charger_by_password(self, charge_point_id: str, password: str) -> dict[str, Any] | None:
        """Match an inbound OCPP connection to a pending onboarding session via password.

        Returns the newly created charger dict on success, None if no matching session.
        Raises ValueError if the charger ID is already assigned.
        """
        now = datetime.now(UTC)
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, user_id, ocpp_password_hash
                FROM onboarding_sessions
                WHERE consumed_at IS NULL AND expires_at > ?
                ORDER BY created_at ASC
                """,
                (now.isoformat(),),
            ).fetchall()

        for row in rows:
            if hmac.compare_digest(
                _hash_secret(password),
                row["ocpp_password_hash"],
            ):
                user_id = row["user_id"]
                session_id = row["id"]
                # Refuse to adopt a charger into the demo account
                with self._connect() as conn:
                    u = conn.execute(
                        "SELECT is_demo FROM users WHERE id = ?", (user_id,)
                    ).fetchone()
                if u and u["is_demo"]:
                    return None
                # Consume the session first so reconnects don't hit it again
                with self._connect() as conn:
                    conn.execute(
                        "UPDATE onboarding_sessions SET consumed_at = ?, status = ? WHERE id = ?",
                        (now.isoformat(), "consumed", session_id),
                    )
                charger = self.assign_charger_to_user(
                    user_id,
                    charge_point_id,
                    display_name=charge_point_id,
                    ocpp_password_hash=_hash_secret(password),
                )
                return charger
        return None

    def record_charger_offline(self, charge_point_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE chargers SET went_offline_at = ? WHERE charge_point_id = ? AND deleted_at IS NULL",
                (_now(), charge_point_id),
            )

    def record_charger_online(self, charge_point_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE chargers SET went_offline_at = NULL WHERE charge_point_id = ? AND deleted_at IS NULL",
                (charge_point_id,),
            )

    def record_session_start(
        self,
        charge_point_id: str,
        started_by: str | None,
        meter_start: float | None,
        started_at: str,
    ) -> str:
        session_id = uuid4().hex
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO charging_sessions
                    (id, charge_point_id, started_by, meter_start, started_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (session_id, charge_point_id, started_by, meter_start, started_at, _now()),
            )
        return session_id

    def seed_sample_charging_sessions(
        self,
        charge_point_id: str = "1234567890",
        *,
        days: int = 92,
        max_daily_kwh: float = 40.0,
    ) -> int:
        """Create realistic demo energy history if the charger has no sessions."""
        charge_point_id = str(charge_point_id or "").strip()
        if not charge_point_id:
            return 0

        today = datetime.now().date()
        rng = random.Random(f"sample-energy:{charge_point_id}:{today.isoformat()}:v1")
        now = _now()
        rows: list[tuple[Any, ...]] = []
        meter_wh = 8_000_000.0

        for offset in range(days - 1, -1, -1):
            session_date = today - timedelta(days=offset)
            weekend = session_date.weekday() >= 5
            skip_chance = 0.46 if weekend else 0.34
            if rng.random() < skip_chance:
                continue

            if rng.random() < 0.12:
                kwh = rng.uniform(28.0, max_daily_kwh)
            elif weekend:
                kwh = rng.uniform(8.0, 30.0)
            else:
                kwh = rng.uniform(4.0, 22.0)
            kwh = round(min(max_daily_kwh, max(1.2, kwh)), 2)

            start_hour = rng.choice((6, 7, 8, 17, 18, 19, 20))
            start_minute = rng.choice((0, 5, 10, 15, 20, 30, 45))
            started_at_dt = datetime(
                session_date.year,
                session_date.month,
                session_date.day,
                start_hour,
                start_minute,
                tzinfo=UTC,
            )
            duration_minutes = int(max(25, min(520, round((kwh / rng.uniform(6.0, 7.4)) * 60))))
            stopped_at_dt = started_at_dt + timedelta(minutes=duration_minutes)

            meter_start = round(meter_wh, 3)
            meter_stop = round(meter_start + (kwh * 1000.0), 3)
            meter_wh = meter_stop + rng.uniform(0.0, 5.0)

            rows.append((
                f"sample-{charge_point_id}-{session_date.isoformat()}",
                charge_point_id,
                "sample_seed",
                meter_start,
                started_at_dt.isoformat(),
                "sample_seed",
                meter_stop,
                stopped_at_dt.isoformat(),
                "Sample data",
                now,
            ))

        if not rows:
            return 0

        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT COUNT(*)
                FROM charging_sessions
                WHERE charge_point_id = ?
                  AND stopped_at IS NOT NULL
                  AND meter_start IS NOT NULL
                  AND meter_stop IS NOT NULL
                """,
                (charge_point_id,),
            ).fetchone()[0]
            if existing:
                return 0
            conn.executemany(
                """
                INSERT OR IGNORE INTO charging_sessions
                    (id, charge_point_id, started_by, meter_start, started_at,
                     stopped_by, meter_stop, stopped_at, stop_reason, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return conn.execute(
                """
                SELECT COUNT(*)
                FROM charging_sessions
                WHERE charge_point_id = ? AND started_by = 'sample_seed'
                """,
                (charge_point_id,),
            ).fetchone()[0]

    def record_session_stop(
        self,
        charge_point_id: str,
        stopped_by: str | None,
        meter_stop: float | None,
        stopped_at: str,
        stop_reason: str | None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE charging_sessions
                SET stopped_by = ?, meter_stop = ?, stopped_at = ?, stop_reason = ?
                WHERE id = (
                    SELECT id FROM charging_sessions
                    WHERE charge_point_id = ? AND stopped_at IS NULL
                    AND started_at = (
                        SELECT MAX(started_at) FROM charging_sessions
                        WHERE charge_point_id = ? AND stopped_at IS NULL
                    )
                )
                """,
                (stopped_by, meter_stop, stopped_at, stop_reason, charge_point_id, charge_point_id),
            )

    def get_session_stats(self, charge_point_id: str) -> dict:
        with self._connect() as conn:
            last_row = conn.execute(
                """
                SELECT (meter_stop - meter_start) / 1000.0 AS kwh
                FROM charging_sessions
                WHERE charge_point_id = ? AND stopped_at IS NOT NULL
                  AND meter_stop IS NOT NULL AND meter_start IS NOT NULL
                ORDER BY stopped_at DESC
                LIMIT 1
                """,
                (charge_point_id,),
            ).fetchone()
            today_row = conn.execute(
                """
                SELECT SUM(meter_stop - meter_start) / 1000.0 AS kwh
                FROM charging_sessions
                WHERE charge_point_id = ? AND stopped_at IS NOT NULL
                  AND meter_stop IS NOT NULL AND meter_start IS NOT NULL
                  AND DATE(started_at) = DATE('now')
                """,
                (charge_point_id,),
            ).fetchone()
            month_row = conn.execute(
                """
                SELECT SUM(meter_stop - meter_start) / 1000.0 AS kwh
                FROM charging_sessions
                WHERE charge_point_id = ? AND stopped_at IS NOT NULL
                  AND meter_stop IS NOT NULL AND meter_start IS NOT NULL
                  AND DATE(started_at) >= DATE('now', 'start of month')
                  AND DATE(started_at) < DATE('now', 'start of month', '+1 month')
                """,
                (charge_point_id,),
            ).fetchone()
        return {
            "last_session_kwh": round(last_row["kwh"], 3) if last_row and last_row["kwh"] is not None else None,
            "today_kwh": round(today_row["kwh"], 3) if today_row and today_row["kwh"] is not None else None,
            "month_kwh": round(month_row["kwh"], 3) if month_row and month_row["kwh"] is not None else None,
        }

    def list_charging_sessions(
        self,
        user_id: str,
        charge_point_id: str,
        start_time: str | None = None,
        end_time: str | None = None,
        page: int = 1,
        per_page: int = 15,
    ) -> tuple[list[dict], int]:
        charger = self.get_charger_for_user(user_id, charge_point_id)
        if charger is None:
            return [], 0
        filters = ["charge_point_id = ?"]
        params: list = [charge_point_id]
        if start_time:
            filters.append("started_at >= ?")
            params.append(start_time)
        if end_time:
            filters.append("started_at <= ?")
            params.append(end_time)
        where = " AND ".join(filters)
        with self._connect() as conn:
            total = conn.execute(
                f"SELECT COUNT(*) FROM charging_sessions WHERE {where}", params
            ).fetchone()[0]
            offset = (page - 1) * per_page
            rows = conn.execute(
                f"""
                SELECT id, started_by, meter_start, started_at,
                       stopped_by, meter_stop, stopped_at, stop_reason
                FROM charging_sessions
                WHERE {where}
                ORDER BY started_at DESC
                LIMIT ? OFFSET ?
                """,
                [*params, per_page, offset],
            ).fetchall()
        return [dict(r) for r in rows], total

    def get_energy_buckets(
        self,
        user_id: str,
        charge_point_id: str,
        period: str = "daily",
        anchor_date: str | None = None,
    ) -> dict[str, Any] | None:
        if self.get_charger_for_user(user_id, charge_point_id) is None:
            return None

        period = str(period or "daily").strip().lower()
        if period not in {"daily", "weekly", "monthly"}:
            period = "daily"
        anchor = _parse_date_only(anchor_date) or datetime.now().date()

        tick_labels: list[str] | None = None
        if period == "weekly":
            range_start = anchor - timedelta(days=anchor.weekday())
            range_end = range_start + timedelta(days=7)
            labels = [(range_start + timedelta(days=i)).strftime("%a %-d %b %y") for i in range(7)]
            tick_labels = [(range_start + timedelta(days=i)).strftime("%a %d") for i in range(7)]
            buckets = [0.0 for _ in labels]
        elif period == "monthly":
            range_start = anchor.replace(day=1)
            days = monthrange(anchor.year, anchor.month)[1]
            range_end = range_start + timedelta(days=days)
            labels = [(range_start + timedelta(days=i)).strftime("%-d %b %y") for i in range(days)]
            tick_labels = [str(i) for i in range(1, days + 1)]
            buckets = [0.0 for _ in labels]
        else:
            range_start = anchor
            range_end = range_start + timedelta(days=1)
            labels = [f"{hour:02d}:00" for hour in range(24)]
            buckets = [0.0 for _ in labels]

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT meter_start, meter_stop, started_at, stopped_at
                FROM charging_sessions
                WHERE charge_point_id = ?
                  AND stopped_at IS NOT NULL
                  AND meter_start IS NOT NULL
                  AND meter_stop IS NOT NULL
                  AND stopped_at >= ?
                  AND stopped_at < ?
                ORDER BY stopped_at ASC
                """,
                (
                    charge_point_id,
                    f"{(range_start - timedelta(days=1)).isoformat()}T00:00:00",
                    f"{(range_end + timedelta(days=1)).isoformat()}T00:00:00",
                ),
            ).fetchall()

        sessions: list[dict[str, Any]] = []
        for row in rows:
            stopped_at = _parse_datetime(row["stopped_at"])
            if stopped_at is None:
                continue
            local_stopped_at = stopped_at.astimezone() if stopped_at.tzinfo else stopped_at
            stopped_date = local_stopped_at.date()
            if stopped_date < range_start or stopped_date >= range_end:
                continue
            kwh = max(0.0, (float(row["meter_stop"]) - float(row["meter_start"])) / 1000.0)
            if period == "weekly":
                index = (stopped_date - range_start).days
            elif period == "monthly":
                index = stopped_date.day - 1
            else:
                index = local_stopped_at.hour
            if 0 <= index < len(buckets):
                buckets[index] += kwh
            sessions.append({
                "started_at": row["started_at"],
                "stopped_at": row["stopped_at"],
                "kwh": round(kwh, 3),
            })

        rounded_buckets = [round(value, 3) for value in buckets]
        return {
            "period": period,
            "date": anchor.isoformat(),
            "range_start": range_start.isoformat(),
            "range_end": range_end.isoformat(),
            "labels": labels,
            "tick_labels": tick_labels or labels,
            "buckets": rounded_buckets,
            "total_kwh": round(sum(rounded_buckets), 3),
            "sessions": sessions,
        }

    def record_meter_values(self, charge_point_id: str, flattened_samples: list[dict]) -> None:
        if not flattened_samples:
            return
        now = _now()
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO meter_readings
                    (charge_point_id, timestamp, group_index, measurand, phase, unit,
                     raw_value, normalized_value, context, location, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        charge_point_id,
                        s["timestamp"],
                        s["group_index"],
                        s["measurand"],
                        s.get("phase"),
                        s.get("unit"),
                        s.get("raw_value"),
                        s.get("normalized_value"),
                        s.get("context"),
                        s.get("location"),
                        now,
                    )
                    for s in flattened_samples
                ],
            )

    def list_meter_readings(
        self,
        user_id: str,
        charge_point_id: str,
        start_time: str | None = None,
        end_time: str | None = None,
        measurands: list[str] | None = None,
        group_index: int | None = None,
        page: int = 1,
        per_page: int = 15,
    ) -> tuple[list[dict], int]:
        if self.get_charger_for_user(user_id, charge_point_id) is None:
            return [], 0
        filters = ["charge_point_id = ?"]
        params: list = [charge_point_id]
        if start_time:
            filters.append("timestamp >= ?")
            params.append(start_time)
        if end_time:
            filters.append("timestamp <= ?")
            params.append(end_time)
        if measurands:
            placeholders = ",".join("?" * len(measurands))
            filters.append(f"measurand IN ({placeholders})")
            params.extend(measurands)
        if group_index is not None:
            filters.append("group_index = ?")
            params.append(group_index)
        where = " AND ".join(filters)
        with self._connect() as conn:
            total = conn.execute(
                f"SELECT COUNT(*) FROM meter_readings WHERE {where}", params
            ).fetchone()[0]
            offset = (page - 1) * per_page
            rows = conn.execute(
                f"""
                SELECT id, timestamp, group_index AS meter_id, measurand, phase, unit,
                       raw_value, normalized_value, context, location
                FROM meter_readings
                WHERE {where}
                ORDER BY timestamp DESC, group_index, id
                LIMIT ? OFFSET ?
                """,
                [*params, per_page, offset],
            ).fetchall()
        return [dict(r) for r in rows], total

    def get_power_samples(
        self,
        user_id: str,
        charge_point_id: str,
        anchor_date: str | None = None,
        period: str = "daily",
    ) -> dict[str, Any] | None:
        if self.get_charger_for_user(user_id, charge_point_id) is None:
            return None

        anchor = _parse_date_only(anchor_date) or datetime.now().date()
        period = period if period in ("daily", "weekly", "monthly") else "daily"

        if period == "weekly":
            range_start = anchor - timedelta(days=6)
            range_end = anchor + timedelta(days=1)
        elif period == "monthly":
            range_start = anchor - timedelta(days=29)
            range_end = anchor + timedelta(days=1)
        else:
            range_start = anchor
            range_end = anchor + timedelta(days=1)

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT timestamp, group_index, measurand, phase, unit, normalized_value
                FROM meter_readings
                WHERE charge_point_id = ?
                  AND timestamp >= ?
                  AND timestamp < ?
                  AND (
                    (group_index IN (0, 1) AND measurand = 'Power.Active.Import')
                    OR (group_index = 0 AND measurand = 'Current.Import')
                    OR (group_index = 0 AND measurand = 'Voltage')
                  )
                  AND normalized_value IS NOT NULL
                ORDER BY timestamp ASC, group_index ASC, id ASC
                """,
                (
                    charge_point_id,
                    f"{range_start.isoformat()}T00:00:00",
                    f"{range_end.isoformat()}T00:00:00",
                ),
            ).fetchall()

        _SAMPLE_FIELDS = ("grid_power_kw", "ev_power_kw", "ev_current_a", "ev_voltage_v")

        if period == "daily":
            samples_by_timestamp: dict[str, dict[str, Any]] = {}
            scores_by_timestamp: dict[str, dict[str, int]] = {}
            for row in rows:
                parsed = _parse_datetime(row["timestamp"])
                if parsed is None:
                    continue
                local_ts = parsed.astimezone() if parsed.tzinfo else parsed
                if local_ts.date() < range_start or local_ts.date() >= range_end:
                    continue
                field = _power_sample_field(row["group_index"], row["measurand"])
                if field is None:
                    continue
                value = _normalise_power_sample(field, row["normalized_value"])
                if value is None:
                    continue
                timestamp_key = local_ts.replace(second=0, microsecond=0).isoformat()
                sample = samples_by_timestamp.setdefault(timestamp_key, {
                    "timestamp": timestamp_key,
                    "label": local_ts.strftime("%H:%M"),
                    **{f: None for f in _SAMPLE_FIELDS},
                })
                scores = scores_by_timestamp.setdefault(timestamp_key, {})
                score = _power_sample_score(field, row["phase"], row["unit"])
                if field not in scores or score >= scores[field]:
                    sample[field] = round(value, 3 if field.endswith("_kw") else 1)
                    scores[field] = score

            samples = [samples_by_timestamp[k] for k in sorted(samples_by_timestamp)]

        else:
            # Weekly / monthly: aggregate per calendar day — peak power, mean current, mean voltage
            accum: dict[date, dict[str, list[float]]] = {}
            for row in rows:
                parsed = _parse_datetime(row["timestamp"])
                if parsed is None:
                    continue
                local_ts = parsed.astimezone() if parsed.tzinfo else parsed
                d = local_ts.date()
                if d < range_start or d >= range_end:
                    continue
                field = _power_sample_field(row["group_index"], row["measurand"])
                if field is None:
                    continue
                value = _normalise_power_sample(field, row["normalized_value"])
                if value is None:
                    continue
                bucket = accum.setdefault(d, {f: [] for f in _SAMPLE_FIELDS})
                bucket[field].append(value)

            samples = []
            cur = range_start
            while cur < range_end:
                bucket = accum.get(cur, {})
                label = cur.strftime("%-d %b") if period == "monthly" else cur.strftime("%a %-d")

                def _agg(field: str, vals: list[float]) -> float | None:
                    if not vals:
                        return None
                    # Peak for power; mean for current/voltage
                    if field.endswith("_kw"):
                        return round(max(vals), 3)
                    return round(sum(vals) / len(vals), 1)

                samples.append({
                    "timestamp": cur.isoformat(),
                    "label": label,
                    **{f: _agg(f, bucket.get(f, [])) for f in _SAMPLE_FIELDS},
                })
                cur += timedelta(days=1)

        return {
            "date": anchor.isoformat(),
            "period": period,
            "labels": [s["label"] for s in samples],
            "samples": samples,
            "grid_power_kw": [s["grid_power_kw"] for s in samples],
            "ev_power_kw": [s["ev_power_kw"] for s in samples],
            "ev_current_a": [s["ev_current_a"] for s in samples],
            "ev_voltage_v": [s["ev_voltage_v"] for s in samples],
        }

    def verify_charger_password(self, charge_point_id: str, password: str) -> bool:
        """Return True if password matches, or if no password has been set (legacy adoption)."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT ocpp_password_hash FROM chargers WHERE charge_point_id = ? AND deleted_at IS NULL",
                (charge_point_id,),
            ).fetchone()
        if row is None:
            return False
        if not row["ocpp_password_hash"]:
            # Charger adopted before password enforcement — allow through
            return True
        return hmac.compare_digest(_hash_secret(password), row["ocpp_password_hash"])

    def list_chargers(self, user_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            self._ensure_active_charger(conn, user_id)
            rows = conn.execute(
                """
                SELECT
                    c.id,
                    c.charge_point_id,
                    c.display_name,
                    c.created_at,
                    c.went_offline_at,
                    c.id = u.active_charger_id AS active
                FROM chargers c
                JOIN users u ON u.id = c.user_id
                WHERE c.user_id = ? AND c.deleted_at IS NULL
                ORDER BY active DESC, c.created_at DESC
                """,
                (user_id,),
            ).fetchall()
        return [_charger_from_row(row) for row in rows]

    def get_active_charger(self, user_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            self._ensure_active_charger(conn, user_id)
            row = conn.execute(
                """
                SELECT
                    c.id,
                    c.user_id,
                    c.charge_point_id,
                    c.display_name,
                    c.created_at,
                    c.id = u.active_charger_id AS active
                FROM chargers c
                JOIN users u ON u.id = c.user_id
                WHERE c.user_id = ?
                  AND c.id = u.active_charger_id
                  AND c.deleted_at IS NULL
                """,
                (user_id,),
            ).fetchone()
        return _charger_from_row(row) if row else None

    def delete_charger(self, user_id: str, charger_id: str) -> bool:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                """
                UPDATE chargers
                SET deleted_at = ?
                WHERE id = ? AND user_id = ? AND deleted_at IS NULL
                """,
                (_now(), charger_id, user_id),
            )
            if cur.rowcount:
                active = conn.execute(
                    "SELECT active_charger_id FROM users WHERE id = ?",
                    (user_id,),
                ).fetchone()
                if active and active["active_charger_id"] == charger_id:
                    self._ensure_active_charger(conn, user_id, force=True)
        return cur.rowcount > 0

    def switch_active_charger(self, user_id: str, charger_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id
                FROM chargers
                WHERE id = ? AND user_id = ? AND deleted_at IS NULL
                """,
                (charger_id, user_id),
            ).fetchone()
            if row is None:
                raise ValueError("Charger not found")
            now = _now()
            conn.execute(
                "UPDATE users SET active_charger_id = ?, updated_at = ? WHERE id = ?",
                (charger_id, now, user_id),
            )
        return {"active_charger_id": charger_id, "chargers": self.list_chargers(user_id)}

    def _ensure_active_charger(
        self,
        conn: sqlite3.Connection,
        user_id: str,
        *,
        force: bool = False,
    ) -> str | None:
        row = conn.execute(
            "SELECT active_charger_id FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
        active_id = row["active_charger_id"] if row else None
        if active_id and not force:
            active_exists = conn.execute(
                """
                SELECT id
                FROM chargers
                WHERE id = ? AND user_id = ? AND deleted_at IS NULL
                """,
                (active_id, user_id),
            ).fetchone()
            if active_exists:
                return active_id
        replacement = conn.execute(
            """
            SELECT id
            FROM chargers
            WHERE user_id = ? AND deleted_at IS NULL
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()
        replacement_id = replacement["id"] if replacement else None
        conn.execute(
            "UPDATE users SET active_charger_id = ?, updated_at = ? WHERE id = ?",
            (replacement_id, _now(), user_id),
        )
        return replacement_id

    def find_user_by_email(self, email: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, email, display_name, role, disabled_at, email_verified_at,
                       totp_enabled_at, created_at, updated_at
                FROM users
                WHERE email = ?
                """,
                (_normalise_email(email),),
            ).fetchone()
        return _admin_user_from_row(row) if row else None

    def find_user_by_id(self, user_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, email, display_name, role, disabled_at, email_verified_at,
                       totp_enabled_at, is_demo, created_at, updated_at
                FROM users
                WHERE id = ?
                """,
                (str(user_id),),
            ).fetchone()
        return _admin_user_from_row(row) if row else None

    def search_users(self, query: str, limit: int = 8) -> list[dict[str, Any]]:
        query = str(query or "").strip().lower()
        if len(query) < 2:
            return []
        like = f"%{query}%"
        limit = max(1, min(int(limit), 20))
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, email, display_name, role, disabled_at, email_verified_at,
                       totp_enabled_at, created_at, updated_at
                FROM users
                WHERE lower(email) LIKE ? OR lower(coalesce(display_name, '')) LIKE ?
                ORDER BY
                    CASE WHEN lower(email) = ? THEN 0
                         WHEN lower(email) LIKE ? THEN 1
                         ELSE 2
                    END,
                    email ASC
                LIMIT ?
                """,
                (like, like, query, f"{query}%", limit),
            ).fetchall()
        return [_admin_user_from_row(row) for row in rows]

    def set_user_disabled(self, user_id: str, disabled: bool) -> dict[str, Any]:
        now = _now()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM users WHERE id = ?",
                (user_id,),
            ).fetchone()
            if row is None:
                raise ValueError("User not found")
            conn.execute(
                "UPDATE users SET disabled_at = ?, updated_at = ? WHERE id = ?",
                (now if disabled else None, now, user_id),
            )
            if disabled:
                conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
            updated = conn.execute(
                """
                SELECT id, email, display_name, role, disabled_at, email_verified_at,
                       totp_enabled_at, created_at, updated_at
                FROM users
                WHERE id = ?
                """,
                (user_id,),
            ).fetchone()
        return _admin_user_from_row(updated)

    def reset_user_totp(self, user_id: str) -> dict[str, Any]:
        now = _now()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM users WHERE id = ?",
                (user_id,),
            ).fetchone()
            if row is None:
                raise ValueError("User not found")
            conn.execute(
                """
                UPDATE users
                SET totp_secret_ciphertext = NULL,
                    totp_enabled_at = NULL,
                    totp_pending_secret_ciphertext = NULL,
                    totp_pending_created_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (now, user_id),
            )
            updated = conn.execute(
                """
                SELECT id, email, display_name, role, disabled_at, email_verified_at,
                       totp_enabled_at, created_at, updated_at
                FROM users
                WHERE id = ?
                """,
                (user_id,),
            ).fetchone()
        return _admin_user_from_row(updated)

    def delete_user(self, user_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id, email FROM users WHERE id = ?",
                (user_id,),
            ).fetchone()
            if row is None:
                raise ValueError("User not found")
            snapshot = {"id": row["id"], "email": row["email"]}
            conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
        return snapshot

    def list_api_keys(self, user_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, name, scope, key_prefix, created_at, expires_at, last_used_at
                FROM api_keys
                WHERE user_id = ? AND revoked_at IS NULL
                ORDER BY created_at DESC
                """,
                (user_id,),
            ).fetchall()
        return [_api_key_from_row(row) for row in rows]

    def create_api_key(
        self,
        user_id: str,
        name: str,
        scope: str,
        expiry: str,
    ) -> dict[str, Any]:
        name = str(name or "").strip()
        if not name:
            raise ValueError("API key name is required")
        if len(name) > 80:
            raise ValueError("API key name must be 80 characters or fewer")
        scope = str(scope or "").strip().lower()
        if scope not in {"read", "write"}:
            raise ValueError("Scope must be read or write")
        expires_at = _api_key_expiry_date(expiry)
        key_id = uuid4().hex
        prefix = secrets.token_urlsafe(6).replace("-", "").replace("_", "")[:8]
        api_key = f"gevc_{prefix}_{secrets.token_urlsafe(32)}"
        now = _now()
        with self._connect() as conn:
            user = conn.execute(
                "SELECT id FROM users WHERE id = ? AND disabled_at IS NULL",
                (user_id,),
            ).fetchone()
            if user is None:
                raise ValueError("User not found or disabled")
            conn.execute(
                """
                INSERT INTO api_keys (
                    id, user_id, name, scope, key_prefix, key_hash,
                    created_at, expires_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (key_id, user_id, name, scope, f"gevc_{prefix}", _hash_secret(api_key), now, expires_at),
            )
            row = conn.execute(
                """
                SELECT id, name, scope, key_prefix, created_at, expires_at, last_used_at
                FROM api_keys
                WHERE id = ?
                """,
                (key_id,),
            ).fetchone()
        return {"api_key": api_key, "key": _api_key_from_row(row)}

    def validate_api_key(self, api_key: str) -> dict[str, Any] | None:
        """Return (user_id, scope) if the key is valid and not expired/revoked, else None."""
        if not api_key or not api_key.startswith("gevc_"):
            return None
        key_hash = _hash_secret(api_key)
        now = _now()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT k.id, k.user_id, k.scope, u.email, u.role
                FROM api_keys k
                JOIN users u ON u.id = k.user_id
                WHERE k.key_hash = ?
                  AND k.revoked_at IS NULL
                  AND (k.expires_at IS NULL OR k.expires_at > ?)
                  AND u.disabled_at IS NULL
                  AND u.is_demo = 0
                """,
                (key_hash, now),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE api_keys SET last_used_at = ? WHERE id = ?",
                (now, row["id"]),
            )
        return {"user_id": row["user_id"], "scope": row["scope"], "email": row["email"], "role": row["role"]}

    def revoke_api_key(self, user_id: str, key_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE api_keys
                SET revoked_at = ?
                WHERE id = ? AND user_id = ? AND revoked_at IS NULL
                """,
                (_now(), key_id, user_id),
            )
        return cur.rowcount > 0

    def get_charger_by_charge_point_id(self, charge_point_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT c.id, c.user_id, c.charge_point_id, c.display_name, c.created_at, u.email AS owner_email
                FROM chargers c
                JOIN users u ON u.id = c.user_id
                WHERE c.charge_point_id = ? AND c.deleted_at IS NULL
                """,
                (str(charge_point_id),),
            ).fetchone()
        return dict(row) if row else None

    def get_charger_for_user(self, user_id: str, charge_point_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT c.id, c.charge_point_id, c.display_name, c.created_at, c.went_offline_at,
                       c.id = u.active_charger_id AS active
                FROM chargers c
                JOIN users u ON u.id = c.user_id
                WHERE c.charge_point_id = ? AND c.user_id = ? AND c.deleted_at IS NULL
                """,
                (str(charge_point_id), user_id),
            ).fetchone()
        return _charger_from_row(row) if row else None

    def assign_charger_to_user(
        self,
        user_id: str,
        charge_point_id: str,
        display_name: str | None = None,
        ocpp_password_hash: str | None = None,
    ) -> dict[str, Any]:
        charge_point_id = str(charge_point_id or "").strip()
        if not charge_point_id:
            raise ValueError("Charge point id is required")
        now = _now()
        with self._connect() as conn:
            user = conn.execute(
                "SELECT id, email FROM users WHERE id = ? AND disabled_at IS NULL",
                (user_id,),
            ).fetchone()
            if user is None:
                raise ValueError("Target user not found or disabled")
            existing = conn.execute(
                "SELECT id FROM chargers WHERE charge_point_id = ? AND deleted_at IS NULL",
                (charge_point_id,),
            ).fetchone()
            if existing:
                raise ValueError("Charger is already assigned")
            charger_id = uuid4().hex
            conn.execute(
                """
                INSERT INTO chargers (id, user_id, charge_point_id, display_name, ocpp_password_hash, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    charger_id,
                    user_id,
                    charge_point_id,
                    display_name or charge_point_id,
                    ocpp_password_hash,
                    now,
                ),
            )
            self._ensure_active_charger(conn, user_id)
            row = conn.execute(
                """
                SELECT c.id, c.user_id, c.charge_point_id, c.display_name, c.created_at, u.email AS owner_email
                FROM chargers c
                JOIN users u ON u.id = c.user_id
                WHERE c.id = ?
                """,
                (charger_id,),
            ).fetchone()
        return dict(row)

    def seed_demo_account(self) -> None:
        """Drop and re-create the demo user, charger, and session history on every startup."""
        now = _now()
        user_id = "demo-user-00000000000000000000000000000001"
        charger_id = "demo-charger-row-000000000000000000001"
        password_hash = _hash_password(DEMO_PASSWORD)
        ocpp_password_hash = _hash_secret(DEMO_PASSWORD)

        with self._connect() as conn:
            # Remove old demo data completely
            conn.execute("DELETE FROM users WHERE is_demo = 1")
            conn.execute(
                "DELETE FROM charging_sessions WHERE charge_point_id = ?",
                (DEMO_CHARGE_POINT_ID,),
            )
            conn.execute(
                "DELETE FROM meter_readings WHERE charge_point_id = ?",
                (DEMO_CHARGE_POINT_ID,),
            )
            conn.execute(
                "DELETE FROM charger_state_snapshots WHERE charge_point_id = ?",
                (DEMO_CHARGE_POINT_ID,),
            )
            tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            if "portal_actions" in tables:
                conn.execute(
                    "DELETE FROM portal_actions WHERE charge_point_id = ?",
                    (DEMO_CHARGE_POINT_ID,),
                )

            # Insert demo user (pre-verified, no 2FA, not admin)
            conn.execute(
                """
                INSERT INTO users
                    (id, email, password_hash, display_name, role, is_demo,
                     email_verified_at, created_at, updated_at)
                VALUES (?, ?, ?, 'Demo User', 'User', 1, ?, ?, ?)
                """,
                (user_id, DEMO_EMAIL, password_hash, now, now, now),
            )

            # Insert demo charger pre-assigned to demo user
            conn.execute(
                """
                INSERT INTO chargers
                    (id, user_id, charge_point_id, display_name, ocpp_password_hash, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (charger_id, user_id, DEMO_CHARGE_POINT_ID, DEMO_DISPLAY_NAME, ocpp_password_hash, now),
            )
            conn.execute(
                "UPDATE users SET active_charger_id = ? WHERE id = ?",
                (charger_id, user_id),
            )

        # Seed historical sessions and meter readings
        self._seed_demo_sessions()
        self._seed_demo_meter_readings()
        self._seed_demo_coordinator_state()

    def _seed_demo_sessions(self) -> None:
        """Generate 92 days of realistic charging history for the demo charger."""
        today = datetime.now().date()
        rng = random.Random(f"demo-energy:{DEMO_CHARGE_POINT_ID}:{today.isoformat()}:v1")
        now = _now()
        rows: list[tuple] = []
        meter_wh = 8_000_000.0

        for offset in range(91, -1, -1):
            session_date = today - timedelta(days=offset)
            weekend = session_date.weekday() >= 5
            skip_chance = 0.46 if weekend else 0.34
            if rng.random() < skip_chance:
                continue

            if rng.random() < 0.12:
                kwh = rng.uniform(28.0, 40.0)
            elif weekend:
                kwh = rng.uniform(8.0, 30.0)
            else:
                kwh = rng.uniform(4.0, 22.0)
            kwh = round(min(40.0, max(1.2, kwh)), 2)

            start_hour = rng.choice((6, 7, 8, 17, 18, 19, 20))
            start_minute = rng.choice((0, 5, 10, 15, 20, 30, 45))
            started_at_dt = datetime(
                session_date.year, session_date.month, session_date.day,
                start_hour, start_minute, tzinfo=UTC,
            )
            duration_minutes = int(max(25, min(520, round((kwh / rng.uniform(6.0, 7.4)) * 60))))
            stopped_at_dt = started_at_dt + timedelta(minutes=duration_minutes)

            meter_start = round(meter_wh, 3)
            meter_stop = round(meter_start + (kwh * 1000.0), 3)
            meter_wh = meter_stop + rng.uniform(0.0, 5.0)

            rows.append((
                f"demo-{DEMO_CHARGE_POINT_ID}-{session_date.isoformat()}",
                DEMO_CHARGE_POINT_ID,
                "demo_seed",
                meter_start,
                started_at_dt.isoformat(),
                "demo_seed",
                meter_stop,
                stopped_at_dt.isoformat(),
                "Demo data",
                now,
            ))

        if rows:
            with self._connect() as conn:
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO charging_sessions
                        (id, charge_point_id, started_by, meter_start, started_at,
                         stopped_by, meter_stop, stopped_at, stop_reason, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )

    def _seed_demo_meter_readings(self) -> None:
        """Generate 30 days of 5-minute meter readings for groups 0 (EV charger) and 1 (grid).

        Group 0 carries power/current/energy only when a charging session is active.
        Group 1 represents the whole-house grid connection — always has a baseline house load,
        plus the EV charger load on top, so its energy register is always >= group 0's.
        """
        today = datetime.now(UTC).date()
        rng = random.Random(f"demo-meter:{DEMO_CHARGE_POINT_ID}:{today.isoformat()}:v1")
        now = _now()
        sample_interval_minutes = 5
        voltage_v = 230.0

        # Fetch the sessions seeded for the past 30 days so we can align charging windows.
        cutoff = datetime.combine(today - timedelta(days=30), datetime.min.time()).replace(tzinfo=UTC)
        with self._connect() as conn:
            session_rows = conn.execute(
                """
                SELECT started_at, stopped_at, meter_start, meter_stop
                FROM charging_sessions
                WHERE charge_point_id = ? AND stopped_at IS NOT NULL
                  AND started_at >= ?
                ORDER BY started_at
                """,
                (DEMO_CHARGE_POINT_ID, cutoff.isoformat()),
            ).fetchall()

        # Build a lookup: for each 5-min timestamp, is a session active and what power?
        sessions: list[tuple[datetime, datetime, float]] = []
        for row in session_rows:
            try:
                started = _parse_datetime(row["started_at"])
                stopped = _parse_datetime(row["stopped_at"])
                kwh = max(0.0, (float(row["meter_stop"]) - float(row["meter_start"])) / 1000.0)
                duration_h = (stopped - started).total_seconds() / 3600.0
                avg_power_w = (kwh * 1000.0 / duration_h) if duration_h > 0 else 0.0
                sessions.append((started, stopped, avg_power_w))
            except Exception:
                continue

        def _ev_power_at(ts: datetime) -> float:
            for started, stopped, power_w in sessions:
                if started <= ts < stopped:
                    return power_w
            return 0.0

        rows: list[tuple] = []
        # Cumulative energy registers (Wh) — start at realistic lifetime totals
        ev_total_wh = 8_000_000.0       # EV charger lifetime register
        grid_total_wh = 42_000_000.0    # Grid meter lifetime register (much larger)

        start_dt = cutoff
        end_dt = datetime.combine(today + timedelta(days=1), datetime.min.time()).replace(tzinfo=UTC)
        ts = start_dt

        while ts < end_dt:
            ts_str = ts.replace(microsecond=0).isoformat().replace("+00:00", "Z")
            interval_h = sample_interval_minutes / 60.0

            ev_power_w = _ev_power_at(ts)
            # Add a small per-sample jitter to power (±5%)
            if ev_power_w > 0:
                ev_power_w = round(ev_power_w * rng.uniform(0.95, 1.05), 1)
            ev_current_a = round(ev_power_w / voltage_v, 2) if ev_power_w > 0 else 0.0
            ev_delta_wh = ev_power_w * interval_h
            ev_total_wh += ev_delta_wh

            # House baseline: 200–600 W with slow variation
            house_power_w = round(rng.uniform(200.0, 600.0), 1)
            grid_power_w = round(house_power_w + ev_power_w, 1)
            grid_current_a = round(grid_power_w / voltage_v, 2)
            grid_delta_wh = grid_power_w * interval_h
            grid_total_wh += grid_delta_wh

            context = "Sample.Periodic"

            # Group 0 — EV Charger
            for measurand, phase, unit, raw, norm in (
                ("Energy.Active.Import.Register", None,  "Wh", f"{ev_total_wh:.0f}",  ev_total_wh),
                ("Power.Active.Import",           "L1",  "W",  f"{ev_power_w:.1f}",   ev_power_w),
                ("Current.Import",                "L1",  "A",  f"{ev_current_a:.2f}", ev_current_a),
                ("Voltage",                       "L1-N","V",  f"{voltage_v:.1f}",    voltage_v),
            ):
                rows.append((
                    DEMO_CHARGE_POINT_ID, ts_str, 0,
                    measurand, phase, unit, raw, norm, context, "Outlet", now,
                ))

            # Group 1 — Grid Meter
            for measurand, phase, unit, raw, norm in (
                ("Energy.Active.Import.Register", None,  "Wh", f"{grid_total_wh:.0f}",  grid_total_wh),
                ("Power.Active.Import",           "L1",  "W",  f"{grid_power_w:.1f}",   grid_power_w),
                ("Current.Import",                "L1",  "A",  f"{grid_current_a:.2f}", grid_current_a),
                ("Voltage",                       "L1-N","V",  f"{voltage_v:.1f}",      voltage_v),
            ):
                rows.append((
                    DEMO_CHARGE_POINT_ID, ts_str, 1,
                    measurand, phase, unit, raw, norm, context, "Outlet", now,
                ))

            ts += timedelta(minutes=sample_interval_minutes)

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO meter_readings
                    (charge_point_id, timestamp, group_index, measurand, phase, unit,
                     raw_value, normalized_value, context, location, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def _seed_demo_coordinator_state(self) -> None:
        """Inject a realistic coordinator state snapshot for the demo charger.

        Seeds:
        - Octopus Go charging schedule (daily 00:30–05:30 @ 32A, enabled)
        - Two RFID cards with aliases
        - Activity log entries aligned to the last few charging sessions
        """
        today = datetime.now(UTC).date()
        one_month_out = (today + timedelta(days=30)).isoformat() + "T23:59:59+00:00"

        rfid_tags = [
            {
                "id_tag": "04:A3:F2:1B:9C:DE:80",
                "alias": "Home Fob",
                "expires_at": None,
                "enabled": True,
            },
            {
                "id_tag": "04:7E:B8:3A:2F:11:60",
                "alias": "Spare Key Card",
                "expires_at": one_month_out,
                "enabled": True,
            },
        ]

        charging_schedule = [
            {
                "id": "sched-1",
                "name": "Octopus Go",
                "enabled": True,
                "start": "00:30",
                "end": "05:30",
                "days": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
                "current_a": 32,
            }
        ]

        # Fetch the most recent charging sessions to build aligned log entries
        cutoff = datetime.combine(today - timedelta(days=14), datetime.min.time()).replace(tzinfo=UTC)
        with self._connect() as conn:
            session_rows = conn.execute(
                """
                SELECT started_at, stopped_at, meter_start, meter_stop
                FROM charging_sessions
                WHERE charge_point_id = ? AND stopped_at IS NOT NULL
                  AND started_at >= ?
                ORDER BY started_at DESC
                LIMIT 10
                """,
                (DEMO_CHARGE_POINT_ID, cutoff.isoformat()),
            ).fetchall()

        action_log: list[dict[str, Any]] = []

        def _ts(dt: datetime) -> str:
            return dt.replace(microsecond=0).isoformat()

        # One-off config events scattered across the past few days
        base_dt = datetime.combine(today - timedelta(days=3), datetime.min.time()).replace(tzinfo=UTC)
        action_log.append({
            "ts": _ts(base_dt.replace(hour=9, minute=12)),
            "user": "You",
            "action": "Save Schedule",
            "detail": "Octopus Go: daily 00:30–05:30 @ 32A",
            "response": "Success",
            "success": True,
            "via": "Portal",
        })
        action_log.append({
            "ts": _ts(base_dt.replace(hour=9, minute=10)),
            "user": "You",
            "action": "Change Charge Mode",
            "detail": "Boost",
            "response": "Accepted",
            "success": True,
            "via": "Portal",
        })
        action_log.append({
            "ts": _ts((base_dt - timedelta(days=1)).replace(hour=14, minute=3)),
            "user": "You",
            "action": "Add RFID Tag",
            "detail": "Home Fob added",
            "response": "Accepted",
            "success": True,
            "via": "Portal",
        })
        action_log.append({
            "ts": _ts((base_dt - timedelta(days=1)).replace(hour=14, minute=5)),
            "user": "You",
            "action": "Add RFID Tag",
            "detail": "Spare Key Card added",
            "response": "Accepted",
            "success": True,
            "via": "Portal",
        })
        action_log.append({
            "ts": _ts((base_dt - timedelta(days=2)).replace(hour=11, minute=44)),
            "user": "You",
            "action": "Set DNO Fuse Size",
            "detail": "80A",
            "response": "Accepted",
            "success": True,
            "via": "Portal",
        })

        # Per-session start/stop log entries
        for row in session_rows:
            try:
                started = _parse_datetime(row["started_at"])
                stopped = _parse_datetime(row["stopped_at"])
                kwh = round(max(0.0, (float(row["meter_stop"]) - float(row["meter_start"])) / 1000.0), 2)
                action_log.append({
                    "ts": _ts(stopped + timedelta(seconds=2)),
                    "user": "Charger",
                    "action": "Stop Transaction",
                    "detail": f"Session complete — {kwh} kWh delivered",
                    "response": "Accepted",
                    "success": True,
                    "via": "OCPP",
                })
                action_log.append({
                    "ts": _ts(started),
                    "user": "Charger",
                    "action": "Start Transaction",
                    "detail": "RFID: Home Fob",
                    "response": "Accepted",
                    "success": True,
                    "via": "OCPP",
                })
            except Exception:
                continue

        # Sort ascending so newest is last (coordinator appends newest at end)
        action_log.sort(key=lambda e: e["ts"])

        snapshot = {
            "charge_point_id": DEMO_CHARGE_POINT_ID,
            "rfid_tags": rfid_tags,
            "charging_schedule": charging_schedule,
            "action_log": action_log,
        }
        self.save_coordinator_state(snapshot)

    def is_demo_mode_enabled(self) -> bool:
        val = self.get_system_setting(SYSTEM_SETTING_DEMO_MODE)
        return str(val or "").lower() not in ("0", "false", "disabled", "off", "no")

    def set_demo_mode_enabled(self, enabled: bool) -> None:
        self.set_system_setting(SYSTEM_SETTING_DEMO_MODE, "1" if enabled else "0")

    def seed_startup_data(self) -> None:
        """Run all startup pre-seeding in one place."""
        self.seed_demo_account()


_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def _normalise_email(value: str) -> str:
    email = str(value or "").strip().lower()
    if not _EMAIL_RE.match(email) or len(email) > 254:
        raise ValueError("Enter a valid email address")
    return email


def _validate_password(value: str) -> None:
    if len(value or "") < 10:
        raise ValueError("Password must be at least 10 characters")


def _api_key_expiry_date(value: str) -> str | None:
    raw = str(value or "").strip().lower()
    days_by_option = {
        "30": 30,
        "30d": 30,
        "30_days": 30,
        "90": 90,
        "90d": 90,
        "90_days": 90,
        "180": 180,
        "180d": 180,
        "180_days": 180,
        "365": 365,
        "365d": 365,
        "1_year": 365,
        "never": None,
        "none": None,
        "no_expiry": None,
    }
    if raw not in days_by_option:
        raise ValueError("Expiry must be 30 days, 90 days, 180 days, 1 year, or no expiry")
    days = days_by_option[raw]
    if days is None:
        return None
    return (datetime.now(UTC).date() + timedelta(days=days)).isoformat()


def _hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, PASSWORD_ITERATIONS)
    return "pbkdf2_sha256${}${}${}".format(
        PASSWORD_ITERATIONS,
        base64.b64encode(salt).decode(),
        base64.b64encode(digest).decode(),
    )


def _verify_password(password: str, encoded: str) -> bool:
    try:
        algo, iterations_s, salt_s, digest_s = encoded.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        iterations = int(iterations_s)
        salt = base64.b64decode(salt_s.encode())
        expected = base64.b64decode(digest_s.encode())
    except Exception:
        return False
    actual = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    return hmac.compare_digest(actual, expected)


def _hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def _hash_email_otp(user_id: str, otp: str) -> str:
    return _hash_secret(f"email-otp:{user_id}:{otp}")


def _load_secret_key(path: Path) -> bytes:
    configured = os.environ.get("AUTH_SECRET")
    if configured:
        material = configured.encode()
    else:
        key_path = path.parent / "auth_secret.key"
        if not key_path.exists():
            key_path.write_text(secrets.token_urlsafe(48))
            try:
                key_path.chmod(0o600)
            except OSError:
                pass
        material = key_path.read_text().strip().encode()
    return hashlib.sha256(material).digest()


def _fernet(key: bytes) -> Fernet:
    return Fernet(base64.urlsafe_b64encode(key))


def _encrypt_secret(value: str, key: bytes) -> str:
    return _fernet(key).encrypt(value.encode()).decode()


def _decrypt_secret(value: str, key: bytes) -> str:
    # Support legacy v1$nonce$ciphertext$tag format for existing rows.
    if value.startswith("v1$"):
        return _decrypt_secret_legacy(value, key)
    try:
        return _fernet(key).decrypt(value.encode()).decode()
    except Exception as exc:
        raise ValueError("Invalid encrypted secret") from exc


def _decrypt_secret_legacy(value: str, key: bytes) -> str:
    try:
        _, nonce_s, ciphertext_s, tag_s = value.split("$", 3)
        nonce = _b64url_decode(nonce_s)
        ciphertext = _b64url_decode(ciphertext_s)
        tag = _b64url_decode(tag_s)
    except Exception as exc:
        raise ValueError("Invalid encrypted secret") from exc
    expected = hmac.new(key, b"auth-v1" + nonce + ciphertext, hashlib.sha256).digest()
    if not hmac.compare_digest(tag, expected):
        raise ValueError("Invalid encrypted secret")
    out = bytearray()
    counter = 0
    while len(out) < len(ciphertext):
        out.extend(hmac.new(key, b"enc-v1" + nonce + counter.to_bytes(4, "big"), hashlib.sha256).digest())
        counter += 1
    return bytes(a ^ b for a, b in zip(ciphertext, bytes(out[:len(ciphertext)]), strict=True)).decode()


def _b64url_decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _new_ocpp_password() -> str:
    return secrets.token_urlsafe(24)


def _new_totp_secret() -> str:
    return base64.b32encode(secrets.token_bytes(20)).decode().rstrip("=")


def _totp_provisioning_uri(email: str, secret: str) -> str:
    label = f"{TOTP_ISSUER}:{email}"
    return (
        f"otpauth://totp/{quote(label)}"
        f"?secret={quote(secret)}&issuer={quote(TOTP_ISSUER)}&algorithm=SHA1&digits=6&period=30"
    )


def _verify_totp(secret: str, code: str, window: int = 1) -> bool:
    normalised = "".join(ch for ch in str(code or "") if ch.isdigit())
    if len(normalised) != 6:
        return False
    step = int(time.time() // 30)
    return any(
        hmac.compare_digest(_totp_code(secret, step + offset), normalised)
        for offset in range(-window, window + 1)
    )


def _totp_code(secret: str, step: int) -> str:
    key = base64.b32decode(secret + "=" * (-len(secret) % 8), casefold=True)
    digest = hmac.new(key, struct.pack(">Q", step), hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    value = struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7FFFFFFF
    return f"{value % 1_000_000:06d}"


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _parse_date_only(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _power_sample_field(group_index: int, measurand: str) -> str | None:
    if measurand == "Power.Active.Import" and group_index == 1:
        return "grid_power_kw"
    if measurand == "Power.Active.Import" and group_index == 0:
        return "ev_power_kw"
    if measurand == "Current.Import" and group_index == 0:
        return "ev_current_a"
    if measurand == "Voltage" and group_index == 0:
        return "ev_voltage_v"
    return None


def _normalise_power_sample(field: str, value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if field.endswith("_kw"):
        return numeric / 1000.0
    # current and voltage are already in correct units (A, V)
    return numeric


def _power_sample_score(field: str, phase: str | None, unit: str | None) -> int:
    if field == "ev_voltage_v":
        preferred_phases = ("L1-N", None, "L1", "N")
        preferred_units = ("V", None)
    elif field == "ev_current_a":
        preferred_phases = ("L1", None, "L1-N", "N")
        preferred_units = ("A", None)
    else:
        preferred_phases = ("L1", None, "L1-N", "N")
        preferred_units = ("W", "kW", None)
    phase_score = len(preferred_phases) - preferred_phases.index(phase) if phase in preferred_phases else 0
    unit_score = len(preferred_units) - preferred_units.index(unit) if unit in preferred_units else 0
    return phase_score * 10 + unit_score


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _user_from_row(row: sqlite3.Row) -> AuthUser:
    keys = row.keys()
    theme_preference = row["theme_preference"] if "theme_preference" in keys else None
    email_verified_at = row["email_verified_at"] if "email_verified_at" in keys else None
    is_demo = bool(row["is_demo"]) if "is_demo" in keys else False
    return AuthUser(
        row["id"],
        row["email"],
        row["display_name"],
        row["role"],
        bool(row["totp_enabled_at"]),
        row["disabled_at"],
        theme_preference,
        email_verified_at,
        is_demo,
    )


def _admin_user_from_row(row: sqlite3.Row) -> dict[str, Any]:
    keys = row.keys()
    return {
        "id": row["id"],
        "email": row["email"],
        "display_name": row["display_name"],
        "role": row["role"],
        "disabled": bool(row["disabled_at"]),
        "disabled_at": row["disabled_at"],
        "email_verified": bool(row["email_verified_at"]) if "email_verified_at" in keys else True,
        "email_verified_at": row["email_verified_at"] if "email_verified_at" in keys else None,
        "totp_enabled": bool(row["totp_enabled_at"]),
        "is_demo": bool(row["is_demo"]) if "is_demo" in keys else False,
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _charger_from_row(row: sqlite3.Row) -> dict[str, Any]:
    data = dict(row)
    data["active"] = bool(data.get("active"))
    return data


def _api_key_from_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "name": row["name"],
        "scope": row["scope"],
        "key_prefix": row["key_prefix"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "last_used_at": row["last_used_at"],
    }


def _onboarding_from_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "endpoint_url": None,
        "ocpp_password": None,
        "credentials_available": False,
        "status": row["status"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "consumed_at": row["consumed_at"],
    }
